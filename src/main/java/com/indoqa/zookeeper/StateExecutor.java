/*
 * Licensed to the Indoqa Software Design und Beratung GmbH (Indoqa) under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Indoqa licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.indoqa.zookeeper;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StateExecutor {

    private static final int FAILURE_DELAY = 10000;

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected final Map<String, Object> environmentValues = new HashMap<>();

    protected ZooKeeper zooKeeper;

    private boolean active = true;
    private final Set<ExecutionImpl> executions = new HashSet<>();
    private final AtomicLong executionCount = new AtomicLong();

    private final String connectString;
    private final int sessionTimeout;
    private boolean connecting;

    public StateExecutor(String connectString, int sessionTimeout) {
        this.connectString = connectString;
        this.sessionTimeout = sessionTimeout;

        this.connect();
    }

    public Execution executeState(ZooKeeperState zooKeeperState) {
        ExecutionImpl execution = new ExecutionImpl(zooKeeperState);
        this.executions.add(execution);

        Thread executionThread = new Thread(() -> this.execute(execution), "Execution-" + this.executionCount.getAndIncrement());
        executionThread.start();

        return execution;
    }

    public void setEnvironmentValue(String key, Object value) {
        this.environmentValues.put(key, value);
    }

    public void setEnvironmentValues(String key, Collection<? extends Object> values) {
        this.environmentValues.put(key, values);
    }

    public void stop() {
        this.logger.info("Stopping ...");

        this.active = false;

        // terminate all executions so they can end gracefully
        for (ExecutionImpl eachExecution : this.executions) {
            eachExecution.terminate();
        }

        // close the ZooKeeper instance
        while (this.zooKeeper.getState().isAlive()) {
            try {
                this.zooKeeper.close();
                this.logger.info("ZooKeeper closed.");
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    protected void process(WatchedEvent event) {
        this.logger.debug("Received {}", event);

        if (event.getType() != EventType.None) {
            return;
        }

        switch (event.getState()) {
            case Disconnected:
                this.logger.warn("ZooKeeper session was disconnected.");
                this.restartAllExecutions();
                break;

            case Expired:
                this.logger.warn("ZooKeeper session was expired.");
                this.restartAllExecutions();
                break;

            case ConnectedReadOnly:
            case SyncConnected:
                this.logger.info("Connected to the ZooKeeper ensemble.");
                this.connecting = false;
                this.restartAllExecutions();
                break;

            default:
                this.logger.warn("Unhandled event of type {} and state {}", event.getType(), event.getState());
                break;
        }
    }

    private void connect() {
        this.logger.info("Connecting to ZooKeeper ensemble ...");

        try {
            this.connecting = true;
            this.zooKeeper = new ZooKeeper(this.connectString, this.sessionTimeout, this::process);
        } catch (IOException e) {
            this.logger.error("Could not connect to ZooKeeper ensemble.", e);

            // wait a little before making another attempt
            try {
                Thread.sleep(FAILURE_DELAY);
            } catch (InterruptedException e1) {
                // ignore
            }
        }
    }

    private void execute(ExecutionImpl execution) {
        while (this.active && !execution.isTerminationRequested()) {
            synchronized (this) {
                if (this.connecting) {
                    this.logger.debug("ZooKeeper is still establishing a connection.");
                    this.waitForEvent();
                    continue;
                }

                if (!this.zooKeeper.getState().isAlive()) {
                    this.logger.debug("ZooKeeper is not alive.");
                    this.connect();
                    this.waitForEvent();
                    continue;
                }

                if (!this.zooKeeper.getState().isConnected()) {
                    this.logger.debug("ZooKeeper is not connected.");
                    this.waitForEvent();
                    continue;
                }
            }

            execution.executeStep();
        }

        execution.setTerminated(true);
        this.executions.remove(execution);
    }

    private synchronized void restartAllExecutions() {
        this.logger.info("Restarting executions");

        for (ExecutionImpl eachExecution : this.executions) {
            eachExecution.restart();
        }

        this.notifyAll();
    }

    private synchronized void waitForEvent() {
        try {
            this.wait();
        } catch (InterruptedException e) {
            // do nothing
        }
    }

    private class ExecutionImpl implements Execution {

        private final ZooKeeperState initialState;

        private ZooKeeperState currentState;
        private ZooKeeperState nextState;

        private boolean terminationRequested;
        private boolean terminated;

        public ExecutionImpl(ZooKeeperState initialState) {
            super();

            this.initialState = initialState;
            this.nextState = initialState;
        }

        public void executeStep() {
            try {
                this.currentState = this.nextState;
                this.nextState = null;

                this.currentState.start(StateExecutor.this.zooKeeper, this);
            } catch (ConnectionLossException e) {
                StateExecutor.this.logger.error("Connection lost in state '{}'", this.currentState.getName(), e);
                this.transitionTo(this.initialState);
            } catch (Exception e) {
                StateExecutor.this.logger.error("Failed to start state '{}'.", this.currentState.getName(), e);
                this.restart();
            }

            while (this.nextState == null && !this.terminationRequested) {
                synchronized (this) {
                    try {
                        this.wait();
                    } catch (InterruptedException e) {
                        // do nothing
                    }
                }
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T getValue(String key) {
            return (T) StateExecutor.this.environmentValues.get(key);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Collection<T> getValues(String key) {
            return (Collection<T>) StateExecutor.this.environmentValues.get(key);
        }

        @Override
        public boolean isActive(ZooKeeperState state) {
            return this.currentState == state;
        }

        @Override
        public boolean isTerminated() {
            return this.terminated;
        }

        @Override
        public boolean isTerminationRequested() {
            return this.terminationRequested;
        }

        public void restart() {
            this.nextState = this.initialState;

            synchronized (this) {
                this.notifyAll();
            }
        }

        public void setTerminated(boolean terminated) {
            this.terminated = terminated;
        }

        @Override
        public void terminate() {
            this.terminationRequested = true;

            synchronized (this) {
                this.notifyAll();
            }
        }

        @Override
        public void transitionTo(ZooKeeperState state) {
            this.nextState = state;

            synchronized (this) {
                this.notifyAll();
            }
        }
    }
}
