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

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StateExecutor implements Closeable {

    private static final int FAILURE_DELAY = 10000;
    private static final int DEFAULT_SESSION_TIMEOUT = 30000;

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected final Map<String, Object> environmentValues = new HashMap<>();

    protected ZooKeeper zooKeeper;

    private boolean active = true;
    private final Set<ExecutionImpl> executions = new HashSet<>();
    private final AtomicLong executionCount = new AtomicLong();

    private final String connectString;
    private final int sessionTimeout;
    private boolean connecting;

    public StateExecutor(String connectString) {
        this(connectString, DEFAULT_SESSION_TIMEOUT);
    }

    public StateExecutor(String connectString, int sessionTimeout) {
        this.connectString = connectString;
        this.sessionTimeout = sessionTimeout;

        this.connect();
    }

    @Override
    public void close() {
        this.logger.info("Closing ZooKeeper connection @ {} using a time-out of {} ms ...", this.connectString, this.sessionTimeout);

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

    public Execution executeState(ZooKeeperState zooKeeperState) {
        return this.executeState(zooKeeperState, null);
    }

    public Execution executeState(ZooKeeperState zooKeeperState, Map<String, Object> additionalEnvironmentValues) {
        String id = UUID.randomUUID().toString();

        this.logger.info("Starting execution '{}' with initial state '{}'.", id, zooKeeperState.getName());
        ExecutionImpl execution = new ExecutionImpl(id, zooKeeperState);
        this.executions.add(execution);

        if (additionalEnvironmentValues != null) {
            additionalEnvironmentValues.entrySet().forEach(entry -> execution.setEnvironmentValue(entry.getKey(), entry.getValue()));
        }

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

    public void waitForTermination(Execution execution) {
        while (!execution.isTerminated()) {
            synchronized (execution) {
                try {
                    execution.wait();
                } catch (InterruptedException e) {
                    // ignore
                }
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

            case Closed:
                this.logger.info("ZooKeeper connection was closed.");
                break;

            default:
                this.logger.warn("Unhandled event of type {} and state {}", event.getType(), event.getState());
                break;
        }
    }

    private void connect() {
        this.logger.info("Connecting to ZooKeeper ensemble @ {} using a time-out of {} ms.", this.connectString, this.sessionTimeout);

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

        this.terminateExecution(execution);
    }

    private synchronized void restartAllExecutions() {
        this.logger.info("Restarting executions");

        for (ExecutionImpl eachExecution : this.executions) {
            eachExecution.restart();
        }

        this.notifyAll();
    }

    private void terminateExecution(ExecutionImpl execution) {
        execution.setTerminated(true);
        this.executions.remove(execution);

        // notify everyone that might be waiting
        synchronized (execution) {
            execution.notifyAll();
        }

        this.logger.info("Terminated execution '{}'.", execution.getId());
    }

    private synchronized void waitForEvent() {
        try {
            this.wait();
        } catch (InterruptedException e) {
            // do nothing
        }
    }

    private class ExecutionImpl implements Execution {

        private final String id;

        private final ZooKeeperState initialState;

        private ZooKeeperState currentState;
        private ZooKeeperState nextState;

        private boolean terminationRequested;
        private boolean terminated;

        private Map<String, Object> values = new HashMap<>();

        public ExecutionImpl(String id, ZooKeeperState initialState) {
            super();

            this.id = id;
            this.initialState = initialState;
            this.nextState = initialState;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T getEnvironmentValue(String key) {
            T result = (T) this.values.get(key);
            if (result != null) {
                return result;
            }

            return (T) StateExecutor.this.environmentValues.get(key);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Collection<T> getEnvironmentValues(String key) {
            Collection<T> result = (Collection<T>) this.values.get(key);
            if (result != null) {
                return result;
            }

            return (Collection<T>) StateExecutor.this.environmentValues.get(key);
        }

        @Override
        public String getId() {
            return this.id;
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

        @Override
        public void setEnvironmentValue(String key, Object value) {
            this.values.put(key, value);
        }

        @Override
        public void setEnvironmentValues(String key, Collection<? extends Object> values) {
            this.values.put(key, values);
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

        protected void executeStep() {
            try {
                this.currentState = this.nextState;
                this.nextState = null;

                this.currentState.start(StateExecutor.this.zooKeeper, this);
            } catch (RuntimeException e) {
                this.handleRuntimeException(e);
            } catch (KeeperException e) {
                this.handleKeeperException(e);
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

        protected void restart() {
            this.nextState = this.initialState;

            synchronized (this) {
                this.notifyAll();
            }
        }

        protected void setTerminated(boolean terminated) {
            this.terminated = terminated;
        }

        private boolean canExecutorRecoverFrom(KeeperException e) {
            if (e == null) {
                return false;
            }

            switch (e.code()) {
                case AUTHFAILED:
                    return false;
                default:
                    return true;
            }
        }

        private boolean canStateRecoverFrom(RuntimeException runtimeException) {
            try {
                return this.currentState.canRecoverFrom(runtimeException);
            } catch (Exception e) {
                StateExecutor.this.logger.error("State could not determine if exception {} is recoverable.", runtimeException, e);
                return false;
            }
        }

        private void handleKeeperException(KeeperException e) {
            if (this.canExecutorRecoverFrom(e)) {
                StateExecutor.this.logger.error("Encountered recoverable exception in state '{}'.", this.currentState.getName(), e);
                this.restart();
                return;
            }

            StateExecutor.this.logger.error("Encountered fatal exception in state '{}'.", this.currentState.getName(), e);
            this.terminate();
        }

        private void handleRuntimeException(RuntimeException e) {
            if (this.canStateRecoverFrom(e)) {
                StateExecutor.this.logger.error("Encountered recoverable exception in state '{}'.", this.currentState.getName(), e);
                this.restart();
                return;
            }

            StateExecutor.this.logger.error("Encountered fatal exception in state '{}'.", this.currentState.getName(), e);
            this.terminate();
        }
    }
}
