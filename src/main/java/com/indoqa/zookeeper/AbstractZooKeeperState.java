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

import static org.apache.zookeeper.CreateMode.PERSISTENT;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractZooKeeperState implements ZooKeeperState, Watcher {

    private static final byte[] NO_DATA = new byte[0];

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected ZooKeeper zooKeeper;
    protected Execution environment;

    private final String name;

    protected AbstractZooKeeperState(String name) {
        super();
        this.name = name;
    }

    @Override
    public final String getName() {
        return this.name;
    }

    @Override
    public void process(WatchedEvent event) {
        // default does nothing
    }

    public final void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    @Override
    public final void start(ZooKeeper zk, Execution executionEnvironment) throws KeeperException {
        this.zooKeeper = zk;
        this.environment = executionEnvironment;

        this.onStart();
    }

    protected final String createNode(String path, byte[] data, CreateMode createMode) throws KeeperException {
        return this.getResult(() -> {
            String result = this.zooKeeper.create(path, data, OPEN_ACL_UNSAFE, createMode);
            this.logger.debug("Created {}", result);
            return result;
        });
    }

    protected final void deleteNode(String path) throws KeeperException {
        this.execute(() -> {
            this.zooKeeper.delete(path, -1);
            this.logger.debug("Deleted {}", path);
        });
    }

    protected final void ensureNodeExists(String path) throws KeeperException {
        this.execute(() -> {
            try {
                this.zooKeeper.create(path, NO_DATA, OPEN_ACL_UNSAFE, PERSISTENT);
                this.logger.debug("Created {}", path);
            } catch (NodeExistsException e) {
                // that's fine
            }
        });
    }

    protected final boolean exists(String path) throws KeeperException {
        return this.getResult(() -> this.zooKeeper.exists(path, false) != null);
    }

    protected final List<String> getChildren(String path) throws KeeperException {
        return this.getResult(() -> this.zooKeeper.getChildren(path, false));
    }

    protected final List<String> getChildrenAndWatch(String path) throws KeeperException {
        return this.getResult(() -> this.zooKeeper.getChildren(path, this));
    }

    protected final byte[] getData(String path, Stat stat) throws KeeperException {
        return this.getResult(() -> this.zooKeeper.getData(path, false, stat));
    }

    protected final String getLastName(String path) {
        int lastSeparator = path.lastIndexOf('/');
        if (lastSeparator == -1) {
            return path;
        }

        return path.substring(lastSeparator + 1);
    }

    protected final List<String> getSortedChildren(String path) throws KeeperException {
        List<String> result = this.getChildren(path);
        Collections.sort(result);
        return result;
    }

    protected final List<String> getSortedChildrenAndWatch(String path) throws KeeperException {
        List<String> result = this.getChildrenAndWatch(path);
        Collections.sort(result);
        return result;
    }

    protected final Stat getStat(String path) throws KeeperException {
        return this.getResult(() -> this.zooKeeper.exists(path, false));
    }

    protected final boolean obtainLock(String path) throws KeeperException {
        return this.getResult(() -> {
            try {
                this.zooKeeper.create(path, NO_DATA, OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                this.logger.debug("Created lock {}", path);
                return true;
            } catch (NodeExistsException e) {
                return false;
            }
        });
    }

    @SuppressWarnings("unused")
    protected void onStart() throws KeeperException {
        // default does nothing
    }

    protected final void setData(String path, byte[] data, int version) throws KeeperException {
        this.execute(() -> this.zooKeeper.setData(path, data, version));
    }

    protected final void terminate() {
        this.environment.terminate();
    }

    protected final void transitionTo(ZooKeeperState zooKeeperState) {
        if (this.environment.isActive(this)) {
            this.environment.transitionTo(zooKeeperState);
        }
    }

    private void execute(InterruptibleZookeeperOperation operation) throws KeeperException {
        while (true) {
            try {
                operation.execute();
                break;
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    private <T> T getResult(InterruptibleZookeeperRequest<T> operation) throws KeeperException {
        while (true) {
            try {
                return operation.execute();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }
}
