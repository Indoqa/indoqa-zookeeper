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

import java.util.Set;
import java.util.TreeSet;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

public final class WorkingWriterState extends AbstractZooKeeperState {

    public static final WorkingWriterState WORKING_WRITER_STATE = new WorkingWriterState();

    private final byte[] data = new byte[0];
    private Set<String> createdItems = new TreeSet<>();
    private int targetCount;

    private WorkingWriterState() {
        super("Writing");
    }

    private static String getItemName(String path) {
        int index = path.lastIndexOf('/');
        if (index == -1) {
            return path;
        }

        return path.substring(index + 1);
    }

    public int getCreatedCount() {
        return this.createdItems.size();
    }

    public Set<String> getCreatedItems() {
        return this.createdItems;
    }

    public void setTargetCount(int targetCount) {
        this.targetCount = targetCount;
    }

    @Override
    protected void onStart() throws KeeperException {
        this.logger.info("Creating items. {} remaining.", this.targetCount - this.createdItems.size());

        // keep writing items as long as the pending count is not 0
        // don't worry about connection problems, the StateExecutor will restart our execution when necessary
        while (this.targetCount > this.createdItems.size()) {
            // simulate some short running operation
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // ignore
            }

            String node = this.createNode("/queue/item-", this.data, CreateMode.PERSISTENT_SEQUENTIAL);
            this.createdItems.add(getItemName(node));
        }

        // we're done writing, terminate this execution
        this.terminate();
    }
}
