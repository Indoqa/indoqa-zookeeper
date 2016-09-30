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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;

public class WorkingReaderState extends AbstractZooKeeperState {

    public static final WorkingReaderState WORKING_READER_STATE = new WorkingReaderState();

    private final AtomicInteger readCount = new AtomicInteger();

    private boolean terminateIfEmpty;

    private WorkingReaderState() {
        super("Reading");
    }

    public int getReadCount() {
        return this.readCount.get();
    }

    public boolean isTerminateIfEmpty() {
        return this.terminateIfEmpty;
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            this.processItems();
        } catch (KeeperException e) {
            // ignore during test
        }
    }

    public void setTerminateIfEmpty(boolean terminateIfEmpty) {
        this.terminateIfEmpty = terminateIfEmpty;

        // make sure there will be a state transition to enter "onStart" again, in case we're currently waiting for new items
        this.transitionTo(this);
    }

    @Override
    protected void onStart() throws KeeperException {
        this.processItems();
    }

    private void processItems() throws KeeperException {
        // get all children under "/queue" and create a watch on it using "this" as the watcher
        List<String> items = this.getChildrenAndWatch("/queue");
        if (items.isEmpty()) {
            if (this.terminateIfEmpty) {
                this.terminate();
            }
            return;
        }

        // process all items that were found
        for (String eachItem : items) {
            // simulate some short running operation
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // ignore
            }

            // remove the completed item and increase our counter
            this.deleteNode("/queue/" + eachItem);
            this.readCount.incrementAndGet();
        }
    }
}
