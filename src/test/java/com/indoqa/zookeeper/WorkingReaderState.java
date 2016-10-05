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

import static com.indoqa.zookeeper.WaitingReaderState.WAITING_READER_STATE;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.KeeperException;

public class WorkingReaderState extends AbstractZooKeeperState {

    public static final WorkingReaderState WORKING_READER_STATE = new WorkingReaderState();

    private final Set<String> processedItems = new HashSet<>();

    private WorkingReaderState() {
        super("Reading");
    }

    public int getReadCount() {
        return this.processedItems.size();
    }

    @Override
    protected void onStart() throws KeeperException {
        List<String> items = this.getChildren("/queue");

        this.processItems(items);

        // nothing left to process, go back to waiting for more items
        this.transitionTo(WAITING_READER_STATE);
    }

    private void processItems(List<String> items) throws KeeperException {
        for (String eachItem : items) {
            // simulate some short running operation
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // ignore
            }

            // remove the completed item and increase our counter
            this.deleteNode("/queue/" + eachItem);
            this.processedItems.add(eachItem);
        }
    }
}
