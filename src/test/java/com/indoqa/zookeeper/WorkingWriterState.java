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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

public final class WorkingWriterState extends AbstractZooKeeperState {

    public static final WorkingWriterState WORKING_WRITER_STATE = new WorkingWriterState();

    private final AtomicInteger pendingCount = new AtomicInteger(0);

    private final byte[] data = new byte[0];

    private WorkingWriterState() {
        super("Writing");
    }

    public int getPendingCount() {
        return this.pendingCount.get();
    }

    public void setPendingCount(int count) {
        this.pendingCount.set(count);
    }

    @Override
    protected void onStart() throws KeeperException {
        this.logger.info("Creating items. {} remaining.", this.pendingCount.get());

        while (this.pendingCount.get() > 0) {
            this.createNode("/queue/item-", this.data, CreateMode.PERSISTENT_SEQUENTIAL);
            this.pendingCount.decrementAndGet();

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // ignore
            }
        }

        this.environment.terminate();
    }
}
