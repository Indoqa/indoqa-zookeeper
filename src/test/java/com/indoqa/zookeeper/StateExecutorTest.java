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

import static com.indoqa.zookeeper.InitialReaderState.INITIAL_READER_STATE;
import static com.indoqa.zookeeper.InitialWriterState.INITIAL_WRITER_STATE;
import static com.indoqa.zookeeper.WaitingReaderState.WAITING_READER_STATE;
import static com.indoqa.zookeeper.WorkingWriterState.WORKING_WRITER_STATE;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StateExecutorTest {

    private static final int MAX_WAIT_SECONDS = 120;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private TestingCluster testingCluster;

    @After
    public void after() throws IOException {
        this.logger.info("Stopping test cluster");
        this.testingCluster.stop();
        this.wait(1000);
        this.testingCluster.close();
    }

    @Before
    public void before() throws Exception {
        this.logger.info("Starting test cluster");
        this.testingCluster = new TestingCluster(3);
        this.testingCluster.start();
        this.wait(1000);
    }

    @Test
    public void stressTest() throws Exception {
        int itemCount = 1000;

        WORKING_WRITER_STATE.setTargetCount(itemCount);

        StateExecutor stateExecutor = new StateExecutor(this.testingCluster.getConnectString(), 30000);

        Execution writerExecution = stateExecutor.executeState(INITIAL_WRITER_STATE);
        Execution readerExecution = stateExecutor.executeState(INITIAL_READER_STATE);

        // slowly kill off all ZooKeeper instances, eventually leading to a broken ensemble
        for (InstanceSpec eachInstance : this.testingCluster.getInstances()) {
            this.wait(5000);
            this.logger.info("Killing server {}", eachInstance.getConnectString());
            this.testingCluster.killServer(eachInstance);
        }

        // slowly restart all ZooKeeper instances, eventually restoring normal operation
        for (InstanceSpec eachInstance : this.testingCluster.getInstances()) {
            this.wait(5000);
            this.logger.info("Restarting server {}", eachInstance.getConnectString());
            this.testingCluster.restartServer(eachInstance);
        }

        // wait for the writer to finish
        this.logger.info("Waiting for writer to finish");
        this.waitForTermination(writerExecution);

        // now signal the reader that it can stop when no more items are available
        WAITING_READER_STATE.setTerminateIfEmpty(true);

        // wait for the reader to finish
        this.logger.info("Waiting for reader to finish");
        this.waitForTermination(readerExecution);

        assertEquals("The writer did not create the required number of items.", itemCount, WORKING_WRITER_STATE.getCreatedCount());

        List<String> children = stateExecutor.zooKeeper.getChildren("/queue", false);
        assertEquals("The reader did not process all of the created items.", 0, children.size());

        stateExecutor.close();
    }

    private void wait(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void waitForTermination(Execution execution) {
        for (int i = 0; i < MAX_WAIT_SECONDS && !execution.isTerminated(); i++) {
            this.wait(1000);
        }
    }
}
