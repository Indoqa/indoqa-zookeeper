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
import static com.indoqa.zookeeper.WorkingReaderState.WORKING_READER_STATE;
import static com.indoqa.zookeeper.WorkingWriterState.WORKING_WRITER_STATE;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StateExecutorTest {

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
        int itemCount = 2000;

        WORKING_WRITER_STATE.setPendingCount(itemCount);

        StateExecutor stateExecutor = new StateExecutor(this.testingCluster.getConnectString(), 30000);

        Execution writerEnvironment = stateExecutor.executeState(INITIAL_WRITER_STATE);
        Execution readerEnvironment = stateExecutor.executeState(INITIAL_READER_STATE);

        for (InstanceSpec eachInstance : this.testingCluster.getInstances()) {
            this.wait(5000);
            this.logger.info("Killing server " + eachInstance.getConnectString());
            this.testingCluster.killServer(eachInstance);
        }

        for (InstanceSpec eachInstance : this.testingCluster.getInstances()) {
            this.wait(5000);
            this.logger.info("Restarting server " + eachInstance.getConnectString());
            this.testingCluster.restartServer(eachInstance);
        }

        this.logger.info("Waiting for writer to finish");
        for (int i = 0; i < 60 && !writerEnvironment.isTerminated(); i++) {
            this.wait(1000);
        }
        WORKING_READER_STATE.setTerminateIfEmpty(true);

        this.logger.info("Waiting for reader to finish");
        for (int i = 0; i < 60 && !readerEnvironment.isTerminated(); i++) {
            this.wait(1000);
        }

        assertEquals(0, WorkingWriterState.WORKING_WRITER_STATE.getPendingCount());
        assertEquals(itemCount, WorkingReaderState.WORKING_READER_STATE.getReadCount());

        stateExecutor.stop();
    }

    private void wait(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
