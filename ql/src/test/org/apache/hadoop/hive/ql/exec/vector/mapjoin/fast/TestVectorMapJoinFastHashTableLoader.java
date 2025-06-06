/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TestVectorMapJoinFastHashTableLoader {

    private VectorMapJoinFastHashTableLoader vectorMapJoinFastHashTableLoader;
    private ExecutorService mockExecutorService;
    private VectorMapJoinFastTableContainer mockTableContainer;
    private Configuration conf;
    private final int numLoadThreads = 2;


    @BeforeEach
    void setUp() {
        conf = new Configuration(true);
        mockTableContainer = mock(VectorMapJoinFastTableContainer.class);
        vectorMapJoinFastHashTableLoader = new VectorMapJoinFastHashTableLoader();
    }
    @Test
    public void testSubmitQueueDrainThreads_SubmitsCorrectNumberOfTasks() throws IOException, InterruptedException, SerDeException {
        mockExecutorService = mock(ExecutorService.class);
        vectorMapJoinFastHashTableLoader.initHTLoadingServiceForTest(conf, 1048577, mockExecutorService);
        Future<?> mockFuture = mock(Future.class);
        doReturn(mockFuture)
                .when(mockExecutorService)
                .submit(any(Runnable.class));
        List<Future<?>> futures = vectorMapJoinFastHashTableLoader.submitQueueDrainThreadsForTest(mockTableContainer);
        assertEquals(2, futures.size());
        assertTrue(futures.contains(mockFuture));
        for (Future<?> f : futures) {
            // Since mockFuture.get() returns null by default, this should not throw
            assertDoesNotThrow(() -> f.get());

        }
    }
    @Test
    public void testSubmitQueueDrainThreads_FutureGetThrowsExecutionException() throws
            IOException, InterruptedException, SerDeException, ExecutionException, HiveException {
        ExecutorService executorService = Executors.newFixedThreadPool(numLoadThreads,
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setPriority(Thread.NORM_PRIORITY)
                        .setNameFormat("HT-Load-Thread-%d")
                        .build());
        BlockingQueue<?>[] loadBatchQueues = new BlockingQueue[numLoadThreads];
        for (int i = 0; i < numLoadThreads; i++) {
            loadBatchQueues[i] = mock(LinkedBlockingQueue.class); //We need to mock its behaviour to throw InterruptedException
            doThrow(new InterruptedException("Simulated interruption")).when(loadBatchQueues[i]).take();

        }

        vectorMapJoinFastHashTableLoader.initHTLoadingServiceForTest(conf, 1048577, executorService, loadBatchQueues);
        List<Future<?>> futures = vectorMapJoinFastHashTableLoader.submitQueueDrainThreadsForTest(mockTableContainer);

        assertEquals(2, futures.size());
        for (Future<?> f : futures) {
            ExecutionException thrown = assertThrows(ExecutionException.class, f::get);
            Throwable cause = thrown.getCause();
            assertInstanceOf(RuntimeException.class, cause);
            assertInstanceOf(InterruptedException.class, cause.getCause());
        }
        if (!executorService.isShutdown()) {
            executorService.shutdownNow();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    System.err.println("Executor did not terminate in time");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @AfterEach
    void tearDown() {
    }

}