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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.tez.TezContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.runtime.api.ProcessorContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TestVectorMapJoinFastHashTableLoader {

    private VectorMapJoinFastHashTableLoader vectorMapJoinFastHashTableLoader;
    private Configuration hconf;

    @Mock
    private VectorMapJoinFastTableContainer mockTableContainer;

    @Mock
    private TezContext mockTezContext;

    @Mock
    private MapJoinOperator mockJoinOp;

    @Mock
    private ProcessorContext mockProcessorContext;

    @Mock
    private TezCounters mockTezCounters;

    @Mock
    private TezCounter mockTezCounter;

    @Mock
    private MapJoinDesc joinDesc;


    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        hconf = new Configuration(true);
        when(mockTezContext.getTezProcessorContext()).thenReturn(mockProcessorContext);
        when(mockProcessorContext.getCounters()).thenReturn(mockTezCounters);
        when(mockTezCounters.findCounter(anyString(), anyString())).thenReturn(mockTezCounter);

        when(mockJoinOp.getConf()).thenReturn(joinDesc);
        when(mockJoinOp.getCacheKey()).thenReturn("testCacheKey");

        vectorMapJoinFastHashTableLoader = spy(new VectorMapJoinFastHashTableLoader(
                mockTezContext, hconf, mockJoinOp));

    }

    @Test
    public void testSubmitQueueDrainThreads_FutureGetThrowsExecutionException() throws
            IOException, InterruptedException, SerDeException, ExecutionException, HiveException {
        doThrow(new InterruptedException("Simulated interruption")).when(vectorMapJoinFastHashTableLoader)
                .drainAndLoadForPartition(anyInt(), any(VectorMapJoinFastTableContainer.class));
        vectorMapJoinFastHashTableLoader.initHTLoadingService(1048577);
        List<CompletableFuture<Void>> loaderTasks = vectorMapJoinFastHashTableLoader.submitQueueDrainThreads(mockTableContainer);
        assertEquals(2, loaderTasks.size());
        vectorMapJoinFastHashTableLoader.getLoadExecService().shutdown();

        ExecutionException thrown = assertThrows(ExecutionException.class, () -> {
            CompletableFuture.allOf(loaderTasks.toArray(new CompletableFuture[0]))
                    .get(2, TimeUnit.MINUTES);
        });
        Throwable cause = thrown.getCause();
        assertInstanceOf(RuntimeException.class, cause);
        assertInstanceOf(InterruptedException.class, cause.getCause());
    }

    @AfterEach
    void tearDown() {
        ExecutorService executorService = vectorMapJoinFastHashTableLoader.getLoadExecService();
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

}
