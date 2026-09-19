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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.mapjoin.MapJoinMemoryExhaustionError;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.tez.TezContext;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.io.BytesWritable;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Drives the real {@link VectorMapJoinFastHashTableLoader#load} with a hash table container whose
 * putRow throws for one partition, and records what the loader does with the failure.
 */
public class TestVectorMapJoinFastHashTableLoaderDrain {

  private static final int NUM_THREADS = 4;
  private static final int ROWS = 4096;
  // initHTLoadingService only uses more than one thread above FIRST_SIZE_UP
  private static final long EST_KEYS = 4L * VectorMapJoinFastHashTable.FIRST_SIZE_UP;

  /** Feeds ROWS distinct 4-byte keys; key i hashes to partition i % NUM_THREADS. */
  private static final class Rows extends KeyValueReader {
    private int idx = -1;
    private final BytesWritable key = new BytesWritable(new byte[4]);
    private final BytesWritable value = new BytesWritable(new byte[] {1, 2, 3, 4});

    @Override public boolean next() { return ++idx < ROWS; }

    @Override public Object getCurrentKey() {
      byte[] b = key.getBytes();
      b[0] = (byte) idx;
      b[1] = (byte) (idx >>> 8);
      b[2] = (byte) (idx >>> 16);
      b[3] = (byte) (idx >>> 24);
      return key;
    }

    @Override public Object getCurrentValue() { return value; }
  }

  private static long hashOf(BytesWritable k) {
    byte[] b = k.getBytes();
    return (b[0] & 0xffL) | ((b[1] & 0xffL) << 8) | ((b[2] & 0xffL) << 16) | ((b[3] & 0xffL) << 24);
  }

  @Test
  public void drainThreadErrorMustNotProduceAShortHashTable() throws Exception {
    HiveConf hconf = new HiveConf();
    hconf.setIntVar(HiveConf.ConfVars.HIVE_MAPJOIN_PARALEL_HASHTABLE_THREADS, NUM_THREADS);

    Map<Integer, String> parentToInput = new HashMap<>();
    parentToInput.put(1, "smallTableInput");
    Map<Integer, Long> parentKeyCounts = new HashMap<>();
    parentKeyCounts.put(1, EST_KEYS);

    MapJoinDesc desc = mock(MapJoinDesc.class);
    when(desc.getParentToInput()).thenReturn(parentToInput);
    when(desc.getParentKeyCounts()).thenReturn(parentKeyCounts);
    when(desc.getMemoryMonitorInfo()).thenReturn(null);
    when(desc.getPosBigTable()).thenReturn(0);

    MapJoinOperator joinOp = mock(MapJoinOperator.class);
    when(joinOp.getConf()).thenReturn(desc);
    when(joinOp.getCacheKey()).thenReturn("cacheKey");

    TezCounter counter = mock(TezCounter.class);
    when(counter.getValue()).thenReturn(EST_KEYS);
    TezCounters counters = mock(TezCounters.class);
    when(counters.findCounter(anyString(), anyString())).thenReturn(counter);
    ProcessorContext processorContext = mock(ProcessorContext.class);
    when(processorContext.getCounters()).thenReturn(counters);
    InputContext inputContext = mock(InputContext.class);
    when(inputContext.getCounters()).thenReturn(counters);
    AbstractLogicalInput input = mock(AbstractLogicalInput.class);
    when(input.getContext()).thenReturn(inputContext);
    when(input.getReader()).thenReturn(new Rows());
    TezContext tezContext = mock(TezContext.class);
    when(tezContext.getTezProcessorContext()).thenReturn(processorContext);
    when(tezContext.getInput(anyString())).thenReturn(input);

    AtomicInteger loadedRows = new AtomicInteger();
    MapJoinMemoryExhaustionError injected =
        new MapJoinMemoryExhaustionError("simulated expand failure while draining partition 0");

    MapJoinTableContainer[] tables = new MapJoinTableContainer[2];
    AtomicReference<Throwable> thrown = new AtomicReference<>();
    int sealed;

    try (MockedConstruction<VectorMapJoinFastTableContainer> construction =
        Mockito.mockConstruction(VectorMapJoinFastTableContainer.class, (container, ctx) -> {
          when(container.getHashCode(any(BytesWritable.class)))
              .thenAnswer(inv -> hashOf(inv.getArgument(0)));
          doAnswer(inv -> {
            long hashCode = inv.getArgument(0);
            // the loader routes a row to partition (numLoadThreads - 1) & hashCode
            if (((NUM_THREADS - 1) & hashCode) == 0) {
              throw injected;
            }
            loadedRows.incrementAndGet();
            return null;
          }).when(container).putRow(anyLong(), any(BytesWritable.class), any(BytesWritable.class));
        })) {

      VectorMapJoinFastHashTableLoader loader =
          new VectorMapJoinFastHashTableLoader(tezContext, hconf, joinOp);
      try {
        loader.load(tables, null);
      } catch (Throwable t) {
        thrown.set(t);
      }
      sealed = Mockito.mockingDetails(construction.constructed().get(0)).getInvocations().stream()
          .filter(i -> "seal".equals(i.getMethod().getName())).toList().size();
    }

    System.out.println("[drain-failure] thrown=" + thrown.get()
        + " published=" + (tables[1] != null)
        + " sealCalls=" + sealed
        + " rowsLoaded=" + loadedRows.get() + " of " + ROWS);

    Assert.assertEquals("one of " + NUM_THREADS + " partitions must have been dropped",
        ROWS - ROWS / NUM_THREADS, loadedRows.get());
    Assert.assertSame("the drain thread's Error must reach the caller unwrapped",
        injected, thrown.get());
    Assert.assertNull("a hash table missing a partition must never be published", tables[1]);
    Assert.assertEquals("a hash table missing a partition must never be sealed", 0, sealed);
  }

}
