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
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.*;

import org.apache.hadoop.hive.llap.LlapDaemonInfo;
import org.apache.hadoop.hive.ql.exec.MemoryMonitorInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mapjoin.MapJoinMemoryExhaustionError;
import org.apache.tez.common.counters.TezCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.exec.tez.TezContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.api.AbstractLogicalInput;

/**
 * HashTableLoader for Tez constructs the hashtable from records read from
 * a broadcast edge.
 */
public class VectorMapJoinFastHashTableLoader implements org.apache.hadoop.hive.ql.exec.HashTableLoader {

  private static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinFastHashTableLoader.class.getName());

  public class HashTableElement {
    private BytesWritable key;
    private BytesWritable value;
    private long hashCode;
    private long deserializeKey;

    public HashTableElement(BytesWritable key, BytesWritable value, long hashCode, long deserializeKey) {
      this.key = key;
      this.value = value;
      this.hashCode = hashCode;
      this.deserializeKey = deserializeKey;
    }

    public BytesWritable getKey() {
      return key;
    }

    public BytesWritable getValue() {
      return value;
    }

    public long getDeserializeKey() {
      return deserializeKey;
    }

    public long getHashCode() {
      return hashCode;
    }
  }

  private Configuration hconf;
  protected MapJoinDesc desc;
  private TezContext tezContext;
  private String cacheKey;
  private TezCounter htLoadCounter;
  private Deque<HashTableElement>[] sharedQ;
  private boolean queueFilled;
  private long numEntries;

  @Override
  public void init(ExecMapperContext context, MapredContext mrContext,
      Configuration hconf, MapJoinOperator joinOp) {
    this.tezContext = (TezContext) mrContext;
    this.hconf = hconf;
    this.desc = joinOp.getConf();
    this.cacheKey = joinOp.getCacheKey();
    String counterGroup = HiveConf.getVar(hconf, HiveConf.ConfVars.HIVECOUNTERGROUP);
    String vertexName = hconf.get(Operator.CONTEXT_NAME_KEY, "");
    String counterName = Utilities.getVertexCounterName(HashTableLoaderCounters.HASHTABLE_LOAD_TIME_MS.name(), vertexName);
    this.htLoadCounter = tezContext.getTezProcessorContext().getCounters().findCounter(counterGroup, counterName);
    this.sharedQ = new Deque[4];
    for(int i=0; i<sharedQ.length; i++){
      sharedQ[i] = new LinkedList<>();
    }
    this.queueFilled = false;
    this.numEntries = 0;
  }

  public void drainQueueAndLoad(VectorMapJoinFastTableContainer vectorMapJoinFastTableContainer, boolean doMemCheck,
      String inputName, MemoryMonitorInfo memoryMonitorInfo, long effectiveThreshold, int partitionId)
      throws InterruptedException, IOException, HiveException, SerDeException {
    while(!sharedQ[partitionId].isEmpty() || !queueFilled) {
      HashTableElement e = sharedQ[partitionId].remove();
      vectorMapJoinFastTableContainer.putRow(e.getKey(), e.getValue(), e.getHashCode(), e.getDeserializeKey());
      synchronized(this) {
        numEntries++;
      }
      if (doMemCheck && (numEntries % memoryMonitorInfo.getMemoryCheckInterval() == 0)) {
        final long estMemUsage = vectorMapJoinFastTableContainer.getEstimatedMemorySize();
        if (estMemUsage > effectiveThreshold) {
          String msg = "Hash table loading exceeded memory limits for input: " + inputName +
              " numEntries: " + numEntries + " estimatedMemoryUsage: " + estMemUsage +
              " effectiveThreshold: " + effectiveThreshold + " memoryMonitorInfo: " + memoryMonitorInfo;
          LOG.error(msg);
          throw new MapJoinMemoryExhaustionError(msg);
        } else {
          if (LOG.isInfoEnabled()) {
            LOG.info("Checking hash table loader memory usage for input: {} numEntries: {} " +
                    "estimatedMemoryUsage: {} effectiveThreshold: {}", inputName, numEntries, estMemUsage,
                effectiveThreshold);
          }
        }
      }
    }
  }

  public void drain(VectorMapJoinFastTableContainer vectorMapJoinFastTableContainer, boolean doMemCheck,
      String inputName, MemoryMonitorInfo memoryMonitorInfo, long effectiveThreshold, ExecutorService executorService) {
    boolean finalDoMemCheck = doMemCheck;
    long finalEffectiveThreshold = effectiveThreshold;
    for (int partitionId = 0; partitionId < 4; ++partitionId) {
      int finalPartitionId = partitionId;
      executorService.submit(() -> {
        try {
          drainQueueAndLoad(vectorMapJoinFastTableContainer, finalDoMemCheck, inputName, memoryMonitorInfo,
              finalEffectiveThreshold, finalPartitionId);
        } catch (HiveException e) {
          e.printStackTrace();
        } catch (IOException | InterruptedException | SerDeException e) {
          e.printStackTrace();
        }
      });
    }
  }

  @Override
  public void load(MapJoinTableContainer[] mapJoinTables,
      MapJoinTableContainerSerDe[] mapJoinTableSerdes)
      throws HiveException {

    Map<Integer, String> parentToInput = desc.getParentToInput();
    Map<Integer, Long> parentKeyCounts = desc.getParentKeyCounts();

    MemoryMonitorInfo memoryMonitorInfo = desc.getMemoryMonitorInfo();
    boolean doMemCheck = false;
    long effectiveThreshold = 0;
    if (memoryMonitorInfo != null) {
      effectiveThreshold = memoryMonitorInfo.getEffectiveThreshold(desc.getMaxMemoryAvailable());

      // hash table loading happens in server side, LlapDecider could kick out some fragments to run outside of LLAP.
      // Flip the flag at runtime in case if we are running outside of LLAP
      if (!LlapDaemonInfo.INSTANCE.isLlap()) {
        memoryMonitorInfo.setLlap(false);
      }
      if (memoryMonitorInfo.doMemoryMonitoring()) {
        doMemCheck = true;
        if (LOG.isInfoEnabled()) {
          LOG.info("Memory monitoring for hash table loader enabled. {}", memoryMonitorInfo);
        }
      }
    }

    if (!doMemCheck) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Not doing hash table memory monitoring. {}", memoryMonitorInfo);
      }
    }
    for (int pos = 0; pos < mapJoinTables.length; pos++) {
      if (pos == desc.getPosBigTable()) {
        continue;
      }

      long numActualEntries = 0;
      String inputName = parentToInput.get(pos);
      LogicalInput input = tezContext.getInput(inputName);

      try {
        input.start();
        tezContext.getTezProcessorContext().waitForAnyInputReady(
            Collections.<Input> singletonList(input));
      } catch (Exception e) {
        throw new HiveException(e);
      }

      try {
        KeyValueReader kvReader = (KeyValueReader) input.getReader();

        Long keyCountObj = parentKeyCounts.get(pos);
        long estKeyCount = (keyCountObj == null) ? -1 : keyCountObj;

        long inputRecords = -1;
        try {
          //TODO : Need to use class instead of string.
          // https://issues.apache.org/jira/browse/HIVE-23981
          inputRecords = ((AbstractLogicalInput) input).getContext().getCounters().
                  findCounter("org.apache.tez.common.counters.TaskCounter",
                          "APPROXIMATE_INPUT_RECORDS").getValue();
        } catch (Exception e) {
          LOG.debug("Failed to get value for counter APPROXIMATE_INPUT_RECORDS", e);
        }
        long keyCount = Math.max(estKeyCount, inputRecords);

        VectorMapJoinFastTableContainer vectorMapJoinFastTableContainer =
                new VectorMapJoinFastTableContainer(desc, hconf, keyCount);

        LOG.info("Loading hash table for input: {} cacheKey: {} tableContainer: {} smallTablePos: {} starttime: {}",
            inputName, cacheKey, vectorMapJoinFastTableContainer.getClass().getSimpleName(), pos,
            System.currentTimeMillis());

        vectorMapJoinFastTableContainer.setSerde(null, null); // No SerDes here.
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        boolean drained = false;
        long startTime = System.currentTimeMillis();
        while (kvReader.next()) {
          long key = vectorMapJoinFastTableContainer.deserializeToKey((BytesWritable) kvReader.getCurrentKey());
          long hashCode = vectorMapJoinFastTableContainer.calculateLongHashCode(key,
              (BytesWritable) kvReader.getCurrentKey());
          sharedQ[(int) (((1 << 2) - 1) & (hashCode >> 62 - 1))].add(new HashTableElement(
              (BytesWritable) kvReader.getCurrentKey(), (BytesWritable) kvReader.getCurrentValue(), hashCode, key));
          ++numActualEntries;
          if (!drained && numActualEntries == 10000) {
            drain(vectorMapJoinFastTableContainer, doMemCheck, inputName, memoryMonitorInfo,
                effectiveThreshold, executorService);
            drained = true;
          }
        }
        queueFilled = true;
        if (!drained) {
          drain(vectorMapJoinFastTableContainer, doMemCheck, inputName, memoryMonitorInfo,
              effectiveThreshold, executorService);
        }

        executorService.shutdown();
        try {
          executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        long delta = System.currentTimeMillis() - startTime;
        htLoadCounter.increment(delta);

        vectorMapJoinFastTableContainer.seal();
        mapJoinTables[pos] = vectorMapJoinFastTableContainer;
        if (doMemCheck) {
          LOG.info("Finished loading hash table for input: {} cacheKey: {} numActualEntries: {} numEntries: {} " +
              "estimatedMemoryUsage: {} Load Time : {} ", inputName, cacheKey, numActualEntries, numEntries,
            vectorMapJoinFastTableContainer.getEstimatedMemorySize(), delta);
        } else {
          LOG.info("Finished loading hash table for input: {} cacheKey: {} numActualEntries: {} numEntries: {} Load Time : {} ",
                  inputName, cacheKey, numActualEntries, numEntries, delta);
        }
      } catch (IOException e) {
        throw new HiveException(e);
      } catch (SerDeException e) {
        throw new HiveException(e);
      } catch (Exception e) {
        throw new HiveException(e);
      }
    }
  }
}
