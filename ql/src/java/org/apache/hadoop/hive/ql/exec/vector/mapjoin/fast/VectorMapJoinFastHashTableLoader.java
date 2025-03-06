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
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAccumulator;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hive.common.Pool;
import org.apache.hadoop.hive.llap.LlapDaemonInfo;
import org.apache.hadoop.hive.ql.exec.MemoryMonitorInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mapjoin.MapJoinMemoryExhaustionError;
import org.apache.hive.common.util.FixedSizedObjectPool;
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

  private Configuration hconf;
  protected MapJoinDesc desc;
  private TezContext tezContext;
  private String cacheKey;
  private TezCounter htLoadCounter;
  private LongAccumulator totalEntries;

  // Parallel loading variables
  private int numLoadThreads;
  private ExecutorService loadExecService;
  private HashTableElementBatch[] elementBatches;
  private FixedSizedObjectPool<HashTableElementBatch> batchPool;
  private BlockingQueue<HashTableElementBatch>[] loadBatchQueues;
  private static final HashTableElementBatch DONE_SENTINEL = new HashTableElementBatch();

  public VectorMapJoinFastHashTableLoader() {

  }

  public VectorMapJoinFastHashTableLoader(TezContext context, Configuration hconf, MapJoinOperator joinOp) {
    this.tezContext = context;
    this.hconf = hconf;
    this.desc = joinOp.getConf();
    this.cacheKey = joinOp.getCacheKey();
    this.htLoadCounter = this.tezContext.getTezProcessorContext().getCounters().findCounter(
        HiveConf.getVar(hconf, HiveConf.ConfVars.HIVE_COUNTER_GROUP), hconf.get(Operator.CONTEXT_NAME_KEY, ""));
  }

  @Override
  public void init(ExecMapperContext context, MapredContext mrContext,
      Configuration hconf, MapJoinOperator joinOp) {
    this.tezContext = (TezContext) mrContext;
    this.hconf = hconf;
    this.desc = joinOp.getConf();
    this.cacheKey = joinOp.getCacheKey();
    String counterGroup = HiveConf.getVar(hconf, HiveConf.ConfVars.HIVE_COUNTER_GROUP);
    String vertexName = hconf.get(Operator.CONTEXT_NAME_KEY, "");
    String counterName = Utilities.getVertexCounterName(HashTableLoaderCounters.HASHTABLE_LOAD_TIME_MS.name(), vertexName);
    this.htLoadCounter = tezContext.getTezProcessorContext().getCounters().findCounter(counterGroup, counterName);
  }

  private void initHTLoadingService(long estKeyCount) {
    if (estKeyCount < VectorMapJoinFastHashTable.FIRST_SIZE_UP) {
      // Avoid many small HTs that will rehash multiple times causing GCs
      this.numLoadThreads = 1;
    } else {
      int initialValue = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVE_MAPJOIN_PARALEL_HASHTABLE_THREADS);
      Preconditions.checkArgument(initialValue > 0, "The number of HT-loading-threads should be positive.");

      int adjustedValue = Integer.highestOneBit(initialValue);
      if (initialValue != adjustedValue) {
        LOG.info("Adjust the number of HT-loading-threads to {}. (Previous value: {})",
            adjustedValue, initialValue);
      }

      this.numLoadThreads = adjustedValue;
    }
    this.totalEntries = new LongAccumulator(Long::sum, 0L);
    this.loadExecService = Executors.newFixedThreadPool(numLoadThreads,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setPriority(Thread.NORM_PRIORITY)
            .setNameFormat("HT-Load-Thread-%d")
            .build());
    // Reuse HashTableElementBatches to reduce GC pressure
    this.batchPool = new FixedSizedObjectPool<>(8 * numLoadThreads, new Pool.PoolObjectHelper<HashTableElementBatch>() {
      @Override
      public HashTableElementBatch create() {
        return new HashTableElementBatch();
      }

      @Override
      public void resetBeforeOffer(HashTableElementBatch elementBatch) {
        elementBatch.reset();
      }
    });
    this.elementBatches = new HashTableElementBatch[numLoadThreads];
    this.loadBatchQueues = new BlockingQueue[numLoadThreads];
    for (int i = 0; i < numLoadThreads; i++) {
      loadBatchQueues[i] = new LinkedBlockingQueue();
      elementBatches[i] = batchPool.take();
    }
  }

  private void submitQueueDrainThreads(VectorMapJoinFastTableContainer vectorMapJoinFastTableContainer)
      throws InterruptedException, IOException, SerDeException {
    for (int partitionId = 0; partitionId < numLoadThreads; partitionId++) {
      int finalPartitionId = partitionId;
      this.loadExecService.submit(() -> {
        try {
          LOG.info("Partition id {} with Queue size {}", finalPartitionId, loadBatchQueues[finalPartitionId].size());
          drainAndLoadForPartition(finalPartitionId, vectorMapJoinFastTableContainer);
        } catch (IOException | InterruptedException | SerDeException | HiveException e) {
          throw new RuntimeException("Failed to start HT Load threads", e);
        }
      });
    }
  }

  private void drainAndLoadForPartition(int partitionId, VectorMapJoinFastTableContainer tableContainer)
      throws InterruptedException, IOException, HiveException, SerDeException {
    LOG.info("Starting draining thread {}", partitionId);
    long totalProcessedEntries = 0;
    HashTableElementBatch batch = null;
    while (batch != DONE_SENTINEL) {
      batch = this.loadBatchQueues[partitionId].take();
      LOG.debug("Draining thread {} batchSize {}", partitionId, batch.getSize());
      for (int i = 0; i < batch.getSize(); i++) {
        try {
          HashTableElement h = batch.getBatch(i);
          tableContainer.putRow(h.getHashCode(), h.getKey(), h.getValue());
        }
        catch (Exception e) {
          throw new HiveException("Exception in draining thread put row", e);
        }
      }
      totalProcessedEntries += batch.getSize();
      LOG.debug("Draining thread {} added {} entries", partitionId, batch.getSize());
      totalEntries.accumulate(batch.getSize());
      // Offer must be at the end as it is resetting the Index(size)
      this.batchPool.offer(batch);
    }

    LOG.info("Terminating draining thread {} after processing Entries {}", partitionId, totalProcessedEntries);
  }

  private void addQueueDoneSentinel() {
    // Add sentinel at the end of queue
    for (int i = 0; i < numLoadThreads; i++) {
      // add sentinel to the Queue not the batch!
      loadBatchQueues[i].add(elementBatches[i]);
      loadBatchQueues[i].add(DONE_SENTINEL);
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
        LOG.info("Memory monitoring for hash table loader enabled. {}", memoryMonitorInfo);
      }
    }

    if (!doMemCheck) {
      LOG.info("Not doing hash table memory monitoring. {}", memoryMonitorInfo);
    }
    for (int pos = 0; pos < mapJoinTables.length; pos++) {
      if (pos == desc.getPosBigTable()) {
        continue;
      }

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
        initHTLoadingService(keyCount);

        VectorMapJoinFastTableContainer tableContainer =
            new VectorMapJoinFastTableContainer(desc, hconf, keyCount, numLoadThreads);

        LOG.info("Loading hash table for input: {} cacheKey: {} tableContainer: {} smallTablePos: {} " +
                "estKeyCount : {} keyCount : {}", inputName, cacheKey,
                tableContainer.getClass().getSimpleName(), pos, estKeyCount, keyCount);

        tableContainer.setSerde(null, null); // No SerDes here.
        // Submit parallel loading Threads
        submitQueueDrainThreads(tableContainer);

        long receivedEntries = 0;
        long startTime = System.currentTimeMillis();
        while (kvReader.next()) {
          BytesWritable currentKey = (BytesWritable) kvReader.getCurrentKey();
          BytesWritable currentValue = (BytesWritable) kvReader.getCurrentValue();
          long hashCode = tableContainer.getHashCode(currentKey);
          int partitionId = (int) ((numLoadThreads - 1) & hashCode); // numLoadThreads divisor must be a power of 2!
          // call getBytes as copy is called later
          HashTableElement h = new HashTableElement(hashCode, currentValue.copyBytes(), currentKey.copyBytes());
          if (elementBatches[partitionId].addElement(h)) {
            loadBatchQueues[partitionId].add(elementBatches[partitionId]);
            elementBatches[partitionId] = batchPool.take();
          }
          receivedEntries++;
          if (doMemCheck && (receivedEntries % memoryMonitorInfo.getMemoryCheckInterval() == 0)) {
            final long estMemUsage = tableContainer.getEstimatedMemorySize();
            if (estMemUsage > effectiveThreshold) {
              String msg = "Hash table loading exceeded memory limits for input: " + inputName +
                  " numEntries: " + receivedEntries + " estimatedMemoryUsage: " + estMemUsage +
                  " effectiveThreshold: " + effectiveThreshold + " memoryMonitorInfo: " + memoryMonitorInfo;
                LOG.error(msg);
                throw new MapJoinMemoryExhaustionError(msg);
              } else {
              LOG.info(
                  "Checking hash table loader memory usage for input: {} numEntries: {} "
                      + "estimatedMemoryUsage: {} effectiveThreshold: {}",
                  inputName, receivedEntries, estMemUsage, effectiveThreshold);
            }
          }
        }

        LOG.info("Finished loading the queue for input: {} waiting {} minutes for TPool shutdown", inputName, 2);
        addQueueDoneSentinel();
        loadExecService.shutdown();

        if (!loadExecService.awaitTermination(2, TimeUnit.MINUTES)) {
          throw new HiveException("Failed to complete the hash table loader. Loading timed out.");
        }
        batchPool.clear();
        LOG.info("Total received entries: {} Threads {} HT entries: {}", receivedEntries, numLoadThreads, totalEntries.get());

        long delta = System.currentTimeMillis() - startTime;
        htLoadCounter.increment(delta);

        tableContainer.seal();
        mapJoinTables[pos] = tableContainer;
        if (doMemCheck) {
          LOG.info("Finished loading hash table for input: {} cacheKey: {} numEntries: {} " +
              "estimatedMemoryUsage: {} Load Time : {} ", inputName, cacheKey, receivedEntries,
            tableContainer.getEstimatedMemorySize(), delta);
        } else {
          LOG.info("Finished loading hash table for input: {} cacheKey: {} numEntries: {} Load Time : {} ",
                  inputName, cacheKey, receivedEntries, delta);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new HiveException(e);
      } catch (HiveException e) {
        throw e;
      } catch (Exception e) {
        throw new HiveException(e);
      } finally {
        if (loadExecService != null && !loadExecService.isTerminated()) {
          loadExecService.shutdownNow();
        }
      }
    }
  }

  private static class HashTableElement {
    private final long hashCode;
    private final byte[] keyBytes;
    private final byte[] valueBytes;

    public HashTableElement(long hashCode, byte[] valueBytes, byte[] keyBytes) {
      this.hashCode = hashCode;
      this.keyBytes = keyBytes;
      this.valueBytes = valueBytes;
    }

    public BytesWritable getKey() {
      return new BytesWritable(this.keyBytes, this.keyBytes.length);
    }

    public BytesWritable getValue() {
      return new BytesWritable(this.valueBytes, this.valueBytes.length);
    }

    public long getHashCode() {
      return this.hashCode;
    }
  }

  private static class HashTableElementBatch {
    private static final int BATCH_SIZE = 1024;
    private final HashTableElement[] batch;
    private int currentIndex;

    public HashTableElementBatch() {
      this.batch = new HashTableElement[BATCH_SIZE];
      this.currentIndex = 0;
    }

    public boolean addElement(HashTableElement h) {
      this.batch[this.currentIndex++] = h;
      return (this.currentIndex == BATCH_SIZE);
    }

    public HashTableElement getBatch(int i) {
      return this.batch[i];
    }

    public int getSize() {
      return this.currentIndex;
    }

    public void reset() {
      this.currentIndex = 0;
    }
  }
}
