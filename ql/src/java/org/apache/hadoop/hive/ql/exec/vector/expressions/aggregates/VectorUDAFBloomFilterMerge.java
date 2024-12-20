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

package org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationBufferRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationDesc;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFBloomFilter.GenericUDAFBloomFilterEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hive.common.util.BloomKFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.ql.exec.FunctionRegistry.BLOOM_FILTER_FUNCTION;
import static org.apache.hive.common.util.BloomKFilter.START_OF_SERIALIZED_LONGS;

public class VectorUDAFBloomFilterMerge extends VectorAggregateExpression {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(VectorUDAFBloomFilterMerge.class);

  private long expectedEntries = -1;
  private transient int aggBufferSize;
  private transient int numThreads = HiveConf.ConfVars.TEZ_BLOOM_FILTER_MERGE_THREADS.defaultIntVal;

  /**
   * class for storing the current aggregate value.
   */
  static final class Aggregation implements AggregationBuffer {
    private static final long serialVersionUID = 1L;

    byte[] bfBytes;
    private ExecutorService executor;
    private int numThreads;
    private BloomFilterMergeWorker[] workers;
    private AtomicBoolean aborted = new AtomicBoolean(false);

    public Aggregation(long expectedEntries, int numThreads) {
      bfBytes = BloomKFilter.getInitialBytes(expectedEntries);

      if (numThreads < 0) {
        throw new RuntimeException(
            "invalid number of threads for bloom filter merge: " + numThreads);
      }

      this.numThreads = numThreads;
    }

    @Override
    public int getVariableSize() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {
      // Do not change the initial bytes which contain NumHashFunctions/NumBits!
      Arrays.fill(bfBytes, BloomKFilter.START_OF_SERIALIZED_LONGS, bfBytes.length, (byte) 0);
    }

    public void mergeBloomFilterBytesFromInputColumn(BytesColumnVector inputColumn,
        int batchSize, boolean selectedInUse, int[] selected) {
      if (executor == null) {
        initExecutor();
      }

      // split every bloom filter (represented by a part of a byte[]) across workers
      for (int j = 0; j < batchSize; j++) {
        if (!selectedInUse) {
          if (inputColumn.noNulls) {
            splitVectorAcrossWorkers(workers, inputColumn.vector[j], inputColumn.start[j],
                inputColumn.length[j]);
          } else if (!inputColumn.isNull[j]) {
            splitVectorAcrossWorkers(workers, inputColumn.vector[j], inputColumn.start[j],
                inputColumn.length[j]);
          }
        } else if (inputColumn.noNulls) {
          int i = selected[j];
          splitVectorAcrossWorkers(workers, inputColumn.vector[i], inputColumn.start[i],
              inputColumn.length[i]);
        } else {
          int i = selected[j];
          if (!inputColumn.isNull[i]) {
            splitVectorAcrossWorkers(workers, inputColumn.vector[i], inputColumn.start[i],
                inputColumn.length[i]);
          }
        }
      }
    }

    private void initExecutor() {
      LOG.info("Number of threads used for bloom filter merge: {}", numThreads);

      executor = Executors.newFixedThreadPool(numThreads);

      workers = new BloomFilterMergeWorker[numThreads];
      for (int f = 0; f < numThreads; f++) {
        workers[f] = new BloomFilterMergeWorker(bfBytes, 0, bfBytes.length, aborted);
        executor.submit(workers[f]);
      }
    }

    public int getNumberOfWaitingMergeTasks(){
      int size = 0;
      for (BloomFilterMergeWorker w : workers){
        size += w.queue.size();
      }
      return size;
    }

    private static void splitVectorAcrossWorkers(BloomFilterMergeWorker[] workers, byte[] bytes,
        int start, int length) {
      if (bytes == null || length == 0) {
        return;
      }
      /*
       * This will split a byte[] across workers as below:
       * let's say there are 10 workers for 7813 bytes, in this case
       * length: 7813, elementPerBatch: 781
       * bytes assigned to workers: inclusive lower bound, exclusive upper bound
       * 1. worker: 5 -> 786
       * 2. worker: 786 -> 1567
       * 3. worker: 1567 -> 2348
       * 4. worker: 2348 -> 3129
       * 5. worker: 3129 -> 3910
       * 6. worker: 3910 -> 4691
       * 7. worker: 4691 -> 5472
       * 8. worker: 5472 -> 6253
       * 9. worker: 6253 -> 7034
       * 10. worker: 7034 -> 7813 (last element per batch is: 779)
       *
       * This way, a particular worker will be given with the same part
       * of all bloom filters along with the shared base bloom filter,
       * so the bitwise OR function will not be a subject of threading/sync issues.
       */
      int elementPerBatch =
          (int) Math.ceil((double) (length - START_OF_SERIALIZED_LONGS) / workers.length);

      for (int w = 0; w < workers.length; w++) {
        int modifiedStart = START_OF_SERIALIZED_LONGS + w * elementPerBatch;
        int modifiedLength = (w == workers.length - 1)
          ? length - (START_OF_SERIALIZED_LONGS + w * elementPerBatch) : elementPerBatch;

        ElementWrapper wrapper =
            new ElementWrapper(bytes, start, length, modifiedStart, modifiedLength);
        workers[w].add(wrapper);
      }
    }

    public void shutdownAndWaitForMergeTasks(Aggregation agg, boolean aborted) {
      if (aborted){
        agg.aborted.set(true);
      }
      /**
       * Executor.shutdownNow() is supposed to send Thread.interrupt to worker threads, and they are
       * supposed to finish their work.
       */
      executor.shutdownNow();
      try {
        executor.awaitTermination(180, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOG.warn("Bloom filter merge is interrupted while waiting to finish, this is unexpected",
            e);
      }
    }
  }

  private static class BloomFilterMergeWorker implements Runnable {
    private BlockingQueue<ElementWrapper> queue;
    private byte[] bfAggregation;
    private int bfAggregationStart;
    private int bfAggregationLength;
    private AtomicBoolean aborted;

    public BloomFilterMergeWorker(byte[] bfAggregation, int bfAggregationStart,
        int bfAggregationLength, AtomicBoolean aborted) {
      this.bfAggregation = bfAggregation;
      this.bfAggregationStart = bfAggregationStart;
      this.bfAggregationLength = bfAggregationLength;
      this.queue = new LinkedBlockingDeque<>();
      this.aborted = aborted;
    }

    public void add(ElementWrapper wrapper) {
      queue.add(wrapper);
    }

    @Override
    public void run() {
      while (true) {
        ElementWrapper currentBf = null;
        try {
          currentBf = queue.take();
          // at this point we have a currentBf wrapper which contains the whole byte[] of the
          // serialized bloomfilter, but we only want to merge a modified "start -> start+length"
          // part of it, which is pointed by modifiedStart/modifiedLength fields by ElementWrapper
          merge(currentBf);
        } catch (InterruptedException e) {// Executor.shutdownNow() is called
          if (!queue.isEmpty()){
            LOG.info(
                "bloom filter merge was interrupted while processing and queue is still not empty (size: {})"
                    + ", this is fine in case of shutdownNow", queue.size());
          }
          if (aborted.get()) {
            LOG.info("bloom filter merge was aborted, won't finish merging...");
            break;
          }
          while (!queue.isEmpty()) { // time to finish work if any
            ElementWrapper lastBloomFilter = queue.poll();
            merge(lastBloomFilter);
          }
          break;
        }
      }
    }

    private void merge(ElementWrapper bloomFilterWrapper) {
      BloomKFilter.mergeBloomFilterBytes(bfAggregation, bfAggregationStart, bfAggregationLength,
          bloomFilterWrapper.bytes, bloomFilterWrapper.start, bloomFilterWrapper.length,
          bloomFilterWrapper.modifiedStart,
          bloomFilterWrapper.modifiedStart + bloomFilterWrapper.modifiedLength);
    }
  }

  public static class ElementWrapper {
    public byte[] bytes;
    public int start;
    public int length;
    public int modifiedStart;
    public int modifiedLength;

    public ElementWrapper(byte[] bytes, int start, int length, int modifiedStart, int modifiedLength) {
      this.bytes = bytes;
      this.start = start;
      this.length = length;
      this.modifiedStart = modifiedStart;
      this.modifiedLength = modifiedLength;
    }
  }

  // This constructor is used to momentarily create the object so match can be called.
  public VectorUDAFBloomFilterMerge() {
    super();
  }

  public VectorUDAFBloomFilterMerge(VectorAggregationDesc vecAggrDesc) {
    super(vecAggrDesc);
    init();
  }

  public VectorUDAFBloomFilterMerge(VectorAggregationDesc vecAggrDesc, int numThreads) {
    super(vecAggrDesc);
    this.numThreads = numThreads;
    init();
  }

  private void init() {

    GenericUDAFBloomFilterEvaluator udafBloomFilter =
        (GenericUDAFBloomFilterEvaluator) vecAggrDesc.getEvaluator();
    expectedEntries = udafBloomFilter.getExpectedEntries();

    aggBufferSize = -1;
  }

  @Override
  public void finish(AggregationBuffer myagg, boolean aborted) {
    VectorUDAFBloomFilterMerge.Aggregation agg = (VectorUDAFBloomFilterMerge.Aggregation) myagg;
    if (agg.numThreads > 0) {
      LOG.info("bloom filter merge: finishing aggregation, waiting tasks: {}",
          agg.getNumberOfWaitingMergeTasks());
      agg.shutdownAndWaitForMergeTasks(agg, aborted);
    }
  }

  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    if (expectedEntries < 0) {
      throw new IllegalStateException("expectedEntries not initialized");
    }
    return new Aggregation(expectedEntries, numThreads);
  }

  @Override
  public void aggregateInput(AggregationBuffer agg, VectorizedRowBatch batch)
      throws HiveException {

    inputExpression.evaluate(batch);

    ColumnVector inputColumn =  batch.cols[this.inputExpression.getOutputColumnNum()];

    int batchSize = batch.size;

    if (batchSize == 0) {
      return;
    }

    Aggregation myagg = (Aggregation) agg;

    if (inputColumn.isRepeating) {
      if (inputColumn.noNulls || !inputColumn.isNull[0]) {
        processValue(myagg, inputColumn, 0);
      }
      return;
    }

    if (myagg.numThreads != 0) {
      processValues(myagg, inputColumn, batchSize, batch.selectedInUse, batch.selected);
    } else {
      if (!batch.selectedInUse && inputColumn.noNulls) {
        iterateNoSelectionNoNulls(myagg, inputColumn, batchSize);
      } else if (!batch.selectedInUse) {
        iterateNoSelectionHasNulls(myagg, inputColumn, batchSize);
      } else if (inputColumn.noNulls) {
        iterateSelectionNoNulls(myagg, inputColumn, batchSize, batch.selected);
      } else {
        iterateSelectionHasNulls(myagg, inputColumn, batchSize, batch.selected);
      }
    }
  }

  private void processValues(
    Aggregation myagg,
    ColumnVector inputColumn,
    int batchSize, boolean selectedInUse, int[] selected){

    VectorUDAFBloomFilterMerge.Aggregation agg = (VectorUDAFBloomFilterMerge.Aggregation)myagg;

    agg.mergeBloomFilterBytesFromInputColumn((BytesColumnVector) inputColumn, batchSize,
        selectedInUse, selected);
  }

  private void iterateNoSelectionNoNulls(
      Aggregation myagg,
      ColumnVector inputColumn,
      int batchSize) {
    for (int i=0; i< batchSize; ++i) {
      processValue(myagg, inputColumn, i);
    }
  }

  private void iterateNoSelectionHasNulls(
      Aggregation myagg,
      ColumnVector inputColumn,
      int batchSize) {

    for (int i=0; i< batchSize; ++i) {
      if (!inputColumn.isNull[i]) {
        processValue(myagg, inputColumn, i);
      }
    }
  }

  private void iterateSelectionNoNulls(
      Aggregation myagg,
      ColumnVector inputColumn,
      int batchSize,
      int[] selected) {

    for (int j=0; j< batchSize; ++j) {
      int i = selected[j];
      processValue(myagg, inputColumn, i);
    }
  }

  private void iterateSelectionHasNulls(
      Aggregation myagg,
      ColumnVector inputColumn,
      int batchSize,
      int[] selected) {

    for (int j=0; j< batchSize; ++j) {
      int i = selected[j];
      if (!inputColumn.isNull[i]) {
        processValue(myagg, inputColumn, i);
      }
    }
  }

  @Override
  public void aggregateInputSelection(
      VectorAggregationBufferRow[] aggregationBufferSets, int aggregateIndex,
      VectorizedRowBatch batch) throws HiveException {

    int batchSize = batch.size;

    if (batchSize == 0) {
      return;
    }

    inputExpression.evaluate(batch);

    ColumnVector inputColumn = batch.cols[this.inputExpression.getOutputColumnNum()];

    if (inputColumn.noNulls) {
      if (inputColumn.isRepeating) {
        iterateNoNullsRepeatingWithAggregationSelection(
          aggregationBufferSets, aggregateIndex,
          inputColumn, batchSize);
      } else {
        if (batch.selectedInUse) {
          iterateNoNullsSelectionWithAggregationSelection(
            aggregationBufferSets, aggregateIndex,
            inputColumn, batch.selected, batchSize);
        } else {
          iterateNoNullsWithAggregationSelection(
            aggregationBufferSets, aggregateIndex,
            inputColumn, batchSize);
        }
      }
    } else {
      if (inputColumn.isRepeating) {
        if (!inputColumn.isNull[0]) {
          iterateNoNullsRepeatingWithAggregationSelection(
              aggregationBufferSets, aggregateIndex,
              inputColumn, batchSize);
        }
      } else {
        if (batch.selectedInUse) {
          iterateHasNullsSelectionWithAggregationSelection(
            aggregationBufferSets, aggregateIndex,
            inputColumn, batchSize, batch.selected);
        } else {
          iterateHasNullsWithAggregationSelection(
            aggregationBufferSets, aggregateIndex,
            inputColumn, batchSize);
        }
      }
    }
  }

  private void iterateNoNullsRepeatingWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      ColumnVector inputColumn,
      int batchSize) {

    for (int i=0; i < batchSize; ++i) {
      Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          aggregateIndex,
          i);
      processValue(myagg, inputColumn, 0);
    }
  }

  private void iterateNoNullsSelectionWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      ColumnVector inputColumn,
      int[] selection,
      int batchSize) {

    for (int i=0; i < batchSize; ++i) {
      int row = selection[i];
      Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          aggregateIndex,
          i);
      processValue(myagg, inputColumn, row);
    }
  }

  private void iterateNoNullsWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      ColumnVector inputColumn,
      int batchSize) {
    for (int i=0; i < batchSize; ++i) {
      Aggregation myagg = getCurrentAggregationBuffer(
          aggregationBufferSets,
          aggregateIndex,
          i);
      processValue(myagg, inputColumn, i);
    }
  }

  private void iterateHasNullsSelectionWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      ColumnVector inputColumn,
      int batchSize,
      int[] selection) {

    for (int i=0; i < batchSize; ++i) {
      int row = selection[i];
      if (!inputColumn.isNull[row]) {
        Aggregation myagg = getCurrentAggregationBuffer(
            aggregationBufferSets,
            aggregateIndex,
            i);
        processValue(myagg, inputColumn, i);
      }
    }
  }

  private void iterateHasNullsWithAggregationSelection(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      ColumnVector inputColumn,
      int batchSize) {

    for (int i=0; i < batchSize; ++i) {
      if (!inputColumn.isNull[i]) {
        Aggregation myagg = getCurrentAggregationBuffer(
            aggregationBufferSets,
            aggregateIndex,
            i);
        processValue(myagg, inputColumn, i);
      }
    }
  }

  private Aggregation getCurrentAggregationBuffer(
      VectorAggregationBufferRow[] aggregationBufferSets,
      int aggregateIndex,
      int row) {
    VectorAggregationBufferRow mySet = aggregationBufferSets[row];
    Aggregation myagg = (Aggregation) mySet.getAggregationBuffer(aggregateIndex);
    return myagg;
  }

  @Override
  public void reset(AggregationBuffer agg) throws HiveException {
    agg.reset();
  }

  @Override
  public long getAggregationBufferFixedSize() {
    if (aggBufferSize < 0) {
      // Not pretty, but we need a way to get the size
      try {
        Aggregation agg = (Aggregation) getNewAggregationBuffer();
        aggBufferSize = agg.bfBytes.length;
      } catch (Exception e) {
        throw new RuntimeException("Unexpected error while creating AggregationBuffer", e);
      }
    }

    return aggBufferSize;
  }

  void processValue(Aggregation myagg, ColumnVector columnVector, int i) {
    // columnVector entry is byte array representing serialized BloomFilter.
    // BloomFilter.mergeBloomFilterBytes() does a simple byte ORing
    // which should be faster than deserialize/merge.
    BytesColumnVector inputColumn = (BytesColumnVector) columnVector;
    BloomKFilter.mergeBloomFilterBytes(myagg.bfBytes, 0, myagg.bfBytes.length,
        inputColumn.vector[i], inputColumn.start[i], inputColumn.length[i]);
  }


  @Override
  public boolean matches(String name, ColumnVector.Type inputColVectorType,
      ColumnVector.Type outputColVectorType, Mode mode) {

    /*
     * Bloom filter merge input and output are BYTES.
     *
     * Just modes (PARTIAL2, FINAL).
     */
    return
        name.equals(BLOOM_FILTER_FUNCTION) &&
        inputColVectorType == ColumnVector.Type.BYTES &&
        outputColVectorType == ColumnVector.Type.BYTES &&
        (mode == Mode.PARTIAL2 || mode == Mode.FINAL);
  }

  @Override
  public void assignRowColumn(VectorizedRowBatch batch, int batchIndex, int columnNum,
      AggregationBuffer agg) throws HiveException {

    BytesColumnVector outputColVector = (BytesColumnVector) batch.cols[columnNum];
    outputColVector.isNull[batchIndex] = false;

    Aggregation bfAgg = (Aggregation) agg;
    outputColVector.setVal(batchIndex, bfAgg.bfBytes, 0, bfAgg.bfBytes.length);
  }

  /**
   * Let's clone the batch when we're working in parallel, see HIVE-26655.
   */
  public boolean batchNeedsClone() {
    return numThreads > 0;
  }
}
