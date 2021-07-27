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

package org.apache.hadoop.hive.ql.exec.vector.ptf;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.ql.exec.PTFPartition;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.IntervalDayTimeColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.ptf.VectorSpillBlockContainer.VectorSpillBlock;
import org.apache.hadoop.hive.ql.exec.vector.rowbytescontainer.VectorRowBytesContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.udf.ptf.PTFRangeUtil;
import org.apache.hadoop.hive.ql.udf.ptf.Range;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * This class is encapsulates one or more VectorizedRowBatch of a PTF group.
 */
public class VectorPTFGroupBatches extends PTFPartition {
  private final Logger LOG = LoggerFactory.getLogger(getClass());

  private VectorPTFEvaluatorBase[] evaluators;
  private int[] outputProjectionColumnMap;
  private int[] bufferedColumnMap;
  private int[] orderColumnMap;
  private int[] keyWithoutOrderColumnMap;

  private int spillLimitBufferedBatchCount;
  private String spillLocalDirs;

  @VisibleForTesting
  ArrayList<BufferedVectorizedRowBatch> bufferedBatches;
  private int allocatedBufferedBatchCount;
  @VisibleForTesting
  int currentBufferedBatchCount;
  private VectorizedRowBatch overflowBatch;
  /**
   * Used for propagating only a portion of the the current buffered batch to an evaluator.
   */
  private BufferedVectorizedRowBatch partialBatch;

  @VisibleForTesting
  VectorSpillBlockContainer blocks;

  private PartitionResults partitionResults;
  private PartitionMetrics partitionMetrics = new PartitionMetrics();

  private int cachedSize = -1;

  RowPositionInBatch[] positionCache;

  List<Integer> inMemoryStartRowIndex;

  /**
   * PartitionResults collects results for the currently evaluated partition. Before HIVE-24761, PTF
   * vectorization only took care of unbounded windows, which means that all of the evaluators run
   * on the same amount of rows (actually, on all rows), so there were only 1 group result per
   * evaluator. After taking proper window ranges into account, evaluators generates the
   * corresponding result for each row in the partition, so there should be a container, that can
   * hold these values until the batch is forwarded, and the values should be filled into the
   * overflowBatch into the correct position: every row's output column should contain the evaluator
   * result for the corresponding row (=> which were calculated on the range that has been
   * calculated for that particular row).
   */
  class PartitionResults {
    /**
     * Holds the results. A static sized array is suitable for this purpose, as we know the number
     * of evaluators and rows at the time we create this object.
     */
    private final Object[][] results;
    int rows = 0;
    /**
     * Used for keeping track of iterations on results.
     */
    int currentRow = 0;

    public PartitionResults(int rows) {
      this.rows = rows;
      results = new Object[evaluators.length][];

      for (int e = 0; e < evaluators.length; e++) {
        results[e] = new Object[rows];
      }
    }

    public void addResult(int evaluatorIndex, int rowNum, Object evaluatorResult) {
      results[evaluatorIndex][rowNum] = evaluatorResult;
    }

    public boolean nextRow() {
      if (currentRow + 1 > rows) {
        return false;
      }
      currentRow += 1;
      return true;
    }

    @Override
    public String toString() {
      return String.format("PartitionResults (evaluators: %d, rows: %d): %s", results.length,
          results[0].length, Arrays.deepToString(results));
    }

    public Object get(int evaluatorIndex, int row) {
      return results[evaluatorIndex][row];
    }

    public Object getResultForCurrentRow(int evaluatorIndex) {
      return results[evaluatorIndex][currentRow];
    }
  }

  /**
   * PartitionMetrics encapsulates some insight about the data within a PTF partition. It's quite
   * cheap to store and maintain, but contains important information about data characteristics. The
   * result is printed after every partition in order to make user able to finetune vectorized PTF
   * for better performance (e.g. more memory stored in memory).
   */
  class PartitionMetrics {
    /**
     * This contains the total number of batches recieved for buffering in non-streaming mode.
     */
    public int totalNumberOfBatches;
    /**
     * This is incremented every time when an evaluator is run for a specific range without
     * optimization.
     */
    public int evaluationSimple;
    /**
     * This is incremented every time when an evaluator is run for a specific range with
     * optimization.
     */
    public int evaluationOptimized;
    /**
     * Total amount of batch spill read. Be aware that this can be even more than the number of
     * batches, as this is incremented every time when a batch is read from spill.
     */
    public int batchesReadFromSpill;
    /**
     * Total amount of batch spill writes. Be aware that this can be even more than the number of
     * batches, as this is incremented every time when a batch is spilled.
     */
    public int batchesWrittenToSpill;
    /**
     * Time spent with processing the partition. This is mainly the time frame of finishPartition
     * method.
     */
    private long durationMs;

    public void clear() {
      totalNumberOfBatches = 0;
      evaluationSimple = 0;
      evaluationOptimized = 0;
      batchesReadFromSpill = 0;
      batchesWrittenToSpill = 0;
      durationMs = 0;
    }

    @Override
    public String toString() {
      return String.format(
          "PartitionMetrics: time spent(ms): %d, number of rows: %d, number of evaluators: %d, number of blocks: %d, "
              + "batches per block: %d, average number of rows per batch: %d, totalNumberOfBatches: %d, "
              + "evaluationSimple: %d, evaluationOptimized: %d, batchesReadFromSpill: %d, batchesWrittenToSpill: %d",
          durationMs, size(), evaluators.length, blocks.size(), spillLimitBufferedBatchCount,
          size() / totalNumberOfBatches, totalNumberOfBatches, evaluationSimple,
          evaluationOptimized, batchesReadFromSpill, batchesWrittenToSpill);
    }

    public void setDuration(long durationMs) {
      this.durationMs = durationMs;
    }
  }

  /**
   * RowPositionInBatch encapsulates some positioning data for a given row index.
   */
  class RowPositionInBatch {

    private VectorSpillBlock block;
    private int batchIndex;
    private int rowIndexInBatch;

    public RowPositionInBatch(VectorSpillBlock block, int batchIndex, int rowIndexInBatch) {
      this.block = block;
      this.batchIndex = batchIndex;
      this.rowIndexInBatch = rowIndexInBatch;
    }

    @Override
    public String toString() {
      return String.format(
          "RowPositionInBatch: batchIndex: %d, rowIndexInBatch: %d, blockIndex: %d, block: %s",
          batchIndex, rowIndexInBatch, block == null ? 0 : block.blockIndex, block);
    }
  }

  class BlockIterator {
    private Range range;
    private RowPositionInBatch startRow;
    private RowPositionInBatch endRow;

    /**
     * Creates a block iterator for a specific range.
     * 
     * @throws HiveException
     */
    public BlockIterator(Range range) throws HiveException {
      this.range = range;

      this.endRow = getPosition(range.getEnd());
      this.startRow = getPosition(range.getStart());
      if (startRow.block != null) {
        jumpToBlock(startRow.block.blockIndex);
      }
    }

    boolean hasNext() {
      return endRow.block != null && blocks.getStartBatchIndexInMemory() < endRow.block.startBatchIndex;
    }

    Object run(VectorPTFEvaluatorBase evaluator) throws HiveException {
      runEvaluator(evaluator, range);

      while (hasNext()) {
        nextBlock();
        runEvaluator(evaluator, range);
      }

      return getEvaluatorResult(evaluator);
    }

    Object getEvaluatorResult(VectorPTFEvaluatorBase evaluator) {
      return evaluator.isGroupResultNull() ? null : evaluator.getGroupResult();
    }
  }

  public VectorPTFGroupBatches(Configuration hconf, int vectorizedPTFMaxMemoryBufferingBatchCount) {
    allocatedBufferedBatchCount = 0;
    currentBufferedBatchCount = 0;

    spillLocalDirs = HiveUtils.getLocalDirList(hconf);

    // Cannot be 0.
    spillLimitBufferedBatchCount = Math.max(1, vectorizedPTFMaxMemoryBufferingBatchCount);
    initBoundaryCache(hconf);
    initValueCache(hconf);
  }

  public void init(VectorPTFEvaluatorBase[] evaluators, int[] outputProjectionColumnMap,
      int[] bufferedColumnMap, TypeInfo[] bufferedTypeInfos, int[] orderColumnMap,
      int[] keyWithoutOrderColumnMap, VectorizedRowBatch overflowBatch) {
    this.evaluators = evaluators;
    this.outputProjectionColumnMap = outputProjectionColumnMap;
    this.bufferedColumnMap = bufferedColumnMap;
    this.orderColumnMap = orderColumnMap;
    this.keyWithoutOrderColumnMap = keyWithoutOrderColumnMap;

    this.overflowBatch = overflowBatch;
    bufferedBatches = new ArrayList<BufferedVectorizedRowBatch>(0);

    if (valueCache != null){
      valueCache.init(evaluators.length);
    }

    this.blocks = new VectorSpillBlockContainer(spillLimitBufferedBatchCount, spillLocalDirs,
        bufferedColumnMap, bufferedTypeInfos);
  }

  /**
   * Finds and element by a partition-level row index. This search can use per-batch indices.
   * Overrides PTFPartition.getAt() but here in the vectorized codepath (as opposed to
   * non-vectorized case), getAt only returns the ordering columns. This is because most of the time
   * getAt is used heavily for range scanning where only the ordering columns are needed. As
   * VectorizedRowBatch maintains columns, extracting a full row needs more computational effort
   * than just 1-2 ordering columns. Accessing a random element is possible via getValue(row, col).
   */
  @Override
  public Object[] getAt(int i) throws HiveException {
    RowPositionInBatch rp = getPosition(i);
    return getOrderingValuesFromRow(bufferedBatches.get(rp.batchIndex), rp.rowIndexInBatch);
  }

  /**
   * Returns the value at row/col. Row index is absolute inside a PTF partition, col should point to
   * the remapped column index according to bufferedColumnMap. The input expressions of the
   * evaluators are already aware of this remapped column as they are changed in
   * VectorPTFOperator.initBufferedColumns.
   * @param row
   * @param col
   * @return
   * @throws HiveException
   */
  public Object getValue(int row, int col) throws HiveException {
    RowPositionInBatch rp = getPosition(row);
    return getValueFromBatch(bufferedBatches.get(rp.batchIndex), col, rp.rowIndexInBatch);
  }

  public Object getValueAndEvaluateInputExpression(VectorPTFEvaluatorBase evaluator, int row,
      int col) throws HiveException {
    RowPositionInBatch rp = getPosition(row);
    BufferedVectorizedRowBatch batch = bufferedBatches.get(rp.batchIndex);
    if (!batch.isInputExpressionEvaluated) {
      evaluator.evaluateInputExpr(batch);
    }
    return getValueFromBatch(batch, col, rp.rowIndexInBatch);
  }

  public RowPositionInBatch getPosition(int i) throws HiveException {
    if (positionCache[i] != null) {
      RowPositionInBatch p = positionCache[i];
      // the caller of getPosition(i) assumes that VectorPTFGroupBatches stands on the actual row
      // (it's loaded into memory), so let's fulfill that requirement
      if (canJump()){
        jumpToBlock(p.block.blockIndex);
      }
      return positionCache[i];
    }

    // there is no blocks at all, all batches are in memory
    if (blocks.isEmpty()){
      return toPositionCache(i, getRowPositionFromBufferedBatches(i));
    }
    VectorSpillBlock lastBlock = blocks.getLast();

    /* this could happen when caller is interested in a row beyond the range's end, let's return the last row
     * E.g if the last block is:
     * [VectorSpillBlock: startBatchIndex: 6, startRowIndex: [10, 11],
     *  spilled: true, spillBatchCount: 2, spillRowCount: 2]]
     * This will return: RowPositionInBatch: row: 12, blockIndex: 3, batchIndex: 1, rowIndexInBatch: 0
     */
    if (i > lastBlock.getLastRowIndex()) {
      return toPositionCache(i,
          new RowPositionInBatch(lastBlock, /* batchIndex */ lastBlock.startRowIndex.length - 1,
              /* rowIndexInBatch */ (int) (lastBlock.startRowIndex[0] + lastBlock.spillRowCount - 1
                  - lastBlock.startRowIndex[lastBlock.spillBatchCount - 1])));
    }

    int blockIndex = 0;
    while (i >= blocks.get(blockIndex).startRowIndex[0] + blocks.get(blockIndex).spillRowCount) {
      blockIndex += 1;
    }

    VectorSpillBlock block = blocks.getCurrentBlock();
    if (blockIndex != block.blockIndex) {
      spillAndResetCurrentBufferedBatches(); // spill if needed
      block = jumpToBlock(blockIndex);
    }

    int batchIndex = findInMemoryBatchIndex(i);
    return toPositionCache(i,
        new RowPositionInBatch(block, batchIndex, i - block.startRowIndex[batchIndex]));
  }

  /**
   * Overrides PTFPartition.size(), this is supposed to return the amount of records in the partition.
   */
  @Override
  public int size() {
    if (cachedSize > -1){
      return cachedSize;
    }
    int count = 0;
    if (blocks.isEmpty()) {
      // Easy case, only buffered batches are present.
      count = countBufferedRows();
    } else {
      /*
       * We need to be aware that any of the blocks can be loaded into memory at the time of
       * counting. If we count the records of a spilled block, we should not count the in-memory
       * records again.
       */
      count += blocks.getRowSize(countBufferedRows());
    }
    cachedSize = count;
    return count;
  }

  public void finishPartition() throws HiveException {
    long startTime = System.currentTimeMillis();
    preFinishPartition();
    int evaluatorIndex = 0;
    /*
     * This could become a very heavy loop, which runs for all rows in the partition
     * and calculates the aggregated value on the defined range relative to the actual row,
     * so if:
     * rows = 10000
     * evaluators = 5
     * avg range size: 100
     *
     * The following cost and optimizations should be considered:
     * 1. getRange calculation with n^2 complexity as described on HIVE-21217 (BoundaryCache)
     * 2. runEvaluator method will calculate ~ 10000 * 5 * 100 = 5000000 values (PTFValueCache)
     */
    for (int rowNum = 0; rowNum < size(); rowNum++) {
      evaluatorIndex = 0;

      for (VectorPTFEvaluatorBase evaluator : evaluators) {
        // calculate and store non-streaming results here
        // streaming evaluators are evaluated on-the-fly in fillGroupResults
        if (!evaluator.streamsResult()) {
          Range range = PTFRangeUtil.getRange(evaluator.windowFrameDef, rowNum, this,
              evaluator.getNullsLast());
          runEvaluatorForRow(evaluatorIndex, evaluator, rowNum, range);
        }
        evaluatorIndex += 1;
      }
    }
    partitionMetrics.setDuration(System.currentTimeMillis() - startTime);
    LOG.info("Finished evaluation of partition, metrics: {}", partitionMetrics);
    if (valueCache != null && valueCache.getStatistics() != null) {
      LOG.info(valueCache.getStatistics());
    }
  }

  /**
   * This should be called, when all of the batches were processed for a group (all of the
   * evaluators have been evaluated for this group). The data is not needed anymore.
   */
  public void cleanupPartition() {
    blocks.clear();
    for (VectorizedRowBatch batch : bufferedBatches) {
      batch.reset();
    }
    currentBufferedBatchCount = 0;

    resetEvaluators();

    if (boundaryCache != null) {
      boundaryCache.clear();
    }
    if (valueCache != null) {
      valueCache.clear();
    }
    cachedSize = -1;
    partitionMetrics.clear();
  }


  /**
   * Buffers a batch at the end of bufferedBatches, and if it's full, spills a block of batches to
   * the disk.
   *
   * @param batch
   * @throws HiveException
   */
  public void bufferGroupBatch(VectorizedRowBatch batch, boolean isLastGroupBatch) throws HiveException {
    partitionMetrics.totalNumberOfBatches += 1;
    if (currentBufferedBatchCount >= spillLimitBufferedBatchCount) {
      spillAndResetCurrentBufferedBatches();
      // a new block spilled here, the latest block.startBatchIndex + spillLimitBufferedBatchCount
      // points to the first in-memory batch's index
      blocks.updateStartBatchIndexInMemory();
    }

    if (allocatedBufferedBatchCount <= currentBufferedBatchCount) {
      BufferedVectorizedRowBatch newBatch = newBufferedBatch(batch);
      bufferedBatches.add(newBatch);
      allocatedBufferedBatchCount++;
    }

    BufferedVectorizedRowBatch bufferedBatch = bufferedBatches.get(currentBufferedBatchCount++);
    copyBufferedColumns(batch, bufferedBatch);
    bufferedBatch.isLastGroupBatch = isLastGroupBatch;
    cachedSize = -1; // clear cached size as we added new batches
  }

  public void resetEvaluators() {
    for (VectorPTFEvaluatorBase evaluator : evaluators) {
      evaluator.resetEvaluator();
      evaluator.onPartitionEnd();
    }
  }

  public void fillGroupResultsAndForward(VectorPTFOperator vecPTFOperator, Object[] partitionKey)
      throws HiveException {
    /*
     * Original logic assumes that the forwarding order is:
     * 1) forwardSpilledBatches,
     * 2) forwardBufferedBatch, so in order to preserve ordering of forwarded batches we point
     * in-memory batches to the last block.
     */
    jumpToLastBlock();
    BufferedVectorizedRowBatch lastBatch = bufferedBatches.get(currentBufferedBatchCount - 1);
    forwardSpilledBatches(vecPTFOperator, partitionKey);

    if (currentBufferedBatchCount > 0) {
      /*
       * currentBufferedBatchCount - 1: don't forward last batch here, it is forwarded in
       * forwardLastBatch
       */
      for (int i = 0; i < currentBufferedBatchCount - 1; i++) {
        forwardBufferedBatch(vecPTFOperator, bufferedBatches.get(i), partitionKey);
      }
      currentBufferedBatchCount = 0;
    }

    forwardLastBatch(vecPTFOperator, lastBatch, partitionKey);
  }

  /**
   * Finds the batch which contains a given row with binary search. The row reflects a global row
   * number within the partition, so as the values in inMemoryStartRowIndex. This method assumes
   * that the caller loaded the needed block into memory, and refresh the index via
   * refreshInMemoryStartRowIndex.
   *
   * @param row
   * @return batchIndex
   */
  int findInMemoryBatchIndex(int row) {
    int batchIndex = Collections.binarySearch(inMemoryStartRowIndex, row);
    batchIndex = batchIndex < 0 ? (batchIndex + 1) * -1 - 1 : batchIndex;
    return batchIndex;
  }

  private RowPositionInBatch toPositionCache(int row, RowPositionInBatch rowPositionInBatch) {
    positionCache[row] = rowPositionInBatch;
    return rowPositionInBatch;
  }

  private RowPositionInBatch getRowPositionFromBufferedBatches(int row) throws HiveException {
    int batchIndex = findInMemoryBatchIndex(row);
    return new RowPositionInBatch(null, /* batchIndex */batchIndex,
        row - inMemoryStartRowIndex.get(batchIndex));
  }

  /**
   * Returns the values of ordering columns from a given batch and given row in an Object[].
   * @param vectorizedRowBatch
   * @param rowIndexInBatch
   * @return
   */
  private Object[] getOrderingValuesFromRow(VectorizedRowBatch vectorizedRowBatch, int rowIndexInBatch) {
    Object[] orderingValues = new Object[orderColumnMap.length];
    // ordering columns are at the beginning in the buffered batch
    for (int i = 0; i < orderColumnMap.length; i++) {
      orderingValues[i] = getValueFromBatch(vectorizedRowBatch, i, rowIndexInBatch);
    }
    return orderingValues;
  }

  private Object getValueFromBatch(VectorizedRowBatch vectorizedRowBatch, int column,
      int rowIndexInBatch) {
    ColumnVector columnVector = vectorizedRowBatch.cols[column];

    if (columnVector.isNull[rowIndexInBatch]
        || (columnVector.isRepeating && columnVector.isNull[0])) {
      return null;
    }
    if (columnVector.isRepeating) {
      rowIndexInBatch = 0;
    }

    switch (columnVector.type) {
    case LONG:
      return ((LongColumnVector) columnVector).vector[rowIndexInBatch];
    case DOUBLE:
      return ((DoubleColumnVector) columnVector).vector[rowIndexInBatch];
    case BYTES:
      // TODO: optimize on bytes, or remove string based windows at all, as it's not supported by
      // SQL standard
      BytesColumnVector inV = (BytesColumnVector) columnVector;
      return new String(inV.vector[rowIndexInBatch], inV.start[rowIndexInBatch],
          inV.length[rowIndexInBatch]);
    case DECIMAL:
      return ((DecimalColumnVector) columnVector).vector[rowIndexInBatch];
    case TIMESTAMP:
      return ((TimestampColumnVector) columnVector).getDouble(rowIndexInBatch);
    case INTERVAL_DAY_TIME:
      return ((IntervalDayTimeColumnVector) columnVector).getTotalSeconds(rowIndexInBatch);
    case STRUCT:
    case LIST:
    case MAP:
    case UNION:
      // No complex type support for now.
    default:
      throw new RuntimeException(
          "Unexpected column vector type while getting value from ordering col: "
              + columnVector.type);
    }
  }

  private int countBufferedRows() {
    int count = 0;
    for (int i = 0; i < currentBufferedBatchCount; i++) {
      count += bufferedBatches.get(i).size;
    }
    return count;
  }

  public void evaluateStreamingGroupBatch(VectorizedRowBatch batch, boolean isLastGroupBatch)
      throws HiveException {

    // Streaming evaluators fill in their results during the evaluate call.
    for (VectorPTFEvaluatorBase evaluator : evaluators) {
      evaluator.evaluateGroupBatch(batch);
      if (isLastGroupBatch) {
        evaluator.doLastBatchWork();
      }
    }
  }

  private void fillGroupResults(VectorizedRowBatch batch, boolean isLastGroupBatch) throws HiveException {
    /*
     * Do careful maintenance of the outputColVector.noNulls flag.
     */
    int evaluatorIndex = -1;
    int startRowIndex = partitionResults.currentRow;

    for (VectorPTFEvaluatorBase evaluator : evaluators) {
      evaluatorIndex += 1;
      if (evaluator.streamsResult()) {
        evaluator.evaluateGroupBatch(batch);
        if (isLastGroupBatch){
          evaluator.doLastBatchWork();
        }
        continue;
      }
      final int outputColumnNum = evaluator.getOutputColumnNum();
      final ColumnVector outputColVector = batch.cols[outputColumnNum];
      partitionResults.currentRow = startRowIndex; //reset row counter before next evaluator
      for (int i = 0; i < batch.size; i++) {
        Object result = partitionResults.getResultForCurrentRow(evaluatorIndex);
        if (result == null) {
          outputColVector.noNulls = false;
          outputColVector.isNull[i] = true;
        } else {
          try{
            switch (evaluator.getResultColumnVectorType()) {
            case LONG:
              ((LongColumnVector) outputColVector).vector[i] =
                  (long) result;
              break;
            case DOUBLE:
              ((DoubleColumnVector) outputColVector).vector[i] =
                  (double) result;
              break;
            case DECIMAL:
              ((DecimalColumnVector) outputColVector).set(i,
                  (HiveDecimalWritable) result);
              break;
            default:
              throw new RuntimeException(
                  "Unexpected column vector type " + evaluator.getResultColumnVectorType());
            }
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("error while setting value from evaluator: %s", evaluator.getClass()),
                e);
          }
        }
        partitionResults.nextRow();
      }
    }
  }

  private void forwardBufferedBatch(VectorPTFOperator vecPTFOperator,
      BufferedVectorizedRowBatch bufferedBatch, Object[] partitionKey) throws HiveException {
    prepareBufferedBatchForForwarding(bufferedBatch, partitionKey);
    vecPTFOperator.forwardBatch(overflowBatch);
  }

  private void prepareBufferedBatchForForwarding(BufferedVectorizedRowBatch bufferedBatch, Object[] partitionKey) throws HiveException {
    overflowBatch.reset();
    copyPartitionColumnToOverflow(partitionKey);

    final int size = bufferedBatch.size;
    final int bufferedColumnCount = bufferedColumnMap.length;
    for (int i = 0; i < bufferedColumnCount; i++) {
      // Copy ColumnVectors to overflowBatch. We remember buffered columns compactly in the
      // buffered VRBs without other columns or scratch columns.
      VectorizedBatchUtil.copyNonSelectedColumnVector(bufferedBatch, i, overflowBatch,
          bufferedColumnMap[i], size);
    }
    overflowBatch.size = size;
    fillGroupResults(overflowBatch, bufferedBatch.isLastGroupBatch);
  }

  private void forwardSpilledBatches(VectorPTFOperator vecPTFOperator, Object[] partitionKey)
      throws HiveException {

    overflowBatch.reset();
    copyPartitionColumnToOverflow(partitionKey);

    try {
      for (int b = 0; b < blocks.size(); b++) {
        VectorSpillBlock block = blocks.get(b);
        if (blocks.isBlockInMemory(block)) {
          /*
           * Buffered blocks will be forwarded in forwardBufferedBatch. We can break instead of
           * continue, as we already called jumpToLastBlock() before starting to forward batches to
           * next operator, so the in-memory block should be the last block.
           */
          break;
        }
        block.setDoFinalRead(true);

        VectorRowBytesContainer rowBytesContainer = block.getSpillRowBytesContainer();
        rowBytesContainer.prepareForReading();
        int batchIndex = 0;
        long spillRowsRead = 0;

        while (rowBytesContainer.readNext()) {
          if (batchIndex < block.startRowIndex.length - 1
              && overflowBatch.size == block.startRowIndex[batchIndex + 1]
                  - block.startRowIndex[batchIndex]) {
            fillGroupResults(overflowBatch, block.isLastGroupBatch[batchIndex]);
            vecPTFOperator.forwardBatch(overflowBatch);

            overflowBatch.reset();
            copyPartitionColumnToOverflow(partitionKey);

            batchIndex += 1;
          }

          block.readSingleRowFromBytesContainer(overflowBatch);
          overflowBatch.size++;
          spillRowsRead++;
        }

        if (overflowBatch.size > 0){
          fillGroupResults(overflowBatch, block.isLastGroupBatch[batchIndex]);
          vecPTFOperator.forwardBatch(overflowBatch);

          overflowBatch.reset();
          copyPartitionColumnToOverflow(partitionKey);
        }

        Preconditions.checkState(spillRowsRead == block.spillRowCount,
            "error while checking forwardSpilledBatches state for spillRowsRead == block.spillRowCount, %s != %s (block %s/%s)",
            spillRowsRead, block.spillRowCount, b, blocks.size() - 1);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  private void copyPartitionColumnToOverflow(Object[] partitionKey) {
    // Set partition column in overflowBatch.
    // We can set by ref since our last batch is held by us.
    final int keyInputColumnCount = keyWithoutOrderColumnMap.length;
    for (int i = 0; i < keyInputColumnCount; i++) {
      final int keyColumnNum = keyWithoutOrderColumnMap[i];
      Preconditions.checkState(overflowBatch.cols[keyColumnNum] != null);
      setRepeatingColumn(partitionKey[i], overflowBatch, keyColumnNum);
    }
  }

  private void setRepeatingColumn(Object partitionKey, VectorizedRowBatch targetBatch,
      int targetColumnNum) {
    ColumnVector targetColVector = targetBatch.cols[targetColumnNum];

    targetColVector.isRepeating = true;

    if (partitionKey == null) {
      targetColVector.noNulls = false;
      targetColVector.isNull[0] = true;
      return;
    }

    switch (targetColVector.type) {
    case LONG:
      ((LongColumnVector) targetColVector).vector[0] = (long) partitionKey;
      break;
    case DOUBLE:
      ((DoubleColumnVector) targetColVector).vector[0] = (double) partitionKey;
      break;
    case BYTES:
      ((BytesColumnVector) targetColVector).setRef(0, (byte[]) partitionKey, 0,
          ((byte[]) partitionKey).length);
      break;
    case DECIMAL:
      ((DecimalColumnVector) targetColVector).set(0, (HiveDecimalWritable) partitionKey);
      break;
    case TIMESTAMP:
      ((TimestampColumnVector) targetColVector).set(0, (Timestamp) partitionKey);
      break;
    case INTERVAL_DAY_TIME:
      ((IntervalDayTimeColumnVector) targetColVector).set(0, (HiveIntervalDayTime) partitionKey);
      break;
    case STRUCT:
    case LIST:
    case MAP:
    case UNION:
      // No complex type support for now.
    default:
      throw new RuntimeException("Unexpected column vector type " + targetColVector.type);
    }
  }

  @VisibleForTesting
  void preFinishPartition() throws HiveException {
    int rows = size();

    positionCache = new RowPositionInBatch[rows + 1];
    partitionResults = new PartitionResults(rows);
    jumpToFirstBlock();
  }

  private void refreshInMemoryStartRowIndex(VectorSpillBlock block) {
    inMemoryStartRowIndex = new ArrayList<>();

    if (block == null || !block.isFullySpilled()) {
      inMemoryStartRowIndex.add(block == null ? 0 : block.startRowIndex[0]);
      for (int i = 1; i < currentBufferedBatchCount + 1; i++) {
        inMemoryStartRowIndex
            .add(inMemoryStartRowIndex.get(i - 1) + bufferedBatches.get(i - 1).size);
      }
    } else {
      for (int i = 0; i < block.startRowIndex.length; i++) {
        inMemoryStartRowIndex.add(block.startRowIndex[i]);
      }
      inMemoryStartRowIndex =
          Arrays.stream(block.startRowIndex).boxed().collect(Collectors.toList());
      inMemoryStartRowIndex.add((int) (block.startRowIndex[0] + block.spillRowCount));
    }
  }

  private void runEvaluatorForRow(int evaluatorIndex, VectorPTFEvaluatorBase evaluator,
      int rowNum, Range range) throws HiveException {
    if (valueCache != null && evaluator.isCacheableForRange()) {
      Object cacheValue = valueCache.get(evaluatorIndex, range);
      if (cacheValue != null) {
        partitionResults.addResult(evaluatorIndex, rowNum, cacheValue);
        return;
      }
    }

    Object result = null;
    if (evaluator.canRunOptimizedCalculation(rowNum, range)) {
      result = copyResultIfNeeded(evaluator, evaluator.runOnRange(rowNum, range, this));
      partitionMetrics.evaluationOptimized += 1;
    } else { // fallback to batch-by-batch manner over the whole range
      result = runEvaluatorOnRange(evaluator, range, false);
      partitionMetrics.evaluationSimple += 1;
    }
    evaluator.onResultCalculated(result, range);
    evaluator.resetEvaluator();
    partitionResults.addResult(evaluatorIndex, rowNum, result);

    if (valueCache != null && evaluator.isCacheableForRange()) {
      valueCache.put(evaluatorIndex, range, result);
    }
  }

  public Object runEvaluatorOnRange(VectorPTFEvaluatorBase evaluator, Range range)
      throws HiveException {
    return runEvaluatorOnRange(evaluator, range, true);
  }

  public Object runEvaluatorOnRange(VectorPTFEvaluatorBase evaluator, Range range,
      boolean resetAfterCalculation) throws HiveException {
    BlockIterator iterator = new BlockIterator(range);
    Object result = iterator.run(evaluator);
    result = copyResultIfNeeded(evaluator, result);
    if (resetAfterCalculation){
      evaluator.resetEvaluator();
    }
    return result;
  }

  private Object copyResultIfNeeded(VectorPTFEvaluatorBase evaluator, Object result) {
    if (evaluator.getResultColumnVectorType() == Type.DECIMAL) {
      /*
       * As HiveDecimalWritable is mutable, we need to copy the value before
       * resetting the evaluator, otherwise we'll lose the value. It's easier to take care of it
       * here instead in every single evaluator.
       */
      result = new HiveDecimalWritable((HiveDecimalWritable) result);
    }
    return result;
  }

  /**
   * Runs the evaluator on a specific range. The provided Ref<Object> is needed because some
   * evaluators (RANK, DENSE_RANK) are supposed to provide the result before handling the last batch
   * in a group (doLastBatchWork). For this purposes, a simple return value would not be sufficient.
   * 
   * @param evaluator
   * @param range
   * @param result
   * @throws HiveException
   */
  private void runEvaluator(VectorPTFEvaluatorBase evaluator, Range range)
      throws HiveException {
    int batchStartIndex = findInMemoryBatchIndex(range.getStart());
    int batchEndIndex = findInMemoryBatchIndex(range.getEnd());

    for (int i = Math.max(0, batchStartIndex); i < Math.min(batchEndIndex + 1,
        bufferedBatches.size()); i++) {
      BufferedVectorizedRowBatch bufferedBatch = bufferedBatches.get(i);
      /*
       * Let's say we have a range: 12-15 and buffered batches like: [0, 1, 2, 3, 4, 5, 6] [7, 8, 9,
       * 10, 11, 12, 13] [14, 15, 16, 17, 18, 19, 20] [21, 22, 23, 24, 25, 26, 27]. We need to
       * create a truncated version of the batches in order to make the evaluator iterate on values
       * [12, 13] and then [14] (range end is exclusive). This way, evaluators will have no idea
       * that they work on a specific range, so the evaluator.evaluateGroupBatch(batch) call works
       * without any changes. Here is a tradeoff, because we might take care of truncated batches at
       * the start/end of ranges (minor performance change), but evaluator instances will remain as
       * simple as they are.
       */
      int currentBatchStartRow = inMemoryStartRowIndex.get(i);
      int currentBatchEndRow = inMemoryStartRowIndex.get(i) + bufferedBatch.size - 1;

      // e.g. in case of the last batch: 14 < 21
      if (range.getEnd() - 1 < currentBatchStartRow) {
        // break the loop, we've already passed the important batches
        break;
      }
      // e.g. in case of the first batch: 12 > 6
      if (range.getStart() > currentBatchEndRow) {
        // loop forward, we haven't reached the batch containing the first row yet
        continue;
      }
      // first important batch: 12 > 7 || 14 < 13
      // second important batch: 12 > 14 || 14 < 20
      if (range.getStart() > currentBatchStartRow || range.getEnd() - 1 < currentBatchEndRow) {
        /*
         * Create truncated batches:
         *
         * 1.
         * [12, 13] -> indices in current batch: 5 --> 6 ([7, 8, 9, 10, 11, 12, 13])
         * max(range.getStart(), currentBatchStartRow) - currentBatchStartRow
         * --> min(range.getEnd() - 1, currentBatchEndRow) - currentBatchStartRow
         * == max(12, 7) - 7 --> min(14, 13) - 7 == 5 --> 6
         *
         * 2.
         * [14] -> indices in current batch: 0 --> 0
         * max(range.getStart(), currentBatchStartRow) - currentBatchStartRow
         * --> min(range.getEnd() - 1, currentBatchEndRow) - currentBatchStartRow
         * == max(12, 14) - 14 --> min(14, 20) - 14 == 0 --> 0
         *
         * The value of currentBatchStartRow should be subtracted from every index as both
         * currentBatchStartRow/currentBatchStartRow and range values are "global" across the
         * blocks, so across the whole partition. After -currentBatchStartRow, the result index is a
         * proper index within the batch.
         */
        evaluator.evaluateGroupBatch(createTruncatedBufferedBatch(bufferedBatch,
            Math.max(range.getStart(), currentBatchStartRow) - currentBatchStartRow,
            Math.min(range.getEnd() - 1, currentBatchEndRow) - currentBatchStartRow));
      } else {
        evaluator.evaluateGroupBatch(bufferedBatch);
      }
    }
  }

  private VectorizedRowBatch createTruncatedBufferedBatch(BufferedVectorizedRowBatch bufferedBatch,
      int startIndexWithinBatch, int endIndexWithinBatch) {
    partialBatch.reset();

    // Copy critical columns.
    final int size = endIndexWithinBatch - startIndexWithinBatch + 1;
    for (int i = 0; i < bufferedColumnMap.length; i++) {
      VectorizedBatchUtil.copyNonSelectedColumnVector(bufferedBatch, i,
          partialBatch, i, size, startIndexWithinBatch);
    }

    partialBatch.size = size;
    partialBatch.isLastGroupBatch = bufferedBatch.isLastGroupBatch;
    partialBatch.isInputExpressionEvaluated = bufferedBatch.isInputExpressionEvaluated;
    return partialBatch;
  }

  private void forwardLastBatch(VectorPTFOperator vecPTFOperator,
      BufferedVectorizedRowBatch lastBatch, Object[] partitionKey) throws HiveException {
    prepareBufferedBatchForForwarding(lastBatch, partitionKey);

    // Project with the output of our operator.
    overflowBatch.projectionSize = outputProjectionColumnMap.length;
    overflowBatch.projectedColumns = outputProjectionColumnMap;

    vecPTFOperator.forwardBatch(overflowBatch);
  }

  private BufferedVectorizedRowBatch newBufferedBatch(VectorizedRowBatch batch) throws HiveException {
    final int bufferedColumnCount = bufferedColumnMap.length;
    BufferedVectorizedRowBatch newBatch = new BufferedVectorizedRowBatch(bufferedColumnCount);
    for (int i = 0; i < bufferedColumnCount; i++) {
      newBatch.cols[i] =
          VectorizedBatchUtil.makeLikeColumnVector(batch.cols[bufferedColumnMap[i]]);
      newBatch.cols[i].init();
    }

    /*
     * At this point, we know the structure of the column batch that we'll use later, so it's time
     * to initialize partialBatch.
     */
    if (partialBatch == null){
      partialBatch = new BufferedVectorizedRowBatch(bufferedColumnMap.length);
      for (int i = 0; i < bufferedColumnCount; i++) {
        partialBatch.cols[i] =
            VectorizedBatchUtil.makeLikeColumnVector(batch.cols[bufferedColumnMap[i]]);
        partialBatch.cols[i].init();
      }
    }
    return newBatch;
  }

  /**
   * This implements the backward movement on batches, the least possible step is a VectorSpillBlock
   * which encapsulates spillLimitBufferedBatchCount number of batches. In the current use-case of
   * VectorPTFGroupBatches, previousBlock() is not used, because random access hits jumpToBlock
   * codepath directly, but this convenience method is ready to use and tested.
   */
  @VisibleForTesting
  boolean previousBlock() throws HiveException {
    if (blocks.isStandingOnFirst()) {
      return false;
    }

    spillAndResetCurrentBufferedBatches();
    blocks.setStartBatchIndexInMemory(blocks.getStartBatchIndexInMemory() - spillLimitBufferedBatchCount);
    VectorSpillBlock block = blocks.getCurrentBlock();
    readBlock(block);

    return true;
  }

  /**
   * This implements the forward movement on batches, the least possible step is a VectorSpillBlock
   * which encapsulates spillLimitBufferedBatchCount number of batches.
   */
  @VisibleForTesting
  boolean nextBlock() throws HiveException {
    VectorSpillBlock block = blocks.getCurrentBlock();

    int nextBlockIndex  = block.blockIndex + 1;
    if (nextBlockIndex > blocks.size() - 1) {
      LOG.info("Cannot move to next block with index {}", nextBlockIndex);
      return false;
    }

    if (!block.didSpillToDisk){
      // this is not expected, as in the beginning we filled the blocks sequentially by
      // bufferGroupBatch, and moved on only after spilling the current batch
      throw new HiveException(
          "Current block (index: " + block.blockIndex + ", " + block
              + ") is not spilled while trying to move forward. Number of current blocks: "
              + blocks.size());
    }

    VectorSpillBlock nextBlock = blocks.get(nextBlockIndex);
    blocks.setStartBatchIndexInMemory(nextBlock);
    readBlock(nextBlock);

    return true;
  }

  @VisibleForTesting
  void jumpToFirstBlock() throws HiveException {
    if (!canJump()){
      // the index might be uninitialized at this point, but caller expects it to be refreshed because
      // of the jump
      refreshInMemoryStartRowIndex(null);
      return;
    }
    spillAndResetCurrentBufferedBatches();
    jumpToBlock(0);
  }

  public void jumpToLastBlock() throws HiveException {
    if (!canJump() || blocks.isStandingOnLast()){
      return;
    }
    spillAndResetCurrentBufferedBatches();
    jumpToBlock(blocks.size() - 1);
  }

  private boolean canJump() {
    return !(blocks.isEmpty() || (blocks.size() == 1 && !blocks.get(0).didSpillToDisk));
  }

  /**
   * Jumps to a given block by index. This method doesn't take care of spilling the current contents
   * from memory (even if it would be needed), so it should be used in cases where the caller
   * already made sure the the current contents of bufferedBatches have been spilled, so this is a
   * private method intentionally.
   *
   * @param toBlock
   * @throws HiveException
   */
  private VectorSpillBlock jumpToBlock(int toBlock) throws HiveException {
    VectorSpillBlock block = blocks.get(toBlock);
    if (blocks.isStandingOnBlock(block)){
      return block; //don't have to jump, we're in the desired position
    }
    blocks.setStartBatchIndexInMemory(block);
    readBlock(block);
    return block;
  }

  private void readBlock(VectorSpillBlock block) throws HiveException {
    block.setDoFinalRead(false);
    readBlockFromSpillToMemory(block);
    refreshInMemoryStartRowIndex(block);
  }

  private void readBlockFromSpillToMemory(VectorSpillBlock block) throws HiveException {
    VectorRowBytesContainer rowBytesContainer = block.getSpillRowBytesContainer();
    try {
      rowBytesContainer.prepareForReading();

      int batchIndex = 0;
      bufferedBatches.get(batchIndex).reset();

      /*
       * On rowBytesContainer level, we have a row-wise structure. As we want to get the rows back
       * into batches, we need to distribute them simply by filling the batches sequentially, but
       * taking care of the original number of rows in the batches (startRowIndex).
       */
      while (rowBytesContainer.readNext()) {
        if (batchIndex < block.startRowIndex.length - 1
            && bufferedBatches.get(batchIndex).size == block.startRowIndex[batchIndex + 1]
                - block.startRowIndex[batchIndex]) {
          batchIndex += 1;
          bufferedBatches.get(batchIndex).reset();
        }
        BufferedVectorizedRowBatch bufferedBatch = bufferedBatches.get(batchIndex);
        block.readSingleRowFromBytesContainer(bufferedBatch);
        bufferedBatch.size += 1;
        bufferedBatch.isLastGroupBatch = block.isLastGroupBatch[batchIndex];
        bufferedBatch.isInputExpressionEvaluated = block.isInputExpressionEvaluated[batchIndex];
      }
      currentBufferedBatchCount = block.spillBatchCount;
      partitionMetrics.batchesReadFromSpill += currentBufferedBatchCount;
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  private void spillAndResetCurrentBufferedBatches() throws HiveException {
    VectorSpillBlock block = blocks.getCurrentBlock();

    if (block.isFullySpilled()) {
      return;
    }
    block.didSpillToDisk = true;

    /*
     * starting from block.spillBatchCount makes only unspilled batches to be spilled
     */
    LOG.debug("Spilling batches from {} to {} in block: {}", block.spillBatchCount,
        currentBufferedBatchCount, block);
    BufferedVectorizedRowBatch previousBatch =
        block.spillBatchCount > 0 ? bufferedBatches.get(block.spillBatchCount - 1) : null;
    for (int i = block.spillBatchCount; i < currentBufferedBatchCount; i++){
      BufferedVectorizedRowBatch bufferedBatch = bufferedBatches.get(i);
      block.spillBatch(bufferedBatch);
      block.isLastGroupBatch[i] = bufferedBatch.isLastGroupBatch;
      block.isInputExpressionEvaluated[i] = bufferedBatch.isInputExpressionEvaluated;
      block.spillRowCount += bufferedBatch.size;
      block.spillBatchCount += 1;
      if (previousBatch != null) {
        block.startRowIndex[i] = block.startRowIndex[i - 1] + previousBatch.size;
        previousBatch.reset();
      }
      previousBatch = bufferedBatch;
    }
    bufferedBatches.get(currentBufferedBatchCount - 1).reset();
    partitionMetrics.batchesWrittenToSpill += currentBufferedBatchCount - block.spillBatchCount;
    currentBufferedBatchCount = 0;
  }

  private void copyBufferedColumns(VectorizedRowBatch batch, VectorizedRowBatch bufferedBatch) {
    // Copy critical columns.
    final int size = batch.size;
    for (int i = 0; i < bufferedColumnMap.length; i++) {
      VectorizedBatchUtil.copyNonSelectedColumnVector(
          batch, bufferedColumnMap[i], bufferedBatch, i, size);
    }

    bufferedBatch.size = size;
  }
}