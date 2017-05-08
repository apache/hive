/**
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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.persistence.HybridHashTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.HybridHashTableContainer.HashPartition;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinBytesTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorDeserializeRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorSerializeRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTableResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.optimized.VectorMapJoinOptimizedCreateHashTable;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.WriteBuffers.ByteSegmentRef;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinaryDeserializeRead;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinarySerializeWrite;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.ByteStream.Output;

/**
 * This class has methods for generating vectorized join results and forwarding batchs.
 *
 * In some cases when can forward the big table batch by setting scratch columns
 * with small table results and then making use of our output projection to pick out all the
 * output result columns.  This can improve performance by avoiding copying big table values.
 * So, we will use the big table batch's selected in use to represent those rows.
 *
 * At the same time, some output results need to be formed in the overflow batch.
 * For example, to form N x M cross product output results.  In this case, we will copy big
 * table values into the overflow batch and set scratch columns in it for small table results.
 * The "schema" of the overflow batch is the same as the big table batch so child operators
 * only need one definition of their input batch.  The overflow batch will be typically be
 * forwarded when it gets full, which might not be during a process call.
 *
 * NOTE: Child operators should not remember a received batch.
 */

public abstract class VectorMapJoinGenerateResultOperator extends VectorMapJoinCommonOperator {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinGenerateResultOperator.class.getName());
  private static final String CLASS_NAME = VectorMapJoinGenerateResultOperator.class.getName();
  private static final int CHECK_INTERRUPT_PER_OVERFLOW_BATCHES = 10;

  //------------------------------------------------------------------------------------------------

  private transient TypeInfo[] bigTableTypeInfos;

  private transient VectorSerializeRow bigTableVectorSerializeRow;

  private transient VectorDeserializeRow bigTableVectorDeserializeRow;

  private transient Thread ownThread;
  private transient int interruptCheckCounter = CHECK_INTERRUPT_PER_OVERFLOW_BATCHES;

  // Debug display.
  protected transient long batchCounter;

  /** Kryo ctor. */
  protected VectorMapJoinGenerateResultOperator() {
    super();
  }

  public VectorMapJoinGenerateResultOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  public VectorMapJoinGenerateResultOperator(CompilationOpContext ctx,
      VectorizationContext vContext, OperatorDesc conf) throws HiveException {
    super(ctx, vContext, conf);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    setUpInterruptChecking();
  }

  private void setUpInterruptChecking() {
    for (Operator<? extends OperatorDesc> child : childOperatorsArray) {
      // We will only do interrupt checking in the lowest-level operator for multiple joins.
      if (child instanceof VectorMapJoinGenerateResultOperator) return;
    }
    ownThread = Thread.currentThread();
  }

  protected void commonSetup(VectorizedRowBatch batch) throws HiveException {
    super.commonSetup(batch);

    batchCounter = 0;

  }

  //------------------------------------------------------------------------------------------------

  protected void performValueExpressions(VectorizedRowBatch batch,
      int[] allMatchs, int allMatchCount) {
    /*
     *  For the moment, pretend all matched are selected so we can evaluate the value
     *  expressions.
     *
     *  Since we may use the overflow batch when generating results, we will assign the
     *  selected and real batch size later...
     */
    int[] saveSelected = batch.selected;
    batch.selected = allMatchs;
    boolean saveSelectedInUse = batch.selectedInUse;
    batch.selectedInUse = true;
    batch.size =  allMatchCount;

    // Run our value expressions over whole batch.
    for(VectorExpression ve: bigTableValueExpressions) {
      ve.evaluate(batch);
    }

    batch.selected = saveSelected;
    batch.selectedInUse = saveSelectedInUse;
  }

  protected void doSmallTableDeserializeRow(VectorizedRowBatch batch, int batchIndex,
      ByteSegmentRef byteSegmentRef, VectorMapJoinHashMapResult hashMapResult)
          throws HiveException {

    byte[] bytes = byteSegmentRef.getBytes();
    int offset = (int) byteSegmentRef.getOffset();
    int length = byteSegmentRef.getLength();
    smallTableVectorDeserializeRow.setBytes(bytes, offset, length);

    try {
      // Our hash tables are immutable.  We can safely do by reference STRING, CHAR/VARCHAR, etc.
      smallTableVectorDeserializeRow.deserializeByRef(batch, batchIndex);
    } catch (Exception e) {
      throw new HiveException(
          "\nHashMapResult detail: " +
              hashMapResult.getDetailedHashMapResultPositionString() +
          "\nDeserializeRead detail: " +
              smallTableVectorDeserializeRow.getDetailedReadPositionString(),
          e);
    }
  }

  //------------------------------------------------------------------------------------------------

  /*
   * Common generate join results from hash maps used by Inner and Outer joins.
   */

  /**
   * Generate join results for a single small table value match.
   *
   * @param batch
   *          The big table batch.
   * @param hashMapResult
   *          The hash map results for the matching key.
   * @param allMatchs
   *          The selection array for all matches key.
   * @param allMatchesIndex
   *          Index into allMatches of the matching key we are generating for.
   * @param duplicateCount
   *          Number of equal key rows.
   * @param numSel
   *          Current number of rows that are remaining in the big table for forwarding.
   * @return
   *          The new count of selected rows.
   */
  protected int generateHashMapResultSingleValue(VectorizedRowBatch batch,
      VectorMapJoinHashMapResult hashMapResult, int[] allMatchs, int allMatchesIndex,
      int duplicateCount, int numSel) throws HiveException, IOException {

    // Read single value.

    ByteSegmentRef byteSegmentRef = hashMapResult.first();

    // Generate result within big table batch itself.

    for (int i = 0; i < duplicateCount; i++) {

      int batchIndex = allMatchs[allMatchesIndex + i];

      // Outer key copying is only used when we are using the input BigTable batch as the output.
      //
      if (bigTableVectorCopyOuterKeys != null) {
        // Copy within row.
        bigTableVectorCopyOuterKeys.copyByReference(batch, batchIndex, batch, batchIndex);
      }

      if (smallTableVectorDeserializeRow != null) {
        doSmallTableDeserializeRow(batch, batchIndex,
            byteSegmentRef, hashMapResult);
      }

      // VectorizedBatchUtil.debugDisplayOneRow(batch, batchIndex, "generateHashMapResultSingleValue big table");

      // Use the big table row as output.
      batch.selected[numSel++] = batchIndex;
    }

    return numSel;
  }

  /**
   * Generate results for a N x M cross product.
   *
   * @param batch
   *          The big table batch.
   * @param hashMapResult
   *          The hash map results for the matching key.
   * @param allMatchs
   *          The all match selected array that contains (physical) batch indices.
   * @param allMatchesIndex
   *          The index of the match key.
   * @param duplicateCount
   *          Number of equal key rows.
   */
  protected void generateHashMapResultMultiValue(VectorizedRowBatch batch,
      VectorMapJoinHashMapResult hashMapResult, int[] allMatchs, int allMatchesIndex,
      int duplicateCount) throws HiveException, IOException {

    if (useOverflowRepeatedThreshold &&
        hashMapResult.isCappedCountAvailable() &&
        hashMapResult.cappedCount() > overflowRepeatedThreshold) {

      // Large cross product: generate the vector optimization using repeating vectorized
      // row batch optimization in the overflow batch.

      generateHashMapResultLargeMultiValue(
                  batch, hashMapResult, allMatchs, allMatchesIndex, duplicateCount);
      return;
    }

    // We do the cross product of the N big table equal key row's values against the
    // small table matching key which has M value rows into overflow batch.

    for (int i = 0; i < duplicateCount; i++) {

      int batchIndex = allMatchs[allMatchesIndex + i];

      ByteSegmentRef byteSegmentRef = hashMapResult.first();
      while (byteSegmentRef != null) {

        // Copy the BigTable values into the overflow batch. Since the overflow batch may
        // not get flushed here, we must copy by value.
        // Note this includes any outer join keys that need to go into the small table "area".
        if (bigTableRetainedVectorCopy != null) {
          bigTableRetainedVectorCopy.copyByValue(batch, batchIndex,
                                                 overflowBatch, overflowBatch.size);
        }

        if (smallTableVectorDeserializeRow != null) {

          doSmallTableDeserializeRow(overflowBatch, overflowBatch.size,
              byteSegmentRef, hashMapResult);
        }

        // VectorizedBatchUtil.debugDisplayOneRow(overflowBatch, overflowBatch.size, "generateHashMapResultMultiValue overflow");

        overflowBatch.size++;
        if (overflowBatch.size == overflowBatch.DEFAULT_SIZE) {
          forwardOverflow();
        }
        byteSegmentRef = hashMapResult.next();
      }
    }
  }

  /**
   * Generate optimized results for a large N x M cross product using repeated vectorized row
   * batch optimization.
   *
   * @param batch
   *          The big table batch.
   * @param hashMapResult
   *          The hash map results for the matching key.
   * @param allMatchs
   *          The all match selected array that contains (physical) batch indices.
   * @param allMatchesIndex
   *          The index of the match key.
   * @param duplicateCount
   *          Number of equal key rows.
   */
  private void generateHashMapResultLargeMultiValue(VectorizedRowBatch batch,
      VectorMapJoinHashMapResult hashMapResult, int[] allMatchs, int allMatchesIndex,
      int duplicateCount) throws HiveException, IOException {

    // Kick out previous overflow batch results.
    if (overflowBatch.size > 0) {
      forwardOverflow();
    }

    ByteSegmentRef byteSegmentRef = hashMapResult.first();
    while (byteSegmentRef != null) {

      // Fill up as much of the overflow batch as possible with small table values.
      while (byteSegmentRef != null) {

        if (smallTableVectorDeserializeRow != null) {
          doSmallTableDeserializeRow(overflowBatch, overflowBatch.size,
              byteSegmentRef, hashMapResult);
        }

        overflowBatch.size++;
        if (overflowBatch.size == overflowBatch.DEFAULT_SIZE) {
          break;
        }
        byteSegmentRef = hashMapResult.next();
      }

      // Forward the overflow batch over and over:
      //
      //    Reference a new one big table row's values each time
      //      cross product
      //    Current "batch" of small table values.
      //
      // TODO: This could be further optimized to copy big table (equal) keys once
      //       and only copy big table values each time...
      //       And, not set repeating every time...
      //

      for (int i = 0; i < duplicateCount; i++) {

        int batchIndex = allMatchs[allMatchesIndex + i];

        if (bigTableRetainedVectorCopy != null) {
          // The one big table row's values repeat.
          bigTableRetainedVectorCopy.copyByReference(batch, batchIndex, overflowBatch, 0);
          for (int column : bigTableRetainedMapping.getOutputColumns()) {
            overflowBatch.cols[column].isRepeating = true;
          }
        }

        // Crucial here that we don't reset the overflow batch, or we will loose the small table
        // values we put in above.
        forwardOverflowNoReset();

        // Hand reset the big table columns.
        for (int column : bigTableRetainedMapping.getOutputColumns()) {
          ColumnVector colVector = overflowBatch.cols[column];
          colVector.reset();
        }
      }

      byteSegmentRef = hashMapResult.next();
      if (byteSegmentRef == null) {
        break;
      }

      // Get ready for a another round of small table values.
      overflowBatch.reset();
    }
    // Clear away any residue from our optimizations.
    overflowBatch.reset();
  }

  /**
   * Generate optimized results when entire batch key is repeated and it matched the hash map.
   *
   * @param batch
   *          The big table batch.
   * @param hashMapResult
   *          The hash map results for the repeated key.
   */
  protected void generateHashMapResultRepeatedAll(VectorizedRowBatch batch,
              VectorMapJoinHashMapResult hashMapResult) throws IOException, HiveException {

    int[] selected = batch.selected;

    if (batch.selectedInUse) {
      // The selected array is already filled in as we want it.
    } else {
      for (int i = 0; i < batch.size; i++) {
        selected[i] = i;
      }
      batch.selectedInUse = true;
    }

    int numSel = 0;
    if (hashMapResult.isSingleRow()) {
      numSel = generateHashMapResultSingleValue(batch, hashMapResult,
          batch.selected, 0, batch.size, numSel);

    } else {
      generateHashMapResultMultiValue(batch, hashMapResult,
          batch.selected, 0, batch.size);
    }

    batch.size = numSel;
  }

  //-----------------------------------------------------------------------------------------------

  /*
   * Spill.
   */


  private void setupSpillSerDe(VectorizedRowBatch batch) throws HiveException {

    TypeInfo[] inputObjInspectorsTypeInfos =
        VectorizedBatchUtil.typeInfosFromStructObjectInspector(
               (StructObjectInspector) inputObjInspectors[posBigTable]);

    List<Integer> projectedColumns = vContext.getProjectedColumns();
    int projectionSize = vContext.getProjectedColumns().size();

    List<TypeInfo> typeInfoList = new ArrayList<TypeInfo>();
    List<Integer> noNullsProjectionList = new ArrayList<Integer>();
    for (int i = 0; i < projectionSize; i++) {
      int projectedColumn = projectedColumns.get(i);
      if (batch.cols[projectedColumn] != null &&
          inputObjInspectorsTypeInfos[i].getCategory() == Category.PRIMITIVE) {
        // Only columns present in the batch and non-complex types.
        typeInfoList.add(inputObjInspectorsTypeInfos[i]);
        noNullsProjectionList.add(projectedColumn);
      }
    }

    int[] noNullsProjection = ArrayUtils.toPrimitive(noNullsProjectionList.toArray(new Integer[0]));
    int noNullsProjectionSize = noNullsProjection.length;
    bigTableTypeInfos = typeInfoList.toArray(new TypeInfo[0]);

    bigTableVectorSerializeRow =
            new VectorSerializeRow<LazyBinarySerializeWrite>(
                new LazyBinarySerializeWrite(noNullsProjectionSize));

    bigTableVectorSerializeRow.init(
                bigTableTypeInfos,
                noNullsProjection);

    bigTableVectorDeserializeRow =
        new VectorDeserializeRow<LazyBinaryDeserializeRead>(
            new LazyBinaryDeserializeRead(
                bigTableTypeInfos,
                /* useExternalBuffer */ true));

    bigTableVectorDeserializeRow.init(noNullsProjection);
  }

  private void spillSerializeRow(VectorizedRowBatch batch, int batchIndex,
      VectorMapJoinHashTableResult hashTableResult) throws IOException {

    int partitionId = hashTableResult.spillPartitionId();

    HybridHashTableContainer ht = (HybridHashTableContainer) mapJoinTables[posSingleVectorMapJoinSmallTable];
    HashPartition hp = ht.getHashPartitions()[partitionId];

    VectorMapJoinRowBytesContainer rowBytesContainer = hp.getMatchfileRowBytesContainer();
    Output output = rowBytesContainer.getOuputForRowBytes();
//  int offset = output.getLength();
    bigTableVectorSerializeRow.setOutputAppend(output);
    bigTableVectorSerializeRow.serializeWrite(batch, batchIndex);
//  int length = output.getLength() - offset;
    rowBytesContainer.finishRow();

//  LOG.debug("spillSerializeRow spilled batchIndex " + batchIndex + ", length " + length);
  }

  protected void spillHashMapBatch(VectorizedRowBatch batch,
      VectorMapJoinHashTableResult[] hashTableResults,
      int[] spills, int[] spillHashTableResultIndices, int spillCount)
          throws HiveException, IOException {

    if (bigTableVectorSerializeRow == null) {
      setupSpillSerDe(batch);
    }

    for (int i = 0; i < spillCount; i++) {
      int batchIndex = spills[i];

      int hashTableResultIndex = spillHashTableResultIndices[i];
      VectorMapJoinHashTableResult hashTableResult = hashTableResults[hashTableResultIndex];

      spillSerializeRow(batch, batchIndex, hashTableResult);
    }
  }

  protected void spillBatchRepeated(VectorizedRowBatch batch,
      VectorMapJoinHashTableResult hashTableResult) throws HiveException, IOException {

    if (bigTableVectorSerializeRow == null) {
      setupSpillSerDe(batch);
    }

    int[] selected = batch.selected;
    boolean selectedInUse = batch.selectedInUse;

    for (int logical = 0; logical < batch.size; logical++) {
      int batchIndex = (selectedInUse ? selected[logical] : logical);
      spillSerializeRow(batch, batchIndex, hashTableResult);
    }
  }

  @Override
  protected void reloadHashTable(byte pos, int partitionId)
          throws IOException, HiveException, SerDeException, ClassNotFoundException {

    // The super method will reload a hash table partition of one of the small tables.
    // Currently, for native vector map join it will only be one small table.
    super.reloadHashTable(pos, partitionId);

    MapJoinTableContainer smallTable = spilledMapJoinTables[pos];

    vectorMapJoinHashTable = VectorMapJoinOptimizedCreateHashTable.createHashTable(conf,
        smallTable);
    needHashTableSetup = true;
    LOG.info("Created " + vectorMapJoinHashTable.getClass().getSimpleName() + " from " + this.getClass().getSimpleName());

    if (isLogDebugEnabled) {
      LOG.debug(CLASS_NAME + " reloadHashTable!");
    }
  }

  @Override
  protected void reProcessBigTable(int partitionId)
      throws HiveException {

    if (isLogDebugEnabled) {
      LOG.debug(CLASS_NAME + " reProcessBigTable enter...");
    }

    if (spillReplayBatch == null) {
      // The process method was not called -- no big table rows.
      return;
    }

    HashPartition partition = firstSmallTable.getHashPartitions()[partitionId];

    int rowCount = 0;
    int batchCount = 0;

    try {
      VectorMapJoinRowBytesContainer bigTable = partition.getMatchfileRowBytesContainer();
      bigTable.prepareForReading();

      while (bigTable.readNext()) {
        rowCount++;

        byte[] bytes = bigTable.currentBytes();
        int offset = bigTable.currentOffset();
        int length = bigTable.currentLength();

        bigTableVectorDeserializeRow.setBytes(bytes, offset, length);
        try {
          bigTableVectorDeserializeRow.deserialize(spillReplayBatch, spillReplayBatch.size);
        } catch (Exception e) {
          throw new HiveException(
              "\nDeserializeRead detail: " +
                  bigTableVectorDeserializeRow.getDetailedReadPositionString(),
              e);
        }
        spillReplayBatch.size++;

        if (spillReplayBatch.size == VectorizedRowBatch.DEFAULT_SIZE) {
          process(spillReplayBatch, posBigTable); // call process once we have a full batch
          spillReplayBatch.reset();
          batchCount++;
        }
      }
      // Process the row batch that has less than DEFAULT_SIZE rows
      if (spillReplayBatch.size > 0) {
        process(spillReplayBatch, posBigTable);
        spillReplayBatch.reset();
        batchCount++;
      }
      bigTable.clear();
    } catch (Exception e) {
      LOG.info(CLASS_NAME + " reProcessBigTable exception! " + e);
      throw new HiveException(e);
    }

    if (isLogDebugEnabled) {
      LOG.debug(CLASS_NAME + " reProcessBigTable exit! " + rowCount + " row processed and " + batchCount + " batches processed");
    }
  }


  //-----------------------------------------------------------------------------------------------

  /*
   * Forwarding.
   */

  /**
   * Forward the big table batch to the children.
   *
   * @param batch
   *          The big table batch.
   */
  public void forwardBigTableBatch(VectorizedRowBatch batch) throws HiveException {

    // Save original projection.
    int[] originalProjections = batch.projectedColumns;
    int originalProjectionSize = batch.projectionSize;

    // Project with the output of our operator.
    batch.projectionSize = outputProjection.length;
    batch.projectedColumns = outputProjection;

    forward(batch, null);

    // Revert the projected columns back, because batch can be re-used by our parent operators.
    batch.projectionSize = originalProjectionSize;
    batch.projectedColumns = originalProjections;
  }


  /**
   * Forward the overflow batch and reset the batch.
   */
  protected void forwardOverflow() throws HiveException {
    forward(overflowBatch, null);
    overflowBatch.reset();
    maybeCheckInterrupt();
  }

  private void maybeCheckInterrupt() throws HiveException {
    if (ownThread == null || --interruptCheckCounter > 0) return;
    if (ownThread.isInterrupted()) {
      throw new HiveException("Thread interrupted");
    }
    interruptCheckCounter = CHECK_INTERRUPT_PER_OVERFLOW_BATCHES;
  }

  /**
   * Forward the overflow batch, but do not reset the batch.
   */
  private void forwardOverflowNoReset() throws HiveException {
    forward(overflowBatch, null);
  }

  /*
   * Close.
   */

  /**
   * On close, make sure a partially filled overflow batch gets forwarded.
   */
  @Override
  public void closeOp(boolean aborted) throws HiveException {
    super.closeOp(aborted);
    if (!aborted && overflowBatch.size > 0) {
      forwardOverflow();
    }
    if (isLogDebugEnabled) {
      LOG.debug("VectorMapJoinInnerLongOperator closeOp " + batchCounter + " batches processed");
    }
  }

  //-----------------------------------------------------------------------------------------------

  /*
   * Debug.
   */

  public boolean verifyMonotonicallyIncreasing(int[] selected, int size) {

    if (size == 0) {
      return true;
    }
    int prevBatchIndex = selected[0];

    for (int i = 1; i < size; i++) {
      int batchIndex = selected[i];
      if (batchIndex <= prevBatchIndex) {
        return false;
      }
      prevBatchIndex = batchIndex;
    }
    return true;
  }

  public static String intArrayToRangesString(int selection[], int size) {
    if (size == 0) {
      return "[]";
    }

    StringBuilder sb = new StringBuilder();

    // Use ranges and duplicate multipliers to reduce the size of the display.
    sb.append("[");
    int firstIndex = 0;
    int firstValue = selection[0];

    boolean duplicates = false;

    int i = 1;
    for ( ; i < size; i++) {
      int newValue = selection[i];
      if (newValue == selection[i - 1]) {

        // Duplicate.
        duplicates = true;

        if (newValue == firstValue) {
          continue;
        } else {
          // Prior none, singleton, or range?
          int priorRangeLength = i - 1 - firstIndex;

          if (priorRangeLength == 0) {
            continue;
          }
          if (firstIndex > 0) {
            sb.append(",");
          }
          sb.append(firstValue);
          if (priorRangeLength > 1) {
            sb.append(".." + selection[i - 2]);
          }
          firstIndex = i - 1;
          firstValue = newValue;
          continue;
        }
      } else {
        if (duplicates) {
          int numDuplicates = i - firstIndex;
          if (firstIndex > 0) {
            sb.append(",");
          }
          sb.append(numDuplicates + "*" + firstValue);
          duplicates = false;
          firstIndex = i;
          firstValue = newValue;
          continue;
        } if (newValue == selection[i - 1] + 1) {
          // Continue range..
          continue;
        } else {
          // Prior singleton or range?
          int priorRangeLength = i - firstIndex;
          if (firstIndex > 0) {
            sb.append(",");
          }
          sb.append(firstValue);
          if (priorRangeLength > 1) {
            sb.append(".." + selection[i - 1]);
          }
          firstIndex = i;
          firstValue = newValue;
          continue;
        }
      }
    }
    if (duplicates) {
      int numDuplicates = i - firstIndex;
      if (firstIndex > 0) {
        sb.append(",");
      }
      sb.append(numDuplicates + "*" + firstValue);
    } else {
      // Last singleton or range?
      int priorRangeLength = i - firstIndex;
      if (firstIndex > 0) {
        sb.append(",");
      }
      sb.append(firstValue);
      if (priorRangeLength > 1) {
        sb.append(".." + selection[i - 1]);
      }
    }
    sb.append("]");
    return sb.toString();
  }

  public static String longArrayToRangesString(long selection[], int size) {
    if (size == 0) {
      return "[]";
    }

    StringBuilder sb = new StringBuilder();

    // Use ranges and duplicate multipliers to reduce the size of the display.
    sb.append("[");
    int firstIndex = 0;
    long firstValue = selection[0];

    boolean duplicates = false;

    int i = 1;
    for ( ; i < size; i++) {
      long newValue = selection[i];
      if (newValue == selection[i - 1]) {

        // Duplicate.
        duplicates = true;

        if (newValue == firstValue) {
          continue;
        } else {
          // Prior none, singleton, or range?
          int priorRangeLength = i - 1 - firstIndex;

          if (priorRangeLength == 0) {
            continue;
          }
          if (firstIndex > 0) {
            sb.append(",");
          }
          sb.append(firstValue);
          if (priorRangeLength > 1) {
            sb.append(".." + selection[i - 2]);
          }
          firstIndex = i - 1;
          firstValue = newValue;
          continue;
        }
      } else {
        if (duplicates) {
          int numDuplicates = i - firstIndex;
          if (firstIndex > 0) {
            sb.append(",");
          }
          sb.append(numDuplicates + "*" + firstValue);
          duplicates = false;
          firstIndex = i;
          firstValue = newValue;
          continue;
        } if (newValue == selection[i - 1] + 1) {
          // Continue range..
          continue;
        } else {
          // Prior singleton or range?
          int priorRangeLength = i - firstIndex;
          if (firstIndex > 0) {
            sb.append(",");
          }
          sb.append(firstValue);
          if (priorRangeLength > 1) {
            sb.append(".." + selection[i - 1]);
          }
          firstIndex = i;
          firstValue = newValue;
          continue;
        }
      }
    }
    if (duplicates) {
      int numDuplicates = i - firstIndex;
      if (firstIndex > 0) {
        sb.append(",");
      }
      sb.append(numDuplicates + "*" + firstValue);
    } else {
      // Last singleton or range?
      int priorRangeLength = i - firstIndex;
      if (firstIndex > 0) {
        sb.append(",");
      }
      sb.append(firstValue);
      if (priorRangeLength > 1) {
        sb.append(".." + selection[i - 1]);
      }
    }
    sb.append("]");
    return sb.toString();
  }
}
