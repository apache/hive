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
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorDeserializeRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorSerializeRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.rowbytescontainer.VectorRowBytesContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinaryDeserializeRead;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinarySerializeWrite;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import com.google.common.base.Preconditions;

/**
 * This class is encapsulates one or more VectorizedRowBatch of a PTF group.
 */
public class VectorPTFGroupBatches {

  private static final long serialVersionUID = 1L;
  private static final String CLASS_NAME = VectorPTFGroupBatches.class.getName();
  private static final Log LOG = LogFactory.getLog(CLASS_NAME);

  private Configuration hconf;

  private VectorPTFEvaluatorBase[] evaluators;
  private int[] outputProjectionColumnMap;
  private int[] keyInputColumnMap;

  private int bufferedColumnCount;
  private int[] bufferedColumnMap;
  private TypeInfo[] bufferedTypeInfos;

  private ArrayList<VectorizedRowBatch> bufferedBatches;

  private VectorizedRowBatch overflowBatch;

  private int allocatedBufferedBatchCount;
  private int currentBufferedBatchCount;

  private int spillLimitBufferedBatchCount;
  private boolean didSpillToDisk;
  private String spillLocalDirs;
  private long spillRowCount;
  private VectorRowBytesContainer spillRowBytesContainer;

  private transient VectorSerializeRow bufferedBatchVectorSerializeRow;
  private transient VectorDeserializeRow bufferedBatchVectorDeserializeRow;

  public VectorPTFGroupBatches(Configuration hconf, int vectorizedPTFMaxMemoryBufferingBatchCount) {
    this.hconf = hconf;
    allocatedBufferedBatchCount = 0;
    currentBufferedBatchCount = 0;

    spillLocalDirs = HiveUtils.getLocalDirList(hconf);

    // Cannot be 0.
    spillLimitBufferedBatchCount = Math.max(1, vectorizedPTFMaxMemoryBufferingBatchCount);

    didSpillToDisk = false;
    spillLocalDirs = null;
    spillRowBytesContainer = null;
    bufferedBatchVectorSerializeRow = null;
    bufferedBatchVectorDeserializeRow = null;
  }

  public void init(
      TypeInfo[] reducerBatchTypeInfos,
      VectorPTFEvaluatorBase[] evaluators,
      int[] outputProjectionColumnMap, TypeInfo[] outputTypeInfos,
      int[] keyInputColumnMap, int[] nonKeyInputColumnMap, int[] streamingEvaluatorNums,
      VectorizedRowBatch overflowBatch) {
    this.evaluators = evaluators;
    this.outputProjectionColumnMap = outputProjectionColumnMap;
    this.keyInputColumnMap = keyInputColumnMap;

    /*
     * If we have more than one group key batch, we will buffer their contents.
     * We don't buffer the key columns since they are a constant for the group key.
     *
     * We buffer the non-key input columns.  And, we buffer any streaming columns that will already
     * have their output values.
     */
    final int nonKeyInputColumnCount = nonKeyInputColumnMap.length;
    final int streamingEvaluatorCount = streamingEvaluatorNums.length;
    bufferedColumnCount = nonKeyInputColumnCount + streamingEvaluatorCount;
    bufferedColumnMap = new int[bufferedColumnCount];
    bufferedTypeInfos = new TypeInfo[bufferedColumnCount];
    for (int i = 0; i < nonKeyInputColumnCount; i++) {
      final int columnNum = nonKeyInputColumnMap[i];
      bufferedColumnMap[i] = columnNum;
      bufferedTypeInfos[i] = reducerBatchTypeInfos[columnNum];
    }

    for (int i = 0; i < streamingEvaluatorCount; i++) {
      final int streamingEvaluatorNum = streamingEvaluatorNums[i];
      final int bufferedMapIndex = nonKeyInputColumnCount + i;
      bufferedColumnMap[bufferedMapIndex] = outputProjectionColumnMap[streamingEvaluatorNum];
      bufferedTypeInfos[bufferedMapIndex] = outputTypeInfos[streamingEvaluatorNum];
    }
    this.overflowBatch = overflowBatch;
    bufferedBatches = new ArrayList<VectorizedRowBatch>(0);
  }

  private VectorRowBytesContainer getSpillRowBytesContainer() throws HiveException {
    if (spillRowBytesContainer == null) {
      spillRowBytesContainer = new VectorRowBytesContainer(spillLocalDirs);

      if (bufferedBatchVectorSerializeRow == null) {
        bufferedBatchVectorSerializeRow =
            new VectorSerializeRow<LazyBinarySerializeWrite>(
                new LazyBinarySerializeWrite(bufferedColumnMap.length));

        // Deserialize just the columns we a buffered batch, which has only the non-key inputs and
        // streamed column outputs.
        bufferedBatchVectorSerializeRow.init(bufferedTypeInfos);

        bufferedBatchVectorDeserializeRow =
            new VectorDeserializeRow<LazyBinaryDeserializeRead>(
                new LazyBinaryDeserializeRead(
                    bufferedTypeInfos,
                    /* useExternalBuffer */ true));

        // Deserialize the fields into the *overflow* batch using the buffered batch column map.
        bufferedBatchVectorDeserializeRow.init(bufferedColumnMap);
      }
    }
    return spillRowBytesContainer;
  }

  private void releaseSpillRowBytesContainer() {
    spillRowBytesContainer.clear();
    spillRowBytesContainer = null;
  }

  public void evaluateStreamingGroupBatch(VectorizedRowBatch batch, boolean isLastGroupBatch) {

    // Streaming evaluators fill in their results during the evaluate call.
    for (VectorPTFEvaluatorBase evaluator : evaluators) {
      evaluator.evaluateGroupBatch(batch, isLastGroupBatch);
    }
  }

  public void evaluateGroupBatch(VectorizedRowBatch batch, boolean isLastGroupBatch) {
    for (VectorPTFEvaluatorBase evaluator : evaluators) {
      evaluator.evaluateGroupBatch(batch, isLastGroupBatch);
    }
  }

  private void fillGroupResults(VectorizedRowBatch batch) {

    /*
     * Do careful maintenance of the outputColVector.noNulls flag.
     */

    for (VectorPTFEvaluatorBase evaluator : evaluators) {
      final int outputColumnNum = evaluator.getOutputColumnNum();
      if (evaluator.streamsResult()) {
        continue;
      }
      final ColumnVector outputColVector = batch.cols[outputColumnNum];
      outputColVector.isRepeating = true;
      final boolean isGroupResultNull = evaluator.isGroupResultNull();
      outputColVector.isNull[0] = isGroupResultNull;
      if (isGroupResultNull) {
        outputColVector.noNulls = false;
      } else {
        switch (evaluator.getResultColumnVectorType()) {
        case LONG:
          ((LongColumnVector) outputColVector).vector[0] = evaluator.getLongGroupResult();
          break;
        case DOUBLE:
          ((DoubleColumnVector) outputColVector).vector[0] = evaluator.getDoubleGroupResult();
          break;
        case DECIMAL:
          ((DecimalColumnVector) outputColVector).vector[0].set(evaluator.getDecimalGroupResult());
          break;
        default:
          throw new RuntimeException("Unexpected column vector type " + evaluator.getResultColumnVectorType());
        }
      }
    }
  }

  private void forwardBufferedBatches(VectorPTFOperator vecPTFOperator, int index)
      throws HiveException {
    VectorizedRowBatch bufferedBatch = bufferedBatches.get(index);

    final int size = bufferedBatch.size;
    final int bufferedColumnCount = bufferedColumnMap.length;
    for (int i = 0; i < bufferedColumnCount; i++) {

      // Copy ColumnVectors to overflowBatch.  We remember buffered columns compactly in the
      // buffered VRBs without other columns or scratch columns.
      VectorizedBatchUtil.copyNonSelectedColumnVector(
          bufferedBatch, i, overflowBatch, bufferedColumnMap[i], size);
    }
    overflowBatch.size = size;
    fillGroupResults(overflowBatch);
    vecPTFOperator.forward(overflowBatch, null);
  }

  private void forwardSpilledBatches(VectorPTFOperator vecPTFOperator, VectorizedRowBatch lastBatch)
      throws HiveException {

    overflowBatch.reset();
    copyPartitionAndOrderColumnsToOverflow(lastBatch);

    long spillRowsRead = 0;
    try {
      VectorRowBytesContainer rowBytesContainer = getSpillRowBytesContainer();
      rowBytesContainer.prepareForReading();

      while (rowBytesContainer.readNext()) {

        byte[] bytes = rowBytesContainer.currentBytes();
        int offset = rowBytesContainer.currentOffset();
        int length = rowBytesContainer.currentLength();

        bufferedBatchVectorDeserializeRow.setBytes(bytes, offset, length);
        try {
          bufferedBatchVectorDeserializeRow.deserialize(overflowBatch, overflowBatch.size);
        } catch (Exception e) {
          throw new HiveException(
              "\nDeserializeRead detail: " +
                  bufferedBatchVectorDeserializeRow.getDetailedReadPositionString(),
              e);
        }
        overflowBatch.size++;
        spillRowsRead++;

        if (overflowBatch.size == VectorizedRowBatch.DEFAULT_SIZE) {

          fillGroupResults(overflowBatch);
          vecPTFOperator.forward(overflowBatch, null);

          overflowBatch.reset();
          copyPartitionAndOrderColumnsToOverflow(lastBatch);
        }
      }
      // Process the row batch that has less than DEFAULT_SIZE rows
      if (overflowBatch.size > 0) {

        fillGroupResults(overflowBatch);
        vecPTFOperator.forward(overflowBatch, null);

        overflowBatch.reset();
        copyPartitionAndOrderColumnsToOverflow(lastBatch);
      }
      Preconditions.checkState(spillRowsRead == spillRowCount);

      // For now, throw away file.
      releaseSpillRowBytesContainer();

    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  private void copyPartitionAndOrderColumnsToOverflow(VectorizedRowBatch lastBatch) {

    // Set partition and order columns in overflowBatch.
    // We can set by ref since our last batch is held by us.
    final int keyInputColumnCount = keyInputColumnMap.length;
    for (int i = 0; i < keyInputColumnCount; i++) {
      final int keyColumnNum = keyInputColumnMap[i];
      Preconditions.checkState(overflowBatch.cols[keyColumnNum] != null);
      VectorizedBatchUtil.copyRepeatingColumn(
          lastBatch, keyColumnNum, overflowBatch, keyColumnNum, /* setByValue */ false);
    }
  }

  public void fillGroupResultsAndForward(VectorPTFOperator vecPTFOperator,
      VectorizedRowBatch lastBatch) throws HiveException {

    if (didSpillToDisk) {
      forwardSpilledBatches(vecPTFOperator, lastBatch);
      didSpillToDisk = false;
    }

    if (currentBufferedBatchCount > 0) {
      overflowBatch.reset();
      copyPartitionAndOrderColumnsToOverflow(lastBatch);
      for (int i = 0; i < currentBufferedBatchCount; i++) {
        forwardBufferedBatches(vecPTFOperator, i);
      }
      currentBufferedBatchCount = 0;
    }

    fillGroupResults(lastBatch);

    // Save original projection.
    int[] originalProjections = lastBatch.projectedColumns;
    int originalProjectionSize = lastBatch.projectionSize;

    // Project with the output of our operator.
    lastBatch.projectionSize = outputProjectionColumnMap.length;
    lastBatch.projectedColumns = outputProjectionColumnMap;

    vecPTFOperator.forward(lastBatch, null);

    // Revert the projected columns back, because batch can be re-used by our parent operators.
    lastBatch.projectionSize = originalProjectionSize;
    lastBatch.projectedColumns = originalProjections;

  }

  public void resetEvaluators() {
    for (VectorPTFEvaluatorBase evaluator : evaluators) {
      evaluator.resetEvaluator();
    }
  }

  private VectorizedRowBatch newBufferedBatch(VectorizedRowBatch batch) throws HiveException {
    final int bufferedColumnCount = bufferedColumnMap.length;
    VectorizedRowBatch newBatch = new VectorizedRowBatch(bufferedColumnCount);
    for (int i = 0; i < bufferedColumnCount; i++) {
      newBatch.cols[i] =
          VectorizedBatchUtil.makeLikeColumnVector(batch.cols[bufferedColumnMap[i]]);
      newBatch.cols[i].init();
    }
    return newBatch;
  }

  public void bufferGroupBatch(VectorizedRowBatch batch) throws HiveException {

    try {
      // When we've buffered the max allowed, spill the oldest one to make space.
      if (currentBufferedBatchCount >= spillLimitBufferedBatchCount) {

        VectorRowBytesContainer rowBytesContainer = getSpillRowBytesContainer();

        if (!didSpillToDisk) {
          // UNDONE: Don't reuse for now.
          // rowBytesContainer.resetWrite();
          didSpillToDisk = true;
          spillRowCount = 0;
        }

        // Grab the oldest in-memory buffered batch and dump it to disk.
        VectorizedRowBatch oldestBufferedBatch = bufferedBatches.remove(0);

        final boolean selectedInUse = oldestBufferedBatch.selectedInUse;
        int[] selected = oldestBufferedBatch.selected;
        final int size = oldestBufferedBatch.size;
        for (int logicalIndex = 0; logicalIndex < size; logicalIndex++) {
          final int batchIndex = (selectedInUse ? selected[logicalIndex] : logicalIndex);

          Output output = rowBytesContainer.getOuputForRowBytes();
          bufferedBatchVectorSerializeRow.setOutputAppend(output);
          bufferedBatchVectorSerializeRow.serializeWrite(oldestBufferedBatch, batchIndex);
          rowBytesContainer.finishRow();
          spillRowCount++;
        }

        // Put now available buffered batch at end.
        oldestBufferedBatch.reset();
        bufferedBatches.add(oldestBufferedBatch);
        currentBufferedBatchCount--;
      }

      final int bufferedColumnCount = bufferedColumnMap.length;
      if (allocatedBufferedBatchCount <= currentBufferedBatchCount) {
        VectorizedRowBatch newBatch = newBufferedBatch(batch);
        bufferedBatches.add(newBatch);
        allocatedBufferedBatchCount++;
      }

      VectorizedRowBatch bufferedBatch = bufferedBatches.get(currentBufferedBatchCount++);

      // Copy critical columns.
      final int size = batch.size;
      for (int i = 0; i < bufferedColumnCount; i++) {
        VectorizedBatchUtil.copyNonSelectedColumnVector(
            batch, bufferedColumnMap[i], bufferedBatch, i, size);
      }

      bufferedBatch.size = size;
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }
}