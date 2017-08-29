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

package org.apache.hadoop.hive.ql.exec.vector.ptf;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import com.google.common.base.Preconditions;

/**
 * This class is encapsulates one or more VectorizedRowBatch of a PTF group.
 */
public class VectorPTFGroupBatches {

  private static final long serialVersionUID = 1L;
  private static final String CLASS_NAME = VectorPTFGroupBatches.class.getName();
  private static final Log LOG = LogFactory.getLog(CLASS_NAME);

  private VectorPTFEvaluatorBase[] evaluators;
  private int[] outputColumnMap;
  private int[] keyInputColumnMap;
  private int[] bufferedColumnMap;

  private ArrayList<VectorizedRowBatch> bufferedBatches;

  private VectorizedRowBatch overflowBatch;

  private int allocatedBufferedBatchCount;
  private int currentBufferedBatchCount;

  public VectorPTFGroupBatches() {
    allocatedBufferedBatchCount = 0;
    currentBufferedBatchCount = 0;
  }

  public void init(VectorPTFEvaluatorBase[] evaluators, int[] outputColumnMap,
      int[] keyInputColumnMap, int[] nonKeyInputColumnMap, int[] streamingColumnMap,
      VectorizedRowBatch overflowBatch) {
    this.evaluators = evaluators;
    this.outputColumnMap = outputColumnMap;
    this.keyInputColumnMap = keyInputColumnMap;
    final int nonKeyInputColumnCount = nonKeyInputColumnMap.length;
    final int streamingColumnCount = streamingColumnMap.length;
    final int bufferedColumnCount = nonKeyInputColumnCount + streamingColumnCount;
    bufferedColumnMap = new int[bufferedColumnCount];
    for (int i = 0; i < nonKeyInputColumnCount; i++) {
      bufferedColumnMap[i] = nonKeyInputColumnMap[i];
    }
    for (int i = nonKeyInputColumnCount; i < bufferedColumnCount; i++) {
      bufferedColumnMap[i] = streamingColumnMap[i - nonKeyInputColumnCount];
    }
    this.overflowBatch = overflowBatch;
    bufferedBatches = new ArrayList<VectorizedRowBatch>(0);
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
        outputColVector.noNulls = true;
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

    final int size = bufferedColumnMap.length;
    for (int i = 0; i < size; i++) {

      // Swap ColumnVectors with overflowBatch.  We remember buffered columns compactly in the
      // buffered VRBs without other columns or scratch columns.
      VectorizedBatchUtil.swapColumnVector(
          bufferedBatch, i, overflowBatch, bufferedColumnMap[i]);

      overflowBatch.size = bufferedBatch.size;
      fillGroupResults(overflowBatch);
      vecPTFOperator.forward(overflowBatch, null);
    }
  }

  public void fillGroupResultsAndForward(VectorPTFOperator vecPTFOperator,
      VectorizedRowBatch lastBatch) throws HiveException {

    if (currentBufferedBatchCount > 0) {

      // Set partition and order columns in overflowBatch.
      // We can set by ref since our last batch is held by us.
      final int keyInputColumnCount = keyInputColumnMap.length;
      for (int i = 0; i < keyInputColumnCount; i++) {
        VectorizedBatchUtil.copyRepeatingColumn(lastBatch, i, overflowBatch, i, /* setByValue */ false);
      }

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
    lastBatch.projectionSize = outputColumnMap.length;
    lastBatch.projectedColumns = outputColumnMap;

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

    final int bufferedColumnCount = bufferedColumnMap.length;
    if (allocatedBufferedBatchCount <= currentBufferedBatchCount) {
      VectorizedRowBatch newBatch = newBufferedBatch(batch);
      bufferedBatches.add(newBatch);
      allocatedBufferedBatchCount++;
    }

    VectorizedRowBatch bufferedBatch = bufferedBatches.get(currentBufferedBatchCount++);

    for (int i = 0; i < bufferedColumnCount; i++) {
      VectorizedBatchUtil.swapColumnVector(
          batch, bufferedColumnMap[i], bufferedBatch, i);
    }

    bufferedBatch.size = batch.size;
  }
}