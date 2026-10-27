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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.exec.vector.ptf;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFVariance.VarianceKind;
import org.apache.hadoop.hive.ql.udf.ptf.Range;

import com.google.common.base.Preconditions;

/**
 * This class evaluates the variance family functions for a PTF group.
 *
 * The group result is calculated from the non-null values of the window range.  When a range
 * starts where the previous one started and does not end before it (e.g. UNBOUNDED PRECEDING
 * frames), the previous aggregation state is reused and only the new rows are iterated.  As the
 * values are still iterated in row order, the result is the same as a full recalculation.
 */
public class VectorPTFEvaluatorVariance extends VectorPTFEvaluatorAbstractVariance {

  private Range previousRange;
  private long previousCount;
  private double previousSum;
  private double previousVariance;

  public VectorPTFEvaluatorVariance(WindowFrameDef windowFrameDef, VectorExpression inputVecExpr,
      int outputColumnNum, Type inputColumnVectorType, VarianceKind varianceKind) {
    super(windowFrameDef, inputVecExpr, outputColumnNum, inputColumnVectorType, varianceKind);
    resetEvaluator();
  }

  @Override
  public void evaluateGroupBatch(VectorizedRowBatch batch)
      throws HiveException {

    // We do not filter when PTF is in reducer.
    Preconditions.checkState(!batch.selectedInUse);

    final int size = batch.size;
    if (size == 0) {
      return;
    }
    ColumnVector colVector = batch.cols[inputColumnNum];
    if (colVector.isRepeating) {
      if (colVector.noNulls || !colVector.isNull[0]) {
        final double value = getDouble(colVector, 0);
        for (int i = 0; i < size; i++) {
          iterate(value);
        }
      }
      return;
    }

    switch (inputColumnVectorType) {
    case LONG:
      iterateLongs((LongColumnVector) colVector, size);
      break;
    case DOUBLE:
      iterateDoubles((DoubleColumnVector) colVector, size);
      break;
    case DECIMAL:
      iterateDecimals((DecimalColumnVector) colVector, size);
      break;
    default:
      throw new IllegalStateException("Unexpected column vector type " + inputColumnVectorType);
    }
  }

  private void iterateLongs(LongColumnVector colVector, int size) {
    for (int i = 0; i < size; i++) {
      if (colVector.noNulls || !colVector.isNull[i]) {
        iterate((double) colVector.vector[i]);
      }
    }
  }

  private void iterateDoubles(DoubleColumnVector colVector, int size) {
    for (int i = 0; i < size; i++) {
      if (colVector.noNulls || !colVector.isNull[i]) {
        iterate(colVector.vector[i]);
      }
    }
  }

  private void iterateDecimals(DecimalColumnVector colVector, int size) {
    for (int i = 0; i < size; i++) {
      if (colVector.noNulls || !colVector.isNull[i]) {
        iterate(colVector.vector[i].doubleValue());
      }
    }
  }

  @Override
  public boolean streamsResult() {
    // We must evaluate whole group before producing a result.
    return false;
  }

  @Override
  public boolean isGroupResultNull() {
    return isResultNull();
  }

  @Override
  public Object getGroupResult() {
    return calculateResult();
  }

  @Override
  public boolean canRunOptimizedCalculation(int rowNum, Range range) {
    return previousRange != null && range.getStart() == previousRange.getStart()
        && range.getEnd() >= previousRange.getEnd();
  }

  /**
   * Continues the aggregation of the previous range with the rows the current range extends it by.
   */
  @Override
  public Object runOnRange(int rowNum, Range range, VectorPTFGroupBatches batches)
      throws HiveException {
    count = previousCount;
    sum = previousSum;
    variance = previousVariance;

    if (range.getEnd() > previousRange.getEnd()) {
      // don't reset this evaluator after calculation
      batches.runEvaluatorOnRange(this, new Range(previousRange.getEnd(), range.getEnd(), batches),
          false);
    }
    return isGroupResultNull() ? null : getGroupResult();
  }

  @Override
  public void onResultCalculated(Object result, Range range) {
    previousRange = range;
    previousCount = count;
    previousSum = sum;
    previousVariance = variance;
  }

  @Override
  public void onPartitionEnd() {
    previousRange = null;
  }
}
