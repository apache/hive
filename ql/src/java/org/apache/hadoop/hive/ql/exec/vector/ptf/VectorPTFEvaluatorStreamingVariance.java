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

import com.google.common.base.Preconditions;

/**
 * This class evaluates the variance family functions for a PTF group in a streaming fashion
 * (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW).
 *
 * Each non-null value is added to the running aggregation and every row gets the running result.
 * The result is only recalculated when a value is added, null rows reuse it.
 */
public class VectorPTFEvaluatorStreamingVariance extends VectorPTFEvaluatorAbstractVariance {

  private boolean isCurrentResultNull;
  private double currentResult;

  public VectorPTFEvaluatorStreamingVariance(WindowFrameDef windowFrameDef,
      VectorExpression inputVecExpr, int outputColumnNum, Type inputColumnVectorType,
      VarianceKind varianceKind) {
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
    DoubleColumnVector outputColVector = (DoubleColumnVector) batch.cols[outputColumnNum];

    if (colVector.isRepeating) {
      if (colVector.noNulls || !colVector.isNull[0]) {
        final double value = getDouble(colVector, 0);
        for (int i = 0; i < size; i++) {
          aggregate(value);
          setOutput(outputColVector, i);
        }
      } else {

        // All nulls, continue the previous result.
        setOutput(outputColVector, 0);
        outputColVector.isRepeating = true;
      }
      return;
    }

    switch (inputColumnVectorType) {
    case LONG:
      evaluateLongs((LongColumnVector) colVector, outputColVector, size);
      break;
    case DOUBLE:
      evaluateDoubles((DoubleColumnVector) colVector, outputColVector, size);
      break;
    case DECIMAL:
      evaluateDecimals((DecimalColumnVector) colVector, outputColVector, size);
      break;
    default:
      throw new IllegalStateException("Unexpected column vector type " + inputColumnVectorType);
    }
  }

  private void evaluateLongs(LongColumnVector colVector, DoubleColumnVector outputColVector,
      int size) {
    for (int i = 0; i < size; i++) {
      if (colVector.noNulls || !colVector.isNull[i]) {
        aggregate((double) colVector.vector[i]);
      }
      setOutput(outputColVector, i);
    }
  }

  private void evaluateDoubles(DoubleColumnVector colVector, DoubleColumnVector outputColVector,
      int size) {
    for (int i = 0; i < size; i++) {
      if (colVector.noNulls || !colVector.isNull[i]) {
        aggregate(colVector.vector[i]);
      }
      setOutput(outputColVector, i);
    }
  }

  private void evaluateDecimals(DecimalColumnVector colVector, DoubleColumnVector outputColVector,
      int size) {
    for (int i = 0; i < size; i++) {
      if (colVector.noNulls || !colVector.isNull[i]) {
        aggregate(colVector.vector[i].doubleValue());
      }
      setOutput(outputColVector, i);
    }
  }

  private void aggregate(double value) {
    iterate(value);
    isCurrentResultNull = isResultNull();
    if (!isCurrentResultNull) {
      currentResult = calculateResult();
    }
  }

  private void setOutput(DoubleColumnVector outputColVector, int i) {
    if (isCurrentResultNull) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    } else {
      outputColVector.isNull[i] = false;
      outputColVector.vector[i] = currentResult;
    }
  }

  @Override
  public boolean streamsResult() {
    // No group value.
    return true;
  }

  @Override
  public void resetEvaluator() {
    super.resetEvaluator();
    isCurrentResultNull = true;
    currentResult = 0.0;
  }
}
