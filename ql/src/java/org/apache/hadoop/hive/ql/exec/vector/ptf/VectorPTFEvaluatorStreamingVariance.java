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
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import com.google.common.base.Preconditions;

/**
 * This class evaluates the variance family functions for a PTF group in a streaming fashion
 * (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW).
 *
 * Each non-null value is added to the running aggregation and every row gets the running result.
 */
public class VectorPTFEvaluatorStreamingVariance extends VectorPTFEvaluatorAbstractVariance {

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
          iterate(value);
          setOutput(outputColVector, i);
        }
      } else {

        // All nulls, continue the previous result.
        setOutput(outputColVector, 0);
        outputColVector.isRepeating = true;
      }
      return;
    }

    final boolean noNulls = colVector.noNulls;
    final boolean[] isNull = colVector.isNull;
    switch (inputColumnVectorType) {
    case LONG:
      final long[] longVector = ((LongColumnVector) colVector).vector;
      for (int i = 0; i < size; i++) {
        if (noNulls || !isNull[i]) {
          iterate(longVector[i]);
        }
        setOutput(outputColVector, i);
      }
      break;
    case DOUBLE:
      final double[] doubleVector = ((DoubleColumnVector) colVector).vector;
      for (int i = 0; i < size; i++) {
        if (noNulls || !isNull[i]) {
          iterate(doubleVector[i]);
        }
        setOutput(outputColVector, i);
      }
      break;
    case DECIMAL:
      final HiveDecimalWritable[] decimalVector = ((DecimalColumnVector) colVector).vector;
      for (int i = 0; i < size; i++) {
        if (noNulls || !isNull[i]) {
          iterate(decimalVector[i].doubleValue());
        }
        setOutput(outputColVector, i);
      }
      break;
    default:
      throw new RuntimeException("Unexpected column vector type " + inputColumnVectorType);
    }
  }

  private void setOutput(DoubleColumnVector outputColVector, int i) {
    if (isResultNull()) {
      outputColVector.noNulls = false;
      outputColVector.isNull[i] = true;
    } else {
      outputColVector.isNull[i] = false;
      outputColVector.vector[i] = calculateResult();
    }
  }

  @Override
  public boolean streamsResult() {
    // No group value.
    return true;
  }
}
