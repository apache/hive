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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;

import com.google.common.base.Preconditions;

/**
 * This class evaluates double min() for a PTF group.
 */
public class VectorPTFEvaluatorStreamingDoubleMin extends VectorPTFEvaluatorBase {

  protected boolean isNull;
  protected double min;

  public VectorPTFEvaluatorStreamingDoubleMin(WindowFrameDef windowFrameDef, VectorExpression inputVecExpr,
      int outputColumnNum) {
    super(windowFrameDef, inputVecExpr, outputColumnNum);
    resetEvaluator();
  }

  @Override
  public void evaluateGroupBatch(VectorizedRowBatch batch)
      throws HiveException {

    evaluateInputExpr(batch);

    // Determine minimum of all non-null double column values; maintain isNull.

    // We do not filter when PTF is in reducer.
    Preconditions.checkState(!batch.selectedInUse);

    final int size = batch.size;
    if (size == 0) {
      return;
    }
    DoubleColumnVector doubleColVector = ((DoubleColumnVector) batch.cols[inputColumnNum]);

    DoubleColumnVector outputColVector = (DoubleColumnVector) batch.cols[outputColumnNum];
    double[] outputVector = outputColVector.vector;

    if (doubleColVector.isRepeating) {

      if (doubleColVector.noNulls || !doubleColVector.isNull[0]) {

        // We have a repeated value but we only need to evaluate once for MIN/MAX.
        final double repeatedMin = doubleColVector.vector[0];

        if (isNull) {
          min = repeatedMin;
          isNull = false;
        } else if (repeatedMin < min) {
          min = repeatedMin;
        }
        outputVector[0] = min;
      } else if (isNull) {
        outputColVector.isNull[0] = true;
        outputColVector.noNulls = false;
      } else {

        // Continue previous MIN.
        outputVector[0] = min;
      }
      outputColVector.isRepeating = true;
    } else if (doubleColVector.noNulls) {
      double[] vector = doubleColVector.vector;
      for (int i = 0; i < size; i++) {
        final double value = vector[i];
        if (isNull) {
          min = value;
          isNull = false;
        } else if (value < min) {
          min = value;
        }
        outputVector[i] = min;
      }
    } else {
      boolean[] batchIsNull = doubleColVector.isNull;
      int i = 0;
      while (batchIsNull[i]) {
        if (isNull) {
          outputColVector.isNull[i] = true;
          outputColVector.noNulls = false;
        } else {

          // Continue previous MIN.
          outputVector[i] = min;
        }
        if (++i >= size) {
          return;
        }
      }

      double[] vector = doubleColVector.vector;

      final double firstValue = vector[i];
      if (isNull) {
        min = firstValue;
        isNull = false;
      } else if (firstValue < min) {
        min = firstValue;
      }

      // Output row i min.
      outputVector[i++] = min;

      for (; i < size; i++) {
        if (!batchIsNull[i]) {
          final double value = vector[i];
          if (isNull) {
            min = value;
            isNull = false;
          } else if (value < min) {
            min = value;
          }

          // Output row i min.
          outputVector[i] = min;
        } else {

          // Continue previous MIN.
          outputVector[i] = min;
        }
      }
    }
  }

  @Override
  public boolean streamsResult() {
    // No group value.
    return true;
  }

  @Override
  public Type getResultColumnVectorType() {
    return Type.DOUBLE;
  }

  @Override
  public void resetEvaluator() {
    isNull = true;
    min = 0.0;
  }
}