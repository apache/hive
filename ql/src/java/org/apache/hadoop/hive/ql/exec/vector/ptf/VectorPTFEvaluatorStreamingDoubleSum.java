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
 * This class evaluates double sum() for a PTF group.
 */
public class VectorPTFEvaluatorStreamingDoubleSum extends VectorPTFEvaluatorBase {

  protected boolean isNull;
  protected double sum;

  public VectorPTFEvaluatorStreamingDoubleSum(WindowFrameDef windowFrameDef, VectorExpression inputVecExpr,
      int outputColumnNum) {
    super(windowFrameDef, inputVecExpr, outputColumnNum);
    resetEvaluator();
  }

  @Override
  public void evaluateGroupBatch(VectorizedRowBatch batch)
      throws HiveException {

    evaluateInputExpr(batch);

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

        // We have a repeated value.
        isNull = false;
        final double repeatedValue = doubleColVector.vector[0];

        for (int i = 0; i < size; i++) {
          sum += repeatedValue;

          // Output row i SUM.
          outputVector[i] = sum;
        }
      } else {
        if (isNull) {
          outputColVector.isNull[0] = true;
          outputColVector.noNulls = false;
        } else {

          // Continue previous SUM.
          outputVector[0] = sum;
        }
        outputColVector.isRepeating = true;
      }
    } else if (doubleColVector.noNulls) {
      isNull = false;
      double[] vector = doubleColVector.vector;
      for (int i = 0; i < size; i++) {
        sum += vector[i];

        // Output row i SUM.
        outputVector[i] = sum;
      }
    } else {
      boolean[] batchIsNull = doubleColVector.isNull;
      int i = 0;
      while (batchIsNull[i]) {
        if (isNull) {
          outputColVector.isNull[i] = true;
          outputColVector.noNulls = false;
        } else {

          // Continue previous SUM.
          outputVector[i] = sum;
        }
        if (++i >= size) {
          return;
        }
      }

      isNull = false;
      double[] vector = doubleColVector.vector;

      sum += vector[i];

      // Output row i sum.
      outputVector[i++] = sum;

      for (; i < size; i++) {
        if (!batchIsNull[i]) {
          sum += vector[i];

          // Output row i sum.
          outputVector[i] = sum;
        } else {

          // Continue previous SUM.
          outputVector[i] = sum;
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
    sum = 0.0;
  }
}