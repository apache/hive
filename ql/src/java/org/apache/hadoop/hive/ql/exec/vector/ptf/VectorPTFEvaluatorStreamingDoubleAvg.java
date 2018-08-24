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
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import com.google.common.base.Preconditions;

/**
 * This class evaluates double avg() for a PTF group.
 *
 * Sum up non-null column values; group result is sum / non-null count.
 */
public class VectorPTFEvaluatorStreamingDoubleAvg extends VectorPTFEvaluatorBase {

  protected boolean isNull;
  protected double sum;
  private int nonNullGroupCount;
  protected double avg;

  public VectorPTFEvaluatorStreamingDoubleAvg(WindowFrameDef windowFrameDef, VectorExpression inputVecExpr,
      int outputColumnNum) {
    super(windowFrameDef, inputVecExpr, outputColumnNum);
    resetEvaluator();
  }

  @Override
  public void evaluateGroupBatch(VectorizedRowBatch batch)
      throws HiveException {

    evaluateInputExpr(batch);

    // Sum all non-null double column values for avg; maintain isGroupResultNull; after last row of
    // last group batch compute the group avg when sum is non-null.

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
          nonNullGroupCount++;

          avg = sum / nonNullGroupCount;

          // Output row i AVG.
          outputVector[i] = avg;
        }
      } else {
        if (isNull) {
          outputColVector.isNull[0] = true;
          outputColVector.noNulls = false;
        } else {

          // Continue previous AVG.
          outputVector[0] = avg;
        }
        outputColVector.isRepeating = true;
      }
    } else if (doubleColVector.noNulls) {
      isNull = false;
      double[] vector = doubleColVector.vector;
      for (int i = 0; i < size; i++) {
        sum += vector[i];
        nonNullGroupCount++;

        avg = sum / nonNullGroupCount;

        // Output row i AVG.
        outputVector[i] = avg;
      }
    } else {
      boolean[] batchIsNull = doubleColVector.isNull;
      int i = 0;
      while (batchIsNull[i]) {
        if (isNull) {
          outputColVector.isNull[i] = true;
          outputColVector.noNulls = false;
        } else {

          // Continue previous AVG.
          outputVector[i] = avg;
        }
        if (++i >= size) {
          return;
        }
      }

      isNull = false;
      double[] vector = doubleColVector.vector;

      sum += vector[i];
      nonNullGroupCount++;

      avg = sum / nonNullGroupCount;

      // Output row i AVG.
      outputVector[i++] = avg;

      for (; i < size; i++) {
        if (!batchIsNull[i]) {
          sum += vector[i];
          nonNullGroupCount++;

          avg = sum / nonNullGroupCount;

          // Output row i average.
          outputVector[i] = avg;
        } else {

          // Continue previous AVG.
          outputVector[i] = avg;
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
    nonNullGroupCount = 0;
    avg = 0.0;
  }
}