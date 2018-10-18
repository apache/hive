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
 * This class evaluates double avg() for a PTF group.
 *
 * Sum up non-null column values; group result is sum / non-null count.
 */
public class VectorPTFEvaluatorDoubleAvg extends VectorPTFEvaluatorBase {

  protected boolean isGroupResultNull;
  protected double sum;
  private int nonNullGroupCount;
  private double avg;

  public VectorPTFEvaluatorDoubleAvg(WindowFrameDef windowFrameDef, VectorExpression inputVecExpr,
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
    if (doubleColVector.isRepeating) {

      if (doubleColVector.noNulls || !doubleColVector.isNull[0]) {

        // We have a repeated value.  The sum increases by value * batch.size.
        if (isGroupResultNull) {

          // First aggregation calculation for group.
          sum = doubleColVector.vector[0] * batch.size;
          isGroupResultNull = false;
        } else {
          sum += doubleColVector.vector[0] * batch.size;
        }
        nonNullGroupCount += size;
      }
    } else if (doubleColVector.noNulls) {
      double[] vector = doubleColVector.vector;
      double varSum = vector[0];
      for (int i = 1; i < size; i++) {
        varSum += vector[i];
      }
      nonNullGroupCount += size;
      if (isGroupResultNull) {

        // First aggregation calculation for group.
        sum = varSum;
        isGroupResultNull = false;
      } else {
        sum += varSum;
      }
    } else {
      boolean[] batchIsNull = doubleColVector.isNull;
      int i = 0;
      while (batchIsNull[i]) {
        if (++i >= size) {
          return;
        }
      }
      double[] vector = doubleColVector.vector;
      double varSum = vector[i++];
      nonNullGroupCount++;
      for (; i < size; i++) {
        if (!batchIsNull[i]) {
          varSum += vector[i];
          nonNullGroupCount++;
        }
      }
      if (isGroupResultNull) {

        // First aggregation calculation for group.
        sum = varSum;
        isGroupResultNull = false;
      } else {
        sum += varSum;
      }
    }
  }

  @Override
  public void doLastBatchWork() {
    if (!isGroupResultNull) {
      avg = sum / nonNullGroupCount;
    }
  }

  @Override
  public boolean streamsResult() {
    // We must evaluate whole group before producing a result.
    return false;
  }

  @Override
  public boolean isGroupResultNull() {
    return isGroupResultNull;
  }

  @Override
  public Type getResultColumnVectorType() {
    return Type.DOUBLE;
  }

  @Override
  public double getDoubleGroupResult() {
    return avg;
  }

  @Override
  public void resetEvaluator() {
    isGroupResultNull = true;
    sum = 0.0;
    nonNullGroupCount = 0;
    avg = 0.0;
  }
}