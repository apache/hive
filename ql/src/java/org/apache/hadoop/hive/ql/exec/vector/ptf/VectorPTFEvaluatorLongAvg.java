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
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;

import com.google.common.base.Preconditions;

/**
 * This class evaluates long avg() for a PTF group.
 *
 * Sum up non-null column values; group result is sum / non-null count.
 */
public class VectorPTFEvaluatorLongAvg extends VectorPTFEvaluatorAbstractAvg<Long> {

  protected double avg;

  public VectorPTFEvaluatorLongAvg(WindowFrameDef windowFrameDef, VectorExpression inputVecExpr,
      int outputColumnNum) {
    super(windowFrameDef, inputVecExpr, outputColumnNum);
    resetEvaluator();
  }

  @Override
  public void evaluateGroupBatch(VectorizedRowBatch batch)
      throws HiveException {

    evaluateInputExpr(batch);

    // Sum all non-null long column values for avg; maintain isGroupResultNull; after last row of
    // last group batch compute the group avg when sum is non-null.

    // We do not filter when PTF is in reducer.
    Preconditions.checkState(!batch.selectedInUse);

    final int size = batch.size;
    if (size == 0) {
      return;
    }
    LongColumnVector longColVector = ((LongColumnVector) batch.cols[inputColumnNum]);
    if (longColVector.isRepeating) {

      if (longColVector.noNulls || !longColVector.isNull[0]) {

        // We have a repeated value.  The sum increases by value * batch.size.
        if (isGroupResultNull) {

          // First aggregation calculation for group.
          sum = longColVector.vector[0] * batch.size;
          isGroupResultNull = false;
        } else {
          sum += longColVector.vector[0] * batch.size;
        }
        nonNullGroupCount += size;
      }
    } else if (longColVector.noNulls) {
      long[] vector = longColVector.vector;
      long varSum = vector[0];
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
      boolean[] batchIsNull = longColVector.isNull;
      int i = 0;
      while (batchIsNull[i]) {
        if (++i >= size) {
          return;
        }
      }
      long[] vector = longColVector.vector;
      long varSum = vector[i++];
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
      avg = ((double) sum) / nonNullGroupCount;
    }
  }

  @Override
  public Type getResultColumnVectorType() {
    return Type.DOUBLE;
  }

  @Override
  public Object getGroupResult() {
    doLastBatchWork(); // make sure we have a fresh avg
    return avg;
  }

  @Override
  protected Long computeValue(Long number) {
    return VectorPTFEvaluatorHelper.computeValue(number);
  }

  @Override
  protected Long plus(Long number1, Long number2) {
    return VectorPTFEvaluatorHelper.plus(number1, number2);
  }

  @Override
  protected Long minus(Long number1, Long number2) {
    return VectorPTFEvaluatorHelper.minus(number1, number2);
  }

  @Override
  protected Double divide(Long number, long divisor) {
    return VectorPTFEvaluatorHelper.divide(number, divisor);
  }

  @Override
  public void resetEvaluator() {
    isGroupResultNull = true;
    sum = 0L;
    nonNullGroupCount = 0;
    avg = 0.0;
  }
}