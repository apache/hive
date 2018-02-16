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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;

import com.google.common.base.Preconditions;

/**
 * This class evaluates long sum() for a PTF group.
 */
public class VectorPTFEvaluatorLongSum extends VectorPTFEvaluatorBase {

  private static final long serialVersionUID = 1L;
  private static final String CLASS_NAME = VectorPTFEvaluatorLongSum.class.getName();
  private static final Log LOG = LogFactory.getLog(CLASS_NAME);

  protected boolean isGroupResultNull;
  protected long sum;

  public VectorPTFEvaluatorLongSum(WindowFrameDef windowFrameDef, VectorExpression inputVecExpr,
      int outputColumnNum) {
    super(windowFrameDef, inputVecExpr, outputColumnNum);
    resetEvaluator();
  }

  public void evaluateGroupBatch(VectorizedRowBatch batch, boolean isLastGroupBatch) {
    evaluateInputExpr(batch);

    // Sum all non-null long column values; maintain isGroupResultNull.

    // We do not filter when PTF is in reducer.
    Preconditions.checkState(!batch.selectedInUse);

    final int size = batch.size;
    if (size == 0) {
      return;
    }
    LongColumnVector longColVector = ((LongColumnVector) batch.cols[inputColumnNum]);
    if (longColVector.isRepeating) {

      if (longColVector.noNulls || !longColVector.isNull[0]) {
        if (isGroupResultNull) {

          // First aggregation calculation for group.
          sum = longColVector.vector[0] * batch.size;
          isGroupResultNull = false;
        } else {
          sum += longColVector.vector[0] * batch.size;
        }
      }
    } else if (longColVector.noNulls) {
      long[] vector = longColVector.vector;
      long varSum = vector[0];
      for (int i = 1; i < size; i++) {
        varSum += vector[i];
      }
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
      for (; i < size; i++) {
        if (!batchIsNull[i]) {
          varSum += vector[i];
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
  public boolean isGroupResultNull() {
    return isGroupResultNull;
  }

  @Override
  public Type getResultColumnVectorType() {
    return Type.LONG;
  }

  @Override
  public long getLongGroupResult() {
    return sum;
  }

  @Override
  public void resetEvaluator() {
    isGroupResultNull = true;
    sum = 0;
  }
}