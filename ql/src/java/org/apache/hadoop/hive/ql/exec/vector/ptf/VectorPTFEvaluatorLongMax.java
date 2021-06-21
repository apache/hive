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
 * This class evaluates long max() for a PTF group.
 */
public class VectorPTFEvaluatorLongMax extends VectorPTFEvaluatorBase {

  protected boolean isGroupResultNull;
  protected long max;

  public VectorPTFEvaluatorLongMax(WindowFrameDef windowFrameDef, VectorExpression inputVecExpr,
      int outputColumnNum) {
    super(windowFrameDef, inputVecExpr, outputColumnNum);
    resetEvaluator();
  }

  @Override
  public void evaluateGroupBatch(VectorizedRowBatch batch)
      throws HiveException {

    evaluateInputExpr(batch);

    // Determine maximum of all non-null long column values; maintain isGroupResultNull.

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
          max = longColVector.vector[0];
          isGroupResultNull = false;
        } else {
          final long repeatedMax = longColVector.vector[0];
          if (repeatedMax > max) {
            max = repeatedMax;
          }
        }
      }
    } else if (longColVector.noNulls) {
      long[] vector = longColVector.vector;
      long varMax = vector[0];
      for (int i = 1; i < size; i++) {
        final long l = vector[i];
        if (l > varMax) {
          varMax = l;
        }
      }
      if (isGroupResultNull) {
        max = varMax;
        isGroupResultNull = false;
      } else if (varMax > max) {
        max = varMax;
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
      long varMax = vector[i++];
      for (; i < size; i++) {
        if (!batchIsNull[i]) {
          final long l = vector[i];
          if (l > varMax) {
            varMax = l;
          }
        }
      }
      if (isGroupResultNull) {
        max = varMax;
        isGroupResultNull = false;
      } else if (varMax > max) {
        max = varMax;
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
    return isGroupResultNull;
  }

  @Override
  public Type getResultColumnVectorType() {
    return Type.LONG;
  }

  @Override
  public Object getGroupResult() {
    return max;
  }

  @Override
  public void resetEvaluator() {
    isGroupResultNull = true;
    max = Long.MIN_VALUE;
  }
}