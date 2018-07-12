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
 * This class evaluates double max() for a PTF group.
 */
public class VectorPTFEvaluatorDoubleMax extends VectorPTFEvaluatorBase {

  protected boolean isGroupResultNull;
  protected double max;

  public VectorPTFEvaluatorDoubleMax(WindowFrameDef windowFrameDef, VectorExpression inputVecExpr,
      int outputColumnNum) {
    super(windowFrameDef, inputVecExpr, outputColumnNum);
    resetEvaluator();
  }

  public void evaluateGroupBatch(VectorizedRowBatch batch, boolean isLastGroupBatch)
      throws HiveException {

    evaluateInputExpr(batch);

    // Determine maximum of all non-null double column values; maintain isGroupResultNull.

    // We do not filter when PTF is in reducer.
    Preconditions.checkState(!batch.selectedInUse);

    final int size = batch.size;
    if (size == 0) {
      return;
    }
    DoubleColumnVector doubleColVector = ((DoubleColumnVector) batch.cols[inputColumnNum]);
    if (doubleColVector.isRepeating) {

      if (doubleColVector.noNulls || !doubleColVector.isNull[0]) {
        if (isGroupResultNull) {
          max = doubleColVector.vector[0];
          isGroupResultNull = false;
        } else {
          final double repeatedMax = doubleColVector.vector[0];
          if (repeatedMax < max) {
            max = repeatedMax;
          }
        }
      }
    } else if (doubleColVector.noNulls) {
      double[] vector = doubleColVector.vector;
      double varMax = vector[0];
      for (int i = 1; i < size; i++) {
        final double d = vector[i];
        if (d > varMax) {
          varMax = d;
        }
      }
      if (isGroupResultNull) {
        max = varMax;
        isGroupResultNull = false;
      } else if (varMax > max) {
        max = varMax;
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
      double varMax = vector[i++];
      for (; i < size; i++) {
        if (!batchIsNull[i]) {
          final double d = vector[i];
          if (d > varMax) {
            varMax = d;
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
  public boolean isGroupResultNull() {
    return isGroupResultNull;
  }

  @Override
  public Type getResultColumnVectorType() {
    return Type.DOUBLE;
  }

  @Override
  public double getDoubleGroupResult() {
    return max;
  }

  @Override
  public void resetEvaluator() {
    isGroupResultNull = true;
    max = Double.MIN_VALUE;
  }
}