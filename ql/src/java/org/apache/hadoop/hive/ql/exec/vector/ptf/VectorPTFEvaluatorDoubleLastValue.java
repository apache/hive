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
 * This class evaluates double first_value() for a PTF group.
 * We capture the last value from the last batch.  It can be NULL.
 * It becomes the group value.
 */
public class VectorPTFEvaluatorDoubleLastValue extends VectorPTFEvaluatorBase {

  protected boolean isGroupResultNull;
  protected double lastValue;

  public VectorPTFEvaluatorDoubleLastValue(WindowFrameDef windowFrameDef,
    VectorExpression inputVecExpr, int outputColumnNum) {
    super(windowFrameDef, inputVecExpr, outputColumnNum);
    resetEvaluator();
  }

  @Override
  public void evaluateGroupBatch(VectorizedRowBatch batch)
    throws HiveException {

    evaluateInputExpr(batch);

    // Last row of last batch determines isGroupResultNull and double lastValue.

    // We do not filter when PTF is in reducer.
    Preconditions.checkState(!batch.selectedInUse);

    final int size = batch.size;
    if (size == 0) {
      return;
    }
    DoubleColumnVector doubleColVector = ((DoubleColumnVector) batch.cols[inputColumnNum]);
    if (doubleColVector.isRepeating) {

      if (doubleColVector.noNulls || !doubleColVector.isNull[0]) {
        lastValue = doubleColVector.vector[0];
        isGroupResultNull = false;
      } else {
        isGroupResultNull = true;
      }
    } else if (doubleColVector.noNulls) {
      lastValue = doubleColVector.vector[size - 1];
      isGroupResultNull = false;
    } else {
      final int lastBatchIndex = size - 1;
      if (!doubleColVector.isNull[lastBatchIndex]) {
        lastValue = doubleColVector.vector[lastBatchIndex];
        isGroupResultNull = false;
      } else {
        isGroupResultNull = true;
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
    return Type.DOUBLE;
  }

  @Override
  public double getDoubleGroupResult() {
    return lastValue;
  }

  @Override
  public void resetEvaluator() {
    isGroupResultNull = true;
    lastValue = 0.0;
  }
}