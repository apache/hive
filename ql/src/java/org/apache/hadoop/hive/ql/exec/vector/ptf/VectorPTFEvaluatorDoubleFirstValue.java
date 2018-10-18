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
 *
 * We capture the first value from the first batch.  It can be NULL.
 * We then set (stream) the output column with that value as repeated in each batch.
 */
public class VectorPTFEvaluatorDoubleFirstValue extends VectorPTFEvaluatorBase {

  protected boolean haveFirstValue;
  protected boolean isGroupResultNull;
  protected double firstValue;

  public VectorPTFEvaluatorDoubleFirstValue(WindowFrameDef windowFrameDef,
      VectorExpression inputVecExpr, int outputColumnNum) {
    super(windowFrameDef, inputVecExpr, outputColumnNum);
    resetEvaluator();
  }

  @Override
  public void evaluateGroupBatch(VectorizedRowBatch batch)
      throws HiveException {

    evaluateInputExpr(batch);

    // First row determines isGroupResultNull and double firstValue; stream fill result as repeated.

    // We do not filter when PTF is in reducer.
    Preconditions.checkState(!batch.selectedInUse);

    if (!haveFirstValue) {
      final int size = batch.size;
      if (size == 0) {
        return;
      }
      DoubleColumnVector doubleColVector = ((DoubleColumnVector) batch.cols[inputColumnNum]);
      if (doubleColVector.isRepeating) {

        if (doubleColVector.noNulls || !doubleColVector.isNull[0]) {
          firstValue = doubleColVector.vector[0];
          isGroupResultNull = false;
        }
      } else if (doubleColVector.noNulls) {
        firstValue = doubleColVector.vector[0];
        isGroupResultNull = false;
      } else {
        if (!doubleColVector.isNull[0]) {
          firstValue = doubleColVector.vector[0];
          isGroupResultNull = false;
        }
      }
      haveFirstValue = true;
    }

    /*
     * Do careful maintenance of the outputColVector.noNulls flag.
     */

    // First value is repeated for all batches.
    DoubleColumnVector outputColVector = (DoubleColumnVector) batch.cols[outputColumnNum];
    outputColVector.isRepeating = true;
    if (isGroupResultNull) {
      outputColVector.noNulls = false;
      outputColVector.isNull[0] = true;
    } else {
      outputColVector.isNull[0] = false;
      outputColVector.vector[0] = firstValue;
    }
  }

  @Override
  public boolean streamsResult() {
    return true;
  }

  @Override
  public Type getResultColumnVectorType() {
    return Type.DOUBLE;
  }

  @Override
  public void resetEvaluator() {
    haveFirstValue = false;
    isGroupResultNull = true;
    firstValue = 0.0;
  }
}