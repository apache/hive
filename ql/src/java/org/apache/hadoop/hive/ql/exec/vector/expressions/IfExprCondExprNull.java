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
package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Do conditional execution of the THEN vector expression with NULL ELSE of a SQL IF statement.
 */
public class IfExprCondExprNull extends IfExprCondExprBase {
  private static final long serialVersionUID = 1L;

  public IfExprCondExprNull(int arg1Column, int arg2Column, int outputColumnNum) {
    super(arg1Column, arg2Column, outputColumnNum);
  }

  public IfExprCondExprNull() {
    super();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    int n = batch.size;
    if (n <= 0) {
      // Nothing to do
      return;
    }

    /*
     * Do common analysis of the IF statement boolean expression.
     *
     * The following protected members can be examined afterwards:
     *
     *   boolean isIfStatementResultRepeated
     *   boolean isIfStatementResultThen
     *
     *   int thenSelectedCount
     *   int[] thenSelected
     *   int elseSelectedCount
     *   int[] elseSelected
     */
    super.evaluate(batch);

    ColumnVector outputColVector = batch.cols[outputColumnNum];
    boolean[] outputIsNull = outputColVector.isNull;

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    // CONSIDER: Should be do this for all vector expressions that can
    //           work on BytesColumnVector output columns???
    outputColVector.init();

    ColumnVector thenColVector = batch.cols[inputColumnNum[1]];

    final int thenCount = thenSelectedCount;
    final int elseCount = elseSelectedCount;

    if (isIfStatementResultRepeated) {
      if (isIfStatementResultThen) {
        // Evaluate THEN expression (only) and copy all its results.
        childExpressions[1].evaluate(batch);
        thenColVector.copySelected(batch.selectedInUse, batch.selected, n, outputColVector);
      } else {
        outputIsNull[0] = true;
        outputColVector.noNulls = false;
        outputColVector.isRepeating = true;
      }
      return;
    }

    // NOTE: We cannot use copySelected below since it is a whole column operation.

    conditionalEvaluate(batch, childExpressions[1], thenSelected, thenCount);
    for (int i = 0; i < thenCount; i++) {
      final int batchIndex = thenSelected[i];
      outputIsNull[batchIndex] = false;
      outputColVector.setElement(batchIndex, batchIndex, thenColVector);
    }

    outputColVector.noNulls = false;
    for (int i = 0; i < elseCount; i++) {
      outputColVector.isNull[elseSelected[i]] = true;
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, inputColumnNum[0]) + ", " + getColumnParamString(1, inputColumnNum[1]) +
        ", null";
  }
}
