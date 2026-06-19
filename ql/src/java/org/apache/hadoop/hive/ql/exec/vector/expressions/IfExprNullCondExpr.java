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
 * Do conditional execution of a NULL THEN and a ELSE vector expression of a SQL IF statement.
 */
public class IfExprNullCondExpr extends IfExprCondExprBase {
  private static final long serialVersionUID = 1L;

  public IfExprNullCondExpr(int arg1Column, int arg3Column, int outputColumnNum) {
    super(arg1Column, arg3Column, outputColumnNum);
  }

  public IfExprNullCondExpr() {
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

    ColumnVector elseColVector = batch.cols[inputColumnNum[1]];

    final int thenCount = thenSelectedCount;
    final int elseCount = elseSelectedCount;

    if (isIfStatementResultRepeated) {
      if (isIfStatementResultThen) {
        outputIsNull[0] = true;
        outputColVector.noNulls = false;
        outputColVector.isRepeating = true;
      } else {
        // Evaluate ELSE expression (only) and copy all its results.
        // Second input parameter but 3rd column.
        childExpressions[1].evaluate(batch);
        elseColVector.copySelected(batch.selectedInUse, batch.selected, n, outputColVector);
      }
      return;
    }

    // NOTE: We cannot use copySelected below since it is a whole column operation.

    outputColVector.noNulls = false;
    for (int i = 0; i < thenCount; i++) {
      outputColVector.isNull[thenSelected[i]] = true;
    }

    // Second input parameter but 3rd column.
    conditionalEvaluate(batch, childExpressions[1], elseSelected, elseCount);
    for (int i = 0; i < elseCount; i++) {
      final int batchIndex = elseSelected[i];
      outputIsNull[batchIndex] = false;
      outputColVector.setElement(batchIndex, batchIndex, elseColVector);
    }
  }

  @Override
  public String vectorExpressionParameters() {
    // Second input parameter but 3rd column.
    return getColumnParamString(0, inputColumnNum[0]) + ", null, " + getColumnParamString(2, inputColumnNum[1]);
  }
}
