/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.vector.expressions;


import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * For conditional expressions, the{@code IfExprConditionalFilter} class updated
 * the selected array of batch parameter after the conditional expression is executed.
 * Then the remaining expression will only do the selected rows instead of all.
 */
public class IfExprConditionalFilter extends VectorExpression {
  protected int arg1Column = -1;
  protected int arg2Column = -1;
  protected int arg3Column = -1;
  protected int arg2ColumnTmp = -1;

  public IfExprConditionalFilter(int arg1Column, int arg2Column, int arg3Column,
      int outputColumnNum) {
    super(outputColumnNum);
    this.arg1Column = arg1Column;
    if(arg2Column == -1){
      this.arg2Column = arg3Column;
      this.arg2ColumnTmp = -1;
    } else{
      this.arg2Column = arg2Column;
      this.arg3Column = arg3Column;
      this.arg2ColumnTmp = arg2Column;
    }
  }

  public IfExprConditionalFilter() {
    super();
  }

  /**
   * For If(expr1,expr2,expr3) expression,
   * Firstly, save the previous selected vector, size and selectedInUse value of batch.
   * Secondly evaluate the conditional expression and update the selected array of batch based
   * on the result of conditional expression(1 denote done, 0 denote not done)
   * Then evaluate the expr2 based on the updated selected.
   * After the expr2 is executed, remove the indexes which have done in expr2.
   * Last, evaluate the expr3 based on the updated selected.
   *
   * @param batch
   * @param childExpressions the childExpressions need to be evaluated.
   */
  public void evaluateIfConditionalExpr(VectorizedRowBatch batch, VectorExpression[] childExpressions) {
    if (childExpressions != null) {
      // Save the previous selected vector, size and selectedInUse value of batch.
      int[] prevSelected = new int[batch.selected.length];
      int[] prevSelectedFalse = new int[batch.selected.length];
      int prevSize = batch.size;
      boolean prevSelectInUse = batch.selectedInUse;
      if (!batch.selectedInUse) {
        for (int i = 0; i < batch.size; i++) {
          prevSelected[i] = i;
        }
        System.arraycopy(batch.selected, 0, prevSelectedFalse, 0, batch.selected.length);
        System.arraycopy(prevSelected, 0, batch.selected, 0, batch.size);
      } else {
        System.arraycopy(batch.selected, 0, prevSelected, 0, batch.selected.length);
      }

      // Evaluate the conditional expression.
      evaluateConditionalExpression(batch, childExpressions[0],
        prevSize, prevSelectInUse);
      if (childExpressions != null && childExpressions.length == 2) {
        // If the length is 2, it has two situations:If(expr1,expr2,null) or
        // If(expr1,null,expr3) distinguished by the indexes.
        if (childExpressions[1].getOutputColumnNum() == arg2ColumnTmp) {
          // Evaluate the expr2 expression.
          childExpressions[1].evaluate(batch);
        } else {
          // Update the selected array of batch to remove the index of being done.
          evaluateSelectedArray(batch, arg1Column, prevSelected, prevSize);
          // If(expr1,null,expr3), if the expr1 is false, expr3 will be evaluated.
          childExpressions[1].evaluate(batch);
        }
      } else if (childExpressions != null && childExpressions.length == 3) {
        // IF(expr1,expr2,expr3). expr1,expr2,expr3 are all the expression.
        // Evaluate the expr2 expression.
        childExpressions[1].evaluate(batch);
        // Update the selected array of batch to remove the index of being done.
        evaluateSelectedArray(batch, arg1Column, prevSelected, prevSize);
        // Evaluate the expr3 expression.
        childExpressions[2].evaluate(batch);
      }
      // When evaluate all the expressions, restore the previous selected
      // vector,size and selectedInUse value of batch.
      batch.size = prevSize;
      batch.selectedInUse = prevSelectInUse;
      if(!prevSelectInUse){
        batch.selected = prevSelectedFalse;
      } else{
        batch.selected = prevSelected;
      }
    }
  }


  /**
   * Update the selected array of batch based on the conditional expression
   * result, remove the index of being done.
   *
   * @param batch
   * @param num  the column num of conditional expression in batch cols
   * @param prevSelected the previous selected array
   */
  private static void evaluateSelectedArray(VectorizedRowBatch batch, int num,
                                            int[] prevSelected, int prevSize) {
    // Get the result of conditional expression.
    LongColumnVector outputColVector = (LongColumnVector) batch.cols[num];
    long[] flag = outputColVector.vector;
    int newSize = 0;
    // Update the selected array of batch
    for (int j = 0; j < prevSize; j++) {
      if (flag[prevSelected[j]] == 0) {
        batch.selected[newSize++] = prevSelected[j];
      }
    }
    batch.size = newSize;
    batch.selectedInUse = true;
  }

  /**
   * Evaluate the conditional expression and update the selected array of batch
   * based on the result of conditional expression.
   *
   * @param batch
   * @param ve   the conditional expression need to evaluate
   * @param prevSize the previous batch size
   * @param prevSelectInUse the previous selectInUse
   */
  private static void evaluateConditionalExpression(VectorizedRowBatch batch,
                                                    VectorExpression ve, int prevSize,
                                                    boolean prevSelectInUse) {
    batch.size = prevSize;
    batch.selectedInUse = prevSelectInUse;
    int colNum = ve.getOutputColumnNum();
    // Evaluate the conditional expression.
    ve.evaluate(batch);
    LongColumnVector outputColVector = (LongColumnVector) batch.cols[colNum];
    long[] flag = outputColVector.vector;
    int[] sel = batch.selected;
    int newSize = 0;
    // Update the selected array of the batch based on the conditional expression.
    for (int j = 0; j < batch.size; j++) {
      int k = sel[j];
      if (flag[k] == 1) {
        sel[newSize++] = k;
      }
    }
    if(newSize < batch.size ) {
      batch.size = newSize;
      batch.selectedInUse = true;
    }
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    throw new UnsupportedOperationException("Undefined descriptor");
  }

  @Override
  public String vectorExpressionParameters() {
    return null;
  }
}

