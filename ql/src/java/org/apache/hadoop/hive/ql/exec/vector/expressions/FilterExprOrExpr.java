/**
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

import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * This class represents an Or expression. This applies short circuit optimization.
 */
public class FilterExprOrExpr extends VectorExpression {
  private static final long serialVersionUID = 1L;
  private transient final int[] initialSelected = new int[VectorizedRowBatch.DEFAULT_SIZE];
  private transient int[] unselected = new int[VectorizedRowBatch.DEFAULT_SIZE];
  private transient final int[] tmp = new int[VectorizedRowBatch.DEFAULT_SIZE];

  public FilterExprOrExpr() {
    super();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {
    int n = batch.size;
    if (n <= 0) {
      return;
    }

    VectorExpression childExpr1 = this.childExpressions[0];
    VectorExpression childExpr2 = this.childExpressions[1];

    boolean prevSelectInUse = batch.selectedInUse;

    // Save the original selected vector
    int[] sel = batch.selected;
    if (batch.selectedInUse) {
      System.arraycopy(sel, 0, initialSelected, 0, n);
    } else {
      for (int i = 0; i < n; i++) {
        initialSelected[i] = i;
        sel[i] = i;
      }
      batch.selectedInUse = true;
    }

    childExpr1.evaluate(batch);

    // Preserve the selected reference and size values generated
    // after the first child is evaluated.
    int sizeAfterFirstChild = batch.size;
    int[] selectedAfterFirstChild = batch.selected;

    // Calculate unselected ones in last evaluate.
    for (int j = 0; j < n; j++) {
      tmp[initialSelected[j]] = 0;
    }
    for (int j = 0; j < batch.size; j++) {
      tmp[selectedAfterFirstChild[j]] = 1;
    }
    int unselectedSize = 0;
    for (int j = 0; j < n; j++) {
      int i = initialSelected[j];
      if (tmp[i] == 0) {
        unselected[unselectedSize++] = i;
      }
    }

    // Evaluate second child expression over unselected ones only.
    batch.selected = unselected;
    batch.size = unselectedSize;

    childExpr2.evaluate(batch);

    // Merge the result of last evaluate to previous evaluate.
    int newSize = batch.size + sizeAfterFirstChild;
    for (int i = 0; i < batch.size; i++) {
      tmp[batch.selected[i]] = 1;
    }
    int k = 0;
    for (int j = 0; j < n; j++) {
      int i = initialSelected[j];
      if (tmp[i] == 1) {
        batch.selected[k++] = i;
      }
    }


    batch.size = newSize;
    if (newSize == n) {
      // Filter didn't do anything
      batch.selectedInUse = prevSelectInUse;
    }

    // unselected array is taken away by the row batch
    // so take the row batch's original one.
    unselected = selectedAfterFirstChild;
  }

  @Override
  public int getOutputColumn() {
    return -1;
  }

  @Override
  public String getOutputType() {
    return "boolean";
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.FILTER)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY,
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }
}
