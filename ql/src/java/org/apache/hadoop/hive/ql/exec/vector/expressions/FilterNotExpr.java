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

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * This class represents an NOT filter expression. This applies short circuit optimization.
 */
public class FilterNotExpr extends VectorExpression {
  private static final long serialVersionUID = 1L;
  private transient final int[] initialSelected = new int[VectorizedRowBatch.DEFAULT_SIZE];
  private transient int[] unselected = new int[VectorizedRowBatch.DEFAULT_SIZE];
  private transient final int[] tmp = new int[VectorizedRowBatch.DEFAULT_SIZE];

  public FilterNotExpr(VectorExpression childExpr1) {
    this();
    this.childExpressions = new VectorExpression[] {childExpr1};
  }

  public FilterNotExpr() {
    super();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {
    int n = batch.size;

    if (n <= 0) {
      return;
    }

    // Clone the selected vector
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

    VectorExpression childExpr1 = this.childExpressions[0];
    childExpr1.evaluate(batch);

    // Calculate unselected ones in last evaluate.
    for (int i = 0; i < n; i++) {
      tmp[initialSelected[i]] = 0;
    }

    // Need to set sel reference again, because the child expression might
    // have invalidated the earlier reference
    sel = batch.selected;
    for (int j = 0; j < batch.size; j++) {
      int i = sel[j];
      tmp[i] = 1;
    }
    int unselectedSize = 0;
    for (int j = 0; j < n; j++) {
      int i = initialSelected[j];
      if (tmp[i] == 0) {
        unselected[unselectedSize++] = i;
      }
    }

    // The unselected is the new selected, swap the arrays
    batch.selected = unselected;
    unselected = sel;
    batch.size = unselectedSize;
  }

  @Override
  public int getOutputColumn() {
    return -1;
  }

  @Override
  public String getOutputType() {
    return "boolean";
  }
}
