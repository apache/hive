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
  VectorExpression childExpr1;
  int [] tmpSelect1 = new int[VectorizedRowBatch.DEFAULT_SIZE];
  int [] unselected = new int[VectorizedRowBatch.DEFAULT_SIZE];
  int [] tmp = new int[VectorizedRowBatch.DEFAULT_SIZE];

  public FilterNotExpr(VectorExpression childExpr1) {
    this.childExpr1 = childExpr1;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {
    int n = batch.size;

    if (n<=0) {
      return;
    }

    //Clone the selected vector
    int [] sel = batch.selected;
    if (batch.selectedInUse) {
      for (int i = 0; i < n; i++) {
        tmpSelect1[i] = sel[i];
      }
    } else {
      for (int i = 0; i < n; i++) {
        tmpSelect1[i] = i;
        sel[i] = i;
      }
      batch.selectedInUse = true;
    }

    childExpr1.evaluate(batch);

    //Calculate unselected ones in last evaluate.
    for (int i = 0; i < tmp.length; i++) {
      tmp[i] = 0;
    }
    for (int j = 0; j < batch.size; j++) {
      int i = sel[j];
      tmp[i] = 1;
    }
    int unselectedSize = 0;
    for (int j =0; j < n; j++) {
      int i = tmpSelect1[j];
      if (tmp[i] == 0) {
        unselected[unselectedSize++] = i;
      }
    }

    //The unselected is the new selected
    batch.selected = unselected;
    batch.size =  unselectedSize;
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
