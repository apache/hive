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

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * This expression selects a row if the given boolean column is true.
 */
public class SelectColumnIsTrue extends VectorExpression {
  private static final long serialVersionUID = 1L;
  private int colNum1;

  public SelectColumnIsTrue(int colNum1) {
    this();
    this.colNum1 = colNum1;
  }

  public SelectColumnIsTrue() {
    super();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {
    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }
    LongColumnVector inputColVector1 = (LongColumnVector) batch.cols[colNum1];
    int[] sel = batch.selected;
    int n = batch.size;
    long[] vector1 = inputColVector1.vector;
    boolean[] nullVector = inputColVector1.isNull;

    if (n <= 0) {
      // Nothing to do
      return;
    }

    if (inputColVector1.noNulls) {
      if (inputColVector1.isRepeating) {
        if (vector1[0] == 0) {
          // All are filtered out
          batch.size = 0;
          return;
        } else {
          // All are selected
          return;
        }
      } else if (batch.selectedInUse) {
        int newSize = 0;
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          if (vector1[i] == 1) {
            sel[newSize++] = i;
          }
        }
        batch.size = newSize;
      } else {
        int newSize = 0;
        for (int i = 0; i != n; i++) {
          if (vector1[i] == 1) {
            sel[newSize++] = i;
          }
        }
        if (newSize < n) {
          batch.selectedInUse = true;
          batch.size = newSize;
        }
      }
    } else {
      if (inputColVector1.isRepeating) {
        if (nullVector[0] || (vector1[0] == 0)) {
          // All are filtered
          batch.size = 0;
          return;
        } else {
          // All are selected
          return;
        }
      } else if (batch.selectedInUse) {
        int newSize = 0;
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          if (vector1[i] == 1 && !nullVector[i]) {
            sel[newSize++] = i;
          }
        }
        batch.size = newSize;
      } else {
        int newSize = 0;
        for (int i = 0; i != n; i++) {
          if (vector1[i] == 1 && !nullVector[i]) {
            sel[newSize++] = i;
          }
        }
        if (newSize < n) {
          batch.selectedInUse = true;
          batch.size = newSize;
        }
      }
    }
  }

  @Override
  public int getOutputColumn() {
    return -1;
  }

  @Override
  public String getOutputType() {
    return "boolean";
  }

  public int getColNum1() {
    return colNum1;
  }

  public void setColNum1(int colNum1) {
    this.colNum1 = colNum1;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.FILTER)
        .setNumArguments(1)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.LONG)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }
}
