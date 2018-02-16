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

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * This expression evaluates to true if the given input columns is not null.
 * The boolean output is stored in the specified output column.
 */
public class IsNotNull extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private final int colNum;

  public IsNotNull(int colNum, int outputColumnNum) {
    super(outputColumnNum);
    this.colNum = colNum;
  }

  public IsNotNull() {
    super();

    // Dummy final assignments.
    colNum = -1;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    ColumnVector inputColVector = batch.cols[colNum];
    int[] sel = batch.selected;
    boolean[] inputIsNull = inputColVector.isNull;
    int n = batch.size;
    LongColumnVector outputColVector = (LongColumnVector) batch.cols[outputColumnNum];
    long[] outputVector = outputColVector.vector;
    boolean[] outputIsNull = outputColVector.isNull;

    if (n <= 0) {
      // Nothing to do
      return;
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

   if (inputColVector.noNulls) {
      outputColVector.isRepeating = true;
      outputIsNull[0] = false;
      outputVector[0] = 1;
    } else if (inputColVector.isRepeating) {
      outputColVector.isRepeating = true;
      outputIsNull[0] = false;
      outputVector[0] = inputIsNull[0] ? 0 : 1;
    } else {

      /*
       * Since we have a result for all rows, we don't need to do conditional NULL maintenance or
       * turn off noNulls..
       */

      if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          outputIsNull[i] = false;
          outputVector[i] = inputIsNull[i] ? 0 : 1;
        }
      } else {
        Arrays.fill(outputIsNull, 0, n, false);
        for (int i = 0; i != n; i++) {
          outputVector[i] = inputIsNull[i] ? 0 : 1;
        }
      }
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, colNum);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(1)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.ALL_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }
}
