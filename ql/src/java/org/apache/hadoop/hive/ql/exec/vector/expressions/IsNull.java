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
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * This expression evaluates to true if the given input columns is null.
 * The boolean output is stored in the specified output column.
 */
public class IsNull extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private final int colNum;

  public IsNull(int colNum, int outputColumnNum) {
    super(outputColumnNum);
    this.colNum = colNum;
  }

  public IsNull() {
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
    boolean[] nullPos = inputColVector.isNull;
    int n = batch.size;
    long[] outputVector = ((LongColumnVector) batch.cols[outputColumnNum]).vector;
    if (n <= 0) {
      // Nothing to do, this is EOF
      return;
    }

    // output never has nulls for this operator
    batch.cols[outputColumnNum].noNulls = true;
    if (inputColVector.noNulls) {
      outputVector[0] = 0;
      batch.cols[outputColumnNum].isRepeating = true;
    } else if (inputColVector.isRepeating) {
      outputVector[0] = nullPos[0] ? 1 : 0;
      batch.cols[outputColumnNum].isRepeating = true;
    } else {
      if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          outputVector[i] = nullPos[i] ? 1 : 0;
        }
      } else {
        for (int i = 0; i != n; i++) {
          outputVector[i] = nullPos[i] ? 1 : 0;
        }
      }
      batch.cols[outputColumnNum].isRepeating = false;
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, colNum);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(1)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.ALL_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }
}
