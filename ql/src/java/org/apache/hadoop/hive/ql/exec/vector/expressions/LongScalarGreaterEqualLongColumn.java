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

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;

public class LongScalarGreaterEqualLongColumn extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private final int colNum;
  private final long value;

  public LongScalarGreaterEqualLongColumn(long value, int colNum, int outputColumnNum) {
    super(outputColumnNum);
    this.colNum = colNum;
    this.value = value;
  }

  public LongScalarGreaterEqualLongColumn() {
    super();

    // Dummy final assignments.
    colNum = -1;
    value = 0;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector inputColVector = (LongColumnVector) batch.cols[colNum];
    LongColumnVector outputColVector = (LongColumnVector) batch.cols[outputColumnNum];
    int[] sel = batch.selected;
    boolean[] nullPos = inputColVector.isNull;
    boolean[] outNulls = outputColVector.isNull;
    int n = batch.size;
    long[] vector = inputColVector.vector;
    long[] outputVector = outputColVector.vector;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    outputColVector.isRepeating = false;
    outputColVector.noNulls = inputColVector.noNulls;
    if (inputColVector.noNulls) {
      if (inputColVector.isRepeating) {
        //All must be selected otherwise size would be zero
        //Repeating property will not change.
        outputVector[0] = value >= vector[0] ? 1 : 0;
        outputColVector.isRepeating = true;
      } else if (batch.selectedInUse) {
        for(int j=0; j != n; j++) {
          int i = sel[j];
          outputVector[i] = value >= vector[i] ? 1 : 0;
        }
      } else {
        for(int i = 0; i != n; i++) {
          // The SIMD optimized form of "a >= b" is "((a - b) >>> 63) ^ 1"
          outputVector[i] = ((value - vector[i]) >>> 63) ^ 1;
        }
      }
    } else {
      if (inputColVector.isRepeating) {
        //All must be selected otherwise size would be zero
        //Repeating property will not change.
        if (!nullPos[0]) {
          outputVector[0] = value >= vector[0] ? 1 : 0;
          outNulls[0] = false;
        } else {
          outNulls[0] = true;
        }
        outputColVector.isRepeating = true;
      } else if (batch.selectedInUse) {
        for(int j=0; j != n; j++) {
          int i = sel[j];
          outputVector[i] = value >= vector[i] ? 1 : 0;
          outNulls[i] = nullPos[i];
        }
      } else {
        System.arraycopy(nullPos, 0, outNulls, 0, n);
        for(int i = 0; i != n; i++) {
          outputVector[i] = ((value - vector[i]) >>> 63) ^ 1;
        }
      }
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return "val " + value + ", " + getColumnParamString(1, colNum);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.getType("long"),
            VectorExpressionDescriptor.ArgumentType.getType("long"))
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.SCALAR,
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }
}
