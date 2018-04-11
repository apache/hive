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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * Vectorized instruction to concatenate a scalar to a string column and put
 * the result in an output column.
 */
public class StringScalarConcatStringGroupCol extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private final int colNum;
  private final byte[] value;

  public StringScalarConcatStringGroupCol(byte[] value, int colNum, int outputColumnNum) {
    super(outputColumnNum);
    this.colNum = colNum;
    this.value = value;
  }

  public StringScalarConcatStringGroupCol() {
    super();

    // Dummy final assignments.
    colNum = -1;
    value = null;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
        super.evaluateChildren(batch);
      }

    BytesColumnVector inputColVector = (BytesColumnVector) batch.cols[colNum];
    BytesColumnVector outputColVector = (BytesColumnVector) batch.cols[outputColumnNum];
    int[] sel = batch.selected;
    int n = batch.size;
    byte[][] vector = inputColVector.vector;
    int[] start = inputColVector.start;
    int[] length = inputColVector.length;
    boolean[] inputIsNull = inputColVector.isNull;
    boolean[] outputIsNull = outputColVector.isNull;

    if (n == 0) {

      // Nothing to do
      return;
    }

    // initialize output vector buffer to receive data
    outputColVector.initBuffer();

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    if (inputColVector.isRepeating) {
      if (inputColVector.noNulls || !inputIsNull[0]) {
        // Set isNull before call in case it changes it mind.
        outputIsNull[0] = false;
        outputColVector.setConcat(0, value, 0, value.length, vector[0], start[0], length[0]);
      } else {
        outputIsNull[0] = true;
        outputColVector.noNulls = false;
      }
      outputColVector.isRepeating = true;
      return;
    }

    if (inputColVector.noNulls) {
      if (batch.selectedInUse) {

        // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.

        if (!outputColVector.noNulls) {
          for(int j = 0; j != n; j++) {
           final int i = sel[j];
           // Set isNull before call in case it changes it mind.
           outputIsNull[i] = false;
           outputColVector.setConcat(i, value, 0, value.length, vector[i], start[i], length[i]);
         }
        } else {
          for(int j = 0; j != n; j++) {
            final int i = sel[j];
            outputColVector.setConcat(i, value, 0, value.length, vector[i], start[i], length[i]);
          }
        }
      } else {
        if (!outputColVector.noNulls) {

          // Assume it is almost always a performance win to fill all of isNull so we can
          // safely reset noNulls.
          Arrays.fill(outputIsNull, false);
          outputColVector.noNulls = true;
        }
        for(int i = 0; i != n; i++) {
          outputColVector.setConcat(i, value, 0, value.length, vector[i], start[i], length[i]);
        }
      }
    } else /* there are NULLs in the inputColVector */ {

      // Carefully handle NULLs...

      /*
       * Handle case with nulls. Don't do function if the value is null, to save time,
       * because calling the function can be expensive.
       */
      outputColVector.noNulls = false;

      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          if (!inputColVector.isNull[i]) {
            outputColVector.setConcat(i, value, 0, value.length, vector[i], start[i], length[i]);
          }
          outputColVector.isNull[i] = inputColVector.isNull[i];
        }
      } else {
        for(int i = 0; i != n; i++) {
          if (!inputColVector.isNull[i]) {
            outputColVector.setConcat(i, value, 0, value.length, vector[i], start[i], length[i]);
          }
          outputColVector.isNull[i] = inputColVector.isNull[i];
        }
      }
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return "val " + displayUtf8Bytes(value) + ", " + getColumnParamString(1, colNum);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.STRING,
            VectorExpressionDescriptor.ArgumentType.STRING_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.SCALAR,
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }
}
