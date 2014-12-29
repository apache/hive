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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * Vectorized instruction to concatenate a string column to a scalar and put
 * the result in an output column.
 */
public class StringGroupColConcatStringScalar extends VectorExpression {
  private static final long serialVersionUID = 1L;
  private int colNum;
  private int outputColumn;
  private byte[] value;

  public StringGroupColConcatStringScalar(int colNum, byte[] value, int outputColumn) {
    this();
    this.colNum = colNum;
    this.outputColumn = outputColumn;
    this.value = value;
  }

  public StringGroupColConcatStringScalar() {
    super();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    BytesColumnVector inputColVector = (BytesColumnVector) batch.cols[colNum];
    BytesColumnVector outV = (BytesColumnVector) batch.cols[outputColumn];
    int[] sel = batch.selected;
    int n = batch.size;
    byte[][] vector = inputColVector.vector;
    int[] start = inputColVector.start;
    int[] length = inputColVector.length;

    if (n == 0) {

      // Nothing to do
      return;
    }

    // initialize output vector buffer to receive data
    outV.initBuffer();

    if (inputColVector.noNulls) {
      outV.noNulls = true;
      if (inputColVector.isRepeating) {
        outV.isRepeating = true;
        outV.setConcat(0, vector[0], start[0], length[0], value, 0, value.length);
      } else if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outV.setConcat(i, vector[i], start[i], length[i], value, 0, value.length);
        }
        outV.isRepeating = false;
      } else {
        for(int i = 0; i != n; i++) {
          outV.setConcat(i, vector[i], start[i], length[i], value, 0, value.length);
        }
        outV.isRepeating = false;
      }
    } else {

      /*
       * Handle case with nulls. Don't do function if the value is null, to save time,
       * because calling the function can be expensive.
       */
      outV.noNulls = false;
      if (inputColVector.isRepeating) {
        outV.isRepeating = true;
        outV.isNull[0] = inputColVector.isNull[0];
        if (!inputColVector.isNull[0]) {
          outV.setConcat(0, vector[0], start[0], length[0], value, 0, value.length);
        }
      } else if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          if (!inputColVector.isNull[i]) {
            outV.setConcat(i, vector[i], start[i], length[i], value, 0, value.length);
          }
          outV.isNull[i] = inputColVector.isNull[i];
        }
        outV.isRepeating = false;
      } else {
        for(int i = 0; i != n; i++) {
          if (!inputColVector.isNull[i]) {
            outV.setConcat(i, vector[i], start[i], length[i], value, 0, value.length);
          }
          outV.isNull[i] = inputColVector.isNull[i];
        }
        outV.isRepeating = false;
      }
    }
  }

  @Override
  public int getOutputColumn() {
    return outputColumn;
  }

  @Override
  public String getOutputType() {
    return "String_Family";
  }

  public int getColNum() {
    return colNum;
  }

  public void setColNum(int colNum) {
    this.colNum = colNum;
  }

  public byte[] getValue() {
    return value;
  }

  public void setValue(byte[] value) {
    this.value = value;
  }

  public void setOutputColumn(int outputColumn) {
    this.outputColumn = outputColumn;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.STRING_FAMILY,
            VectorExpressionDescriptor.ArgumentType.STRING)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR).build();
  }
}
