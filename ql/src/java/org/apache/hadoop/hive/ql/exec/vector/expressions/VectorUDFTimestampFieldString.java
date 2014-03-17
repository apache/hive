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
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import java.text.ParseException;

/**
 * Abstract class to return various fields from a String.
 */
public abstract class VectorUDFTimestampFieldString extends VectorExpression {

  private static final long serialVersionUID = 1L;

  protected int colNum;
  protected int outputColumn;
  protected final int fieldStart;
  protected final int fieldLength;
  private static final String patternMin = "0000-00-00 00:00:00.000000000";
  private static final String patternMax = "9999-19-99 29:59:59.999999999";

  public VectorUDFTimestampFieldString(int colNum, int outputColumn, int fieldStart, int fieldLength) {
    this.colNum = colNum;
    this.outputColumn = outputColumn;
    this.fieldStart = fieldStart;
    this.fieldLength = fieldLength;
  }

  public VectorUDFTimestampFieldString() {
    fieldStart = -1;
    fieldLength = -1;
  }

  private long getField(byte[] bytes, int start, int length) throws ParseException {
    // Validate
    for (int i = 0; i < length; i++) {
      char ch = (char) bytes[start + i];
      if (ch < patternMin.charAt(i) || ch > patternMax.charAt(i)) {
        throw new ParseException("A timestamp string should match 'yyyy-MM-dd HH:mm:ss.fffffffff' pattern.", i);
      }
    }

    return doGetField(bytes, start, length);
  }

  protected long doGetField(byte[] bytes, int start, int length) throws ParseException {
    int field = 0;
    if (length < fieldLength) {
      throw new ParseException("A timestamp string should be longer.", 0);
    }
    for (int i = fieldStart; i < fieldStart + fieldLength; i++) {
      byte ch = bytes[start + i];
      field = 10 * field + (ch - '0');
    }
    return field;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector outV = (LongColumnVector) batch.cols[outputColumn];
    BytesColumnVector inputCol = (BytesColumnVector)batch.cols[this.colNum];

    final int n = inputCol.isRepeating ? 1 : batch.size;
    int[] sel = batch.selected;

    if (batch.size == 0) {

      // n != batch.size when isRepeating
      return;
    }

    // true for all algebraic UDFs with no state
    outV.isRepeating = inputCol.isRepeating;

    if (inputCol.noNulls) {
      outV.noNulls = true;
      if (batch.selectedInUse) {
        for (int j = 0; j < n; j++) {
          int i = sel[j];
          try {
            outV.vector[i] = getField(inputCol.vector[i], inputCol.start[i], inputCol.length[i]);
            outV.isNull[i] = false;
          } catch (ParseException e) {
            outV.noNulls = false;
            outV.isNull[i] = true;
          }
        }
      } else {
        for (int i = 0; i < n; i++) {
          try {
            outV.vector[i] = getField(inputCol.vector[i], inputCol.start[i], inputCol.length[i]);
            outV.isNull[i] = false;
          } catch (ParseException e) {
            outV.noNulls = false;
            outV.isNull[i] = true;
          }
        }
      }
    } else {

      // Handle case with nulls. Don't do function if the value is null, to save time,
      // because calling the function can be expensive.
      outV.noNulls = false;
      if (batch.selectedInUse) {
        for (int j = 0; j < n; j++) {
          int i = sel[j];
          outV.isNull[i] = inputCol.isNull[i];
          if (!inputCol.isNull[i]) {
            try {
              outV.vector[i] = getField(inputCol.vector[i], inputCol.start[i], inputCol.length[i]);
            } catch (ParseException e) {
              outV.isNull[i] = true;
            }
          }
        }
      } else {
        for (int i = 0; i < n; i++) {
          outV.isNull[i] = inputCol.isNull[i];
          if (!inputCol.isNull[i]) {
            try {
              outV.vector[i] = getField(inputCol.vector[i], inputCol.start[i], inputCol.length[i]);
            } catch (ParseException e) {
              outV.isNull[i] = true;
            }
          }
        }
      }
    }
  }

  @Override
  public int getOutputColumn() {
    return this.outputColumn;
  }

  @Override
  public String getOutputType() {
    return "long";
  }

  public int getColNum() {
    return colNum;
  }

  public void setColNum(int colNum) {
    this.colNum = colNum;
  }

  public void setOutputColumn(int outputColumn) {
    this.outputColumn = outputColumn;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(1)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.STRING)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }
}
