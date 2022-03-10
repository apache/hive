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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.util.DateTimeMath;

import java.text.ParseException;
import java.util.Calendar;

/**
 * Abstract class to return various fields from a String.
 */
public abstract class VectorUDFTimestampFieldString extends VectorExpression {

  private static final long serialVersionUID = 1L;

  protected final int fieldStart;
  protected final int fieldLength;
  private static final String patternMin = "0000-00-00 00:00:00.000000000";
  private static final String patternMax = "9999-19-99 29:59:59.999999999";
  protected final transient Calendar calendar = DateTimeMath.getProlepticGregorianCalendarUTC();

  public VectorUDFTimestampFieldString(int colNum, int outputColumnNum, int fieldStart, int fieldLength) {
    super(colNum, outputColumnNum);
    this.fieldStart = fieldStart;
    this.fieldLength = fieldLength;
  }

  public VectorUDFTimestampFieldString() {
    fieldStart = -1;
    fieldLength = -1;
  }

  public void initCalendar() {
  }

  @Override
  public void transientInit(Configuration conf) throws HiveException {
    super.transientInit(conf);
    initCalendar();
  }

  protected long getField(byte[] bytes, int start, int length) throws ParseException {
    int field = 0;
    for (int i = 0; i < length; i++) {
      char ch = (char) bytes[start + i];
      if (ch < patternMin.charAt(i) || ch > patternMax.charAt(i)) {
        throw new ParseException("A timestamp string should match 'yyyy-MM-dd HH:mm:ss.fffffffff' pattern.", i);
      }
    }

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
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector outV = (LongColumnVector) batch.cols[outputColumnNum];
    BytesColumnVector inputCol = (BytesColumnVector)batch.cols[this.inputColumnNum[0]];

    final int n = inputCol.isRepeating ? 1 : batch.size;
    int[] sel = batch.selected;
    final boolean selectedInUse = (inputCol.isRepeating == false) && batch.selectedInUse;

    if (batch.size == 0) {

      // n != batch.size when isRepeating
      return;
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outV.isRepeating = false;

    if (inputCol.isRepeating) {
      if (inputCol.noNulls || !inputCol.isNull[0]) {
        try {
          outV.isNull[0] = false;
          outV.vector[0] = getField(inputCol.vector[0], inputCol.start[0], inputCol.length[0]);
        } catch (ParseException e) {
          outV.noNulls = false;
          outV.isNull[0] = true;
        }
      } else {
        outV.isNull[0] = true;
        outV.noNulls = false;
      }
      outV.isRepeating = true;
      return;
    }

    if (inputCol.noNulls) {
      if (selectedInUse) {
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
    } else /* there are nulls in the inputColVector */ {

      // Carefully handle NULLs...
      outV.noNulls = false;

      if (selectedInUse) {
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
  public String vectorExpressionParameters() {
    if (fieldStart == -1) {
      return getColumnParamString(0, inputColumnNum[0]);
    } else {
      return getColumnParamString(0, inputColumnNum[0]) + ", fieldStart " + fieldStart + ", fieldLength " + fieldLength;
    }
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(1)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.STRING_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }
}
