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
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.Text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

public class VectorUDFDateAddColScalar extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private int colNum;
  private int outputColumn;
  private int numDays;
  protected boolean isPositive = true;
  private transient final Calendar calendar = Calendar.getInstance();
  private transient SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
  private transient final Text text = new Text();

  public VectorUDFDateAddColScalar(int colNum, long numDays, int outputColumn) {
    super();
    this.colNum = colNum;
    this.numDays = (int) numDays;
    this.outputColumn = outputColumn;
  }

  public VectorUDFDateAddColScalar() {
    super();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    BytesColumnVector outV = (BytesColumnVector) batch.cols[outputColumn];
    ColumnVector inputCol = batch.cols[this.colNum];
    /* every line below this is identical for evaluateLong & evaluateString */
    final int n = inputCol.isRepeating ? 1 : batch.size;
    int[] sel = batch.selected;
    final boolean selectedInUse = (inputCol.isRepeating == false) && batch.selectedInUse;

    if(batch.size == 0) {
      /* n != batch.size when isRepeating */
      return;
    }

    /* true for all algebraic UDFs with no state */
    outV.isRepeating = inputCol.isRepeating;

    switch (inputTypes[0]) {
      case DATE:
        if (inputCol.noNulls) {
          outV.noNulls = true;
          if (selectedInUse) {
            for(int j=0; j < n; j++) {
              int i = sel[j];
              outV.vector[i] = evaluateDate(inputCol, i);
              outV.start[i] = 0;
              outV.length[i] = outV.vector[i].length;
            }
          } else {
            for(int i = 0; i < n; i++) {
              outV.vector[i] = evaluateDate(inputCol, i);
              outV.start[i] = 0;
              outV.length[i] = outV.vector[i].length;
            }
          }
        } else {
          // Handle case with nulls. Don't do function if the value is null, to save time,
          // because calling the function can be expensive.
          outV.noNulls = false;
          if (selectedInUse) {
            for(int j = 0; j < n; j++) {
              int i = sel[j];
              outV.isNull[i] = inputCol.isNull[i];
              if (!inputCol.isNull[i]) {
                outV.vector[i] = evaluateDate(inputCol, i);
                outV.start[i] = 0;
                outV.length[i] = outV.vector[i].length;
              }
            }
          } else {
            for(int i = 0; i < n; i++) {
              outV.isNull[i] = inputCol.isNull[i];
              if (!inputCol.isNull[i]) {
                outV.vector[i] = evaluateDate(inputCol, i);
                outV.start[i] = 0;
                outV.length[i] = outV.vector[i].length;
              }
            }
          }
        }
        break;

      case TIMESTAMP:
        if (inputCol.noNulls) {
          outV.noNulls = true;
          if (batch.selectedInUse) {
            for(int j=0; j < n; j++) {
              int i = sel[j];
              outV.vector[i] = evaluateTimestamp(inputCol, i);
              outV.start[i] = 0;
              outV.length[i] = outV.vector[i].length;
            }
          } else {
            for(int i = 0; i < n; i++) {
              outV.vector[i] = evaluateTimestamp(inputCol, i);
              outV.start[i] = 0;
              outV.length[i] = outV.vector[i].length;
            }
          }
        } else {
          // Handle case with nulls. Don't do function if the value is null, to save time,
          // because calling the function can be expensive.
          outV.noNulls = false;
          if (batch.selectedInUse) {
            for(int j = 0; j < n; j++) {
              int i = sel[j];
              outV.isNull[i] = inputCol.isNull[i];
              if (!inputCol.isNull[i]) {
                outV.vector[i] = evaluateTimestamp(inputCol, i);
                outV.start[i] = 0;
                outV.length[i] = outV.vector[i].length;
              }
            }
          } else {
            for(int i = 0; i < n; i++) {
              outV.isNull[i] = inputCol.isNull[i];
              if (!inputCol.isNull[i]) {
                outV.vector[i] = evaluateTimestamp(inputCol, i);
                outV.start[i] = 0;
                outV.length[i] = outV.vector[i].length;
              }
            }
          }
        }
        break;

      case STRING:
      case CHAR:
      case VARCHAR:
        if (inputCol.noNulls) {
          outV.noNulls = true;
          if (batch.selectedInUse) {
            for(int j=0; j < n; j++) {
              int i = sel[j];
              evaluateString(inputCol, outV, i);
            }
          } else {
            for(int i = 0; i < n; i++) {
              evaluateString(inputCol, outV, i);
            }
          }
        } else {
          // Handle case with nulls. Don't do function if the value is null, to save time,
          // because calling the function can be expensive.
          outV.noNulls = false;
          if (batch.selectedInUse) {
            for(int j = 0; j < n; j++) {
              int i = sel[j];
              outV.isNull[i] = inputCol.isNull[i];
              if (!inputCol.isNull[i]) {
                evaluateString(inputCol, outV, i);
              }
            }
          } else {
            for(int i = 0; i < n; i++) {
              outV.isNull[i] = inputCol.isNull[i];
              if (!inputCol.isNull[i]) {
                evaluateString(inputCol, outV, i);
              }
            }
          }
        }
        break;
      default:
          throw new Error("Unsupported input type " + inputTypes[0].name());
    }
  }

  protected byte[] evaluateTimestamp(ColumnVector columnVector, int index) {
    TimestampColumnVector tcv = (TimestampColumnVector) columnVector;
    calendar.setTimeInMillis(tcv.getTime(index));
    if (isPositive) {
      calendar.add(Calendar.DATE, numDays);
    } else {
      calendar.add(Calendar.DATE, -numDays);
    }
    Date newDate = calendar.getTime();
    text.set(formatter.format(newDate));
    return Arrays.copyOf(text.getBytes(), text.getLength());
  }

  protected byte[] evaluateDate(ColumnVector columnVector, int index) {
    LongColumnVector lcv = (LongColumnVector) columnVector;
    if (isPositive) {
      calendar.setTimeInMillis(DateWritable.daysToMillis((int) lcv.vector[index] + numDays));
    } else {
      calendar.setTimeInMillis(DateWritable.daysToMillis((int) lcv.vector[index] - numDays));
    }
    Date newDate = calendar.getTime();
    text.set(formatter.format(newDate));
    return Arrays.copyOf(text.getBytes(), text.getLength());
  }

  protected void evaluateString(ColumnVector columnVector, BytesColumnVector outputVector, int i) {
    BytesColumnVector bcv = (BytesColumnVector) columnVector;
    text.set(bcv.vector[i], bcv.start[i], bcv.length[i]);
    try {
      calendar.setTime(formatter.parse(text.toString()));
    } catch (ParseException e) {
      outputVector.isNull[i] = true;
    }
    if (isPositive) {
      calendar.add(Calendar.DATE, numDays);
    } else {
      calendar.add(Calendar.DATE, -numDays);
    }
    Date newDate = calendar.getTime();
    text.set(formatter.format(newDate));

    byte[] bytes = text.getBytes();
    int size = text.getLength();
    outputVector.vector[i] = Arrays.copyOf(bytes, size);
    outputVector.start[i] = 0;
    outputVector.length[i] = size;
  }

  @Override
  public int getOutputColumn() {
    return this.outputColumn;
  }

  @Override
  public String getOutputType() {
    return "string";
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

  public int getNumDays() {
    return numDays;
  }

  public void setNumDay(int numDays) {
    this.numDays = numDays;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.STRING_DATETIME_FAMILY,
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR);
    return b.build();
  }
}
