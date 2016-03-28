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

public class VectorUDFDateAddColCol extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private int colNum1;
  private int colNum2;
  private int outputColumn;
  protected boolean isPositive = true;
  private transient final Calendar calendar = Calendar.getInstance();
  private transient SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
  private transient final Text text = new Text();

  public VectorUDFDateAddColCol(int colNum1, int colNum2, int outputColumn) {
    this();
    this.colNum1 = colNum1;
    this.colNum2 = colNum2;
    this.outputColumn = outputColumn;
  }

  public VectorUDFDateAddColCol() {
    super();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    ColumnVector inputColVector1 = batch.cols[colNum1];
    LongColumnVector inputColVector2 = (LongColumnVector) batch.cols[colNum2];
    int[] sel = batch.selected;
    int n = batch.size;
    long[] vector2 = inputColVector2.vector;

    BytesColumnVector outV = (BytesColumnVector) batch.cols[outputColumn];
    byte[][] outputVector = outV.vector;
    if (n <= 0) {
      // Nothing to do
      return;
    }

    // Handle null
    NullUtil.propagateNullsColCol(inputColVector1, inputColVector2, outV, batch.selected, batch.size, batch.selectedInUse);

    switch (inputTypes[0]) {
      case DATE:
        // Now disregard null in second pass.
        if ((inputColVector1.isRepeating) && (inputColVector2.isRepeating)) {
          // All must be selected otherwise size would be zero
          // Repeating property will not change.
          outV.isRepeating = true;
          outputVector[0] = evaluateDate(inputColVector1, 0, vector2[0]);
          outV.start[0] = 0;
          outV.length[0] = outputVector[0].length;
        } else if (batch.selectedInUse) {
          for (int j = 0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = evaluateDate(inputColVector1, i, vector2[i]);
            outV.start[i] = 0;
            outV.length[i] = outputVector[0].length;
          }
        } else {
          for (int i = 0; i != n; i++) {
            outputVector[i] = evaluateDate(inputColVector1, i, vector2[i]);
            outV.start[i] = 0;
            outV.length[i] = outputVector[0].length;
          }
        }
        break;

      case TIMESTAMP:
        // Now disregard null in second pass.
        if ((inputColVector1.isRepeating) && (inputColVector2.isRepeating)) {
          // All must be selected otherwise size would be zero
          // Repeating property will not change.
          outV.isRepeating = true;
          outputVector[0] = evaluateTimestamp(inputColVector1, 0, vector2[0]);
        } else if (batch.selectedInUse) {
          for (int j = 0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = evaluateTimestamp(inputColVector1, i, vector2[i]);
            outV.start[i] = 0;
            outV.length[i] = outputVector[0].length;
          }
        } else {
          for (int i = 0; i != n; i++) {
            outputVector[i] = evaluateTimestamp(inputColVector1, i, vector2[i]);
            outV.start[i] = 0;
            outV.length[i] = outputVector[0].length;
          }
        }
        break;

      case STRING:
      case CHAR:
      case VARCHAR:
        // Now disregard null in second pass.
        if ((inputColVector1.isRepeating) && (inputColVector2.isRepeating)) {
          // All must be selected otherwise size would be zero
          // Repeating property will not change.
          outV.isRepeating = true;
          evaluateString((BytesColumnVector) inputColVector1, inputColVector2, outV, 0);
        } else if (batch.selectedInUse) {
          for (int j = 0; j != n; j++) {
            int i = sel[j];
            evaluateString((BytesColumnVector) inputColVector1, inputColVector2, outV, i);
          }
        } else {
          for (int i = 0; i != n; i++) {
            evaluateString((BytesColumnVector) inputColVector1, inputColVector2, outV, i);
          }
        }
        break;
      default:
        throw new Error("Unsupported input type " + inputTypes[0].name());
    }
  }

  protected byte[] evaluateDate(ColumnVector columnVector, int index, long numDays) {
    LongColumnVector lcv = (LongColumnVector) columnVector;
    if (isPositive) {
      calendar.setTimeInMillis(DateWritable.daysToMillis((int) lcv.vector[index] + (int) numDays));
    } else {
      calendar.setTimeInMillis(DateWritable.daysToMillis((int) lcv.vector[index] - (int) numDays));
    }
    Date newDate = calendar.getTime();
    text.set(formatter.format(newDate));
    return Arrays.copyOf(text.getBytes(), text.getLength());
  }

  protected byte[] evaluateTimestamp(ColumnVector columnVector, int index, long numDays) {
    TimestampColumnVector tcv = (TimestampColumnVector) columnVector;
    calendar.setTimeInMillis(tcv.getTime(index));
    if (isPositive) {
      calendar.add(Calendar.DATE, (int) numDays);
    } else {
      calendar.add(Calendar.DATE, (int) -numDays);
    }
    Date newDate = calendar.getTime();
    text.set(formatter.format(newDate));
    return Arrays.copyOf(text.getBytes(), text.getLength());
  }

  protected void evaluateString(BytesColumnVector inputColumnVector1, LongColumnVector inputColumnVector2,
                                BytesColumnVector outputVector, int i) {
    if (inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i]) {
      outputVector.noNulls = false;
      outputVector.isNull[i] = true;
    } else {
      text.set(inputColumnVector1.vector[i], inputColumnVector1.start[i], inputColumnVector1.length[i]);
      try {
        calendar.setTime(formatter.parse(text.toString()));
      } catch (ParseException e) {
        outputVector.noNulls = false;
        outputVector.isNull[i] = true;
      }
      if (isPositive) {
        calendar.add(Calendar.DATE, (int) inputColumnVector2.vector[i]);
      } else {
        calendar.add(Calendar.DATE, -(int) inputColumnVector2.vector[i]);
      }
      Date newDate = calendar.getTime();
      text.set(formatter.format(newDate));

      outputVector.vector[i] = Arrays.copyOf(text.getBytes(), text.getLength());
      outputVector.start[i] = 0;
      outputVector.length[i] = text.getLength();
    }
  }

  @Override
  public int getOutputColumn() {
    return this.outputColumn;
  }

  @Override
  public String getOutputType() {
    return "string";
  }

  public int getColNum1() {
    return colNum1;
  }

  public void setColNum1(int colNum1) {
    this.colNum1 = colNum1;
  }

  public int getColNum2() {
    return colNum2;
  }

  public void setColNum2(int colNum2) {
    this.colNum2 = colNum2;
  }

  public void setOutputColumn(int outputColumn) {
    this.outputColumn = outputColumn;
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
            VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }
}
