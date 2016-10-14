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
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.DateWritable;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class VectorUDFDateDiffColCol extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private int colNum1;
  private int colNum2;
  private int outputColumn;
  private transient SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
  private transient Date date = new Date(0);
  private transient LongColumnVector dateVector1 = new LongColumnVector();
  private transient LongColumnVector dateVector2 = new LongColumnVector();

  public VectorUDFDateDiffColCol(int colNum1, int colNum2, int outputColumn) {
    this();
    this.colNum1 = colNum1;
    this.colNum2 = colNum2;
    this.outputColumn = outputColumn;
  }

  public VectorUDFDateDiffColCol() {
    super();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    ColumnVector inputColVector1 = batch.cols[colNum1];
    ColumnVector inputColVector2 = batch.cols[colNum2];
    int[] sel = batch.selected;
    int n = batch.size;

    LongColumnVector outV = (LongColumnVector) batch.cols[outputColumn];
    long[] outputVector = outV.vector;
    if (n <= 0) {
      // Nothing to do
      return;
    }

    NullUtil.propagateNullsColCol(inputColVector1, inputColVector2, outV, batch.selected, batch.size, batch.selectedInUse);

    LongColumnVector convertedVector1 = toDateArray(batch, inputTypes[0], inputColVector1, dateVector1);
    LongColumnVector convertedVector2 = toDateArray(batch, inputTypes[1], inputColVector2, dateVector2);

    // Now disregard null in second pass.
    if ((inputColVector1.isRepeating) && (inputColVector2.isRepeating)) {
      // All must be selected otherwise size would be zero
      // Repeating property will not change.
      outV.isRepeating = true;
      if (convertedVector1.isNull[0] || convertedVector2.isNull[0]) {
        outV.isNull[0] = true;
      } else {
        outputVector[0] = convertedVector1.vector[0] - convertedVector2.vector[0];
      }
    } else if (inputColVector1.isRepeating) {
      if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          if (convertedVector1.isNull[0] || convertedVector2.isNull[i]) {
            outV.isNull[i] = true;
          } else {
            outputVector[i] = convertedVector1.vector[0] - convertedVector2.vector[i];
          }
        }
      } else {
        for (int i = 0; i != n; i++) {
          if (convertedVector1.isNull[0] || convertedVector2.isNull[i]) {
            outV.isNull[i] = true;
          } else {
            outputVector[i] = convertedVector1.vector[0] - convertedVector2.vector[i];
          }
        }
      }
    } else if (inputColVector2.isRepeating) {
      if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          if (convertedVector1.isNull[i] || convertedVector2.isNull[0]) {
            outV.isNull[i] = true;
          } else {
            outputVector[i] = convertedVector1.vector[i] - convertedVector2.vector[0];
          }
        }
      } else {
        for (int i = 0; i != n; i++) {
          if (convertedVector1.isNull[i] || convertedVector2.isNull[0]) {
            outV.isNull[i] = true;
          } else {
            outputVector[i] = convertedVector1.vector[i] - convertedVector2.vector[0];
          }
        }
      }
    } else {
      if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          if (convertedVector1.isNull[i] || convertedVector2.isNull[i]) {
            outV.isNull[i] = true;
          } else {
            outputVector[i] = convertedVector1.vector[i] - convertedVector2.vector[i];
          }
        }
      } else {
        for (int i = 0; i != n; i++) {
          if (convertedVector1.isNull[i] || convertedVector2.isNull[i]) {
            outV.isNull[i] = true;
          } else {
            outputVector[i] = convertedVector1.vector[i] - convertedVector2.vector[i];
          }
        }
      }
    }
  }

  private LongColumnVector toDateArray(VectorizedRowBatch batch, Type colType,
                                       ColumnVector inputColVector, LongColumnVector dateVector) {
    int size = batch.size;
    if (colType == Type.DATE) {
      return (LongColumnVector) inputColVector;
    }

    if (size > dateVector.vector.length) {
      if (dateVector1 == dateVector) {
        dateVector1 = new LongColumnVector(size * 2);
        dateVector = dateVector1;
      } else {
        dateVector2 = new LongColumnVector(size * 2);
        dateVector = dateVector2;
      }
    }

    switch (colType) {
      case TIMESTAMP:
        TimestampColumnVector tcv = (TimestampColumnVector) inputColVector;
        copySelected(tcv, batch.selectedInUse, batch.selected, batch.size, dateVector);
        return dateVector;

      case STRING:
      case CHAR:
      case VARCHAR:
        BytesColumnVector bcv = (BytesColumnVector) inputColVector;
        copySelected(bcv, batch.selectedInUse, batch.selected, batch.size, dateVector);
        return dateVector;
      default:
        throw new Error("Unsupported input type " + colType.name());
    }
  }

  // Copy the current object contents into the output. Only copy selected entries,
  // as indicated by selectedInUse and the sel array.
  public void copySelected(
      BytesColumnVector input, boolean selectedInUse, int[] sel, int size, LongColumnVector output) {

    // Output has nulls if and only if input has nulls.
    output.noNulls = input.noNulls;
    output.isRepeating = false;

    // Handle repeating case
    if (input.isRepeating) {
      output.isNull[0] = input.isNull[0];
      output.isRepeating = true;

      if (!input.isNull[0]) {
        String string = new String(input.vector[0], input.start[0], input.length[0]);
        try {
          date.setTime(formatter.parse(string).getTime());
          output.vector[0] = DateWritable.dateToDays(date);
        } catch (ParseException e) {
          output.isNull[0] = true;
        }
      }
      return;
    }

    // Handle normal case

    // Copy data values over
    if (input.noNulls) {
      if (selectedInUse) {
        for (int j = 0; j < size; j++) {
          int i = sel[j];
          setDays(input, output, i);
        }
      } else {
        for (int i = 0; i < size; i++) {
          setDays(input, output, i);
        }
      }
    } else {
      if (selectedInUse) {
        for (int j = 0; j < size; j++) {
          int i = sel[j];
          output.isNull[i] = input.isNull[i];
        }
      }
      else {
        System.arraycopy(input.isNull, 0, output.isNull, 0, size);
      }

      if (selectedInUse) {
        for (int j = 0; j < size; j++) {
          int i = sel[j];
          if (!input.isNull[i]) {
           setDays(input, output, i);
          }
        }
      } else {
        for (int i = 0; i < size; i++) {
          if (!input.isNull[i]) {
            setDays(input, output, i);
          }
        }
      }
    }
  }

  private void setDays(BytesColumnVector input, LongColumnVector output, int i) {
    String string = new String(input.vector[i], input.start[i], input.length[i]);
    try {
      date.setTime(formatter.parse(string).getTime());
      output.vector[i] = DateWritable.dateToDays(date);
    } catch (ParseException e) {
      output.isNull[i] = true;
      output.noNulls = false;
    }
  }

  // Copy the current object contents into the output. Only copy selected entries,
  // as indicated by selectedInUse and the sel array.
  public void copySelected(
      TimestampColumnVector input, boolean selectedInUse, int[] sel, int size, LongColumnVector output) {

    // Output has nulls if and only if input has nulls.
    output.noNulls = input.noNulls;
    output.isRepeating = false;

    // Handle repeating case
    if (input.isRepeating) {
      output.isNull[0] = input.isNull[0];
      output.isRepeating = true;

      if (!input.isNull[0]) {
        date.setTime(input.getTime(0));
        output.vector[0] = DateWritable.dateToDays(date);
      }
      return;
    }

    // Handle normal case

    // Copy data values over
    if (input.noNulls) {
      if (selectedInUse) {
        for (int j = 0; j < size; j++) {
          int i = sel[j];
          date.setTime(input.getTime(i));
          output.vector[i] = DateWritable.dateToDays(date);
        }
      } else {
        for (int i = 0; i < size; i++) {
          date.setTime(input.getTime(i));
          output.vector[i] = DateWritable.dateToDays(date);
        }
      }
    } else {
      if (selectedInUse) {
        for (int j = 0; j < size; j++) {
          int i = sel[j];
          output.isNull[i] = input.isNull[i];
        }
      }
      else {
        System.arraycopy(input.isNull, 0, output.isNull, 0, size);
      }

      if (selectedInUse) {
        for (int j = 0; j < size; j++) {
          int i = sel[j];
          if (!input.isNull[i]) {
            date.setTime(input.getTime(i));
            output.vector[i] = DateWritable.dateToDays(date);
          }
        }
      } else {
        for (int i = 0; i < size; i++) {
          if (!input.isNull[i]) {
            date.setTime(input.getTime(i));
            output.vector[i] = DateWritable.dateToDays(date);
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
  public String vectorExpressionParameters() {
    return "col " + colNum1 + ", col " + colNum2;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.STRING_DATETIME_FAMILY,
            VectorExpressionDescriptor.ArgumentType.STRING_DATETIME_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }
}
