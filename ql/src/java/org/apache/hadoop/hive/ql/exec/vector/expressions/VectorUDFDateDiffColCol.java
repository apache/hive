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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;

public class VectorUDFDateDiffColCol extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private final int colNum1;
  private final int colNum2;

  private transient final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
  private transient final Date date = new Date(0);

  // Transient members initialized by transientInit method.
  private transient LongColumnVector dateVector1;
  private transient LongColumnVector dateVector2;

  public VectorUDFDateDiffColCol(int colNum1, int colNum2, int outputColumnNum) {
    super(outputColumnNum);
    this.colNum1 = colNum1;
    this.colNum2 = colNum2;
  }

  public VectorUDFDateDiffColCol() {
    super();

    // Dummy final assignments.
    colNum1 = -1;
    colNum2 = -1;
  }

  @Override
  public void transientInit() throws HiveException {
    super.transientInit();

    dateVector1 = new LongColumnVector();
    dateVector2 = new LongColumnVector();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    ColumnVector inputColVector1 = batch.cols[colNum1];
    ColumnVector inputColVector2 = batch.cols[colNum2];
    int[] sel = batch.selected;
    int n = batch.size;

    LongColumnVector outV = (LongColumnVector) batch.cols[outputColumnNum];
    long[] outputVector = outV.vector;
    if (n <= 0) {
      // Nothing to do
      return;
    }

    /*
     * Propagate null values for a two-input operator and set isRepeating and noNulls appropriately.
     */
    NullUtil.propagateNullsColCol(inputColVector1, inputColVector2, outV, batch.selected, batch.size, batch.selectedInUse);

    LongColumnVector convertedVector1 = toDateArray(batch, inputTypeInfos[0], inputColVector1, dateVector1);
    LongColumnVector convertedVector2 = toDateArray(batch, inputTypeInfos[1], inputColVector2, dateVector2);

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

  private LongColumnVector toDateArray(VectorizedRowBatch batch, TypeInfo typeInfo,
                                       ColumnVector inputColVector, LongColumnVector dateVector) {
    PrimitiveCategory primitiveCategory =
        ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
    int size = batch.size;
    if (primitiveCategory == PrimitiveCategory.DATE) {
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

    switch (primitiveCategory) {
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
        throw new Error("Unsupported input type " + primitiveCategory.name());
    }
  }

  // Copy the current object contents into the output. Only copy selected entries,
  // as indicated by selectedInUse and the sel array.
  public void copySelected(
      BytesColumnVector input, boolean selectedInUse, int[] sel, int size, LongColumnVector output) {

    output.isRepeating = false;

    // Handle repeating case
    if (input.isRepeating) {
      if (input.noNulls || !input.isNull[0]) {
        String string = new String(input.vector[0], input.start[0], input.length[0]);
        try {
          date.setTime(formatter.parse(string).getTime());
          output.vector[0] = DateWritableV2.dateToDays(date);
          output.isNull[0] = false;
        } catch (ParseException e) {
          output.isNull[0] = true;
          output.noNulls = false;
        }
      } else {
        output.isNull[0] = true;
        output.noNulls = false;
      }
      output.isRepeating = true;
      return;
    }

    // Handle normal case

    // Copy data values over
    if (input.noNulls) {
      if (selectedInUse) {
        if (!output.noNulls) {
          for (int j = 0; j < size; j++) {
            int i = sel[j];
            output.isNull[i] = false;
            // setDays resets the isNull[i] to false if there is a parse exception
            setDays(input, output, i);
          }
        } else {
          for (int j = 0; j < size; j++) {
            int i = sel[j];
            // setDays resets the isNull[i] to false if there is a parse exception
            setDays(input, output, i);
          }
        }
      } else {
        if (!output.noNulls) {
          // Assume it is almost always a performance win to fill all of isNull so we can
          // safely reset noNulls.
          Arrays.fill(output.isNull, false);
          output.noNulls = true;
        }
        for (int i = 0; i < size; i++) {
          setDays(input, output, i);
        }
      }
    } else /* there are nulls in our column */ {

      // handle the isNull array first in tight loops
      output.noNulls = false;
      if (selectedInUse) {
        for (int j = 0; j < size; j++) {
          int i = sel[j];
          output.isNull[i] = input.isNull[i];
        }
      } else {
        System.arraycopy(input.isNull, 0, output.isNull, 0, size);
      }

      //now copy over the data where isNull[index] is false
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
      output.vector[i] = DateWritableV2.dateToDays(date);
    } catch (ParseException e) {
      output.isNull[i] = true;
      output.noNulls = false;
    }
  }

  // Copy the current object contents into the output. Only copy selected entries,
  // as indicated by selectedInUse and the sel array.
  public void copySelected(
      TimestampColumnVector input, boolean selectedInUse, int[] sel, int size, LongColumnVector output) {

    output.isRepeating = false;

    // Handle repeating case
    if (input.isRepeating) {
      if (input.noNulls || !input.isNull[0]) {
        date.setTime(input.getTime(0));
        output.vector[0] = DateWritableV2.dateToDays(date);
        output.isNull[0] = false;
      } else {
        output.isNull[0] = true;
        output.noNulls = false;
      }
      output.isRepeating = true;
      return;
    }

    // Handle normal case

    // Copy data values over
    if (input.noNulls) {
      if (selectedInUse) {
        if (!output.noNulls) {
          // output has noNulls set to false so set the isNull[] to false carefully
          for (int j=0; j < size; j++) {
            int i = sel[j];
            date.setTime(input.getTime(i));
            output.vector[i] = DateWritableV2.dateToDays(date);
            output.isNull[i] = false;
          }
        } else {
          for (int j=0; j < size; j++) {
            int i = sel[j];
            date.setTime(input.getTime(i));
            output.vector[i] = DateWritableV2.dateToDays(date);
          }
        }
      } else {
        if (!output.noNulls) {
          //output noNulls is set to false, we need to reset the isNull array
          Arrays.fill(output.isNull, false);
          output.noNulls = true;
        }
        for (int i = 0; i < size; i++) {
          date.setTime(input.getTime(i));
          output.vector[i] = DateWritableV2.dateToDays(date);
        }
      }
    } else /* there are nulls in our column */ {
      output.noNulls = false;
      //handle the isNull array first in tight loops
      if (selectedInUse) {
        for (int j = 0; j < size; j++) {
          int i = sel[j];
          output.isNull[i] = input.isNull[i];
        }
      } else {
        System.arraycopy(input.isNull, 0, output.isNull, 0, size);
      }

      //now copy over the data when isNull[index] is false
      if (selectedInUse) {
        for (int j = 0; j < size; j++) {
          int i = sel[j];
          if (!input.isNull[i]) {
            date.setTime(input.getTime(i));
            output.vector[i] = DateWritableV2.dateToDays(date);
          }
        }
      } else {
        for (int i = 0; i < size; i++) {
          if (!input.isNull[i]) {
            date.setTime(input.getTime(i));
            output.vector[i] = DateWritableV2.dateToDays(date);
          }
        }
      }
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, colNum1) + ", " + getColumnParamString(1, colNum2);
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
