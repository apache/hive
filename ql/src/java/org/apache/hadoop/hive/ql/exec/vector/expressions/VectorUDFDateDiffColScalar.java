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
import org.apache.hadoop.io.Text;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;

public class VectorUDFDateDiffColScalar extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private final int colNum;

  private long longValue;
  private Timestamp timestampValue;
  private byte[] bytesValue;

  private transient final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
  private transient final Text text = new Text();
  private transient final Date date = new Date(0);

  private int baseDate;

  public VectorUDFDateDiffColScalar(int colNum, Object object, int outputColumnNum) {
    super(outputColumnNum);
    this.colNum = colNum;

    if (object instanceof Long) {
      this.longValue = (Long) object;
    } else if (object instanceof Timestamp) {
      this.timestampValue = (Timestamp) object;
    } else if (object instanceof byte []) {
      this.bytesValue = (byte []) object;
    } else {
      throw new RuntimeException("Unexpected scalar object " + object.getClass().getName() + " " + object.toString());
    }
  }

  public VectorUDFDateDiffColScalar() {
    super();

    // Dummy final assignments.
    colNum = -1;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector outputColVector = (LongColumnVector) batch.cols[outputColumnNum];
    ColumnVector inputCol = batch.cols[this.colNum];
    /* every line below this is identical for evaluateLong & evaluateString */
    final int n = inputCol.isRepeating ? 1 : batch.size;
    int[] sel = batch.selected;
    final boolean selectedInUse = (inputCol.isRepeating == false) && batch.selectedInUse;
    boolean[] outputIsNull = outputColVector.isNull;

    if(batch.size == 0) {
      /* n != batch.size when isRepeating */
      return;
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    PrimitiveCategory primitiveCategory1 = ((PrimitiveTypeInfo) inputTypeInfos[1]).getPrimitiveCategory();
    switch (primitiveCategory1) {
      case DATE:
        baseDate = (int) longValue;
        break;

      case TIMESTAMP:
        date.setTime(timestampValue.getTime());
        baseDate = DateWritableV2.dateToDays(date);
        break;

      case STRING:
      case CHAR:
      case VARCHAR:
        try {
          date.setTime(formatter.parse(new String(bytesValue, "UTF-8")).getTime());
          baseDate = DateWritableV2.dateToDays(date);
          break;
        } catch (Exception e) {
          outputColVector.noNulls = false;
          if (selectedInUse) {
            for(int j=0; j < n; j++) {
              int i = sel[j];
              outputColVector.isNull[i] = true;
            }
          } else {
            for(int i = 0; i < n; i++) {
              outputColVector.isNull[i] = true;
            }
          }
          return;
        }
    default:
        throw new Error("Invalid input type #1: " + primitiveCategory1.name());
    }

    PrimitiveCategory primitiveCategory0 = ((PrimitiveTypeInfo) inputTypeInfos[0]).getPrimitiveCategory();
    switch (primitiveCategory0) {
      case DATE:
        if (inputCol.isRepeating) {
          if (inputCol.noNulls || !inputCol.isNull[0]) {
            outputColVector.isNull[0] = false;
            outputColVector.vector[0] = evaluateDate(inputCol, 0);
          } else {
            outputColVector.isNull[0] = true;
            outputColVector.noNulls = false;
          }
          outputColVector.isRepeating = true;
        } else if (inputCol.noNulls) {
          if (batch.selectedInUse) {

            // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.

            if (!outputColVector.noNulls) {
              for(int j = 0; j != n; j++) {
               final int i = sel[j];
               // Set isNull before call in case it changes it mind.
               outputIsNull[i] = false;
               outputColVector.vector[i] = evaluateDate(inputCol, i);
             }
            } else {
              for(int j = 0; j != n; j++) {
                final int i = sel[j];
                outputColVector.vector[i] = evaluateDate(inputCol, i);
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
              outputColVector.vector[i] = evaluateDate(inputCol, i);
            }
          }
        } else /* there are nulls in the inputColVector */ {

          // Carefully handle NULLs..

          // Handle case with nulls. Don't do function if the value is null, to save time,
          // because calling the function can be expensive.
          outputColVector.noNulls = false;

          if (selectedInUse) {
            for(int j = 0; j < n; j++) {
              int i = sel[j];
              outputColVector.isNull[i] = inputCol.isNull[i];
              if (!inputCol.isNull[i]) {
                outputColVector.vector[i] = evaluateDate(inputCol, i);
              }
            }
          } else {
            for(int i = 0; i < n; i++) {
              outputColVector.isNull[i] = inputCol.isNull[i];
              if (!inputCol.isNull[i]) {
                outputColVector.vector[i] = evaluateDate(inputCol, i);
              }
            }
          }
        }
        break;

      case TIMESTAMP:
        if (inputCol.isRepeating) {
          if (inputCol.noNulls || !inputCol.isNull[0]) {
            outputColVector.isNull[0] = false;
            outputColVector.vector[0] = evaluateTimestamp(inputCol, 0);
          } else {
            outputColVector.isNull[0] = true;
            outputColVector.noNulls = false;
          }
          outputColVector.isRepeating = true;
        } else if (inputCol.noNulls) {
          if (batch.selectedInUse) {

            // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.

            if (!outputColVector.noNulls) {
              for(int j = 0; j != n; j++) {
               final int i = sel[j];
               // Set isNull before call in case it changes it mind.
               outputIsNull[i] = false;
               outputColVector.vector[i] = evaluateTimestamp(inputCol, i);
             }
            } else {
              for(int j = 0; j != n; j++) {
                final int i = sel[j];
                outputColVector.vector[i] = evaluateTimestamp(inputCol, i);
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
              outputColVector.vector[i] = evaluateTimestamp(inputCol, i);
            }
          }
        } else /* there are nulls in the inputColVector */ {

          // Carefully handle NULLs..

          // Handle case with nulls. Don't do function if the value is null, to save time,
          // because calling the function can be expensive.
          outputColVector.noNulls = false;

          if (selectedInUse) {
            for(int j = 0; j < n; j++) {
              int i = sel[j];
              outputColVector.isNull[i] = inputCol.isNull[i];
              if (!inputCol.isNull[i]) {
                outputColVector.vector[i] = evaluateTimestamp(inputCol, i);
              }
            }
          } else {
            for(int i = 0; i < n; i++) {
              outputColVector.isNull[i] = inputCol.isNull[i];
              if (!inputCol.isNull[i]) {
                outputColVector.vector[i] = evaluateTimestamp(inputCol, i);
              }
            }
          }
        }
        break;

      case STRING:
      case CHAR:
      case VARCHAR:
        if (inputCol.isRepeating) {
          if (inputCol.noNulls || !inputCol.isNull[0]) {
            outputColVector.isNull[0] = false;
            evaluateString(inputCol, outputColVector, 0);
          } else {
            outputColVector.isNull[0] = true;
            outputColVector.noNulls = false;
          }
          outputColVector.isRepeating = true;
        } else if (inputCol.noNulls) {
          if (batch.selectedInUse) {

            // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.

            if (!outputColVector.noNulls) {
              for(int j = 0; j != n; j++) {
               final int i = sel[j];
               // Set isNull before call in case it changes it mind.
               outputIsNull[i] = false;
               evaluateString(inputCol, outputColVector, i);
             }
            } else {
              for(int j = 0; j != n; j++) {
                final int i = sel[j];
                evaluateString(inputCol, outputColVector, i);
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
              evaluateString(inputCol, outputColVector, i);
            }
          }
        } else /* there are nulls in the inputColVector */ {

          // Carefully handle NULLs..

          // Handle case with nulls. Don't do function if the value is null, to save time,
          // because calling the function can be expensive.
          outputColVector.noNulls = false;

          if (selectedInUse) {
            for(int j = 0; j < n; j++) {
              int i = sel[j];
              outputColVector.isNull[i] = inputCol.isNull[i];
              if (!inputCol.isNull[i]) {
                evaluateString(inputCol, outputColVector, i);
              }
            }
          } else {
            for(int i = 0; i < n; i++) {
              outputColVector.isNull[i] = inputCol.isNull[i];
              if (!inputCol.isNull[i]) {
                evaluateString(inputCol, outputColVector, i);
              }
            }
          }
        }
        break;
    default:
      throw new Error("Invalid input type #0: " + primitiveCategory0.name());
    }
  }

  protected int evaluateTimestamp(ColumnVector columnVector, int index) {
    TimestampColumnVector tcv = (TimestampColumnVector) columnVector;
    date.setTime(tcv.getTime(index));
    return DateWritableV2.dateToDays(date) - baseDate;
  }

  protected int evaluateDate(ColumnVector columnVector, int index) {
    LongColumnVector lcv = (LongColumnVector) columnVector;
    return ((int) lcv.vector[index]) - baseDate;
  }

  protected void evaluateString(ColumnVector columnVector, LongColumnVector output, int i) {
    BytesColumnVector bcv = (BytesColumnVector) columnVector;
    text.set(bcv.vector[i], bcv.start[i], bcv.length[i]);
    try {
      date.setTime(formatter.parse(text.toString()).getTime());
      output.vector[i] = DateWritableV2.dateToDays(date) - baseDate;
    } catch (ParseException e) {
      output.vector[i] = 1;
      output.isNull[i] = true;
    }
  }

  public long getLongValue() {
    return longValue;
  }

  public void setLongValue(int longValue) {
    this.longValue = longValue;
  }

  public byte[] getStringValue() {
    return bytesValue;
  }

  public void setStringValue(byte[] bytesValue) {
    this.bytesValue = bytesValue;
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, colNum) + ", val " + displayUtf8Bytes(bytesValue);
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
            VectorExpressionDescriptor.InputExpressionType.SCALAR);
    return b.build();
  }
}
