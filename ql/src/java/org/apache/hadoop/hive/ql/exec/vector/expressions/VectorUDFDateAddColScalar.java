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
import org.apache.hadoop.hive.common.type.Date;
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
import org.apache.hive.common.util.DateParser;

import java.util.Arrays;

public class VectorUDFDateAddColScalar extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private final int numDays;

  protected boolean isPositive = true;

  private transient final Text text = new Text();

  // Transient members initialized by transientInit method.
  private transient PrimitiveCategory primitiveCategory;

  public VectorUDFDateAddColScalar(int colNum, long numDays, int outputColumnNum) {
    super(colNum, outputColumnNum);
    this.numDays = (int) numDays;
  }

  public VectorUDFDateAddColScalar() {
    super();

    // Dummy final assignments.
    numDays = 0;
  }

  @Override
  public void transientInit(Configuration conf) throws HiveException {
    super.transientInit(conf);

    primitiveCategory =
        ((PrimitiveTypeInfo) inputTypeInfos[0]).getPrimitiveCategory();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector outputColVector = (LongColumnVector) batch.cols[outputColumnNum];
    ColumnVector inputCol = batch.cols[this.inputColumnNum[0]];
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

    switch (primitiveCategory) {
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

          if (batch.selectedInUse) {
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

          if (batch.selectedInUse) {
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
          throw new Error("Unsupported input type " + primitiveCategory.name());
    }
  }

  protected long evaluateTimestamp(ColumnVector columnVector, int index) {
    TimestampColumnVector tcv = (TimestampColumnVector) columnVector;
    // Convert to date value (in days)
    long days = DateWritableV2.millisToDays(tcv.getTime(index));
    if (isPositive) {
      days += numDays;
    } else {
      days -= numDays;
    }
    return days;
  }

  protected long evaluateDate(ColumnVector columnVector, int index) {
    LongColumnVector lcv = (LongColumnVector) columnVector;
    long days = lcv.vector[index];
    if (isPositive) {
      days += numDays;
    } else {
      days -= numDays;
    }
    return days;
  }

  protected void evaluateString(ColumnVector columnVector, LongColumnVector outputVector, int i) {
    BytesColumnVector bcv = (BytesColumnVector) columnVector;
    text.set(bcv.vector[i], bcv.start[i], bcv.length[i]);
    Date hDate = DateParser.parseDate(text.toString());
    if (hDate == null) {
      outputVector.noNulls = false;
      outputVector.isNull[i] = true;
      return;
    }
    long days = DateWritableV2.millisToDays(hDate.toEpochMilli());
    if (isPositive) {
      days += numDays;
    } else {
      days -= numDays;
    }
    outputVector.vector[i] = days;
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, inputColumnNum[0]) + ", val " + numDays;
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
