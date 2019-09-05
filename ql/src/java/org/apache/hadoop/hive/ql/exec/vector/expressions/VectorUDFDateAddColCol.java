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

public class VectorUDFDateAddColCol extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private final int colNum1;
  private final int colNum2;

  protected boolean isPositive = true;

  private transient final Text text = new Text();

  // Transient members initialized by transientInit method.
  private transient PrimitiveCategory primitiveCategory;

  public VectorUDFDateAddColCol(int colNum1, int colNum2, int outputColumnNum) {
    super(outputColumnNum);
    this.colNum1 = colNum1;
    this.colNum2 = colNum2;
  }

  public VectorUDFDateAddColCol() {
    super();

    // Dummy final assignments.
    colNum1 = -1;
    colNum2 = -1;
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

    ColumnVector inputColVector1 = batch.cols[colNum1];
    LongColumnVector inputColVector2 = (LongColumnVector) batch.cols[colNum2];
    int[] sel = batch.selected;
    int n = batch.size;
    long[] vector2 = inputColVector2.vector;

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

    switch (primitiveCategory) {
      case DATE:
        // Now disregard null in second pass.
        if ((inputColVector1.isRepeating) && (inputColVector2.isRepeating)) {
          // All must be selected otherwise size would be zero
          // Repeating property will not change.
          outV.isRepeating = true;
          outputVector[0] = evaluateDate(inputColVector1, 0, vector2[0]);
        } else if (inputColVector1.isRepeating) {
          evaluateRepeatedDate(inputColVector1, vector2, outV,
              batch.selectedInUse, batch.selected, n);
        } else if (inputColVector2.isRepeating) {
          final long repeatedNumDays = vector2[0];
          if (batch.selectedInUse) {
            for (int j = 0; j != n; j++) {
              int i = sel[j];
              outputVector[i] = evaluateDate(inputColVector1, i, repeatedNumDays);
            }
          } else {
            for (int i = 0; i != n; i++) {
              outputVector[i] = evaluateDate(inputColVector1, i, repeatedNumDays);
            }
          }
        } else if (batch.selectedInUse) {
          for (int j = 0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = evaluateDate(inputColVector1, i, vector2[i]);
          }
        } else {
          for (int i = 0; i != n; i++) {
            outputVector[i] = evaluateDate(inputColVector1, i, vector2[i]);
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
        } else if (inputColVector1.isRepeating) {
          evaluateRepeatedTimestamp(inputColVector1, vector2, outV,
              batch.selectedInUse, batch.selected, n);
        } else if (inputColVector2.isRepeating) {
          final long repeatedNumDays = vector2[0];
          if (batch.selectedInUse) {
            for (int j = 0; j != n; j++) {
              int i = sel[j];
              outputVector[i] = evaluateTimestamp(inputColVector1, i, repeatedNumDays);
            }
          } else {
            for (int i = 0; i != n; i++) {
              outputVector[i] = evaluateTimestamp(inputColVector1, i, repeatedNumDays);
            }
          }
        } else if (batch.selectedInUse) {
          for (int j = 0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = evaluateTimestamp(inputColVector1, i, vector2[i]);
          }
        } else {
          for (int i = 0; i != n; i++) {
            outputVector[i] = evaluateTimestamp(inputColVector1, i, vector2[i]);
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
          evaluateString((BytesColumnVector) inputColVector1, outV, 0, vector2[0]);
        } else if (inputColVector1.isRepeating) {
          evaluateRepeatedString((BytesColumnVector) inputColVector1, vector2, outV,
              batch.selectedInUse, batch.selected, n);
        } else if (inputColVector2.isRepeating) {
          final long repeatedNumDays = vector2[0];
          if (batch.selectedInUse) {
            for (int j = 0; j != n; j++) {
              int i = sel[j];
              evaluateString((BytesColumnVector) inputColVector1, outV, i, repeatedNumDays);
            }
          } else {
            for (int i = 0; i != n; i++) {
              evaluateString((BytesColumnVector) inputColVector1, outV, i, repeatedNumDays);
            }
          }
        } else if (batch.selectedInUse) {
          for (int j = 0; j != n; j++) {
            int i = sel[j];
            evaluateString((BytesColumnVector) inputColVector1, outV, i, vector2[i]);
          }
        } else {
          for (int i = 0; i != n; i++) {
            evaluateString((BytesColumnVector) inputColVector1, outV, i, vector2[i]);
          }
        }
        break;
      default:
        throw new Error("Unsupported input type " + primitiveCategory.name());
    }
  }

  protected void evaluateRepeatedCommon(long days, long[] vector2, LongColumnVector outputVector,
      boolean selectedInUse, int[] selected, int n) {
    if (isPositive) {
      if (selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = selected[j];
          outputVector.vector[i] = days + vector2[i];
        }
      } else {
        for (int i = 0; i != n; i++) {
          outputVector.vector[i] = days + vector2[i];
        }
      }
    } else {
      if (selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = selected[j];
          outputVector.vector[i] = days - vector2[i];
        }
      } else {
        for (int i = 0; i != n; i++) {
          outputVector.vector[i] = days - vector2[i];
        }
      }
    }
  }

  protected long evaluateDate(ColumnVector columnVector, int index, long numDays) {
    LongColumnVector lcv = (LongColumnVector) columnVector;
    long days = lcv.vector[index];
    if (isPositive) {
      days += numDays;
    } else {
      days -= numDays;
    }
    return days;
  }

  protected void evaluateRepeatedDate(ColumnVector columnVector,
      long[] vector2, LongColumnVector outputVector,
      boolean selectedInUse, int[] selected, int n) {
    if (columnVector.isNull[0]) {
      outputVector.noNulls = false;
      outputVector.isNull[0] = true;
      outputVector.isRepeating = true;
      return;
    }
    LongColumnVector lcv = (LongColumnVector) columnVector;
    long days = lcv.vector[0];

    evaluateRepeatedCommon(days, vector2, outputVector, selectedInUse, selected, n);
  }

  protected long evaluateTimestamp(ColumnVector columnVector, int index, long numDays) {
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

  protected void evaluateRepeatedTimestamp(ColumnVector columnVector,
      long[] vector2, LongColumnVector outputVector,
      boolean selectedInUse, int[] selected, int n) {
    if (columnVector.isNull[0]) {
      outputVector.noNulls = false;
      outputVector.isNull[0] = true;
      outputVector.isRepeating = true;
      return;
    }
    TimestampColumnVector tcv = (TimestampColumnVector) columnVector;
    // Convert to date value (in days)
    long days = DateWritableV2.millisToDays(tcv.getTime(0));

    evaluateRepeatedCommon(days, vector2, outputVector, selectedInUse, selected, n);
  }

  protected void evaluateString(BytesColumnVector inputColumnVector1, LongColumnVector outputVector, int index, long numDays) {
    if (inputColumnVector1.isNull[index]) {
      outputVector.noNulls = false;
      outputVector.isNull[index] = true;
    } else {
      text.set(inputColumnVector1.vector[index], inputColumnVector1.start[index], inputColumnVector1.length[index]);
      Date hDate = DateParser.parseDate(text.toString());
      if (hDate == null) {
        outputVector.noNulls = false;
        outputVector.isNull[index] = true;
        return;
      }
      long days = DateWritableV2.millisToDays(hDate.toEpochMilli());
      if (isPositive) {
        days += numDays;
      } else {
        days -= numDays;
      }
      outputVector.vector[index] = days;
    }
  }

  protected void evaluateRepeatedString(BytesColumnVector inputColumnVector1,
      long[] vector2, LongColumnVector outputVector,
      boolean selectedInUse, int[] selected, int n) {
    if (inputColumnVector1.isNull[0]) {
      outputVector.noNulls = false;
      outputVector.isNull[0] = true;
      outputVector.isRepeating = true;
      return;
    }
    text.set(
        inputColumnVector1.vector[0], inputColumnVector1.start[0], inputColumnVector1.length[0]);
    Date date = DateParser.parseDate(text.toString());
    if (date == null) {
      outputVector.noNulls = false;
      outputVector.isNull[0] = true;
      outputVector.isRepeating = true;
      return;
    }
    long days = DateWritableV2.millisToDays(date.toEpochMilli());

    evaluateRepeatedCommon(days, vector2, outputVector, selectedInUse, selected, n);
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
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }
}
