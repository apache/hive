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

import org.apache.hadoop.hive.metastore.parser.ExpressionTree.Operator;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.Text;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class VectorUDFDateDiffColScalar extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private int colNum;
  private int outputColumn;
  private long longValue;
  private Timestamp timestampValue;
  private byte[] bytesValue;
  private transient SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
  private transient final Text text = new Text();
  private int baseDate;
  private transient Date date = new Date(0);

  public VectorUDFDateDiffColScalar(int colNum, Object object, int outputColumn) {
    super();
    this.colNum = colNum;
    this.outputColumn = outputColumn;

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
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector outV = (LongColumnVector) batch.cols[outputColumn];
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

    switch (inputTypes[1]) {
      case DATE:
        baseDate = (int) longValue;
        break;

      case TIMESTAMP:
        date.setTime(timestampValue.getTime());
        baseDate = DateWritable.dateToDays(date);
        break;

      case STRING:
      case CHAR:
      case VARCHAR:
        try {
          date.setTime(formatter.parse(new String(bytesValue, "UTF-8")).getTime());
          baseDate = DateWritable.dateToDays(date);
          break;
        } catch (Exception e) {
          outV.noNulls = false;
          if (selectedInUse) {
            for(int j=0; j < n; j++) {
              int i = sel[j];
              outV.isNull[i] = true;
            }
          } else {
            for(int i = 0; i < n; i++) {
              outV.isNull[i] = true;
            }
          }
          return;
        }
    default:
        throw new Error("Invalid input type #1: " + inputTypes[1].name());
    }

    switch (inputTypes[0]) {
      case DATE:
        if (inputCol.noNulls) {
          outV.noNulls = true;
          if (selectedInUse) {
            for(int j=0; j < n; j++) {
              int i = sel[j];
              outV.vector[i] = evaluateDate(inputCol, i);
            }
          } else {
            for(int i = 0; i < n; i++) {
              outV.vector[i] = evaluateDate(inputCol, i);
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
              }
            }
          } else {
            for(int i = 0; i < n; i++) {
              outV.isNull[i] = inputCol.isNull[i];
              if (!inputCol.isNull[i]) {
                outV.vector[i] = evaluateDate(inputCol, i);
              }
            }
          }
        }
        break;

      case TIMESTAMP:
        if (inputCol.noNulls) {
          outV.noNulls = true;
          if (selectedInUse) {
            for(int j=0; j < n; j++) {
              int i = sel[j];
              outV.vector[i] = evaluateTimestamp(inputCol, i);
            }
          } else {
            for(int i = 0; i < n; i++) {
              outV.vector[i] = evaluateTimestamp(inputCol, i);
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
                outV.vector[i] = evaluateTimestamp(inputCol, i);
              }
            }
          } else {
            for(int i = 0; i < n; i++) {
              outV.isNull[i] = inputCol.isNull[i];
              if (!inputCol.isNull[i]) {
                outV.vector[i] = evaluateTimestamp(inputCol, i);
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
          if (selectedInUse) {
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
          if (selectedInUse) {
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
      throw new Error("Invalid input type #0: " + inputTypes[0].name());
    }
  }

  protected int evaluateTimestamp(ColumnVector columnVector, int index) {
    TimestampColumnVector tcv = (TimestampColumnVector) columnVector;
    date.setTime(tcv.getTime(index));
    return DateWritable.dateToDays(date) - baseDate;
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
      output.vector[i] = DateWritable.dateToDays(date) - baseDate;
    } catch (ParseException e) {
      output.vector[i] = 1;
      output.isNull[i] = true;
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
    return "col " + colNum + ", val " + displayUtf8Bytes(bytesValue);
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
