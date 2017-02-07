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

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hive.common.util.DateParser;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;


public class VectorUDFDateAddScalarCol extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private int colNum;
  private int outputColumn;
  private long longValue = 0;
  private Timestamp timestampValue = null;
  private byte[] stringValue = null;
  protected boolean isPositive = true;
  private transient final DateParser dateParser = new DateParser();
  private transient final Date baseDate = new Date(0);

  public VectorUDFDateAddScalarCol() {
    super();
  }

  public VectorUDFDateAddScalarCol(Object object, int colNum, int outputColumn) {
    this();
    this.colNum = colNum;
    this.outputColumn = outputColumn;

    if (object instanceof Long) {
      this.longValue = (Long) object;
    } else if (object instanceof Timestamp) {
        this.timestampValue = (Timestamp) object;
    } else if (object instanceof byte []) {
      this.stringValue = (byte[]) object;
    } else {
      throw new RuntimeException("Unexpected scalar object " + object.getClass().getName() + " " + object.toString());
    }
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector inputCol = (LongColumnVector) batch.cols[this.colNum];
    /* every line below this is identical for evaluateLong & evaluateString */
    final int n = inputCol.isRepeating ? 1 : batch.size;
    int[] sel = batch.selected;
    final boolean selectedInUse = (inputCol.isRepeating == false) && batch.selectedInUse;
    LongColumnVector outV = (LongColumnVector) batch.cols[outputColumn];

    switch (inputTypes[0]) {
      case DATE:
        baseDate.setTime(DateWritable.daysToMillis((int) longValue));
        break;

      case TIMESTAMP:
        baseDate.setTime(timestampValue.getTime());
        break;

      case STRING:
      case CHAR:
      case VARCHAR:
        boolean parsed = dateParser.parseDate(new String(stringValue, StandardCharsets.UTF_8), baseDate);
        if (!parsed) {
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
        break;
      default:
        throw new Error("Unsupported input type " + inputTypes[0].name());
    }

    if(batch.size == 0) {
      /* n != batch.size when isRepeating */
      return;
    }

    /* true for all algebraic UDFs with no state */
    outV.isRepeating = inputCol.isRepeating;

    long baseDateDays = DateWritable.millisToDays(baseDate.getTime());
    if (inputCol.noNulls) {
      outV.noNulls = true;
      if (selectedInUse) {
        for(int j=0; j < n; j++) {
          int i = sel[j];
          evaluate(baseDateDays, inputCol.vector[i], outV, i);
        }
      } else {
        for(int i = 0; i < n; i++) {
          evaluate(baseDateDays, inputCol.vector[i], outV, i);
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
            evaluate(baseDateDays, inputCol.vector[i], outV, i);
          }
        }
      } else {
        for(int i = 0; i < n; i++) {
          outV.isNull[i] = inputCol.isNull[i];
          if (!inputCol.isNull[i]) {
            evaluate(baseDateDays, inputCol.vector[i], outV, i);
          }
        }
      }
    }
  }

  private void evaluate(long baseDateDays, long numDays, LongColumnVector output, int i) {
    long result = baseDateDays;
    if (isPositive) {
      result += numDays;
    } else {
      result -= numDays;
    }
    output.vector[i] = result;
  }

  @Override
  public int getOutputColumn() {
    return this.outputColumn;
  }

  @Override
  public String getOutputType() {
    return "date";
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

  public void setLongValue(long longValue) {
    this.longValue = longValue;
  }

  public byte[] getStringValue() {
    return stringValue;
  }

  public void setStringValue(byte[] stringValue) {
    this.stringValue = stringValue;
  }

  public boolean isPositive() {
    return isPositive;
  }

  public void setPositive(boolean isPositive) {
    this.isPositive = isPositive;
  }

  @Override
  public String vectorExpressionParameters() {
    return "val " + stringValue + ", col " + colNum;
  }

  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.STRING_DATETIME_FAMILY,
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.SCALAR,
            VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }
}
