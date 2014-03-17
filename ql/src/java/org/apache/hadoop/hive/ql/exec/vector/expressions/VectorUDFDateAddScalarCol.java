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
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.Text;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

public class VectorUDFDateAddScalarCol extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private int colNum;
  private int outputColumn;
  private long longValue = 0;
  private byte[] stringValue = null;
  protected boolean isPositive = true;
  private transient final Calendar calendar = Calendar.getInstance();
  private transient SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
  private transient final Text text = new Text();
  private transient Date baseDate = new Date();

  public VectorUDFDateAddScalarCol() {
    super();
  }

  public VectorUDFDateAddScalarCol(Object object, int colNum, int outputColumn) {
    this();
    this.colNum = colNum;
    this.outputColumn = outputColumn;

    if (object instanceof Long) {
      this.longValue = (Long) object;
    } else if (object instanceof byte []) {
      this.stringValue = (byte[]) object;
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
    BytesColumnVector outV = (BytesColumnVector) batch.cols[outputColumn];

    switch (inputTypes[0]) {
      case DATE:
        baseDate.setTime(DateWritable.daysToMillis((int) longValue));
        break;

      case TIMESTAMP:
        baseDate.setTime(longValue / 1000000);
        break;

      case STRING:
        try {
          baseDate = formatter.parse(new String(stringValue, "UTF-8"));
          break;
        } catch (Exception e) {
          outV.noNulls = false;
          if (batch.selectedInUse) {
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
    }

    if(batch.size == 0) {
      /* n != batch.size when isRepeating */
      return;
    }

    /* true for all algebraic UDFs with no state */
    outV.isRepeating = inputCol.isRepeating;

    if (inputCol.noNulls) {
      outV.noNulls = true;
      if (batch.selectedInUse) {
        for(int j=0; j < n; j++) {
          int i = sel[j];
          evaluate(baseDate, inputCol.vector[i], outV, i);
        }
      } else {
        for(int i = 0; i < n; i++) {
          evaluate(baseDate, inputCol.vector[i], outV, i);
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
            evaluate(baseDate, inputCol.vector[i], outV, i);
          }
        }
      } else {
        for(int i = 0; i < n; i++) {
          outV.isNull[i] = inputCol.isNull[i];
          if (!inputCol.isNull[i]) {
            evaluate(baseDate, inputCol.vector[i], outV, i);
          }
        }
      }
    }
  }

  private void evaluate(Date baseDate, long numDays, BytesColumnVector output, int i) {
    calendar.setTime(baseDate);

    if (isPositive) {
      calendar.add(Calendar.DATE, (int) numDays);
    } else {
      calendar.add(Calendar.DATE, -(int) numDays);
    }
    Date newDate = calendar.getTime();
    text.set(formatter.format(newDate));
    int size = text.getLength();
    output.vector[i] = Arrays.copyOf(text.getBytes(), size);
    output.start[i] = 0;
    output.length[i] = size;
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

  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.ANY,
            VectorExpressionDescriptor.ArgumentType.LONG)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.SCALAR,
            VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }
}
