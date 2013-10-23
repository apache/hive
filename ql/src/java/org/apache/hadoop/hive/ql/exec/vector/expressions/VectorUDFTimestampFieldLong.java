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

import java.sql.Timestamp;
import java.util.Calendar;

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * Abstract class to return various fields from a Timestamp.
 */
public abstract class VectorUDFTimestampFieldLong extends VectorExpression {

  private static final long serialVersionUID = 1L;

  protected int colNum;
  protected int outputColumn;
  protected int field;
  protected transient final Calendar calendar = Calendar.getInstance();
  protected transient final Timestamp ts = new Timestamp(0);

  public VectorUDFTimestampFieldLong(int field, int colNum, int outputColumn) {
    this();
    this.colNum = colNum;
    this.outputColumn = outputColumn;
    this.field = field;
  }

  public VectorUDFTimestampFieldLong() {
    super();
  }

  protected final Timestamp getTimestamp(long nanos) {
    /*
     * new Timestamp() stores the millisecond precision values in the nanos field.
     * If you wanted to store 200ms it will result in nanos being set to 200*1000*1000.
     * When you call setNanos(0), because there are no sub-ms times, it will set it to 0,
     * ending up with a Timestamp which refers to 0ms by accident.
     * CAVEAT: never use a sub-second value in new Timestamp() args, just use setNanos to set it.
     */
    long ms = (nanos / (1000 * 1000 * 1000)) * 1000;
    /* the milliseconds should be kept in nanos */
    long ns = nanos % (1000*1000*1000);
    if (ns < 0) {
      /*
       * Due to the way java.sql.Timestamp stores sub-second values, it throws an exception
       * if nano seconds are negative. The timestamp implementation handles this by using
       * negative milliseconds and adjusting the nano seconds up by the same to be positive.
       * Read Timestamp.java:setTime() implementation for this code.
       */
      ms -= 1000;
      ns += 1000*1000*1000;
    }
    ts.setTime(ms);
    ts.setNanos((int) ns);
    return ts;
  }

  protected long getField(long time) {
    calendar.setTime(getTimestamp(time));
    return calendar.get(field);
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {
    LongColumnVector outV = (LongColumnVector) batch.cols[outputColumn];
    LongColumnVector inputCol = (LongColumnVector)batch.cols[this.colNum];
    /* every line below this is identical for evaluateLong & evaluateString */
    final int n = inputCol.isRepeating ? 1 : batch.size;
    int[] sel = batch.selected;

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
          outV.vector[i] = getField(inputCol.vector[i]);
        }
      } else {
        for(int i = 0; i < n; i++) {
          outV.vector[i] = getField(inputCol.vector[i]);
        }
      }
    } else {
      // Handle case with nulls. Don't do function if the value is null, to save time,
      // because calling the function can be expensive.
      outV.noNulls = false;
      if (batch.selectedInUse) {
        for(int j=0; j < n; j++) {
          int i = sel[j];
          outV.isNull[i] = inputCol.isNull[i];
          if (!inputCol.isNull[i]) {
            outV.vector[i] = getField(inputCol.vector[i]);
          }
        }
      } else {
        for(int i = 0; i < n; i++) {
          outV.isNull[i] = inputCol.isNull[i];
          if (!inputCol.isNull[i]) {
            outV.vector[i] = getField(inputCol.vector[i]);
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

  public int getColNum() {
    return colNum;
  }

  public void setColNum(int colNum) {
    this.colNum = colNum;
  }

  public int getField() {
    return field;
  }

  public void setField(int field) {
    this.field = field;
  }

  public void setOutputColumn(int outputColumn) {
    this.outputColumn = outputColumn;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(1)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.LONG)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }
}
