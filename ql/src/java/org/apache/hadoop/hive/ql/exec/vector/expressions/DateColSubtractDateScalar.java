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

import org.apache.hadoop.hive.common.type.PisaTimestamp;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.NullUtil;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.ql.util.DateTimeMath;
import org.apache.hadoop.hive.serde2.io.DateWritable;

// A type date (LongColumnVector storing epoch days) minus a type date produces a
// type interval_day_time (TimestampColumnVector storing nanosecond interval in 2 longs).
public class DateColSubtractDateScalar extends VectorExpression {

  private static final long serialVersionUID = 1L;

  private int colNum;
  private PisaTimestamp value;
  private int outputColumn;
  private PisaTimestamp scratchPisaTimestamp;
  private DateTimeMath dtm = new DateTimeMath();

  public DateColSubtractDateScalar(int colNum, long value, int outputColumn) {
    this.colNum = colNum;
    this.value = new PisaTimestamp().updateFromTimestampMilliseconds(DateWritable.daysToMillis((int) value));
    this.outputColumn = outputColumn;
    scratchPisaTimestamp = new PisaTimestamp();
  }

  public DateColSubtractDateScalar() {
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    // Input #1 is type date (epochDays).
    LongColumnVector inputColVector1 = (LongColumnVector) batch.cols[colNum];

    // Output is type Timestamp.
    TimestampColumnVector outputColVector = (TimestampColumnVector) batch.cols[outputColumn];

    int[] sel = batch.selected;
    boolean[] inputIsNull = inputColVector1.isNull;
    boolean[] outputIsNull = outputColVector.isNull;
    outputColVector.noNulls = inputColVector1.noNulls;
    outputColVector.isRepeating = inputColVector1.isRepeating;
    int n = batch.size;
    long[] vector1 = inputColVector1.vector;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    if (inputColVector1.isRepeating) {
      outputColVector.subtract(
          scratchPisaTimestamp.updateFromTimestampMilliseconds(DateWritable.daysToMillis((int) vector1[0])),
          value,
          0);
      // Even if there are no nulls, we always copy over entry 0. Simplifies code.
      outputIsNull[0] = inputIsNull[0];
    } else if (inputColVector1.noNulls) {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputColVector.subtract(
              scratchPisaTimestamp.updateFromTimestampMilliseconds(DateWritable.daysToMillis((int) vector1[0])),
              value,
              i);
        }
      } else {
        for(int i = 0; i != n; i++) {
          outputColVector.subtract(
              scratchPisaTimestamp.updateFromTimestampMilliseconds(DateWritable.daysToMillis((int) vector1[0])),
              value,
              i);
        }
      }
    } else /* there are nulls */ {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputColVector.subtract(
              scratchPisaTimestamp.updateFromTimestampMilliseconds(DateWritable.daysToMillis((int) vector1[0])),
              value,
              i);
          outputIsNull[i] = inputIsNull[i];
        }
      } else {
        for(int i = 0; i != n; i++) {
          outputColVector.subtract(
              scratchPisaTimestamp.updateFromTimestampMilliseconds(DateWritable.daysToMillis((int) vector1[0])),
              value,
              i);
        }
        System.arraycopy(inputIsNull, 0, outputIsNull, 0, n);
      }
    }

    NullUtil.setNullOutputEntriesColScalar(outputColVector, batch.selectedInUse, sel, n);
  }

  @Override
  public int getOutputColumn() {
    return outputColumn;
  }

  @Override
  public String getOutputType() {
    return "timestamp";
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.getType("date"),
            VectorExpressionDescriptor.ArgumentType.getType("date"))
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR).build();
  }
}
