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

import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.ql.exec.vector.expressions.NullUtil;
import org.apache.hadoop.hive.ql.util.DateTimeMath;
import org.apache.hadoop.hive.serde2.io.DateWritable;

// A type date (LongColumnVector storing epoch days) minus a type date produces a
// type interval_day_time (TimestampColumnVector storing nanosecond interval in 2 longs).
public class DateScalarSubtractDateColumn extends VectorExpression {

  private static final long serialVersionUID = 1L;

  private int colNum;
  private Timestamp value;
  private int outputColumn;
  private Timestamp scratchTimestamp2;
  private DateTimeMath dtm = new DateTimeMath();

  public DateScalarSubtractDateColumn(long value, int colNum, int outputColumn) {
    this.colNum = colNum;
    this.value = new Timestamp(0);
    this.value.setTime(DateWritable.daysToMillis((int) value));
    this.outputColumn = outputColumn;
    scratchTimestamp2 = new Timestamp(0);
  }

  public DateScalarSubtractDateColumn() {
  }

  @Override
  /**
   * Method to evaluate scalar-column operation in vectorized fashion.
   *
   * @batch a package of rows with each column stored in a vector
   */
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    // Input #2 is type date (epochDays).
    LongColumnVector inputColVector2 = (LongColumnVector) batch.cols[colNum];

    // Output is type HiveIntervalDayTime.
    IntervalDayTimeColumnVector outputColVector = (IntervalDayTimeColumnVector) batch.cols[outputColumn];

    int[] sel = batch.selected;
    boolean[] inputIsNull = inputColVector2.isNull;
    boolean[] outputIsNull = outputColVector.isNull;
    outputColVector.noNulls = inputColVector2.noNulls;
    outputColVector.isRepeating = inputColVector2.isRepeating;
    int n = batch.size;

    long[] vector2 = inputColVector2.vector;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    if (inputColVector2.isRepeating) {
      scratchTimestamp2.setTime(DateWritable.daysToMillis((int) vector2[0]));
      dtm.subtract(value, scratchTimestamp2, outputColVector.getScratchIntervalDayTime());
      outputColVector.setFromScratchIntervalDayTime(0);
      // Even if there are no nulls, we always copy over entry 0. Simplifies code.
      outputIsNull[0] = inputIsNull[0];
    } else if (inputColVector2.noNulls) {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          scratchTimestamp2.setTime(DateWritable.daysToMillis((int) vector2[i]));
          dtm.subtract(value, scratchTimestamp2, outputColVector.getScratchIntervalDayTime());
          outputColVector.setFromScratchIntervalDayTime(i);
        }
      } else {
        for(int i = 0; i != n; i++) {
          scratchTimestamp2.setTime(DateWritable.daysToMillis((int) vector2[i]));
          dtm.subtract(value, scratchTimestamp2, outputColVector.getScratchIntervalDayTime());
          outputColVector.setFromScratchIntervalDayTime(i);
        }
      }
    } else {                         /* there are nulls */
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          scratchTimestamp2.setTime(DateWritable.daysToMillis((int) vector2[i]));
          dtm.subtract(value, scratchTimestamp2, outputColVector.getScratchIntervalDayTime());
          outputColVector.setFromScratchIntervalDayTime(i);
          outputIsNull[i] = inputIsNull[i];
        }
      } else {
        for(int i = 0; i != n; i++) {
          scratchTimestamp2.setTime(DateWritable.daysToMillis((int) vector2[i]));
          dtm.subtract(value, scratchTimestamp2, outputColVector.getScratchIntervalDayTime());
          outputColVector.setFromScratchIntervalDayTime(i);
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
  public String vectorExpressionParameters() {
    return "val " + value + ", col " + colNum;
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
            VectorExpressionDescriptor.InputExpressionType.SCALAR,
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }
}
