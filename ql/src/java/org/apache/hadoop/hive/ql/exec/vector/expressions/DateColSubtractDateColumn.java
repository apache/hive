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

import java.sql.Timestamp;

import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.util.DateTimeMath;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;

// A type date (LongColumnVector storing epoch days) minus a type date produces a
// type interval_day_time (IntervalDayTimeColumnVector storing nanosecond interval in 2 longs).
public class DateColSubtractDateColumn extends VectorExpression {

  private static final long serialVersionUID = 1L;

  private transient final Timestamp scratchTimestamp1 = new Timestamp(0);
  private transient final Timestamp scratchTimestamp2 = new Timestamp(0);
  private transient final DateTimeMath dtm = new DateTimeMath();

  public DateColSubtractDateColumn(int colNum1, int colNum2, int outputColumnNum) {
    super(colNum1, colNum2, outputColumnNum);
  }

  public DateColSubtractDateColumn() {
    super();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    // Input #1 is type date (epochDays).
    LongColumnVector inputColVector1 = (LongColumnVector) batch.cols[inputColumnNum[0]];

    // Input #2 is type date (epochDays).
    LongColumnVector inputColVector2 = (LongColumnVector) batch.cols[inputColumnNum[1]];

    // Output is type interval_day_time.
    IntervalDayTimeColumnVector outputColVector = (IntervalDayTimeColumnVector) batch.cols[outputColumnNum];

    int[] sel = batch.selected;
    int n = batch.size;
    long[] vector1 = inputColVector1.vector;
    long[] vector2 = inputColVector2.vector;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    /*
     * Propagate null values for a two-input operator and set isRepeating and noNulls appropriately.
     */
    NullUtil.propagateNullsColCol(
      inputColVector1, inputColVector2, outputColVector, sel, n, batch.selectedInUse);

    HiveIntervalDayTime resultIntervalDayTime = outputColVector.getScratchIntervalDayTime();

    /* Disregard nulls for processing. In other words,
     * the arithmetic operation is performed even if one or
     * more inputs are null. This is to improve speed by avoiding
     * conditional checks in the inner loop.
     */
    if (inputColVector1.isRepeating && inputColVector2.isRepeating) {
      scratchTimestamp1.setTime(DateWritableV2.daysToMillis((int) vector1[0]));
      scratchTimestamp2.setTime(DateWritableV2.daysToMillis((int) vector2[0]));
      dtm.subtract(scratchTimestamp1, scratchTimestamp2, outputColVector.getScratchIntervalDayTime());
      outputColVector.setFromScratchIntervalDayTime(0);
    } else if (inputColVector1.isRepeating) {
      scratchTimestamp1.setTime(DateWritableV2.daysToMillis((int) vector1[0]));
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          scratchTimestamp2.setTime(DateWritableV2.daysToMillis((int) vector2[i]));
          dtm.subtract(scratchTimestamp1, scratchTimestamp2, outputColVector.getScratchIntervalDayTime());
          outputColVector.setFromScratchIntervalDayTime(i);
        }
      } else {
        for(int i = 0; i != n; i++) {
          scratchTimestamp2.setTime(DateWritableV2.daysToMillis((int) vector2[i]));
          dtm.subtract(scratchTimestamp1, scratchTimestamp2, outputColVector.getScratchIntervalDayTime());
          outputColVector.setFromScratchIntervalDayTime(i);
        }
      }
    } else if (inputColVector2.isRepeating) {
      scratchTimestamp2.setTime(DateWritableV2.daysToMillis((int) vector2[0]));
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          scratchTimestamp1.setTime(DateWritableV2.daysToMillis((int) vector1[i]));
          dtm.subtract(scratchTimestamp1, scratchTimestamp2, outputColVector.getScratchIntervalDayTime());
          outputColVector.setFromScratchIntervalDayTime(i);
        }
      } else {
        for(int i = 0; i != n; i++) {
          scratchTimestamp1.setTime(DateWritableV2.daysToMillis((int) vector1[i]));
          dtm.subtract(scratchTimestamp1, scratchTimestamp2, outputColVector.getScratchIntervalDayTime());
          outputColVector.setFromScratchIntervalDayTime(i);
        }
      }
    } else {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          scratchTimestamp1.setTime(DateWritableV2.daysToMillis((int) vector1[i]));
          scratchTimestamp2.setTime(DateWritableV2.daysToMillis((int) vector2[i]));
          dtm.subtract(scratchTimestamp1, scratchTimestamp2, outputColVector.getScratchIntervalDayTime());
          outputColVector.setFromScratchIntervalDayTime(i);
        }
      } else {
        for(int i = 0; i != n; i++) {
          scratchTimestamp1.setTime(DateWritableV2.daysToMillis((int) vector1[i]));
          scratchTimestamp2.setTime(DateWritableV2.daysToMillis((int) vector2[i]));
          dtm.subtract(scratchTimestamp1, scratchTimestamp2, outputColVector.getScratchIntervalDayTime());
          outputColVector.setFromScratchIntervalDayTime(i);
        }
      }
    }

    /* For the case when the output can have null values, follow
     * the convention that the data values must be 1 for long and
     * NaN for double. This is to prevent possible later zero-divide errors
     * in complex arithmetic expressions like col2 / (col1 - 1)
     * in the case when some col1 entries are null.
     */
    NullUtil.setNullDataEntriesIntervalDayTime(outputColVector, batch.selectedInUse, sel, n);
  }

  public String vectorExpressionParameters() {
    return getColumnParamString(0, inputColumnNum[0]) + ", " + getColumnParamString(1, inputColumnNum[1]);
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
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }
}

