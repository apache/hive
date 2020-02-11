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
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.util.DateTimeMath;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;

// A type date (LongColumnVector storing epoch days) minus a type date produces a
// type interval_day_time (TimestampColumnVector storing nanosecond interval in 2 longs).
public class DateScalarSubtractDateColumn extends VectorExpression {

  private static final long serialVersionUID = 1L;

  private final Timestamp value;
  private final int colNum;

  private transient final Timestamp scratchTimestamp2 = new Timestamp(0);
  private transient final DateTimeMath dtm = new DateTimeMath();

  public DateScalarSubtractDateColumn(long value, int colNum, int outputColumnNum) {
    super(outputColumnNum);
    this.colNum = colNum;
    this.value = new Timestamp(0);
    this.value.setTime(DateWritableV2.daysToMillis((int) value));
  }

  public DateScalarSubtractDateColumn() {
    super();

    // Dummy final assignments.
    value = null;
    colNum = -1;
  }

  @Override
  /**
   * Method to evaluate scalar-column operation in vectorized fashion.
   *
   * @batch a package of rows with each column stored in a vector
   */
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    // Input #2 is type date (epochDays).
    LongColumnVector inputColVector2 = (LongColumnVector) batch.cols[colNum];

    // Output is type HiveIntervalDayTime.
    IntervalDayTimeColumnVector outputColVector = (IntervalDayTimeColumnVector) batch.cols[outputColumnNum];

    int[] sel = batch.selected;
    boolean[] inputIsNull = inputColVector2.isNull;
    boolean[] outputIsNull = outputColVector.isNull;
    int n = batch.size;

    long[] vector2 = inputColVector2.vector;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    if (inputColVector2.isRepeating) {
      if (inputColVector2.noNulls || !inputIsNull[0]) {
        outputIsNull[0] = false;
        scratchTimestamp2.setTime(DateWritableV2.daysToMillis((int) vector2[0]));
        dtm.subtract(value, scratchTimestamp2, outputColVector.getScratchIntervalDayTime());
        outputColVector.setFromScratchIntervalDayTime(0);
      } else {
        outputIsNull[0] = true;
        outputColVector.noNulls = false;
      }
      outputColVector.isRepeating = true;
      NullUtil.setNullOutputEntriesColScalar(outputColVector, batch.selectedInUse, sel, n);
      return;
    }

    if (inputColVector2.noNulls) {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputIsNull[i] = false;
          scratchTimestamp2.setTime(DateWritableV2.daysToMillis((int) vector2[i]));
          dtm.subtract(value, scratchTimestamp2, outputColVector.getScratchIntervalDayTime());
          outputColVector.setFromScratchIntervalDayTime(i);
        }
      } else {
        Arrays.fill(outputIsNull, 0, n, false);
        for(int i = 0; i != n; i++) {
          scratchTimestamp2.setTime(DateWritableV2.daysToMillis((int) vector2[i]));
          dtm.subtract(value, scratchTimestamp2, outputColVector.getScratchIntervalDayTime());
          outputColVector.setFromScratchIntervalDayTime(i);
        }
      }
    } else /* there are NULLs in the inputColVector */ {

      // Carefully handle NULLs...
      outputColVector.noNulls = false;

      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputIsNull[i] = inputIsNull[i];
          scratchTimestamp2.setTime(DateWritableV2.daysToMillis((int) vector2[i]));
          dtm.subtract(value, scratchTimestamp2, outputColVector.getScratchIntervalDayTime());
          outputColVector.setFromScratchIntervalDayTime(i);
        }
      } else {
        System.arraycopy(inputIsNull, 0, outputIsNull, 0, n);
        for(int i = 0; i != n; i++) {
          scratchTimestamp2.setTime(DateWritableV2.daysToMillis((int) vector2[i]));
          dtm.subtract(value, scratchTimestamp2, outputColVector.getScratchIntervalDayTime());
          outputColVector.setFromScratchIntervalDayTime(i);
        }
      }
    }

    NullUtil.setNullOutputEntriesColScalar(outputColVector, batch.selectedInUse, sel, n);
  }

  @Override
  public String vectorExpressionParameters() {
    return "val " + org.apache.hadoop.hive.common.type.Date.ofEpochMilli(value.getTime()) + ", " + getColumnParamString(1, colNum);
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
