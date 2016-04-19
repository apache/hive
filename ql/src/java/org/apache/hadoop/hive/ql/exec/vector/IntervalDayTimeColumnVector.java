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
package org.apache.hadoop.hive.ql.exec.vector;

import java.util.Arrays;

import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.io.Writable;

/**
 * This class represents a nullable interval day time column vector capable of handing a
 * wide range of interval day time values.
 *
 * We store the 2 (value) fields of a HiveIntervalDayTime class in primitive arrays.
 *
 * We do this to avoid an array of Java HiveIntervalDayTime objects which would have poor storage
 * and memory access characteristics.
 *
 * Generally, the caller will fill in a scratch HiveIntervalDayTime object with values from a row,
 * work using the scratch HiveIntervalDayTime, and then perhaps update the column vector row
 * with a result.
 */
public class IntervalDayTimeColumnVector extends ColumnVector {

  /*
   * The storage arrays for this column vector corresponds to the storage of a HiveIntervalDayTime:
   */
  private long[] totalSeconds;
      // The values from HiveIntervalDayTime.getTotalSeconds().

  private int[] nanos;
      // The values from HiveIntervalDayTime.getNanos().

  /*
   * Scratch objects.
   */
  private final HiveIntervalDayTime scratchIntervalDayTime;

  private Writable scratchWritable;
      // Supports keeping a HiveIntervalDayTimeWritable object without having to import
      // that definition...

  /**
   * Use this constructor by default. All column vectors
   * should normally be the default size.
   */
  public IntervalDayTimeColumnVector() {
    this(VectorizedRowBatch.DEFAULT_SIZE);
  }

  /**
   * Don't use this except for testing purposes.
   *
   * @param len the number of rows
   */
  public IntervalDayTimeColumnVector(int len) {
    super(len);

    totalSeconds = new long[len];
    nanos = new int[len];

    scratchIntervalDayTime = new HiveIntervalDayTime();

    scratchWritable = null;     // Allocated by caller.
  }

  /**
   * Return the number of rows.
   * @return
   */
  public int getLength() {
    return totalSeconds.length;
  }

  /**
   * Return a row's HiveIntervalDayTime.getTotalSeconds() value.
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param elementNum
   * @return
   */
  public long getTotalSeconds(int elementNum) {
    return totalSeconds[elementNum];
  }

  /**
   * Return a row's HiveIntervalDayTime.getNanos() value.
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param elementNum
   * @return
   */
  public long getNanos(int elementNum) {
    return nanos[elementNum];
  }

  /**
   * Return a row's HiveIntervalDayTime.getDouble() value.
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param elementNum
   * @return
   */
  public double getDouble(int elementNum) {
    return asScratchIntervalDayTime(elementNum).getDouble();
  }

  /**
   * Set a HiveIntervalDayTime object from a row of the column.
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param intervalDayTime
   * @param elementNum
   */
  public void intervalDayTimeUpdate(HiveIntervalDayTime intervalDayTime, int elementNum) {
    intervalDayTime.set(totalSeconds[elementNum], nanos[elementNum]);
  }


  /**
   * Return the scratch HiveIntervalDayTime object set from a row.
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param elementNum
   * @return
   */
  public HiveIntervalDayTime asScratchIntervalDayTime(int elementNum) {
    scratchIntervalDayTime.set(totalSeconds[elementNum], nanos[elementNum]);
    return scratchIntervalDayTime;
  }

  /**
   * Return the scratch HiveIntervalDayTime (contents undefined).
   * @return
   */
  public HiveIntervalDayTime getScratchIntervalDayTime() {
    return scratchIntervalDayTime;
  }

  /**
   * Compare row to HiveIntervalDayTime.
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param elementNum
   * @param intervalDayTime
   * @return -1, 0, 1 standard compareTo values.
   */
  public int compareTo(int elementNum, HiveIntervalDayTime intervalDayTime) {
    return asScratchIntervalDayTime(elementNum).compareTo(intervalDayTime);
  }

  /**
   * Compare HiveIntervalDayTime to row.
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param intervalDayTime
   * @param elementNum
   * @return -1, 0, 1 standard compareTo values.
   */
  public int compareTo(HiveIntervalDayTime intervalDayTime, int elementNum) {
    return intervalDayTime.compareTo(asScratchIntervalDayTime(elementNum));
  }

  /**
   * Compare a row to another TimestampColumnVector's row.
   * @param elementNum1
   * @param intervalDayTimeColVector2
   * @param elementNum2
   * @return
   */
  public int compareTo(int elementNum1, IntervalDayTimeColumnVector intervalDayTimeColVector2,
      int elementNum2) {
    return asScratchIntervalDayTime(elementNum1).compareTo(
        intervalDayTimeColVector2.asScratchIntervalDayTime(elementNum2));
  }

  /**
   * Compare another TimestampColumnVector's row to a row.
   * @param intervalDayTimeColVector1
   * @param elementNum1
   * @param elementNum2
   * @return
   */
  public int compareTo(IntervalDayTimeColumnVector intervalDayTimeColVector1, int elementNum1,
      int elementNum2) {
    return intervalDayTimeColVector1.asScratchIntervalDayTime(elementNum1).compareTo(
        asScratchIntervalDayTime(elementNum2));
  }

  @Override
  public void setElement(int outElementNum, int inputElementNum, ColumnVector inputVector) {

    IntervalDayTimeColumnVector timestampColVector = (IntervalDayTimeColumnVector) inputVector;

    totalSeconds[outElementNum] = timestampColVector.totalSeconds[inputElementNum];
    nanos[outElementNum] = timestampColVector.nanos[inputElementNum];
  }

  // Simplify vector by brute-force flattening noNulls and isRepeating
  // This can be used to reduce combinatorial explosion of code paths in VectorExpressions
  // with many arguments.
  public void flatten(boolean selectedInUse, int[] sel, int size) {
    flattenPush();
    if (isRepeating) {
      isRepeating = false;
      long repeatFastTime = totalSeconds[0];
      int repeatNanos = nanos[0];
      if (selectedInUse) {
        for (int j = 0; j < size; j++) {
          int i = sel[j];
          totalSeconds[i] = repeatFastTime;
          nanos[i] = repeatNanos;
        }
      } else {
        Arrays.fill(totalSeconds, 0, size, repeatFastTime);
        Arrays.fill(nanos, 0, size, repeatNanos);
      }
      flattenRepeatingNulls(selectedInUse, sel, size);
    }
    flattenNoNulls(selectedInUse, sel, size);
  }

  /**
   * Set a row from a HiveIntervalDayTime.
   * We assume the entry has already been isRepeated adjusted.
   * @param elementNum
   * @param intervalDayTime
   */
  public void set(int elementNum, HiveIntervalDayTime intervalDayTime) {
    this.totalSeconds[elementNum] = intervalDayTime.getTotalSeconds();
    this.nanos[elementNum] = intervalDayTime.getNanos();
  }

  /**
   * Set a row from the current value in the scratch interval day time.
   * @param elementNum
   */
  public void setFromScratchIntervalDayTime(int elementNum) {
    this.totalSeconds[elementNum] = scratchIntervalDayTime.getTotalSeconds();
    this.nanos[elementNum] = scratchIntervalDayTime.getNanos();
  }

  /**
   * Set row to standard null value(s).
   * We assume the entry has already been isRepeated adjusted.
   * @param elementNum
   */
  public void setNullValue(int elementNum) {
    totalSeconds[elementNum] = 0;
    nanos[elementNum] = 1;
  }

  // Copy the current object contents into the output. Only copy selected entries,
  // as indicated by selectedInUse and the sel array.
  public void copySelected(
      boolean selectedInUse, int[] sel, int size, IntervalDayTimeColumnVector output) {

    // Output has nulls if and only if input has nulls.
    output.noNulls = noNulls;
    output.isRepeating = false;

    // Handle repeating case
    if (isRepeating) {
      output.totalSeconds[0] = totalSeconds[0];
      output.nanos[0] = nanos[0];
      output.isNull[0] = isNull[0];
      output.isRepeating = true;
      return;
    }

    // Handle normal case

    // Copy data values over
    if (selectedInUse) {
      for (int j = 0; j < size; j++) {
        int i = sel[j];
        output.totalSeconds[i] = totalSeconds[i];
        output.nanos[i] = nanos[i];
      }
    }
    else {
      System.arraycopy(totalSeconds, 0, output.totalSeconds, 0, size);
      System.arraycopy(nanos, 0, output.nanos, 0, size);
    }

    // Copy nulls over if needed
    if (!noNulls) {
      if (selectedInUse) {
        for (int j = 0; j < size; j++) {
          int i = sel[j];
          output.isNull[i] = isNull[i];
        }
      }
      else {
        System.arraycopy(isNull, 0, output.isNull, 0, size);
      }
    }
  }

  /**
   * Fill all the vector entries with a HiveIntervalDayTime.
   * @param intervalDayTime
   */
  public void fill(HiveIntervalDayTime intervalDayTime) {
    noNulls = true;
    isRepeating = true;
    totalSeconds[0] = intervalDayTime.getTotalSeconds();
    nanos[0] = intervalDayTime.getNanos();
  }

  /**
   * Return a convenience writable object stored by this column vector.
   * Supports keeping a TimestampWritable object without having to import that definition...
   * @return
   */
  public Writable getScratchWritable() {
    return scratchWritable;
  }

  /**
   * Set the convenience writable object stored by this column vector
   * @param scratchWritable
   */
  public void setScratchWritable(Writable scratchWritable) {
    this.scratchWritable = scratchWritable;
  }

  @Override
  public void stringifyValue(StringBuilder buffer, int row) {
    if (isRepeating) {
      row = 0;
    }
    if (noNulls || !isNull[row]) {
      scratchIntervalDayTime.set(totalSeconds[row], nanos[row]);
      buffer.append(scratchIntervalDayTime.toString());
    } else {
      buffer.append("null");
    }
  }
}