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

import java.sql.Timestamp;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

/**
 * This class represents a nullable timestamp column vector capable of handing a wide range of
 * timestamp values.
 *
 * We store the 2 (value) fields of a Timestamp class in primitive arrays.
 *
 * We do this to avoid an array of Java Timestamp objects which would have poor storage
 * and memory access characteristics.
 *
 * Generally, the caller will fill in a scratch timestamp object with values from a row, work
 * using the scratch timestamp, and then perhaps update the column vector row with a result.
 */
public class TimestampColumnVector extends ColumnVector {

  /*
   * The storage arrays for this column vector corresponds to the storage of a Timestamp:
   */
  public long[] time;
      // The values from Timestamp.getTime().

  public int[] nanos;
      // The values from Timestamp.getNanos().

  /*
   * Scratch objects.
   */
  private final Timestamp scratchTimestamp;

  private Writable scratchWritable;
      // Supports keeping a TimestampWritable object without having to import that definition...

  /**
   * Use this constructor by default. All column vectors
   * should normally be the default size.
   */
  public TimestampColumnVector() {
    this(VectorizedRowBatch.DEFAULT_SIZE);
  }

  /**
   * Don't use this except for testing purposes.
   *
   * @param len the number of rows
   */
  public TimestampColumnVector(int len) {
    super(len);

    time = new long[len];
    nanos = new int[len];

    scratchTimestamp = new Timestamp(0);

    scratchWritable = null;     // Allocated by caller.
  }

  /**
   * Return the number of rows.
   * @return
   */
  public int getLength() {
    return time.length;
  }

  /**
   * Return a row's Timestamp.getTime() value.
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param elementNum
   * @return
   */
  public long getTime(int elementNum) {
    return time[elementNum];
  }

  /**
   * Return a row's Timestamp.getNanos() value.
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param elementNum
   * @return
   */
  public int getNanos(int elementNum) {
    return nanos[elementNum];
  }

  /**
   * Set a Timestamp object from a row of the column.
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param timestamp
   * @param elementNum
   */
  public void timestampUpdate(Timestamp timestamp, int elementNum) {
    timestamp.setTime(time[elementNum]);
    timestamp.setNanos(nanos[elementNum]);
  }

  /**
   * Return the scratch Timestamp object set from a row.
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param elementNum
   * @return
   */
  public Timestamp asScratchTimestamp(int elementNum) {
    scratchTimestamp.setTime(time[elementNum]);
    scratchTimestamp.setNanos(nanos[elementNum]);
    return scratchTimestamp;
  }

  /**
   * Return the scratch timestamp (contents undefined).
   * @return
   */
  public Timestamp getScratchTimestamp() {
    return scratchTimestamp;
  }

  /**
   * Return a long representation of a Timestamp.
   * @param elementNum
   * @return
   */
  public long getTimestampAsLong(int elementNum) {
    scratchTimestamp.setTime(time[elementNum]);
    scratchTimestamp.setNanos(nanos[elementNum]);
    return getTimestampAsLong(scratchTimestamp);
  }

  /**
   * Return a long representation of a Timestamp.
   * @param timestamp
   * @return
   */
  public static long getTimestampAsLong(Timestamp timestamp) {
    return millisToSeconds(timestamp.getTime());
  }

  // Copy of TimestampWritable.millisToSeconds
  /**
   * Rounds the number of milliseconds relative to the epoch down to the nearest whole number of
   * seconds. 500 would round to 0, -500 would round to -1.
   */
  private static long millisToSeconds(long millis) {
    if (millis >= 0) {
      return millis / 1000;
    } else {
      return (millis - 999) / 1000;
    }
  }

  /**
   * Return a double representation of a Timestamp.
   * @param elementNum
   * @return
   */
  public double getDouble(int elementNum) {
    scratchTimestamp.setTime(time[elementNum]);
    scratchTimestamp.setNanos(nanos[elementNum]);
    return getDouble(scratchTimestamp);
  }

  /**
   * Return a double representation of a Timestamp.
   * @param timestamp
   * @return
   */
  public static double getDouble(Timestamp timestamp) {
    // Same algorithm as TimestampWritable (not currently import-able here).
    double seconds, nanos;
    seconds = millisToSeconds(timestamp.getTime());
    nanos = timestamp.getNanos();
    return seconds + nanos / 1000000000;
  }

  /**
   * Compare row to Timestamp.
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param elementNum
   * @param timestamp
   * @return -1, 0, 1 standard compareTo values.
   */
  public int compareTo(int elementNum, Timestamp timestamp) {
    return asScratchTimestamp(elementNum).compareTo(timestamp);
  }

  /**
   * Compare Timestamp to row.
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param timestamp
   * @param elementNum
   * @return -1, 0, 1 standard compareTo values.
   */
  public int compareTo(Timestamp timestamp, int elementNum) {
    return timestamp.compareTo(asScratchTimestamp(elementNum));
  }

  /**
   * Compare a row to another TimestampColumnVector's row.
   * @param elementNum1
   * @param timestampColVector2
   * @param elementNum2
   * @return
   */
  public int compareTo(int elementNum1, TimestampColumnVector timestampColVector2,
      int elementNum2) {
    return asScratchTimestamp(elementNum1).compareTo(
        timestampColVector2.asScratchTimestamp(elementNum2));
  }

  /**
   * Compare another TimestampColumnVector's row to a row.
   * @param timestampColVector1
   * @param elementNum1
   * @param elementNum2
   * @return
   */
  public int compareTo(TimestampColumnVector timestampColVector1, int elementNum1,
      int elementNum2) {
    return timestampColVector1.asScratchTimestamp(elementNum1).compareTo(
        asScratchTimestamp(elementNum2));
  }

  @Override
  public void setElement(int outElementNum, int inputElementNum, ColumnVector inputVector) {

    TimestampColumnVector timestampColVector = (TimestampColumnVector) inputVector;

    time[outElementNum] = timestampColVector.time[inputElementNum];
    nanos[outElementNum] = timestampColVector.nanos[inputElementNum];
  }

  // Simplify vector by brute-force flattening noNulls and isRepeating
  // This can be used to reduce combinatorial explosion of code paths in VectorExpressions
  // with many arguments.
  public void flatten(boolean selectedInUse, int[] sel, int size) {
    flattenPush();
    if (isRepeating) {
      isRepeating = false;
      long repeatFastTime = time[0];
      int repeatNanos = nanos[0];
      if (selectedInUse) {
        for (int j = 0; j < size; j++) {
          int i = sel[j];
          time[i] = repeatFastTime;
          nanos[i] = repeatNanos;
        }
      } else {
        Arrays.fill(time, 0, size, repeatFastTime);
        Arrays.fill(nanos, 0, size, repeatNanos);
      }
      flattenRepeatingNulls(selectedInUse, sel, size);
    }
    flattenNoNulls(selectedInUse, sel, size);
  }

  /**
   * Set a row from a timestamp.
   * We assume the entry has already been isRepeated adjusted.
   * @param elementNum
   * @param timestamp
   */
  public void set(int elementNum, Timestamp timestamp) {
    if (timestamp == null) {
      this.noNulls = false;
      this.isNull[elementNum] = true;
    } else {
      this.time[elementNum] = timestamp.getTime();
      this.nanos[elementNum] = timestamp.getNanos();
    }
  }

  /**
   * Set a row from the current value in the scratch timestamp.
   * @param elementNum
   */
  public void setFromScratchTimestamp(int elementNum) {
    this.time[elementNum] = scratchTimestamp.getTime();
    this.nanos[elementNum] = scratchTimestamp.getNanos();
  }

  /**
   * Set row to standard null value(s).
   * We assume the entry has already been isRepeated adjusted.
   * @param elementNum
   */
  public void setNullValue(int elementNum) {
    time[elementNum] = 0;
    nanos[elementNum] = 1;
  }

  // Copy the current object contents into the output. Only copy selected entries,
  // as indicated by selectedInUse and the sel array.
  public void copySelected(
      boolean selectedInUse, int[] sel, int size, TimestampColumnVector output) {

    // Output has nulls if and only if input has nulls.
    output.noNulls = noNulls;
    output.isRepeating = false;

    // Handle repeating case
    if (isRepeating) {
      output.time[0] = time[0];
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
        output.time[i] = time[i];
        output.nanos[i] = nanos[i];
      }
    }
    else {
      System.arraycopy(time, 0, output.time, 0, size);
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
   * Fill all the vector entries with a timestamp.
   * @param timestamp
   */
  public void fill(Timestamp timestamp) {
    noNulls = true;
    isRepeating = true;
    time[0] = timestamp.getTime();
    nanos[0] = timestamp.getNanos();
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
      scratchTimestamp.setTime(time[row]);
      scratchTimestamp.setNanos(nanos[row]);
      buffer.append(scratchTimestamp.toString());
    } else {
      buffer.append("null");
    }
  }

  @Override
  public void ensureSize(int size, boolean preserveData) {
    super.ensureSize(size, preserveData);
    if (size <= time.length) return;
    long[] oldTime = time;
    int[] oldNanos = nanos;
    time = new long[size];
    nanos = new int[size];
    if (preserveData) {
      if (isRepeating) {
        time[0] = oldTime[0];
        nanos[0] = oldNanos[0];
      } else {
        System.arraycopy(oldTime, 0, time, 0, oldTime.length);
        System.arraycopy(oldNanos, 0, nanos, 0, oldNanos.length);
      }
    }
  }

  @Override
  public void shallowCopyTo(ColumnVector otherCv) {
    TimestampColumnVector other = (TimestampColumnVector)otherCv;
    super.shallowCopyTo(other);
    other.time = time;
    other.nanos = nanos;
  }
}
