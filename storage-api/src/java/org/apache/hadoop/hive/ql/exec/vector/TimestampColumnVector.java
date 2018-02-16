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
    super(Type.TIMESTAMP, len);

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

  /**
   * Set the element in this column vector from the given input vector.
   *
   * The inputElementNum will be adjusted to 0 if the input column has isRepeating set.
   *
   * On the other hand, the outElementNum must have been adjusted to 0 in ADVANCE when the output
   * has isRepeating set.
   *
   * IMPORTANT: if the output entry is marked as NULL, this method will do NOTHING.  This
   * supports the caller to do output NULL processing in advance that may cause the output results
   * operation to be ignored.  Thus, make sure the output isNull entry is set in ADVANCE.
   *
   * The inputColVector noNulls and isNull entry will be examined.  The output will only
   * be set if the input is NOT NULL.  I.e. noNulls || !isNull[inputElementNum] where
   * inputElementNum may have been adjusted to 0 for isRepeating.
   *
   * If the input entry is NULL or out-of-range, the output will be marked as NULL.
   * I.e. set output noNull = false and isNull[outElementNum] = true.  An example of out-of-range
   * is the DecimalColumnVector which can find the input decimal does not fit in the output
   * precision/scale.
   *
   * (Since we return immediately if the output entry is NULL, we have no need and do not mark
   * the output entry to NOT NULL).
   *
   */
  @Override
  public void setElement(int outputElementNum, int inputElementNum, ColumnVector inputColVector) {

    // Invariants.
    if (isRepeating && outputElementNum != 0) {
      throw new RuntimeException("Output column number expected to be 0 when isRepeating");
    }
    if (inputColVector.isRepeating) {
      inputElementNum = 0;
    }

    // Do NOTHING if output is NULL.
    if (!noNulls && isNull[outputElementNum]) {
      return;
    }

    if (inputColVector.noNulls || !inputColVector.isNull[inputElementNum]) {
      TimestampColumnVector timestampColVector = (TimestampColumnVector) inputColVector;
      time[outputElementNum] = timestampColVector.time[inputElementNum];
      nanos[outputElementNum] = timestampColVector.nanos[inputElementNum];
    } else {

      // Only mark output NULL when input is NULL.
      isNull[outputElementNum] = true;
      noNulls = false;
    }
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
   * Set a field from a Timestamp.
   *
   * This is a FAST version that assumes the caller has checked to make sure elementNum
   * is correctly adjusted for isRepeating.  And, that the isNull entry
   * has been set.  Only the output entry fields will be set by this method.
   *
   * For backward compatibility, this method does check if the timestamp is null and set the
   * isNull entry appropriately.
   *
   * @param elementNum
   * @param timestamp
   */
  public void set(int elementNum, Timestamp timestamp) {
    if (timestamp == null) {
      isNull[elementNum] = true;
      noNulls = false;
      return;
    }
    this.time[elementNum] = timestamp.getTime();
    this.nanos[elementNum] = timestamp.getNanos();
  }

  /**
   * Set a field from the current value in the scratch timestamp.
   *
   * This is a FAST version that assumes the caller has checked to make sure the current value in
   * the scratch timestamp is valid and elementNum is correctly adjusted for isRepeating.  And,
   * that the isNull entry has been set.  Only the output entry fields will be set by this method.
   *
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
  @Override
  public void copySelected(
      boolean selectedInUse, int[] sel, int size, ColumnVector outputColVector) {

    TimestampColumnVector output = (TimestampColumnVector) outputColVector;
    boolean[] outputIsNull = output.isNull;

    // We do not need to do a column reset since we are carefully changing the output.
    output.isRepeating = false;

    // Handle repeating case
    if (isRepeating) {
      if (noNulls || !isNull[0]) {
        outputIsNull[0] = false;
        output.time[0] = time[0];
        output.nanos[0] = nanos[0];
      } else {
        outputIsNull[0] = true;
        output.noNulls = false;
      }
      output.isRepeating = true;
      return;
    }

    // Handle normal case

    if (noNulls) {
      if (selectedInUse) {

        // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.

        if (!outputColVector.noNulls) {
          for(int j = 0; j != size; j++) {
           final int i = sel[j];
           // Set isNull before call in case it changes it mind.
           outputIsNull[i] = false;
           output.time[i] = time[i];
           output.nanos[i] = nanos[i];
         }
        } else {
          for(int j = 0; j != size; j++) {
            final int i = sel[j];
            output.time[i] = time[i];
            output.nanos[i] = nanos[i];
          }
        }
      } else {
        if (!outputColVector.noNulls) {

          // Assume it is almost always a performance win to fill all of isNull so we can
          // safely reset noNulls.
          Arrays.fill(outputIsNull, false);
          outputColVector.noNulls = true;
        }
        System.arraycopy(time, 0, output.time, 0, size);
        System.arraycopy(nanos, 0, output.nanos, 0, size);
      }
    } else /* there are nulls in our column */ {

      // Carefully handle NULLs...

      /*
       * For better performance on LONG/DOUBLE we don't want the conditional
       * statements inside the for loop.
       */
      output.noNulls = false;

      if (selectedInUse) {
        for (int j = 0; j < size; j++) {
          int i = sel[j];
          output.isNull[i] = isNull[i];
          output.time[i] = time[i];
          output.nanos[i] = nanos[i];
        }
      } else {
        System.arraycopy(isNull, 0, output.isNull, 0, size);
        System.arraycopy(time, 0, output.time, 0, size);
        System.arraycopy(nanos, 0, output.nanos, 0, size);
      }
    }
  }

  /**
   * Fill all the vector entries with a timestamp.
   * @param timestamp
   */
  public void fill(Timestamp timestamp) {
    isRepeating = true;
    isNull[0] = false;
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
