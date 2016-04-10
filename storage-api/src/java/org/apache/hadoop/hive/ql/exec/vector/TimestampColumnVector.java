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

import org.apache.hadoop.hive.common.type.PisaTimestamp;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;

/**
 * This class represents a nullable timestamp column vector capable of handing a wide range of
 * timestamp values.
 *
 * We use the PisaTimestamp which is designed to be mutable and avoid the heavy memory allocation
 * and CPU data cache miss costs.
 */
public class TimestampColumnVector extends ColumnVector {

  /*
   * The storage arrays for this column vector corresponds to the storage of a PisaTimestamp:
   */
  private long[] epochDay;
      // An array of the number of days since 1970-01-01 (similar to Java 8 LocalDate).

  private long[] nanoOfDay;
      // An array of the number of nanoseconds within the day, with the range of
      // 0 to 24 * 60 * 60 * 1,000,000,000 - 1 (similar to Java 8 LocalTime).

  /*
   * Scratch objects.
   */
  private PisaTimestamp scratchPisaTimestamp;
      // Convenience scratch Pisa timestamp object.

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

    epochDay = new long[len];
    nanoOfDay = new long[len];

    scratchPisaTimestamp = new PisaTimestamp();

    scratchWritable = null;     // Allocated by caller.
  }

  /**
   * Return the number of rows.
   * @return
   */
  public int getLength() {
    return epochDay.length;
  }

  /**
   * Returnt a row's epoch day.
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param elementNum
   * @return
   */
  public long getEpochDay(int elementNum) {
    return epochDay[elementNum];
  }

  /**
   * Return a row's nano of day.
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param elementNum
   * @return
   */
  public long getNanoOfDay(int elementNum) {
    return nanoOfDay[elementNum];
  }

  /**
   * Get a scratch PisaTimestamp object from a row of the column.
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param elementNum
   * @return scratch
   */
  public PisaTimestamp asScratchPisaTimestamp(int elementNum) {
    scratchPisaTimestamp.update(epochDay[elementNum], nanoOfDay[elementNum]);
    return scratchPisaTimestamp;
  }

  /**
   * Set a PisaTimestamp object from a row of the column.
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param pisaTimestamp
   * @param elementNum
   */
  public void pisaTimestampUpdate(PisaTimestamp pisaTimestamp, int elementNum) {
    pisaTimestamp.update(epochDay[elementNum], nanoOfDay[elementNum]);
  }

  /**
   * Set a Timestamp object from a row of the column.
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param timestamp
   * @param elementNum
   */
  public void timestampUpdate(Timestamp timestamp, int elementNum) {
    scratchPisaTimestamp.update(epochDay[elementNum], nanoOfDay[elementNum]);
    scratchPisaTimestamp.timestampUpdate(timestamp);
  }

  /**
   * Compare row to PisaTimestamp.
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param elementNum
   * @param pisaTimestamp
   * @return -1, 0, 1 standard compareTo values.
   */
  public int compareTo(int elementNum, PisaTimestamp pisaTimestamp) {
    return PisaTimestamp.compareTo(epochDay[elementNum], nanoOfDay[elementNum], pisaTimestamp);
  }

  /**
   * Compare PisaTimestamp to row.
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param pisaTimestamp
   * @param elementNum
   * @return -1, 0, 1 standard compareTo values.
   */
  public int compareTo(PisaTimestamp pisaTimestamp, int elementNum) {
    return PisaTimestamp.compareTo(pisaTimestamp, epochDay[elementNum], nanoOfDay[elementNum]);
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
    return PisaTimestamp.compareTo(
        epochDay[elementNum1], nanoOfDay[elementNum1],
        timestampColVector2.epochDay[elementNum2], timestampColVector2.nanoOfDay[elementNum2]);
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
    return PisaTimestamp.compareTo(
        timestampColVector1.epochDay[elementNum1], timestampColVector1.nanoOfDay[elementNum1],
        epochDay[elementNum2], nanoOfDay[elementNum2]);
  }

  public void add(PisaTimestamp timestamp1, PisaTimestamp timestamp2, int resultElementNum) {
    PisaTimestamp.add(timestamp1, timestamp2, scratchPisaTimestamp);
    epochDay[resultElementNum] = scratchPisaTimestamp.getEpochDay();
    nanoOfDay[resultElementNum] = scratchPisaTimestamp.getNanoOfDay();
  }

  public void subtract(PisaTimestamp timestamp1, PisaTimestamp timestamp2, int resultElementNum) {
    PisaTimestamp.subtract(timestamp1, timestamp2, scratchPisaTimestamp);
    epochDay[resultElementNum] = scratchPisaTimestamp.getEpochDay();
    nanoOfDay[resultElementNum] = scratchPisaTimestamp.getNanoOfDay();
  }

  /**
   * Return row as a double with the integer part as the seconds and the fractional part as
   * the nanoseconds the way the Timestamp class does it.
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param elementNum
   * @return seconds.nanoseconds
   */
  public double getTimestampSecondsWithFractionalNanos(int elementNum) {
    scratchPisaTimestamp.update(epochDay[elementNum], nanoOfDay[elementNum]);
    return scratchPisaTimestamp.getTimestampSecondsWithFractionalNanos();
  }

  /**
   * Return row as integer as the seconds the way the Timestamp class does it.
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param elementNum
   * @return seconds
   */
  public long getTimestampSeconds(int elementNum) {
    scratchPisaTimestamp.update(epochDay[elementNum], nanoOfDay[elementNum]);
    return scratchPisaTimestamp.getTimestampSeconds();
  }


  /**
   * Return row as milliseconds the way the Timestamp class does it.
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param elementNum
   * @return
   */
  public long getTimestampMilliseconds(int elementNum) {
    scratchPisaTimestamp.update(epochDay[elementNum], nanoOfDay[elementNum]);
    return scratchPisaTimestamp.getTimestampMilliseconds();
  }

  /**
   * Return row as epoch seconds.
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param elementNum
   * @return
   */
  public long getEpochSeconds(int elementNum) {
    return PisaTimestamp.getEpochSecondsFromEpochDayAndNanoOfDay(epochDay[elementNum], nanoOfDay[elementNum]);
  }

  /**
   * Return row as epoch milliseconds.
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param elementNum
   * @return
   */
  public long getEpochMilliseconds(int elementNum) {
    return PisaTimestamp.getEpochMillisecondsFromEpochDayAndNanoOfDay(epochDay[elementNum], nanoOfDay[elementNum]);
  }

  /**
   * Return row as signed nanos (-999999999 to 999999999).
   * NOTE: This is not the same as the Timestamp class nanos (which is always positive).
   * We assume the entry has already been NULL checked and isRepeated adjusted.
   * @param elementNum
   * @return
   */
  public int getSignedNanos(int elementNum) {
    return PisaTimestamp.getSignedNanos(nanoOfDay[elementNum]);
  }

  /**
   * Get scratch timestamp with value of a row.
   * @param elementNum
   * @return
   */
  public Timestamp asScratchTimestamp(int elementNum) {
    scratchPisaTimestamp.update(epochDay[elementNum], nanoOfDay[elementNum]);
    return scratchPisaTimestamp.asScratchTimestamp();
  }

  /**
   * Get scratch Pisa timestamp for use by the caller.
   * @return
   */
  public PisaTimestamp useScratchPisaTimestamp() {
    return scratchPisaTimestamp;
  }

  @Override
  public void setElement(int outElementNum, int inputElementNum, ColumnVector inputVector) {

    TimestampColumnVector timestampColVector = (TimestampColumnVector) inputVector;

    epochDay[outElementNum] = timestampColVector.epochDay[inputElementNum];
    nanoOfDay[outElementNum] = timestampColVector.nanoOfDay[inputElementNum];
  }

  // Simplify vector by brute-force flattening noNulls and isRepeating
  // This can be used to reduce combinatorial explosion of code paths in VectorExpressions
  // with many arguments.
  public void flatten(boolean selectedInUse, int[] sel, int size) {
    flattenPush();
    if (isRepeating) {
      isRepeating = false;
      long repeatEpochDay = epochDay[0];
      long repeatNanoOfDay = nanoOfDay[0];
      if (selectedInUse) {
        for (int j = 0; j < size; j++) {
          int i = sel[j];
          epochDay[i] = repeatEpochDay;
          nanoOfDay[i] = repeatNanoOfDay;
        }
      } else {
        Arrays.fill(epochDay, 0, size, repeatEpochDay);
        Arrays.fill(nanoOfDay, 0, size, repeatNanoOfDay);
      }
      flattenRepeatingNulls(selectedInUse, sel, size);
    }
    flattenNoNulls(selectedInUse, sel, size);
  }

  /**
   * Set a row from a PisaTimestamp.
   * We assume the entry has already been isRepeated adjusted.
   * @param elementNum
   * @param pisaTimestamp
   */
  public void set(int elementNum, PisaTimestamp pisaTimestamp) {
    this.epochDay[elementNum] = pisaTimestamp.getEpochDay();
    this.nanoOfDay[elementNum] = pisaTimestamp.getNanoOfDay();
  }

  /**
   * Set a row from a timestamp.
   * We assume the entry has already been isRepeated adjusted.
   * @param elementNum
   * @param timestamp
   */
  public void set(int elementNum, Timestamp timestamp) {
    scratchPisaTimestamp.updateFromTimestamp(timestamp);
    this.epochDay[elementNum] = scratchPisaTimestamp.getEpochDay();
    this.nanoOfDay[elementNum] = scratchPisaTimestamp.getNanoOfDay();
  }

  /**
   * Set a row from a epoch seconds and signed nanos (-999999999 to 999999999).
   * @param elementNum
   * @param epochSeconds
   * @param signedNanos
   */
  public void setEpochSecondsAndSignedNanos(int elementNum, long epochSeconds, int signedNanos) {
    scratchPisaTimestamp.updateFromEpochSecondsAndSignedNanos(epochSeconds, signedNanos);
    set(elementNum, scratchPisaTimestamp);
  }

  /**
   * Set a row from timestamp milliseconds.
   * We assume the entry has already been isRepeated adjusted.
   * @param elementNum
   * @param timestampMilliseconds
   */
  public void setTimestampMilliseconds(int elementNum, long timestampMilliseconds) {
    scratchPisaTimestamp.updateFromTimestampMilliseconds(timestampMilliseconds);
    set(elementNum, scratchPisaTimestamp.useScratchTimestamp());
  }

  /**
   * Set a row from timestamp seconds.
   * We assume the entry has already been isRepeated adjusted.
   * @param elementNum
   * @param timestamp
   */
  public void setTimestampSeconds(int elementNum, long timestampSeconds) {
    scratchPisaTimestamp.updateFromTimestampSeconds(timestampSeconds);
    set(elementNum, scratchPisaTimestamp);
  }

  /**
   * Set a row from a double timestamp seconds with fractional nanoseconds.
   * We assume the entry has already been isRepeated adjusted.
   * @param elementNum
   * @param timestamp
   */
  public void setTimestampSecondsWithFractionalNanoseconds(int elementNum,
      double secondsWithFractionalNanoseconds) {
    scratchPisaTimestamp.updateFromTimestampSecondsWithFractionalNanoseconds(secondsWithFractionalNanoseconds);
    set(elementNum, scratchPisaTimestamp);
  }

  /**
   * Set row to standard null value(s).
   * We assume the entry has already been isRepeated adjusted.
   * @param elementNum
   */
  public void setNullValue(int elementNum) {
    epochDay[elementNum] = 0;
    nanoOfDay[elementNum] = 1;
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
      output.epochDay[0] = epochDay[0];
      output.nanoOfDay[0] = nanoOfDay[0];
      output.isNull[0] = isNull[0];
      output.isRepeating = true;
      return;
    }

    // Handle normal case

    // Copy data values over
    if (selectedInUse) {
      for (int j = 0; j < size; j++) {
        int i = sel[j];
        output.epochDay[i] = epochDay[i];
        output.nanoOfDay[i] = nanoOfDay[i];
      }
    }
    else {
      System.arraycopy(epochDay, 0, output.epochDay, 0, size);
      System.arraycopy(nanoOfDay, 0, output.nanoOfDay, 0, size);
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
   * Fill all the vector entries with a PisaTimestamp.
   * @param pisaTimestamp
   */
  public void fill(PisaTimestamp pisaTimestamp) {
    noNulls = true;
    isRepeating = true;
    epochDay[0] = pisaTimestamp.getEpochDay();
    nanoOfDay[0] = pisaTimestamp.getNanoOfDay();
  }

  /**
   * Fill all the vector entries with a timestamp.
   * @param timestamp
   */
  public void fill(Timestamp timestamp) {
    noNulls = true;
    isRepeating = true;
    scratchPisaTimestamp.updateFromTimestamp(timestamp);
    epochDay[0] = scratchPisaTimestamp.getEpochDay();
    nanoOfDay[0] = scratchPisaTimestamp.getNanoOfDay();
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
      scratchPisaTimestamp.update(epochDay[row], nanoOfDay[row]);
      buffer.append(scratchPisaTimestamp.toString());
    } else {
      buffer.append("null");
    }
  }
}