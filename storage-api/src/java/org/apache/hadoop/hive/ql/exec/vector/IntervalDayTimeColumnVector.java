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
    super(Type.INTERVAL_DAY_TIME, len);

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
      IntervalDayTimeColumnVector timestampColVector = (IntervalDayTimeColumnVector) inputColVector;
      totalSeconds[outputElementNum] = timestampColVector.totalSeconds[inputElementNum];
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
   * Set a field from a HiveIntervalDayTime.
   *
   * This is a FAST version that assumes the caller has checked to make sure the sourceBuf
   * is not null and elementNum is correctly adjusted for isRepeating.  And, that the isNull entry
   * has been set.  Only the output entry fields will be set by this method.
   *
   * @param elementNum
   * @param intervalDayTime
   */
  public void set(int elementNum, HiveIntervalDayTime intervalDayTime) {
    this.totalSeconds[elementNum] = intervalDayTime.getTotalSeconds();
    this.nanos[elementNum] = intervalDayTime.getNanos();
  }

  /**
   * Set a field from the current value in the scratch interval day time.
   *
   * This is a FAST version that assumes the caller has checked to make sure the scratch interval
   * day time is valid and elementNum is correctly adjusted for isRepeating.  And, that the isNull
   * entry has been set.  Only the output entry fields will be set by this method.
   *
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
  @Override
  public void copySelected(
      boolean selectedInUse, int[] sel, int size, ColumnVector outputColVector) {

    IntervalDayTimeColumnVector output = (IntervalDayTimeColumnVector) outputColVector;
    boolean[] outputIsNull = output.isNull;

    // We do not need to do a column reset since we are carefully changing the output.
    output.isRepeating = false;

    // Handle repeating case
    if (isRepeating) {
      if (noNulls || !isNull[0]) {
        outputIsNull[0] = false;
        output.totalSeconds[0] = totalSeconds[0];
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
           output.totalSeconds[i] = totalSeconds[i];
           output.nanos[i] = nanos[i];
         }
        } else {
          for(int j = 0; j != size; j++) {
            final int i = sel[j];
            output.totalSeconds[i] = totalSeconds[i];
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
        for(int i = 0; i != size; i++) {
          output.totalSeconds[i] = totalSeconds[i];
          output.nanos[i] = nanos[i];
        }
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
          output.totalSeconds[i] = totalSeconds[i];
          output.nanos[i] = nanos[i];
        }
      } else {
        System.arraycopy(isNull, 0, output.isNull, 0, size);
        System.arraycopy(totalSeconds, 0, output.totalSeconds, 0, size);
        System.arraycopy(nanos, 0, output.nanos, 0, size);
      }
    }
  }

  /**
   * Fill all the vector entries with a HiveIntervalDayTime.
   * @param intervalDayTime
   */
  public void fill(HiveIntervalDayTime intervalDayTime) {
    isRepeating = true;
    isNull[0] = false;
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

  @Override
  public void ensureSize(int size, boolean preserveData) {
    super.ensureSize(size, preserveData);
    if (size <= totalSeconds.length) return;
    long[] oldTime = totalSeconds;
    int[] oldNanos = nanos;
    totalSeconds = new long[size];
    nanos = new int[size];
    if (preserveData) {
      if (isRepeating) {
        totalSeconds[0] = oldTime[0];
        nanos[0] = oldNanos[0];
      } else {
        System.arraycopy(oldTime, 0, totalSeconds, 0, oldTime.length);
        System.arraycopy(oldNanos, 0, nanos, 0, oldNanos.length);
      }
    }
  }

  @Override
  public void shallowCopyTo(ColumnVector otherCv) {
    IntervalDayTimeColumnVector other = (IntervalDayTimeColumnVector)otherCv;
    super.shallowCopyTo(other);
    other.totalSeconds = totalSeconds;
    other.nanos = nanos;
  }
}