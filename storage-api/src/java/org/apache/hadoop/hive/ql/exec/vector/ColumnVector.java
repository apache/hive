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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ColumnVector contains the shared structure for the sub-types,
 * including NULL information, and whether this vector
 * repeats, i.e. has all values the same, so only the first
 * one is set. This is used to accelerate query performance
 * by handling a whole vector in O(1) time when applicable.
 *
 * The fields are public by design since this is a performance-critical
 * structure that is used in the inner loop of query execution.
 */
public abstract class ColumnVector {


  /** Reference count. */
  private AtomicInteger refCount = new AtomicInteger(0);

  /**
   * The current kinds of column vectors.
   */
  public enum Type {
    NONE,    // Useful when the type of column vector has not be determined yet.
    LONG,
    DOUBLE,
    BYTES,
    DECIMAL,
    DECIMAL_64,
    TIMESTAMP,
    INTERVAL_DAY_TIME,
    STRUCT,
    LIST,
    MAP,
    UNION,
    VOID
  }

  public final Type type;

  /*
   * If hasNulls is true, then this array contains true if the value
   * is null, otherwise false. The array is always allocated, so a batch can be re-used
   * later and nulls added.
   */
  public boolean[] isNull;

  // If the whole column vector has no nulls, this is true, otherwise false.
  public boolean noNulls;

  /*
   * True if same value repeats for whole column vector.
   * If so, vector[0] holds the repeating value.
   */
  public boolean isRepeating;

  // Variables to hold state from before flattening so it can be easily restored.
  private boolean preFlattenIsRepeating;
  private boolean preFlattenNoNulls;

  /**
   * Constructor for super-class ColumnVector. This is not called directly,
   * but used to initialize inherited fields.
   *
   * @param len Vector length
   */
  public ColumnVector(Type type, int len) {
    this.type = type;
    isNull = new boolean[len];
    noNulls = true;
    isRepeating = false;
    preFlattenNoNulls = true;
    preFlattenIsRepeating = false;
  }

  /**
   * Resets the column to default state
   *  - fills the isNull array with false
   *  - sets noNulls to true
   *  - sets isRepeating to false
   */
  public void reset() {
    assert (refCount.get() == 0);
    if (!noNulls) {
      Arrays.fill(isNull, false);
    }
    noNulls = true;
    isRepeating = false;
    preFlattenNoNulls = true;
    preFlattenIsRepeating = false;
  }


  public final void incRef() {
    refCount.incrementAndGet();
  }

  public final int getRef() {
    return refCount.get();
  }

  public final int decRef() {
    int i = refCount.decrementAndGet();
    assert i >= 0;
    return i;
  }

  /**
   * Sets the isRepeating flag. Recurses over structs and unions so that the
   * flags are set correctly.
   * @param isRepeating flag for repeating value.
   */
  public void setRepeating(boolean isRepeating) {
    this.isRepeating = isRepeating;
  }

  abstract public void flatten(boolean selectedInUse, int[] sel, int size);

  // Simplify vector by brute-force flattening noNulls if isRepeating
  // This can be used to reduce combinatorial explosion of code paths in VectorExpressions
  // with many arguments.
  protected void flattenRepeatingNulls(boolean selectedInUse, int[] sel,
                                       int size) {

    boolean nullFillValue;

    if (noNulls) {
      nullFillValue = false;
    } else {
      nullFillValue = isNull[0];
    }

    if (selectedInUse) {
      for (int j = 0; j < size; j++) {
        int i = sel[j];
        isNull[i] = nullFillValue;
      }
    } else {
      Arrays.fill(isNull, 0, size, nullFillValue);
    }

    // all nulls are now explicit
    noNulls = false;
  }

  protected void flattenNoNulls(boolean selectedInUse, int[] sel,
                                int size) {
    if (noNulls) {
      noNulls = false;
      if (selectedInUse) {
        for (int j = 0; j < size; j++) {
          isNull[sel[j]] = false;
        }
      } else {
        Arrays.fill(isNull, 0, size, false);
      }
    }
  }

  /**
   * Restore the state of isRepeating and noNulls to what it was
   * before flattening. This must only be called just after flattening
   * and then evaluating a VectorExpression on the column vector.
   * It is an optimization that allows other operations on the same
   * column to continue to benefit from the isRepeating and noNulls
   * indicators.
   */
  public void unFlatten() {
    isRepeating = preFlattenIsRepeating;
    noNulls = preFlattenNoNulls;
  }

  // Record repeating and no nulls state to be restored later.
  protected void flattenPush() {
    preFlattenIsRepeating = isRepeating;
    preFlattenNoNulls = noNulls;
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
  public abstract void setElement(int outputElementNum, int inputElementNum,
                                  ColumnVector inputColVector);

  /*
   * Copy the current object contents into the output. Only copy selected entries
   * as indicated by selectedInUse and the sel array.
   */
  public abstract void copySelected(
      boolean selectedInUse, int[] sel, int size, ColumnVector outputColVector);

  /**
   * Initialize the column vector. This method can be overridden by specific column vector types.
   * Use this method only if the individual type of the column vector is not known, otherwise its
   * preferable to call specific initialization methods.
   */
  public void init() {
    // Do nothing by default
  }

  /**
   * Ensure the ColumnVector can hold at least size values.
   * This method is deliberately *not* recursive because the complex types
   * can easily have more (or less) children than the upper levels.
   * @param size the new minimum size
   * @param preserveData should the old data be preserved?
   */
  public void ensureSize(int size, boolean preserveData) {
    if (isNull.length < size) {
      boolean[] oldArray = isNull;
      isNull = new boolean[size];
      if (preserveData && !noNulls) {
        if (isRepeating) {
          isNull[0] = oldArray[0];
        } else {
          System.arraycopy(oldArray, 0, isNull, 0, oldArray.length);
        }
      }
    }
  }

  /**
   * Print the value for this column into the given string builder.
   * @param buffer the buffer to print into
   * @param row the id of the row to print
   */
  public abstract void stringifyValue(StringBuilder buffer,
                                      int row);

  /**
   * Shallow copy of the contents of this vector to the other vector;
   * replaces other vector's values.
   */
  public void shallowCopyTo(ColumnVector otherCv) {
    otherCv.isNull = isNull;
    otherCv.noNulls = noNulls;
    otherCv.isRepeating = isRepeating;
    otherCv.preFlattenIsRepeating = preFlattenIsRepeating;
    otherCv.preFlattenNoNulls = preFlattenNoNulls;
    otherCv.refCount = refCount;
  }
}
