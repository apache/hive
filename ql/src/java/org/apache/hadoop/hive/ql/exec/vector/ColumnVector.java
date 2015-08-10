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

import org.apache.hadoop.io.Writable;

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

  /*
   * The current kinds of column vectors.
   */
  public static enum Type {
    LONG,
    DOUBLE,
    BYTES,
    DECIMAL
  }

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

  public abstract Writable getWritableObject(int index);

  /**
   * Constructor for super-class ColumnVector. This is not called directly,
   * but used to initialize inherited fields.
   *
   * @param len Vector length
   */
  public ColumnVector(int len) {
    isNull = new boolean[len];
    noNulls = true;
    isRepeating = false;
  }

  /**
     * Resets the column to default state
     *  - fills the isNull array with false
     *  - sets noNulls to true
     *  - sets isRepeating to false
     */
    public void reset() {
      if (false == noNulls) {
        Arrays.fill(isNull, false);
      }
      noNulls = true;
      isRepeating = false;
    }

    abstract public void flatten(boolean selectedInUse, int[] sel, int size);

    // Simplify vector by brute-force flattening noNulls if isRepeating
    // This can be used to reduce combinatorial explosion of code paths in VectorExpressions
    // with many arguments.
    public void flattenRepeatingNulls(boolean selectedInUse, int[] sel, int size) {

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

    public void flattenNoNulls(boolean selectedInUse, int[] sel, int size) {
      if (noNulls) {
        noNulls = false;
        if (selectedInUse) {
          for (int j = 0; j < size; j++) {
            int i = sel[j];
            isNull[i] = false;
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
     */
    public abstract void setElement(int outElementNum, int inputElementNum, ColumnVector inputVector);

    /**
     * Initialize the column vector. This method can be overridden by specific column vector types.
     * Use this method only if the individual type of the column vector is not known, otherwise its
     * preferable to call specific initialization methods.
     */
    public void init() {
      // Do nothing by default
    }
  }

