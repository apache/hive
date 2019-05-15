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

/**
 * This class represents a nullable double precision floating point column vector.
 * This class will be used for operations on all floating point types (float, double)
 * and as such will use a 64-bit double value to hold the biggest possible value.
 * During copy-in/copy-out, smaller types (i.e. float) will be converted as needed. This will
 * reduce the amount of code that needs to be generated and also will run fast since the
 * machine operates with 64-bit words.
 *
 * The vector[] field is public by design for high-performance access in the inner
 * loop of query execution.
 */
public class DoubleColumnVector extends ColumnVector {
  public double[] vector;
  public static final double NULL_VALUE = Double.NaN;

  /**
   * Use this constructor by default. All column vectors
   * should normally be the default size.
   */
  public DoubleColumnVector() {
    this(VectorizedRowBatch.DEFAULT_SIZE);
  }

  /**
   * Don't use this except for testing purposes.
   *
   * @param len
   */
  public DoubleColumnVector(int len) {
    super(Type.DOUBLE, len);
    vector = new double[len];
  }

  // Copy the current object contents into the output. Only copy selected entries,
  // as indicated by selectedInUse and the sel array.
  @Override
  public void copySelected(
      boolean selectedInUse, int[] sel, int size, ColumnVector outputColVector) {

    DoubleColumnVector output = (DoubleColumnVector) outputColVector;
    boolean[] outputIsNull = output.isNull;

    // We do not need to do a column reset since we are carefully changing the output.
    output.isRepeating = false;

    // Handle repeating case
    if (isRepeating) {
      if (noNulls || !isNull[0]) {
        outputIsNull[0] = false;
        output.vector[0] = vector[0];
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
           output.vector[i] = vector[i];
         }
        } else {
          for(int j = 0; j != size; j++) {
            final int i = sel[j];
            output.vector[i] = vector[i];
          }
        }
      } else {
        if (!outputColVector.noNulls) {

          // Assume it is almost always a performance win to fill all of isNull so we can
          // safely reset noNulls.
          Arrays.fill(outputIsNull, false);
          outputColVector.noNulls = true;
        }
        System.arraycopy(vector, 0, output.vector, 0, size);
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
          output.vector[i] = vector[i];
        }
      } else {
        System.arraycopy(isNull, 0, output.isNull, 0, size);
        for (int i = 0; i < size; i++) {
          output.vector[i] = vector[i];
        }
      }
    }
  }

  // Fill the column vector with the provided value
  public void fill(double value) {
    isRepeating = true;
    isNull[0] = false;
    vector[0] = value;
  }

  // Fill the column vector with nulls
  public void fillWithNulls() {
    noNulls = false;
    isRepeating = true;
    vector[0] = NULL_VALUE;
    isNull[0] = true;
  }

  // Simplify vector by brute-force flattening noNulls and isRepeating
  // This can be used to reduce combinatorial explosion of code paths in VectorExpressions
  // with many arguments.
  public void flatten(boolean selectedInUse, int[] sel, int size) {
    flattenPush();
    if (isRepeating) {
      isRepeating = false;
      double repeatVal = vector[0];
      if (selectedInUse) {
        for (int j = 0; j < size; j++) {
          int i = sel[j];
          vector[i] = repeatVal;
        }
      } else {
        Arrays.fill(vector, 0, size, repeatVal);
      }
      flattenRepeatingNulls(selectedInUse, sel, size);
    }
    flattenNoNulls(selectedInUse, sel, size);
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
      vector[outputElementNum] =
          ((DoubleColumnVector) inputColVector).vector[inputElementNum];
    } else {

      // Only mark output NULL when input is NULL.
      isNull[outputElementNum] = true;
      noNulls = false;
    }
  }

  @Override
  public void stringifyValue(StringBuilder buffer, int row) {
    if (isRepeating) {
      row = 0;
    }
    if (noNulls || !isNull[row]) {
      buffer.append(vector[row]);
    } else {
      buffer.append("null");
    }
  }

  @Override
  public void ensureSize(int size, boolean preserveData) {
    super.ensureSize(size, preserveData);
    if (size > vector.length) {
      double[] oldArray = vector;
      vector = new double[size];
      if (preserveData) {
        if (isRepeating) {
          vector[0] = oldArray[0];
        } else {
          System.arraycopy(oldArray, 0, vector, 0 , oldArray.length);
        }
      }
    }
  }

  @Override
  public void shallowCopyTo(ColumnVector otherCv) {
    DoubleColumnVector other = (DoubleColumnVector)otherCv;
    super.shallowCopyTo(other);
    other.vector = vector;
  }
}
