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
 * The representation of a vectorized column of list objects.
 *
 * Each list is composed of a range of elements in the underlying child
 * ColumnVector. The range for list i is
 * offsets[i]..offsets[i]+lengths[i]-1 inclusive.
 */
public class ListColumnVector extends MultiValuedColumnVector {

  public ColumnVector child;

  public ListColumnVector() {
    this(VectorizedRowBatch.DEFAULT_SIZE, null);
  }

  /**
   * Constructor for ListColumnVector.
   *
   * @param len Vector length
   * @param child The child vector
   */
  public ListColumnVector(int len, ColumnVector child) {
    super(Type.LIST, len);
    this.child = child;
  }

  @Override
  protected void childFlatten(boolean useSelected, int[] selected, int size) {
    child.flatten(useSelected, selected, size);
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

    // CONCERN: isRepeating
    if (inputColVector.noNulls || !inputColVector.isNull[inputElementNum]) {
      ListColumnVector input = (ListColumnVector) inputColVector;
      int offset = childCount;
      int length = (int) input.lengths[inputElementNum];
      int inputOffset = (int) input.offsets[inputElementNum];
      offsets[outputElementNum] = offset;
      childCount += length;
      lengths[outputElementNum] = length;
      child.ensureSize(childCount, true);
      for (int i = 0; i < length; ++i) {
        final int outputIndex = i + offset;
        child.isNull[outputIndex] = false;
        child.setElement(outputIndex, inputOffset + i, input.child);
      }
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
      buffer.append('[');
      boolean isFirst = true;
      for(long i=offsets[row]; i < offsets[row] + lengths[row]; ++i) {
        if (isFirst) {
          isFirst = false;
        } else {
          buffer.append(", ");
        }
        child.stringifyValue(buffer, (int) i);
      }
      buffer.append(']');
    } else {
      buffer.append("null");
    }
  }

  @Override
  public void init() {
    super.init();
    child.init();
  }

  @Override
  public void reset() {
    super.reset();
    child.reset();
  }

  @Override
  public void unFlatten() {
    super.unFlatten();
    if (!isRepeating || noNulls || !isNull[0]) {
      child.unFlatten();
    }
  }

  @Override
  public void copySelected(boolean selectedInUse, int[] sel, int size,
      ColumnVector outputColVector) {
    ListColumnVector output = (ListColumnVector) outputColVector;
    boolean[] outputIsNull = output.isNull;

    // We do not need to do a column reset since we are carefully changing the output.
    output.isRepeating = false;

    // Handle repeating case
    if (isRepeating) {
      if (noNulls || !isNull[0]) {
        outputIsNull[0] = false;
        outputColVector.setElement(0, 0, this);
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
            outputColVector.setElement(i, i, this);
          }
        } else {
          for(int j = 0; j != size; j++) {
            final int i = sel[j];
            outputColVector.setElement(i, i, this);
          }
        }
      } else {
        if (!outputColVector.noNulls) {

          // Assume it is almost always a performance win to fill all of isNull so we can
          // safely reset noNulls.
          Arrays.fill(outputIsNull, false);
          outputColVector.noNulls = true;
        }
        child.shallowCopyTo(output.child);
        System.arraycopy(offsets, 0, output.offsets, 0, size);
        System.arraycopy(lengths, 0, output.lengths, 0, size);
        output.childCount = childCount;
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
          outputColVector.setElement(i, i, this);
        }
      } else {
        child.shallowCopyTo(output.child);
        System.arraycopy(isNull, 0, output.isNull, 0, size);
        System.arraycopy(offsets, 0, output.offsets, 0, size);
        System.arraycopy(lengths, 0, output.lengths, 0, size);
        output.childCount = childCount;
      }
    }
  }

  @Override
  public void shallowCopyTo(ColumnVector otherCv) {
    ListColumnVector other = (ListColumnVector)otherCv;
    super.shallowCopyTo(other);
    child.shallowCopyTo(other.child);
  }
}
