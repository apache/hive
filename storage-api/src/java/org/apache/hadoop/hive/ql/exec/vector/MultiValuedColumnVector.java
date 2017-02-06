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

/**
 * The representation of a vectorized column of multi-valued objects, such
 * as lists and maps.
 *
 * Each object is composed of a range of elements in the underlying child
 * ColumnVector. The range for list i is
 * offsets[i]..offsets[i]+lengths[i]-1 inclusive.
 */
public abstract class MultiValuedColumnVector extends ColumnVector {

  public long[] offsets;
  public long[] lengths;
  // the number of children slots used
  public int childCount;

  /**
   * Constructor for MultiValuedColumnVector.
   *
   * @param len Vector length
   */
  public MultiValuedColumnVector(int len) {
    super(len);
    childCount = 0;
    offsets = new long[len];
    lengths = new long[len];
  }

  protected abstract void childFlatten(boolean useSelected, int[] selected,
                                       int size);

  @Override
  public void flatten(boolean selectedInUse, int[] sel, int size) {
    flattenPush();

    if (isRepeating) {
      if (noNulls || !isNull[0]) {
        if (selectedInUse) {
          for (int i = 0; i < size; ++i) {
            int row = sel[i];
            offsets[row] = offsets[0];
            lengths[row] = lengths[0];
            isNull[row] = false;
          }
        } else {
          Arrays.fill(offsets, 0, size, offsets[0]);
          Arrays.fill(lengths, 0, size, lengths[0]);
          Arrays.fill(isNull, 0, size, false);
        }
        // We optimize by assuming that a repeating list/map will run from
        // from 0 .. lengths[0] in the child vector.
        // Sanity check the assumption that we can start at 0.
        if (offsets[0] != 0) {
          throw new IllegalArgumentException("Repeating offset isn't 0, but " +
                                             offsets[0]);
        }
        childFlatten(false, null, (int) lengths[0]);
      } else {
        if (selectedInUse) {
          for(int i=0; i < size; ++i) {
            isNull[sel[i]] = true;
          }
        } else {
          Arrays.fill(isNull, 0, size, true);
        }
      }
      isRepeating = false;
      noNulls = false;
    } else {
      if (selectedInUse) {
        int childSize = 0;
        for(int i=0; i < size; ++i) {
          childSize += lengths[sel[i]];
        }
        int[] childSelection = new int[childSize];
        int idx = 0;
        for(int i=0; i < size; ++i) {
          int row = sel[i];
          for(int elem=0; elem < lengths[row]; ++elem) {
            childSelection[idx++] = (int) (offsets[row] + elem);
          }
        }
        childFlatten(true, childSelection, childSize);
      } else {
        childFlatten(false, null, childCount);
      }
      flattenNoNulls(selectedInUse, sel, size);
    }
  }

  @Override
  public void ensureSize(int size, boolean preserveData) {
    super.ensureSize(size, preserveData);
    if (size > offsets.length) {
      long[] oldOffsets = offsets;
      offsets = new long[size];
      long oldLengths[] = lengths;
      lengths = new long[size];
      if (preserveData) {
        if (isRepeating) {
          offsets[0] = oldOffsets[0];
          lengths[0] = oldLengths[0];
        } else {
          System.arraycopy(oldOffsets, 0, offsets, 0 , oldOffsets.length);
          System.arraycopy(oldLengths, 0, lengths, 0, oldLengths.length);
        }
      }
    }
  }

  /**
   * Initializee the vector
   */
  @Override
  public void init() {
    super.init();
    childCount = 0;
  }

  /**
   * Reset the vector for the next batch.
   */
  @Override
  public void reset() {
    super.reset();
    childCount = 0;
  }

  @Override
  public void shallowCopyTo(ColumnVector otherCv) {
    throw new UnsupportedOperationException(); // Implement in future, if needed.
  }
}
