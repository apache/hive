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
    super(len);
    this.child = child;
  }

  @Override
  protected void childFlatten(boolean useSelected, int[] selected, int size) {
    child.flatten(useSelected, selected, size);
  }

  @Override
  public void setElement(int outElementNum, int inputElementNum,
                         ColumnVector inputVector) {
    ListColumnVector input = (ListColumnVector) inputVector;
    if (input.isRepeating) {
      inputElementNum = 0;
    }
    if (!input.noNulls && input.isNull[inputElementNum]) {
      isNull[outElementNum] = true;
      noNulls = false;
    } else {
      isNull[outElementNum] = false;
      int offset = childCount;
      int length = (int) input.lengths[inputElementNum];
      int inputOffset = (int) input.offsets[inputElementNum];
      offsets[outElementNum] = offset;
      childCount += length;
      lengths[outElementNum] = length;
      child.ensureSize(childCount, true);
      for (int i = 0; i < length; ++i) {
        child.setElement(i + offset, inputOffset + i, input.child);
      }
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

}
