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

/**
 * The representation of a vectorized column of struct objects.
 *
 * Each field is represented by a separate inner ColumnVector. Since this
 * ColumnVector doesn't own any per row data other that the isNull flag, the
 * isRepeating only covers the isNull array.
 */
public class UnionColumnVector extends ColumnVector {

  public int[] tags;
  public ColumnVector[] fields;

  public UnionColumnVector() {
    this(VectorizedRowBatch.DEFAULT_SIZE);
  }

  /**
   * Constructor for UnionColumnVector
   *
   * @param len Vector length
   * @param fields the field column vectors
   */
  public UnionColumnVector(int len, ColumnVector... fields) {
    super(Type.UNION, len);
    tags = new int[len];
    this.fields = fields;
  }

  @Override
  public void flatten(boolean selectedInUse, int[] sel, int size) {
    flattenPush();
    for(int i=0; i < fields.length; ++i) {
      fields[i].flatten(selectedInUse, sel, size);
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
      UnionColumnVector input = (UnionColumnVector) inputColVector;
      final int tag = input.tags[inputElementNum];
      tags[outputElementNum] = tag;
      ColumnVector inputField = input.fields[tag];
      ColumnVector outputField = fields[tag];
      outputField.isNull[outputElementNum] = false;
      outputField.setElement(
          outputElementNum, inputElementNum, inputField);
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
      buffer.append("{\"tag\": ");
      buffer.append(tags[row]);
      buffer.append(", \"value\": ");
      fields[tags[row]].stringifyValue(buffer, row);
      buffer.append('}');
    } else {
      buffer.append("null");
    }
  }

  @Override
  public void ensureSize(int size, boolean preserveData) {
    super.ensureSize(size, preserveData);
    if (tags.length < size) {
      if (preserveData) {
        int[] oldTags = tags;
        tags = new int[size];
        System.arraycopy(oldTags, 0, tags, 0, oldTags.length);
      } else {
        tags = new int[size];
      }
      for(int i=0; i < fields.length; ++i) {
        fields[i].ensureSize(size, preserveData);
      }
    }
  }

  @Override
  public void reset() {
    super.reset();
    for(int i =0; i < fields.length; ++i) {
      fields[i].reset();
    }
  }

  @Override
  public void init() {
    super.init();
    for(int i =0; i < fields.length; ++i) {
      fields[i].init();
    }
  }

  @Override
  public void unFlatten() {
    super.unFlatten();
    for(int i=0; i < fields.length; ++i) {
      fields[i].unFlatten();
    }
  }

  @Override
  public void setRepeating(boolean isRepeating) {
    super.setRepeating(isRepeating);
    for(int i=0; i < fields.length; ++i) {
      fields[i].setRepeating(isRepeating);
    }
  }

  @Override
  public void shallowCopyTo(ColumnVector otherCv) {
    throw new UnsupportedOperationException(); // Implement if needed.
  }

  @Override
  public void copySelected(boolean selectedInUse, int[] sel, int size,
      ColumnVector outputColVector) {
    throw new RuntimeException("Not supported");
  }
}
