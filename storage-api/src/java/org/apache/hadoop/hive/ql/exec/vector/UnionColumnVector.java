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
    super(len);
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

  @Override
  public void setElement(int outElementNum, int inputElementNum,
                         ColumnVector inputVector) {
    if (inputVector.isRepeating) {
      inputElementNum = 0;
    }
    if (inputVector.noNulls || !inputVector.isNull[inputElementNum]) {
      isNull[outElementNum] = false;
      UnionColumnVector input = (UnionColumnVector) inputVector;
      tags[outElementNum] = input.tags[inputElementNum];
      fields[tags[outElementNum]].setElement(outElementNum, inputElementNum,
          input.fields[tags[outElementNum]]);
    } else {
      noNulls = false;
      isNull[outElementNum] = true;
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
}
