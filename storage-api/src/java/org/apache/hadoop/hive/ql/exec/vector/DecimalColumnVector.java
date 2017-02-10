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


import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.common.type.HiveDecimal;

public class DecimalColumnVector extends ColumnVector {

  /**
   * A vector of HiveDecimalWritable objects.
   *
   * For high performance and easy access to this low-level structure,
   * the fields are public by design (as they are in other ColumnVector
   * types).
   */
  public HiveDecimalWritable[] vector;
  public short scale;
  public short precision;

  public DecimalColumnVector(int precision, int scale) {
    this(VectorizedRowBatch.DEFAULT_SIZE, precision, scale);
  }

  public DecimalColumnVector(int size, int precision, int scale) {
    super(size);
    this.precision = (short) precision;
    this.scale = (short) scale;
    vector = new HiveDecimalWritable[size];
    for (int i = 0; i < size; i++) {
      vector[i] = new HiveDecimalWritable(0);  // Initially zero.
    }
  }

  // Fill the all the vector entries with provided value
  public void fill(HiveDecimal value) {
    noNulls = true;
    isRepeating = true;
    if (vector[0] == null) {
      vector[0] = new HiveDecimalWritable(value);
    } else {
      vector[0].set(value);
    }
  }

  @Override
  public void flatten(boolean selectedInUse, int[] sel, int size) {
    // TODO Auto-generated method stub
  }

  @Override
  public void setElement(int outElementNum, int inputElementNum, ColumnVector inputVector) {
    if (inputVector.isRepeating) {
      inputElementNum = 0;
    }
    if (inputVector.noNulls || !inputVector.isNull[inputElementNum]) {
      vector[outElementNum].set(
          ((DecimalColumnVector) inputVector).vector[inputElementNum],
          precision, scale);
      if (!vector[outElementNum].isSet()) {
        isNull[outElementNum] = true;
        noNulls = false;
      } else {
        isNull[outElementNum] = false;
      }
    } else {
      isNull[outElementNum] = true;
      noNulls = false;
    }
  }

  @Override
  public void stringifyValue(StringBuilder buffer, int row) {
    if (isRepeating) {
      row = 0;
    }
    if (noNulls || !isNull[row]) {
      buffer.append(vector[row].toString());
    } else {
      buffer.append("null");
    }
  }

  public void set(int elementNum, HiveDecimalWritable writeable) {
    vector[elementNum].set(writeable, precision, scale);
    if (!vector[elementNum].isSet()) {
      noNulls = false;
      isNull[elementNum] = true;
    } else {
      isNull[elementNum] = false;
    }
  }

  public void set(int elementNum, HiveDecimal hiveDec) {
    vector[elementNum].set(hiveDec, precision, scale);
    if (!vector[elementNum].isSet()) {
      noNulls = false;
      isNull[elementNum] = true;
    } else {
      isNull[elementNum] = false;
    }
  }

  public void setNullDataValue(int elementNum) {
    // E.g. For scale 2 the minimum is "0.01"
    vector[elementNum].setFromLongAndScale(1L, scale);
  }

  @Override
  public void ensureSize(int size, boolean preserveData) {
    super.ensureSize(size, preserveData);
    if (size <= vector.length) return; // We assume the existing vector is always valid.
    HiveDecimalWritable[] oldArray = vector;
    vector = new HiveDecimalWritable[size];
    int initPos = 0;
    if (preserveData) {
      // we copy all of the values to avoid creating more objects
      // TODO: it might be cheaper to always preserve data or reset existing objects
      initPos = oldArray.length;
      System.arraycopy(oldArray, 0, vector, 0 , oldArray.length);
    }
    for (int i = initPos; i < vector.length; ++i) {
      vector[i] = new HiveDecimalWritable(0);  // Initially zero.
    }
  }

  @Override
  public void shallowCopyTo(ColumnVector otherCv) {
    DecimalColumnVector other = (DecimalColumnVector)otherCv;
    super.shallowCopyTo(other);
    other.scale = scale;
    other.precision = precision;
    other.vector = vector;
  }
}
