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

import java.math.BigInteger;

import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

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

  private final HiveDecimalWritable writableObj = new HiveDecimalWritable();

  public DecimalColumnVector(int precision, int scale) {
    this(VectorizedRowBatch.DEFAULT_SIZE, precision, scale);
  }

  public DecimalColumnVector(int size, int precision, int scale) {
    super(size);
    this.precision = (short) precision;
    this.scale = (short) scale;
    final int len = size;
    vector = new HiveDecimalWritable[len];
    for (int i = 0; i < len; i++) {
      vector[i] = new HiveDecimalWritable(HiveDecimal.ZERO);
    }
  }

  @Override
  public Writable getWritableObject(int index) {
    if (isRepeating) {
      index = 0;
    }
    if (!noNulls && isNull[index]) {
      return NullWritable.get();
    } else {
      writableObj.set(vector[index]);
      return writableObj;
    }
  }

  @Override
  public void flatten(boolean selectedInUse, int[] sel, int size) {
    // TODO Auto-generated method stub
  }

  @Override
  public void setElement(int outElementNum, int inputElementNum, ColumnVector inputVector) {
    HiveDecimal hiveDec = ((DecimalColumnVector) inputVector).vector[inputElementNum].getHiveDecimal(precision, scale);
    if (hiveDec == null) {
      noNulls = false;
      isNull[outElementNum] = true;
    } else {
      vector[outElementNum].set(hiveDec);
    }
  }

  public void set(int elementNum, HiveDecimalWritable writeable) {
    HiveDecimal hiveDec = writeable.getHiveDecimal(precision, scale);
    if (hiveDec == null) {
      noNulls = false;
      isNull[elementNum] = true;
    } else {
      vector[elementNum].set(hiveDec);
    }
  }

  public void set(int elementNum, HiveDecimal hiveDec) {
    HiveDecimal checkedDec = HiveDecimal.enforcePrecisionScale(hiveDec, precision, scale);
    if (checkedDec == null) {
      noNulls = false;
      isNull[elementNum] = true;
    } else {
      vector[elementNum].set(checkedDec);
    }
  }

  public void setNullDataValue(int elementNum) {
    // E.g. For scale 2 the minimum is "0.01"
    HiveDecimal minimumNonZeroValue = HiveDecimal.create(BigInteger.ONE, scale);
    vector[elementNum].set(minimumNonZeroValue);
  }
}
