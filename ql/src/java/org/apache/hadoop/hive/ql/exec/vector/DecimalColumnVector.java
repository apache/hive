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
import org.apache.hadoop.hive.common.type.Decimal128;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

public class DecimalColumnVector extends ColumnVector {

  /**
   * A vector if Decimal128 objects. These are mutable and have fairly
   * efficient operations on them. This will make it faster to load
   * column vectors and perform decimal vector operations with decimal-
   * specific VectorExpressions.
   *
   * For high performance and easy access to this low-level structure,
   * the fields are public by design (as they are in other ColumnVector
   * types).
   */
  public Decimal128[] vector;
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
    vector = new Decimal128[len];
    for (int i = 0; i < len; i++) {
      vector[i] = new Decimal128(0, this.scale);
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
      Decimal128 dec = vector[index];
      writableObj.set(HiveDecimal.create(dec.toBigDecimal()));
      return writableObj;
    }
  }

  @Override
  public void flatten(boolean selectedInUse, int[] sel, int size) {
    // TODO Auto-generated method stub
  }

  @Override
  public void setElement(int outElementNum, int inputElementNum, ColumnVector inputVector) {
    vector[outElementNum].update(((DecimalColumnVector) inputVector).vector[inputElementNum]);
    vector[outElementNum].changeScaleDestructive(scale);
  }

  /**
   * Check if the value at position i fits in the available precision,
   * and convert the value to NULL if it does not.
   */
  public void checkPrecisionOverflow(int i) {
    try {
      vector[i].checkPrecisionOverflow(precision);
    } catch (ArithmeticException e) {

      // If the value won't fit in the available precision, the result is NULL
      noNulls = false;
      isNull[i] = true;
    }
  }
}
