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

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

/**

 */
public class Decimal64ColumnVector extends LongColumnVector implements IDecimalColumnVector {

  public short scale;
  public short precision;

  private HiveDecimalWritable scratchHiveDecWritable;

  public Decimal64ColumnVector(int precision, int scale) {
    this(VectorizedRowBatch.DEFAULT_SIZE, precision, scale);
  }

  public Decimal64ColumnVector(int size, int precision, int scale) {
    super(size);
    this.precision = (short) precision;
    this.scale = (short) scale;
    scratchHiveDecWritable = new HiveDecimalWritable();
  }

  /**
   * Set a Decimal64 field from a HiveDecimalWritable.
   *
   * This is a FAST version that assumes the caller has checked to make sure the writable
   * is not null and elementNum is correctly adjusted for isRepeating.  And, that the isNull entry
   * has been set.
   *
   * We will check for precision/scale range, so the entry's NULL may get set.
   * Otherwise, only the output entry fields will be set by this method.
   *
   * @param elementNum
   * @param writable
   */
  public void set(int elementNum, HiveDecimalWritable writable) {
    scratchHiveDecWritable.set(writable);
    scratchHiveDecWritable.mutateEnforcePrecisionScale(precision, scale);
    if (!scratchHiveDecWritable.isSet()) {
      noNulls = false;
      isNull[elementNum] = true;
    } else {
      vector[elementNum] = scratchHiveDecWritable.serialize64(scale);
    }
  }

  /**
   * Set a Decimal64 field from a HiveDecimal.
   *
   * This is a FAST version that assumes the caller has checked to make sure the hiveDec
   * is not null and elementNum is correctly adjusted for isRepeating.  And, that the isNull entry
   * has been set.
   *
   * We will check for precision/scale range, so the entry's NULL may get set.
   * Otherwise, only the output entry fields will be set by this method.
   *
   * @param elementNum
   * @param hiveDec
   */
  public void set(int elementNum, HiveDecimal hiveDec) {
    scratchHiveDecWritable.set(hiveDec);
    scratchHiveDecWritable.mutateEnforcePrecisionScale(precision, scale);
    if (!scratchHiveDecWritable.isSet()) {
      noNulls = false;
      isNull[elementNum] = true;
    } else {
      vector[elementNum] = scratchHiveDecWritable.serialize64(scale);
    }
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
      Decimal64ColumnVector decimal64ColVector = (Decimal64ColumnVector) inputColVector;
      scratchHiveDecWritable.deserialize64(
          decimal64ColVector.vector[inputElementNum], decimal64ColVector.scale);
      scratchHiveDecWritable.mutateEnforcePrecisionScale(precision, scale);
      if (scratchHiveDecWritable.isSet()) {
        vector[outputElementNum] = scratchHiveDecWritable.serialize64(scale);
      } else {

        // In effect, the input is NULL because of out-of-range precision/scale.
        noNulls = false;
        isNull[outputElementNum] = true;
      }
    } else {

      // Only mark output NULL when input is NULL.
      isNull[outputElementNum] = true;
      noNulls = false;
    }
  }

  /**
   * Return a convenience writable object stored by this column vector.
   * @return
   */
  public HiveDecimalWritable getScratchWritable() {
    return scratchHiveDecWritable;
  }
}
