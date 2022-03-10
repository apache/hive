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
package org.apache.hadoop.hive.serde2.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.hive.common.type.HiveDecimalV1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hive.common.util.SuppressFBWarnings;

public class HiveDecimalWritableV1 implements WritableComparable<HiveDecimalWritableV1> {

  private byte[] internalStorage = new byte[0];
  private int scale;

  @HiveDecimalWritableVersionV1
  public HiveDecimalWritableV1() {
  }

  @HiveDecimalWritableVersionV1
  public HiveDecimalWritableV1(String value) {
    set(HiveDecimalV1.create(value));
  }

  @HiveDecimalWritableVersionV1
  public HiveDecimalWritableV1(byte[] bytes, int scale) {
    set(bytes, scale);
  }

  @HiveDecimalWritableVersionV1
  public HiveDecimalWritableV1(HiveDecimalWritableV1 writable) {
    set(writable.getHiveDecimal());
  }

  @HiveDecimalWritableVersionV1
  public HiveDecimalWritableV1(HiveDecimalV1 value) {
    set(value);
  }

  @HiveDecimalWritableVersionV1
  public HiveDecimalWritableV1(long value) {
    set((HiveDecimalV1.create(value)));
  }

  @HiveDecimalWritableVersionV1
  public void set(HiveDecimalV1 value) {
    set(value.unscaledValue().toByteArray(), value.scale());
  }

  @HiveDecimalWritableVersionV1
  public void set(HiveDecimalV1 value, int maxPrecision, int maxScale) {
    set(HiveDecimalV1.enforcePrecisionScale(value, maxPrecision, maxScale));
  }

  @HiveDecimalWritableVersionV1
  public void set(HiveDecimalWritableV1 writable) {
    set(writable.getHiveDecimal());
  }

  @HiveDecimalWritableVersionV1
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "Ref external obj for efficiency")
  public void set(byte[] bytes, int scale) {
    this.internalStorage = bytes;
    this.scale = scale;
  }

  @HiveDecimalWritableVersionV1
  public HiveDecimalV1 getHiveDecimal() {
    return HiveDecimalV1.create(new BigInteger(internalStorage), scale);
  }

  /**
   * Get a OldHiveDecimal instance from the writable and constraint it with maximum precision/scale.
   *
   * @param maxPrecision maximum precision
   * @param maxScale maximum scale
   * @return OldHiveDecimal instance
   */
  @HiveDecimalWritableVersionV1
  public HiveDecimalV1 getHiveDecimal(int maxPrecision, int maxScale) {
     return HiveDecimalV1.enforcePrecisionScale(HiveDecimalV1.
             create(new BigInteger(internalStorage), scale),
         maxPrecision, maxScale);
  }

  @HiveDecimalWritableVersionV1
  @Override
  public void readFields(DataInput in) throws IOException {
    scale = WritableUtils.readVInt(in);
    int byteArrayLen = WritableUtils.readVInt(in);
    if (internalStorage.length != byteArrayLen) {
      internalStorage = new byte[byteArrayLen];
    }
    in.readFully(internalStorage);
  }

  @HiveDecimalWritableVersionV1
  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, scale);
    WritableUtils.writeVInt(out, internalStorage.length);
    out.write(internalStorage);
  }

  @HiveDecimalWritableVersionV1
  @Override
  public int compareTo(HiveDecimalWritableV1 that) {
    return getHiveDecimal().compareTo(that.getHiveDecimal());
  }

  @HiveDecimalWritableVersionV1
  @Override
  public String toString() {
    return getHiveDecimal().toString();
  }

  @HiveDecimalWritableVersionV1
  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    HiveDecimalWritableV1 bdw = (HiveDecimalWritableV1) other;

    // 'equals' and 'compareTo' are not compatible with HiveDecimals. We want
    // compareTo which returns true iff the numbers are equal (e.g.: 3.14 is
    // the same as 3.140). 'Equals' returns true iff equal and the same scale
    // is set in the decimals (e.g.: 3.14 is not the same as 3.140)
    return getHiveDecimal().compareTo(bdw.getHiveDecimal()) == 0;
  }

  @HiveDecimalWritableVersionV1
  @Override
  public int hashCode() {
    return getHiveDecimal().hashCode();
  }

  /* (non-Javadoc)
   * In order to update a Decimal128 fast (w/o allocation) we need to expose access to the
   * internal storage bytes and scale.
   * @return
   */
  @HiveDecimalWritableVersionV1
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "Expose internal rep for efficiency")
  public byte[] getInternalStorage() {
    return internalStorage;
  }

  /* (non-Javadoc)
   * In order to update a Decimal128 fast (w/o allocation) we need to expose access to the
   * internal storage bytes and scale.
   */
  @HiveDecimalWritableVersionV1
  public int getScale() {
    return scale;
  }

  @HiveDecimalWritableVersionV1
  public static
  HiveDecimalWritableV1 enforcePrecisionScale(HiveDecimalWritableV1 writable,
                                            int precision, int scale) {
    if (writable == null) {
      return null;
    }

    HiveDecimalV1 dec =
        HiveDecimalV1.enforcePrecisionScale(writable.getHiveDecimal(), precision,
            scale);
    return dec == null ? null : new HiveDecimalWritableV1(dec);
  }
}
