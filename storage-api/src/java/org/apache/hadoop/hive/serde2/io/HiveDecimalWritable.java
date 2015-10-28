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
package org.apache.hadoop.hive.serde2.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.hive.common.type.HiveDecimal;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class HiveDecimalWritable implements WritableComparable<HiveDecimalWritable> {

  private byte[] internalStorage = new byte[0];
  private int scale;

  public HiveDecimalWritable() {
  }

  public HiveDecimalWritable(String value) {
    set(HiveDecimal.create(value));
  }

  public HiveDecimalWritable(byte[] bytes, int scale) {
    set(bytes, scale);
  }

  public HiveDecimalWritable(HiveDecimalWritable writable) {
    set(writable.getHiveDecimal());
  }

  public HiveDecimalWritable(HiveDecimal value) {
    set(value);
  }

  public HiveDecimalWritable(long value) {
    set((HiveDecimal.create(value)));
  }

  public void set(HiveDecimal value) {
    set(value.unscaledValue().toByteArray(), value.scale());
  }

  public void set(HiveDecimal value, int maxPrecision, int maxScale) {
    set(HiveDecimal.enforcePrecisionScale(value, maxPrecision, maxScale));
  }

  public void set(HiveDecimalWritable writable) {
    set(writable.getHiveDecimal());
  }

  public void set(byte[] bytes, int scale) {
    this.internalStorage = bytes;
    this.scale = scale;
  }

  public HiveDecimal getHiveDecimal() {
    return HiveDecimal.create(new BigInteger(internalStorage), scale);
  }

  /**
   * Get a HiveDecimal instance from the writable and constraint it with maximum precision/scale.
   *
   * @param maxPrecision maximum precision
   * @param maxScale maximum scale
   * @return HiveDecimal instance
   */
  public HiveDecimal getHiveDecimal(int maxPrecision, int maxScale) {
     return HiveDecimal.enforcePrecisionScale(HiveDecimal.
             create(new BigInteger(internalStorage), scale),
         maxPrecision, maxScale);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    scale = WritableUtils.readVInt(in);
    int byteArrayLen = WritableUtils.readVInt(in);
    if (internalStorage.length != byteArrayLen) {
      internalStorage = new byte[byteArrayLen];
    }
    in.readFully(internalStorage);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, scale);
    WritableUtils.writeVInt(out, internalStorage.length);
    out.write(internalStorage);
  }

  @Override
  public int compareTo(HiveDecimalWritable that) {
    return getHiveDecimal().compareTo(that.getHiveDecimal());
  }

  @Override
  public String toString() {
    return getHiveDecimal().toString();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    HiveDecimalWritable bdw = (HiveDecimalWritable) other;

    // 'equals' and 'compareTo' are not compatible with HiveDecimals. We want
    // compareTo which returns true iff the numbers are equal (e.g.: 3.14 is
    // the same as 3.140). 'Equals' returns true iff equal and the same scale
    // is set in the decimals (e.g.: 3.14 is not the same as 3.140)
    return getHiveDecimal().compareTo(bdw.getHiveDecimal()) == 0;
  }

  @Override
  public int hashCode() {
    return getHiveDecimal().hashCode();
  }

  /* (non-Javadoc)
   * In order to update a Decimal128 fast (w/o allocation) we need to expose access to the
   * internal storage bytes and scale.
   * @return
   */
  public byte[] getInternalStorage() {
    return internalStorage;
  }

  /* (non-Javadoc)
   * In order to update a Decimal128 fast (w/o allocation) we need to expose access to the
   * internal storage bytes and scale.
   */
  public int getScale() {
    return scale;
  }

  public static
  HiveDecimalWritable enforcePrecisionScale(HiveDecimalWritable writable,
                                            int precision, int scale) {
    if (writable == null) {
      return null;
    }

    HiveDecimal dec =
        HiveDecimal.enforcePrecisionScale(writable.getHiveDecimal(), precision,
            scale);
    return dec == null ? null : new HiveDecimalWritable(dec);
  }
}
