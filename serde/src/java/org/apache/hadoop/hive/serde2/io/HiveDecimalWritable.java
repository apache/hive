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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.VInt;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class HiveDecimalWritable implements WritableComparable<HiveDecimalWritable> {

  static final private Log LOG = LogFactory.getLog(HiveDecimalWritable.class);

  private byte[] internalStorage = new byte[0];
  private int scale;

  private final VInt vInt = new VInt(); // reusable integer

  public HiveDecimalWritable() {
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

  public void set(HiveDecimal value) {
    set(value.unscaledValue().toByteArray(), value.scale());
  }

  public void set(HiveDecimalWritable writable) {
    set(writable.getHiveDecimal());
  }

  public void set(byte[] bytes, int scale) {
    this.internalStorage = bytes;
    this.scale = scale;
  }

  public void setFromBytes(byte[] bytes, int offset, int length) {
    LazyBinaryUtils.readVInt(bytes, offset, vInt);
    scale = vInt.value;
    offset += vInt.length;
    LazyBinaryUtils.readVInt(bytes, offset, vInt);
    offset += vInt.length;
    if (internalStorage.length != vInt.value) {
      internalStorage = new byte[vInt.value];
    }
    System.arraycopy(bytes, offset, internalStorage, 0, vInt.value);
  }

  public HiveDecimal getHiveDecimal() {
    return new HiveDecimal(new BigInteger(internalStorage), scale);
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

  public void writeToByteStream(Output byteStream) {
    LazyBinaryUtils.writeVInt(byteStream, scale);
    LazyBinaryUtils.writeVInt(byteStream, internalStorage.length);
    byteStream.write(internalStorage, 0, internalStorage.length);
  }

  @Override
  public String toString() {
    return getHiveDecimal().toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof HiveDecimalWritable)) {
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
}
