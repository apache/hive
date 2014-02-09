/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.VInt;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * This file is taken from a patch to hive 0.11
 * Issue : https://issues.apache.org/jira/browse/HIVE-2693
 *
 */
public class BigDecimalWritable implements WritableComparable<BigDecimalWritable> {

  private byte[] internalStorage = new byte[0];
  private int scale;

  private final VInt vInt = new VInt(); // reusable integer

  public BigDecimalWritable() {
  }

  public BigDecimalWritable(final byte[] bytes, final int scale) {
    set(bytes, scale);
  }

  public BigDecimalWritable(final BigDecimalWritable writable) {
    set(writable.getBigDecimal());
  }

  public BigDecimalWritable(final BigDecimal value) {
    set(value);
  }

  public void set(BigDecimal value) {
    value = value.stripTrailingZeros();
    if (value.compareTo(BigDecimal.ZERO) == 0) {
      // Special case for 0, because java doesn't strip zeros correctly on
      // that number.
      value = BigDecimal.ZERO;
    }
    set(value.unscaledValue().toByteArray(), value.scale());
  }

  public void set(final BigDecimalWritable writable) {
    set(writable.getBigDecimal());
  }

  public void set(final byte[] bytes, final int scale) {
    this.internalStorage = bytes;
    this.scale = scale;
  }

  public void setFromBytes(final byte[] bytes, int offset, final int length) {
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

  public BigDecimal getBigDecimal() {
    return new BigDecimal(new BigInteger(internalStorage), scale);
  }

  @Override
  public void readFields(final DataInput in) throws IOException {
    scale = WritableUtils.readVInt(in);
    final int byteArrayLen = WritableUtils.readVInt(in);
    if (internalStorage.length != byteArrayLen) {
      internalStorage = new byte[byteArrayLen];
    }
    in.readFully(internalStorage);
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, scale);
    WritableUtils.writeVInt(out, internalStorage.length);
    out.write(internalStorage);
  }

  @Override
  public int compareTo(final BigDecimalWritable that) {
    return getBigDecimal().compareTo(that.getBigDecimal());
  }

  public void writeToByteStream(final Output byteStream) {
    LazyBinaryUtils.writeVInt(byteStream, scale);
    LazyBinaryUtils.writeVInt(byteStream, internalStorage.length);
    byteStream.write(internalStorage, 0, internalStorage.length);
  }

  @Override
  public String toString() {
    return getBigDecimal().toString();
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof BigDecimalWritable)) {
      return false;
    }
    final BigDecimalWritable bdw = (BigDecimalWritable) other;

    // 'equals' and 'compareTo' are not compatible with BigDecimals. We want
    // compareTo which returns true iff the numbers are equal (e.g.: 3.14 is
        // the same as 3.140). 'Equals' returns true iff equal and the same
    // scale
    // is set in the decimals (e.g.: 3.14 is not the same as 3.140)
    return getBigDecimal().compareTo(bdw.getBigDecimal()) == 0;
  }

  @Override
  public int hashCode() {
    return getBigDecimal().hashCode();
  }

}
