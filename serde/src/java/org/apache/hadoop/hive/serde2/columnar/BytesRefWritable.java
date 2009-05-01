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

package org.apache.hadoop.hive.serde2.columnar;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * <tt>BytesRefWritable</tt> referenced a section of byte array. It can be used
 * to avoid unnecessary byte copy.
 */
public class BytesRefWritable implements Writable, Comparable<BytesRefWritable> {

  private static final byte[] EMPTY_BYTES = new byte[0];
  public static BytesRefWritable ZeroBytesRefWritable = new BytesRefWritable();

  int start = 0;
  int length = 0;
  byte[] bytes = null;

  /**
   * Create a zero-size bytes.
   */
  public BytesRefWritable() {
    this(EMPTY_BYTES);
  }

  /**
   * Create a BytesRefWritable with <tt>length</tt> bytes.
   */
  public BytesRefWritable(int length) {
    assert length > 0;
    this.length = length;
    bytes = new byte[this.length];
    start = 0;
  }

  /**
   * Create a BytesRefWritable referenced to the given bytes.
   */
  public BytesRefWritable(byte[] bytes) {
    this.bytes = bytes;
    length = bytes.length;
    start = 0;
  }

  /**
   * Create a BytesRefWritable referenced to one section of the given bytes. The
   * section is determined by argument <tt>offset</tt> and <tt>len</tt>.
   */
  public BytesRefWritable(byte[] data, int offset, int len) {
    bytes = data;
    start = offset;
    length = len;
  }

  /**
   * Returns a copy of the underlying bytes referenced by this instance.
   * 
   * @return a new copied byte array
   */
  public byte[] getBytesCopy() {
    byte[] bb = new byte[length];
    System.arraycopy(bytes, start, bb, 0, length);
    return bb;
  }

  /**
   * Returns the underlying bytes.
   */
  public byte[] getData() {
    return bytes;
  }

  /**
   * readFields() will corrupt the array. So use the set method whenever
   * possible.
   * 
   * @see #readFields(DataInput)
   */
  public void set(byte[] newData, int offset, int len) {
    bytes = newData;
    start = offset;
    length = len;
  }

  public void writeDataTo(DataOutput out) throws IOException {
    out.write(bytes, start, length);
  }

  /**
   * Always reuse the bytes array if length of bytes array is equal or greater
   * to the current record, otherwise create a new one. readFields will corrupt
   * the array. Please use set() whenever possible.
   * 
   * @see #set(byte[], int, int)
   */
  public void readFields(DataInput in) throws IOException {
    int len = in.readInt();
    if (len > bytes.length) {
      bytes = new byte[len];
    }
    start = 0;
    length = len;
    in.readFully(bytes, start, length);
  }

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    out.writeInt(length);
    out.write(bytes, start, length);
  }

  /** {@inheritDoc} */
  public int hashCode() {
    return super.hashCode();
  }

  /** {@inheritDoc} */
  public String toString() {
    StringBuffer sb = new StringBuffer(3 * length);
    for (int idx = start; idx < length; idx++) {
      // if not the first, put a blank separator in
      if (idx != 0) {
        sb.append(' ');
      }
      String num = Integer.toHexString(0xff & bytes[idx]);
      // if it is only one digit, add a leading 0.
      if (num.length() < 2) {
        sb.append('0');
      }
      sb.append(num);
    }
    return sb.toString();
  }

  /** {@inheritDoc} */
  @Override
  public int compareTo(BytesRefWritable other) {
    if (other == null)
      throw new IllegalArgumentException("Argument can not be null.");
    if (this == other)
      return 0;
    return WritableComparator.compareBytes(getData(), start, getLength(), other
        .getData(), other.start, other.getLength());
  }

  /** {@inheritDoc} */
  public boolean equals(Object right_obj) {
    if (right_obj == null || !(right_obj instanceof BytesRefWritable))
      return false;
    return compareTo((BytesRefWritable) right_obj) == 0;
  }

  static {
    WritableFactories.setFactory(BytesRefWritable.class, new WritableFactory() {

      @Override
      public Writable newInstance() {
        return new BytesRefWritable();
      }

    });
  }

  public int getLength() {
    return length;
  }

  public int getStart() {
    return start;
  }
}