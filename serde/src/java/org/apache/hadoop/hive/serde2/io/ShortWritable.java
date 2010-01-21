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

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ShortWritable implements WritableComparable {
  private short value;

  public void write(DataOutput out) throws IOException {
    out.writeShort(value);
  }

  public void readFields(DataInput in) throws IOException {
    value = in.readShort();
  }

  public ShortWritable(short s) {
    value = s;
  }

  public ShortWritable() {
    value = 0;
  }

  public void set(short value) {
    this.value = value;
  }

  public short get() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || o.getClass() != ShortWritable.class) {
      return false;
    }
    return get() == ((ShortWritable) o).get();
  }

  @Override
  public int hashCode() {
    return value;
  }

  @Override
  public String toString() {
    return String.valueOf(get());
  }

  public int compareTo(Object o) {
    int thisValue = value;
    int thatValue = ((ShortWritable) o).value;
    return thisValue - thatValue;
  }

  /** A Comparator optimized for BytesWritable. */
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(ShortWritable.class);
    }

    /**
     * Compare the buffers in serialized form.
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      int a1 = (short) (readUnsignedShort(b1, s1));
      int a2 = (short) (readUnsignedShort(b2, s2));
      return a1 - a2;
    }
  }

  static { // register this comparator
    WritableComparator.define(ShortWritable.class, new Comparator());
  }

}
