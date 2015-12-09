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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.serde2.ByteStream.RandomAccessOutput;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.VInt;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class HiveIntervalYearMonthWritable
    implements WritableComparable<HiveIntervalYearMonthWritable> {

  static final private Logger LOG = LoggerFactory.getLogger(HiveIntervalYearMonthWritable.class);

  protected HiveIntervalYearMonth intervalValue = new HiveIntervalYearMonth();

  public HiveIntervalYearMonthWritable() {
  }

  public HiveIntervalYearMonthWritable(HiveIntervalYearMonth hiveInterval) {
    intervalValue.set(hiveInterval);
  }

  public HiveIntervalYearMonthWritable(HiveIntervalYearMonthWritable hiveIntervalWritable) {
    intervalValue.set(hiveIntervalWritable.intervalValue);
  }

  public void set(int years, int months) {
    intervalValue.set(years, months);
  }

  public void set(HiveIntervalYearMonth hiveInterval) {
    intervalValue.set(hiveInterval);
  }

  public void set(HiveIntervalYearMonthWritable hiveIntervalWritable) {
    intervalValue.set(hiveIntervalWritable.intervalValue);
  }

  public void set(int totalMonths) {
    intervalValue.set(totalMonths);
  }

  public HiveIntervalYearMonth getHiveIntervalYearMonth() {
    return new HiveIntervalYearMonth(intervalValue);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // read totalMonths from DataInput
    set(WritableUtils.readVInt(in));
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // write totalMonths to DataOutput
    WritableUtils.writeVInt(out, intervalValue.getTotalMonths());
  }

  public void writeToByteStream(RandomAccessOutput byteStream) {
    LazyBinaryUtils.writeVInt(byteStream, intervalValue.getTotalMonths());
  }

  public void setFromBytes(byte[] bytes, int offset, int length, VInt vInt) {
    LazyBinaryUtils.readVInt(bytes, offset, vInt);
    assert (length == vInt.length);
    set(vInt.value);
  }

  @Override
  public int compareTo(HiveIntervalYearMonthWritable other) {
    return this.intervalValue.compareTo(other.intervalValue);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof HiveIntervalYearMonthWritable)) {
      return false;
    }
    return 0 == compareTo((HiveIntervalYearMonthWritable) obj);
  }

  @Override
  public int hashCode() {
    return intervalValue.hashCode();
  }

  @Override
  public String toString() {
    return intervalValue.toString();
  }
}
