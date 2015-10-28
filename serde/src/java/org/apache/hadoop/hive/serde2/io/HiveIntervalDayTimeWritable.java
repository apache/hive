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
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.serde2.ByteStream.RandomAccessOutput;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.VInt;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.VLong;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class HiveIntervalDayTimeWritable
    implements WritableComparable<HiveIntervalDayTimeWritable> {

  static final private Logger LOG = LoggerFactory.getLogger(HiveIntervalDayTimeWritable.class);

  protected HiveIntervalDayTime intervalValue = new HiveIntervalDayTime();

  public HiveIntervalDayTimeWritable() {
  }

  public HiveIntervalDayTimeWritable(HiveIntervalDayTime value) {
    intervalValue.set(value);
  }

  public HiveIntervalDayTimeWritable(HiveIntervalDayTimeWritable writable) {
    intervalValue.set(writable.intervalValue);
  }

  public void set(int days, int hours, int minutes, int seconds, int nanos) {
    intervalValue.set(days, hours, minutes, seconds, nanos);
  }

  public void set(HiveIntervalDayTime value) {
    intervalValue.set(value);
  }

  public void set(HiveIntervalDayTimeWritable writable) {
    intervalValue.set(writable.intervalValue);
  }

  public void set(long totalSeconds, int nanos) {
    intervalValue.set(totalSeconds, nanos);
  }

  public HiveIntervalDayTime getHiveIntervalDayTime() {
    return new HiveIntervalDayTime(intervalValue);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // read totalSeconds, nanos from DataInput
    set(WritableUtils.readVLong(in), WritableUtils.readVInt(in));
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // write totalSeconds, nanos to DataOutput
    WritableUtils.writeVLong(out, intervalValue.getTotalSeconds());
    WritableUtils.writeVInt(out, intervalValue.getNanos());
  }

  public void writeToByteStream(RandomAccessOutput byteStream) {
    LazyBinaryUtils.writeVLong(byteStream, intervalValue.getTotalSeconds());
    LazyBinaryUtils.writeVInt(byteStream, intervalValue.getNanos());
  }

  public void setFromBytes(byte[] bytes, int offset, int length, VInt vInt, VLong vLong) {
    LazyBinaryUtils.readVLong(bytes, offset, vLong);
    LazyBinaryUtils.readVInt(bytes, offset + vLong.length, vInt);
    assert (length == (vInt.length + vLong.length));
    set(vLong.value, vInt.value);
  }

  @Override
  public int compareTo(HiveIntervalDayTimeWritable other) {
    return this.intervalValue.compareTo(other.intervalValue);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof HiveIntervalDayTimeWritable)) {
      return false;
    }
    return 0 == compareTo((HiveIntervalDayTimeWritable) obj);
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
