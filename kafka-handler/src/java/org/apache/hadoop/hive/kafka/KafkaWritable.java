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

package org.apache.hadoop.hive.kafka;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nullable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Writable implementation of Kafka ConsumerRecord.
 * Serialized in the form:
 * {@code timestamp} long| {@code partition} (int) | {@code offset} (long) | {@code value.size()} (int) |
 * {@code value} (byte []) | {@code recordKey.size()}| {@code recordKey (byte [])}
 */
public class KafkaWritable implements Writable {

  private int partition;
  private long offset;
  private long timestamp;
  private byte[] value;
  private byte[] recordKey;

  void set(ConsumerRecord<byte[], byte[]> consumerRecord) {
    this.partition = consumerRecord.partition();
    this.timestamp = consumerRecord.timestamp();
    this.offset = consumerRecord.offset();
    this.value = consumerRecord.value();
    this.recordKey = consumerRecord.key();
  }

  KafkaWritable(int partition, long offset, long timestamp, byte[] value, @Nullable byte[] recordKey) {
    this.partition = partition;
    this.offset = offset;
    this.timestamp = timestamp;
    this.value = value;
    this.recordKey = recordKey;
  }

  KafkaWritable(int partition, long timestamp, byte[] value, @Nullable byte[] recordKey) {
    this(partition, -1, timestamp, value, recordKey);
  }

  @SuppressWarnings("WeakerAccess") public KafkaWritable() {
  }

  @Override public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(timestamp);
    dataOutput.writeInt(partition);
    dataOutput.writeLong(offset);
    dataOutput.writeInt(value.length);
    dataOutput.write(value);
    if (recordKey != null) {
      dataOutput.writeInt(recordKey.length);
      dataOutput.write(recordKey);
    } else {
      dataOutput.writeInt(-1);
    }
  }

  @Override public void readFields(DataInput dataInput) throws IOException {
    timestamp = dataInput.readLong();
    partition = dataInput.readInt();
    offset = dataInput.readLong();
    int dataSize = dataInput.readInt();
    if (dataSize > 0) {
      value = new byte[dataSize];
      dataInput.readFully(value);
    } else {
      value = new byte[0];
    }
    int keyArraySize = dataInput.readInt();
    if (keyArraySize > -1) {
      recordKey = new byte[keyArraySize];
      dataInput.readFully(recordKey);
    } else {
      recordKey = null;
    }
  }

  int getPartition() {
    return partition;
  }

  @SuppressWarnings("WeakerAccess") long getOffset() {
    return offset;
  }

  long getTimestamp() {
    return timestamp;
  }

  byte[] getValue() {
    return value;
  }

  @Nullable byte[] getRecordKey() {
    return recordKey;
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KafkaWritable)) {
      return false;
    }
    KafkaWritable writable = (KafkaWritable) o;
    return partition == writable.partition
        && offset == writable.offset
        && timestamp == writable.timestamp
        && Arrays.equals(value, writable.value)
        && Arrays.equals(recordKey, writable.recordKey);
  }

  @Override public int hashCode() {
    int result = Objects.hash(partition, offset, timestamp);
    result = 31 * result + Arrays.hashCode(value);
    result = 31 * result + Arrays.hashCode(recordKey);
    return result;
  }

  @Override public String toString() {
    return "KafkaWritable{"
        + "partition="
        + partition
        + ", offset="
        + offset
        + ", timestamp="
        + timestamp
        + ", value="
        + Arrays.toString(value)
        + ", recordKey="
        + Arrays.toString(recordKey)
        + '}';
  }

  Writable getHiveWritable(MetadataColumn metadataColumn) {
    switch (metadataColumn) {
    case OFFSET:
      return new LongWritable(getOffset());
    case PARTITION:
      return new IntWritable(getPartition());
    case TIMESTAMP:
      return new LongWritable(getTimestamp());
    case KEY:
      return getRecordKey() == null ? null : new BytesWritable(getRecordKey());
    default:
      throw new IllegalArgumentException("Unknown metadata column [" + metadataColumn.getName() + "]");
    }
  }

}
