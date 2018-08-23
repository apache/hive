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

import org.apache.hadoop.io.Writable;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Writable implementation of Kafka ConsumerRecord.
 * Serialized in the form
 * {@code timestamp} long| {@code partition} (int) | {@code offset} (long) |
 * {@code startOffset} (long) | {@code endOffset} (long) | {@code value.size()} (int) | {@code value} (byte [])
 */
public class KafkaRecordWritable implements Writable {

  /**
   * Kafka partition id
   */
  private int partition;
  /**
   * Record Offset
   */
  private long offset;
  /**
   * Fist offset given by the input split used to pull the event {@link KafkaPullerInputSplit#getStartOffset()}
   */
  private long startOffset;
  /**
   * Last Offset given by the input split used to pull the event {@link KafkaPullerInputSplit#getEndOffset()}
   */
  private long endOffset;
  /**
   * Event timestamp provided by Kafka Record {@link ConsumerRecord#timestamp()}
   */
  private long timestamp;
  /**
   * Record value
   */
  private byte[] value;

  void set(ConsumerRecord<byte[], byte[]> consumerRecord, long startOffset, long endOffset) {
    this.partition = consumerRecord.partition();
    this.timestamp = consumerRecord.timestamp();
    this.offset = consumerRecord.offset();
    this.value = consumerRecord.value();
    this.startOffset = startOffset;
    this.endOffset = endOffset;
  }

   KafkaRecordWritable(int partition,
      long offset,
      long timestamp,
      byte[] value,
      long startOffset,
      long endOffset) {
    this.partition = partition;
    this.offset = offset;
    this.timestamp = timestamp;
    this.value = value;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
  }

  @SuppressWarnings("WeakerAccess") public KafkaRecordWritable() {
  }

  @Override public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(timestamp);
    dataOutput.writeInt(partition);
    dataOutput.writeLong(offset);
    dataOutput.writeLong(startOffset);
    dataOutput.writeLong(endOffset);
    dataOutput.writeInt(value.length);
    dataOutput.write(value);
  }

  @Override public void readFields(DataInput dataInput) throws IOException {
    timestamp = dataInput.readLong();
    partition = dataInput.readInt();
    offset = dataInput.readLong();
    startOffset = dataInput.readLong();
    endOffset = dataInput.readLong();
    int size = dataInput.readInt();
    if (size > 0) {
      value = new byte[size];
      dataInput.readFully(value);
    } else {
      value = new byte[0];
    }
  }

  int getPartition() {
    return partition;
  }

  long getOffset() {
    return offset;
  }

  long getTimestamp() {
    return timestamp;
  }

  byte[] getValue() {
    return value;
  }

  long getStartOffset() {
    return startOffset;
  }

  long getEndOffset() {
    return endOffset;
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KafkaRecordWritable)) {
      return false;
    }
    KafkaRecordWritable writable = (KafkaRecordWritable) o;
    return partition == writable.partition
        && offset == writable.offset
        && startOffset == writable.startOffset
        && endOffset == writable.endOffset
        && timestamp == writable.timestamp
        && Arrays.equals(value, writable.value);
  }

  @Override public int hashCode() {
    int result = Objects.hash(partition, offset, startOffset, endOffset, timestamp);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }

  @Override public String toString() {
    return "KafkaRecordWritable{"
        + "partition="
        + partition
        + ", offset="
        + offset
        + ", startOffset="
        + startOffset
        + ", endOffset="
        + endOffset
        + ", timestamp="
        + timestamp
        + ", value="
        + Arrays.toString(value)
        + '}';
  }
}
