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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Test class for kafka Writable.
 */
@SuppressWarnings("unchecked") public class KafkaWritableTest {
  public KafkaWritableTest() {
  }

  @Test public void testWriteReadFields() throws IOException {
    ConsumerRecord<byte[], byte[]> record = new ConsumerRecord("topic", 0, 3L, "key".getBytes(), "value".getBytes());
    KafkaWritable kafkaWritable =
        new KafkaWritable(record.partition(),
            record.offset(),
            record.timestamp(),
            record.value(), null);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream w = new DataOutputStream(baos);
    kafkaWritable.write(w);
    w.flush();

    ByteArrayInputStream input = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream inputStream = new DataInputStream(input);
    KafkaWritable actualKafkaWritable = new KafkaWritable();
    actualKafkaWritable.readFields(inputStream);
    Assert.assertEquals(kafkaWritable, actualKafkaWritable);
  }


  @Test public void testWriteReadFields2() throws IOException {
    ConsumerRecord<byte[], byte[]> record = new ConsumerRecord("topic", 0, 3L, "key".getBytes(), "value".getBytes());
    KafkaWritable kafkaWritable =
        new KafkaWritable(record.partition(),
            record.offset(),
            record.timestamp(),
            record.value(), "thisKey".getBytes());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream w = new DataOutputStream(baos);
    kafkaWritable.write(w);
    w.flush();

    ByteArrayInputStream input = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream inputStream = new DataInputStream(input);
    KafkaWritable actualKafkaWritable = new KafkaWritable();
    actualKafkaWritable.readFields(inputStream);
    Assert.assertEquals(kafkaWritable, actualKafkaWritable);
  }

  @Test public void getHiveWritableDoesHandleAllCases() {
    KafkaWritable kafkaWritable = new KafkaWritable(5,
        1000L,
        1L,
        "value".getBytes(), "key".getBytes());
    Arrays.stream(MetadataColumn.values()).forEach(kafkaWritable::getHiveWritable);
  }
}
