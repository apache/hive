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

/**
 * Test class for kafka Writable.
 */
public class KafkaRecordWritableTest {
  public KafkaRecordWritableTest() {
  }

  @Test public void testWriteReadFields() throws IOException {
    ConsumerRecord<byte[], byte[]> record = new ConsumerRecord("topic", 0, 3L, "key".getBytes(), "value".getBytes());
    KafkaRecordWritable kafkaRecordWritable = new KafkaRecordWritable(record.partition(), record.offset(), record.timestamp(), record.value(), 0L, 100L);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream w = new DataOutputStream(baos);
    kafkaRecordWritable.write(w);
    w.flush();

    ByteArrayInputStream input = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream inputStream = new DataInputStream(input);
    KafkaRecordWritable actualKafkaRecordWritable = new KafkaRecordWritable();
    actualKafkaRecordWritable.readFields(inputStream);
    Assert.assertEquals(kafkaRecordWritable, actualKafkaRecordWritable);
  }
}
