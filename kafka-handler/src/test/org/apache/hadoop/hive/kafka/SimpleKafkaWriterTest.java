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

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Test class for Kafka simple writer.
 */
@RunWith(Parameterized.class) public class SimpleKafkaWriterTest {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleKafkaWriterTest.class);

  private static final int RECORD_NUMBER = 17384;
  private static final byte[] KEY_BYTES = "KEY".getBytes(Charset.forName("UTF-8"));
  private static final KafkaBrokerResource KAFKA_BROKER_RESOURCE = new KafkaBrokerResource();
  private static final List<KafkaWritable> RECORDS_WRITABLES = IntStream
      .range(0, RECORD_NUMBER)
      .mapToObj(number -> {
        final byte[] value = ("VALUE-" + Integer.toString(number)).getBytes(Charset.forName("UTF-8"));
        return new KafkaWritable(0, (long) number, value, KEY_BYTES);
      }).collect(Collectors.toList());
  private final Configuration conf = new Configuration();
  private final KafkaOutputFormat.WriteSemantic writeSemantic;
  private KafkaConsumer<byte[], byte[]> consumer;

  public SimpleKafkaWriterTest(KafkaOutputFormat.WriteSemantic writeSemantic) {
    this.writeSemantic = writeSemantic;
  }

  @Parameterized.Parameters public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] {{KafkaOutputFormat.WriteSemantic.AT_LEAST_ONCE}});
  }

  @BeforeClass public static void setupCluster() throws Throwable {
    KAFKA_BROKER_RESOURCE.before();
  }

  @AfterClass public static void tearDownCluster() {
    KAFKA_BROKER_RESOURCE.after();
  }

  @Before public void setUp() {
    LOG.info("setting up Config");
    setupConsumer();
  }

  @After public void tearDown() {
    consumer.close();
    LOG.info("tearDown");
  }

  private void setupConsumer() {
    this.conf.set("kafka.bootstrap.servers", KafkaBrokerResource.BROKER_IP_PORT);
    Properties consumerProps = new Properties();
    consumerProps.setProperty("enable.auto.commit", "false");
    consumerProps.setProperty("auto.offset.reset", "none");
    consumerProps.setProperty("bootstrap.servers", KafkaBrokerResource.BROKER_IP_PORT);
    consumerProps.setProperty("key.deserializer", ByteArrayDeserializer.class.getName());
    consumerProps.setProperty("value.deserializer", ByteArrayDeserializer.class.getName());
    consumerProps.setProperty("request.timeout.ms", "3002");
    consumerProps.setProperty("fetch.max.wait.ms", "3001");
    consumerProps.setProperty("session.timeout.ms", "3001");
    consumerProps.setProperty("metadata.max.age.ms", "100");
    this.consumer = new KafkaConsumer<>(consumerProps);
  }

  @Test(expected = IllegalStateException.class) public void testMissingBrokerString() {
    new SimpleKafkaWriter("t",
        null, new Properties());
  }

  @Test public void testCheckWriterId() {
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9290");
    SimpleKafkaWriter
        writer =
        new SimpleKafkaWriter("t",
            null, properties);
    Assert.assertNotNull(writer.getWriterId());
  }

  @Test public void testSendToNoWhere() throws IOException {
    String notValidBroker = "localhost:6090";
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, notValidBroker);
    properties.setProperty("request.timeout.ms", "100");
    properties.setProperty("metadata.max.age.ms", "100");
    properties.setProperty("max.block.ms", "1000");
    KafkaWritable record = new KafkaWritable(-1, -1, "value".getBytes(), null);
    SimpleKafkaWriter writer = new SimpleKafkaWriter("t", null, properties);
    Exception exception = null;
    try {
      writer.write(record);
      writer.close(false);
    } catch (IOException e) {
      exception = e;
    }
    Assert.assertNotNull("Must rethrow exception", exception);
    Assert.assertEquals("Expect sent records not matching", 1, writer.getSentRecords());
    Assert.assertEquals("Expect lost records is not matching", 1, writer.getLostRecords());
  }

  @Test public void testSend() throws IOException {
    String topic = UUID.randomUUID().toString();
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBrokerResource.BROKER_IP_PORT);
    SimpleKafkaWriter
        writer =
        new SimpleKafkaWriter(topic,
            null, properties);
    RECORDS_WRITABLES.forEach(kafkaRecordWritable -> {
      try {
        writer.write(kafkaRecordWritable);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    writer.close(false);
    Assert.assertEquals(RECORD_NUMBER, writer.getSentRecords());
    Assert.assertEquals(0, writer.getLostRecords());
    Set<TopicPartition> assignment = Collections.singleton(new TopicPartition(topic, 0));
    consumer.assign(assignment);
    consumer.seekToBeginning(assignment);
    long numRecords = 0;
    while (numRecords < RECORD_NUMBER) {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(1000));

      Assert.assertFalse(records.records(new TopicPartition(topic, 0))
          .stream()
          .anyMatch(consumerRecord -> !RECORDS_WRITABLES.contains(new KafkaWritable(0,
              consumerRecord.timestamp(),
              consumerRecord.value(),
              consumerRecord.key()))));

      numRecords += records.count();
    }
    Assert.assertEquals(RECORD_NUMBER, numRecords);
  }
}
