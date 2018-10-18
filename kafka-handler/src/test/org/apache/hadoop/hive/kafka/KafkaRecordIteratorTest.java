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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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

import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Kafka Iterator Tests.
 */
@RunWith(Parameterized.class) public class KafkaRecordIteratorTest {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaRecordIteratorTest.class);
  private static final int RECORD_NUMBER = 19384;
  private static final String TOPIC = "my_test_topic";
  private static final String TX_TOPIC = "tx_test_topic";
  private static final byte[] KEY_BYTES = "KEY".getBytes(Charset.forName("UTF-8"));

  private static final KafkaBrokerResource BROKER_RESOURCE = new KafkaBrokerResource();
  private static final List<ConsumerRecord<byte[], byte[]>> RECORDS = getRecords(TOPIC);
  private static final List<ConsumerRecord<byte[], byte[]>> TX_RECORDS = getRecords(TX_TOPIC);
  private static final long POLL_TIMEOUT_MS = 900L;
  private static KafkaProducer<byte[], byte[]> producer;

  @Parameterized.Parameters public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] {{TOPIC, true, RECORDS}, {TX_TOPIC, false, TX_RECORDS}});
  }

  private static List<ConsumerRecord<byte[], byte[]>> getRecords(String topic) {
    return IntStream.range(0, RECORD_NUMBER).mapToObj(number -> {
      final byte[] value = ("VALUE-" + Integer.toString(number)).getBytes(Charset.forName("UTF-8"));
      return new ConsumerRecord<>(topic, 0, (long) number, 0L, null, 0L, 0, 0, KEY_BYTES, value);
    }).collect(Collectors.toList());
  }

  private final String currentTopic;
  private final boolean readUncommitted;
  private final List<ConsumerRecord<byte[], byte[]>> expectedRecords;
  private final TopicPartition topicPartition;
  private KafkaConsumer<byte[], byte[]> consumer = null;
  private KafkaRecordIterator kafkaRecordIterator = null;
  private final Configuration conf = new Configuration();

  public KafkaRecordIteratorTest(String currentTopic,
      boolean readUncommitted,
      List<ConsumerRecord<byte[], byte[]>> expectedRecords) {
    this.currentTopic = currentTopic;
    // when true means the the topic is not Transactional topic
    this.readUncommitted = readUncommitted;
    this.expectedRecords = expectedRecords;
    this.topicPartition = new TopicPartition(currentTopic, 0);
  }

  @BeforeClass public static void setupCluster() throws Throwable {
    BROKER_RESOURCE.before();
    sendData(RECORDS, null);
    sendData(TX_RECORDS, UUID.randomUUID().toString());
  }

  @Before public void setUp() {
    LOG.info("setting up consumer");
    setupConsumer();
    this.kafkaRecordIterator = null;
  }

  @Test public void testHasNextAbsoluteStartEnd() {
    this.kafkaRecordIterator =
        new KafkaRecordIterator(this.consumer, topicPartition, 0L, (long) expectedRecords.size(), POLL_TIMEOUT_MS);
    this.compareIterator(expectedRecords, this.kafkaRecordIterator);
  }

  @Test public void testHasNextGivenStartEnd() {
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, topicPartition, 2L, 4L, POLL_TIMEOUT_MS);
    this.compareIterator(expectedRecords.stream()
        .filter((consumerRecord) -> consumerRecord.offset() >= 2L && consumerRecord.offset() < 4L)
        .collect(Collectors.toList()), this.kafkaRecordIterator);
  }

  @Test public void testHasNextNoOffsets() {
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, topicPartition, POLL_TIMEOUT_MS);
    this.compareIterator(expectedRecords, this.kafkaRecordIterator);
  }

  @Test public void testHasNextLastRecord() {
    long startOffset = (long) (expectedRecords.size() - 1);
    long lastOffset = (long) expectedRecords.size();
    this.kafkaRecordIterator =
        new KafkaRecordIterator(this.consumer, topicPartition, startOffset, lastOffset, POLL_TIMEOUT_MS);
    this.compareIterator(expectedRecords.stream()
        .filter((consumerRecord) -> consumerRecord.offset() >= startOffset && consumerRecord.offset() < lastOffset)
        .collect(Collectors.toList()), this.kafkaRecordIterator);
  }

  @Test public void testHasNextFirstRecord() {
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, topicPartition, 0L, 1L, POLL_TIMEOUT_MS);
    this.compareIterator(expectedRecords.stream()
        .filter((consumerRecord) -> consumerRecord.offset() >= 0L && consumerRecord.offset() < 1L)
        .collect(Collectors.toList()), this.kafkaRecordIterator);
  }

  @Test public void testHasNextNoStart() {
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, topicPartition, null, 10L, POLL_TIMEOUT_MS);
    this.compareIterator(expectedRecords.stream()
        .filter((consumerRecord) -> consumerRecord.offset() >= 0L && consumerRecord.offset() < 10L)
        .collect(Collectors.toList()), this.kafkaRecordIterator);
  }

  @Test public void testHasNextNoEnd() {
    long lastOffset = (long) expectedRecords.size();
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, topicPartition, 5L, null, POLL_TIMEOUT_MS);
    this.compareIterator(expectedRecords.stream()
        .filter((consumerRecord) -> consumerRecord.offset() >= 5L && consumerRecord.offset() < lastOffset)
        .collect(Collectors.toList()), this.kafkaRecordIterator);
  }

  @Test public void testRecordReader() {
    List<KafkaWritable>
        serRecords =
        expectedRecords.stream()
            .map((consumerRecord) -> new KafkaWritable(consumerRecord.partition(),
                consumerRecord.offset(),
                consumerRecord.timestamp(),
                consumerRecord.value(),
                50L,
                100L,
                consumerRecord.key()))
            .collect(Collectors.toList());
    KafkaRecordReader recordReader = new KafkaRecordReader();
    TaskAttemptContext context = new TaskAttemptContextImpl(this.conf, new TaskAttemptID());
    recordReader.initialize(new KafkaInputSplit(currentTopic, 0, 50L, 100L, null), context);

    for (int i = 50; i < 100; ++i) {
      KafkaWritable record = new KafkaWritable();
      Assert.assertTrue(recordReader.next(null, record));
      Assert.assertEquals(serRecords.get(i), record);
    }

    recordReader.close();
  }

  @Test(expected = KafkaRecordIterator.PollTimeoutException.class) public void testPullingBeyondLimit() {
    //FYI In the Tx world Commits can introduce offset gaps therefore
    //thus(RECORD_NUMBER + 1) as beyond limit offset is only true if the topic has not Tx or any Control msg.
    long increment = readUncommitted ? 1 : 2;
    long requestedEnd = expectedRecords.size() + increment;
    long requestedStart = expectedRecords.size() - 1;
    this.kafkaRecordIterator =
        new KafkaRecordIterator(this.consumer, topicPartition, requestedStart, requestedEnd, POLL_TIMEOUT_MS);
    this.compareIterator(expectedRecords.stream()
        .filter((consumerRecord) -> consumerRecord.offset() >= requestedStart)
        .collect(Collectors.toList()), this.kafkaRecordIterator);
  }

  @Test(expected = IllegalStateException.class) public void testPullingStartGreaterThanEnd() {
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, topicPartition, 10L, 1L, POLL_TIMEOUT_MS);
    this.compareIterator(expectedRecords, this.kafkaRecordIterator);
  }

  @Test(expected = KafkaRecordIterator.PollTimeoutException.class) public void testPullingFromEmptyTopic() {
    this.kafkaRecordIterator =
        new KafkaRecordIterator(this.consumer, new TopicPartition("noHere", 0), 0L, 100L, POLL_TIMEOUT_MS);
    this.compareIterator(expectedRecords, this.kafkaRecordIterator);
  }

  @Test(expected = KafkaRecordIterator.PollTimeoutException.class) public void testPullingFromEmptyPartition() {
    this.kafkaRecordIterator =
        new KafkaRecordIterator(this.consumer, new TopicPartition(currentTopic, 1), 0L, 100L, POLL_TIMEOUT_MS);
    this.compareIterator(expectedRecords, this.kafkaRecordIterator);
  }

  @Test public void testStartIsEqualEnd() {
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, topicPartition, 10L, 10L, POLL_TIMEOUT_MS);
    this.compareIterator(ImmutableList.of(), this.kafkaRecordIterator);
  }

  @Test public void testStartIsTheLastOffset() {
    this.kafkaRecordIterator =
        new KafkaRecordIterator(this.consumer,
            topicPartition,
            (long) expectedRecords.size(),
            (long) expectedRecords.size(),
            POLL_TIMEOUT_MS);
    this.compareIterator(ImmutableList.of(), this.kafkaRecordIterator);
  }

  @Test public void testStartIsTheFirstOffset() {
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, topicPartition, 0L, 0L, POLL_TIMEOUT_MS);
    this.compareIterator(ImmutableList.of(), this.kafkaRecordIterator);
  }

  private void compareIterator(List<ConsumerRecord<byte[], byte[]>> expected,
      Iterator<ConsumerRecord<byte[], byte[]>> kafkaRecordIterator) {
    expected.forEach((expectedRecord) -> {
      Assert.assertTrue("Record with offset is missing" + expectedRecord.offset(), kafkaRecordIterator.hasNext());
      ConsumerRecord record = kafkaRecordIterator.next();
      Assert.assertEquals(expectedRecord.topic(), record.topic());
      Assert.assertEquals(expectedRecord.partition(), record.partition());
      Assert.assertEquals("Offsets not matching", expectedRecord.offset(), record.offset());
      byte[] binaryExceptedValue = expectedRecord.value();
      byte[] binaryExceptedKey = expectedRecord.key();
      byte[] binaryValue = (byte[]) record.value();
      byte[] binaryKey = (byte[]) record.key();
      Assert.assertArrayEquals("Values not matching", binaryExceptedValue, binaryValue);
      Assert.assertArrayEquals("Keys not matching", binaryExceptedKey, binaryKey);
    });
    Assert.assertFalse(kafkaRecordIterator.hasNext());
  }

  private void setupConsumer() {
    Properties consumerProps = new Properties();
    consumerProps.setProperty("enable.auto.commit", "false");
    consumerProps.setProperty("auto.offset.reset", "none");
    consumerProps.setProperty("bootstrap.servers", KafkaBrokerResource.BROKER_IP_PORT);
    conf.set("kafka.bootstrap.servers", KafkaBrokerResource.BROKER_IP_PORT);
    conf.set(KafkaTableProperties.KAFKA_POLL_TIMEOUT.getName(),
        KafkaTableProperties.KAFKA_POLL_TIMEOUT.getDefaultValue());

    consumerProps.setProperty("key.deserializer", ByteArrayDeserializer.class.getName());
    consumerProps.setProperty("value.deserializer", ByteArrayDeserializer.class.getName());
    consumerProps.setProperty("request.timeout.ms", "3002");
    consumerProps.setProperty("fetch.max.wait.ms", "3001");
    consumerProps.setProperty("session.timeout.ms", "3001");
    consumerProps.setProperty("metadata.max.age.ms", "100");
    if (!readUncommitted) {
      consumerProps.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
      conf.set("kafka.consumer.isolation.level", "read_committed");
    }
    consumerProps.setProperty("max.poll.records", String.valueOf(RECORD_NUMBER - 1));
    this.consumer = new KafkaConsumer<>(consumerProps);
  }

  private static void sendData(List<ConsumerRecord<byte[], byte[]>> recordList, @Nullable String txId) {
    LOG.info("Setting up kafka producer");
    Properties producerProps = new Properties();
    producerProps.setProperty("bootstrap.servers", KafkaBrokerResource.BROKER_IP_PORT);
    producerProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProps.setProperty("max.block.ms", "10000");
    if (txId != null) {
      producerProps.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txId);
    }
    producer = new KafkaProducer<>(producerProps);
    LOG.info("kafka producer started");
    LOG.info("Sending [{}] records", RECORDS.size());
    if (txId != null) {
      producer.initTransactions();
      producer.beginTransaction();
    }
    recordList.stream()
        .map(consumerRecord -> new ProducerRecord<>(consumerRecord.topic(),
            consumerRecord.partition(),
            consumerRecord.timestamp(),
            consumerRecord.key(),
            consumerRecord.value()))
        .forEach(producerRecord -> producer.send(producerRecord));
    if (txId != null) {
      producer.commitTransaction();
    }
    producer.close();
  }

  @After public void tearDown() {
    this.kafkaRecordIterator = null;
    if (this.consumer != null) {
      this.consumer.close();
    }
  }

  @AfterClass public static void tearDownCluster() {
    BROKER_RESOURCE.after();
  }
}
