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
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Time;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Kafka Iterator Tests.
 */
public class KafkaRecordIteratorTest {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaRecordIteratorTest.class);
  private static final int RECORD_NUMBER = 100;
  private static final String TOPIC = "my_test_topic";
  private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, 0);
  public static final byte[] KEY_BYTES = "KEY".getBytes(Charset.forName("UTF-8"));
  private static final List<ConsumerRecord<byte[], byte[]>>
      RECORDS =
      IntStream.range(0, RECORD_NUMBER).mapToObj(number -> {
        final byte[] value = ("VALUE-" + Integer.toString(number)).getBytes(Charset.forName("UTF-8"));
        return new ConsumerRecord<>(TOPIC, 0, (long) number, 0L, null, 0L, 0, 0, KEY_BYTES, value);
      }).collect(Collectors.toList());
  public static final long POLL_TIMEOUT_MS = 900L;
  private static ZkUtils zkUtils;
  private static ZkClient zkClient;
  private static KafkaProducer<byte[], byte[]> producer;
  private static KafkaServer kafkaServer;
  private static String zkConnect;
  private KafkaConsumer<byte[], byte[]> consumer = null;
  private KafkaRecordIterator kafkaRecordIterator = null;
  private Configuration conf = new Configuration();
  private static EmbeddedZookeeper zkServer;

  public KafkaRecordIteratorTest() {
  }

  @BeforeClass public static void setupCluster() throws IOException {
    LOG.info("init embedded Zookeeper");
    zkServer = new EmbeddedZookeeper();
    zkConnect = "127.0.0.1:" + zkServer.port();
    zkClient = new ZkClient(zkConnect, 3000, 3000, ZKStringSerializer$.MODULE$);
    zkUtils = ZkUtils.apply(zkClient, false);
    LOG.info("init kafka broker");
    Properties brokerProps = new Properties();
    brokerProps.setProperty("zookeeper.connect", zkConnect);
    brokerProps.setProperty("broker.id", "0");
    brokerProps.setProperty("log.dir", Files.createTempDirectory("kafka-log-dir-").toAbsolutePath().toString());
    brokerProps.setProperty("listeners", "PLAINTEXT://127.0.0.1:9092");
    brokerProps.setProperty("offsets.TOPIC.replication.factor", "1");
    KafkaConfig config = new KafkaConfig(brokerProps);
    Time mock = new MockTime();
    kafkaServer = TestUtils.createServer(config, mock);
    kafkaServer.startup();
    LOG.info("Creating kafka TOPIC [{}]", TOPIC);
    AdminUtils.createTopic(zkUtils, TOPIC, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
    setupProducer();
    sendData();
  }

  @Before public void setUp() {
    LOG.info("setting up consumer");
    this.setupConsumer();
    this.kafkaRecordIterator = null;
  }

  @Test public void testHasNextAbsoluteStartEnd() {
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, TOPIC_PARTITION, 0L, (long) RECORDS.size(), POLL_TIMEOUT_MS);
    this.compareIterator(RECORDS, this.kafkaRecordIterator);
  }

  @Test public void testHasNextGivenStartEnd() {
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, TOPIC_PARTITION, 2L, 4L, POLL_TIMEOUT_MS);
    this.compareIterator(RECORDS.stream()
        .filter((consumerRecord) -> consumerRecord.offset() >= 2L && consumerRecord.offset() < 4L)
        .collect(Collectors.toList()), this.kafkaRecordIterator);
  }

  @Test public void testHasNextNoOffsets() {
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, TOPIC_PARTITION, POLL_TIMEOUT_MS);
    this.compareIterator(RECORDS, this.kafkaRecordIterator);
  }

  @Test public void testHasNextLastRecord() {
    long startOffset = (long) (RECORDS.size() - 1);
    long lastOffset = (long) RECORDS.size();
    this.kafkaRecordIterator =
        new KafkaRecordIterator(this.consumer, TOPIC_PARTITION, startOffset, lastOffset, POLL_TIMEOUT_MS);
    this.compareIterator(RECORDS.stream()
        .filter((consumerRecord) -> consumerRecord.offset() >= startOffset && consumerRecord.offset() < lastOffset)
        .collect(Collectors.toList()), this.kafkaRecordIterator);
  }

  @Test public void testHasNextFirstRecord() {
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, TOPIC_PARTITION, 0L, 1L, POLL_TIMEOUT_MS);
    this.compareIterator(RECORDS.stream()
        .filter((consumerRecord) -> consumerRecord.offset() >= 0L && consumerRecord.offset() < 1L)
        .collect(Collectors.toList()), this.kafkaRecordIterator);
  }

  @Test public void testHasNextNoStart() {
    this.kafkaRecordIterator =
        new KafkaRecordIterator(this.consumer, TOPIC_PARTITION, null, 10L, POLL_TIMEOUT_MS);
    this.compareIterator(RECORDS.stream()
        .filter((consumerRecord) -> consumerRecord.offset() >= 0L && consumerRecord.offset() < 10L)
        .collect(Collectors.toList()), this.kafkaRecordIterator);
  }

  @Test public void testHasNextNoEnd() {
    long lastOffset = (long) RECORDS.size();
    this.kafkaRecordIterator =
        new KafkaRecordIterator(this.consumer, TOPIC_PARTITION, 5L, null, POLL_TIMEOUT_MS);
    this.compareIterator(RECORDS.stream()
        .filter((consumerRecord) -> consumerRecord.offset() >= 5L && consumerRecord.offset() < lastOffset)
        .collect(Collectors.toList()), this.kafkaRecordIterator);
  }

  @Test public void testRecordReader() throws IOException {
    List<KafkaRecordWritable>
        serRecords =
        RECORDS.stream()
            .map((aRecord) -> new KafkaRecordWritable(aRecord.partition(),
                aRecord.offset(),
                aRecord.timestamp(),
                aRecord.value(),
                50L,
                100L))
            .collect(Collectors.toList());
    KafkaPullerRecordReader recordReader = new KafkaPullerRecordReader();
    TaskAttemptContext context = new TaskAttemptContextImpl(this.conf, new TaskAttemptID());
    recordReader.initialize(new KafkaPullerInputSplit(TOPIC, 0, 50L, 100L, null), context);

    for (int i = 50; i < 100; ++i) {
      KafkaRecordWritable record = new KafkaRecordWritable();
      Assert.assertTrue(recordReader.next(null, record));
      Assert.assertEquals(serRecords.get(i), record);
    }

    recordReader.close();
  }

  @Test(expected = TimeoutException.class) public void testPullingBeyondLimit() {
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, TOPIC_PARTITION, 0L, 101L, POLL_TIMEOUT_MS);
    this.compareIterator(RECORDS, this.kafkaRecordIterator);
  }

  @Test(expected = IllegalStateException.class) public void testPullingStartGreaterThanEnd() {
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, TOPIC_PARTITION, 10L, 1L, POLL_TIMEOUT_MS);
    this.compareIterator(RECORDS, this.kafkaRecordIterator);
  }

  @Test(expected = TimeoutException.class) public void testPullingFromEmptyTopic() {
    this.kafkaRecordIterator =
        new KafkaRecordIterator(this.consumer, new TopicPartition("noHere", 0), 0L, 100L, POLL_TIMEOUT_MS);
    this.compareIterator(RECORDS, this.kafkaRecordIterator);
  }

  @Test(expected = TimeoutException.class) public void testPullingFromEmptyPartition() {
    this.kafkaRecordIterator =
        new KafkaRecordIterator(this.consumer, new TopicPartition(TOPIC, 1), 0L, 100L, POLL_TIMEOUT_MS);
    this.compareIterator(RECORDS, this.kafkaRecordIterator);
  }

  @Test public void testStartIsEqualEnd() {
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, TOPIC_PARTITION, 10L, 10L, POLL_TIMEOUT_MS);
    this.compareIterator(ImmutableList.of(), this.kafkaRecordIterator);
  }

  @Test public void testStartIsTheLastOffset() {
    this.kafkaRecordIterator =
        new KafkaRecordIterator(this.consumer,
            TOPIC_PARTITION,
            new Long(RECORD_NUMBER),
            new Long(RECORD_NUMBER),
            POLL_TIMEOUT_MS);
    this.compareIterator(ImmutableList.of(), this.kafkaRecordIterator);
  }

  @Test public void testStartIsTheFirstOffset() {
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, TOPIC_PARTITION, 0L, 0L, POLL_TIMEOUT_MS);
    this.compareIterator(ImmutableList.of(), this.kafkaRecordIterator);
  }

  private void compareIterator(List<ConsumerRecord<byte[], byte[]>> expected,
      Iterator<ConsumerRecord<byte[], byte[]>> kafkaRecordIterator) {
    expected.stream().forEachOrdered((expectedRecord) -> {
      Assert.assertTrue("record with offset " + expectedRecord.offset(), kafkaRecordIterator.hasNext());
      ConsumerRecord record = kafkaRecordIterator.next();
      Assert.assertTrue(record.topic().equals(TOPIC));
      Assert.assertTrue(record.partition() == 0);
      Assert.assertEquals("Offsets not matching", expectedRecord.offset(), record.offset());
      byte[] binaryExceptedValue = expectedRecord.value();
      byte[] binaryExceptedKey = expectedRecord.key();
      byte[] binaryValue = (byte[]) record.value();
      byte[] binaryKey = (byte[]) record.key();
      Assert.assertArrayEquals(binaryExceptedValue, binaryValue);
      Assert.assertArrayEquals(binaryExceptedKey, binaryKey);
    });
    Assert.assertFalse(kafkaRecordIterator.hasNext());
  }

  private static void setupProducer() {
    LOG.info("Setting up kafka producer");
    Properties producerProps = new Properties();
    producerProps.setProperty("bootstrap.servers", "127.0.0.1:9092");
    producerProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProps.setProperty("max.block.ms", "10000");
    producer = new KafkaProducer(producerProps);
    LOG.info("kafka producer started");
  }

  private void setupConsumer() {
    Properties consumerProps = new Properties();
    consumerProps.setProperty("enable.auto.commit", "false");
    consumerProps.setProperty("auto.offset.reset", "none");
    consumerProps.setProperty("bootstrap.servers", "127.0.0.1:9092");
    this.conf.set("kafka.bootstrap.servers", "127.0.0.1:9092");
    consumerProps.setProperty("key.deserializer", ByteArrayDeserializer.class.getName());
    consumerProps.setProperty("value.deserializer", ByteArrayDeserializer.class.getName());
    consumerProps.setProperty("request.timeout.ms", "3002");
    consumerProps.setProperty("fetch.max.wait.ms", "3001");
    consumerProps.setProperty("session.timeout.ms", "3001");
    consumerProps.setProperty("metadata.max.age.ms", "100");
    this.consumer = new KafkaConsumer(consumerProps);
  }

  private static void sendData() {
    LOG.info("Sending {} records", RECORD_NUMBER);
    RECORDS.stream()
        .map(consumerRecord -> new ProducerRecord(consumerRecord.topic(),
            consumerRecord.partition(),
            consumerRecord.timestamp(),
            consumerRecord.key(),
            consumerRecord.value()))
        .forEach(producerRecord -> producer.send(producerRecord));
    producer.close();
  }

  @After public void tearDown() {
    this.kafkaRecordIterator = null;
    if (this.consumer != null) {
      this.consumer.close();
    }
  }

  @AfterClass public static void tearDownCluster() {
    if (kafkaServer != null) {
      kafkaServer.shutdown();
      kafkaServer.zkUtils().close();
      kafkaServer.awaitShutdown();
    }
    zkServer.shutdown();
    zkClient.close();
    zkUtils.close();
  }
}
