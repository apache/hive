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

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Test class for Hive Kafka Producer.
 */
@SuppressWarnings("unchecked") public class HiveKafkaProducerTest {

  private static final Logger LOG = LoggerFactory.getLogger(HiveKafkaProducerTest.class);
  private static final int RECORD_NUMBER = 17384;
  private static final byte[] KEY_BYTES = "KEY".getBytes(Charset.forName("UTF-8"));
  private static final KafkaBrokerResource KAFKA_BROKER_RESOURCE = new KafkaBrokerResource();

  private static final String TOPIC = "test-tx-producer";
  private static final List<ProducerRecord<byte[], byte[]>>
      RECORDS =
      IntStream.range(0, RECORD_NUMBER).mapToObj(number -> {
        final byte[] value = ("VALUE-" + Integer.toString(number)).getBytes(Charset.forName("UTF-8"));
        return new ProducerRecord<>(TOPIC, value, KEY_BYTES);
      }).collect(Collectors.toList());

  @BeforeClass public static void setupCluster() throws Throwable {
    KAFKA_BROKER_RESOURCE.before();
  }

  @AfterClass public static void tearDownCluster() {
    KAFKA_BROKER_RESOURCE.after();
  }

  private KafkaConsumer<byte[], byte[]> consumer;
  private Properties producerProperties;
  private HiveKafkaProducer producer;

  @Before public void setUp() {
    LOG.info("setting up Config");
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
    consumerProps.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    this.consumer = new KafkaConsumer<>(consumerProps);

    String txId = UUID.randomUUID().toString();
    producerProperties = new Properties();
    producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBrokerResource.BROKER_IP_PORT);
    producerProperties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txId);
    producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producer = new HiveKafkaProducer(producerProperties);
  }

  @After public void tearDown() {
    LOG.info("tearDown");
    consumer.close();
    consumer = null;
  }

  @Test public void resumeTransaction() {
    producer.initTransactions();
    producer.beginTransaction();
    long pid = producer.getProducerId();
    short epoch = producer.getEpoch();
    Assert.assertTrue(pid > -1);
    Assert.assertTrue(epoch > -1);
    //noinspection unchecked
    RECORDS.forEach(producer::send);
    producer.flush();
    producer.close();

    HiveKafkaProducer secondProducer = new HiveKafkaProducer(producerProperties);
    secondProducer.resumeTransaction(pid, epoch);
    secondProducer.sendOffsetsToTransaction(ImmutableMap.of(), "__dummy_consumer_group");
    secondProducer.commitTransaction();
    secondProducer.close();

    Collection<TopicPartition> assignment = Collections.singletonList(new TopicPartition(TOPIC, 0));
    consumer.assign(assignment);
    consumer.seekToBeginning(assignment);
    long numRecords = 0;
    @SuppressWarnings("unchecked") final List<ConsumerRecord<byte[], byte[]>> actualRecords = new ArrayList();
    while (numRecords < RECORD_NUMBER) {
      ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofMillis(1000));
      actualRecords.addAll(consumerRecords.records(new TopicPartition(TOPIC, 0)));
      numRecords += consumerRecords.count();
    }
    Assert.assertEquals("Size matters !!", RECORDS.size(), actualRecords.size());
    Iterator<ProducerRecord<byte[], byte[]>> expectedIt = RECORDS.iterator();
    Iterator<ConsumerRecord<byte[], byte[]>> actualIt = actualRecords.iterator();
    while (expectedIt.hasNext()) {
      ProducerRecord<byte[], byte[]> expected = expectedIt.next();
      ConsumerRecord<byte[], byte[]> actual = actualIt.next();
      Assert.assertArrayEquals("value not matching", expected.value(), actual.value());
      Assert.assertArrayEquals("key not matching", expected.key(), actual.key());
    }
  }

  @Test(expected = org.apache.kafka.common.KafkaException.class) public void testWrongEpochAndId() {
    HiveKafkaProducer secondProducer = new HiveKafkaProducer(producerProperties);
    secondProducer.resumeTransaction(3434L, (short) 12);
    secondProducer.sendOffsetsToTransaction(ImmutableMap.of(), "__dummy_consumer_group");
    secondProducer.close();
  }

  @Test(expected = org.apache.kafka.common.KafkaException.class) public void testWrongEpoch() {
    producer.initTransactions();
    producer.beginTransaction();
    long pid = producer.getProducerId();
    producer.close();
    HiveKafkaProducer secondProducer = new HiveKafkaProducer(producerProperties);
    secondProducer.resumeTransaction(pid, (short) 12);
    secondProducer.sendOffsetsToTransaction(ImmutableMap.of(), "__dummy_consumer_group");
    secondProducer.close();
  }

  @Test(expected = org.apache.kafka.common.KafkaException.class) public void testWrongPID() {
    producer.initTransactions();
    producer.beginTransaction();
    short epoch = producer.getEpoch();
    producer.close();
    HiveKafkaProducer secondProducer = new HiveKafkaProducer(producerProperties);
    secondProducer.resumeTransaction(45L, epoch);
    secondProducer.sendOffsetsToTransaction(ImmutableMap.of(), "__dummy_consumer_group");
    secondProducer.close();
  }
}
