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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KafkaStorageHandlerInfoTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaStorageHandlerInfoTest.class);

    private static final int RECORD_NUMBER = 17384;
    private static final String TOPIC = "TEST1";
    private static final byte[] KEY_BYTES = "KEY".getBytes(Charset.forName("UTF-8"));
    private static final KafkaBrokerResource KAFKA_BROKER_RESOURCE = new KafkaBrokerResource();

    private static List<ConsumerRecord<byte[], byte[]>> getRecords(String topic) {
        return IntStream.range(0, RECORD_NUMBER).mapToObj(number -> {
            final byte[] value = ("VALUE-" + Integer.toString(number)).getBytes(Charset.forName("UTF-8"));
            return new ConsumerRecord<>(topic, 0, (long) number, 0L, null, 0L, 0, 0, KEY_BYTES, value);
        }).collect(Collectors.toList());
    }

    private KafkaConsumer<byte[], byte[]> consumer;

    private Properties consumerProps;

    private static final String RESULT = "Partition(topic = TEST1, partition = 0, leader = 0, replicas = [0], isr = [0]," +
        " offlineReplicas = []) [start offset = [0], end offset = [" + RECORD_NUMBER + "]]";

    @BeforeClass
    public static void setupCluster() throws Throwable {
        KAFKA_BROKER_RESOURCE.before();
        sendData();
    }

    @AfterClass
    public static void tearDownCluster() {
        KAFKA_BROKER_RESOURCE.deleteTopic(TOPIC);
        KAFKA_BROKER_RESOURCE.after();
    }

    @Before
    public void setUp() {
        setupConsumer();
    }

    @After
    public void tearDown() {
        consumer.close();
    }

    private void setupConsumer() {
        consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBrokerResource.BROKER_IP_PORT);
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        //The configuration values are not default and are given randomly , these can be changed and tested if required
        consumerProps.setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "3002");
        consumerProps.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "3001");
        consumerProps.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "3001");
        consumerProps.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "100");
        LOG.info("setting up kafka consumer with props {}", consumerProps);
        this.consumer = new KafkaConsumer<>(consumerProps);
    }

    @Test
    public void testFormatAsText() {
        KafkaStorageHandlerInfo storageHandlerInfo = new KafkaStorageHandlerInfo(TOPIC, consumerProps);
        Assert.assertEquals(String.class, storageHandlerInfo.formatAsText().getClass());
        String text = storageHandlerInfo.formatAsText();
        Assert.assertEquals(RESULT, text);
    }

    private static void sendData() {
        List<ConsumerRecord<byte[], byte[]>> RECORDS = getRecords(TOPIC);
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBrokerResource.BROKER_IP_PORT);
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        LOG.info("Setting up kafka producer with props {}", producerProps);

        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps);
        LOG.info("Kafka producer started \n Sending [{}] records", RECORDS.size());
        RECORDS.stream()
                .map(consumerRecord -> new ProducerRecord<>(consumerRecord.topic(),
                        consumerRecord.partition(),
                        consumerRecord.timestamp(),
                        consumerRecord.key(),
                        consumerRecord.value()))
                .forEach(producer::send);
        producer.close();
    }
}
