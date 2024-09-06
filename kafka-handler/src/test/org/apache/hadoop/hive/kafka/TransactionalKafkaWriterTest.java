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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Test Transactional Writer.
 */
@Ignore("HIVE-28348: TransactionalKafkaWriterTest hangs in precommit")
public class TransactionalKafkaWriterTest {

  private static final String TOPIC = "TOPIC_TEST";
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final KafkaBrokerResource KAFKA_BROKER_RESOURCE = new KafkaBrokerResource();

  private static final int RECORD_NUMBER = 1000;
  private static final byte[] KEY_BYTES = "key".getBytes();
  private static final List<KafkaWritable> RECORDS_WRITABLES = IntStream
      .range(0, RECORD_NUMBER)
      .mapToObj(number -> {
        final byte[] value = ("VALUE-" + Integer.toString(number)).getBytes(Charset.forName("UTF-8"));
        return new KafkaWritable(0, (long) number, value, KEY_BYTES);
      })
      .collect(Collectors.toList());

  private final Configuration configuration = new Configuration();
  private final FileSystem fs = FileSystem.getLocal(configuration);
  private final String queryId = UUID.randomUUID().toString();
  private final Map<String, String> parameters = new HashMap<>();
  private Path queryWorkingPath;
  private KafkaStorageHandler kafkaStorageHandler;
  private final Table table = Mockito.mock(Table.class);
  private KafkaConsumer<byte[], byte[]> consumer;
  private Properties properties;

  private void setupConsumer() {
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
    consumerProps.setProperty("max.poll.interval.ms", "300");
    consumerProps.setProperty("max.block.ms", "1000");
    consumerProps.setProperty("isolation.level", "read_committed");
    this.consumer = new KafkaConsumer<>(consumerProps);
  }

  public TransactionalKafkaWriterTest() throws IOException {
  }

  @BeforeClass public static void setupCluster() throws Throwable {
    KAFKA_BROKER_RESOURCE.before();
  }

  @AfterClass public static void tearDownCluster() {
    KAFKA_BROKER_RESOURCE.after();
  }

  @Before public void setup() throws IOException {
    setupConsumer();
    temporaryFolder.create();
    Path tableLocation = new Path(temporaryFolder.newFolder().toURI());
    queryWorkingPath = new Path(tableLocation, queryId);
    configuration.set(HiveConf.ConfVars.HIVE_QUERY_ID.varname, queryId);
    String taskId = "attempt_m_0001_0";
    configuration.set("mapred.task.id", taskId);
    configuration.set(KafkaTableProperties.HIVE_KAFKA_BOOTSTRAP_SERVERS.getName(), KafkaBrokerResource.BROKER_IP_PORT);
    Arrays.stream(KafkaTableProperties.values())
        .filter(kafkaTableProperties -> !kafkaTableProperties.isMandatory())
        .forEach(key -> {
          configuration.set(key.getName(), key.getDefaultValue());
          parameters.put(key.getName(), key.getDefaultValue());
        });
    parameters.put(KafkaTableProperties.WRITE_SEMANTIC_PROPERTY.getName(),
        KafkaOutputFormat.WriteSemantic.EXACTLY_ONCE.name());
    configuration.set(KafkaTableProperties.WRITE_SEMANTIC_PROPERTY.getName(),
        KafkaOutputFormat.WriteSemantic.EXACTLY_ONCE.name());
    parameters.put(KafkaTableProperties.HIVE_KAFKA_TOPIC.getName(), TOPIC);
    parameters.put(KafkaTableProperties.HIVE_KAFKA_BOOTSTRAP_SERVERS.getName(), KafkaBrokerResource.BROKER_IP_PORT);
    parameters.put(KafkaTableProperties.HIVE_KAFKA_OPTIMISTIC_COMMIT.getName(), "false");
    kafkaStorageHandler = new KafkaStorageHandler();
    Mockito.when(table.getParameters()).thenReturn(parameters);
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(tableLocation.toString());
    Mockito.when(table.getSd()).thenReturn(sd);
    kafkaStorageHandler.setConf(configuration);
    properties = KafkaUtils.producerProperties(configuration);
  }

  @After public void tearAfterTest() {
    KAFKA_BROKER_RESOURCE.deleteTopic(TOPIC);
    consumer.close(Duration.ZERO);
    consumer = null;
  }

  @Test public void writeAndCommit() throws IOException, MetaException {
    TransactionalKafkaWriter
        zombieWriter =
        new TransactionalKafkaWriter(TOPIC, properties, queryWorkingPath, fs, false);
    RECORDS_WRITABLES.forEach(kafkaRecordWritable -> {
      try {
        zombieWriter.write(kafkaRecordWritable);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    Assert.assertEquals(RECORD_NUMBER, zombieWriter.getSentRecords());

    TransactionalKafkaWriter
        writer =
        new TransactionalKafkaWriter(TOPIC, properties, queryWorkingPath, fs, false);

    RECORDS_WRITABLES.forEach(kafkaRecordWritable -> {
      try {
        writer.write(kafkaRecordWritable);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    //TEST zombie id is the same as current writer and epoch is greater
    Assert.assertEquals(writer.getProducerId(), zombieWriter.getProducerId());
    Assert.assertTrue(writer.getProducerEpoch() > zombieWriter.getProducerEpoch());

    zombieWriter.close(false);
    writer.close(false);

    kafkaStorageHandler.commitInsertTable(table, false);
    checkData();
  }

  @Test(expected = java.lang.AssertionError.class) public void writeAndNoCommit() throws IOException {
    TransactionalKafkaWriter
        writer =
        new TransactionalKafkaWriter(TOPIC, properties, queryWorkingPath, fs, false);
    RECORDS_WRITABLES.forEach(kafkaRecordWritable -> {
      try {
        writer.write(kafkaRecordWritable);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    writer.close(false);
    Assert.assertEquals(writer.getSentRecords(), RECORD_NUMBER);
    //DATA is not committed
    checkData();
  }

  @Ignore("HIVE-23400 flaky")
  @Test(expected = IOException.class) public void writerFencedOut() throws IOException {
    TransactionalKafkaWriter
        writer =
        new TransactionalKafkaWriter(TOPIC, properties, queryWorkingPath, fs, false);

    //noinspection unused this is actually used, the contstructor start the TX that is what we need
    TransactionalKafkaWriter
        newWriter =
        new TransactionalKafkaWriter(TOPIC, properties, queryWorkingPath, fs, false);

    try {
      for (KafkaWritable record : RECORDS_WRITABLES) {
        writer.write(record);
      }
    } catch (IOException e) {
      Assert.assertTrue(e.getCause() instanceof ProducerFencedException);
      throw e;
    }
  }

  private void checkData() {
    Set<TopicPartition> assignment = Collections.singleton(new TopicPartition(TOPIC, 0));
    consumer.assign(assignment);
    consumer.seekToBeginning(assignment);
    long numRecords = 0;
    boolean emptyPoll = false;
    while (numRecords < RECORD_NUMBER && !emptyPoll) {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(10000));

      Assert.assertFalse(records.records(new TopicPartition(TOPIC, 0))
          .stream()
          .anyMatch(consumerRecord -> !RECORDS_WRITABLES.contains(new KafkaWritable(0,
              consumerRecord.timestamp(),
              consumerRecord.value(),
              consumerRecord.key()))));

      emptyPoll = records.isEmpty();
      numRecords += records.count();
    }
    Assert.assertEquals(RECORD_NUMBER, numRecords);
  }
}
