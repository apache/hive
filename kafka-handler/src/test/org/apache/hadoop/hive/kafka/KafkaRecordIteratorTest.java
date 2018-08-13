//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

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
import org.apache.kafka.common.record.TimestampType;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class KafkaRecordIteratorTest
{
  private static final Logger log = LoggerFactory.getLogger(KafkaRecordIteratorTest.class);
  private static final String topic = "my_topic2";
  private static final List<ConsumerRecord<byte[], byte[]>> RECORDS = new ArrayList();
  private static final int RECORD_NUMBER = 100;
  private static final TopicPartition TOPIC_PARTITION = new TopicPartition("my_topic2", 0);
  public static final byte[] KEY_BYTES = "KEY".getBytes(Charset.forName("UTF-8"));
  private static ZkUtils zkUtils;
  private static ZkClient zkClient;
  private static KafkaProducer<byte[], byte[]> producer;
  private static KafkaServer kafkaServer;
  private static String zkConnect;
  private KafkaConsumer<byte[], byte[]> consumer = null;
  private KafkaRecordIterator kafkaRecordIterator = null;
  private Configuration conf = new Configuration();


  public KafkaRecordIteratorTest()
  {
  }

  @BeforeClass
  public static void setupCluster() throws IOException, InterruptedException
  {
    log.info("init embedded Zookeeper");
    EmbeddedZookeeper zkServer = new EmbeddedZookeeper();
    zkConnect = "127.0.0.1:" + zkServer.port();
    zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
    zkUtils = ZkUtils.apply(zkClient, false);
    log.info("init kafka broker");
    Properties brokerProps = new Properties();
    brokerProps.setProperty("zookeeper.connect", zkConnect);
    brokerProps.setProperty("broker.id", "0");
    brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
    brokerProps.setProperty("listeners", "PLAINTEXT://127.0.0.1:9092");
    brokerProps.setProperty("offsets.topic.replication.factor", "1");
    brokerProps.setProperty("log.retention.ms", "1000000");
    KafkaConfig config = new KafkaConfig(brokerProps);
    Time mock = new MockTime();
    kafkaServer = TestUtils.createServer(config, mock);
    log.info("Creating kafka topic [{}]", "my_topic2");
    AdminUtils.createTopic(zkUtils, topic, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
    setupProducer();
    sendData();
  }

  @Before
  public void setUp()
  {
    log.info("setting up consumer");
    this.setupConsumer();
    this.kafkaRecordIterator = null;
  }

  @Test
  public void testHasNextAbsoluteStartEnd()
  {
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, TOPIC_PARTITION, 0L, (long) RECORDS.size(), 100L);
    this.compareIterator(RECORDS, this.kafkaRecordIterator);
  }

  @Test
  public void testHasNextGivenStartEnd()
  {
    long startOffset = 2L;
    long lastOffset = 4L;
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, TOPIC_PARTITION, 2L, 4L, 100L);
    this.compareIterator((List) RECORDS.stream().filter((consumerRecord) -> {
      return consumerRecord.offset() >= 2L && consumerRecord.offset() < 4L;
    }).collect(Collectors.toList()), this.kafkaRecordIterator);
  }

  @Test
  public void testHasNextNoOffsets()
  {
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, TOPIC_PARTITION, 100L);
    this.compareIterator(RECORDS, this.kafkaRecordIterator);
  }

  @Test
  public void testHasNextLastRecord()
  {
    long startOffset = (long) (RECORDS.size() - 1);
    long lastOffset = (long) RECORDS.size();
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, TOPIC_PARTITION, startOffset, lastOffset, 100L);
    this.compareIterator((List) RECORDS.stream().filter((consumerRecord) -> {
      return consumerRecord.offset() >= startOffset && consumerRecord.offset() < lastOffset;
    }).collect(Collectors.toList()), this.kafkaRecordIterator);
  }

  @Test
  public void testHasNextFirstRecord()
  {
    long startOffset = 0L;
    long lastOffset = 1L;
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, TOPIC_PARTITION, 0L, 1L, 100L);
    this.compareIterator((List) RECORDS.stream().filter((consumerRecord) -> {
      return consumerRecord.offset() >= 0L && consumerRecord.offset() < 1L;
    }).collect(Collectors.toList()), this.kafkaRecordIterator);
  }

  @Test
  public void testHasNextNoStart()
  {
    long startOffset = 0L;
    long lastOffset = 10L;
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, TOPIC_PARTITION, (Long) null, 10L, 100L);
    this.compareIterator((List) RECORDS.stream().filter((consumerRecord) -> {
      return consumerRecord.offset() >= 0L && consumerRecord.offset() < 10L;
    }).collect(Collectors.toList()), this.kafkaRecordIterator);
  }

  @Test
  public void testHasNextNoEnd()
  {
    long startOffset = 5L;
    long lastOffset = (long) RECORDS.size();
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, TOPIC_PARTITION, 5L, (Long) null, 100L);
    this.compareIterator((List) RECORDS.stream().filter((consumerRecord) -> {
      return consumerRecord.offset() >= 5L && consumerRecord.offset() < lastOffset;
    }).collect(Collectors.toList()), this.kafkaRecordIterator);
  }

  @Test
  public void testRecordReader() throws IOException, InterruptedException
  {
    InputSplit inputSplits = new KafkaPullerInputSplit("my_topic2", 0, 0L, 50L, null);
    KafkaPullerRecordReader recordReader = new KafkaPullerRecordReader((KafkaPullerInputSplit) inputSplits, this.conf);
    List<KafkaRecordWritable> serRecords = (List) RECORDS.stream().map((recordx) -> {
      return KafkaRecordWritable.fromKafkaRecord(recordx);
    }).collect(Collectors.toList());

    for (int i = 0; i < 50; ++i) {
      KafkaRecordWritable record = new KafkaRecordWritable();
      Assert.assertTrue(recordReader.next((NullWritable) null, record));
      Assert.assertEquals(serRecords.get(i), record);
    }

    recordReader.close();
    recordReader = new KafkaPullerRecordReader();
    TaskAttemptContext context = new TaskAttemptContextImpl(this.conf, new TaskAttemptID());
    recordReader.initialize(new KafkaPullerInputSplit("my_topic2", 0, 50L, 100L, null), context);

    for (int i = 50; i < 100; ++i) {
      KafkaRecordWritable record = new KafkaRecordWritable();
      Assert.assertTrue(recordReader.next((NullWritable) null, record));
      Assert.assertEquals(serRecords.get(i), record);
    }

    recordReader.close();
  }

  @Test(
      expected = IllegalStateException.class
  )
  public void testPullingBeyondLimit()
  {
    this.kafkaRecordIterator = new KafkaRecordIterator(
        this.consumer,
        TOPIC_PARTITION,
        0L,
        (long) RECORDS.size() + 1L,
        100L
    );
    this.compareIterator(RECORDS, this.kafkaRecordIterator);
  }

  @Test(
      expected = IllegalStateException.class
  )
  public void testPullingStartGreaterThanEnd()
  {
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, TOPIC_PARTITION, 10L, 1L, 100L);
    this.compareIterator(RECORDS, this.kafkaRecordIterator);
  }

  @Test(
      expected = IllegalStateException.class
  )
  public void testPullingFromEmptyTopic()
  {
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, new TopicPartition("noHere", 0), 0L, 100L, 100L);
    this.compareIterator(RECORDS, this.kafkaRecordIterator);
  }

  @Test(
      expected = IllegalStateException.class
  )
  public void testPullingFromEmptyPartition()
  {
    this.kafkaRecordIterator = new KafkaRecordIterator(
        this.consumer,
        new TopicPartition("my_topic2", 1),
        0L,
        100L,
        100L
    );
    this.compareIterator(RECORDS, this.kafkaRecordIterator);
  }

  @Test
  public void testStartIsEqualEnd()
  {
    this.kafkaRecordIterator = new KafkaRecordIterator(this.consumer, TOPIC_PARTITION, 10L, 10L, 100L);
    this.compareIterator(ImmutableList.of(), this.kafkaRecordIterator);
  }


  @Test
  public void testStartIsTheLastOffset()
  {
    this.kafkaRecordIterator = new KafkaRecordIterator(
        this.consumer,
        TOPIC_PARTITION,
        new Long(RECORD_NUMBER),
        new Long(RECORD_NUMBER),
        100L
    );
    this.compareIterator(ImmutableList.of(), this.kafkaRecordIterator);
  }

  private void compareIterator(
      List<ConsumerRecord<byte[], byte[]>> expected,
      Iterator<ConsumerRecord<byte[], byte[]>> kafkaRecordIterator
  )
  {
    expected.stream().forEachOrdered((expectedRecord) -> {
      Assert.assertTrue("record with offset " + expectedRecord.offset(), kafkaRecordIterator.hasNext());
      ConsumerRecord record = kafkaRecordIterator.next();
      Assert.assertTrue(record.topic().equals(topic));
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

  private static void setupProducer()
  {
    log.info("Setting up kafka producer");
    Properties producerProps = new Properties();
    producerProps.setProperty("bootstrap.servers", "127.0.0.1:9092");
    producerProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProps.setProperty("max.block.ms", "10000");
    producer = new KafkaProducer(producerProps);
    log.info("kafka producer started");
  }

  private void setupConsumer()
  {
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

  private static void sendData() throws InterruptedException
  {
    log.info("Sending {} records", RECORD_NUMBER);
    RECORDS.clear();
    for (int i = 0; i < RECORD_NUMBER; ++i) {

      final byte[] value = ("VALUE-" + Integer.toString(i)).getBytes(Charset.forName("UTF-8"));
      //noinspection unchecked
      producer.send(new ProducerRecord(
          topic,
          0,
          0L,
          KEY_BYTES,
          value
      ));

      //noinspection unchecked
      RECORDS.add(new ConsumerRecord(
          topic,
          0,
          (long) i,
          0L,
          (TimestampType) null,
          0L,
          0,
          0,
          KEY_BYTES,
          value
      ));
    }

    producer.close();
  }

  @After
  public void tearDown()
  {
    this.kafkaRecordIterator = null;
    if (this.consumer != null) {
      this.consumer.close();
    }
  }

  @AfterClass
  public static void tearDownCluster()
  {
    if (kafkaServer != null) {
      kafkaServer.shutdown();
      kafkaServer.awaitShutdown();
    }

    zkClient.close();
    zkUtils.close();
  }
}
