/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.kafka;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

public class KafkaPullerRecordReader extends RecordReader<NullWritable, KafkaRecordWritable>
    implements org.apache.hadoop.mapred.RecordReader<NullWritable, KafkaRecordWritable> {

  private static final Logger log = LoggerFactory.getLogger(KafkaPullerRecordReader.class);
  public static final String HIVE_KAFKA_POLL_TIMEOUT = "hive.kafka.poll.timeout.ms";
  public static final long DEFAULT_CONSUMER_POLL_TIMEOUT_MS = 5000l;

  private final Closer closer = Closer.create();
  private KafkaConsumer<byte[], byte[]> consumer = null;
  private Configuration config = null;
  private KafkaRecordWritable currentWritableValue;
  private Iterator<ConsumerRecord<byte[], byte[]>> recordsCursor = null;

  private TopicPartition topicPartition;
  private long startOffset;
  private long endOffset;

  private long totalNumberRecords = 0l;
  private long consumedRecords = 0l;
  private long readBytes = 0l;
  private long pollTimeout;
  private volatile boolean started = false;

  public KafkaPullerRecordReader() {
  }

  private void initConsumer() {
    if (consumer == null) {
      log.info("Initializing Kafka Consumer");
      final Properties properties = KafkaStreamingUtils.consumerProperties(config);
      String brokerString = properties.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
      Preconditions.checkNotNull(brokerString, "broker end point can not be null");
      log.info("Starting Consumer with Kafka broker string [{}]", brokerString);
      consumer = new KafkaConsumer(properties);
      closer.register(consumer);
    }
  }

  public KafkaPullerRecordReader(KafkaPullerInputSplit inputSplit, Configuration jobConf) {
    initialize(inputSplit, jobConf);
  }

  synchronized private void initialize(KafkaPullerInputSplit inputSplit, Configuration jobConf) {
    if (!started) {
      this.config = jobConf;
      computeTopicPartitionOffsets(inputSplit);
      initConsumer();
      pollTimeout = config.getLong(HIVE_KAFKA_POLL_TIMEOUT, DEFAULT_CONSUMER_POLL_TIMEOUT_MS);
      log.debug("Consumer poll timeout [{}] ms", pollTimeout);
      recordsCursor = new KafkaRecordIterator(consumer, topicPartition, startOffset, endOffset, pollTimeout);
      started = true;
    }
  }

  private void computeTopicPartitionOffsets(KafkaPullerInputSplit split) {
    String topic = split.getTopic();
    int partition = split.getPartition();
    startOffset = split.getStartOffset();
    endOffset = split.getEndOffset();
    topicPartition = new TopicPartition(topic, partition);
    Preconditions.checkState(
        startOffset >= 0 && startOffset <= endOffset,
        "Start [%s] has to be positive and less or equal than End [%s]",
        startOffset,
        endOffset
    );
    totalNumberRecords += endOffset - startOffset;
  }

  @Override synchronized public void initialize(org.apache.hadoop.mapreduce.InputSplit inputSplit, TaskAttemptContext context) {
    initialize((KafkaPullerInputSplit) inputSplit, context.getConfiguration());
  }

  @Override public boolean next(NullWritable nullWritable, KafkaRecordWritable bytesWritable) {
    if (started && recordsCursor.hasNext()) {
      ConsumerRecord<byte[], byte[]> record = recordsCursor.next();
      bytesWritable.set(record);
      consumedRecords += 1;
      readBytes += record.serializedValueSize();
      return true;
    }
    return false;
  }

  @Override public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override public KafkaRecordWritable createValue() {
    return new KafkaRecordWritable();
  }

  @Override public long getPos() throws IOException {

    return consumedRecords;
  }

  @Override public boolean nextKeyValue() throws IOException {
    currentWritableValue = new KafkaRecordWritable();
    if (next(NullWritable.get(), currentWritableValue)) {
      return true;
    }
    currentWritableValue = null;
    return false;
  }

  @Override public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override public KafkaRecordWritable getCurrentValue() throws IOException, InterruptedException {
    return Preconditions.checkNotNull(currentWritableValue);
  }

  @Override public float getProgress() throws IOException {
    if (consumedRecords == 0) {
      return 0f;
    }
    if (consumedRecords >= totalNumberRecords) {
      return 1f;
    }
    return consumedRecords * 1.0f / totalNumberRecords;
  }

  @Override public void close() throws IOException {
    if (!started) {
      return;
    }
    log.trace("total read bytes [{}]", readBytes);
    if (consumer != null) {
      consumer.wakeup();
    }
    closer.close();
  }
}
