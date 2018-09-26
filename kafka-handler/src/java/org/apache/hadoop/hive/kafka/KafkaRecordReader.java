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

import com.google.common.base.Preconditions;
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

import java.util.Iterator;
import java.util.Properties;

/**
 * Kafka Records Reader implementation.
 */
@SuppressWarnings("WeakerAccess") public class KafkaRecordReader extends RecordReader<NullWritable, KafkaWritable>
    implements org.apache.hadoop.mapred.RecordReader<NullWritable, KafkaWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaRecordReader.class);

  private KafkaConsumer<byte[], byte[]> consumer = null;
  private Configuration config = null;
  private KafkaWritable currentWritableValue;
  private Iterator<ConsumerRecord<byte[], byte[]>> recordsCursor = null;

  private long totalNumberRecords = 0L;
  private long consumedRecords = 0L;
  private long readBytes = 0L;
  private volatile boolean started = false;
  private long startOffset = -1L;
  private long endOffset = Long.MAX_VALUE;

  @SuppressWarnings("WeakerAccess") public KafkaRecordReader() {
  }

  private void initConsumer() {
    if (consumer == null) {
      LOG.info("Initializing Kafka Consumer");
      final Properties properties = KafkaUtils.consumerProperties(config);
      String brokerString = properties.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
      Preconditions.checkNotNull(brokerString, "broker end point can not be null");
      LOG.info("Starting Consumer with Kafka broker string [{}]", brokerString);
      consumer = new KafkaConsumer<>(properties);
    }
  }

  @SuppressWarnings("WeakerAccess") public KafkaRecordReader(KafkaInputSplit inputSplit,
      Configuration jobConf) {
    initialize(inputSplit, jobConf);
  }

  private synchronized void initialize(KafkaInputSplit inputSplit, Configuration jobConf) {
    if (!started) {
      this.config = jobConf;
      startOffset = inputSplit.getStartOffset();
      endOffset = inputSplit.getEndOffset();
      TopicPartition topicPartition = new TopicPartition(inputSplit.getTopic(), inputSplit.getPartition());
      Preconditions.checkState(startOffset >= 0 && startOffset <= endOffset,
          "Start [%s] has to be positive and less or equal than End [%s]",
          startOffset,
          endOffset);
      totalNumberRecords += endOffset - startOffset;
      initConsumer();
      long
          pollTimeout =
          config.getLong(KafkaTableProperties.KAFKA_POLL_TIMEOUT.getName(), -1);
      LOG.debug("Consumer poll timeout [{}] ms", pollTimeout);
      this.recordsCursor =
          startOffset == endOffset ?
              new EmptyIterator() :
              new KafkaRecordIterator(consumer, topicPartition, startOffset, endOffset, pollTimeout);
      started = true;
    }
  }

  @Override public void initialize(org.apache.hadoop.mapreduce.InputSplit inputSplit, TaskAttemptContext context) {
    initialize((KafkaInputSplit) inputSplit, context.getConfiguration());
  }

  @Override public boolean next(NullWritable nullWritable, KafkaWritable bytesWritable) {
    if (started && recordsCursor.hasNext()) {
      ConsumerRecord<byte[], byte[]> record = recordsCursor.next();
      bytesWritable.set(record, startOffset, endOffset);
      consumedRecords += 1;
      readBytes += record.serializedValueSize();
      return true;
    }
    return false;
  }

  @Override public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override public KafkaWritable createValue() {
    return new KafkaWritable();
  }

  @Override public long getPos() {
    return -1;
  }

  @Override public boolean nextKeyValue() {
    currentWritableValue = new KafkaWritable();
    if (next(NullWritable.get(), currentWritableValue)) {
      return true;
    }
    currentWritableValue = null;
    return false;
  }

  @Override public NullWritable getCurrentKey() {
    return NullWritable.get();
  }

  @Override public KafkaWritable getCurrentValue() {
    return Preconditions.checkNotNull(currentWritableValue);
  }

  @Override public float getProgress() {
    if (consumedRecords == 0) {
      return 0f;
    }
    if (consumedRecords >= totalNumberRecords) {
      return 1f;
    }
    return consumedRecords * 1.0f / totalNumberRecords;
  }

  @Override public void close() {
    LOG.trace("total read bytes [{}]", readBytes);
    if (consumer != null) {
      consumer.wakeup();
      consumer.close();
    }
  }

  /**
   * Empty iterator for empty splits when startOffset == endOffset, this is added to avoid clumsy if condition.
   */
  private static final class EmptyIterator implements Iterator<ConsumerRecord<byte[], byte[]>> {
    @Override public boolean hasNext() {
      return false;
    }

    @Override public ConsumerRecord<byte[], byte[]> next() {
      throw new IllegalStateException("this is an empty iterator");
    }
  }
}
