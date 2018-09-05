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
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Iterator over Kafka Records to read records from a single topic partition inclusive start exclusive end.
 * <p>
 * If {@code startOffset} is not null will seek up to that offset
 * Else If {@code startOffset} is null will seek to beginning see
 * {@link org.apache.kafka.clients.consumer.Consumer#seekToBeginning(java.util.Collection)}
 * <p>
 * When provided with an end offset it will return records up to the record with offset == endOffset - 1,
 * Else If end offsets is null it will read up to the current end see
 * {@link org.apache.kafka.clients.consumer.Consumer#endOffsets(java.util.Collection)}
 * <p>
 * Current implementation of this Iterator will throw and exception if can not poll up to the endOffset - 1
 */
public class KafkaRecordIterator implements Iterator<ConsumerRecord<byte[], byte[]>> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaRecordIterator.class);

  private final Consumer<byte[], byte[]> consumer;
  private final TopicPartition topicPartition;
  private long endOffset;
  private long startOffset;
  private final long pollTimeoutMs;
  private final Stopwatch stopwatch = Stopwatch.createUnstarted();
  private ConsumerRecords<byte[], byte[]> records;
  private long currentOffset;
  private ConsumerRecord<byte[], byte[]> nextRecord;
  private boolean hasMore = true;
  private final boolean started;

  //Kafka consumer poll method return an iterator of records.
  private Iterator<ConsumerRecord<byte[], byte[]>> consumerRecordIterator = null;

  /**
   * @param consumer       functional kafka consumer
   * @param topicPartition kafka topic partition
   * @param startOffset    start position of stream.
   * @param endOffset      requested end position. If null will read up to current last
   * @param pollTimeoutMs  poll time out in ms
   */
  KafkaRecordIterator(Consumer<byte[], byte[]> consumer,
      TopicPartition topicPartition,
      @Nullable Long startOffset,
      @Nullable Long endOffset,
      long pollTimeoutMs) {
    this.consumer = Preconditions.checkNotNull(consumer, "Consumer can not be null");
    this.topicPartition = Preconditions.checkNotNull(topicPartition, "Topic partition can not be null");
    this.pollTimeoutMs = pollTimeoutMs;
    Preconditions.checkState(this.pollTimeoutMs > 0, "poll timeout has to be positive number");
    this.startOffset = startOffset == null ? -1L : startOffset;
    this.endOffset = endOffset == null ? -1L : endOffset;
    assignAndSeek();
    this.started = true;
  }

  KafkaRecordIterator(Consumer<byte[], byte[]> consumer, TopicPartition tp, long pollTimeoutMs) {
    this(consumer, tp, null, null, pollTimeoutMs);
  }

  private void assignAndSeek() {
    // assign topic partition to consumer
    final List<TopicPartition> topicPartitionList = ImmutableList.of(topicPartition);
    if (LOG.isTraceEnabled()) {
      stopwatch.reset().start();
    }

    consumer.assign(topicPartitionList);
    // compute offsets and seek to start
    if (startOffset > -1) {
      LOG.info("Seeking to offset [{}] of topic partition [{}]", startOffset, topicPartition);
      consumer.seek(topicPartition, startOffset);
    } else {
      LOG.info("Seeking to beginning of topic partition [{}]", topicPartition);
      // seekToBeginning is lazy thus need to call position() or poll(0)
      this.consumer.seekToBeginning(Collections.singleton(topicPartition));
      startOffset = consumer.position(topicPartition);
    }
    if (endOffset == -1) {
      this.endOffset = consumer.endOffsets(topicPartitionList).get(topicPartition);
      LOG.info("EndOffset set to {}", endOffset);
    }
    currentOffset = consumer.position(topicPartition);
    Preconditions.checkState(this.endOffset >= currentOffset,
        "End offset [%s] need to be greater than start offset [%s]",
        this.endOffset,
        currentOffset);
    LOG.info("Kafka Iterator ready, assigned TopicPartition [{}]; startOffset [{}]; endOffset [{}]",
        topicPartition,
        currentOffset,
        this.endOffset);
    if (LOG.isTraceEnabled()) {
      stopwatch.stop();
      LOG.trace("Time to assign and seek [{}] ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
  }

  @Override
  public boolean hasNext() {
    /*
    Poll more records from Kafka queue IF:
    Initial poll case -> (records == null)
      OR
    Need to poll at least one more record (currentOffset + 1 < endOffset) AND consumerRecordIterator is empty (!hasMore)
    */
    if (!hasMore && currentOffset + 1 < endOffset || records == null) {
      pollRecords();
      findNext();
    }
    return hasMore;
  }

  /**
   * Poll more records or Fail with {@link TimeoutException} if no records returned before reaching target end offset.
   */
  private void pollRecords() {
    if (LOG.isTraceEnabled()) {
      stopwatch.reset().start();
    }
    Preconditions.checkArgument(started);
    records = consumer.poll(pollTimeoutMs);
    if (LOG.isTraceEnabled()) {
      stopwatch.stop();
      LOG.trace("Pulled [{}] records in [{}] ms", records.count(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
    // Fail if we can not poll within one lap of pollTimeoutMs.
    if (records.isEmpty() && currentOffset < endOffset) {
      throw new TimeoutException(String.format("Current offset: [%s]-TopicPartition:[%s], target End offset:[%s]."
              + "Consumer returned 0 record due to exhausted poll timeout [%s]ms, try increasing[%s]",
          currentOffset,
          topicPartition.toString(),
          endOffset,
          pollTimeoutMs,
          KafkaStreamingUtils.HIVE_KAFKA_POLL_TIMEOUT));
    }
    consumerRecordIterator = records.iterator();
  }

  @Override public ConsumerRecord<byte[], byte[]> next() {
    ConsumerRecord<byte[], byte[]> value = nextRecord;
    Preconditions.checkState(value.offset() < endOffset);
    findNext();
    return Preconditions.checkNotNull(value);
  }

  /**
   * Find the next element in the batch of returned records by previous poll or set hasMore to false tp poll more next
   * call to {@link KafkaRecordIterator#hasNext()}.
   */
  private void findNext() {
    if (consumerRecordIterator.hasNext()) {
      nextRecord = consumerRecordIterator.next();
      hasMore = true;
      if (nextRecord.offset() < endOffset) {
        currentOffset = nextRecord.offset();
        return;
      }
    }
    hasMore = false;
    nextRecord = null;
  }

  /**
   * Empty iterator for empty splits when startOffset == endOffset, this is added to avoid clumsy if condition.
   */
  protected static final class EmptyIterator implements Iterator<ConsumerRecord<byte[], byte[]>>  {
    @Override public boolean hasNext() {
      return false;
    }

    @Override public ConsumerRecord<byte[], byte[]> next() {
      throw new IllegalStateException("this is an empty iterator");
    }
  }
}
