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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Iterator over Kafka Records to read records from a single topic partition inclusive start, exclusive end.
 */
public class KafkaRecordIterator implements Iterator<ConsumerRecord<byte[], byte[]>> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaRecordIterator.class);
  private static final String
      POLL_TIMEOUT_HINT =
      String.format("Try increasing poll timeout using Hive Table property [%s]",
          KafkaStreamingUtils.HIVE_KAFKA_POLL_TIMEOUT);
  private static final String
      ERROR_POLL_TIMEOUT_FORMAT =
      "Consumer returned [0] record due to exhausted poll timeout [%s]ms from TopicPartition:[%s] "
          + "start Offset [%s], current consumer position [%s], target end offset [%s], "
          + POLL_TIMEOUT_HINT;

  private final Consumer<byte[], byte[]> consumer;
  private final TopicPartition topicPartition;
  private final long endOffset;
  private final long startOffset;
  private final long pollTimeoutMs;
  private final Stopwatch stopwatch = Stopwatch.createUnstarted();
  private ConsumerRecords<byte[], byte[]> records;
  private long consumerPosition;
  private ConsumerRecord<byte[], byte[]> nextRecord;
  private boolean hasMore = true;
  private Iterator<ConsumerRecord<byte[], byte[]>> consumerRecordIterator = null;

  /**
   * Iterator over Kafka Records over a single {@code topicPartition} inclusive {@code startOffset},
   * up to exclusive {@code endOffset}.
   * <p>
   * If {@code requestedStartOffset} is not null will seek up to that offset
   * Else If {@code requestedStartOffset} is null will seek to beginning see
   * {@link org.apache.kafka.clients.consumer.Consumer#seekToBeginning(java.util.Collection)}
   * <p>
   * When provided with {@code requestedEndOffset}, will return records up to consumer position == endOffset
   * Else If {@code requestedEndOffset} is null it will read up to the current end of the stream
   * {@link org.apache.kafka.clients.consumer.Consumer#seekToEnd(java.util.Collection)}
   *  <p>
   * @param consumer       functional kafka consumer.
   * @param topicPartition kafka topic partition.
   * @param requestedStartOffset    requested start position.
   * @param requestedEndOffset      requested end position. If null will read up to current last
   * @param pollTimeoutMs  poll time out in ms.
   */
  KafkaRecordIterator(Consumer<byte[], byte[]> consumer,
      TopicPartition topicPartition,
      @Nullable Long requestedStartOffset,
      @Nullable Long requestedEndOffset,
      long pollTimeoutMs) {
    this.consumer = Preconditions.checkNotNull(consumer, "Consumer can not be null");
    this.topicPartition = Preconditions.checkNotNull(topicPartition, "Topic partition can not be null");
    this.pollTimeoutMs = pollTimeoutMs;
    Preconditions.checkState(this.pollTimeoutMs > 0, "Poll timeout has to be positive number");
    final List<TopicPartition> topicPartitionList = Collections.singletonList(topicPartition);
    // assign topic partition to consumer
    consumer.assign(topicPartitionList);

    // do to End Offset first in case of we have to seek to end to figure out the last available offset
    if (requestedEndOffset == null) {
      consumer.seekToEnd(topicPartitionList);
      this.endOffset = consumer.position(topicPartition);
      LOG.info("End Offset set to [{}]", this.endOffset);
    } else {
      this.endOffset = requestedEndOffset;
    }

    // seek to start offsets
    if (requestedStartOffset != null) {
      LOG.info("Seeking to offset [{}] of topic partition [{}]", requestedStartOffset, topicPartition);
      consumer.seek(topicPartition, requestedStartOffset);
      this.startOffset = consumer.position(topicPartition);
      if (this.startOffset != requestedStartOffset) {
        LOG.warn("Current Start Offset [{}] is different form the requested start position [{}]",
            this.startOffset,
            requestedStartOffset);
      }
    } else {
      // case seek to beginning of stream
      consumer.seekToBeginning(Collections.singleton(topicPartition));
      // seekToBeginning is lazy thus need to call position() or poll(0)
      this.startOffset = consumer.position(topicPartition);
      LOG.info("Consumer at beginning of topic partition [{}], current start offset [{}]",
          topicPartition,
          this.startOffset);
    }

    consumerPosition = consumer.position(topicPartition);
    Preconditions.checkState(this.endOffset >= consumerPosition,
        "End offset [%s] need to be greater or equal than start offset [%s]",
        this.endOffset,
        consumerPosition);
    LOG.info("Kafka Iterator assigned to TopicPartition [{}]; start Offset [{}]; end Offset [{}]",
        topicPartition,
        consumerPosition,
        this.endOffset);

  }

  @VisibleForTesting KafkaRecordIterator(Consumer<byte[], byte[]> consumer, TopicPartition tp, long pollTimeoutMs) {
    this(consumer, tp, null, null, pollTimeoutMs);
  }

  /**
   * @throws IllegalStateException if the kafka consumer poll call can not reach the target offset.
   * @return true if has more records to be consumed.
   */
  @Override public boolean hasNext() {
    /*
    Poll more records from Kafka queue IF:
    Initial poll -> (records == null)
      OR
    Need to poll at least one more record (consumerPosition < endOffset) AND consumerRecordIterator is empty (!hasMore)
    */
    if (!hasMore && consumerPosition < endOffset || records == null) {
      pollRecords();
      findNext();
    }
    return hasMore;
  }

  /**
   * Poll more records from the Kafka Broker.
   *
   * @throws IllegalStateException if no records returned before consumer position reaches target end offset.
   */
  private void pollRecords() {
    if (LOG.isTraceEnabled()) {
      stopwatch.reset().start();
    }
    records = consumer.poll(pollTimeoutMs);
    if (LOG.isTraceEnabled()) {
      stopwatch.stop();
      LOG.trace("Pulled [{}] records in [{}] ms", records.count(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
    // Fail if we can not poll within one lap of pollTimeoutMs.
    if (records.isEmpty() && consumer.position(topicPartition) < endOffset) {
      throw new IllegalStateException(String.format(ERROR_POLL_TIMEOUT_FORMAT,
          pollTimeoutMs,
          topicPartition.toString(),
          startOffset,
          consumer.position(topicPartition),
          endOffset));
    }
    consumerRecordIterator = records.iterator();
    consumerPosition = consumer.position(topicPartition);
  }

  @Override public ConsumerRecord<byte[], byte[]> next() {
    ConsumerRecord<byte[], byte[]> value = nextRecord;
    Preconditions.checkState(value.offset() < endOffset);
    findNext();
    return value;
  }

  /**
   * Find the next element in the current batch OR schedule {@link KafkaRecordIterator#pollRecords()} (hasMore = false).
   */
  private void findNext() {
    if (consumerRecordIterator.hasNext()) {
      nextRecord = consumerRecordIterator.next();
      hasMore = nextRecord.offset() < endOffset;
    } else {
      hasMore = false;
      nextRecord = null;
    }
  }
}
