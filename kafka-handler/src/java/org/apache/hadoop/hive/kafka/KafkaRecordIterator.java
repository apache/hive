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
import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This class implements an Iterator over a single Kafka topic partition.
 *
 * Notes:
 * The user of this class has to provide a functional Kafka Consumer and then has to clean it afterward.
 *
 * Iterator position is related to the position of the consumer therefore consumer CAN NOT BE SHARED between threads.
 *
 * The polling of new record will only occur if the current buffered records are consumed by the iterator via:
 * {@link org.apache.hadoop.hive.kafka.KafkaRecordIterator#next()}
 *
 * org.apache.hadoop.hive.kafka.KafkaRecordIterator#hasNext() throws PollTimeoutException in case Kafka consumer poll,
 * returns 0 record and consumer position did not reach requested endOffset.
 * Such an exception is a retryable exception, and it can be a transient exception that if retried may succeed.
 *
 */
class KafkaRecordIterator implements Iterator<ConsumerRecord<byte[], byte[]>> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaRecordIterator.class);
  private static final String
      POLL_TIMEOUT_HINT =
      String.format("Try increasing poll timeout using Hive Table property [%s]",
          KafkaTableProperties.KAFKA_POLL_TIMEOUT.getName());
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
  private final Duration pollTimeoutDurationMs;
  private final Stopwatch stopwatch = Stopwatch.createUnstarted();
  private ConsumerRecords<byte[], byte[]> records;
  /**
   * Holds the kafka consumer position after the last poll() call.
   */
  private long consumerPosition;
  private ConsumerRecord<byte[], byte[]> nextRecord;
  private boolean hasMore = true;
  /**
   * On each Kafka Consumer poll() call we get a batch of records, this Iterator will be used to loop over it.
   */
  private Iterator<ConsumerRecord<byte[], byte[]>> consumerRecordIterator = null;

  /**
   * Kafka record Iterator pulling from a single {@code topicPartition} an inclusive {@code requestedStartOffset},
   * up to exclusive {@code requestedEndOffset}.
   * Iterator position is related to the position of the consumer therefore consumer can not be shared between threads.
   *
   * This iterator can block on polling up to a designated timeout.
   *
   * If no record is returned by brokers after poll timeout duration such case will be considered as an exception.
   * Although the timeout exception it is a retryable exception, therefore users of this class can retry if needed.
   *
   * Other than the Kafka consumer, No Resources cleaning is needed.
   *
   * @param consumer       Functional kafka consumer, user must initialize and close it.
   * @param topicPartition Target Kafka topic partition.
   * @param requestedStartOffset    Requested start offset position, if NULL iterator will seek to beginning using:
   *                                {@link Consumer#seekToBeginning(java.util.Collection)}.
   *
   * @param requestedEndOffset      Requested end position. If null will read up to last available offset,
   *                                such position is given by:
   *                                {@link Consumer#seekToEnd(java.util.Collection)}.
   * @param pollTimeoutMs  positive number indicating poll time out in ms.
   */
  KafkaRecordIterator(Consumer<byte[], byte[]> consumer,
      TopicPartition topicPartition,
      @Nullable Long requestedStartOffset,
      @Nullable Long requestedEndOffset,
      long pollTimeoutMs) {
    this.consumer = Preconditions.checkNotNull(consumer, "Consumer can not be null");
    this.topicPartition = Preconditions.checkNotNull(topicPartition, "Topic partition can not be null");
    this.pollTimeoutMs = pollTimeoutMs;
    this.pollTimeoutDurationMs = Duration.ofMillis(pollTimeoutMs);
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
   * Check if there is more records to be consumed and pull more from the broker if current batch of record is empty.
   * This method might block up to {@link this#pollTimeoutMs} to pull records from Kafka Broker.
   *
   * @throws PollTimeoutException if poll returns 0 record and consumer position did not reach requested endOffset.
   * Such an exception is a retryable exception, and it can be a transient exception that if retried may succeed.
   *
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
   * @throws PollTimeoutException if poll returns 0 record  and consumer's position < requested endOffset.
   */
  private void pollRecords() {
    if (LOG.isTraceEnabled()) {
      stopwatch.reset().start();
    }
    records = consumer.poll(pollTimeoutDurationMs);
    if (LOG.isTraceEnabled()) {
      stopwatch.stop();
      LOG.trace("Pulled [{}] records in [{}] ms", records.count(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
    // Fail if we can not poll within one lap of pollTimeoutMs.
    if (records.isEmpty() && consumer.position(topicPartition) < endOffset) {
      throw new PollTimeoutException(String.format(ERROR_POLL_TIMEOUT_FORMAT,
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

  static final class PollTimeoutException extends RetriableException {
    private static final long serialVersionUID = 1L;

    PollTimeoutException(String message) {
      super(message);
    }
  }
}
