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
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Hive to Kafka Simple Record Writer. It can be used to achieve AT LEAST ONCE semantic, or no guaranties at all.
 */
class SimpleKafkaWriter implements FileSinkOperator.RecordWriter, RecordWriter<BytesWritable, KafkaWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleKafkaWriter.class);
  private static final  String
      TIMEOUT_CONFIG_HINT =
      "Try increasing producer property [`retries`] and [`retry.backoff.ms`] to avoid this error [{}].";
  private static final  String
      ABORT_MSG =
      "Writer [%s] aborting Send. Caused by [%s]. Sending to topic [%s]. Record offset [%s];";
  private static final String
      ACTION_ABORT =
      "WriterId [{}] lost record from Topic [{}], delivery Semantic [{}] -> ACTION=ABORT, ERROR caused by [{}]";
  private static final String
      ACTION_CARRY_ON =
      "WriterId [{}], lost record from Topic [{}], delivery Semantic [{}] -> ACTION=CARRY-ON";

  private final String topic;
  private final String writerId;
  private final KafkaOutputFormat.WriteSemantic writeSemantic = KafkaOutputFormat.WriteSemantic.AT_LEAST_ONCE;;
  private final KafkaProducer<byte[], byte[]> producer;
  private final Callback callback;
  private final AtomicReference<Exception> sendExceptionRef = new AtomicReference<>();
  private final AtomicLong lostRecords = new AtomicLong(0L);
  private long sentRecords = 0L;

  /**
   * @param topic Kafka Topic.
   * @param writerId Writer Id use for logging.
   * @param properties Kafka Producer properties.
   */
  SimpleKafkaWriter(String topic, @Nullable String writerId, Properties properties) {
    this.writerId = writerId == null ? UUID.randomUUID().toString() : writerId;
    this.topic = Preconditions.checkNotNull(topic, "Topic can not be null");
    Preconditions.checkState(properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG) != null,
        "set [" + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG + "] property");
    producer = new KafkaProducer<>(properties, new ByteArraySerializer(), new ByteArraySerializer());

    this.callback = (metadata, exception) -> {
      if (exception != null) {
        lostRecords.getAndIncrement();
        LOG.error(ACTION_ABORT, getWriterId(), topic, writeSemantic, exception.getMessage());
        sendExceptionRef.compareAndSet(null, exception);
      }
    };
    LOG.info("Starting WriterId [{}], Delivery Semantic [{}], Target Kafka Topic [{}]",
        writerId,
        writeSemantic,
        topic);
  }

  @Override public void write(Writable w) throws IOException {
    checkExceptions();
    try {
      sentRecords++;
      producer.send(KafkaUtils.toProducerRecord(topic, (KafkaWritable) w), callback);
    } catch (KafkaException kafkaException) {
      handleKafkaException(kafkaException);
      checkExceptions();
    }
  }

  private void handleKafkaException(KafkaException kafkaException) {
    if (kafkaException instanceof TimeoutException) {
      //This might happen if the producer cannot send data to the Kafka cluster and thus, its internal buffer fills up.
      LOG.error(TIMEOUT_CONFIG_HINT, kafkaException.getMessage());
    }
    if (KafkaUtils.exceptionIsFatal(kafkaException)) {
      LOG.error(String.format(ABORT_MSG, writerId, kafkaException.getMessage(), topic, -1L));
      sendExceptionRef.compareAndSet(null, kafkaException);
    } else {
      LOG.error(ACTION_ABORT, writerId, topic, writeSemantic, kafkaException.getMessage());
      sendExceptionRef.compareAndSet(null, kafkaException);
    }
  }

  @Override public void close(boolean abort) throws IOException {
    if (abort) {
      LOG.info("Aborting is set to TRUE, Closing writerId [{}] without flush.", writerId);
      producer.close(0, TimeUnit.MICROSECONDS);
      return;
    } else {
      LOG.info("Flushing Kafka Producer with writerId [{}]", writerId);
      producer.flush();
      LOG.info("Closing WriterId [{}]", writerId);
      producer.close();
    }
    LOG.info("Closed WriterId [{}] Delivery semantic [{}], Topic[{}], Total sent Records [{}], Total Lost Records [{}]",
        writerId, writeSemantic,
        topic,
        sentRecords,
        lostRecords.get());
    checkExceptions();
  }

  @VisibleForTesting String getWriterId() {
    return writerId;
  }

  @VisibleForTesting long getLostRecords() {
    return lostRecords.get();
  }

  @VisibleForTesting long getSentRecords() {
    return sentRecords;
  }

  @Override public void write(BytesWritable bytesWritable, KafkaWritable kafkaWritable) throws IOException {
    this.write(kafkaWritable);
  }

  @Override public void close(Reporter reporter) throws IOException {
    this.close(false);
  }

  private void checkExceptions() throws IOException {
    if (sendExceptionRef.get() != null) {
      LOG.error("Send Exception Aborting write from writerId [{}]", writerId);
      producer.close(0, TimeUnit.MICROSECONDS);
      throw new IOException(sendExceptionRef.get());
    }
  }

}
