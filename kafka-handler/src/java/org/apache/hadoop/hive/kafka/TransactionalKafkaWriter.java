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
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Transactional Kafka Record Writer used to achieve Exactly once semantic.
 */
class TransactionalKafkaWriter implements FileSinkOperator.RecordWriter, RecordWriter<BytesWritable, KafkaWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionalKafkaWriter.class);
  private static final String TRANSACTION_DIR = "transaction_states";
  private static final Duration DURATION_0 = Duration.ofMillis(0);

  private final String topic;
  private final HiveKafkaProducer<byte[], byte[]> producer;
  private final Callback callback;
  private final AtomicReference<Exception> sendExceptionRef = new AtomicReference<>();
  private final Path openTxFileName;
  private final boolean optimisticCommit;
  private final FileSystem fileSystem;
  private final Map<TopicPartition, Long> offsets = new HashMap<>();
  private final String writerIdTopicId;
  private final long producerId;
  private final short producerEpoch;
  private long sentRecords = 0L;

  /**
   *  @param topic Kafka topic.
   * @param producerProperties kafka producer properties.
   * @param queryWorkingPath the Query working directory as, table_directory/hive_query_id.
 *                         Used to store the state of the transaction and/or log sent records and partitions.
 *                         for more information see:
 *                         {@link KafkaStorageHandler#getQueryWorkingDir(org.apache.hadoop.hive.metastore.api.Table)}
   * @param fileSystem file system handler.
   * @param optimisticCommit if true the commit will happen at the task level otherwise will be delegated to HS2.
   */
  TransactionalKafkaWriter(String topic, Properties producerProperties,
      Path queryWorkingPath,
      FileSystem fileSystem,
      @Nullable Boolean optimisticCommit) {
    this.fileSystem = fileSystem;
    this.topic = Preconditions.checkNotNull(topic, "NULL topic !!");

    Preconditions.checkState(producerProperties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG) != null,
        "set [" + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG + "] property");
    producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    this.producer = new HiveKafkaProducer<>(producerProperties);
    this.optimisticCommit = optimisticCommit == null ? true : optimisticCommit;
    this.callback = (metadata, exception) -> {
      if (exception != null) {
        sendExceptionRef.compareAndSet(null, exception);
      } else {
        //According to https://kafka.apache.org/0110/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html
        //Callbacks form the same TopicPartition will return in order thus this will keep track of most recent offset.
        final TopicPartition tp = new TopicPartition(metadata.topic(), metadata.partition());
        offsets.put(tp, metadata.offset());
      }
    };
    // Start Tx
    assert producer.getTransactionalId() != null;
    try {
      producer.initTransactions();
      producer.beginTransaction();
    } catch (Exception exception) {
      logHints(exception);
      if (tryToAbortTx(exception)) {
        LOG.error("Aborting Transaction [{}] cause by ERROR [{}]",
            producer.getTransactionalId(),
            exception.getMessage());
        producer.abortTransaction();
      }
      LOG.error("Closing writer [{}] caused by ERROR [{}]", producer.getTransactionalId(), exception.getMessage());
      producer.close(DURATION_0);
      throw exception;
    }
    writerIdTopicId = String.format("WriterId [%s], Kafka Topic [%s]", producer.getTransactionalId(), topic);
    producerEpoch = this.optimisticCommit ? -1 : producer.getEpoch();
    producerId = this.optimisticCommit ? -1 : producer.getProducerId();
    LOG.info("DONE with Initialization of {}, Epoch[{}], internal ID[{}]", writerIdTopicId, producerEpoch, producerId);
    //Writer base working directory
    openTxFileName =
        this.optimisticCommit ?
            null :
            new Path(new Path(new Path(queryWorkingPath, TRANSACTION_DIR), producer.getTransactionalId()),
                String.valueOf(producerEpoch));
  }

  @Override public void write(Writable w) throws IOException {
    checkExceptions();
    try {
      sentRecords++;
      producer.send(KafkaUtils.toProducerRecord(topic, (KafkaWritable) w), callback);
    } catch (Exception e) {
      if (tryToAbortTx(e)) {
        // producer.send() may throw a KafkaException which wraps a FencedException re throw its wrapped inner cause.
        producer.abortTransaction();
      }
      producer.close(DURATION_0);
      sendExceptionRef.compareAndSet(null, e);
      checkExceptions();
    }
  }

  private void logHints(Exception e) {
    if (e instanceof TimeoutException) {
      LOG.error("Maybe Try to increase [`retry.backoff.ms`] to avoid this error [{}].", e.getMessage());
    }
  }

  /**
   * The non Abort Close method can be split into 2 parts.
   * Part one is to Flush to Kafka all the buffered Records then Log (Topic-Partition, Offset).
   * Part two is To either commit the TX or Save the state of the TX to WAL and let HS2 do the commit.
   *
   * @param abort if set to true will abort flush and exit
   * @throws IOException exception causing the failure
   */
  @Override public void close(boolean abort) throws IOException {
    if (abort) {
      // Case Abort, try to AbortTransaction -> Close producer ASAP -> Exit;
      LOG.warn("Aborting Transaction and Sending from {}", writerIdTopicId);
      try {
        producer.abortTransaction();
      } catch (Exception e) {
        LOG.error("Aborting Transaction {} failed due to [{}]", writerIdTopicId, e.getMessage());
      }
      producer.close(DURATION_0);
      return;
    }

    // Normal Case ->  lOG and Commit then Close
    LOG.info("Flushing Kafka buffer of writerId {}", writerIdTopicId);
    producer.flush();

    // No exception good let's log to a file whatever Flushed.
    String formattedMsg = "Topic[%s] Partition [%s] -> Last offset [%s]";
    String
        flushedOffsetMsg =
        offsets.entrySet()
            .stream()
            .map(topicPartitionLongEntry -> String.format(formattedMsg,
                topicPartitionLongEntry.getKey().topic(),
                topicPartitionLongEntry.getKey().partition(),
                topicPartitionLongEntry.getValue()))
            .collect(Collectors.joining(","));

    LOG.info("WriterId {} flushed the following [{}] ", writerIdTopicId, flushedOffsetMsg);
    // OPTIMISTIC COMMIT OR PERSIST STATE OF THE TX_WAL
    checkExceptions();
    if (optimisticCommit) {
      // Case Commit at the task level
      commitTransaction();
    } else {
      // Case delegate TX commit to HS2
      persistTxState();
    }
    checkExceptions();
    LOG.info("Closed writerId [{}], Sent [{}] records to Topic [{}]",
        producer.getTransactionalId(),
        sentRecords,
        topic);
    producer.close(Duration.ZERO);
  }

  private void commitTransaction() {
    LOG.info("Attempting Optimistic commit by {}", writerIdTopicId);
    try {
      producer.commitTransaction();
    } catch (Exception e) {
      sendExceptionRef.compareAndSet(null, e);
    }
  }

  /**
   * Write the Kafka Consumer PID and Epoch to checkpoint file {@link TransactionalKafkaWriter#openTxFileName}.
   */
  private void persistTxState() {
    LOG.info("Committing state to path [{}] by [{}]", openTxFileName.toString(), writerIdTopicId);
    try (FSDataOutputStream outStream = fileSystem.create(openTxFileName)) {
      outStream.writeLong(producerId);
      outStream.writeShort(producerEpoch);
    } catch (Exception e) {
      sendExceptionRef.compareAndSet(null, e);
    }
  }

  @Override public void write(BytesWritable bytesWritable, KafkaWritable kafkaWritable) throws IOException {
    write(kafkaWritable);
  }

  @Override public void close(Reporter reporter) throws IOException {
    close(false);
  }

  @VisibleForTesting long getSentRecords() {
    return sentRecords;
  }

  @VisibleForTesting short getProducerEpoch() {
    return producerEpoch;
  }

  @VisibleForTesting long getProducerId() {
    return producerId;
  }

  /**
   * Checks for existing exception. In case of exception will close consumer and rethrow as IOException
   * @throws IOException abort if possible, close consumer then rethrow exception.
   */
  private void checkExceptions() throws IOException {
    if (sendExceptionRef.get() != null && sendExceptionRef.get() instanceof KafkaException && sendExceptionRef.get()
        .getCause() instanceof ProducerFencedException) {
      // producer.send() may throw a KafkaException which wraps a FencedException re throw its wrapped inner cause.
      sendExceptionRef.updateAndGet(e -> (KafkaException) e.getCause());
    }
    if (sendExceptionRef.get() != null) {
      final Exception exception = sendExceptionRef.get();
      logHints(exception);
      if (tryToAbortTx(exception)) {
        LOG.error("Aborting Transaction [{}] cause by ERROR [{}]", writerIdTopicId, exception.getMessage());
        producer.abortTransaction();
      }
      LOG.error("Closing writer [{}] caused by ERROR [{}]", writerIdTopicId, exception.getMessage());
      producer.close(DURATION_0);
      throw new IOException(exception);
    }
  }

  private boolean tryToAbortTx(Throwable e) {
    // According to https://kafka.apache.org/0110/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html
    // We can't recover from these exceptions, so our only option is to close the producer and exit.
    boolean
        isNotFencedOut =
        !(e instanceof ProducerFencedException)
            && !(e instanceof OutOfOrderSequenceException)
            && !(e instanceof AuthenticationException);
    // producer.send() may throw a KafkaException which wraps a FencedException therefore check inner cause.
    boolean causeIsNotFencedOut = !(e.getCause() != null && e.getCause() instanceof ProducerFencedException);
    return isNotFencedOut && causeIsNotFencedOut;
  }

  /**
   * Given a query workingDirectory as table_directory/hive_query_id/ will fetch the open transaction states.
   * Table directory is {@link org.apache.hadoop.hive.metastore.api.Table#getSd()#getLocation()}.
   * Hive Query ID is inferred from the JobConf see {@link KafkaStorageHandler#getQueryId()}.
   *
   * The path to a transaction state is as follow.
   * .../{@code queryWorkingDir}/{@code TRANSACTION_DIR}/{@code writerId}/{@code producerEpoch}
   *
   * The actual state is stored in the file {@code producerEpoch}.
   * The file contains a {@link Long} as internal producer Id and a {@link Short} as the producer epoch.
   * According to Kafka API, highest epoch corresponds to the active Producer, therefore if there is multiple
   * {@code producerEpoch} files will pick the maximum based on {@link Short::compareTo}.
   *
   * @param fs File system handler.
   * @param queryWorkingDir Query working Directory, see:
   *                        {@link KafkaStorageHandler#getQueryWorkingDir(org.apache.hadoop.hive.metastore.api.Table)}.
   * @return Map of Transaction Ids to Pair of Kafka Producer internal ID (Long) and producer epoch (short)
   * @throws IOException if any of the IO operations fail.
   */
  static Map<String, Pair<Long, Short>> getTransactionsState(FileSystem fs, Path queryWorkingDir) throws IOException {
    //list all current Dir
    final Path transactionWorkingDir = new Path(queryWorkingDir, TRANSACTION_DIR);
    final FileStatus[] files = fs.listStatus(transactionWorkingDir);
    final Set<FileStatus>
        transactionSet =
        Arrays.stream(files).filter(FileStatus::isDirectory).collect(Collectors.toSet());
    Set<Path> setOfTxPath = transactionSet.stream().map(FileStatus::getPath).collect(Collectors.toSet());
    ImmutableMap.Builder<String, Pair<Long, Short>> builder = ImmutableMap.builder();
    setOfTxPath.forEach(path -> {
      final String txId = path.getName();
      try {
        FileStatus[] epochFiles = fs.listStatus(path);
        // List all the Epoch if any and select the max.
        // According to Kafka API recent venison of Producer with the same TxID will have greater epoch and same PID.
        Optional<Short>
            maxEpoch =
            Arrays.stream(epochFiles)
                .filter(FileStatus::isFile)
                .map(fileStatus -> Short.valueOf(fileStatus.getPath().getName()))
                .max(Short::compareTo);
        short
            epoch =
            maxEpoch.orElseThrow(() -> new RuntimeException("Missing sub directory epoch from directory ["
                + path.toString()
                + "]"));
        Path openTxFileName = new Path(path, String.valueOf(epoch));
        long internalId;
        try (FSDataInputStream inStream = fs.open(openTxFileName)) {
          internalId = inStream.readLong();
          short fileEpoch = inStream.readShort();
          if (epoch != fileEpoch) {
            throw new RuntimeException(String.format("Was expecting [%s] but got [%s] from path [%s]",
                epoch,
                fileEpoch,
                path.toString()));
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        builder.put(txId, Pair.of(internalId, epoch));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    return builder.build();
  }
}
