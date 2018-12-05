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
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorAssignRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

/**
 * Vectorized Kafka record reader.
 */
class VectorizedKafkaRecordReader implements RecordReader<NullWritable, VectorizedRowBatch> {
  private static final Logger LOG = LoggerFactory.getLogger(VectorizedKafkaRecordReader.class);

  private final KafkaConsumer<byte[], byte[]> consumer;
  private final Iterator<ConsumerRecord<byte[], byte[]>> recordsCursor;
  private long totalNumberRecords = 0L;
  private long consumedRecords = 0L;
  private long readBytes = 0L;
  private final VectorizedRowBatchCtx rbCtx;
  /**
   * actual projected columns needed by the query, this can be empty in case of query like: select count(*) from src;
   */
  private final int[] projectedColumns;

  /**
   * underlying row deserializer.
   */
  private final KafkaSerDe serDe;

  private final VectorAssignRow vectorAssignRow = new VectorAssignRow();

  private final KafkaWritable kafkaWritable = new KafkaWritable();
  final Object[] row;

  VectorizedKafkaRecordReader(KafkaInputSplit inputSplit, Configuration jobConf) {
    // VectorBatch Context initializing
    this.rbCtx = Utilities.getVectorizedRowBatchCtx(jobConf);
    if (rbCtx.getDataColumnNums() != null) {
      projectedColumns = rbCtx.getDataColumnNums();
    } else {
      // case all the columns are selected
      projectedColumns = new int[rbCtx.getRowColumnTypeInfos().length];
      for (int i = 0; i < projectedColumns.length; i++) {
        projectedColumns[i] = i;
      }
    }

    // row parser and row assigner initializing
    serDe = createAndInitializeSerde(jobConf);
    try {
      vectorAssignRow.init((StructObjectInspector) serDe.getObjectInspector());
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }

    // Kafka iterator initializing
    long startOffset = inputSplit.getStartOffset();
    long endOffset = inputSplit.getEndOffset();
    TopicPartition topicPartition = new TopicPartition(inputSplit.getTopic(), inputSplit.getPartition());
    Preconditions.checkState(startOffset >= 0 && startOffset <= endOffset,
        "Start [%s] has to be positive and Less than or equal to End [%s]",
        startOffset,
        endOffset);
    totalNumberRecords += endOffset - startOffset;
    final Properties properties = KafkaUtils.consumerProperties(jobConf);
    consumer = new KafkaConsumer<>(properties);
    long pollTimeout = jobConf.getLong(KafkaTableProperties.KAFKA_POLL_TIMEOUT.getName(), -1);
    LOG.debug("Consumer poll timeout [{}] ms", pollTimeout);
    this.recordsCursor =
        startOffset == endOffset ?
            new KafkaRecordReader.EmptyIterator() :
            new KafkaRecordIterator(consumer, topicPartition, startOffset, endOffset, pollTimeout);

    // row has to be as wide as the entire kafka row plus metadata
    row = new Object[((StructObjectInspector) serDe.getObjectInspector()).getAllStructFieldRefs().size()];
  }

  @Override public boolean next(NullWritable nullWritable, VectorizedRowBatch vectorizedRowBatch) throws IOException {
    vectorizedRowBatch.reset();
    try {
      return readNextBatch(vectorizedRowBatch, recordsCursor) > 0;
    } catch (SerDeException e) {
      throw new IOException("Serde exception", e);
    }
  }

  private void cleanRowBoat() {
    for (int i = 0; i < row.length; i++) {
      row[i] = null;
    }
  }

  @Override public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override public VectorizedRowBatch createValue() {
    return rbCtx.createVectorizedRowBatch();
  }

  @Override public long getPos() throws IOException {
    return -1;
  }

  @Override public float getProgress() {
    if (consumedRecords >= totalNumberRecords) {
      return 1f;
    } else if (consumedRecords == 0) {
      return 0f;
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

  private int readNextBatch(VectorizedRowBatch vectorizedRowBatch,
      Iterator<ConsumerRecord<byte[], byte[]>> recordIterator) throws SerDeException {
    int rowsCount = 0;
    while (recordIterator.hasNext() && rowsCount < vectorizedRowBatch.getMaxSize()) {
      ConsumerRecord<byte[], byte[]> kRecord = recordIterator.next();
      kafkaWritable.set(kRecord);
      readBytes += kRecord.serializedKeySize() + kRecord.serializedValueSize();
      if (projectedColumns.length > 0) {
        serDe.deserializeKWritable(kafkaWritable, row);
        for (int i : projectedColumns) {
          vectorAssignRow.assignRowColumn(vectorizedRowBatch, rowsCount, i, row[i]);
        }
      }
      rowsCount++;
    }
    vectorizedRowBatch.size = rowsCount;
    consumedRecords += rowsCount;
    cleanRowBoat();
    return rowsCount;
  }

  @SuppressWarnings("Duplicates") private static KafkaSerDe createAndInitializeSerde(Configuration jobConf) {
    KafkaSerDe serDe = new KafkaSerDe();
    MapWork mapWork = Preconditions.checkNotNull(Utilities.getMapWork(jobConf), "Map work is null");
    Properties
        properties =
        mapWork.getPartitionDescs()
            .stream()
            .map(partitionDesc -> partitionDesc.getTableDesc().getProperties())
            .findAny()
            .orElseThrow(() -> new RuntimeException("Can not find table property at the map work"));
    try {
      serDe.initialize(jobConf, properties, null);
    } catch (SerDeException e) {
      throw new RuntimeException("Can not initialized the serde", e);
    }
    return serDe;
  }
}
