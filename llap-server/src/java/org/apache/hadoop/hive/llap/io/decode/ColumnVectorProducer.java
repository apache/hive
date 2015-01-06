/**
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

package org.apache.hadoop.hive.llap.io.decode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.llap.Consumer;
import org.apache.hadoop.hive.llap.ConsumerFeedback;
import org.apache.hadoop.hive.llap.io.api.EncodedColumn;
import org.apache.hadoop.hive.llap.io.api.VectorReader.ColumnVectorBatch;
import org.apache.hadoop.hive.llap.io.api.cache.Allocator;
import org.apache.hadoop.hive.llap.io.api.cache.Allocator.LlapBuffer;
import org.apache.hadoop.hive.llap.io.encoded.EncodedDataProducer;
import org.apache.hadoop.hive.llap.io.encoded.EncodedDataReader;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.mapred.InputSplit;

/** Middle layer - gets encoded blocks, produces proto-VRBs */
public abstract class ColumnVectorProducer<BatchKey> {
  static class EncodedColumnBatch {
    public EncodedColumnBatch(int colCount) {
      columnDatas = new LlapBuffer[colCount];
      columnsRemaining = colCount;
    }
    public LlapBuffer[] columnDatas;
    public int columnsRemaining;
  }

  private class EncodedDataConsumer implements ConsumerFeedback<ColumnVectorBatch>,
      Consumer<EncodedColumn<BatchKey>> {
    private volatile boolean isStopped = false;
    // TODO: use array, precreate array based on metadata first? Works for ORC. For now keep dumb.
    private final HashMap<BatchKey, EncodedColumnBatch> pendingData =
        new HashMap<BatchKey, EncodedColumnBatch>();
    private ConsumerFeedback<LlapBuffer> upstreamFeedback;
    private final Consumer<ColumnVectorBatch> downstreamConsumer;
    private final int colCount;

    public EncodedDataConsumer(Consumer<ColumnVectorBatch> consumer, int colCount) {
      this.downstreamConsumer = consumer;
      this.colCount = colCount;
    }

    public void init(ConsumerFeedback<LlapBuffer> upstreamFeedback) {
      this.upstreamFeedback = upstreamFeedback;
    }

    @Override
    public void consumeData(EncodedColumn<BatchKey> data) {
      EncodedColumnBatch targetBatch = null;
      boolean localIsStopped = false;
      synchronized (pendingData) {
        localIsStopped = isStopped;
        if (!localIsStopped) {
          targetBatch = pendingData.get(data.batchKey);
          if (targetBatch == null) {
            targetBatch = new EncodedColumnBatch(colCount);
            pendingData.put(data.batchKey, targetBatch);
          }
        }
      }
      if (localIsStopped) {
        upstreamFeedback.returnData(data.columnData);
        return;
      }

      int colsRemaining = -1;
      synchronized (targetBatch) {
        // Check if we are stopped and the batch was already cleaned.
        localIsStopped = (targetBatch.columnDatas == null);
        if (!localIsStopped) {
          targetBatch.columnDatas[data.columnIndex] = data.columnData;
          colsRemaining = --targetBatch.columnsRemaining;
          if (0 == colsRemaining) {
            synchronized (pendingData) {
              targetBatch = isStopped ? null : pendingData.remove(data.batchKey);
            }
            // Check if we are stopped and the batch had been removed from map.
            localIsStopped = (targetBatch == null);
            // We took the batch out of the map. No more contention with stop possible.
          }
        }
      }
      if (localIsStopped) {
        upstreamFeedback.returnData(data.columnData);
        return;
      }
      if (0 == colsRemaining) {
        ColumnVectorProducer.this.decodeBatch(data.batchKey, targetBatch, downstreamConsumer);
      }
    }

    @Override
    public void setDone() {
      synchronized (pendingData) {
        if (!pendingData.isEmpty()) {
          throw new AssertionError("Not all data has been sent downstream: " + pendingData.size());
        }
      }
      downstreamConsumer.setDone();
    }


    @Override
    public void setError(Throwable t) {
      downstreamConsumer.setError(t);
      dicardPendingData(false);
    }

    @Override
    public void stop() {
      upstreamFeedback.stop();
      dicardPendingData(true);
    }

    @Override
    public void returnData(ColumnVectorBatch data) {
      for (LlapBuffer lockedBuffer : data.lockedBuffers) {
        upstreamFeedback.returnData(lockedBuffer);
      }
    }

    private void dicardPendingData(boolean isStopped) {
      List<LlapBuffer> dataToDiscard = new ArrayList<LlapBuffer>(pendingData.size() * colCount);
      List<EncodedColumnBatch> batches = new ArrayList<EncodedColumnBatch>(pendingData.size());
      synchronized (pendingData) {
        if (isStopped) {
          this.isStopped = true;
        }
        batches.addAll(pendingData.values());
        pendingData.clear();
      }
      for (EncodedColumnBatch batch : batches) {
        synchronized (batch) {
          for (LlapBuffer b : batch.columnDatas) {
            dataToDiscard.add(b);
          }
          batch.columnDatas = null;
        }
      }
      for (LlapBuffer data : dataToDiscard) {
        upstreamFeedback.returnData(data);
      }
    }
  }

  /**
   * Reads ColumnVectorBatch-es.
   * @param consumer Consumer that will receive the batches asynchronously.
   * @return Feedback that can be used to stop reading, and should be used
   *         to return consumed batches.
   * @throws IOException 
   */
  public ConsumerFeedback<ColumnVectorBatch> read(InputSplit split, List<Integer> columnIds,
      SearchArgument sarg, Consumer<ColumnVectorBatch> consumer) throws IOException {
    // Create the consumer of encoded data; it will coordinate decoding to CVBs.
    EncodedDataConsumer edc = new EncodedDataConsumer(consumer, columnIds.size());
    // Get the source of encoded data.
    EncodedDataProducer<BatchKey> edp = getEncodedDataProducer();
    // Then, get the specific reader of encoded data out of the producer.
    EncodedDataReader<BatchKey> reader = edp.getReader(split, columnIds, sarg, edc);
    // Set the encoded data reader as upstream feedback for encoded data consumer, and start.
    edc.init(reader);
    reader.start();
    return edc;
  }

  protected abstract EncodedDataProducer<BatchKey> getEncodedDataProducer();

  protected abstract void decodeBatch(BatchKey batchKey, EncodedColumnBatch batch,
      Consumer<ColumnVectorBatch> downstreamConsumer);
}
