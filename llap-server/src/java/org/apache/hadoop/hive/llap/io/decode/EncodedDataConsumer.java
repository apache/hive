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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.llap.Consumer;
import org.apache.hadoop.hive.llap.ConsumerFeedback;
import org.apache.hadoop.hive.llap.io.api.EncodedColumnBatch;
import org.apache.hadoop.hive.llap.io.api.EncodedColumnBatch.StreamBuffer;
import org.apache.hadoop.hive.llap.io.api.impl.ColumnVectorBatch;

/**
 *
 */
public abstract class EncodedDataConsumer<BatchKey> implements ConsumerFeedback<ColumnVectorBatch>,
    Consumer<EncodedColumnBatch<BatchKey>> {
  private volatile boolean isStopped = false;
  // TODO: use array, precreate array based on metadata first? Works for ORC. For now keep dumb.
  private final HashMap<BatchKey, EncodedColumnBatch<BatchKey>> pendingData = new HashMap<>();
  private ConsumerFeedback<EncodedColumnBatch.StreamBuffer> upstreamFeedback;
  private final Consumer<ColumnVectorBatch> downstreamConsumer;
  private final int colCount;
  private ColumnVectorProducer<BatchKey> cvp;

  public EncodedDataConsumer(ColumnVectorProducer<BatchKey> cvp,
      Consumer<ColumnVectorBatch> consumer, int colCount) {
    this.downstreamConsumer = consumer;
    this.colCount = colCount;
    this.cvp = cvp;
  }

  public void init(ConsumerFeedback<EncodedColumnBatch.StreamBuffer> upstreamFeedback) {
    this.upstreamFeedback = upstreamFeedback;
  }

  @Override
  public void consumeData(EncodedColumnBatch<BatchKey> data) {
    EncodedColumnBatch<BatchKey> targetBatch = null;
    boolean localIsStopped = false;
    synchronized (pendingData) {
      localIsStopped = isStopped;
      if (!localIsStopped) {
        targetBatch = pendingData.get(data.batchKey);
        if (targetBatch == null) {
          targetBatch = data;
          pendingData.put(data.batchKey, data);
        }
      }
    }
    if (localIsStopped) {
      returnProcessed(data.columnData);
      return;
    }

    synchronized (targetBatch) {
      // Check if we are stopped and the batch was already cleaned.
      localIsStopped = (targetBatch.columnData == null);
      if (!localIsStopped) {
        if (targetBatch != data) {
          targetBatch.merge(data);
        }
        if (0 == targetBatch.colsRemaining) {
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
      returnProcessed(data.columnData);
      return;
    }
    if (0 == targetBatch.colsRemaining) {
      cvp.decodeBatch(this, targetBatch, downstreamConsumer);
      // Batch has been decoded; unlock the buffers in cache
      returnProcessed(targetBatch.columnData);
    }
  }

  protected void returnProcessed(EncodedColumnBatch.StreamBuffer[][] data) {
    for (EncodedColumnBatch.StreamBuffer[] sbs : data) {
      for (EncodedColumnBatch.StreamBuffer sb : sbs) {
        if (sb.decRef() != 0) continue;
        upstreamFeedback.returnData(sb);
      }
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
  public void returnData(ColumnVectorBatch data) {
    // TODO: column vectors could be added to object pool here
  }

  private void dicardPendingData(boolean isStopped) {
    List<EncodedColumnBatch<BatchKey>> batches = new ArrayList<EncodedColumnBatch<BatchKey>>(
        pendingData.size());
    synchronized (pendingData) {
      if (isStopped) {
        this.isStopped = true;
      }
      batches.addAll(pendingData.values());
      pendingData.clear();
    }
    List<EncodedColumnBatch.StreamBuffer> dataToDiscard = new ArrayList<StreamBuffer>(
        batches.size() * colCount * 2);
    for (EncodedColumnBatch<BatchKey> batch : batches) {
      synchronized (batch) {
        for (EncodedColumnBatch.StreamBuffer[] bb : batch.columnData) {
          for (EncodedColumnBatch.StreamBuffer b : bb) {
            dataToDiscard.add(b);
          }
        }
        batch.columnData = null;
      }
    }
    for (EncodedColumnBatch.StreamBuffer data : dataToDiscard) {
      if (data.decRef() == 0) {
        upstreamFeedback.returnData(data);
      }
    }
  }

  @Override
  public void stop() {
    upstreamFeedback.stop();
    dicardPendingData(true);
  }

  @Override
  public void pause() {
    // We are just a relay; send pause to encoded data producer.
    upstreamFeedback.pause();
  }

  @Override
  public void unpause() {
    // We are just a relay; send unpause to encoded data producer.
    upstreamFeedback.unpause();
  }
}
