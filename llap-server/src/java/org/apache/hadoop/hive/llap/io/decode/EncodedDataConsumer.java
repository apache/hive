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
import java.util.concurrent.Callable;

import org.apache.hadoop.hive.llap.Consumer;
import org.apache.hadoop.hive.llap.ConsumerFeedback;
import org.apache.hadoop.hive.llap.io.api.EncodedColumnBatch;
import org.apache.hadoop.hive.llap.io.api.impl.ColumnVectorBatch;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonQueueMetrics;
import org.apache.hive.common.util.FixedSizedObjectPool;
import org.apache.hive.common.util.FixedSizedObjectPool.PoolObjectHelper;

/**
 *
 */
public abstract class EncodedDataConsumer<BatchKey, BatchType extends EncodedColumnBatch<BatchKey>>
  implements Consumer<BatchType>, ReadPipeline {
  private volatile boolean isStopped = false;
  // TODO: use array, precreate array based on metadata first? Works for ORC. For now keep dumb.
  private final HashMap<BatchKey, BatchType> pendingData = new HashMap<>();
  private ConsumerFeedback<BatchType> upstreamFeedback;
  private final Consumer<ColumnVectorBatch> downstreamConsumer;
  private Callable<Void> readCallable;
  private final LlapDaemonQueueMetrics queueMetrics;
  // TODO: if we were using Exchanger, pool would not be necessary here - it would be 1/N items
  private final static int CVB_POOL_SIZE = 8;
  // Note that the pool is per EDC - within EDC, CVBs are expected to have the same schema.
  protected final FixedSizedObjectPool<ColumnVectorBatch> cvbPool;

  public EncodedDataConsumer(Consumer<ColumnVectorBatch> consumer, final int colCount,
      LlapDaemonQueueMetrics queueMetrics) {
    this.downstreamConsumer = consumer;
    this.queueMetrics = queueMetrics;
    cvbPool = new FixedSizedObjectPool<ColumnVectorBatch>(CVB_POOL_SIZE,
        new PoolObjectHelper<ColumnVectorBatch>() {
              @Override
              public ColumnVectorBatch create() {
                return new ColumnVectorBatch(colCount);
              }
        });
  }

  public void init(ConsumerFeedback<BatchType> upstreamFeedback,
      Callable<Void> readCallable) {
    this.upstreamFeedback = upstreamFeedback;
    this.readCallable = readCallable;
  }

  @Override
  public Callable<Void> getReadCallable() {
    return readCallable;
  }

  @Override
  public void consumeData(BatchType data) {
    // TODO: data arrives in whole batches now, not in columns. We could greatly simplify this.
    BatchType targetBatch = null;
    boolean localIsStopped = false;
    Integer targetBatchVersion = null;
    synchronized (pendingData) {
      localIsStopped = isStopped;
      if (!localIsStopped) {
        targetBatch = pendingData.get(data.batchKey);
        if (targetBatch == null) {
          targetBatch = data;
          pendingData.put(data.batchKey, data);
        }
        // We have the map locked; the code the throws things away from map only bumps the version
        // under the same map lock; code the throws things away here only bumps the version when
        // the batch was taken out of the map.
        targetBatchVersion = targetBatch.version;
      }
      queueMetrics.setQueueSize(pendingData.size());
    }
    if (localIsStopped) {
      returnSourceData(data);
      return;
    }
    assert targetBatchVersion != null;
    synchronized (targetBatch) {
      if (targetBatch != data) {
        throw new UnsupportedOperationException("Merging is not supported");
      }
      synchronized (pendingData) {
        targetBatch = isStopped ? null : pendingData.remove(data.batchKey);
        // Check if someone already threw this away and changed the version.
        localIsStopped = (targetBatchVersion != targetBatch.version);
      }
      // We took the batch out of the map. No more contention with stop possible.
    }
    if (localIsStopped && (targetBatch != data)) {
      returnSourceData(data);
      return;
    }
    long start = System.currentTimeMillis();
    decodeBatch(targetBatch, downstreamConsumer);
    long end = System.currentTimeMillis();
    queueMetrics.addProcessingTime(end - start);
    returnSourceData(targetBatch);
  }

  /**
   * Returns the ECB to caller for reuse. Only safe to call if the thread is the only owner
   * of the ECB in question; or, if ECB is still in pendingData, pendingData must be locked.
   */
  private void returnSourceData(BatchType data) {
    ++data.version;
    upstreamFeedback.returnData(data);
  }

  protected abstract void decodeBatch(BatchType batch,
      Consumer<ColumnVectorBatch> downstreamConsumer);

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
    cvbPool.offer(data);
  }

  private void dicardPendingData(boolean isStopped) {
    List<BatchType> batches = new ArrayList<BatchType>(
        pendingData.size());
    synchronized (pendingData) {
      if (isStopped) {
        this.isStopped = true;
      }
      for (BatchType ecb : pendingData.values()) {
        ++ecb.version;
        batches.add(ecb);
      }
      pendingData.clear();
    }
    for (BatchType batch : batches) {
      upstreamFeedback.returnData(batch);
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
