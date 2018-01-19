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
package org.apache.hadoop.hive.llap.io.decode;

import java.util.concurrent.Callable;

import org.apache.hadoop.hive.common.Pool;
import org.apache.hadoop.hive.common.io.encoded.EncodedColumnBatch;
import org.apache.hadoop.hive.llap.ConsumerFeedback;
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.io.api.impl.ColumnVectorBatch;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonIOMetrics;
import org.apache.hadoop.hive.ql.io.orc.encoded.Consumer;
import org.apache.hadoop.hive.ql.io.orc.encoded.IoTrace;
import org.apache.hive.common.util.FixedSizedObjectPool;
import org.apache.orc.TypeDescription;

public abstract class EncodedDataConsumer<BatchKey, BatchType extends EncodedColumnBatch<BatchKey>>
  implements Consumer<BatchType>, ReadPipeline {
  private volatile boolean isStopped = false;
  private ConsumerFeedback<BatchType> upstreamFeedback;
  private final Consumer<ColumnVectorBatch> downstreamConsumer;
  private Callable<Void> readCallable;
  private final LlapDaemonIOMetrics ioMetrics;
  // Note that the pool is per EDC - within EDC, CVBs are expected to have the same schema.
  private static final int CVB_POOL_SIZE = 128;
  protected final FixedSizedObjectPool<ColumnVectorBatch> cvbPool;

  public EncodedDataConsumer(Consumer<ColumnVectorBatch> consumer, final int colCount,
      LlapDaemonIOMetrics ioMetrics) {
    this.downstreamConsumer = consumer;
    this.ioMetrics = ioMetrics;
    cvbPool = new FixedSizedObjectPool<ColumnVectorBatch>(CVB_POOL_SIZE,
        new Pool.PoolObjectHelper<ColumnVectorBatch>() {
          @Override
          public ColumnVectorBatch create() {
            return new ColumnVectorBatch(colCount);
          }
          @Override
          public void resetBeforeOffer(ColumnVectorBatch t) {
            // Don't reset anything, we are reusing column vectors.
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
  public void consumeData(BatchType data) throws InterruptedException {
    if (isStopped) {
      returnSourceData(data);
      return;
    }
    long start = System.currentTimeMillis();
    try {
      decodeBatch(data, downstreamConsumer);
    } catch (Throwable ex) {
      // This probably should not happen; but it does... at least also stop the consumer.
      LlapIoImpl.LOG.error("decodeBatch threw", ex);
      downstreamConsumer.setError(ex);
      throw ex;
    } finally {
      long end = System.currentTimeMillis();
      ioMetrics.addDecodeBatchTime(end - start);
    }
    returnSourceData(data);
  }

  /**
   * Returns the ECB to caller for reuse. Only safe to call if the thread is the only owner
   * of the ECB in question; or, if ECB is still in pendingData, pendingData must be locked.
   */
  private void returnSourceData(BatchType data) {
    upstreamFeedback.returnData(data);
  }

  protected abstract void decodeBatch(BatchType batch,
      Consumer<ColumnVectorBatch> downstreamConsumer) throws InterruptedException;

  @Override
  public void setDone() throws InterruptedException {
    downstreamConsumer.setDone();
  }

  @Override
  public void setError(Throwable t) throws InterruptedException {
    downstreamConsumer.setError(t);
  }

  @Override
  public void returnData(ColumnVectorBatch data) {
    cvbPool.offer(data);
  }

  @Override
  public void stop() {
    upstreamFeedback.stop();
    this.isStopped = true;
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
