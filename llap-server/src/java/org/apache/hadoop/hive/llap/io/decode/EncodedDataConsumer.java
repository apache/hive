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

import java.lang.management.ThreadMXBean;
import java.util.concurrent.Callable;

import org.apache.hadoop.hive.common.Pool;
import org.apache.hadoop.hive.common.io.encoded.EncodedColumnBatch;
import org.apache.hadoop.hive.llap.ConsumerFeedback;
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.llap.counters.QueryFragmentCounters;
import org.apache.hadoop.hive.llap.io.api.impl.ColumnVectorBatch;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.io.encoded.TezCounterSource;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonIOMetrics;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.io.orc.encoded.Consumer;
import org.apache.hadoop.hive.ql.io.orc.encoded.IoTrace;
import org.apache.hive.common.util.FixedSizedObjectPool;
import org.apache.orc.TypeDescription;
import org.apache.tez.common.counters.FileSystemCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.runtime.task.TaskRunner2Callable;

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
  protected final QueryFragmentCounters counters;
  private final ThreadMXBean mxBean;

  public EncodedDataConsumer(Consumer<ColumnVectorBatch> consumer, final int colCount,
      LlapDaemonIOMetrics ioMetrics, QueryFragmentCounters counters) {
    this.downstreamConsumer = consumer;
    this.ioMetrics = ioMetrics;
    this.mxBean = LlapUtil.initThreadMxBean();
    cvbPool = new FixedSizedObjectPool<>(CVB_POOL_SIZE, new Pool.PoolObjectHelper<ColumnVectorBatch>() {
      @Override public ColumnVectorBatch create() {
        return new ColumnVectorBatch(colCount);
      }

      @Override public void resetBeforeOffer(ColumnVectorBatch t) {
        // Don't reset anything, we are reusing column vectors.
      }
    });
    this.counters = counters;
  }

  // Implementing TCS is needed for StatsRecordingThreadPool.
  private class CpuRecordingCallable implements Callable<Void>, TezCounterSource {
    private final Callable<Void> readCallable;

    public CpuRecordingCallable(Callable<Void> readCallable) {
      this.readCallable = readCallable;
    }

    @Override
    public Void call() throws Exception {
      if (mxBean == null) {
        return readCallable.call();
      }
      long cpuTime = mxBean.getCurrentThreadCpuTime(),
          userTime = mxBean.getCurrentThreadUserTime();
      try {
        return readCallable.call();
      } finally {
        counters.recordThreadTimes(mxBean.getCurrentThreadCpuTime() - cpuTime,
            mxBean.getCurrentThreadUserTime() - userTime);
      }
    }

    @Override
    public TezCounters getTezCounters() {
      return (readCallable instanceof TezCounterSource)
          ? ((TezCounterSource) readCallable).getTezCounters() : null;
    }

  }

  public void init(ConsumerFeedback<BatchType> upstreamFeedback,
      Callable<Void> readCallable) {
    this.upstreamFeedback = upstreamFeedback;
    this.readCallable = mxBean == null ? readCallable : new CpuRecordingCallable(readCallable);
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
    cvbPool.clear();
  }

  @Override
  public void setError(Throwable t) throws InterruptedException {
    downstreamConsumer.setError(t);
  }

  @Override
  public void returnData(ColumnVectorBatch data) {
    //In case a writer has a lock on any of the vectors we don't return it to the pool.
    for (ColumnVector cv : data.cols) {
      if (cv != null && cv.getRef() > 0) {
        return;
      }
    }
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
