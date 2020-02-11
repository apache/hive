/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.daemon.impl;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.llap.counters.LlapIOCounters;
import org.apache.hadoop.hive.llap.io.encoded.TezCounterSource;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.common.counters.FileSystemCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.runtime.task.TaskRunner2Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Custom thread pool implementation that records per thread file system statistics in TezCounters.
 * The way it works is we capture before and after snapshots of file system thread statistics,
 * compute the delta difference in statistics and update them in tez task counters.
 */
public class StatsRecordingThreadPool extends ThreadPoolExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(StatsRecordingThreadPool.class);
  // uncaught exception handler that will be set for all threads before execution
  private Thread.UncaughtExceptionHandler uncaughtExceptionHandler;
  private final ThreadMXBean mxBean;

  public StatsRecordingThreadPool(final int corePoolSize, final int maximumPoolSize,
      final long keepAliveTime,
      final TimeUnit unit,
      final BlockingQueue<Runnable> workQueue,
      final ThreadFactory threadFactory) {
    this(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, null);
  }

  public StatsRecordingThreadPool(final int corePoolSize, final int maximumPoolSize,
      final long keepAliveTime,
      final TimeUnit unit,
      final BlockingQueue<Runnable> workQueue,
      final ThreadFactory threadFactory, Thread.UncaughtExceptionHandler handler) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    this.uncaughtExceptionHandler = handler;
    this.mxBean = LlapUtil.initThreadMxBean();
  }

  @Override
  protected <T> RunnableFuture<T> newTaskFor(final Callable<T> callable) {
    return new FutureTask(new WrappedCallable(callable, uncaughtExceptionHandler, mxBean));
  }

  public void setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler handler) {
    this.uncaughtExceptionHandler = handler;
  }

  /**
   * Callable that wraps the actual callable submitted to the thread pool and invokes completion
   * listener in finally block.
   *
   * @param <V> - actual callable
   */
  private static class WrappedCallable<V> implements Callable<V> {
    private Callable<V> actualCallable;
    private Thread.UncaughtExceptionHandler uncaughtExceptionHandler;
    private ThreadMXBean mxBean;

    WrappedCallable(final Callable<V> callable,
        final Thread.UncaughtExceptionHandler uncaughtExceptionHandler, ThreadMXBean mxBean) {
      this.actualCallable = callable;
      this.uncaughtExceptionHandler = uncaughtExceptionHandler;
      this.mxBean = mxBean;
    }

    @Override
    public V call() throws Exception {
      Thread thread = Thread.currentThread();

      // setup uncaught exception handler for the current thread
      if (uncaughtExceptionHandler != null) {
        thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
      }

      // clone thread local file system statistics
      List<LlapUtil.StatisticsData> statsBefore = LlapUtil.cloneThreadLocalFileSystemStatistics();
      long cpuTime = mxBean == null ? -1 : mxBean.getCurrentThreadCpuTime(),
          userTime = mxBean == null ? -1 : mxBean.getCurrentThreadUserTime();
      setupMDCFromNDC(actualCallable);
      try {
        return actualCallable.call();
      } finally {
        if (mxBean != null) {
          cpuTime = mxBean.getCurrentThreadCpuTime() - cpuTime;
          userTime = mxBean.getCurrentThreadUserTime() - userTime;
        }
        updateCounters(statsBefore, actualCallable, cpuTime, userTime);

        MDC.clear();
      }
    }

    private void setupMDCFromNDC(final Callable<V> actualCallable) {
      if (actualCallable instanceof CallableWithNdc) {
        CallableWithNdc callableWithNdc = (CallableWithNdc) actualCallable;
        try {
          // CallableWithNdc inherits from NDC only when call() is invoked. CallableWithNdc has to
          // extended to provide access to its ndcStack that is cloned during creation. Until, then
          // we will use reflection to access the private field.
          // FIXME: HIVE-14243 follow to remove this reflection
          Field field = callableWithNdc.getClass().getSuperclass().getDeclaredField("ndcStack");
          field.setAccessible(true);
          Stack ndcStack = (Stack) field.get(callableWithNdc);

          final Stack clonedStack = (Stack) ndcStack.clone();
          final String fragmentId = (String) clonedStack.pop();
          final String queryId = (String) clonedStack.pop();
          final String dagId = (String) clonedStack.pop();
          MDC.put("dagId", dagId);
          MDC.put("queryId", queryId);
          MDC.put("fragmentId", fragmentId);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Received dagId: {} queryId: {} instanceType: {}",
                dagId, queryId, actualCallable.getClass().getSimpleName());
          }
        } catch (Exception e) {
          LOG.warn("Not setting up MDC as NDC stack cannot be accessed reflectively for" +
                  " instance type: {} exception type: {}",
              actualCallable.getClass().getSimpleName(), e.getClass().getSimpleName());
        }
      } else {
        LOG.warn("Not setting up MDC as unknown callable instance type received: {}",
            actualCallable.getClass().getSimpleName());
      }
    }

    /**
     * LLAP IO related counters.
     */
    public enum LlapExecutorCounters {
      EXECUTOR_CPU_NS,
      EXECUTOR_USER_NS;

    }

    private void updateCounters(final List<LlapUtil.StatisticsData> statsBefore,
        final Callable<V> actualCallable, long cpuTime, long userTime) {
      Thread thread = Thread.currentThread();
      TezCounters tezCounters = null;
      // add tez counters for task execution and llap io
      if (actualCallable instanceof TaskRunner2Callable) {
        TaskRunner2Callable taskRunner2Callable = (TaskRunner2Callable) actualCallable;
        // counters for task execution side
        tezCounters = taskRunner2Callable.addAndGetTezCounter(FileSystemCounter.class.getName());
      } else if (actualCallable instanceof TezCounterSource) {
        // Other counter sources (currently used in LLAP IO).
        tezCounters = ((TezCounterSource) actualCallable).getTezCounters();
      } else {
        LOG.warn("Unexpected callable {}; cannot get counters", actualCallable);
      }

      if (tezCounters != null) {
        if (cpuTime >= 0 && userTime >= 0) {
          tezCounters.findCounter(LlapExecutorCounters.EXECUTOR_CPU_NS).increment(cpuTime);
          tezCounters.findCounter(LlapExecutorCounters.EXECUTOR_USER_NS).increment(userTime);
        }
        if (statsBefore != null) {
          // if there are multiple stats for the same scheme (from different NameNode), this
          // method will squash them together
          Map<String, FileSystem.Statistics> schemeToStats = LlapUtil
              .getCombinedFileSystemStatistics();
          for (Map.Entry<String, FileSystem.Statistics> entry : schemeToStats.entrySet()) {
            final String scheme = entry.getKey();
            FileSystem.Statistics statistics = entry.getValue();
            FileSystem.Statistics.StatisticsData threadFSStats = statistics
                .getThreadStatistics();
            List<LlapUtil.StatisticsData> allStatsBefore = LlapUtil
                .getStatisticsForScheme(scheme, statsBefore);
            long bytesReadDelta = 0;
            long bytesWrittenDelta = 0;
            long readOpsDelta = 0;
            long largeReadOpsDelta = 0;
            long writeOpsDelta = 0;
            // there could be more scheme after execution as execution might be accessing a
            // different filesystem. So if we don't find a matching scheme before execution we
            // just use the after execution values directly without computing delta difference
            if (allStatsBefore != null && !allStatsBefore.isEmpty()) {
              for (LlapUtil.StatisticsData sb : allStatsBefore) {
                bytesReadDelta += threadFSStats.getBytesRead() - sb.getBytesRead();
                bytesWrittenDelta += threadFSStats.getBytesWritten() - sb.getBytesWritten();
                readOpsDelta += threadFSStats.getReadOps() - sb.getReadOps();
                largeReadOpsDelta += threadFSStats.getLargeReadOps() - sb.getLargeReadOps();
                writeOpsDelta += threadFSStats.getWriteOps() - sb.getWriteOps();
              }
            } else {
              bytesReadDelta = threadFSStats.getBytesRead();
              bytesWrittenDelta = threadFSStats.getBytesWritten();
              readOpsDelta = threadFSStats.getReadOps();
              largeReadOpsDelta = threadFSStats.getLargeReadOps();
              writeOpsDelta = threadFSStats.getWriteOps();
            }
            tezCounters.findCounter(scheme, FileSystemCounter.BYTES_READ)
                .increment(bytesReadDelta);
            tezCounters.findCounter(scheme, FileSystemCounter.BYTES_WRITTEN)
                .increment(bytesWrittenDelta);
            tezCounters.findCounter(scheme, FileSystemCounter.READ_OPS).increment(readOpsDelta);
            tezCounters.findCounter(scheme, FileSystemCounter.LARGE_READ_OPS)
                .increment(largeReadOpsDelta);
            tezCounters.findCounter(scheme, FileSystemCounter.WRITE_OPS)
                .increment(writeOpsDelta);

            if (LOG.isDebugEnabled()) {
              LOG.debug("Updated stats: instance: {} thread name: {} thread id: {} scheme: {} " +
                      "bytesRead: {} bytesWritten: {} readOps: {} largeReadOps: {} writeOps: {}",
                  actualCallable.getClass().getSimpleName(), thread.getName(), thread.getId(),
                  scheme, bytesReadDelta, bytesWrittenDelta, readOpsDelta, largeReadOpsDelta,
                  writeOpsDelta);
            }
          }
        } else {
          LOG.warn("File system statistics snapshot before execution of thread is null." +
                  "Thread name: {} id: {} allStats: {}", thread.getName(), thread.getId(),
              statsBefore);
        }
      } else {
        LOG.warn("TezCounters is null for callable type: {}",
            actualCallable.getClass().getSimpleName());
      }
    }
  }
}
