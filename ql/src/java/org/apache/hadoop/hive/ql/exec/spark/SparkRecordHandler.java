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

package org.apache.hadoop.hive.ql.exec.spark;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.Iterator;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;


public abstract class SparkRecordHandler {
  protected static final String CLASS_NAME = SparkRecordHandler.class.getName();
  protected final PerfLogger perfLogger = SessionState.getPerfLogger();
  private static final Logger LOG = LoggerFactory.getLogger(SparkRecordHandler.class);

  // used to log memory usage periodically
  private final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

  protected JobConf jc;
  protected OutputCollector<?, ?> oc;
  protected Reporter rp;
  protected boolean abort = false;
  /**
   * Using volatile for rowNumber and logThresholdInterval instead of
   *  Atomic even though they are used in non-atomic context. This is because
   *  we know that they will be updated only by a single thread at a time and
   *  there is no contention on these variables.
   */
  private volatile long rowNumber = 0;
  private volatile long logThresholdInterval = 15000;
  boolean anyRow = false;
  private final long maxLogThresholdInterval = 900000;
  // We use this ScheduledFuture while closing to cancel any logger thread that is scheduled.
  private ScheduledFuture memoryAndRowLogFuture;

  private final ScheduledThreadPoolExecutor memoryAndRowLogExecutor = getMemoryAndRowLogExecutor();

  private ScheduledThreadPoolExecutor getMemoryAndRowLogExecutor() {
    ScheduledThreadPoolExecutor executor =  new ScheduledThreadPoolExecutor(1,
        new ThreadFactoryBuilder()
            .setNameFormat("MemoryAndRowInfoLogger")
            .setDaemon(true)
            .setUncaughtExceptionHandler((Thread t, Throwable e) -> LOG.error(t + " throws exception: " + e))
            .build(),
        new ThreadPoolExecutor.DiscardPolicy());
    executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    return executor;
  }

  public <K, V> void init(JobConf job, OutputCollector<K, V> output, Reporter reporter) throws Exception {
    jc = job;
    MapredContext.init(false, new JobConf(jc));
    MapredContext.get().setReporter(reporter);

    oc = output;
    rp = reporter;

    LOG.info("maximum memory = " + memoryMXBean.getHeapMemoryUsage().getMax());
    MemoryInfoLogger memoryInfoLogger = new MemoryInfoLogger();
    memoryInfoLogger.run();
    Utilities.tryLoggingClassPaths(job, LOG);
  }

  /**
   * Process row with key and single value.
   */
  public abstract void processRow(Object key, Object value) throws IOException;

  /**
   * Process row with key and value collection.
   */
  public abstract <E> void processRow(Object key, Iterator<E> values) throws IOException;

  /**
   * Increments rowNumber to indicate # of rows processed.
   */
  void incrementRowNumber() {
    ++rowNumber;
  }

  /**
   * Logs every 'logThresholdInterval' milliseconds and doubles the
   * logThresholdInterval value after each time it logs until it
   * reaches maxLogThresholdInterval.
   * */
  class MemoryInfoLogger implements Runnable {
    @Override
    public void run() {
      if (anyRow) {
        logThresholdInterval = Math.min(maxLogThresholdInterval, 2 * logThresholdInterval);
        logMemoryInfo();
      }
      memoryAndRowLogFuture =
          memoryAndRowLogExecutor.schedule(new MemoryInfoLogger(), logThresholdInterval, TimeUnit.MILLISECONDS);
    }
  }

  public void close() {
    memoryAndRowLogExecutor.shutdown();
    memoryAndRowLogFuture.cancel(false);
    try {
      if (!memoryAndRowLogExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        memoryAndRowLogExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      memoryAndRowLogExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    }

    if (LOG.isInfoEnabled()) {
      logMemoryInfo();
    }
  }

  public abstract boolean getDone();

  /**
   * Logger information to be logged at the end.
   */
  private void logMemoryInfo() {
    long usedMemory = memoryMXBean.getHeapMemoryUsage().getUsed();
    LOG.info("Processed " + rowNumber + " rows: used memory = " + usedMemory);
  }

  public boolean isAbort() {
    return abort;
  }

  public void setAbort(boolean abort) {
    this.abort = abort;
  }
}
