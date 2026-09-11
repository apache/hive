/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.txn.service;

import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetastoreTaskThread;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.txn.NoMutex;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Metastore background thread that drives {@link TxnStore#performDeadlockDetection()} on
 * the configured interval, gated by the {@code DeadlockDetector} mutex.
 */
public class DeadlockDetectorService implements MetastoreTaskThread {

  private static final Logger LOG = LoggerFactory.getLogger(DeadlockDetectorService.class);

  private Configuration conf;
  private TxnStore txnHandler;
  private boolean shouldUseMutex = true;

  @Override
  public void setConf(Configuration configuration) {
    conf = configuration;
    txnHandler = TxnUtils.getTxnStore(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public long runFrequency(TimeUnit unit) {
    return MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.TXN_DEADLOCK_DETECTOR_INTERVAL, unit);
  }

  @Override
  public void run() {
    if (!MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.TXN_DEADLOCK_DETECTOR_ENABLED)) {
      return;
    }
    TxnStore.MutexAPI mutex = shouldUseMutex ? txnHandler.getMutexAPI() : new NoMutex();
    Timer scanTimer = Metrics.getOrCreateTimer(MetricsConstants.DEADLOCK_DETECTOR_SCAN_DURATION);
    try (AutoCloseable ignored = mutex.acquireLock(TxnStore.MUTEX_KEY.DeadlockDetector.name())) {
      // Time only the scan itself; mutex-wait must not pollute the duration histogram.
      Timer.Context timerCtx = scanTimer != null ? scanTimer.time() : null;
      try {
        long start = System.currentTimeMillis();
        int aborted = txnHandler.performDeadlockDetection();
        long durationMs = System.currentTimeMillis() - start;
        if (aborted > 0) {
          LOG.info("Deadlock detector aborted {} txn(s) in {} ms.", aborted, durationMs);
        } else {
          LOG.debug("Deadlock detector found no cycles. Took {} ms.", durationMs);
        }
      } finally {
        if (timerCtx != null) {
          timerCtx.stop();
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Deadlock detector thread interrupted: {}",
          Thread.currentThread().getName(), e);
    } catch (Throwable e) {
      // Catch Throwable: ScheduledThreadPoolExecutor cancels the periodic task on any
      // uncaught Throwable, which would silently disable detection until restart. Bump
      // the scan-failures counter here too so a broken detector is observable in metrics
      // and not just logs.
      LOG.error("Unexpected throwable in thread: {}, message: {}",
          Thread.currentThread().getName(), String.valueOf(e), e);
      Metrics.getOrCreateCounter(MetricsConstants.TOTAL_NUM_DEADLOCK_DETECTOR_SCAN_FAILURES).inc();
    }
  }

  @Override
  public void enforceMutex(boolean enableMutex) {
    this.shouldUseMutex = enableMutex;
  }
}