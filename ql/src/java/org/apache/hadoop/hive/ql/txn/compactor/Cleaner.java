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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorUtil.ThrowingRunnable;
import org.apache.hadoop.hive.ql.txn.compactor.handler.CleaningRequestHandler;
import org.apache.hadoop.hive.ql.txn.compactor.handler.CleaningRequestHandlerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hive.conf.Constants.COMPACTOR_CLEANER_THREAD_NAME_FORMAT;

/**
 * A class to clean directories after compactions.  This will run in a separate thread.
 */
public class Cleaner extends MetaStoreCompactorThread {

  static final private String CLASS_NAME = Cleaner.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  private boolean metricsEnabled = false;

  private ExecutorService cleanerExecutor;
  private List<CleaningRequestHandler> cleaningRequestHandlers;
  private FSRemover fsRemover;

  public Cleaner() {
  }

  public Cleaner(List<CleaningRequestHandler> cleaningRequestHandlers) {
    this.cleaningRequestHandlers = cleaningRequestHandlers;
  }

  @Override
  public void init(AtomicBoolean stop) throws Exception {
    super.init(stop);
    checkInterval = conf.getTimeVar(
            HiveConf.ConfVars.HIVE_COMPACTOR_CLEANER_RUN_INTERVAL, TimeUnit.MILLISECONDS);
    cleanerExecutor = CompactorUtil.createExecutorWithThreadFactory(
            conf.getIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_CLEANER_THREADS_NUM),
            COMPACTOR_CLEANER_THREAD_NAME_FORMAT);
    metricsEnabled = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED) &&
        MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_EXT_ON);
    if (cleaningRequestHandlers == null || cleaningRequestHandlers.isEmpty()) {
      cleaningRequestHandlers = CleaningRequestHandlerFactory.getInstance().getHandlers(conf, txnHandler, metricsEnabled);
    }
    fsRemover = new FSRemover(conf, cleaningRequestHandlers, ReplChangeManager.getInstance(conf));
  }

  @Override
  public void run() {
    LOG.info("Starting Cleaner thread");
    try {
      do {
        TxnStore.MutexAPI.LockHandle handle = null;
        cleaningRequestHandlers.forEach(CleaningRequestHandler::invalidateMetaCache);
        long startedAt = -1;

        // Make sure nothing escapes this run method and kills the metastore at large,
        // so wrap it in a big catch Throwable statement.
        try {
          handle = txnHandler.getMutexAPI().acquireLock(TxnStore.MUTEX_KEY.Cleaner.name());
          startedAt = System.currentTimeMillis();

          if (metricsEnabled) {
            stopCycleUpdater();
            startCycleUpdater(HiveConf.getTimeVar(conf,
                    HiveConf.ConfVars.HIVE_COMPACTOR_CLEANER_DURATION_UPDATE_INTERVAL, TimeUnit.MILLISECONDS),
                    new CleanerCycleUpdater(MetricsConstants.COMPACTION_CLEANER_CYCLE_DURATION, startedAt));
          }

          for (CleaningRequestHandler cleaningRequestHandler : cleaningRequestHandlers) {
            try {
              List<CleaningRequest> readyToClean = cleaningRequestHandler.findReadyToClean();
              checkInterrupt();

              if (!readyToClean.isEmpty()) {
                List<CompletableFuture<Void>> cleanerList = new ArrayList<>();
                for (CleaningRequest cr : readyToClean) {

                  //Check for interruption before scheduling each cleaning request and return if necessary
                  checkInterrupt();

                  CompletableFuture<Void> asyncJob = CompletableFuture.runAsync(
                                  ThrowingRunnable.unchecked(() -> fsRemover.clean(cr)), cleanerExecutor)
                          .exceptionally(t -> {
                            LOG.error("Error clearing: {}", cr.getFullPartitionName(), t);
                            return null;
                          });
                  cleanerList.add(asyncJob);
                }

                //Use get instead of join, so we can receive InterruptedException and shutdown gracefully
                CompletableFuture.allOf(cleanerList.toArray(new CompletableFuture[0])).get();
              }
            } catch (InterruptedException e) {
              return;
            } catch (Throwable t) {
              LOG.error("Caught an exception while executing cleaningRequestHandler loop : {} of compactor cleaner, {}",
                      cleaningRequestHandler.getClass().getName(), StringUtils.stringifyException(t));
            }
          }
        } catch (Throwable t) {
          LOG.error("Caught an exception in the main loop of compactor cleaner, " +
              StringUtils.stringifyException(t));
        } finally {
          if (handle != null) {
            handle.releaseLocks();
          }
          if (metricsEnabled) {
            updateCycleDurationMetric(MetricsConstants.COMPACTION_CLEANER_CYCLE_DURATION, startedAt);
          }
          stopCycleUpdater();
        }
        // Now, go back to bed until it's time to do this again
        doPostLoopActions(System.currentTimeMillis() - startedAt);
      } while (!stop.get());
    } catch (InterruptedException ie) {
      LOG.error("Compactor cleaner thread interrupted, exiting " +
        StringUtils.stringifyException(ie));
    } finally {
      if (Thread.currentThread().isInterrupted()) {
        LOG.info("Interrupt received, Cleaner is shutting down.");
      }
      if (cleanerExecutor != null) {
        cleanerExecutor.shutdownNow();
      }
    }
  }

  private static class CleanerCycleUpdater implements Runnable {
    private final String metric;
    private final long startedAt;

    CleanerCycleUpdater(String metric, long startedAt) {
      this.metric = metric;
      this.startedAt = startedAt;
    }

    @Override
    public void run() {
      updateCycleDurationMetric(metric, startedAt);
    }
  }
}
