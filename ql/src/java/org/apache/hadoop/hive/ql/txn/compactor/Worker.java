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

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.HiveMetaStoreUtils;
import org.apache.hadoop.hive.metastore.MetaStoreThread;
import org.apache.hadoop.hive.metastore.api.FindNextCompactRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.txn.TxnErrorMsg;
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.txn.compactor.service.CompactionService;
import org.apache.hadoop.hive.ql.txn.compactor.service.CompactionExecutorFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A class to do compactions.  This will run in a separate thread.  It will spin on the
 * compaction queue and look for new work to do.
 */
public class Worker extends RemoteCompactorThread implements MetaStoreThread {
  static final private String CLASS_NAME = Worker.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  private static long SLEEP_TIME_MAX;
  static private long SLEEP_TIME;

  private String workerName;
  private final CompactorFactory compactorFactory;

  public Worker() {
    compactorFactory = CompactorFactory.getInstance();
  }

  public Worker(CompactorFactory compactorFactory) {
    this.compactorFactory = compactorFactory;
  }

  static StatsUpdater statsUpdater = new StatsUpdater();

  // TODO: this doesn't check if compaction is already running (even though Initiator does but we
  //  don't go through Initiator for user initiated compactions)
  @Override
  public void run() {
    LOG.info("Starting Worker thread");
    boolean genericStats = conf.getBoolVar(HiveConf.ConfVars.HIVE_COMPACTOR_GATHER_STATS);
    boolean mrStats = conf.getBoolVar(HiveConf.ConfVars.HIVE_MR_COMPACTOR_GATHER_STATS);
    long timeout = conf.getTimeVar(HiveConf.ConfVars.HIVE_COMPACTOR_WORKER_TIMEOUT, TimeUnit.MILLISECONDS);
    long nextSleep = SLEEP_TIME;
    boolean launchedJob;
    ExecutorService executor = getTimeoutHandlingExecutor();
    try {
      do {
        long startedAt = System.currentTimeMillis();
        boolean err = false;
        launchedJob = false;
        Future<Boolean> singleRun = executor.submit(() -> findNextCompactionAndExecute(genericStats, mrStats));
        try {
          launchedJob = singleRun.get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException te) {
          LOG.info("Timeout during executing compaction", te);
          // Cancel the job, and recreate the Executor as well, so we can be sure that we have an available thread
          // even if we can not interrupt the task somehow. (Trade possible resource hogging for compactor stability)
          singleRun.cancel(true);
          executor.shutdownNow();
          executor = getTimeoutHandlingExecutor();
          err = true;
        } catch (ExecutionException e) {
          LOG.info("Exception during executing compaction", e);
          err = true;
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        } catch (Throwable t) {
          err = true;
        }

        doPostLoopActions(System.currentTimeMillis() - startedAt);

        // If we didn't try to launch a job it either means there was no work to do or we got
        // here as the result of an error like communication failure with the DB, schema failures etc.  Either way we want to wait
        // a bit before, otherwise we can start over the loop immediately.
        if ((!launchedJob || err) && !stop.get()) {
          Thread.sleep(nextSleep);
        }
        //Backoff mechanism
        //Increase sleep time if error persist
        //Reset sleep time to default once error is resolved
        nextSleep = (err) ? nextSleep * 2 : SLEEP_TIME;
        if (nextSleep > SLEEP_TIME_MAX) nextSleep = SLEEP_TIME_MAX;

      } while (!stop.get());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Throwable t) {
      LOG.error("Caught an exception in the main loop of compactor worker, exiting.", t);
    } finally {
      if (Thread.currentThread().isInterrupted()) {
        LOG.info("Interrupt received, Worker is shutting down.");
      }
      executor.shutdownNow();
      if (msc != null) {
        msc.close();
      }
    }
  }

  @Override
  public void init(AtomicBoolean stop) throws Exception {
    super.init(stop);
    SLEEP_TIME = conf.getTimeVar(HiveConf.ConfVars.HIVE_COMPACTOR_WORKER_SLEEP_TIME, TimeUnit.MILLISECONDS);
    SLEEP_TIME_MAX = conf.getTimeVar(HiveConf.ConfVars.HIVE_COMPACTOR_WORKER_MAX_SLEEP_TIME, TimeUnit.MILLISECONDS);
    this.workerName = getWorkerId();
    setName(workerName);
  }

  /**
   * Creates a single threaded executor used for handling timeouts.
   * The thread settings are inherited from the current thread.
   * @return Single threaded executor service to be used for timeout handling
   */
  private ExecutorService getTimeoutHandlingExecutor() {
    return Executors.newSingleThreadExecutor((r) -> {
      Thread masterThread = Thread.currentThread();
      Thread t = new Thread(masterThread.getThreadGroup(), r, masterThread.getName() + "_timeout_executor");
      t.setDaemon(masterThread.isDaemon());
      t.setPriority(masterThread.getPriority());
      return t;
    });
  }

  /**
   * Finds the next compaction and executes it. The main thread might interrupt the execution of this method
   * in case of timeout.
   * @param collectGenericStats If true then for both MR and Query based compaction the stats are regenerated
   * @param collectMrStats If true then for MR compaction the stats are regenerated
   * @return Returns true, if there was compaction in the queue, and we started working on it.
   */
  @VisibleForTesting
  protected Boolean findNextCompactionAndExecute(boolean collectGenericStats, boolean collectMrStats) {
    // Make sure nothing escapes this run method and kills the metastore at large,
    // so wrap it in a big catch Throwable statement.
    PerfLogger perfLogger = SessionState.getPerfLogger(false);
    String workerMetric = null;
    CompactionInfo ci = null;
    Table table = null;
    CompactionService compactionService = null;
    boolean compactionResult = false;

    // If an exception is thrown in the try-with-resources block below, msc is closed and nulled, so a new instance
    // is need to be obtained here.
    if (msc == null) {
      try {
        msc = HiveMetaStoreUtils.getHiveMetastoreClient(conf);
      } catch (Exception e) {
        LOG.error("Failed to connect to HMS", e);
        return false;
      }
    }

    try {

      FindNextCompactRequest findNextCompactRequest = new FindNextCompactRequest();
      findNextCompactRequest.setWorkerId(workerName);
      findNextCompactRequest.setWorkerVersion(runtimeVersion);
      findNextCompactRequest.setPoolName(this.getPoolName());
      ci = CompactionInfo.optionalCompactionInfoStructToInfo(msc.findNextCompact(findNextCompactRequest));
      LOG.info("Processing compaction request {}", ci);

      if (ci == null) {
        return false;
      }
      if ((runtimeVersion == null && ci.initiatorVersion != null) || (runtimeVersion != null && !runtimeVersion.equals(ci.initiatorVersion))) {
        LOG.warn("Worker and Initiator versions do not match. Worker: v{}, Initiator: v{}", runtimeVersion, ci.initiatorVersion);
      }

      if (StringUtils.isBlank(getPoolName()) && StringUtils.isNotBlank(ci.poolName)) {
        LOG.warn("A timed out copmaction pool entry ({}) is picked up by one of the default compaction pool workers.", ci);
      }
      if (StringUtils.isNotBlank(getPoolName()) && StringUtils.isNotBlank(ci.poolName) && !getPoolName().equals(ci.poolName)) {
        LOG.warn("The returned compaction request ({}) belong to a different pool. Although the worker is assigned to the {} pool," +
            " it will process the request.", ci, getPoolName());
      }
      CompactorUtil.checkInterrupt(CLASS_NAME);

      if (MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_EXT_ON)) {
        workerMetric = MetricsConstants.COMPACTION_WORKER_CYCLE + "_" +
            (ci.type != null ? ci.type.toString().toLowerCase() : null);
        perfLogger.perfLogBegin(CLASS_NAME, workerMetric);
      }

      // Find the table we will be working with.
      try {
        table = resolveTable(ci);
        if (table == null) {
          ci.errorMessage = "Unable to find table " + ci.getFullTableName() + ", assuming it was dropped and moving on.";
          LOG.warn(ci.errorMessage + " Compaction info: {}", ci);
          msc.markRefused(CompactionInfo.compactionInfoToStruct(ci));
          return false;
        }
      } catch (MetaException e) {
        LOG.error("Unexpected error during resolving table. Compaction info: " + ci, e);
        ci.errorMessage = e.getMessage();
        msc.markFailed(CompactionInfo.compactionInfoToStruct(ci));
        return false;
      }

      CompactorUtil.checkInterrupt(CLASS_NAME);
      compactionService = CompactionExecutorFactory.createExecutor(conf, msc, compactorFactory, table, collectGenericStats, collectMrStats);

      try {
        compactionResult = compactionService.compact(table, ci);
      } catch (Throwable e) {
        LOG.error("Caught exception while trying to compact " + ci +
            ". Marking failed to avoid repeated failures", e);
        markFailed(ci, e.getMessage());

        if (CompactorUtil.runJobAsSelf(ci.runAs)) {
          compactionService.cleanupResultDirs(ci);
        } else {
          LOG.info("Cleaning as user " + ci.runAs);
          UserGroupInformation ugi = UserGroupInformation.createProxyUser(ci.runAs,
              UserGroupInformation.getLoginUser());

          CompactionService finalCompactionService = compactionService;
          CompactionInfo finalCi = ci;
          ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            finalCompactionService.cleanupResultDirs(finalCi);
            return null;
          });
          try {
            FileSystem.closeAllForUGI(ugi);
          } catch (IOException ex) {
            LOG.error("Could not clean up file-system handles for UGI: " + ugi, e);
          }
        }
      }
    } catch (TException | IOException t) {
      LOG.error("Caught an exception in the main loop of compactor worker " + workerName, t);

      markFailed(ci, t.getMessage());

      if (msc != null) {
        msc.close();
        msc = null;
      }
    } catch (Throwable t) {
      LOG.error("Caught an exception in the main loop of compactor worker " + workerName, t);
    } finally {
      if (workerMetric != null && MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_EXT_ON)) {
        perfLogger.perfLogEnd(CLASS_NAME, workerMetric);
      }
    }

    if (Optional.ofNullable(compactionService).map(CompactionService::isComputeStats).orElse(false)) {
      statsUpdater.gatherStats(ci, conf, CompactorUtil.runJobAsSelf(ci.runAs) ? ci.runAs : table.getOwner(),
          CompactorUtil.getCompactorJobQueueName(conf, ci, table), msc);
    }

    return compactionResult;
  }

  private void markFailed(CompactionInfo ci, String errorMessage) {
    if (ci == null) {
      LOG.warn("CompactionInfo client was null. Could not mark failed");
      return;
    }
    if (ci != null && StringUtils.isNotBlank(errorMessage)) {
      ci.errorMessage = errorMessage;
    }
    if (msc == null) {
      LOG.warn("Metastore client was null. Could not mark failed: {}", ci);
      return;
    }
    try {
      msc.markFailed(CompactionInfo.compactionInfoToStruct(ci));
    } catch (Throwable t) {
      LOG.error("Caught an exception while trying to mark compaction {} as failed: {}", ci, t);
    }
  }

  private String getWorkerId() {
    StringBuilder name = new StringBuilder(this.hostName);
    name.append("-");
    name.append(getId());
    return name.toString();
  }
}
