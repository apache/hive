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
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.ServerUtils;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.metrics.PerfLogger;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnCommonUtils;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.conf.Constants.COMPACTOR_INTIATOR_THREAD_NAME_FORMAT;

/**
 * A class to initiate compactions.  This will run in a separate thread.
 * It's critical that there exactly 1 of these in a given warehouse.
 */
public class Initiator extends InitiatorBase {
  static final private String CLASS_NAME = Initiator.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  private ExecutorService compactionExecutor;

  private boolean metricsEnabled;

  @Override
  public void run() {
    LOG.info("Starting Initiator thread");
    // Make sure nothing escapes this run method and kills the metastore at large,
    // so wrap it in a big catch Throwable statement.
    try {
      recoverFailedCompactions(false);

      int abortedThreshold = HiveConf.getIntVar(conf,
          HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD);
      long abortedTimeThreshold = HiveConf
          .getTimeVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_TIME_THRESHOLD,
              TimeUnit.MILLISECONDS);

      // Make sure we run through the loop once before checking to stop as this makes testing
      // much easier.  The stop value is only for testing anyway and not used when called from
      // HiveMetaStore.
      do {
        PerfLogger perfLogger = PerfLogger.getPerfLogger(false);
        long startedAt = -1;
        long prevStart;
        TxnStore.MutexAPI.LockHandle handle = null;
        boolean exceptionally = false;

        // Wrap the inner parts of the loop in a catch throwable so that any errors in the loop
        // don't doom the entire thread.
        try {
          handle = txnHandler.getMutexAPI().acquireLock(TxnStore.MUTEX_KEY.Initiator.name());
          startedAt = System.currentTimeMillis();
          prevStart = handle.getLastUpdateTime();

          if (metricsEnabled) {
            perfLogger.perfLogBegin(CLASS_NAME, MetricsConstants.COMPACTION_INITIATOR_CYCLE);
            stopCycleUpdater();
            startCycleUpdater(HiveConf.getTimeVar(conf,
                    HiveConf.ConfVars.HIVE_COMPACTOR_INITIATOR_DURATION_UPDATE_INTERVAL, TimeUnit.MILLISECONDS),
                new InitiatorCycleUpdater(MetricsConstants.COMPACTION_INITIATOR_CYCLE_DURATION,
                    startedAt,
                    MetastoreConf.getTimeVar(conf,
                        MetastoreConf.ConfVars.COMPACTOR_LONG_RUNNING_INITIATOR_THRESHOLD_WARNING,
                        TimeUnit.MILLISECONDS),
                    MetastoreConf.getTimeVar(conf,
                        MetastoreConf.ConfVars.COMPACTOR_LONG_RUNNING_INITIATOR_THRESHOLD_ERROR,
                        TimeUnit.MILLISECONDS)));
          }

          final ShowCompactResponse currentCompactions = txnHandler.showCompact(new ShowCompactRequest());

          CompactorUtil.checkInterrupt(CLASS_NAME);

          // Currently we invalidate all entries after each cycle, because the bootstrap replication is marked via
          // table property hive.repl.first.inc.pending which would be cached.
          metadataCache.invalidate();
          Set<String> skipDBs = Sets.newConcurrentHashSet();
          Set<String> skipTables = Sets.newConcurrentHashSet();

          Set<CompactionInfo> potentials = compactionExecutor.submit(() ->
            txnHandler.findPotentialCompactions(abortedThreshold, abortedTimeThreshold, prevStart)
              .parallelStream()
              .filter(ci -> isEligibleForCompaction(ci, currentCompactions, skipDBs, skipTables))
              .collect(Collectors.toSet())).get();
          LOG.debug("Found {} potential compactions, checking to see if we should compact any of them", potentials.size());

          CompactorUtil.checkInterrupt(CLASS_NAME);

          Map<String, String> tblNameOwners = new HashMap<>();
          List<CompletableFuture<Void>> compactionList = new ArrayList<>();

          if (!potentials.isEmpty()) {
            ValidTxnList validTxnList = TxnCommonUtils.createValidReadTxnList(
                txnHandler.getOpenTxns(), 0);
            conf.set(ValidTxnList.VALID_TXNS_KEY, validTxnList.writeToString());
          }

          for (CompactionInfo ci : potentials) {
            try {
              //Check for interruption before scheduling each compactionInfo and return if necessary
              if (Thread.currentThread().isInterrupted()) {
                return;
              }

              Table t = metadataCache.computeIfAbsent(ci.getFullTableName(), () -> resolveTable(ci));
              ci.poolName = getPoolName(ci, t);
              Partition p = resolvePartition(ci);
              if (p == null && ci.partName != null) {
                LOG.info("Can't find partition " + ci.getFullPartitionName() +
                    ", assuming it has been dropped and moving on.");
                continue;
              }
              String runAs = resolveUserToRunAs(tblNameOwners, t, p);
              /* checkForCompaction includes many file metadata checks and may be expensive.
               * Therefore, using a thread pool here and running checkForCompactions in parallel */
              String tableName = ci.getFullTableName();
              String partition = ci.getFullPartitionName();

              CompletableFuture<Void> asyncJob =
                  CompletableFuture.runAsync(
                          CompactorUtil.ThrowingRunnable.unchecked(() ->
                              scheduleCompactionIfRequired(ci, t, p, runAs, metricsEnabled)), compactionExecutor)
                      .exceptionally(exc -> {
                        LOG.error("Error while running scheduling the compaction on the table {} / partition {}.", tableName, partition, exc);
                        return null;
                      });
              compactionList.add(asyncJob);
            } catch (Throwable t) {
              LOG.error("Caught exception while trying to determine if we should compact {}. " +
                  "Marking failed to avoid repeated failures, {}", ci, t);
              ci.errorMessage = t.getMessage();
              txnHandler.markFailed(ci);
            }
          }

          //Use get instead of join, so we can receive InterruptedException and shutdown gracefully
          CompletableFuture.allOf(compactionList.toArray(new CompletableFuture[0])).get();

          // Check for timed out remote workers.
          recoverFailedCompactions(true);
        } catch (InterruptedException e) {
          // do not ignore interruption requests
          return;
        } catch (Throwable t) {
          LOG.error("Initiator loop caught unexpected exception this time through the loop", t);
          exceptionally = true;
        } finally {
          if (handle != null) {
            if (!exceptionally) handle.releaseLocks(startedAt); else handle.releaseLocks();
          }
          if (metricsEnabled) {
            perfLogger.perfLogEnd(CLASS_NAME, MetricsConstants.COMPACTION_INITIATOR_CYCLE);
            updateCycleDurationMetric(MetricsConstants.COMPACTION_INITIATOR_CYCLE_DURATION, startedAt);
          }
          stopCycleUpdater();
        }

        doPostLoopActions(System.currentTimeMillis() - startedAt);
      } while (!stop.get());
    } catch (Throwable t) {
      LOG.error("Caught an exception in the main loop of compactor initiator, exiting.", t);
    } finally {
      if (Thread.currentThread().isInterrupted()) {
        LOG.info("Interrupt received, Initiator is shutting down.");
      }
      if (compactionExecutor != null) {
        compactionExecutor.shutdownNow();
      }
    }
  }

  @Override
  protected boolean isCacheEnabled() {
    return MetastoreConf.getBoolVar(conf,
            MetastoreConf.ConfVars.COMPACTOR_INITIATOR_TABLECACHE_ON);
  }

  private String getPoolName(CompactionInfo ci, Table t) throws Exception {
    Map<String, String> params = t.getParameters();
    String poolName = params == null ? null : params.get(Constants.HIVE_COMPACTOR_WORKER_POOL);
    if (StringUtils.isBlank(poolName)) {
      params = metadataCache.computeIfAbsent(ci.dbname, () -> resolveDatabase(ci)).getParameters();
      poolName = params == null ? null : params.get(Constants.HIVE_COMPACTOR_WORKER_POOL);
    }
    return poolName;
  }

  private Database resolveDatabase(CompactionInfo ci) throws MetaException, NoSuchObjectException {
    return CompactorUtil.resolveDatabase(conf, ci.dbname);
  }



  @VisibleForTesting
  protected String resolveUserToRunAs(Map<String, String> cache, Table t, Partition p)
      throws IOException, InterruptedException {
    //Figure out who we should run the file operations as
    String fullTableName = TxnUtils.getFullTableName(t.getDbName(), t.getTableName());
    StorageDescriptor sd = CompactorUtil.resolveStorageDescriptor(t, p);

    String user = cache.get(fullTableName);
    if (user == null) {
      user = TxnUtils.findUserToRunAs(sd.getLocation(), t, conf);
      cache.put(fullTableName, user);
    }
    return user;
  }

  @Override
  public void init(AtomicBoolean stop) throws Exception {
    super.init(stop);
    checkInterval = conf.getTimeVar(HiveConf.ConfVars.HIVE_COMPACTOR_CHECK_INTERVAL, TimeUnit.MILLISECONDS);
    compactionExecutor = CompactorUtil.createExecutorWithThreadFactory(
            conf.getIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_REQUEST_QUEUE),
            COMPACTOR_INTIATOR_THREAD_NAME_FORMAT);
    metricsEnabled = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED) &&
        MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_EXT_ON);
  }

  private void recoverFailedCompactions(boolean remoteOnly) throws MetaException {
    if (!remoteOnly) txnHandler.revokeFromLocalWorkers(ServerUtils.hostname());
    txnHandler.revokeTimedoutWorkers(HiveConf.getTimeVar(conf,
        HiveConf.ConfVars.HIVE_COMPACTOR_WORKER_TIMEOUT, TimeUnit.MILLISECONDS));
  }

  private boolean foundCurrentOrFailedCompactions(ShowCompactResponse compactions, CompactionInfo ci) throws MetaException {
    if (compactions.getCompacts() == null) {
      return false;
    }

    //In case of an aborted Dynamic partition insert, the created entry in the compaction queue does not contain
    //a partition name even for partitioned tables. As a result it can happen that the ShowCompactResponse contains
    //an element without partition name for partitioned tables. Therefore, it is necessary to null check the partition
    //name of the ShowCompactResponseElement even if the CompactionInfo.partName is not null. These special compaction
    //requests are skipped by the worker, and only cleaner will pick them up, so we should allow to schedule a 'normal'
    //compaction for partitions of those tables which has special (DP abort) entry with undefined partition name.
    List<ShowCompactResponseElement> filteredElements = compactions.getCompacts().stream()
      .filter(e -> e.getDbname().equals(ci.dbname)
        && e.getTablename().equals(ci.tableName)
        && (e.getPartitionname() == null && ci.partName == null ||
              (Objects.equals(e.getPartitionname(),ci.partName))))
      .collect(Collectors.toList());

    // Figure out if there are any currently running compactions on the same table or partition.
    if (filteredElements.stream().anyMatch(
        e -> TxnStore.WORKING_RESPONSE.equals(e.getState()) || TxnStore.INITIATED_RESPONSE.equals(e.getState()))) {

      LOG.info("Found currently initiated or working compaction for " +
        ci.getFullPartitionName() + " so we will not initiate another compaction");
      return true;
    }

    // Check if there is already sufficient number of consecutive failures for this table/partition
    // so that no new automatic compactions needs to be scheduled.
    int failedThreshold = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD);

    LongSummaryStatistics failedStats = filteredElements.stream()
      .filter(e -> TxnStore.SUCCEEDED_RESPONSE.equals(e.getState()) || TxnStore.FAILED_RESPONSE.equals(e.getState()))
      .sorted(Comparator.comparingLong(ShowCompactResponseElement::getId).reversed())
      .limit(failedThreshold)

      .filter(e -> TxnStore.FAILED_RESPONSE.equals(e.getState()))
      .collect(Collectors.summarizingLong(ShowCompactResponseElement::getEnqueueTime));

    // If the last attempt was too long ago, ignore the failed threshold and try compaction again
    long retryTime = MetastoreConf.getTimeVar(conf,
      MetastoreConf.ConfVars.COMPACTOR_INITIATOR_FAILED_RETRY_TIME, TimeUnit.MILLISECONDS);

    boolean needsRetry = (retryTime > 0) && (failedStats.getMax() + retryTime < System.currentTimeMillis());
    if (failedStats.getCount() == failedThreshold && !needsRetry) {
      LOG.warn("Will not initiate compaction for " + ci.getFullPartitionName() + " since last " +
        MetastoreConf.ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD + " attempts to compact it failed.");

      ci.errorMessage = "Compaction is not initiated since last " +
        MetastoreConf.ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD + " consecutive compaction attempts failed)";

      txnHandler.markFailed(ci);
      return true;
    }
    return false;
  }

  // Check if it's a dynamic partitioning case. If so, do not initiate compaction for streaming ingest, only for aborts.
  private static boolean isDynPartIngest(Table t, CompactionInfo ci){
    if (t.getPartitionKeys() != null && t.getPartitionKeys().size() > 0 &&
            ci.partName  == null && !ci.hasOldAbort) {
      LOG.info("Skipping entry for " + ci.getFullTableName() + " as it is from dynamic" +
              " partitioning");
      return  true;
    }
    return false;
  }

  private boolean isEligibleForCompaction(CompactionInfo ci,
      ShowCompactResponse currentCompactions, Set<String> skipDBs, Set<String> skipTables) {
    try {
      if (skipDBs.contains(ci.dbname)) {
        LOG.info("Skipping {}::{}, skipDBs::size:{}", ci.dbname, ci.tableName, skipDBs.size());
        return false;
      } else {
        if (replIsCompactionDisabledForDatabase(ci.dbname)) {
          skipDBs.add(ci.dbname);
          LOG.info("Skipping {} as compaction is disabled due to repl; skipDBs::size:{}",
              ci.dbname, skipDBs.size());
          return false;
        }
      }

      if (skipTables.contains(ci.getFullTableName())) {
        return false;
      }

      LOG.info("Checking to see if we should compact " + ci.getFullPartitionName());

      // Check if we have already initiated or are working on a compaction for this table/partition.
      // Also make sure we haven't exceeded configured number of consecutive failures.
      // If any of the above applies, skip it.
      // Note: if we are just waiting on cleaning we can still check, as it may be time to compact again even though we haven't cleaned.
      if (foundCurrentOrFailedCompactions(currentCompactions, ci)) {
        return false;
      }

      Table t = metadataCache.computeIfAbsent(ci.getFullTableName(), () -> resolveTable(ci));
      if (t == null) {
        LOG.info("Can't find table " + ci.getFullTableName() + ", assuming it's a temp " +
            "table or has been dropped and moving on.");
        return false;
      }

      if (replIsCompactionDisabledForTable(t)) {
        skipTables.add(ci.getFullTableName());
        return false;
      }

      Map<String, String> dbParams = metadataCache.computeIfAbsent(ci.dbname, () -> resolveDatabase(ci)).getParameters();
      if (MetaStoreUtils.isNoAutoCompactSet(dbParams, t.getParameters())) {
        if (Boolean.parseBoolean(MetaStoreUtils.getNoAutoCompact(dbParams))) {
          skipDBs.add(ci.dbname);
          LOG.info("DB " + ci.dbname + " marked " + hive_metastoreConstants.NO_AUTO_COMPACT +
              "=true so we will not compact it.");
        } else {
          skipTables.add(ci.getFullTableName());
          LOG.info("Table " + tableName(t) + " marked " + hive_metastoreConstants.NO_AUTO_COMPACT +
              "=true so we will not compact it.");
        }
        return false;
      }
      if (AcidUtils.isInsertOnlyTable(t.getParameters()) && !HiveConf
          .getBoolVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_COMPACT_MM)) {
        skipTables.add(ci.getFullTableName());
        LOG.info("Table " + tableName(t) + " is insert only and " + HiveConf.ConfVars.HIVE_COMPACTOR_COMPACT_MM.varname
            + "=false so we will not compact it.");
        return false;
      }
      if (isDynPartIngest(t, ci)) {
        return false;
      }

    } catch (Throwable e) {
      LOG.error("Caught exception while checking compaction eligibility.", e);
      try {
        ci.errorMessage = e.getMessage();
        txnHandler.markFailed(ci);
      } catch (MetaException ex) {
        LOG.error("Caught exception while marking compaction as failed.", e);
      }
      return false;
    }
    return true;
  }

  private static class InitiatorCycleUpdater implements Runnable {
    private final String metric;
    private final long startedAt;
    private final long warningThreshold;
    private final long errorThreshold;

    private boolean errorReported;
    private boolean warningReported;

    InitiatorCycleUpdater(String metric, long startedAt,
        long warningThreshold, long errorThreshold) {
      this.metric = metric;
      this.startedAt = startedAt;
      this.warningThreshold = warningThreshold;
      this.errorThreshold = errorThreshold;
    }

    @Override
    public void run() {
      long elapsed = updateCycleDurationMetric(metric, startedAt);
      if (elapsed >= errorThreshold) {
        if (!errorReported) {
          LOG.error("Long running Initiator has been detected, duration {}", elapsed);
          errorReported = true;
        }
      } else if (elapsed >= warningThreshold) {
        if (!warningReported && !errorReported) {
          warningReported = true;
          LOG.warn("Long running Initiator has been detected, duration {}", elapsed);
        }
      }
    }
  }
}
