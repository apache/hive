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
import org.apache.hadoop.hive.common.ServerUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.metrics.PerfLogger;
import org.apache.hadoop.hive.metastore.txn.NoMutex;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
public abstract class InitiatorBase extends MetaStoreCompactorThread {

  protected ExecutorService compactionExecutor;

  private boolean metricsEnabled;
  private boolean shouldUseMutex = true;

  protected abstract Set<CompactionInfo> findPotentialCompactions(long lastChecked, ShowCompactResponse currentCompactions,
      Set<String> skipDBs, Set<String> skipTables) throws MetaException;
  protected abstract TxnStore.MUTEX_KEY getMutexKey();
  protected abstract void invalidateCaches();
  protected abstract String getClassName();
  protected abstract Logger getLogger();
  protected abstract Partition getPartition(CompactionInfo ci) throws MetaException, SemanticException;

  @Override
  public void run() {
    getLogger().info("Starting Initiator thread");
    // Make sure nothing escapes this run method and kills the metastore at large,
    // so wrap it in a big catch Throwable statement.
    try {
      recoverFailedCompactions(false);
      TxnStore.MutexAPI mutex = shouldUseMutex ? txnHandler.getMutexAPI() : new NoMutex();

      // Make sure we run through the loop once before checking to stop as this makes testing
      // much easier.  The stop value is only for testing anyway and not used when called from
      // HiveMetaStore.
      do {
        PerfLogger perfLogger = PerfLogger.getPerfLogger(false);
        long startedAt = -1;
        long prevStart;

        // Wrap the inner parts of the loop in a catch throwable so that any errors in the loop
        // don't doom the entire thread.
        try (TxnStore.MutexAPI.LockHandle handle = mutex.acquireLock(getMutexKey().name())) {
          startedAt = System.currentTimeMillis();
          prevStart = handle.getLastUpdateTime();

          if (metricsEnabled) {
            perfLogger.perfLogBegin(getClassName(), MetricsConstants.COMPACTION_INITIATOR_CYCLE);
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
                        TimeUnit.MILLISECONDS), getLogger()));
          }

          final ShowCompactResponse currentCompactions = txnHandler.showCompact(new ShowCompactRequest());

          CompactorUtil.checkInterrupt(getClassName());

          // Currently we invalidate all entries after each cycle, because the bootstrap replication is marked via
          // table property hive.repl.first.inc.pending which would be cached.
          invalidateCaches();
          Set<String> skipDBs = Sets.newConcurrentHashSet();
          Set<String> skipTables = Sets.newConcurrentHashSet();

          Set<CompactionInfo> potentials = compactionExecutor.submit(() ->
            findPotentialCompactions(prevStart, currentCompactions, skipDBs, skipTables)).get();
          getLogger().debug("Found {} potential compactions, checking to see if we should compact any of them", potentials.size());

          CompactorUtil.checkInterrupt(getClassName());

          Map<String, String> tblNameOwners = new HashMap<>();
          List<CompletableFuture<Void>> compactionList = new ArrayList<>();

          for (CompactionInfo ci : potentials) {
            try {
              //Check for interruption before scheduling each compactionInfo and return if necessary
              if (Thread.currentThread().isInterrupted()) {
                return;
              }

              Table t = metadataCache.computeIfAbsent(ci.getFullTableName(), () -> resolveTable(ci));
              ci.poolName = CompactorUtil.getPoolName(conf, t, metadataCache);
              Partition p = getPartition(ci);
              if (p == null && ci.partName != null) {
                getLogger().info("Can't find partition {}, assuming it has been dropped and moving on.", 
                    ci.getFullPartitionName());
                continue;
              }
              String runAs = resolveUserToRunAs(tblNameOwners, t, p);
              /* checkForCompaction includes many file metadata checks and may be expensive.
               * Therefore, using a thread pool here and running checkForCompactions in parallel */
              String tableName = ci.getFullTableName();
              String partition = ci.getFullPartitionName();
              ci.initiatorVersion = this.runtimeVersion;
              CompletableFuture<Void> asyncJob =
                  CompletableFuture.runAsync(
                          CompactorUtil.ThrowingRunnable.unchecked(() ->
                                  CompactorUtil.scheduleCompactionIfRequired(ci, t, p, runAs, metricsEnabled, hostName, txnHandler, conf)), compactionExecutor)
                      .exceptionally(exc -> {
                        getLogger().error("Error while running scheduling the compaction on the table {} / partition {}.", tableName, partition, exc);
                        return null;
                      });
              compactionList.add(asyncJob);
            } catch (Throwable t) {
              getLogger().error("Caught exception while trying to determine if we should compact {}. " +
                  "Marking failed to avoid repeated failures, {}", ci, t);
              ci.errorMessage = t.getMessage();
              txnHandler.markFailed(ci);
            }
          }

          //Use get instead of join, so we can receive InterruptedException and shutdown gracefully
          CompletableFuture.allOf(compactionList.toArray(new CompletableFuture[0])).get();

          // Check for timed out remote workers.
          recoverFailedCompactions(true);
          handle.releaseLocks(startedAt);
        } catch (InterruptedException e) {
          // do not ignore interruption requests
          return;
        } catch (Throwable t) {
          getLogger().error("Initiator loop caught unexpected exception this time through the loop", t);
        } finally {
          if (metricsEnabled) {
            perfLogger.perfLogEnd(getClassName(), MetricsConstants.COMPACTION_INITIATOR_CYCLE);
            updateCycleDurationMetric(MetricsConstants.COMPACTION_INITIATOR_CYCLE_DURATION, startedAt);
          }
          stopCycleUpdater();
        }

        doPostLoopActions(System.currentTimeMillis() - startedAt);
      } while (!stop.get());
    } catch (Throwable t) {
      getLogger().error("Caught an exception in the main loop of compactor initiator, exiting.", t);
    } finally {
      if (Thread.currentThread().isInterrupted()) {
        getLogger().info("Interrupt received, Initiator is shutting down.");
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

  protected Database resolveDatabase(CompactionInfo ci) throws MetaException, NoSuchObjectException {
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

  protected boolean foundCurrentOrFailedCompactions(ShowCompactResponse compactions, CompactionInfo ci) throws MetaException {
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
      .toList();

    // Figure out if there are any currently running compactions on the same table or partition.
    if (filteredElements.stream().anyMatch(
        e -> TxnStore.WORKING_RESPONSE.equals(e.getState()) || TxnStore.INITIATED_RESPONSE.equals(e.getState()))) {

      getLogger().info("Found currently initiated or working compaction for {} so we will not initiate another compaction", 
          ci.getFullPartitionName());
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
      getLogger().warn("Will not initiate compaction for {} since last {} attempts to compact it failed.", 
          ci.getFullPartitionName(), MetastoreConf.ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD);

      ci.errorMessage = "Compaction is not initiated since last " +
        MetastoreConf.ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD + " consecutive compaction attempts failed)";

      txnHandler.markFailed(ci);
      return true;
    }
    return false;
  }

  // Check if it's a dynamic partitioning case. If so, do not initiate compaction for streaming ingest, only for aborts.
  protected static boolean isDynPartIngest(Table t, CompactionInfo ci, Logger logger) {
    if (t.getPartitionKeys() != null && !t.getPartitionKeys().isEmpty() &&
            ci.partName  == null && !ci.hasOldAbort) {
      logger.info("Skipping entry for {} as it is from dynamic partitioning", ci.getFullTableName());
      return true;
    }
    return false;
  }

  private static class InitiatorCycleUpdater implements Runnable {
    private final String metric;
    private final long startedAt;
    private final long warningThreshold;
    private final long errorThreshold;

    private boolean errorReported;
    private boolean warningReported;
    private final Logger logger;

    InitiatorCycleUpdater(String metric, long startedAt, long warningThreshold, long errorThreshold, Logger logger) {
      this.metric = metric;
      this.startedAt = startedAt;
      this.warningThreshold = warningThreshold;
      this.errorThreshold = errorThreshold;
      this.logger = logger; 
    }

    @Override
    public void run() {
      long elapsed = updateCycleDurationMetric(metric, startedAt);
      if (elapsed >= errorThreshold) {
        if (!errorReported) {
          logger.error("Long running Initiator has been detected, duration {}", elapsed);
          errorReported = true;
        }
      } else if (elapsed >= warningThreshold) {
        if (!warningReported && !errorReported) {
          warningReported = true;
          logger.warn("Long running Initiator has been detected, duration {}", elapsed);
        }
      }
    }
  }

  @Override
  public void enforceMutex(boolean enableMutex) {
    this.shouldUseMutex = enableMutex;
  }

  protected boolean isEligibleForCompaction(CompactionInfo ci, ShowCompactResponse currentCompactions,
      Set<String> skipDBs, Set<String> skipTables) {
    try {
      if (skipDBs.contains(ci.dbname)) {
        getLogger().info("Skipping {}::{}, skipDBs::size:{}", ci.dbname, ci.tableName, skipDBs.size());
        return false;
      } else {
        if (replIsCompactionDisabledForDatabase(ci.dbname)) {
          skipDBs.add(ci.dbname);
          getLogger().info("Skipping {} as compaction is disabled due to repl; skipDBs::size:{}",
              ci.dbname, skipDBs.size());
          return false;
        }
      }

      if (skipTables.contains(ci.getFullTableName())) {
        return false;
      }

      getLogger().info("Checking to see if we should compact {}", ci.getFullPartitionName());

      // Check if we have already initiated or are working on a compaction for this table/partition.
      // Also make sure we haven't exceeded configured number of consecutive failures.
      // If any of the above applies, skip it.
      // Note: if we are just waiting on cleaning we can still check, as it may be time to compact again even though we haven't cleaned.
      if (foundCurrentOrFailedCompactions(currentCompactions, ci)) {
        return false;
      }

      Table t = metadataCache.computeIfAbsent(ci.getFullTableName(), () -> resolveTable(ci));
      if (t == null) {
        getLogger().info("Can't find table {}, assuming it's a temp table or has been dropped and moving on.",
            ci.getFullTableName());
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
          getLogger().info("DB {} marked {}=true so we will not compact it.", hive_metastoreConstants.NO_AUTO_COMPACT, ci.dbname);
        } else {
          skipTables.add(ci.getFullTableName());
          getLogger().info("Table {} marked {}=true so we will not compact it.", hive_metastoreConstants.NO_AUTO_COMPACT, 
              tableName(t));
        }
        return false;
      }
    } catch (Throwable e) {
      getLogger().error("Caught exception while checking compaction eligibility.", e);
      try {
        ci.errorMessage = e.getMessage();
        txnHandler.markFailed(ci);
      } catch (MetaException ex) {
        getLogger().error("Caught exception while marking compaction as failed.", e);
      }
      return false;
    }
    return true;
  }
}
