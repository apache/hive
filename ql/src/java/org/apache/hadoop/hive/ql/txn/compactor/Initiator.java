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
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.metrics.PerfLogger;
import org.apache.hadoop.hive.metastore.txn.NoMutex;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hive.conf.Constants.COMPACTOR_INTIATOR_THREAD_NAME_FORMAT;

/**
 * A class to initiate compactions.  This will run in a separate thread.
 * It's critical that there exactly 1 of these in a given warehouse.
 */
public class Initiator extends MetaStoreCompactorThread {
  private static final String CLASS_NAME = Initiator.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  private ExecutorService compactionExecutor;

  private boolean metricsEnabled;
  private boolean shouldUseMutex = true;
  private List<TableOptimizer> optimizers;

  @Override
  public void run() {
    LOG.info("Starting Initiator thread");
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
        try (TxnStore.MutexAPI.LockHandle handle = mutex.acquireLock(TxnStore.MUTEX_KEY.Initiator.name())) {
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

          Set<CompactionInfo> potentials = Sets.newHashSet();
          for (TableOptimizer optimizer : optimizers) {
            potentials.addAll(compactionExecutor.submit(() ->
                optimizer.findPotentialCompactions(prevStart, currentCompactions, skipDBs, skipTables)).get());
          }
          LOG.debug("Found {} potential compactions, checking to see if we should compact any of them", potentials.size());

          CompactorUtil.checkInterrupt(CLASS_NAME);

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
              Partition p = resolvePartition(ci);
              if (p == null && ci.partName != null) {
                LOG.info("Can't find partition {}, assuming it has been dropped and moving on.",
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
          handle.releaseLocks(startedAt);
        } catch (InterruptedException e) {
          // do not ignore interruption requests
          return;
        } catch (Throwable t) {
          LOG.error("Initiator loop caught unexpected exception this time through the loop", t);
        } finally {
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
    optimizers = Arrays.stream(MetastoreConf.getTrimmedStringsVar(conf,
            MetastoreConf.ConfVars.COMPACTOR_INITIATOR_TABLE_OPTIMIZERS))
        .map(this::instantiateTableOptimizer).toList();
  }
  
  private TableOptimizer instantiateTableOptimizer(String className) {
    try {
      Class<? extends TableOptimizer> icebergInitiatorClazz = (Class<? extends TableOptimizer>)
          Class.forName(className, true,
              Utilities.getSessionSpecifiedClassLoader());

      Class<?>[] constructorParameterTypes = {HiveConf.class, TxnStore.class, MetadataCache.class};
      Constructor<?> constructor = icebergInitiatorClazz.getConstructor(constructorParameterTypes);

      Object[] constructorArgs = new Object[] {conf, txnHandler, metadataCache};
      return (TableOptimizer) constructor.newInstance(constructorArgs);
    }
    catch (Exception e) {
      throw new CompactionException(e, "Failed instantiating and calling table optimizer %s", className);
    }
  }

  private void recoverFailedCompactions(boolean remoteOnly) throws MetaException {
    if (!remoteOnly) txnHandler.revokeFromLocalWorkers(ServerUtils.hostname());
    txnHandler.revokeTimedoutWorkers(HiveConf.getTimeVar(conf,
        HiveConf.ConfVars.HIVE_COMPACTOR_WORKER_TIMEOUT, TimeUnit.MILLISECONDS));
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

  @Override
  public void enforceMutex(boolean enableMutex) {
    this.shouldUseMutex = enableMutex;
  }

  @VisibleForTesting
  protected Partition resolvePartition(CompactionInfo ci) throws Exception {
    Table table = metadataCache.computeIfAbsent(ci.getFullTableName(), () ->
        CompactorUtil.resolveTable(conf, ci.dbname, ci.tableName));

    if (!MetaStoreUtils.isIcebergTable(table.getParameters())) {
      return CompactorUtil.resolvePartition(conf, null, ci.dbname, ci.tableName, ci.partName,
          CompactorUtil.METADATA_FETCH_MODE.LOCAL);
    } else {
      if (ci.partName == null) {
        return null;
      }

      org.apache.hadoop.hive.metastore.api.Partition partition = new org.apache.hadoop.hive.metastore.api.Partition();
      partition.setSd(table.getSd().deepCopy());
      partition.setParameters(com.google.common.collect.Maps.newHashMap());

      return partition;
    }
  }
}
