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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ServerUtils;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.AcidMetricService;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.metrics.PerfLogger;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnCommonUtils;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils.ParsedDirectory;
import org.apache.hadoop.hive.shims.HadoopShims.HdfsFileStatusWithId;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.common.util.Ref;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.conf.Constants.COMPACTOR_INTIATOR_THREAD_NAME_FORMAT;

/**
 * A class to initiate compactions.  This will run in a separate thread.
 * It's critical that there exactly 1 of these in a given warehouse.
 */
public class Initiator extends MetaStoreCompactorThread {
  static final private String CLASS_NAME = Initiator.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  static final private String COMPACTORTHRESHOLD_PREFIX = "compactorthreshold.";

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

          checkInterrupt();

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

          checkInterrupt();

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
              String poolName = getPoolName(ci, t);
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
                              scheduleCompactionIfRequired(ci, t, p, poolName, runAs, metricsEnabled)), compactionExecutor)
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

  private void scheduleCompactionIfRequired(CompactionInfo ci, Table t, Partition p, String poolName,
                                            String runAs, boolean metricsEnabled)
      throws MetaException {
    StorageDescriptor sd = resolveStorageDescriptor(t, p);
    try {
      ValidWriteIdList validWriteIds = resolveValidWriteIds(t);

      checkInterrupt();

      CompactionType type = checkForCompaction(ci, validWriteIds, sd, t.getParameters(), runAs);
      if (type != null) {
        ci.type = type;
        ci.poolName = poolName;
        requestCompaction(ci, runAs);
      }
    } catch (InterruptedException e) {
      //Handle InterruptedException separately so the compactioninfo won't be marked as failed.
      LOG.info("Initiator pool is being shut down, task received interruption.");
    } catch (Throwable ex) {
      String errorMessage = "Caught exception while trying to determine if we should compact " + ci + ". Marking "
          + "failed to avoid repeated failures, " + ex;
      LOG.error(errorMessage);
      ci.errorMessage = errorMessage;
      if (metricsEnabled) {
        Metrics.getOrCreateCounter(MetricsConstants.COMPACTION_INITIATOR_FAILURE_COUNTER).inc();
      }
      txnHandler.markFailed(ci);
    }
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

  private ValidWriteIdList resolveValidWriteIds(Table t) throws NoSuchTxnException, MetaException {
    ValidTxnList validTxnList = new ValidReadTxnList(conf.get(ValidTxnList.VALID_TXNS_KEY));
    // The response will have one entry per table and hence we get only one ValidWriteIdList
    String fullTableName = TxnUtils.getFullTableName(t.getDbName(), t.getTableName());
    GetValidWriteIdsRequest rqst = new GetValidWriteIdsRequest(Collections.singletonList(fullTableName));
    rqst.setValidTxnList(validTxnList.writeToString());

    return TxnUtils.createValidCompactWriteIdList(
        txnHandler.getValidWriteIds(rqst).getTblValidWriteIds().get(0));
  }

  @VisibleForTesting
  protected String resolveUserToRunAs(Map<String, String> cache, Table t, Partition p)
      throws IOException, InterruptedException {
    //Figure out who we should run the file operations as
    String fullTableName = TxnUtils.getFullTableName(t.getDbName(), t.getTableName());
    StorageDescriptor sd = resolveStorageDescriptor(t, p);

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

  private CompactionType checkForCompaction(final CompactionInfo ci,
                                            final ValidWriteIdList writeIds,
                                            final StorageDescriptor sd,
                                            final Map<String, String> tblproperties,
                                            final String runAs)
      throws IOException, InterruptedException {
    // If it's marked as too many aborted, we already know we need to compact
    if (ci.tooManyAborts) {
      LOG.debug("Found too many aborted transactions for " + ci.getFullPartitionName() + ", " +
          "initiating major compaction");
      return CompactionType.MAJOR;
    }

    if (ci.hasOldAbort) {
      HiveConf.ConfVars oldAbortedTimeoutProp =
          HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_TIME_THRESHOLD;
      LOG.debug("Found an aborted transaction for " + ci.getFullPartitionName()
          + " with age older than threshold " + oldAbortedTimeoutProp + ": " + conf
          .getTimeVar(oldAbortedTimeoutProp, TimeUnit.HOURS) + " hours. "
          + "Initiating minor compaction.");
      return CompactionType.MINOR;
    }
    AcidDirectory acidDirectory = getAcidDirectory(sd, writeIds);
    long baseSize = getBaseSize(acidDirectory);
    FileSystem fs = acidDirectory.getFs();
    Map<Path, Long> deltaSizes = new HashMap<>();
    for (AcidUtils.ParsedDelta delta : acidDirectory.getCurrentDirectories()) {
      deltaSizes.put(delta.getPath(), getDirSize(fs, delta));
    }
    long deltaSize = deltaSizes.values().stream().reduce(0L, Long::sum);
    AcidMetricService.updateMetricsFromInitiator(ci.dbname, ci.tableName, ci.partName, conf, txnHandler,
        baseSize, deltaSizes, acidDirectory.getObsolete());

    if (runJobAsSelf(runAs)) {
      return determineCompactionType(ci, acidDirectory, tblproperties, baseSize, deltaSize);
    } else {
      LOG.info("Going to initiate as user " + runAs + " for " + ci.getFullPartitionName());
      UserGroupInformation ugi = UserGroupInformation.createProxyUser(runAs,
        UserGroupInformation.getLoginUser());
      CompactionType compactionType;
      try {
        compactionType = ugi.doAs(
            (PrivilegedExceptionAction<CompactionType>) () -> determineCompactionType(ci, acidDirectory, tblproperties, baseSize, deltaSize));
      } finally {
        try {
          FileSystem.closeAllForUGI(ugi);
        } catch (IOException exception) {
          LOG.error("Could not clean up file-system handles for UGI: " + ugi + " for " +
              ci.getFullPartitionName(), exception);
        }
      }
      return compactionType;
    }
  }

  private AcidDirectory getAcidDirectory(StorageDescriptor sd, ValidWriteIdList writeIds) throws IOException {
    Path location = new Path(sd.getLocation());
    FileSystem fs = location.getFileSystem(conf);
    return AcidUtils.getAcidState(fs, location, conf, writeIds, Ref.from(false), false);
  }

  private CompactionType determineCompactionType(CompactionInfo ci, AcidDirectory dir, Map<String,
      String> tblproperties, long baseSize, long deltaSize) {
    boolean noBase = false;
    List<AcidUtils.ParsedDelta> deltas = dir.getCurrentDirectories();
    if (baseSize == 0 && deltaSize > 0) {
      noBase = true;
    } else {
      String deltaPctProp = tblproperties.get(COMPACTORTHRESHOLD_PREFIX +
          HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_PCT_THRESHOLD);
      float deltaPctThreshold = deltaPctProp == null ?
          HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_PCT_THRESHOLD) :
          Float.parseFloat(deltaPctProp);
      boolean bigEnough =   (float)deltaSize/(float)baseSize > deltaPctThreshold;
      boolean multiBase = dir.getObsolete().stream()
              .anyMatch(path -> path.getName().startsWith(AcidUtils.BASE_PREFIX));

      boolean initiateMajor =  bigEnough || (deltaSize == 0  && multiBase);
      if (LOG.isDebugEnabled()) {
        StringBuilder msg = new StringBuilder("delta size: ");
        msg.append(deltaSize);
        msg.append(" base size: ");
        msg.append(baseSize);
        msg.append(" multiBase ");
        msg.append(multiBase);
        msg.append(" deltaSize ");
        msg.append(deltaSize);
        msg.append(" threshold: ");
        msg.append(deltaPctThreshold);
        msg.append(" delta/base ratio > ").append(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_PCT_THRESHOLD.varname)
            .append(": ");
        msg.append(bigEnough);
        msg.append(".");
        if (!initiateMajor) {
          msg.append("not");
        }
        msg.append(" initiating major compaction.");
        LOG.debug(msg.toString());
      }
      if (initiateMajor) return CompactionType.MAJOR;
    }

    String deltaNumProp = tblproperties.get(COMPACTORTHRESHOLD_PREFIX +
        HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD);
    int deltaNumThreshold = deltaNumProp == null ?
        HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD) :
        Integer.parseInt(deltaNumProp);
    boolean enough = deltas.size() > deltaNumThreshold;
    if (!enough) {
      LOG.debug("Not enough deltas to initiate compaction for table=" + ci.tableName + "partition=" + ci.partName
          + ". Found: " + deltas.size() + " deltas, threshold is " + deltaNumThreshold);
      return null;
    }
    // If there's no base file, do a major compaction
    LOG.debug("Found " + deltas.size() + " delta files, and " + (noBase ? "no" : "has") + " base," +
        "requesting " + (noBase ? "major" : "minor") + " compaction");

    return noBase || !isMinorCompactionSupported(tblproperties, dir) ?
            CompactionType.MAJOR : CompactionType.MINOR;
  }

  private long getBaseSize(AcidDirectory dir) throws IOException {
    long baseSize = 0;
    if (dir.getBase() != null) {
      baseSize = getDirSize(dir.getFs(), dir.getBase());
    } else {
      for (HdfsFileStatusWithId origStat : dir.getOriginalFiles()) {
        baseSize += origStat.getFileStatus().getLen();
      }
    }
    return baseSize;
  }

  private long getDirSize(FileSystem fs, ParsedDirectory dir) throws IOException {
    return dir.getFiles(fs, Ref.from(false)).stream()
        .map(HdfsFileStatusWithId::getFileStatus)
        .mapToLong(FileStatus::getLen)
        .sum();
  }

  private void requestCompaction(CompactionInfo ci, String runAs) throws MetaException {
    CompactionRequest rqst = new CompactionRequest(ci.dbname, ci.tableName, ci.type);
    if (ci.partName != null) rqst.setPartitionname(ci.partName);
    rqst.setRunas(runAs);
    rqst.setInitiatorId(getInitiatorId(Thread.currentThread().getId()));
    rqst.setInitiatorVersion(this.runtimeVersion);
    rqst.setPoolName(ci.poolName);
    LOG.info("Requesting compaction: " + rqst);
    CompactionResponse resp = txnHandler.compact(rqst);
    if(resp.isAccepted()) {
      ci.id = resp.getId();
    }
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

  private String getInitiatorId(long threadId) {
    StringBuilder name = new StringBuilder(this.hostName);
    name.append("-");
    name.append(threadId);
    return name.toString();
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
