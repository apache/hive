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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ServerUtils;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
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
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnCommonUtils;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils.ParsedDirectory;
import org.apache.hadoop.hive.shims.HadoopShims.HdfsFileStatusWithId;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.common.util.Ref;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

  private long checkInterval;
  private long prevStart = -1;
  private ExecutorService compactionExecutor;

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
      boolean metricsEnabled = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED);

      // Make sure we run through the loop once before checking to stop as this makes testing
      // much easier.  The stop value is only for testing anyway and not used when called from
      // HiveMetaStore.
      do {
        PerfLogger perfLogger = PerfLogger.getPerfLogger(false);
        long startedAt = -1;
        TxnStore.MutexAPI.LockHandle handle = null;

        // Wrap the inner parts of the loop in a catch throwable so that any errors in the loop
        // don't doom the entire thread.
        try {
          handle = txnHandler.getMutexAPI().acquireLock(TxnStore.MUTEX_KEY.Initiator.name());
          if (metricsEnabled) {
            perfLogger.perfLogBegin(CLASS_NAME, MetricsConstants.COMPACTION_INITIATOR_CYCLE);
          }
          startedAt = System.currentTimeMillis();

          long compactionInterval = (prevStart < 0) ? prevStart : (startedAt - prevStart)/1000;
          prevStart = startedAt;

          ShowCompactResponse currentCompactions = txnHandler.showCompact(new ShowCompactRequest());

          Set<String> skipDBs = new HashSet<>();
          Set<String> skipTables = new HashSet<>();

          Set<CompactionInfo> potentials = txnHandler.findPotentialCompactions(abortedThreshold,
              abortedTimeThreshold, compactionInterval)
              .stream()
              .filter(ci -> isEligibleForCompaction(ci, currentCompactions, skipDBs, skipTables))
              .collect(Collectors.toSet());
          LOG.debug("Found " + potentials.size() + " potential compactions, " +
              "checking to see if we should compact any of them");

          Map<String, String> tblNameOwners = new HashMap<>();
          List<CompletableFuture> compactionList = new ArrayList<>();

          if (!potentials.isEmpty()) {
            ValidTxnList validTxnList = TxnCommonUtils.createValidReadTxnList(
                txnHandler.getOpenTxns(), 0);
            conf.set(ValidTxnList.VALID_TXNS_KEY, validTxnList.writeToString());
          }

          for (CompactionInfo ci : potentials) {
            try {
              Table t = resolveTable(ci);
              Partition p = resolvePartition(ci);
              if (p == null && ci.partName != null) {
                LOG.info("Can't find partition " + ci.getFullPartitionName() +
                    ", assuming it has been dropped and moving on.");
                continue;
              }
              String runAs = resolveUserToRunAs(tblNameOwners, t, p);
              /* checkForCompaction includes many file metadata checks and may be expensive.
               * Therefore, using a thread pool here and running checkForCompactions in parallel */
              compactionList.add(CompletableFuture.runAsync(CompactorUtil.ThrowingRunnable.unchecked(() ->
                  scheduleCompactionIfRequired(ci, t, p, runAs)), compactionExecutor));
            } catch (Throwable t) {
              LOG.error("Caught exception while trying to determine if we should compact {}. " +
                  "Marking failed to avoid repeated failures, {}", ci, t);
              ci.errorMessage = t.getMessage();
              txnHandler.markFailed(ci);
            }
          }
          CompletableFuture.allOf(compactionList.toArray(new CompletableFuture[0]))
            .join();

          // Check for timed out remote workers.
          recoverFailedCompactions(true);
        } catch (Throwable t) {
          LOG.error("Initiator loop caught unexpected exception this time through the loop: " +
              StringUtils.stringifyException(t));
        }
        finally {
          if (handle != null) {
            handle.releaseLocks();
          }
          if (metricsEnabled) {
            perfLogger.perfLogEnd(CLASS_NAME, MetricsConstants.COMPACTION_INITIATOR_CYCLE);
          }
        }

        long elapsedTime = System.currentTimeMillis() - startedAt;
        if (elapsedTime < checkInterval && !stop.get()) {
          Thread.sleep(checkInterval - elapsedTime);
        }

        LOG.info("Initiator thread finished one loop.");
      } while (!stop.get());
    } catch (Throwable t) {
      LOG.error("Caught an exception in the main loop of compactor initiator, exiting " +
          StringUtils.stringifyException(t));
    } finally {
      if (compactionExecutor != null) {
        this.compactionExecutor.shutdownNow();
      }
    }
  }

  private void scheduleCompactionIfRequired(CompactionInfo ci, Table t, Partition p, String runAs)
      throws MetaException {
    StorageDescriptor sd = resolveStorageDescriptor(t, p);
    try {
      ValidWriteIdList validWriteIds = resolveValidWriteIds(t);
      CompactionType type = checkForCompaction(ci, validWriteIds, sd, t.getParameters(), runAs);
      if (type != null) {
        requestCompaction(ci, runAs, type);
      }
    } catch (Throwable ex) {
      String errorMessage = "Caught exception while trying to determine if we should compact " + ci + ". Marking "
          + "failed to avoid repeated failures, " + ex;
      LOG.error(errorMessage);
      ci.errorMessage = errorMessage;
      txnHandler.markFailed(ci);
    }
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

    cache.putIfAbsent(fullTableName, findUserToRunAs(sd.getLocation(), t));
    return cache.get(fullTableName);
  }

  @Override
  public void init(AtomicBoolean stop) throws Exception {
    super.init(stop);
    checkInterval = conf.getTimeVar(HiveConf.ConfVars.HIVE_COMPACTOR_CHECK_INTERVAL, TimeUnit.MILLISECONDS);
    compactionExecutor = CompactorUtil.createExecutorWithThreadFactory(
            conf.getIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_REQUEST_QUEUE),
            COMPACTOR_INTIATOR_THREAD_NAME_FORMAT);
  }

  private void recoverFailedCompactions(boolean remoteOnly) throws MetaException {
    if (!remoteOnly) txnHandler.revokeFromLocalWorkers(ServerUtils.hostname());
    txnHandler.revokeTimedoutWorkers(HiveConf.getTimeVar(conf,
        HiveConf.ConfVars.HIVE_COMPACTOR_WORKER_TIMEOUT, TimeUnit.MILLISECONDS));
  }

  // Figure out if there are any currently running compactions on the same table or partition.
  private boolean lookForCurrentCompactions(ShowCompactResponse compactions,
                                            CompactionInfo ci) {
    if (compactions.getCompacts() != null) {
      for (ShowCompactResponseElement e : compactions.getCompacts()) {
         if ((e.getState().equals(TxnStore.WORKING_RESPONSE) || e.getState().equals(TxnStore.INITIATED_RESPONSE)) &&
            e.getDbname().equals(ci.dbname) &&
            e.getTablename().equals(ci.tableName) &&
            (e.getPartitionname() == null && ci.partName == null ||
                  e.getPartitionname().equals(ci.partName))) {
          return true;
        }
      }
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

    if (runJobAsSelf(runAs)) {
      return determineCompactionType(ci, writeIds, sd, tblproperties);
    } else {
      LOG.info("Going to initiate as user " + runAs + " for " + ci.getFullPartitionName());
      UserGroupInformation ugi = UserGroupInformation.createProxyUser(runAs,
        UserGroupInformation.getLoginUser());
      CompactionType compactionType;
      try {
        compactionType = ugi.doAs(new PrivilegedExceptionAction<CompactionType>() {
          @Override
          public CompactionType run() throws Exception {
            return determineCompactionType(ci, writeIds, sd, tblproperties);
          }
        });
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

  private CompactionType determineCompactionType(CompactionInfo ci, ValidWriteIdList writeIds,
                                                 StorageDescriptor sd, Map<String, String> tblproperties)
      throws IOException {

    boolean noBase = false;
    Path location = new Path(sd.getLocation());
    FileSystem fs = location.getFileSystem(conf);
    AcidDirectory dir = AcidUtils.getAcidState(fs, location, conf, writeIds, Ref.from(false), false);
    long baseSize = 0;
    if (dir.getBase() != null) {
      baseSize = sumDirSize(fs, dir.getBase());
    } else {
      for (HdfsFileStatusWithId origStat : dir.getOriginalFiles()) {
        baseSize += origStat.getFileStatus().getLen();
      }
    }

    long deltaSize = 0;
    List<AcidUtils.ParsedDelta> deltas = dir.getCurrentDirectories();
    for (AcidUtils.ParsedDelta delta : deltas) {
      deltaSize += sumDirSize(fs, delta);
    }

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
              .filter(path -> path.getName().startsWith(AcidUtils.BASE_PREFIX)).findAny().isPresent();

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
    if (AcidUtils.isInsertOnlyTable(tblproperties)) {
      LOG.debug("Requesting a major compaction for a MM table; found " + deltas.size() + " deltas, threshold is "
          + deltaNumThreshold);
      return CompactionType.MAJOR;
    }
    // If there's no base file, do a major compaction
    LOG.debug("Found " + deltas.size() + " delta files, and " + (noBase ? "no" : "has") + " base," +
        "requesting " + (noBase ? "major" : "minor") + " compaction");
    return noBase ? CompactionType.MAJOR : CompactionType.MINOR;
  }

  private long sumDirSize(FileSystem fs, ParsedDirectory dir) throws IOException {
    long size = dir.getFiles(fs, Ref.from(false)).stream()
        .map(HdfsFileStatusWithId::getFileStatus)
        .mapToLong(FileStatus::getLen)
        .sum();
    return size;
  }

  private void requestCompaction(CompactionInfo ci, String runAs, CompactionType type) throws MetaException {
    CompactionRequest rqst = new CompactionRequest(ci.dbname, ci.tableName, type);
    if (ci.partName != null) rqst.setPartitionname(ci.partName);
    rqst.setRunas(runAs);
    rqst.setInitiatorId(getInitiatorId(Thread.currentThread().getId()));
    rqst.setInitiatorVersion(this.runtimeVersion);
    LOG.info("Requesting compaction: " + rqst);
    CompactionResponse resp = txnHandler.compact(rqst);
    if(resp.isAccepted()) {
      ci.id = resp.getId();
    }
  }

  // Because TABLE_NO_AUTO_COMPACT was originally assumed to be NO_AUTO_COMPACT and then was moved
  // to no_auto_compact, we need to check it in both cases.
  private boolean noAutoCompactSet(Table t) {
    String noAutoCompact =
        t.getParameters().get(hive_metastoreConstants.TABLE_NO_AUTO_COMPACT);
    if (noAutoCompact == null) {
      noAutoCompact =
          t.getParameters().get(hive_metastoreConstants.TABLE_NO_AUTO_COMPACT.toUpperCase());
    }
    return noAutoCompact != null && noAutoCompact.equalsIgnoreCase("true");
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
        LOG.debug("Skipping {}::{}, skipDBs:{}", ci.dbname, ci.tableName, skipDBs);
        return false;
      } else {
        if (replIsCompactionDisabledForDatabase(ci.dbname)) {
          skipDBs.add(ci.dbname);
          LOG.debug("Skipping {}::{}, skipDBs:{}", ci.dbname, ci.tableName, skipDBs);
          return false;
        }
      }

      if (skipTables.contains(ci.getFullTableName())) {
        return false;
      }

      LOG.info("Checking to see if we should compact " + ci.getFullPartitionName());

      // Check if we already have initiated or are working on a compaction for this partition
      // or table. If so, skip it. If we are just waiting on cleaning we can still check,
      // as it may be time to compact again even though we haven't cleaned.
      // todo: this is not robust. You can easily run `alter table` to start a compaction between
      // the time currentCompactions is generated and now
      if (lookForCurrentCompactions(currentCompactions, ci)) {
        LOG.info("Found currently initiated or working compaction for " +
            ci.getFullPartitionName() + " so we will not initiate another compaction");
        return false;
      }

      //TODO: avoid repeated HMS lookup for same table (e.g partitions within table)
      Table t = resolveTable(ci);
      if (t == null) {
        LOG.info("Can't find table " + ci.getFullTableName() + ", assuming it's a temp " +
            "table or has been dropped and moving on.");
        return false;
      }

      if (replIsCompactionDisabledForTable(t)) {
        skipTables.add(ci.getFullTableName());
        return false;
      }

      if (noAutoCompactSet(t)) {
        LOG.info("Table " + tableName(t) + " marked " + hive_metastoreConstants.TABLE_NO_AUTO_COMPACT +
            "=true so we will not compact it.");
        return false;
      }
      if (AcidUtils.isInsertOnlyTable(t.getParameters()) && !HiveConf
          .getBoolVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_COMPACT_MM)) {
        LOG.info("Table " + tableName(t) + " is insert only and " + HiveConf.ConfVars.HIVE_COMPACTOR_COMPACT_MM.varname
            + "=false so we will not compact it.");
        return false;
      }
      if (isDynPartIngest(t, ci)) {
        return false;
      }

      if (txnHandler.checkFailedCompactions(ci)) {
        LOG.warn("Will not initiate compaction for " + ci.getFullPartitionName() + " since last " +
            MetastoreConf.ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD + " attempts to compact it failed.");
        ci.errorMessage = "Compaction is not initiated since last " +
            MetastoreConf.ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD + " consecutive compaction attempts failed)";
        txnHandler.markFailed(ci);
        return false;
      }
    } catch (Throwable e) {
      LOG.error("Caught exception while checking compaction eligibility.", e);
      try {
        ci.errorMessage = e.getMessage();
        txnHandler.markFailed(ci);
      } catch (MetaException ex) {
        LOG.error("Caught exception while marking compaction as failed.", e);
        return false;
      }
    }
    return true;
  }

  private String getInitiatorId(long threadId) {
    StringBuilder name = new StringBuilder(this.hostName);
    name.append("-");
    name.append(threadId);
    return name.toString();
  }
}
