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

import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.AcidMetricService;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.metrics.PerfLogger;
import org.apache.hadoop.hive.metastore.txn.TxnCommonUtils;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorUtil.ThrowingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.common.util.Ref;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.commons.collections4.ListUtils.subtract;
import static org.apache.hadoop.hive.conf.Constants.COMPACTOR_CLEANER_THREAD_NAME_FORMAT;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_COMPACTOR_CLEANER_RETENTION_TIME;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_COMPACTOR_DELAYED_CLEANUP_ENABLED;
import static org.apache.hadoop.hive.metastore.HMSHandler.getMSForConf;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.HIVE_COMPACTOR_CLEANER_MAX_RETRY_ATTEMPTS;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.HIVE_COMPACTOR_CLEANER_RETRY_RETENTION_TIME;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.getIntVar;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.getTimeVar;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

/**
 * A class to clean directories after compactions.  This will run in a separate thread.
 */
public class Cleaner extends MetaStoreCompactorThread {

  static final private String CLASS_NAME = Cleaner.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  private boolean metricsEnabled = false;

  private ReplChangeManager replChangeManager;
  private ExecutorService cleanerExecutor;

  @Override
  public void init(AtomicBoolean stop) throws Exception {
    super.init(stop);
    replChangeManager = ReplChangeManager.getInstance(conf);
    checkInterval = conf.getTimeVar(
            HiveConf.ConfVars.HIVE_COMPACTOR_CLEANER_RUN_INTERVAL, TimeUnit.MILLISECONDS);
    cleanerExecutor = CompactorUtil.createExecutorWithThreadFactory(
            conf.getIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_CLEANER_THREADS_NUM),
            COMPACTOR_CLEANER_THREAD_NAME_FORMAT);
    metricsEnabled = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED) &&
        MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_EXT_ON);
  }

  @Override
  public void run() {
    LOG.info("Starting Cleaner thread");
    try {
      do {
        TxnStore.MutexAPI.LockHandle handle = null;
        long startedAt = -1;
        long retentionTime = HiveConf.getBoolVar(conf, HIVE_COMPACTOR_DELAYED_CLEANUP_ENABLED)
                ? HiveConf.getTimeVar(conf, HIVE_COMPACTOR_CLEANER_RETENTION_TIME, TimeUnit.MILLISECONDS)
                : 0;

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

          long minOpenTxnId = txnHandler.findMinOpenTxnIdForCleaner();

          List<CompactionInfo> readyToClean = txnHandler.findReadyToClean(minOpenTxnId, retentionTime);
          if (!readyToClean.isEmpty()) {
            long minTxnIdSeenOpen = txnHandler.findMinTxnIdSeenOpen();
            final long cleanerWaterMark =
                minTxnIdSeenOpen < 0 ? minOpenTxnId : Math.min(minOpenTxnId, minTxnIdSeenOpen);

            LOG.info("Cleaning based on min open txn id: " + cleanerWaterMark);
            List<CompletableFuture<Void>> cleanerList = new ArrayList<>();
            // For checking which compaction can be cleaned we can use the minOpenTxnId
            // However findReadyToClean will return all records that were compacted with old version of HMS
            // where the CQ_NEXT_TXN_ID is not set. For these compactions we need to provide minTxnIdSeenOpen
            // to the clean method, to avoid cleaning up deltas needed for running queries
            // when min_history_level is finally dropped, than every HMS will commit compaction the new way
            // and minTxnIdSeenOpen can be removed and minOpenTxnId can be used instead.
            for (CompactionInfo compactionInfo : readyToClean) {
              CompletableFuture<Void> asyncJob =
                  CompletableFuture.runAsync(
                          ThrowingRunnable.unchecked(() -> clean(compactionInfo, cleanerWaterMark, metricsEnabled)),
                          cleanerExecutor)
                      .exceptionally(t -> {
                        LOG.error("Error clearing {}", compactionInfo.getFullPartitionName(), t);
                        return null;
                      });
              cleanerList.add(asyncJob);
            }
            CompletableFuture.allOf(cleanerList.toArray(new CompletableFuture[0])).join();
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
        long elapsedTime = System.currentTimeMillis() - startedAt;
        doPostLoopActions(elapsedTime, CompactorThreadType.CLEANER);
      } while (!stop.get());
    } catch (InterruptedException ie) {
      LOG.error("Compactor cleaner thread interrupted, exiting " +
        StringUtils.stringifyException(ie));
    } finally {
      if (cleanerExecutor != null) {
        this.cleanerExecutor.shutdownNow();
      }
    }
  }

  private void clean(CompactionInfo ci, long minOpenTxnGLB, boolean metricsEnabled) throws MetaException {
    LOG.info("Starting cleaning for " + ci);
    PerfLogger perfLogger = PerfLogger.getPerfLogger(false);
    String cleanerMetric = MetricsConstants.COMPACTION_CLEANER_CYCLE + "_" +
        (ci.type != null ? ci.type.toString().toLowerCase() : null);
    try {
      if (metricsEnabled) {
        perfLogger.perfLogBegin(CLASS_NAME, cleanerMetric);
      }
      final String location = ci.getProperty("location");

      Callable<Boolean> cleanUpTask;
      Table t = null;
      Partition p = null;

      if (location == null) {
        t = resolveTable(ci);
        if (t == null) {
          // The table was dropped before we got around to cleaning it.
          LOG.info("Unable to find table " + ci.getFullTableName() + ", assuming it was dropped." +
            idWatermark(ci));
          txnHandler.markCleaned(ci);
          return;
        }
        if (MetaStoreUtils.isNoCleanUpSet(t.getParameters())) {
          // The table was marked no clean up true.
          LOG.info("Skipping table " + ci.getFullTableName() + " clean up, as NO_CLEANUP set to true");
          txnHandler.markCleaned(ci);
          return;
        }
        if (ci.partName != null) {
          p = resolvePartition(ci);
          if (p == null) {
            // The partition was dropped before we got around to cleaning it.
            LOG.info("Unable to find partition " + ci.getFullPartitionName() +
              ", assuming it was dropped." + idWatermark(ci));
            txnHandler.markCleaned(ci);
            return;
          }
          if (MetaStoreUtils.isNoCleanUpSet(p.getParameters())) {
            // The partition was marked no clean up true.
            LOG.info("Skipping partition " + ci.getFullPartitionName() + " clean up, as NO_CLEANUP set to true");
            txnHandler.markCleaned(ci);
            return;
          }
        }
      }
      txnHandler.markCleanerStart(ci);

      if (t != null || ci.partName != null) {
        String path = location == null
            ? resolveStorageDescriptor(t, p).getLocation()
            : location;
        boolean dropPartition = ci.partName != null && p == null;
        cleanUpTask = () -> removeFiles(path, minOpenTxnGLB, ci, dropPartition);
      } else {
        cleanUpTask = () -> removeFiles(location, ci);
      }

      Ref<Boolean> removedFiles = Ref.from(false);
      if (runJobAsSelf(ci.runAs)) {
        removedFiles.value = cleanUpTask.call();
      } else {
        LOG.info("Cleaning as user " + ci.runAs + " for " + ci.getFullPartitionName());
        UserGroupInformation ugi = UserGroupInformation.createProxyUser(ci.runAs,
            UserGroupInformation.getLoginUser());
        try {
          ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            removedFiles.value = cleanUpTask.call();
            return null;
          });
        } finally {
          try {
            FileSystem.closeAllForUGI(ugi);
          } catch (IOException exception) {
            LOG.error("Could not clean up file-system handles for UGI: " + ugi + " for " +
                ci.getFullPartitionName() + idWatermark(ci), exception);
          }
        }
      }
      if (removedFiles.value || isDynPartAbort(t, ci)) {
        txnHandler.markCleaned(ci);
      } else {
        txnHandler.clearCleanerStart(ci);
        LOG.warn("No files were removed. Leaving queue entry " + ci + " in ready for cleaning state.");
      }
    } catch (Exception e) {
      LOG.error("Caught exception when cleaning, unable to complete cleaning of " + ci + " " +
          StringUtils.stringifyException(e));
      ci.errorMessage = e.getMessage();
      if (metricsEnabled) {
        Metrics.getOrCreateCounter(MetricsConstants.COMPACTION_CLEANER_FAILURE_COUNTER).inc();
      }
      handleCleanerAttemptFailure(ci);
    }  finally {
      if (metricsEnabled) {
        perfLogger.perfLogEnd(CLASS_NAME, cleanerMetric);
      }
    }
  }

  private void handleCleanerAttemptFailure(CompactionInfo ci) throws MetaException {
    long defaultRetention = getTimeVar(conf, HIVE_COMPACTOR_CLEANER_RETRY_RETENTION_TIME, TimeUnit.MILLISECONDS);
    int cleanAttempts = 0;
    if (ci.retryRetention > 0) {
      cleanAttempts = (int)(Math.log(ci.retryRetention / defaultRetention) / Math.log(2)) + 1;
    }
    if (cleanAttempts >= getIntVar(conf, HIVE_COMPACTOR_CLEANER_MAX_RETRY_ATTEMPTS)) {
      //Mark it as failed if the max attempt threshold is reached.
      txnHandler.markFailed(ci);
    } else {
      //Calculate retry retention time and update record.
      ci.retryRetention = (long)Math.pow(2, cleanAttempts) * defaultRetention;
      txnHandler.setCleanerRetryRetentionTimeOnError(ci);
    }
  }

  private ValidReaderWriteIdList getValidCleanerWriteIdList(CompactionInfo ci, ValidTxnList validTxnList)
      throws NoSuchTxnException, MetaException {
    List<String> tblNames = Collections.singletonList(AcidUtils.getFullTableName(ci.dbname, ci.tableName));
    GetValidWriteIdsRequest request = new GetValidWriteIdsRequest(tblNames);
    request.setValidTxnList(validTxnList.writeToString());
    GetValidWriteIdsResponse rsp = txnHandler.getValidWriteIds(request);
    // we could have no write IDs for a table if it was never written to but
    // since we are in the Cleaner phase of compactions, there must have
    // been some delta/base dirs
    assert rsp != null && rsp.getTblValidWriteIdsSize() == 1;
    ValidReaderWriteIdList validWriteIdList =
        TxnCommonUtils.createValidReaderWriteIdList(rsp.getTblValidWriteIds().get(0));
    /*
     * We need to filter the obsoletes dir list, to only remove directories that were made obsolete by this compaction
     * If we have a higher retentionTime it is possible for a second compaction to run on the same partition. Cleaning up the first compaction
     * should not touch the newer obsolete directories to not to violate the retentionTime for those.
     */
    if (ci.highestWriteId < validWriteIdList.getHighWatermark()) {
      validWriteIdList = validWriteIdList.updateHighWatermark(ci.highestWriteId);
    }
    return validWriteIdList;
  }

  private static boolean isDynPartAbort(Table t, CompactionInfo ci) {
    return Optional.ofNullable(t).map(Table::getPartitionKeys).filter(pk -> pk.size() > 0).isPresent()
        && ci.partName == null;
  }

  private static String idWatermark(CompactionInfo ci) {
    return " id=" + ci.id;
  }

  private boolean removeFiles(String location, long minOpenTxnGLB, CompactionInfo ci, boolean dropPartition)
      throws MetaException, IOException, NoSuchObjectException, NoSuchTxnException {

    if (dropPartition) {
      LockRequest lockRequest = createLockRequest(ci, 0, LockType.EXCL_WRITE, DataOperationType.DELETE);
      LockResponse res = null;

      try {
        res = txnHandler.lock(lockRequest);
        if (res.getState() == LockState.ACQUIRED) {
          //check if partition wasn't re-created
          if (resolvePartition(ci) == null) {
            return removeFiles(location, ci);
          }
        }
      } catch (NoSuchTxnException | TxnAbortedException e) {
        LOG.error(e.getMessage());
      } finally {
        if (res != null) {
          try {
            txnHandler.unlock(new UnlockRequest(res.getLockid()));
          } catch (NoSuchLockException | TxnOpenException e) {
            LOG.error(e.getMessage());
          }
        }
      }
    }

    ValidTxnList validTxnList =
      TxnUtils.createValidTxnListForCleaner(txnHandler.getOpenTxns(), minOpenTxnGLB);
    //save it so that getAcidState() sees it
    conf.set(ValidTxnList.VALID_TXNS_KEY, validTxnList.writeToString());
    /**
     * {@code validTxnList} is capped by minOpenTxnGLB so if
     * {@link AcidUtils#getAcidState(Path, Configuration, ValidWriteIdList)} sees a base/delta
     * produced by a compactor, that means every reader that could be active right now see it
     * as well.  That means if this base/delta shadows some earlier base/delta, the it will be
     * used in favor of any files that it shadows.  Thus the shadowed files are safe to delete.
     *
     *
     * The metadata about aborted writeIds (and consequently aborted txn IDs) cannot be deleted
     * above COMPACTION_QUEUE.CQ_HIGHEST_WRITE_ID.
     * See {@link TxnStore#markCleaned(CompactionInfo)} for details.
     * For example given partition P1, txnid:150 starts and sees txnid:149 as open.
     * Say compactor runs in txnid:160, but 149 is still open and P1 has the largest resolved
     * writeId:17.  Compactor will produce base_17_c160.
     * Suppose txnid:149 writes delta_18_18
     * to P1 and aborts.  Compactor can only remove TXN_COMPONENTS entries
     * up to (inclusive) writeId:17 since delta_18_18 may be on disk (and perhaps corrupted) but
     * not visible based on 'validTxnList' capped at minOpenTxn so it will not not be cleaned by
     * {@link #removeFiles(String, ValidWriteIdList, CompactionInfo)} and so we must keep the
     * metadata that says that 18 is aborted.
     * In a slightly different case, whatever txn created delta_18 (and all other txn) may have
     * committed by the time cleaner runs and so cleaner will indeed see delta_18_18 and remove
     * it (since it has nothing but aborted data).  But we can't tell which actually happened
     * in markCleaned() so make sure it doesn't delete meta above CG_CQ_HIGHEST_WRITE_ID.
     *
     * We could perhaps make cleaning of aborted and obsolete and remove all aborted files up
     * to the current Min Open Write Id, this way aborted TXN_COMPONENTS meta can be removed
     * as well up to that point which may be higher than CQ_HIGHEST_WRITE_ID.  This could be
     * useful if there is all of a sudden a flood of aborted txns.  (For another day).
     */

    // Creating 'reader' list since we are interested in the set of 'obsolete' files
    ValidReaderWriteIdList validWriteIdList = getValidCleanerWriteIdList(ci, validTxnList);
    LOG.debug("Cleaning based on writeIdList: {}", validWriteIdList);

    return removeFiles(location, validWriteIdList, ci);
  }
  /**
   * @return true if any files were removed
   */
  private boolean removeFiles(String location, ValidWriteIdList writeIdList, CompactionInfo ci)
      throws IOException, NoSuchObjectException, MetaException {
    Path path = new Path(location);
    FileSystem fs = path.getFileSystem(conf);
    
    // Collect all of the files/dirs
    Map<Path, AcidUtils.HdfsDirSnapshot> dirSnapshots = AcidUtils.getHdfsDirSnapshotsForCleaner(fs, path);
    AcidDirectory dir = AcidUtils.getAcidState(fs, path, conf, writeIdList, Ref.from(false), false, 
        dirSnapshots);
    Table table = resolveTable(ci);
    boolean isDynPartAbort = isDynPartAbort(table, ci);
    
    List<Path> obsoleteDirs = getObsoleteDirs(dir, isDynPartAbort);
    if (isDynPartAbort || dir.hasUncompactedAborts()) {
      ci.setWriteIds(dir.hasUncompactedAborts(), dir.getAbortedWriteIds());
    }
    List<Path> deleted = remove(location, ci, obsoleteDirs, true, fs);
    if (dir.getObsolete().size() > 0) {
      AcidMetricService.updateMetricsFromCleaner(ci.dbname, ci.tableName, ci.partName, dir.getObsolete(), conf,
          txnHandler);
    }
    // Make sure there are no leftovers below the compacted watermark
    conf.set(ValidTxnList.VALID_TXNS_KEY, new ValidReadTxnList().toString());
    dir = AcidUtils.getAcidState(fs, path, conf, new ValidReaderWriteIdList(
        ci.getFullTableName(), new long[0], new BitSet(), ci.highestWriteId, Long.MAX_VALUE),
      Ref.from(false), false, dirSnapshots);
    
    List<Path> remained = subtract(getObsoleteDirs(dir, isDynPartAbort), deleted);
    if (!remained.isEmpty()) {
      LOG.warn(idWatermark(ci) + " Remained " + remained.size() +
        " obsolete directories from " + location + ". " + getDebugInfo(remained));
      return false;
    }
    LOG.debug(idWatermark(ci) + " All cleared below the watermark: " + ci.highestWriteId + " from " + location);
    return true;
  }
  
  private List<Path> getObsoleteDirs(AcidDirectory dir, boolean isDynPartAbort) {
    List<Path> obsoleteDirs = dir.getObsolete();
    /**
     * add anything in 'dir'  that only has data from aborted transactions - no one should be
     * trying to read anything in that dir (except getAcidState() that only reads the name of
     * this dir itself)
     * So this may run ahead of {@link CompactionInfo#highestWriteId} but it's ok (suppose there
     * are no active txns when cleaner runs).  The key is to not delete metadata about aborted
     * txns with write IDs > {@link CompactionInfo#highestWriteId}.
     * See {@link TxnStore#markCleaned(CompactionInfo)}
     */
    obsoleteDirs.addAll(dir.getAbortedDirectories());
    if (isDynPartAbort) {
      // In the event of an aborted DP operation, we should only consider the aborted directories for cleanup.
      // Including obsolete directories for partitioned tables can result in data loss.
      obsoleteDirs = dir.getAbortedDirectories();
    }
    return obsoleteDirs;
  }

  private boolean removeFiles(String location, CompactionInfo ci)
      throws NoSuchObjectException, IOException, MetaException {
    String strIfPurge = ci.getProperty("ifPurge");
    boolean ifPurge = strIfPurge != null || Boolean.parseBoolean(ci.getProperty("ifPurge"));
    
    Path path = new Path(location);
    return !remove(location, ci, Collections.singletonList(path), ifPurge,
      path.getFileSystem(conf)).isEmpty();
  }

  private List<Path> remove(String location, CompactionInfo ci, List<Path> paths, boolean ifPurge, FileSystem fs)
      throws MetaException, IOException {
    List<Path> deleted = new ArrayList<>();
    if (paths.size() < 1) {
      return deleted;
    }
    LOG.info(idWatermark(ci) + " About to remove " + paths.size() +
      " obsolete directories from " + location + ". " + getDebugInfo(paths));
    boolean needCmRecycle;
    try {
      Database db = getMSForConf(conf).getDatabase(getDefaultCatalog(conf), ci.dbname);
      needCmRecycle = ReplChangeManager.isSourceOfReplication(db);
    } catch (NoSuchObjectException ex) {
      // can not drop a database which is a source of replication
      needCmRecycle = false;
    }
    for (Path dead : paths) {
      LOG.debug("Going to delete path " + dead.toString());
      if (needCmRecycle) {
        replChangeManager.recycle(dead, ReplChangeManager.RecycleType.MOVE, ifPurge);
      }
      if (FileUtils.moveToTrash(fs, dead, conf, ifPurge)) {
        deleted.add(dead);
      }
    }
    return deleted;
  }
  
  private String getDebugInfo(List<Path> paths) {
    return "[" + paths.stream().map(Path::getName).collect(Collectors.joining(",")) + ']';
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
