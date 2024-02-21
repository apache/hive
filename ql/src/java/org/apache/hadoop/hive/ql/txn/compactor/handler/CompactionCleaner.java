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
package org.apache.hadoop.hive.ql.txn.compactor.handler;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.LockComponentBuilder;
import org.apache.hadoop.hive.metastore.LockRequestBuilder;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.metrics.PerfLogger;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.txn.compactor.CleanupRequest;
import org.apache.hadoop.hive.ql.txn.compactor.CleanupRequest.CleanupRequestBuilder;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorUtil;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorUtil.ThrowingRunnable;
import org.apache.hadoop.hive.ql.txn.compactor.FSRemover;
import org.apache.hadoop.hive.ql.txn.compactor.MetadataCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_COMPACTOR_CLEANER_RETENTION_TIME;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_COMPACTOR_DELAYED_CLEANUP_ENABLED;
import static java.util.Objects.isNull;

/**
 * A compaction based implementation of TaskHandler.
 * Provides implementation of creation of compaction clean tasks.
 */
class CompactionCleaner extends TaskHandler {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionCleaner.class.getName());

  public CompactionCleaner(HiveConf conf, TxnStore txnHandler,
                                MetadataCache metadataCache, boolean metricsEnabled,
                                FSRemover fsRemover) {
    super(conf, txnHandler, metadataCache, metricsEnabled, fsRemover);
  }

  @Override
  public List<Runnable> getTasks() throws MetaException {
    long minOpenTxnId = txnHandler.findMinOpenTxnIdForCleaner();
    long retentionTime = HiveConf.getBoolVar(conf, HIVE_COMPACTOR_DELAYED_CLEANUP_ENABLED)
            ? HiveConf.getTimeVar(conf, HIVE_COMPACTOR_CLEANER_RETENTION_TIME, TimeUnit.MILLISECONDS)
            : 0;
    List<CompactionInfo> readyToClean = txnHandler.findReadyToClean(minOpenTxnId, retentionTime);
    if (!readyToClean.isEmpty()) {
      long minTxnIdSeenOpen = Math.min(minOpenTxnId, txnHandler.findMinTxnIdSeenOpen());
      // For checking which compaction can be cleaned we can use the minOpenTxnId
      // However findReadyToClean will return all records that were compacted with old version of HMS
      // where the CQ_NEXT_TXN_ID is not set. For these compactions we need to provide minTxnIdSeenOpen
      // to the clean method, to avoid cleaning up deltas needed for running queries
      // when min_history_level is finally dropped, than every HMS will commit compaction the new way
      // and minTxnIdSeenOpen can be removed and minOpenTxnId can be used instead.
      return readyToClean.stream().map(ci -> {
        long cleanerWaterMark = (ci.minOpenWriteId >= 0) ? ci.nextTxnId + 1 : minTxnIdSeenOpen;
        LOG.info("Cleaning based on min open txn id: {}", cleanerWaterMark);
        return ThrowingRunnable.unchecked(() -> clean(ci, cleanerWaterMark, metricsEnabled));
      }).collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  private void clean(CompactionInfo ci, long minOpenTxn, boolean metricsEnabled) throws MetaException {
    LOG.info("Starting cleaning for {}", ci);
    PerfLogger perfLogger = PerfLogger.getPerfLogger(false);
    String cleanerMetric = MetricsConstants.COMPACTION_CLEANER_CYCLE + "_" +
            (!isNull(ci.type) ? ci.type.toString().toLowerCase() : null);
    try {
      if (metricsEnabled) {
        perfLogger.perfLogBegin(CompactionCleaner.class.getName(), cleanerMetric);
      }
      final String location = ci.getProperty("location");

      Table t = null;
      Partition p = null;

      if (isNull(location)) {
        t = metadataCache.computeIfAbsent(ci.getFullTableName(), () -> resolveTable(ci.dbname, ci.tableName));
        if (isNull(t)) {
          // The table was dropped before we got around to cleaning it.
          LOG.info("Unable to find table {}, assuming it was dropped. {}", ci.getFullTableName(),
                  idWatermark(ci));
          txnHandler.markCleaned(ci);
          return;
        }
        if (MetaStoreUtils.isNoCleanUpSet(t.getParameters())) {
          // The table was marked no clean up true.
          LOG.info("Skipping table {} clean up, as NO_CLEANUP set to true", ci.getFullTableName());
          txnHandler.markRefused(ci);
          return;
        }
        if (!isNull(ci.partName)) {
          p = resolvePartition(ci.dbname, ci.tableName, ci.partName);
          if (isNull(p)) {
            // The partition was dropped before we got around to cleaning it.
            LOG.info("Unable to find partition {}, assuming it was dropped. {}",
                    ci.getFullPartitionName(), idWatermark(ci));
            txnHandler.markCleaned(ci);
            return;
          }
          if (MetaStoreUtils.isNoCleanUpSet(p.getParameters())) {
            // The partition was marked no clean up true.
            LOG.info("Skipping partition {} clean up, as NO_CLEANUP set to true", ci.getFullPartitionName());
            txnHandler.markRefused(ci);
            return;
          }
        }
      }
      txnHandler.markCleanerStart(ci);

      if (!isNull(t) || !isNull(ci.partName)) {
        String path = isNull(location)
                ? CompactorUtil.resolveStorageDescriptor(t, p).getLocation()
                : location;
        boolean dropPartition = !isNull(ci.partName) && isNull(p);

        //check if partition wasn't re-created
        if (dropPartition && isNull(resolvePartition(ci.dbname, ci.tableName, ci.partName))) {
          cleanUsingLocation(ci, path, true);
        } else {
          cleanUsingAcidDir(ci, path, minOpenTxn);
        }
      } else {
        cleanUsingLocation(ci, location, false);
      }
    } catch (Exception e) {
      LOG.error("Caught exception when cleaning, unable to complete cleaning of {} due to {}", ci,
              e.getMessage());
      if (metricsEnabled) {
        Metrics.getOrCreateCounter(MetricsConstants.COMPACTION_CLEANER_FAILURE_COUNTER).inc();
      }
      handleCleanerAttemptFailure(ci, e.getMessage());
    }  finally {
      if (metricsEnabled) {
        perfLogger.perfLogEnd(CompactionCleaner.class.getName(), cleanerMetric);
      }
    }
  }

  private void cleanUsingLocation(CompactionInfo ci, String path, boolean requiresLock) throws MetaException {
    List<Path> deleted;
    if (requiresLock) {
      LockRequest lockRequest = createLockRequest(ci);
      LockResponse res = null;
      try {
        res = txnHandler.lock(lockRequest);
        deleted = fsRemover.clean(getCleaningRequestBasedOnLocation(ci, path));
      } catch (NoSuchTxnException | TxnAbortedException e) {
        LOG.error("Error while trying to acquire exclusive write lock: {}", e.getMessage());
        throw new MetaException(e.getMessage());
      } finally {
        if (res != null) {
          try {
            txnHandler.unlock(new UnlockRequest(res.getLockid()));
          } catch (NoSuchLockException | TxnOpenException e) {
            LOG.error("Error while trying to release exclusive write lock: {}", e.getMessage());
          }
        }
      }
    } else {
      deleted = fsRemover.clean(getCleaningRequestBasedOnLocation(ci, path));
    }
    if (!deleted.isEmpty()) {
      txnHandler.markCleaned(ci);
    } else {
      txnHandler.clearCleanerStart(ci);
    }
  }

  private void cleanUsingAcidDir(CompactionInfo ci, String location, long minOpenTxn) throws Exception {
    ValidTxnList validTxnList =
            TxnUtils.createValidTxnListForCleaner(txnHandler.getOpenTxns(), minOpenTxn, false);
    //save it so that getAcidState() sees it
    conf.set(ValidTxnList.VALID_TXNS_KEY, validTxnList.writeToString());
    /*
     * {@code validTxnList} is capped by minOpenTxnGLB so if
     * {@link AcidUtils#getAcidState(Path, Configuration, ValidWriteIdList)} sees a base/delta
     * produced by a compactor, that means every reader that could be active right now see it
     * as well.  That means if this base/delta shadows some earlier base/delta, it will be
     * used in favor of any files that it shadows. Thus, the shadowed files are safe to delete.
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
     * not visible based on 'validTxnList' capped at minOpenTxn, so it will  not be cleaned by
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
    Table table = metadataCache.computeIfAbsent(ci.getFullTableName(), () -> resolveTable(ci.dbname, ci.tableName));
    LOG.debug("Cleaning based on writeIdList: {}", validWriteIdList);

    boolean success = cleanAndVerifyObsoleteDirectories(ci, location, validWriteIdList, table);
    if (success || CompactorUtil.isDynPartAbort(table, ci.partName)) {
      txnHandler.markCleaned(ci);
    } else {
      txnHandler.clearCleanerStart(ci);
      LOG.warn("No files were removed. Leaving queue entry {} in ready for cleaning state.", ci);
    }
  }

  protected LockRequest createLockRequest(CompactionInfo ci) {
    String agentInfo = Thread.currentThread().getName();
    LockRequestBuilder requestBuilder = new LockRequestBuilder(agentInfo);
    requestBuilder.setUser(ci.runAs);
    requestBuilder.setTransactionId(0);

    LockComponentBuilder lockCompBuilder = new LockComponentBuilder()
            .setLock(LockType.EXCL_WRITE)
            .setOperationType(DataOperationType.DELETE)
            .setDbName(ci.dbname)
            .setTableName(ci.tableName)
            .setIsTransactional(true);

    if (ci.partName != null) {
      lockCompBuilder.setPartitionName(ci.partName);
    }
    requestBuilder.addLockComponent(lockCompBuilder.build());

    requestBuilder.setZeroWaitReadEnabled(!conf.getBoolVar(HiveConf.ConfVars.TXN_OVERWRITE_X_LOCK) ||
            !conf.getBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK));
    return requestBuilder.build();
  }

  private static String idWatermark(CompactionInfo ci) {
    return " id=" + ci.id;
  }

  @Override
  protected ValidReaderWriteIdList getValidCleanerWriteIdList(CompactionInfo ci, ValidTxnList validTxnList)
          throws NoSuchTxnException, MetaException {
    ValidReaderWriteIdList validWriteIdList = super.getValidCleanerWriteIdList(ci, validTxnList);
    /*
     * We need to filter the obsoletes dir list, to only remove directories that were made obsolete by this compaction
     * If we have a higher retentionTime it is possible for a second compaction to run on the same partition. Cleaning up the first compaction
     * should not touch the newer obsolete directories to not violate the retentionTime for those.
     */
    if (ci.highestWriteId < validWriteIdList.getHighWatermark()) {
      validWriteIdList.setHighWatermark(ci.highestWriteId);
    }
    return validWriteIdList;
  }

  private CleanupRequest getCleaningRequestBasedOnLocation(CompactionInfo ci, String location) {
    String strIfPurge = ci.getProperty("ifPurge");
    boolean ifPurge = strIfPurge != null || Boolean.parseBoolean(ci.getProperty("ifPurge"));

    Path obsoletePath = new Path(location);
    return new CleanupRequestBuilder()
            .setLocation(location).setDbName(ci.dbname).setFullPartitionName(ci.getFullPartitionName())
            .setRunAs(ci.runAs).setPurge(ifPurge).setObsoleteDirs(Collections.singletonList(obsoletePath))
            .build();
  }
}
