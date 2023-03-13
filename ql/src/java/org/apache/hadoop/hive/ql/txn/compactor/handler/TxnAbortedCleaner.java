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

import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.metrics.PerfLogger;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
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

import static java.util.Objects.isNull;

class TxnAbortedCleaner extends AcidTxnCleaner {

  private static final Logger LOG = LoggerFactory.getLogger(TxnAbortedCleaner.class.getName());

  public TxnAbortedCleaner(HiveConf conf, TxnStore txnHandler,
                           MetadataCache metadataCache, boolean metricsEnabled,
                           FSRemover fsRemover) {
    super(conf, txnHandler, metadataCache, metricsEnabled, fsRemover);
  }

  /**
   The following cleanup is based on the following idea - <br>
   1. Aborted cleanup is independent of compaction. This is because directories which are written by
      aborted txns are not visible by any open txns. It is only visible while determining the AcidState (which
      only sees the aborted deltas and does not read the file).<br><br>

   The following algorithm is used to clean the set of aborted directories - <br>
      a. Find the list of entries which are suitable for cleanup (This is done in {@link TxnStore#findReadyToCleanForAborts(long, int)}).<br>
      b. If the table/partition does not exist, then remove the associated aborted entry in TXN_COMPONENTS table. <br>
      c. Get the AcidState of the table by using the min open txnID, database name, tableName, partition name, highest write ID <br>
      d. Fetch the aborted directories and delete the directories. <br>
      e. Fetch the aborted write IDs from the AcidState and use it to delete the associated metadata in the TXN_COMPONENTS table.
   **/
  @Override
  public List<Runnable> getTasks() throws MetaException {
    int abortedThreshold = HiveConf.getIntVar(conf,
              HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD);
    long abortedTimeThreshold = HiveConf
              .getTimeVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_TIME_THRESHOLD,
                      TimeUnit.MILLISECONDS);
    List<CompactionInfo> readyToCleanAborts = txnHandler.findReadyToCleanForAborts(abortedTimeThreshold, abortedThreshold);

    if (!readyToCleanAborts.isEmpty()) {
      return readyToCleanAborts.stream().map(ci -> ThrowingRunnable.unchecked(() ->
                      clean(ci, ci.txnId > 0 ? ci.txnId : Long.MAX_VALUE, metricsEnabled)))
              .collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  private void clean(CompactionInfo ci, long minOpenTxn, boolean metricsEnabled) throws MetaException {
    LOG.info("Starting cleaning for {}", ci);
    PerfLogger perfLogger = PerfLogger.getPerfLogger(false);
    String cleanerMetric = MetricsConstants.COMPACTION_CLEANER_CYCLE + "_";
    try {
      if (metricsEnabled) {
        perfLogger.perfLogBegin(TxnAbortedCleaner.class.getName(), cleanerMetric);
      }
      Table t;
      Partition p = null;
      t = metadataCache.computeIfAbsent(ci.getFullTableName(), () -> resolveTable(ci.dbname, ci.tableName));
      if (isNull(t)) {
        // The table was dropped before we got around to cleaning it.
        LOG.info("Unable to find table {}, assuming it was dropped.", ci.getFullTableName());
        txnHandler.markCleanedForAborts(ci);
        return;
      }
      if (MetaStoreUtils.isNoCleanUpSet(t.getParameters())) {
        // The table was marked no clean up true.
        LOG.info("Skipping table {} clean up, as NO_CLEANUP set to true", ci.getFullTableName());
        return;
      }
      if (!isNull(ci.partName)) {
        p = resolvePartition(ci.dbname, ci.tableName, ci.partName);
        if (isNull(p)) {
          // The partition was dropped before we got around to cleaning it.
          LOG.info("Unable to find partition {}, assuming it was dropped.",
                  ci.getFullPartitionName());
          txnHandler.markCleanedForAborts(ci);
          return;
        }
        if (MetaStoreUtils.isNoCleanUpSet(p.getParameters())) {
          // The partition was marked no clean up true.
          LOG.info("Skipping partition {} clean up, as NO_CLEANUP set to true", ci.getFullPartitionName());
          return;
        }
      }

      String location = CompactorUtil.resolveStorageDescriptor(t,p).getLocation();
      ci.runAs = TxnUtils.findUserToRunAs(location, t, conf);
      abortCleanUsingAcidDir(ci, location, minOpenTxn);

    } catch (Exception e) {
      LOG.error("Caught exception when cleaning, unable to complete cleaning of {} due to {}", ci,
              e.getMessage());
      throw new MetaException(e.getMessage());
    } finally {
      if (metricsEnabled) {
        perfLogger.perfLogEnd(TxnAbortedCleaner.class.getName(), cleanerMetric);
      }
    }
  }

  private void abortCleanUsingAcidDir(CompactionInfo ci, String location, long minOpenTxn) throws Exception {
    ValidTxnList validTxnList =
            TxnUtils.createValidTxnListForTxnAbortedCleaner(txnHandler.getOpenTxns(), minOpenTxn);
    //save it so that getAcidState() sees it
    conf.set(ValidTxnList.VALID_TXNS_KEY, validTxnList.writeToString());

    ValidReaderWriteIdList validWriteIdList = getValidCleanerWriteIdList(ci, validTxnList);

    // Set the highestWriteId of the cleanup equal to the min(minOpenWriteId - 1, highWatermark).
    // This is necessary for looking at the complete state of the table till the min open write Id
    // (if there is an open txn on the table) or the highestWatermark.
    // This is used later on while deleting the records in TXN_COMPONENTS table.
    ci.highestWriteId = Math.min(isNull(validWriteIdList.getMinOpenWriteId()) ?
            Long.MAX_VALUE : validWriteIdList.getMinOpenWriteId() - 1, validWriteIdList.getHighWatermark());
    Table table = metadataCache.computeIfAbsent(ci.getFullTableName(), () -> resolveTable(ci.dbname, ci.tableName));
    LOG.debug("Cleaning based on writeIdList: {}", validWriteIdList);

    boolean success = cleanAndVerifyObsoleteDirectories(ci, location, validWriteIdList, table);
    if (success || CompactorUtil.isDynPartAbort(table, ci.partName)) {
      txnHandler.markCleanedForAborts(ci);
    } else {
      LOG.warn("Leaving aborted entry {} in TXN_COMPONENTS table.", ci);
    }

  }
}
