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
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
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

/**
 * Abort-cleanup based implementation of TaskHandler.
 * Provides implementation of creation of abort clean tasks.
 */
class AbortedTxnCleaner extends TaskHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AbortedTxnCleaner.class.getName());

  public AbortedTxnCleaner(HiveConf conf, TxnStore txnHandler,
                           MetadataCache metadataCache, boolean metricsEnabled,
                           FSRemover fsRemover) {
    super(conf, txnHandler, metadataCache, metricsEnabled, fsRemover);
  }

  /**
   The following cleanup is based on the following idea - <br>
   Aborted cleanup is independent of compaction. This is because directories which are written by
   aborted txns are not visible by any open txns in most cases. The one case, wherein abort deltas
   are visible for open txns are streaming aborts wherein, a single delta file can have writes
   from committed and aborted txns. These deltas are also referred to as uncompacted aborts.
   In such cases, we do not delete uncompacted aborts or its associated metadata.

   The following algorithm is used to clean the set of aborted directories - <br>
      a. Find the list of entries which are suitable for cleanup (This is done in {@link TxnStore#findReadyToCleanAborts(long, int)}).<br>
      b. If the table/partition does not exist, then remove the associated aborted entry in TXN_COMPONENTS table. <br>
      c. Get the AcidState of the table by using the min open txnID, database name, tableName, partition name, highest write ID.
         The construction of AcidState helps in identifying obsolete/aborted directories in the table/partition. <br>
      d. Fetch the obsolete/aborted directories and delete the directories using the AcidState. <br>
      e. Fetch the aborted write IDs from the AcidState and use it to delete the associated metadata in the TXN_COMPONENTS table.
   **/
  @Override
  public List<Runnable> getTasks() throws MetaException {
    int abortedThreshold = HiveConf.getIntVar(conf,
              HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD);
    long abortedTimeThreshold = HiveConf
              .getTimeVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_TIME_THRESHOLD,
                      TimeUnit.MILLISECONDS);
    List<CompactionInfo> readyToCleanAborts = txnHandler.findReadyToCleanAborts(abortedTimeThreshold, abortedThreshold);

    if (!readyToCleanAborts.isEmpty()) {
      return readyToCleanAborts.stream().map(info -> ThrowingRunnable.unchecked(() ->
                      clean(info, info.minOpenWriteTxnId, metricsEnabled)))
              .collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  private void clean(CompactionInfo info, long minOpenWriteTxn, boolean metricsEnabled) throws MetaException, InterruptedException {
    LOG.info("Starting cleaning for {}", info);
    PerfLogger perfLogger = PerfLogger.getPerfLogger(false);
    String cleanerMetric = MetricsConstants.COMPACTION_CLEANER_CYCLE + "_";
    try {
      if (metricsEnabled) {
        perfLogger.perfLogBegin(AbortedTxnCleaner.class.getName(), cleanerMetric);
      }
      Partition p = null;
      Table t = metadataCache.computeIfAbsent(info.getFullTableName(), () -> resolveTable(info.dbname, info.tableName));
      if (isNull(t)) {
        // The table was dropped before we got around to cleaning it.
        LOG.info("Unable to find table {}, assuming it was dropped.", info.getFullTableName());
        txnHandler.markCleaned(info);
        return;
      }
      if (!isNull(info.partName)) {
        p = resolvePartition(info.dbname, info.tableName, info.partName);
        if (isNull(p)) {
          // The partition was dropped before we got around to cleaning it.
          LOG.info("Unable to find partition {}, assuming it was dropped.",
                  info.getFullPartitionName());
          txnHandler.markCleaned(info);
          return;
        }
      }

      String location = CompactorUtil.resolveStorageDescriptor(t,p).getLocation();
      info.runAs = TxnUtils.findUserToRunAs(location, t, conf);
      abortCleanUsingAcidDir(info, location, minOpenWriteTxn);

    } catch (InterruptedException e) {
      LOG.error("Caught an interrupted exception when cleaning, unable to complete cleaning of {} due to {}", info,
              e.getMessage());
      handleCleanerAttemptFailure(info, e.getMessage());
      throw e;
    } catch (Exception e) {
      LOG.error("Caught exception when cleaning, unable to complete cleaning of {} due to {}", info,
              e.getMessage());
      handleCleanerAttemptFailure(info, e.getMessage());
      throw new MetaException(e.getMessage());
    } finally {
      if (metricsEnabled) {
        perfLogger.perfLogEnd(AbortedTxnCleaner.class.getName(), cleanerMetric);
      }
    }
  }

  private void abortCleanUsingAcidDir(CompactionInfo info, String location, long minOpenWriteTxn) throws Exception {
    ValidTxnList validTxnList =
            TxnUtils.createValidTxnListForCleaner(txnHandler.getOpenTxns(), minOpenWriteTxn, true);
    //save it so that getAcidState() sees it
    conf.set(ValidTxnList.VALID_TXNS_KEY, validTxnList.writeToString());

    ValidReaderWriteIdList validWriteIdList = getValidCleanerWriteIdList(info, validTxnList);

    // Set the highestWriteId of the cleanup equal to the min(minOpenWriteId - 1, highWatermark).
    // This is necessary for looking at the complete state of the table till the min open write Id
    // (if there is an open txn on the table) or the highestWatermark.
    // This is used later on while deleting the records in TXN_COMPONENTS table.
    info.highestWriteId = Math.min(isNull(validWriteIdList.getMinOpenWriteId()) ?
            Long.MAX_VALUE : validWriteIdList.getMinOpenWriteId() - 1, validWriteIdList.getHighWatermark());
    Table table = metadataCache.computeIfAbsent(info.getFullTableName(), () -> resolveTable(info.dbname, info.tableName));
    LOG.debug("Cleaning based on writeIdList: {}", validWriteIdList);

    boolean success = cleanAndVerifyObsoleteDirectories(info, location, validWriteIdList, table);
    if (success || CompactorUtil.isDynPartAbort(table, info.partName)) {
      txnHandler.markCleaned(info);
    } else {
      LOG.warn("Leaving aborted entry {} in TXN_COMPONENTS table.", info);
    }

  }
}
