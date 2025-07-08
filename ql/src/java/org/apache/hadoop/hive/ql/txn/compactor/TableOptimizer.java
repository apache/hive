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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;

import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.metastore.HMSHandler.getMSForConf;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

public abstract class TableOptimizer {
  private static final String CLASS_NAME = TableOptimizer.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  public abstract Set<CompactionInfo> findPotentialCompactions(long lastChecked, ShowCompactResponse currentCompactions,
    Set<String> skipDBs, Set<String> skipTables) throws MetaException;

  protected final HiveConf conf;
  protected final TxnStore txnHandler;
  protected final MetadataCache metadataCache;

  protected TableOptimizer(HiveConf conf, TxnStore txnHandler, MetadataCache metadataCache) {
    this.conf = conf;
    this.txnHandler = txnHandler;
    this.metadataCache = metadataCache;
  }
  
  protected boolean isEligibleForCompaction(CompactionInfo ci, ShowCompactResponse currentCompactions,
      Set<String> skipDBs, Set<String> skipTables) {
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

      String qualifiedTableName = ci.getFullTableName();
      if (skipTables.contains(qualifiedTableName)) {
        return false;
      }

      LOG.info("Checking to see if we should compact {}", ci.getFullPartitionName());

      // Check if we have already initiated or are working on a compaction for this table/partition.
      // Also make sure we haven't exceeded configured number of consecutive failures.
      // If any of the above applies, skip it.
      // Note: if we are just waiting on cleaning we can still check, as it may be time to compact again even though we haven't cleaned.
      if (foundCurrentOrFailedCompactions(currentCompactions, ci)) {
        return false;
      }

      Table t = metadataCache.computeIfAbsent(qualifiedTableName, () -> 
          CompactorUtil.resolveTable(conf, ci.dbname, ci.tableName));
      if (t == null) {
        LOG.info("Can't find table {}, assuming it's a temp table or has been dropped and moving on.",
            qualifiedTableName);
        return false;
      }

      if (replIsCompactionDisabledForTable(t)) {
        skipTables.add(qualifiedTableName);
        return false;
      }

      Map<String, String> dbParams = metadataCache.computeIfAbsent(ci.dbname, () -> resolveDatabase(ci)).getParameters();
      if (MetaStoreUtils.isNoAutoCompactSet(dbParams, t.getParameters())) {
        if (Boolean.parseBoolean(MetaStoreUtils.getNoAutoCompact(dbParams))) {
          skipDBs.add(ci.dbname);
          LOG.info("DB {} marked {}=true so we will not compact it.", hive_metastoreConstants.NO_AUTO_COMPACT, ci.dbname);
        } else {
          skipTables.add(qualifiedTableName);
          LOG.info("Table {} marked {}=true so we will not compact it.", hive_metastoreConstants.NO_AUTO_COMPACT,
              qualifiedTableName);
        }
        return false;
      }
    } catch (Exception e) {
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

  private boolean replIsCompactionDisabledForTable(Table tbl) {
    // Compaction is disabled until after first successful incremental load. Check HIVE-21197 for more detail.
    boolean isCompactDisabled = ReplUtils.isFirstIncPending(tbl.getParameters());
    if (isCompactDisabled) {
      LOG.info("Compaction is disabled for table {}", tbl.getTableName());
    }
    return isCompactDisabled;
  }

  private boolean replIsCompactionDisabledForDatabase(String dbName) throws TException {
    try {
      Database database = getMSForConf(conf).getDatabase(getDefaultCatalog(conf), dbName);
      // Compaction is disabled until after first successful incremental load. Check HIVE-21197 for more detail.
      boolean isReplCompactDisabled = ReplUtils.isFirstIncPending(database.getParameters());
      if (isReplCompactDisabled) {
        LOG.info("Compaction is disabled for database {}", dbName);
      }
      return isReplCompactDisabled;
    } catch (NoSuchObjectException e) {
      LOG.info("Unable to find database {}", dbName);
      return true;
    }
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

      LOG.info("Found currently initiated or working compaction for {} so we will not initiate another compaction",
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
      LOG.warn("Will not initiate compaction for {} since last {} attempts to compact it failed.",
          ci.getFullPartitionName(), MetastoreConf.ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD);

      ci.errorMessage = "Compaction is not initiated since last " +
          MetastoreConf.ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD + " consecutive compaction attempts failed)";

      txnHandler.markFailed(ci);
      return true;
    }
    return false;
  }

  protected Database resolveDatabase(CompactionInfo ci) throws MetaException, NoSuchObjectException {
    return CompactorUtil.resolveDatabase(conf, ci.dbname);
  }

  protected void invalidateCache() {
  }
}
