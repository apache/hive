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

import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.TxnCommonUtils;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class AcidTableOptimizer extends TableOptimizer {
  private static final String CLASS_NAME = AcidTableOptimizer.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  public AcidTableOptimizer(HiveConf conf, TxnStore txnHandler, MetadataCache metadataCache) {
    super(conf, txnHandler, metadataCache);
  }

  @Override
  public Set<CompactionInfo> findPotentialCompactions(long lastChecked, ShowCompactResponse currentCompactions, 
      Set<String> skipDBs, Set<String> skipTables) throws MetaException {

    int abortedThreshold = HiveConf.getIntVar(conf,
        HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD);
    long abortedTimeThreshold = HiveConf
        .getTimeVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_TIME_THRESHOLD,
            TimeUnit.MILLISECONDS);

    Set<CompactionInfo> potentials = txnHandler.findPotentialCompactions(abortedThreshold, abortedTimeThreshold, lastChecked)
            .parallelStream()
            .filter(ci -> isEligibleForCompaction(ci, currentCompactions, skipDBs, skipTables))
            .collect(Collectors.toSet());

    if (!potentials.isEmpty()) {
      ValidTxnList validTxnList = TxnCommonUtils.createValidReadTxnList(
          txnHandler.getOpenTxns(), 0);
      conf.set(ValidTxnList.VALID_TXNS_KEY, validTxnList.writeToString());
    }
    
    return potentials;
  }

  @Override
  protected boolean isEligibleForCompaction(CompactionInfo ci, ShowCompactResponse currentCompactions, 
      Set<String> skipDBs, Set<String> skipTables) {
    try {
      if (!super.isEligibleForCompaction(ci, currentCompactions, skipDBs, skipTables)) {
        return false;
      }
      String qualifiedTableName = ci.getFullTableName();
      Table t = metadataCache.computeIfAbsent(qualifiedTableName, () -> 
          CompactorUtil.resolveTable(conf, ci.dbname, ci.tableName));
      if (AcidUtils.isInsertOnlyTable(t.getParameters()) && !HiveConf
          .getBoolVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_COMPACT_MM)) {
        skipTables.add(ci.getFullTableName());
        LOG.info("Table {} is insert only and {}=false so we will not compact it.", qualifiedTableName,
            HiveConf.ConfVars.HIVE_COMPACTOR_COMPACT_MM.varname);
        return false;
      }
      if (isDynPartIngest(t, ci)) {
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

  // Check if it's a dynamic partitioning case. If so, do not initiate compaction for streaming ingest, only for aborts.
  protected static boolean isDynPartIngest(Table t, CompactionInfo ci) {
    if (t.getPartitionKeys() != null && !t.getPartitionKeys().isEmpty() &&
        ci.partName  == null && !ci.hasOldAbort) {
      LOG.info("Skipping entry for {} as it is from dynamic partitioning", ci.getFullTableName());
      return true;
    }
    return false;
  }
}
