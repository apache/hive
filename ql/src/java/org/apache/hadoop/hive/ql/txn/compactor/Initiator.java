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
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.txn.TxnCommonUtils;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A class to initiate compactions.  This will run in a separate thread.
 * It's critical that there exactly 1 of these in a given warehouse.
 */
public class Initiator extends InitiatorBase {
  static final private String CLASS_NAME = Initiator.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  
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
  public TxnStore.MUTEX_KEY getMutexKey() {
    return TxnStore.MUTEX_KEY.Initiator;
  }

  @Override
  public void invalidateCaches() {
    metadataCache.invalidate();
  }

  @Override
  public String getClassName() {
    return CLASS_NAME;
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }

  @Override
  protected Partition getPartition(CompactionInfo ci) throws MetaException {
    return resolvePartition(ci);
  }

  protected boolean isEligibleForCompaction(CompactionInfo ci, ShowCompactResponse currentCompactions, 
      Set<String> skipDBs, Set<String> skipTables) {
    try {
      if (!super.isEligibleForCompaction(ci, currentCompactions, skipDBs, skipTables)) {
        return false;
      }
      Table t = metadataCache.computeIfAbsent(ci.getFullTableName(), () -> resolveTable(ci));
      if (AcidUtils.isInsertOnlyTable(t.getParameters()) && !HiveConf
          .getBoolVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_COMPACT_MM)) {
        skipTables.add(ci.getFullTableName());
        LOG.info("Table {} is insert only and {}=false so we will not compact it.", tableName(t),
            HiveConf.ConfVars.HIVE_COMPACTOR_COMPACT_MM.varname);
        return false;
      }
      if (isDynPartIngest(t, ci, LOG)) {
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
}
