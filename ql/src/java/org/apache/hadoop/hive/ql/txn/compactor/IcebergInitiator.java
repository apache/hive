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

import com.google.common.collect.Maps;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A class to initiate Iceberg compactions. This will run in a separate thread.
 * It's critical that there exactly 1 of these in a given warehouse.
 */
public class IcebergInitiator extends InitiatorBase {
  static final private String CLASS_NAME = IcebergInitiator.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  protected HiveCache hiveCache;
  private HiveMetaStoreClient client;

  @Override
  public Set<CompactionInfo> findPotentialCompactions(long lastChecked, ShowCompactResponse currentCompactions, 
      Set<String> skipDBs, Set<String> skipTables) throws MetaException {
    Set<CompactionInfo> compactionTargets = new HashSet<>();
    try {
      SessionState sessionState = SessionState.get();
      if (sessionState == null) {
        sessionState = new SessionState(conf);
        SessionState.start(sessionState);
      }

      List<String> allDatabases = client.getAllDatabases();

      for (String dbName : allDatabases) {
        List<String> allTablesInDb = client.getAllTables(dbName);

        for (String tableName : allTablesInDb) {
          String qualifiedTableName = dbName + "." + tableName;
          Table table = metadataCache.computeIfAbsent(qualifiedTableName, () ->
              CompactorUtil.resolveTable(conf, dbName, tableName));

          if (MetaStoreUtils.isIcebergTable(table.getParameters())) {
            org.apache.hadoop.hive.ql.metadata.Table hiveTable = hiveCache.computeIfAbsent(qualifiedTableName, () ->
                Hive.get().getTable(dbName, tableName));

            HiveStorageHandler storageHandler = hiveTable.getStorageHandler();

            if (hiveTable.isPartitioned()) {
              List<org.apache.hadoop.hive.ql.metadata.Partition> partitions =
                  storageHandler.getPartitions(hiveTable, Maps.newHashMap(), true);

              for (org.apache.hadoop.hive.ql.metadata.Partition partition : partitions) {
                addCompactionTargetIfEligible(storageHandler, table, partition.getName(), compactionTargets,
                    currentCompactions, skipDBs, skipTables);
              }

              if (hiveTable.getStorageHandler().hasUndergonePartitionEvolution(hiveTable) && !hiveTable
                  .getStorageHandler().getPartitionsByExpr(hiveTable, null, false).isEmpty()) {
                addCompactionTargetIfEligible(storageHandler, table, null, compactionTargets,
                    currentCompactions, skipDBs, skipTables);
              }
            } else {
              addCompactionTargetIfEligible(storageHandler, table, null, compactionTargets,
                  currentCompactions, skipDBs, skipTables);
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Error while finding compaction targets for Iceberg tables", e);
      throw new MetaException(e.getMessage());
    }

    return compactionTargets;
  }

  @Override
  public TxnStore.MUTEX_KEY getMutexKey() {
    return TxnStore.MUTEX_KEY.IcebergInitiator;
  }

  @Override
  public void invalidateCaches() {
    metadataCache.invalidate();
    hiveCache.invalidate();
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
  public void init(AtomicBoolean stop) throws Exception {
    super.init(stop);
    client = new HiveMetaStoreClient(conf);
    hiveCache = new HiveCache(isCacheEnabled());
  }

  @Override
  protected Partition getPartition(CompactionInfo ci) throws MetaException, SemanticException {
    org.apache.hadoop.hive.ql.metadata.Table hiveTable = hiveCache.get(ci.getFullTableName());
    Map<String, String> partSpecMap = Warehouse.makeSpecFromName(ci.partName);
    return hiveCache.get(ci.getFullTableName()).getStorageHandler()
        .getPartition(hiveTable, partSpecMap).getTPartition();
  }

  private void addCompactionTargetIfEligible(HiveStorageHandler storageHandler, Table table, String partName,
      Set<CompactionInfo> compactions, ShowCompactResponse currentCompactions, Set<String> skipDBs, 
      Set<String> skipTables) throws IOException {
    CompactionInfo ci = 
        new CompactionInfo(table.getDbName(), table.getTableName(), partName, CompactionType.SMART_OPTIMIZE);
    if (isEligibleForCompaction(ci, currentCompactions, skipDBs, skipTables)) {
      CompactionType type = storageHandler.getEligibleCompactionType(table, ci);
      if (type != null) {
        ci.type = type;
        compactions.add(ci);
      } 
    }
  }
}
