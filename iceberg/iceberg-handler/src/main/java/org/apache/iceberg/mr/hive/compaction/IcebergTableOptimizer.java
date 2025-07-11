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

package org.apache.iceberg.mr.hive.compaction;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorUtil;
import org.apache.hadoop.hive.ql.txn.compactor.MetadataCache;
import org.apache.hadoop.hive.ql.txn.compactor.TableOptimizer;
import org.apache.iceberg.hive.RuntimeMetaException;
import org.apache.iceberg.mr.hive.IcebergTableUtil;
import org.apache.iceberg.mr.hive.compaction.evaluator.CompactionEvaluator;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

public class IcebergTableOptimizer extends TableOptimizer {
  private HiveMetaStoreClient client;
  private Map<String, Long> snapshotIdCache;

  public IcebergTableOptimizer(HiveConf conf, TxnStore txnHandler, MetadataCache metadataCache) throws MetaException {
    super(conf, txnHandler, metadataCache);
    init();
  }

  /**
   * Scans all databases and tables in the Hive Metastore to identify Iceberg tables
   * that are potential candidates for compaction.
   * <p>
   * The method filters tables based on provided databases and tables skip lists and a snapshot ID cache to avoid
   * re-processing tables that haven't changed. For eligible Iceberg tables, it determines
   * the appropriate compaction targets (table or specific partitions) and adds them to
   * the {@link CompactionInfo} set.
   * </p>
   * @param lastChecked A timestamp of previous auto compaction's invocation.
   * @param currentCompactions A {@link ShowCompactResponse} containing currently active or pending
   * compaction requests, used to avoid duplicates.
   * @param skipDBs A {@link Set} of database names to explicitly skip during the scan.
   * @param skipTables A {@link Set} of fully qualified table names to explicitly skip during the scan.
   * @return A {@link Set} of {@link CompactionInfo} objects representing tables and/or partitions
   * identified as eligible for compaction.
   * during {@link SessionState} initialization.
   */
  @Override
  public Set<CompactionInfo> findPotentialCompactions(long lastChecked, ShowCompactResponse currentCompactions,
      Set<String> skipDBs, Set<String> skipTables) {
    Set<CompactionInfo> compactionTargets = Sets.newHashSet();

    getTables().stream()
        .filter(table -> !skipDBs.contains(table.getDb()))
        .filter(table -> !skipTables.contains(table.getNotEmptyDbTable()))
        .map(table -> Pair.of(table, resolveMetastoreTable(table.getNotEmptyDbTable())))
        .filter(tablePair -> MetaStoreUtils.isIcebergTable(tablePair.getValue().getParameters()))
        .filter(tablePair -> {
          long currentSnapshotId = Long.parseLong(tablePair.getValue().getParameters().get("current-snapshot-id"));
          Long cachedSnapshotId = snapshotIdCache.get(tablePair.getKey().getNotEmptyDbTable());
          return cachedSnapshotId == null || cachedSnapshotId != currentSnapshotId;
        })
        .forEach(tablePair -> {
          org.apache.iceberg.Table icebergTable = IcebergTableUtil.getTable(conf, tablePair.getValue());

          if (icebergTable.spec().isPartitioned()) {
            List<String> partitions = getPartitions(icebergTable);

            partitions.forEach(partition -> addCompactionTargetIfEligible(tablePair.getValue(), icebergTable,
                partition, compactionTargets, currentCompactions, skipDBs, skipTables));
          }

          if (icebergTable.spec().isUnpartitioned() || IcebergTableUtil.hasUndergonePartitionEvolution(icebergTable)) {
            addCompactionTargetIfEligible(tablePair.getValue(), icebergTable, null, compactionTargets,
                currentCompactions, skipDBs, skipTables);
          }

          snapshotIdCache.put(tablePair.getKey().getNotEmptyDbTable(), icebergTable.currentSnapshot().snapshotId());
        });

    return compactionTargets;
  }

  private List<org.apache.hadoop.hive.common.TableName> getTables() {
    try {
      return IcebergTableUtil.getTableFetcher(client, null, "*", null).getTables();
    } catch (Exception e) {
      throw new RuntimeMetaException(e, "Error getting table names");
    }
  }

  private List<String> getPartitions(org.apache.iceberg.Table icebergTable) {
    try {
      return IcebergTableUtil.getPartitionNames(icebergTable, Maps.newHashMap(), true);
    } catch (SemanticException e) {
      throw new RuntimeMetaException(e, "Error getting partition names for Iceberg table %s", icebergTable.name());
    }
  }

  private Table resolveMetastoreTable(String qualifiedTableName) {
    String[] dbTableName = TxnUtils.getDbTableName(qualifiedTableName);
    try {
      return metadataCache.computeIfAbsent(qualifiedTableName,
          () -> CompactorUtil.resolveTable(conf, dbTableName[0], dbTableName[1]));
    } catch (Exception e) {
      throw new RuntimeMetaException(e, "Error resolving table %s", qualifiedTableName);
    }
  }

  public void init() throws MetaException {
    client = new HiveMetaStoreClient(new HiveConf());
    snapshotIdCache = Maps.newConcurrentMap();
  }

  private void addCompactionTargetIfEligible(Table table, org.apache.iceberg.Table icebergTable, String partitionName,
      Set<CompactionInfo> compactions, ShowCompactResponse currentCompactions, Set<String> skipDBs,
      Set<String> skipTables) {

    CompactionInfo ci = new CompactionInfo(table.getDbName(), table.getTableName(), partitionName,
        CompactionType.SMART_OPTIMIZE);

    // Common Hive compaction eligibility checks
    if (!isEligibleForCompaction(ci, currentCompactions, skipDBs, skipTables)) {
      return;
    }

    // Iceberg specific compaction checks: determine if compaction is needed and which type is needed
    CompactionEvaluator compactionEvaluator;
    try {
      compactionEvaluator = new CompactionEvaluator(icebergTable, ci, table.getParameters());
    } catch (IOException e) {
      throw new RuntimeMetaException(e, "Error construction compaction evaluator for table %s", table.getTableName());
    }

    if (!compactionEvaluator.isEligibleForCompaction()) {
      return;
    }

    ci.type = compactionEvaluator.determineCompactionType();
    compactions.add(ci);
  }
}
