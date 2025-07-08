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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.txn.compactor.MetadataCache;
import org.apache.hadoop.hive.ql.txn.compactor.TableOptimizer;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.hive.RuntimeMetaException;
import org.apache.iceberg.io.FileIO;
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
        .map(table -> {
          org.apache.hadoop.hive.ql.metadata.Table hiveTable = getHiveTable(table.getDb(), table.getTable());
          org.apache.iceberg.Table icebergTable = IcebergTableUtil.getTable(conf, hiveTable.getTTable());
          return Triple.of(table, hiveTable, icebergTable);
        })
        .filter(t -> {
          long currentSnapshotId = Long.parseLong(t.getMiddle().getParameters().get("current-snapshot-id"));
          Long cachedSnapshotId = snapshotIdCache.get(t.getLeft().getNotEmptyDbTable());
          return cachedSnapshotId == null || cachedSnapshotId != currentSnapshotId &&
              hasNonCompactionCommits(t.getRight(), cachedSnapshotId);
        })
        .forEach(t -> {
          String qualifiedTableName = t.getLeft().getNotEmptyDbTable();
          org.apache.hadoop.hive.ql.metadata.Table hiveTable = t.getMiddle();
          org.apache.iceberg.Table icebergTable = t.getRight();

          if (icebergTable.spec().isPartitioned()) {
            List<org.apache.hadoop.hive.ql.metadata.Partition> partitions = findModifiedPartitions(hiveTable,
                icebergTable, snapshotIdCache.get(qualifiedTableName), true);

            partitions.forEach(partition -> addCompactionTargetIfEligible(hiveTable.getTTable(), icebergTable,
                partition.getName(), compactionTargets, currentCompactions, skipDBs, skipTables));

            if (IcebergTableUtil.hasUndergonePartitionEvolution(icebergTable) && !findModifiedPartitions(hiveTable,
                icebergTable, snapshotIdCache.get(qualifiedTableName), false).isEmpty()) {
              addCompactionTargetIfEligible(hiveTable.getTTable(), icebergTable,
                  null, compactionTargets, currentCompactions, skipDBs, skipTables);
            }
          } else {
            addCompactionTargetIfEligible(hiveTable.getTTable(), icebergTable, null, compactionTargets,
                currentCompactions, skipDBs, skipTables);
          }

          snapshotIdCache.put(qualifiedTableName, icebergTable.currentSnapshot().snapshotId());
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

  private org.apache.hadoop.hive.ql.metadata.Table getHiveTable(String dbName, String tableName) {
    try {
      return Hive.get().getTable(dbName, tableName);
    } catch (Exception e) {
      throw new RuntimeMetaException(e, "Error getting Hive table");
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

  /**
   * Finds all unique non-compaction-modified partitions (with added or deleted files) between a given past
   * snapshot ID and the table's current (latest) snapshot.
   * @param hiveTable The {@link org.apache.hadoop.hive.ql.metadata.Table} instance to inspect.
   * @param pastSnapshotId The ID of the older snapshot (exclusive).
   * @param latestSpecOnly when True, returns partitions with the current spec only;
   *                       False - older specs only;
   *                       Null - any spec
   * @return A List of {@link org.apache.hadoop.hive.ql.metadata.Partition} representing the unique modified
   *                       partition names.
   * @throws IllegalArgumentException if snapshot IDs are invalid or out of order, or if the table has no current
   *                       snapshot.
   */
  private List<Partition> findModifiedPartitions(org.apache.hadoop.hive.ql.metadata.Table hiveTable,
      org.apache.iceberg.Table icebergTable, Long pastSnapshotId, Boolean latestSpecOnly) {
    Snapshot currentSnapshot = icebergTable.currentSnapshot();
    if (currentSnapshot == null) {
      throw new IllegalArgumentException(String.format("Table %s has no current snapshot. Cannot determine range.",
          icebergTable.name()));
    }
    Snapshot pastSnapshot = pastSnapshotId != null ? icebergTable.snapshot(pastSnapshotId) : null;

    List<Snapshot> relevantSnapshots = StreamSupport.stream(icebergTable.snapshots().spliterator(), false)
        .filter(s -> pastSnapshot == null || s.timestampMillis() > pastSnapshot.timestampMillis() &&
            s.timestampMillis() <= currentSnapshot.timestampMillis())
        .filter(s -> s.summary().get(IcebergTableUtil.SNAPSHOT_SOURCE_PROP) == null)
        .sorted(Comparator.comparingLong(Snapshot::timestampMillis))
        .toList();

    Set<String> modifiedPartitions = Sets.newHashSet();

    for (Snapshot snapshot : relevantSnapshots) {
      FileIO io = icebergTable.io();
      modifiedPartitions.addAll(IcebergTableUtil.getPartitionNames(icebergTable, snapshot.addedDataFiles(io),
          latestSpecOnly));
      modifiedPartitions.addAll(IcebergTableUtil.getPartitionNames(icebergTable, snapshot.removedDataFiles(io),
          latestSpecOnly));
      modifiedPartitions.addAll(IcebergTableUtil.getPartitionNames(icebergTable, snapshot.addedDeleteFiles(io),
          latestSpecOnly));
      modifiedPartitions.addAll(IcebergTableUtil.getPartitionNames(icebergTable, snapshot.removedDeleteFiles(io),
          latestSpecOnly));
    }

    return IcebergTableUtil.convertNameToMetastorePartition(hiveTable, modifiedPartitions);
  }

  /**
   * Checks if a table has had new commits since a given snapshot that were not caused by compaction.
   * @param icebergTable The Iceberg table to check.
   * @param pastSnapshotId The ID of the snapshot to check from (exclusive).
   * @return true if at least one non-compaction snapshot exists since the pastSnapshotId
   * whose source is not compaction, false otherwise.
   * @throws IllegalArgumentException if the table has no current snapshot.
   */
  private boolean hasNonCompactionCommits(org.apache.iceberg.Table icebergTable, Long pastSnapshotId) {
    Snapshot currentSnapshot = icebergTable.currentSnapshot();
    if (currentSnapshot == null) {
      throw new IllegalArgumentException(String.format("Table %s has no current snapshot. Cannot determine range.",
          icebergTable.name()));
    }
    Snapshot pastSnapshot = icebergTable.snapshot(pastSnapshotId);

    return StreamSupport.stream(icebergTable.snapshots().spliterator(), false)
        .filter(s -> pastSnapshot == null || s.timestampMillis() > pastSnapshot.timestampMillis() &&
            s.timestampMillis() <= currentSnapshot.timestampMillis())
        .filter(s -> !s.summary().containsValue(IcebergTableUtil.SnapshotSource.COMPACTION.name()))
        .anyMatch(s -> true);
  }
}
