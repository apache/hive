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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.txn.compactor.MetadataCache;
import org.apache.hadoop.hive.ql.txn.compactor.TableOptimizer;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.hive.RuntimeMetaException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.mr.hive.IcebergTableUtil;
import org.apache.iceberg.mr.hive.compaction.evaluator.CompactionEvaluator;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

public class IcebergTableOptimizer extends TableOptimizer {
  private HiveMetaStoreClient client;
  private Map<String, Long> snapshotTimeMilCache;

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

    Iterable<Table> tables = getTables(skipDBs, skipTables);

    for (Table table : tables) {
      org.apache.hadoop.hive.ql.metadata.Table hiveTable = new org.apache.hadoop.hive.ql.metadata.Table(table);
      org.apache.iceberg.Table icebergTable = IcebergTableUtil.getTable(conf, table);
      String qualifiedTableName = hiveTable.getFullyQualifiedName();

      if (hasNewCommits(icebergTable, snapshotTimeMilCache.get(qualifiedTableName))) {
        if (icebergTable.spec().isPartitioned()) {
          List<org.apache.hadoop.hive.ql.metadata.Partition> partitions = findModifiedPartitions(hiveTable,
              icebergTable, snapshotTimeMilCache.get(qualifiedTableName));

          partitions.forEach(partition -> addCompactionTargetIfEligible(table, icebergTable,
              partition.getName(), compactionTargets, currentCompactions, skipDBs, skipTables));

          if (IcebergTableUtil.hasUndergonePartitionEvolution(icebergTable) && hasModifiedPartitions(icebergTable,
              snapshotTimeMilCache.get(qualifiedTableName))) {
            addCompactionTargetIfEligible(table, icebergTable,
                null, compactionTargets, currentCompactions, skipDBs, skipTables);
          }
        } else {
          addCompactionTargetIfEligible(table, icebergTable, null, compactionTargets,
              currentCompactions, skipDBs, skipTables);
        }

        snapshotTimeMilCache.put(qualifiedTableName, icebergTable.currentSnapshot().timestampMillis());
      }
    }

    return compactionTargets;
  }

  private Iterable<Table> getTables(Set<String> skipDBs, Set<String> skipTables) {
    try {
      int maxBatchSize = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX);
      return IcebergTableUtil.getTableFetcher(client, null, "*", null).getTables(skipDBs, skipTables, maxBatchSize);
    } catch (Exception e) {
      throw new RuntimeMetaException(e, "Error getting tables");
    }
  }

  public void init() throws MetaException {
    client = new HiveMetaStoreClient(new HiveConf());
    snapshotTimeMilCache = Maps.newConcurrentMap();
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

  private <R> R findModifiedPartitionsInternal(
      org.apache.iceberg.Table icebergTable,
      Long pastSnapshotTimeMil,
      Boolean latestSpecOnly,
      Supplier<R> resultSupplier,
      BiFunction<R, Set<String>, Boolean> resultConsumer
  ) {

    List<Snapshot> relevantSnapshots =
        getRelevantSnapshots(icebergTable, pastSnapshotTimeMil).toList();

    R result = resultSupplier.get();
    if (relevantSnapshots.isEmpty()) {
      return result;
    }

    List<Callable<Set<String>>> tasks =
        createPartitionNameTasks(icebergTable, relevantSnapshots, latestSpecOnly);

    try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
      CompletionService<Set<String>> cs = new ExecutorCompletionService<>(executor);

      // submit tasks
      for (Callable<Set<String>> task : tasks) {
        cs.submit(task);
      }

      // process results
      for (int i = 0; i < tasks.size(); i++) {
        if (resultConsumer.apply(result, cs.take().get())) {
          return (R) Boolean.TRUE; // short-circuit
        }
      }

      return result;

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeMetaException(
          e, "Interrupted while processing modified partitions");
    } catch (ExecutionException e) {
      throw new RuntimeMetaException(
          e, "Failed to process modified partitions in parallel");
    }
  }

  private boolean hasModifiedPartitions(
      org.apache.iceberg.Table icebergTable,
      Long pastSnapshotTimeMil) {

    return findModifiedPartitionsInternal(
        icebergTable,
        pastSnapshotTimeMil,
        false,
        () -> false,
        (ignored, partitions) -> !partitions.isEmpty()
    );
  }

  private List<Partition> findModifiedPartitions(
      org.apache.hadoop.hive.ql.metadata.Table hiveTable,
      org.apache.iceberg.Table icebergTable,
      Long pastSnapshotTimeMil) {

    Set<String> modifiedPartitions = findModifiedPartitionsInternal(
        icebergTable,
        pastSnapshotTimeMil,
        true,
        Sets::newHashSet,
        (acc, partitions) -> {
          acc.addAll(partitions);
          return false; // never short-circuit
        }
    );

    return IcebergTableUtil.convertNameToMetastorePartition(
        hiveTable, modifiedPartitions);
  }

  private List<Callable<Set<String>>> createPartitionNameTasks(
      org.apache.iceberg.Table icebergTable,
      List<Snapshot> relevantSnapshots,
      Boolean latestSpecOnly) {

    return relevantSnapshots.stream()
        .map(snapshot -> (Callable<Set<String>>) () ->
            IcebergTableUtil.getPartitionNames(
                icebergTable,
                getAffectedFiles(snapshot, icebergTable.io()),
                latestSpecOnly))
        .toList();
  }

  private List<ContentFile<?>> getAffectedFiles(Snapshot snapshot, FileIO io) {
    return FluentIterable.<ContentFile<?>>concat(
            snapshot.addedDataFiles(io),
            snapshot.removedDataFiles(io),
            snapshot.addedDeleteFiles(io),
            snapshot.removedDeleteFiles(io))
        .toList();
  }

  /**
   * Checks if a table has had new commits since a given snapshot that were not caused by compaction.
   * @param icebergTable The Iceberg table to check.
   * @param pastSnapshotTimeMil The timestamp in milliseconds of the snapshot to check from (exclusive).
   * @return true if at least one non-compaction snapshot exists since the pastSnapshotTimeMil
   * whose source is not compaction, false otherwise.
   */
  private boolean hasNewCommits(org.apache.iceberg.Table icebergTable, Long pastSnapshotTimeMil) {
    return getRelevantSnapshots(icebergTable, pastSnapshotTimeMil)
        .findAny().isPresent();
  }

  private Stream<Snapshot> getRelevantSnapshots(org.apache.iceberg.Table icebergTable, Long pastSnapshotTimeMil) {
    Snapshot currentSnapshot = icebergTable.currentSnapshot();
    if (currentSnapshot == null || Objects.equals(currentSnapshot.timestampMillis(), pastSnapshotTimeMil)) {
      return Stream.empty();
    }

    return StreamSupport.stream(icebergTable.snapshots().spliterator(), false)
        .filter(s -> pastSnapshotTimeMil == null || s.timestampMillis() > pastSnapshotTimeMil)
        .filter(s -> s.timestampMillis() <= currentSnapshot.timestampMillis())
        .filter(s -> !s.operation().equals(DataOperations.REPLACE));
  }
}
