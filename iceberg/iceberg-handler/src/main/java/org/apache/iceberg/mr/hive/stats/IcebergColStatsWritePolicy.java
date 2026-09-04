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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.stats;

import java.util.function.BooleanSupplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.ql.Context.RewritePolicy;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorContext;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.mr.hive.IcebergTableUtil;
import org.apache.iceberg.mr.hive.compaction.IcebergCompactionService;

/**
 * What a write does to the stored column statistics: replace them, merge into them, or leave
 * them alone. They live in one file written whole, so every write is one of these three.
 */
public enum IcebergColStatsWritePolicy {
  /** Write the computed statistics, discarding the stored ones. */
  REPLACE,
  /**
   * Write the computed statistics completed by the stored ones: at table level the write's rows
   * add to them, at partition level its partitions replace theirs and the rest carry over.
   */
  MERGE,
  /** Write nothing: leave the stored statistics as they are. */
  SKIP;

  /**
   * Everything the decision reads, so that it can be made without a session. {@code statsAccurate}
   * is a supplier because answering it walks the table's metadata, and most cases never ask.
   */
  record Inputs(
      // what this write computed
      boolean tableWideStats,
      // the table it computed them for, and what it already holds for this snapshot
      boolean keepsStatsPerPartition,
      boolean partitioned,
      BooleanSupplier statsAccurate,
      // the statement that computed them
      boolean analyze,
      boolean analyzePartition,
      boolean compaction,
      boolean majorCompaction,
      boolean fullTableRewrite,
      boolean singlePartitionRewrite,
      // what its commit did to the rows
      boolean holdsOnlyAddedRows,
      boolean emptySnapshot,
      boolean wroteNoRows,
      boolean replacePartitions) {

    boolean isStatsAccurate() {
      return statsAccurate.getAsBoolean();
    }
  }

  /**
   * What to do with the statistics a write computed.
   *
   * @param tbl the table the statistics were computed for
   * @param snapshot the snapshot the statistics describe: the table's current one, or a branch head
   * @param head the first of the computed entries, which states whether they are of the table or
   *     of its partitions
   * @param conf the configuration the statement runs under
   */
  static IcebergColStatsWritePolicy resolve(
      Table tbl, Snapshot snapshot, ColumnStatistics head, Configuration conf) {

    return resolve(new Inputs(
        head.getStatsDesc().isIsTblLevel(),
        IcebergTableUtil.isPartitionStats(tbl, conf),
        tbl.spec().isPartitioned(),
        () -> IcebergStoredStats.colStatsAccurate(tbl, snapshot, conf),
        isAnalyze(conf),
        isAnalyzePartition(conf),
        isCompaction(),
        isMajorCompaction(conf),
        isFullTableRewrite(conf),
        isSinglePartitionRewrite(conf),
        holdsOnlyAddedRows(snapshot),
        IcebergTableUtil.isEmptySnapshot(snapshot),
        wroteNoRows(conf),
        Boolean.parseBoolean(
            snapshot.summary().get(SnapshotSummary.REPLACE_PARTITIONS_PROP))));
  }

  /** The decision itself, over the facts alone, so that every case of it can be stated as one. */
  static IcebergColStatsWritePolicy resolve(Inputs write) {
    if (!write.tableWideStats()) {
      return resolveForPartitions(write);
    }
    if (write.keepsStatsPerPartition()) {
      // the table stores its statistics per partition; these describe it as a whole
      return SKIP;
    }
    if (write.compaction()) {
      // Compaction changes no rows, so only a whole-table one has read enough to refresh stale stats.
      return write.fullTableRewrite() && write.majorCompaction() && !write.isStatsAccurate() ?
          REPLACE : SKIP;
    }
    // ANALYZE reads the whole table, so it replaces.
    return write.analyze() ? REPLACE : resolveForWrite(write);
  }

  /** What a statement does to the file holding one blob per partition. */
  private static IcebergColStatsWritePolicy resolveForPartitions(Inputs write) {
    if (write.analyze()) {
      // what it read is what it named, not what the snapshot it reads happens to hold
      return write.analyzePartition() ? MERGE : REPLACE;
    }
    if (write.compaction()) {
      // a partition read whole describes itself; part of one describes none of it
      return write.singlePartitionRewrite() && write.majorCompaction() ? MERGE : SKIP;
    }
    if (write.holdsOnlyAddedRows() || write.emptySnapshot()) {
      return REPLACE;
    }
    if (write.wroteNoRows() || write.isStatsAccurate()) {
      return SKIP;
    }
    // only a write that replaced its partitions measured everything they now hold; an insert
    // measured rows it added to partitions holding more, so it may not stand in for them
    return write.replacePartitions() ? MERGE : SKIP;
  }

  /** What an INSERT, INSERT OVERWRITE or CTAS does to the stored statistics. */
  private static IcebergColStatsWritePolicy resolveForWrite(Inputs write) {
    if (write.emptySnapshot()) {
      // The table is now empty, so stats of the rows it held must go. Must precede the
      // wroteNoRows check, which an emptying write also matches.
      return REPLACE;
    }
    if (write.wroteNoRows() || write.isStatsAccurate()) {
      // Nothing to record: the statement wrote no rows, or this snapshot already has stats.
      return SKIP;
    }
    if (write.holdsOnlyAddedRows()) {
      // Every row came from this write: a CTAS, a whole-table INSERT OVERWRITE, or the first
      // INSERT after a TRUNCATE.
      return REPLACE;
    }
    if (write.replacePartitions()) {
      // Iceberg flags whole-table and partition overwrites alike, so the partition spec decides:
      // only an unpartitioned table had every row replaced.
      return write.partitioned() ? SKIP : REPLACE;
    }
    // An INSERT: its stats cover the rows it added, the stored ones cover the rest.
    return MERGE;
  }

  /**
   * Whether the table holds nothing but the rows this snapshot added. Iceberg carries the row
   * total across commits, so it stays above the added count while older rows remain. False if
   * either count is missing from the summary.
   */
  private static boolean holdsOnlyAddedRows(Snapshot snapshot) {
    String added = snapshot.summary().get(SnapshotSummary.ADDED_RECORDS_PROP);
    return added != null && added.equals(snapshot.summary().get(SnapshotSummary.TOTAL_RECORDS_PROP));
  }

  /** Whether the statement wrote no rows, as its file sink reported to the query state. */
  private static boolean wroteNoRows(Configuration conf) {
    return SessionStateUtil.getQueryState(conf)
        .map(qs -> qs.getNumModifiedRows() == 0)
        .orElse(false);
  }

  private static boolean isAnalyze(Configuration conf) {
    return SessionStateUtil.getQueryState(conf)
        .map(qs -> HiveOperation.ANALYZE_TABLE == qs.getHiveOperation())
        .orElse(false);
  }

  /** Whether the ANALYZE named the partitions it is for, leaving the rest of the table alone. */
  private static boolean isAnalyzePartition(Configuration conf) {
    return SessionStateUtil.getQueryState(conf).map(QueryState::isAnalyzePartition)
        .orElse(false);
  }

  private static boolean isCompaction() {
    return SessionState.get() != null && SessionState.get().isCompaction();
  }

  /** Whether the compaction read every file of what it was pointed at: a minor one skips by size. */
  private static boolean isMajorCompaction(Configuration conf) {
    return isCompaction() && conf.get(CompactorContext.COMPACTION_FILE_SIZE_THRESHOLD) == null;
  }

  /** Whether what it was pointed at was the whole table, which only an unpartitioned one is. */
  private static boolean isFullTableRewrite(Configuration conf) {
    return RewritePolicy.FULL_TABLE.name().equals(HiveConf.getVar(conf, ConfVars.REWRITE_POLICY));
  }

  /** Whether the compaction was pointed at one partition: a spec-evolution one carries PARTITION too. */
  private static boolean isSinglePartitionRewrite(Configuration conf) {
    return conf.get(IcebergCompactionService.PARTITION_NAME) != null;
  }
}
