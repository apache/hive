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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.util.Sets;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotChanges;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.mr.hive.IcebergTableUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The statistics a table stores - which file answers for a snapshot, and whether what it states
 * still holds.
 *
 * <p>A statistics file describes the snapshot it was written for. A read asks for a snapshot that
 * may be a later one, so finding the file is a walk back through the history to the newest one of
 * the granularity wanted, and judging it is a walk forward over what has been written since.
 *
 * <p>The walk back reads the table metadata alone, so a read that finds nothing to serve pays
 * almost nothing to find that out. The walk forward reads the manifests of the snapshots it
 * crosses, which is why it is bounded and why its answer is kept across queries.
 */
public final class IcebergStoredStats {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergStoredStats.class);

  private static final String CHANGED_PARTITIONS_KEY = "changedPartitions.%s.%d.%d.%d";
  /**
   * What changed between two snapshots is settled the moment the later one commits, so the answer
   * is kept across queries, and concurrent compilations share one walk. Bounded by the names held
   * across all entries, since one answer can name every partition of a wide table.
   */
  private static final Cache<String, Optional<Set<String>>> CHANGED_PARTITIONS =
      Caffeine.newBuilder()
          .maximumWeight(100_000)
          .weigher((String key, Optional<Set<String>> changed) ->
              changed.map(names -> Math.max(names.size(), 1)).orElse(1))
          .build();

  private IcebergStoredStats() {
  }

  /**
   * The statistics file only while it still describes the given snapshot: the walk to it stops at
   * any snapshot that changed what the statistics measure. For the nearest file ever written,
   * whatever happened since, see {@link #findColStatsFile}.
   */
  public static StatisticsFile getColStatsFile(Table table, long snapshotId, boolean partitionLevel) {
    // A rewrite leaves every row in the partition it was already in, and so separates statistics
    // from nothing - unless the table has evolved, where compaction selects the rows of the older
    // specs and writes them under the current one. That is the same condition the compactor
    // branches on, and only per partition statistics can tell the difference.
    boolean rewritesKeepPartitions = !partitionLevel || table.specs().size() == 1;
    return colStatsFileOf(
        table, snapshotId, partitionLevel,
        snapshot -> changesPartitions(snapshot, rewritesKeepPartitions));
  }

  /**
   * The nearest column statistics file describing the snapshot: its own, or the closest
   * ancestor's. Statistics of an ancestor describe an earlier state of the data, which
   * {@link #colStatsAccurate} reports and the planner treats as partial.
   */
  public static StatisticsFile findColStatsFile(Table table, long snapshotId, Configuration conf) {
    return findColStatsFile(table, snapshotId, IcebergTableUtil.isPartitionStats(table, conf));
  }

  public static StatisticsFile findColStatsFile(Table table, long snapshotId, boolean partitionLevel) {
    // a snapshot holding no rows (truncate) ends the walk: what precedes it didn't survive
    return colStatsFileOf(
        table, snapshotId, partitionLevel, IcebergTableUtil::isEmptySnapshot);
  }

  /**
   * The newest column statistics file of the asked granularity, at the snapshot or at an ancestor
   * of it, up to the one {@code last} names.
   */
  private static StatisticsFile colStatsFileOf(Table table, long snapshotId, boolean partitionLevel,
      Predicate<Snapshot> last) {
    return statsFileOf(table, snapshotId, stats -> holdsHiveColStats(stats, partitionLevel), last);
  }

  /**
   * The newest statistics file the given test admits, at the snapshot or at an ancestor of it, up
   * to the one {@code last} names.
   */
  private static StatisticsFile statsFileOf(Table table, long snapshotId,
      Predicate<StatisticsFile> holds, Predicate<Snapshot> last) {
    if (table.statisticsFiles().isEmpty()) {
      return null;
    }
    for (Snapshot snapshot = table.snapshot(snapshotId); snapshot != null;
        snapshot = snapshot.parentId() != null ? table.snapshot(snapshot.parentId()) : null) {
      long walked = snapshot.snapshotId();
      StatisticsFile statsFile = table.statisticsFiles().stream()
          .filter(stats -> stats.snapshotId() == walked)
          .filter(holds)
          .findAny().orElse(null);
      if (statsFile != null) {
        return statsFile;
      }
      if (last.test(snapshot)) {
        return null;
      }
    }
    return null;
  }

  /**
   * The file whose blobs are Hive's own - Iceberg keeps statistics of its own in the same format -
   * at the asked-for granularity: a blob describing one partition names it in its metadata.
   *
   * <p>A file that holds any partition is a per partition one, whatever else it holds. The entries
   * it aggregates from them state the table only while it holds every partition, which a gather of some
   * of them does not, so a whole-table read passes it by and takes the file gathered as one.
   */
  private static boolean holdsHiveColStats(StatisticsFile stats, boolean partitionLevel) {
    boolean holdsPartitions = stats.blobMetadata().stream()
        .anyMatch(metadata -> metadata.properties().containsKey(IcebergColStatsWriter.PARTITION_FIELD));
    if (partitionLevel) {
      return holdsPartitions && stats.blobMetadata().stream().anyMatch(
          metadata -> IcebergColStatsWriter.HIVE_PART_COL_STATS_BLOB_V1.equals(metadata.type()));
    }
    return !holdsPartitions && stats.blobMetadata().stream().anyMatch(
        metadata -> IcebergColStatsWriter.HIVE_COL_STATS_BLOB_V1.equals(metadata.type()) ||
            IcebergColStatsWriter.LEGACY_COL_STATS_BLOB.equals(metadata.type()));
  }

  /**
   * The distinct count each column's blob states for the snapshot, from the newest file that still
   * describes it. A blob states one whatever wrote it: Hive's own, and the sketch blobs Iceberg's
   * statistics are written as, name it the same way, so a table another engine analyzed is read
   * here too. Nothing is opened - the counts travel in the metadata that names the blobs.
   */
  public static Map<Integer, Long> readStatedNdvs(Table table, long snapshotId) {
    StatisticsFile statsFile = statsFileOf(table, snapshotId, IcebergStoredStats::statesNdv,
        snapshot -> !DataOperations.REPLACE.equals(snapshot.operation()));
    if (statsFile == null) {
      return Map.of();
    }
    Map<Integer, Long> ndvs = Maps.newHashMap();
    for (org.apache.iceberg.BlobMetadata blob : statsFile.blobMetadata()) {
      if (blob.properties().containsKey(IcebergColStatsWriter.PARTITION_FIELD) || blob.fields().size() != 1) {
        continue;
      }
      IcebergColStatsProperties.ndv(blob).ifPresent(ndv -> ndvs.put(blob.fields().get(0), ndv));
    }
    return ndvs;
  }

  /** Whether any of the file's table-level blobs states a distinct count. */
  private static boolean statesNdv(StatisticsFile stats) {
    return stats.blobMetadata().stream()
        .anyMatch(blob -> !blob.properties().containsKey(IcebergColStatsWriter.PARTITION_FIELD) &&
            IcebergColStatsProperties.ndv(blob).isPresent());
  }

  /**
   * Whether the stored column statistics still describe the table: the current snapshot owns them,
   * or only row-preserving commits (compaction) separate it from the snapshot that does. Derived
   * from the table metadata, so it holds for the writes of every engine.
   */
  public static boolean colStatsAccurate(Table table, Snapshot snapshot, Configuration conf) {
    return colStatsAccurate(table, snapshot, IcebergTableUtil.isPartitionStats(table, conf));
  }

  /** The same, judging the granularity the caller goes on to read. */
  public static boolean colStatsAccurate(Table table, Snapshot snapshot, boolean partitionLevel) {
    return getColStatsFile(table, snapshot.snapshotId(), partitionLevel) != null;
  }

  /**
   * The partitions written since a snapshot. Null when the walk cannot answer - an expired
   * snapshot broke the chain, or the bound was reached - and every partition then counts as
   * changed. The bound is on manifests read, not on snapshots walked.
   */
  static Set<String> partitionsChangedSince(Table table, Snapshot snapshot, long sinceSnapshotId,
      Configuration conf, boolean capped) {
    // the bound is what a read will wait for; a write settles its file for good, so it walks the
    // whole way. It keys the answer, since sessions may bound the same walk differently
    int snapshotLookback = capped ?
        HiveConf.getIntVar(conf, ConfVars.HIVE_ICEBERG_STATS_MAX_SNAPSHOT_LOOKBACK) : Integer.MAX_VALUE;
    // the walk reads manifests, and one query can ask it more than once: a table scanned twice
    // over, or a DESC that asks column by column
    String cacheKey = CHANGED_PARTITIONS_KEY
        .formatted(table.name(), snapshot.snapshotId(), sinceSnapshotId, snapshotLookback);
    return CHANGED_PARTITIONS.get(cacheKey,
        key -> Optional.ofNullable(
            computePartitionsChangedSince(table, snapshot, sinceSnapshotId, snapshotLookback)))
        .orElse(null);
  }

  private static Set<String> computePartitionsChangedSince(Table table, Snapshot snapshot,
      long sinceSnapshotId, int snapshotLookback) {
    boolean rewritesKeepPartitions = table.specs().size() == 1;
    if (!canTraceChangesSince(table, snapshot, sinceSnapshotId, rewritesKeepPartitions, snapshotLookback)) {
      return null;
    }
    Set<String> changed = Sets.newHashSet();
    for (Snapshot current = snapshot; current.snapshotId() != sinceSnapshotId;
        current = table.snapshot(current.parentId())) {
      if (changesPartitions(current, rewritesKeepPartitions) &&
          !collectChangedPartitions(table, current, changed)) {
        return null;
      }
    }
    return changed;
  }

  /**
   * Whether what happened since the snapshot can be traced: it sits on the history walked, and no
   * more than the bound allows separates the two. The chain is in the table metadata, so refusing
   * a walk over the bound - or off the history - costs no manifest read.
   */
  private static boolean canTraceChangesSince(Table table, Snapshot snapshot, long sinceSnapshotId,
      boolean rewritesKeepPartitions, int snapshotLookback) {
    int toRead = 0;
    Snapshot current = snapshot;
    while (current != null && current.snapshotId() != sinceSnapshotId) {
      if (changesPartitions(current, rewritesKeepPartitions) && ++toRead > snapshotLookback) {
        LOG.info("Over {} snapshots of {} separate the read from the statistics written at snapshot {}: " +
            "they cannot be judged", snapshotLookback, table.name(), sinceSnapshotId);
        return false;
      }
      Long parentId = current.parentId();
      current = parentId != null ? table.snapshot(parentId) : null;
    }
    // the file has to sit on the history walked, or what happened in between is unknown
    return current != null;
  }

  /**
   * A rewrite (REPLACE) leaves every row in the partition it was already in, unless the table has
   * evolved: compaction then selects the rows of the older specs and writes them under the current
   * one. An overwrite of partitions commits OVERWRITE, despite the name of its API.
   */
  private static boolean changesPartitions(Snapshot snapshot, boolean rewritesKeepPartitions) {
    return !rewritesKeepPartitions || !DataOperations.REPLACE.equals(snapshot.operation());
  }

  /**
   * Records which partitions a snapshot changed, naming each file under its own spec. False when
   * a delete of no partition is reached, since it applies to the rows of every one and names none.
   * Stored entries are named the same way: an ANALYZE names each group after the file its rows came from,
   * and a write only ever lands in a partition of the spec current when it ran.
   */
  private static boolean collectChangedPartitions(Table table, Snapshot snapshot, Set<String> changed) {
    for (ContentFile<?> file : changedFiles(table, snapshot)) {
      PartitionSpec spec = table.specs().get(file.specId());
      // an equality delete of no partition applies to the rows of every one, and names none of them
      if (file.content() != FileContent.DATA && !spec.isPartitioned() && table.spec().isPartitioned()) {
        return false;
      }
      changed.add(IcebergTableUtil.toPartitionName(spec, file.partition()));
    }
    return true;
  }

  /** Every file a snapshot added or removed, whether it holds rows or deletes them. */
  private static Iterable<? extends ContentFile<?>> changedFiles(Table table, Snapshot snapshot) {
    SnapshotChanges changes = SnapshotChanges.builderFor(table).snapshot(snapshot).build();
    return Iterables.concat(
        changes.addedDataFiles(), changes.removedDataFiles(),
        changes.addedDeleteFiles(), changes.removedDeleteFiles());
  }

  /**
   * Whether a stored entry still describes its partition. A file describes the snapshot it was
   * written for, so only what happened after it matters. False for all of them when the writes in
   * between cannot be traced.
   */
  public static Predicate<String> upToDateColStats(Table table, Snapshot snapshot,
      StatisticsFile statsFile, Configuration conf, boolean capped) {
    Set<String> changed =
        partitionsChangedSince(table, snapshot, statsFile.snapshotId(), conf, capped);
    return partition -> changed != null && !changed.contains(partition);
  }

  public static PartitionStatisticsFile getPartitionStatsFile(Table table, long snapshotId) {
    return table.partitionStatisticsFiles().stream()
      .filter(stats -> stats.snapshotId() == snapshotId)
      .findAny().orElse(null);
  }
}
