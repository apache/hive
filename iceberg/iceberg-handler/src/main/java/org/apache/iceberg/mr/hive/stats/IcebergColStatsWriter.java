/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.stats;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinCompressionCodec;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes the column statistics an ANALYZE, or a write told to compute them, produced as the
 * table's statistics file, per the policy the write's facts resolve to: replacing the stored file,
 * merging into it by carrying what no write since has changed, or leaving it alone. The reading
 * side is {@link IcebergColStatsReader}.
 *
 * At table level the file holds one blob per column. At partition level it holds one blob per
 * partition, pulled and written one at a time so the whole of a large table's statistics is never
 * held at once. A partition blob holds one entry per column, each behind the field it is for and
 * its own length, so a read decodes only the columns it was asked for:
 *
 *   content := version, count, count x (field id, length, entry)
 *   entry   := one column's ColumnStatisticsObj, Thrift compact
 *
 * The blob is not compressed and every entry states its length, so a read steps over the columns
 * it did not ask about rather than decoding them. A table-level blob carries its entry alone,
 * with no frame around it.
 *
 * The frame travels under its own blob type, so a file of the older layout reads as absent rather
 * than wrong, and a version bump can change the frame without renaming the type.
 */
public final class IcebergColStatsWriter {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergColStatsWriter.class);

  /**
   * What a blob of one partition's entries is named. It holds a frame of its own rather than a
   * single entry, which is why it is named apart from a table-level one; the frame states its own
   * version, so changing the frame does not rename the type.
   */
  public static final String HIVE_PART_COL_STATS_BLOB_V1 = "hive-partition-column-statistics-v1";
  /** What a blob describing one partition names it under, in the metadata that stands for it. */
  public static final String PARTITION_FIELD = "partition";
  /**
   * What a table-level entry is named now that it holds a Thrift struct rather than a serialized
   * Java object. A reader that knows neither name reads it as absent, and one that knows both
   * reads whichever it finds, so nothing has to be recomputed to move between them.
   */
  public static final String HIVE_COL_STATS_BLOB_V1 = "hive-table-column-statistics-v1";
  /**
   * What a released writer named a table-level entry, holding a serialized Java object. Read,
   * never written: the name is what a file already on disk carries, so it cannot be changed, and
   * it stands apart from the names above, which are of a format of their own.
   */
  public static final String LEGACY_COL_STATS_BLOB = ColumnStatisticsObj.class.getSimpleName();
  /**
   * What a statistics file is called: what it holds, the snapshot it describes, and something to
   * tell two writes of one snapshot apart. Named for the kind of statistics rather than the grain
   * of them, since a partition-level file also carries the table-level entries aggregated from its
   * partitions. It sits beside the table's metadata, where Iceberg puts the statistics it writes
   * itself - {snapshot}-{uuid}.stats for a sketch, partition-stats-{snapshot}-{uuid} for the
   * partition counts - so ours says column to stand apart from both, and ends in the container
   * rather than saying stats a second time. FileFormat.PUFFIN knows that ending.
   */
  private static final String STATS_FILE = "column-stats-%d-%s.puffin";

  private IcebergColStatsWriter() {
  }

  /** Everything written describes the snapshot it is written for, so a read asks only what happened after it. */
  public static boolean write(Table tbl, Snapshot snapshot, Iterator<ColumnStatistics> colStats,
      Configuration conf) {
    ColumnStatistics head = colStats.next();

    IcebergColStatsWritePolicy policy = IcebergColStatsWritePolicy.resolve(tbl, snapshot, head, conf);
    if (policy == IcebergColStatsWritePolicy.SKIP) {
      // storing nothing says the stored statistics still stand, and a caller that hears otherwise
      // takes the mark of accuracy off the table - which leaves a good file unread
      boolean accurate = IcebergStoredStats.colStatsAccurate(tbl, snapshot, conf);
      LOG.info("Storing no column statistics for {} at snapshot {}: what was gathered describes" +
          " {}, and what is stored {}", tbl.name(), snapshot.snapshotId(),
          head.getStatsDesc().isIsTblLevel() ? "the whole table" : "a partition",
          accurate ? "still stands" : "does not");
      return accurate;
    }
    LOG.info("Storing column statistics of {} at snapshot {}: {} what was gathered, which describes {}",
        tbl.name(), snapshot.snapshotId(), policy,
        head.getStatsDesc().isIsTblLevel() ? "the whole table" : "a partition");
    Iterator<ColumnStatistics> all = Iterators.concat(Iterators.singletonIterator(head), colStats);
    try {
      return head.getStatsDesc().isIsTblLevel() ?
          writeTable(tbl, snapshot, all, policy) :
          writePart(tbl, snapshot, all, policy, conf);
    } catch (IOException | InvalidObjectException e) {
      // serving no stats degrades the planner to estimates - never wrong
      LOG.warn("Unable to write column stats", e);
      return false;
    }
  }

  private static boolean writeTable(Table tbl, Snapshot snapshot, Iterator<ColumnStatistics> colStats,
      IcebergColStatsWritePolicy policy) throws IOException, InvalidObjectException {
    // the table's statistics are one entry holding every column: nothing to stream
    ColumnStatistics stats = colStats.next();
    if (policy == IcebergColStatsWritePolicy.MERGE) {
      // A write commits a snapshot of its own, so what it completes sits on the one before it. An
      // ANALYZE commits none, but replaces rather than merges, so it never asks.
      Long parentId = snapshot.parentId();
      StatisticsFile statsOldSrc = parentId == null ? null :
          IcebergStoredStats.getColStatsFile(tbl, parentId, false);
      if (statsOldSrc == null) {
        // a table-level increment has nothing to add itself to
        return false;
      }
      List<ColumnStatisticsObj> statsOld = IcebergColStatsReader.readOrThrow(
          tbl, statsOldSrc, null, true);
      // drop columns the stored file does not describe: their stats cover only the inserted
      // rows, and with nothing to merge into they would stand as stats for the whole table
      Set<String> stored = statsOld.stream().map(ColumnStatisticsObj::getColName)
          .collect(Collectors.toSet());
      stats.getStatsObj().removeIf(obj -> !stored.contains(obj.getColName()));
      if (stats.getStatsObj().isEmpty()) {
        return false;
      }
      MetaStoreServerUtils.mergeColStats(stats, new ColumnStatistics(null, statsOld));
    }
    Schema schema = tbl.spec().schema();
    // a column dropped or renamed since the entry was stored resolves no field: its statistics
    // leave with it
    stats.getStatsObj().removeIf(obj -> schema.caseInsensitiveFindField(obj.getColName()) == null);

    return commitFile(tbl, snapshot, writer -> {
      for (ColumnStatisticsObj obj : stats.getStatsObj()) {
        // a column's statistics are one blob, sketches and all, as a partition's entries are one
        // entry: what a read wants of them it settles once they are in hand. The vector stays
        // whatever a read asks for, because a write needs it - an increment merges its own
        // gather into what is stored, and only a vector lets the distinct counts be merged
        writer.add(new Blob(
            HIVE_COL_STATS_BLOB_V1,
            List.of(schema.caseInsensitiveFindField(obj.getColName()).fieldId()),
            snapshot.snapshotId(), snapshot.sequenceNumber(),
            ByteBuffer.wrap(IcebergColStatsCodec.encodeEntry(obj)),
            PuffinCompressionCodec.NONE,
            // the count travels in the metadata, where a read takes it without opening the file
            IcebergColStatsProperties.of(obj)));
      }
      return List.of();
    });
  }

  private static boolean writePart(Table tbl, Snapshot snapshot, Iterator<ColumnStatistics> colStats,
      IcebergColStatsWritePolicy policy, Configuration conf) throws IOException, InvalidObjectException {
    Schema schema = tbl.spec().schema();
    Set<String> written = Sets.newHashSet();
    // an ANALYZE commits no snapshot of its own: it writes to the snapshot it read, where the
    // statistics already are, so the walk starts there rather than at the parent
    StatisticsFile statsOldSrc = policy == IcebergColStatsWritePolicy.MERGE ?
        IcebergStoredStats.findColStatsFile(tbl, snapshot.snapshotId(), true) : null;
    ColStatsAggregate aggregate = new ColStatsAggregate();

    return commitFile(tbl, snapshot, writer -> {
      Set<Integer> gatheredFieldIds = Sets.newLinkedHashSet();

      while (colStats.hasNext()) {
        ColumnStatistics stats = colStats.next();
        String partName = stats.getStatsDesc().getPartName();
        if (partName == null) {
          // a group naming no partition describes none
          continue;
        }
        // a column dropped or renamed since the entry was stored resolves no field: its
        // statistics leave with it
        stats.getStatsObj().removeIf(obj -> schema.caseInsensitiveFindField(obj.getColName()) == null);
        List<Integer> fieldIds = stats.getStatsObj().stream()
            .map(obj -> schema.caseInsensitiveFindField(obj.getColName()).fieldId()).toList();
        gatheredFieldIds.addAll(fieldIds);
        // a blob names its fields itself, entry by entry; the fields the whole file states ride
        // its registered entry, where a read takes them from the table's metadata without opening
        // anything
        writer.add(new Blob(
            HIVE_PART_COL_STATS_BLOB_V1, List.of(),
            snapshot.snapshotId(), snapshot.sequenceNumber(),
            encodePartBlob(stats.getStatsObj(), fieldIds),
            PuffinCompressionCodec.NONE,
            Map.of(PARTITION_FIELD, partName)));
        written.add(partName);
        aggregate.addPartition(stats.getStatsObj());
      }
      if (policy == IcebergColStatsWritePolicy.MERGE) {
        carryForward(tbl, snapshot, writer, written, conf, statsOldSrc, aggregate);
      }
      // the table's own entries, aggregated from every partition the file comes to hold
      aggregate.write(writer, snapshot, schema);

      return mergedFieldIds(List.copyOf(gatheredFieldIds), statsOldSrc, schema);
    });
  }

  /**
   * Starts the aggregate from the stored one's entries, decoded once per column. A stored file
   * without them seeds nothing, which is what aggregating its partitions again would reach: an
   * entry is written only when every partition states the column, and what suppressed it then is
   * carried unchanged now.
   */
  /**
   * Carries forward, bytes for bytes, the stored entries of the partitions this write never
   * measured, as long as no write since the stored file changed them. Carrying is the one place
   * that can settle that without a reader paying for the walk, so the walk here is uncapped.
   */
  private static void carryForward(Table tbl, Snapshot snapshot, PuffinWriter writer,
      Set<String> written, Configuration conf, StatisticsFile statsOldSrc, ColStatsAggregate aggregate)
      throws IOException {
    if (statsOldSrc == null) {
      // a partition describes itself: with nothing stored there is nothing to carry, and what
      // was computed stands on its own
      return;
    }
    Predicate<String> upToDate = IcebergStoredStats.upToDateColStats(tbl, snapshot, statsOldSrc, conf, false);

    try (PuffinReader reader = Puffin.read(tbl.io().newInputFile(statsOldSrc.path()))
        .withFileSize(statsOldSrc.fileSizeInBytes())
        .withFooterSize(statsOldSrc.fileFooterSizeInBytes())
        .build()) {

      Set<String> storedPartitions = Sets.newHashSet();
      List<BlobMetadata> carried = Lists.newArrayList();

      for (BlobMetadata metadata : reader.fileMetadata().blobs()) {
        String partName = metadata.properties().get(PARTITION_FIELD);
        if (!HIVE_PART_COL_STATS_BLOB_V1.equals(metadata.type()) || partName == null) {
          continue;
        }
        storedPartitions.add(partName);
        if (!written.contains(partName) && upToDate.test(partName)) {
          carried.add(metadata);
        }
      }
      // a write that replaced or dropped nothing stored leaves the stored aggregate answering
      // for every carried partition: it becomes the starting point, one entry per column,
      // instead of being rebuilt by decoding every carried blob
      boolean seedFromStored = Sets.intersection(written, storedPartitions).isEmpty() &&
          carried.size() == storedPartitions.size();
      if (seedFromStored) {
        aggregate.seedFrom(reader, carried.size());
      }
      for (Pair<BlobMetadata, ByteBuffer> blob : reader.readAll(carried)) {
        ByteBuffer carriedBytes = blob.second();
        // read before it is written: reading leaves the bytes where they are, so the same buffer
        // travels on untouched
        try {
          if (!seedFromStored) {
            aggregate.addPartition(IcebergColStatsReader.decodePartBlob(carriedBytes, null, true));
          }
        } catch (InvalidObjectException e) {
          throw new IOException(e);
        }
        writer.add(new Blob(
            HIVE_PART_COL_STATS_BLOB_V1, List.of(),
            snapshot.snapshotId(), snapshot.sequenceNumber(),
            carriedBytes,
            PuffinCompressionCodec.NONE,
            Map.of(PARTITION_FIELD,
                blob.first().properties().get(PARTITION_FIELD))));
      }
    }
  }

  /**
   * One entry per column, aggregated from the partitions as they are written - of the whole table
   * only where the file comes to hold every one of them. An ask covering exactly the partitions the
   * file describes is answered from these, so it reads one blob per column asked instead of merging
   * one per partition. A read of the table itself takes a
   * table-level file and never this one, so what is aggregated here answers no such read.
   *
   * A partition contributes once, whether this gather measured it or carried it: nothing can be
   * taken back out of a distinct count, so what is not aggregated here cannot be added later.
   */
  private static final class ColStatsAggregate {

    private final Map<String, ColumnStatisticsObj> byColumn = Maps.newLinkedHashMap();
    /** How many partitions stated each column, so that a column short of any is not stated. */
    private final Map<String, Integer> statedBy = Maps.newHashMap();
    private int partitions;

    private void addPartition(List<ColumnStatisticsObj> statsObjs) throws InvalidObjectException {
      addEntries(statsObjs, 1);
    }

    /**
     * Starts from the stored aggregate's entries, decoded once per column, standing for the given
     * number of carried partitions. A stored file without them seeds nothing, which is what
     * aggregating its partitions again would reach: an entry is written only when every partition
     * states the column, and what suppressed it then is carried unchanged now.
     */
    private void seedFrom(PuffinReader reader, int carriedPartitions) throws IOException {
      List<BlobMetadata> aggregateBlobs = reader.fileMetadata().blobs().stream()
          .filter(metadata -> HIVE_COL_STATS_BLOB_V1.equals(metadata.type()))
          .toList();
      List<ColumnStatisticsObj> entries = Lists.newArrayList();
      for (Pair<BlobMetadata, ByteBuffer> blob : reader.readAll(aggregateBlobs)) {
        entries.add(IcebergColStatsCodec.decodeEntry(
            ByteBuffers.toByteArray(blob.second()), true));
      }
      try {
        addEntries(entries, carriedPartitions);
      } catch (InvalidObjectException e) {
        throw new IOException(e);
      }
    }

    /**
     * Entries standing for the given number of partitions: one gathered or carried partition's
     * own, or a stored aggregate's, which stand for every partition of their file at once - an
     * entry is stored exactly when every one of them stated the column.
     */
    private void addEntries(List<ColumnStatisticsObj> entries, int standingFor)
        throws InvalidObjectException {
      partitions += standingFor;
      for (ColumnStatisticsObj statsObj : entries) {
        statedBy.merge(statsObj.getColName(), standingFor, Integer::sum);
        ColumnStatisticsObj held = byColumn.get(statsObj.getColName());
        if (held == null) {
          byColumn.put(statsObj.getColName(), statsObj.deepCopy());
        } else {
          ColumnStatistics into = new ColumnStatistics(null, Lists.newArrayList(held));
          MetaStoreServerUtils.mergeColStats(into, new ColumnStatistics(null, List.of(statsObj)));
          byColumn.put(statsObj.getColName(), into.getStatsObj().getFirst());
        }
      }
    }

    private void write(PuffinWriter writer, Snapshot snapshot, Schema schema) throws IOException {
      for (ColumnStatisticsObj statsObj : byColumn.values()) {
        Types.NestedField field = schema.caseInsensitiveFindField(statsObj.getColName());
        // a column any partition did not state is not the table's: a rename leaves the partitions
        // this gather did not write naming the column it was, and what they hold is not this
        if (field == null || statedBy.getOrDefault(statsObj.getColName(), 0) != partitions) {
          continue;
        }
        writer.add(new Blob(
            HIVE_COL_STATS_BLOB_V1, List.of(field.fieldId()),
            snapshot.snapshotId(), snapshot.sequenceNumber(),
            ByteBuffer.wrap(IcebergColStatsCodec.encodeEntry(statsObj)),
            PuffinCompressionCodec.NONE,
            IcebergColStatsProperties.of(statsObj)));
      }
    }
  }

  @FunctionalInterface
  private interface BlobWriter {
    /**
     * Streams the blobs; returns the field ids the file names in the table metadata, empty where
     * each blob names its own.
     */
    List<Integer> write(PuffinWriter writer) throws IOException, InvalidObjectException;
  }

  /**
   * Every column the file being written comes to hold: the ones this gather read, and the ones
   * standing in what it carries. A merge keeps partitions gathered separately, so the file holds
   * the columns of both; naming only this gather's would say a column has no statistics when the
   * carried partitions still describe it. Columns the table no longer has leave here, though what
   * carries them keeps their bytes until a gather of the whole table rewrites it.
   */
  private static List<Integer> mergedFieldIds(List<Integer> gathered, StatisticsFile statsOldSrc,
      Schema schema) {
    Set<Integer> fields = Sets.newLinkedHashSet(gathered);
    if (statsOldSrc != null) {
      statsOldSrc.blobMetadata().stream()
          .map(org.apache.iceberg.BlobMetadata::fields)
          .forEach(fields::addAll);
    }
    return fields.stream().filter(id -> schema.findField(id) != null).toList();
  }

  /**
   * What the table's metadata keeps of the file: the entries a read can answer from without
   * opening it. Table-level entries carry a column's field id and distinct count, and the first
   * partition entry names every field the file states and marks the granularity; the rest of a
   * partition-level file is addressed through its own footer, so registering an entry per
   * partition would only write the footer into the table metadata again, once per partition, on
   * every commit.
   */
  private static List<org.apache.iceberg.BlobMetadata> registeredBlobs(
      List<org.apache.iceberg.puffin.BlobMetadata> written, List<Integer> namedFields) {
    boolean fieldsNamed = false;
    List<org.apache.iceberg.BlobMetadata> registered = Lists.newArrayList();
    for (org.apache.iceberg.puffin.BlobMetadata blob : written) {
      if (HIVE_PART_COL_STATS_BLOB_V1.equals(blob.type())) {
        if (fieldsNamed) {
          continue;
        }
        fieldsNamed = true;
        // the entry as written, naming what the whole file states: the writer streams, so what
        // the file came to hold is only known once every blob is
        registered.add(GenericBlobMetadata.from(new org.apache.iceberg.puffin.BlobMetadata(
            blob.type(), namedFields, blob.snapshotId(), blob.sequenceNumber(),
            blob.offset(), blob.length(), blob.compressionCodec(), blob.properties())));
        continue;
      }
      registered.add(GenericBlobMetadata.from(blob));
    }
    return registered;
  }

  static ByteBuffer encodePartBlob(List<ColumnStatisticsObj> statsObjs, List<Integer> fieldIds)
      throws IOException {
    List<byte[]> entries = Lists.newArrayListWithCapacity(statsObjs.size());
    for (ColumnStatisticsObj obj : statsObjs) {
      // vectors and histograms alike: what a read wants of them it settles once they are in hand
      entries.add(IcebergColStatsCodec.encodeEntry(obj));
    }
    return ByteBuffer.wrap(
        IcebergColStatsCodec.encodeBlob(entries, fieldIds));
  }

  /**
   * Opens a statistics file for the snapshot, lets the caller add its blobs, and registers it on
   * the table. A file no blob was added to is left uncommitted, so the statistics standing for the
   * table stay standing; one that fails part-written is deleted rather than left behind.
   */
  private static boolean commitFile(Table tbl, Snapshot snapshot, BlobWriter blobs)
      throws IOException, InvalidObjectException {
    String statsPath = ((HasTableOperations) tbl).operations().metadataFileLocation(
        String.format(STATS_FILE, snapshot.snapshotId(), UUID.randomUUID()));
    StatisticsFile statisticsFile;

    try (PuffinWriter writer = Puffin.write(tbl.io().newOutputFile(statsPath))
        .createdBy(Constants.HIVE_ENGINE)
        .build()) {
      List<Integer> namedFields = blobs.write(writer);
      if (writer.writtenBlobsMetadata().isEmpty()) {
        // committing this would register a file describing nothing, in place of one that may
        // describe something: a read resolves it, finds no statistics of ours in it, and the
        // table is left with none at all. Leave standing whatever stands
        LOG.warn("Gathered no column statistics to store for {} at snapshot {}: leaving what is" +
            " stored as it is", tbl.name(), snapshot.snapshotId());
        return false;
      }
      writer.finish();
      statisticsFile = new GenericStatisticsFile(
          snapshot.snapshotId(),
          statsPath,
          writer.fileSize(),
          writer.footerSize(),
          registeredBlobs(writer.writtenBlobsMetadata(), namedFields));
    } catch (Exception e) {
      tbl.io().deleteFile(statsPath);
      if (!(e instanceof IOException)) {
        // not a write failure but the gather itself: the statement must hear about it
        throw e;
      }
      LOG.warn("Unable to write column stats to the Puffin file", e);
      return false;
    }
    try {
      tbl.updateStatistics()
          .setStatistics(statisticsFile)
          .commit();
    } catch (Exception e) {
      // the rows are committed already: losing the race to register their statistics costs the
      // statistics, never the statement
      LOG.warn("Unable to register the column statistics of {}", tbl.name(), e);
      return false;
    }
    LOG.info("Stored {} column statistics blobs for {} at snapshot {} in {}",
        statisticsFile.blobMetadata().size(), tbl.name(), snapshot.snapshotId(), statsPath);
    return true;
  }

}
