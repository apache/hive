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
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.util.functional.FutureIO;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.DelegatingInputStream;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads the column statistics {@link IcebergColStatsWriter} stores: table-level entries one blob
 * per column, partition entries one framed blob per partition, and the aggregate a partition-level
 * file also holds - fetching and decoding only the columns asked.
 */
public final class IcebergColStatsReader {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergColStatsReader.class);

  /**
   * What a gap has to exceed before it is seeked over rather than read through, where the stream
   * underneath does not state its own. A partition's entries are one blob, so a gap is a whole
   * blob wide, and this bridges the partitions a pruned scan passes over only where the table is
   * narrow enough for one to fit. Hadoop's own defaults; a store that seeks dearly states more.
   */
  private static final long DEFAULT_MIN_SEEK = 16 * 1024;
  /** How much one read may hold where the stream does not state its own. */
  private static final long DEFAULT_MAX_READ_SIZE = 1024 * 1024;

  private IcebergColStatsReader() {
  }

  /** The stored statistics describing the whole table, with what the configuration asks for. */
  public static List<ColumnStatisticsObj> read(Table table, long snapshotId, Collection<String> columns,
      Configuration conf) {
    return read(table, snapshotId, columns, fetchVectors(conf));
  }

  /** The same, told outright whether the vector is wanted. */
  public static List<ColumnStatisticsObj> read(Table table, long snapshotId, Collection<String> columns,
      boolean withVectors) {
    StatisticsFile statsFile = IcebergStoredStats.findColStatsFile(table, snapshotId, false);
    if (statsFile == null) {
      LOG.warn("Column stats file not found for snapshot: {}", snapshotId);
      return Lists.newArrayList();
    }
    return read(table, statsFile, columns, withVectors);
  }

  /** The same, out of a file the caller has already settled on. */
  static List<ColumnStatisticsObj> read(Table table, StatisticsFile statsFile,
      Collection<String> columns, boolean withVectors) {
    try {
      return readOrThrow(table, statsFile, columns, withVectors);
    } catch (Exception e) {
      // serving no stats degrades the planner to estimates - never wrong
      LOG.warn("Unable to read column stats: {}", e.getMessage());
      return Lists.newArrayList();
    }
  }

  /**
   * The strict variant for the merge path: an unreadable statistics file must not be mistaken for
   * an absent one, or the increment would be persisted as the complete statistics.
   */
  static List<ColumnStatisticsObj> readOrThrow(Table table, StatisticsFile statsFile,
      Collection<String> columns, boolean withVectors)
      throws IOException {
    Predicate<BlobMetadata> holdsNeededColumn =
        columns != null ? blobsForColumns(table, columns) : blob -> true;
    List<ColumnStatisticsObj> entries = Lists.newArrayList();
    String statsPath = statsFile.path();

    try (PuffinReader reader = Puffin.read(table.io().newInputFile(statsPath))
        .withFileSize(statsFile.fileSizeInBytes())
        .withFooterSize(statsFile.fileFooterSizeInBytes())
        .build()) {

      List<BlobMetadata> blobMetadata = reader.fileMetadata().blobs().stream()
          .filter(IcebergColStatsReader::holdsColStats)
          .filter(holdsNeededColumn)
          // a column dropped and added back is a different field: what was stored for the one it
          // replaced describes rows the column of that name never held
          .filter(blob -> table.schema().findField(blob.inputFields().getFirst()) != null)
          .toList();

      LOG.info("Using column stats from: {}", statsPath);

      for (Pair<BlobMetadata, ByteBuffer> blob : reader.readAll(blobMetadata)) {
        byte[] raw = ByteBuffers.toByteArray(blob.second());
        entries.add(decodeTableEntry(raw, blob.first().type(), withVectors));
      }
    }
    return entries;
  }

  /**
   * Whether the blob holds an entry this reads. A file may hold blobs of other kinds beside these
   * - a sketch another engine wrote and keeps across its own writes - and one of those is nothing
   * to decode: reading it as an entry would lose the whole file rather than the one blob.
   */
  private static boolean holdsColStats(BlobMetadata blob) {
    return IcebergColStatsWriter.HIVE_COL_STATS_BLOB_V1.equals(blob.type()) ||
        IcebergColStatsWriter.LEGACY_COL_STATS_BLOB.equals(blob.type());
  }

  /** An entry as the blob that names it was written: a Thrift struct, or a serialized Java object. */
  private static ColumnStatisticsObj decodeTableEntry(byte[] raw, String blobType, boolean withVectors) {
    if (IcebergColStatsWriter.HIVE_COL_STATS_BLOB_V1.equals(blobType)) {
      return IcebergColStatsCodec.decodeEntry(raw, withVectors);
    }
    // a table-level entry released before this holds a serialized Java object, and still reads -
    // with the vector it was written with, which is what a merge of its distinct counts needs
    ColumnStatisticsObj released = SerializationUtils.deserialize(raw);
    return withVectors ? released : IcebergColStatsCodec.withoutVectors(released);
  }

  /**
   * Whether a read wants the vector a distinct count is merged from across partitions. It is the
   * bulk of an entry, so a read that will not merge counts leaves it in the file rather than
   * fetching and decoding it. A write never asks: the vector is computed for every column whatever
   * anyone asked for, and a statistic stored without one can never be merged afterwards.
   * A histogram answers to nothing here: one exists only where a statement was told to compute it,
   * and a plan reads one wherever it finds it.
   *
   * <p>A read told not to fetch them is left to bound a distinct count from the entries themselves,
   * as one of a native table is.
   */
  private static boolean fetchVectors(Configuration conf) {
    return MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.STATS_FETCH_BITVECTOR);
  }

  /** The blobs naming any of the asked columns, by the name the table's schema gives the field now. */
  private static Predicate<BlobMetadata> blobsForColumns(Table table, Collection<String> columns) {
    return metadata -> metadata.inputFields().stream()
        .map(fieldId -> table.schema().findColumnName(fieldId))
        .anyMatch(columns::contains);
  }

  /**
   * What the file states of the table itself, where the ask covers every partition it describes
   * and each still does. Null where it does not, where the file holds no such entries - one
   * written before they were kept, or by a gather that measured a partition subset - or where
   * the file cannot be read.
   */
  public static List<ColumnStatisticsObj> readAggr(Table table, StatisticsFile statsFile,
      Set<String> asked, Predicate<String> upToDate, List<String> colNames, Configuration conf) {
    return readAggr(table, statsFile, asked, upToDate, colNames, fetchVectors(conf));
  }

  private static List<ColumnStatisticsObj> readAggr(Table table, StatisticsFile statsFile,
      Set<String> asked, Predicate<String> upToDate, List<String> colNames, boolean withVectors) {
    // the registered entry states how many partitions the file describes: an ask of another
    // size cannot be the exact set, and is turned away without opening the file
    for (var blob : statsFile.blobMetadata()) {
      String numPartitions = blob.properties().get(IcebergColStatsWriter.NUM_PARTITIONS_FIELD);
      if (numPartitions != null && !numPartitions.equals(String.valueOf(asked.size()))) {
        return null;
      }
    }
    Set<String> columns = Sets.newHashSet(colNames);
    List<ColumnStatisticsObj> aggregated = Lists.newArrayList();

    try (PuffinReader reader = Puffin.read(table.io().newInputFile(statsFile.path()))
        .withFileSize(statsFile.fileSizeInBytes())
        .withFooterSize(statsFile.fileFooterSizeInBytes())
        .build()) {
      // the partitions the file holds are in its footer: the table's metadata registers only
      // what answers without opening the file, and this read is opening it anyway
      Set<String> described = Sets.newHashSet();
      for (BlobMetadata blob : reader.fileMetadata().blobs()) {
        String partName = blob.properties().get(IcebergColStatsWriter.PARTITION_FIELD);
        if (partName != null) {
          described.add(partName);
        }
      }
      // nothing can be taken out of an aggregate or added to it: it answers only where the ask is
      // exactly the partitions it holds, each still describing itself
      if (described.isEmpty() || !described.equals(asked) || !described.stream().allMatch(upToDate)) {
        return null;
      }
      Predicate<BlobMetadata> holdsNeededColumn = blobsForColumns(table, columns);

      List<BlobMetadata> blobMetadata = reader.fileMetadata().blobs().stream()
          .filter(IcebergColStatsReader::holdsColStats)
          .filter(blob -> table.schema().findField(blob.inputFields().getFirst()) != null)
          .filter(holdsNeededColumn)
          .toList();

      for (Pair<BlobMetadata, ByteBuffer> blob : reader.readAll(blobMetadata)) {
        byte[] raw = ByteBuffers.toByteArray(blob.second());
        aggregated.add(decodeTableEntry(raw, blob.first().type(), withVectors));
      }
    } catch (Exception e) {
      // serving no stats degrades the planner to estimates - never wrong
      LOG.warn("Unable to read column stats: {}", e.getMessage());
      return null;
    }
    // a rename keeps the field, so a blob written before it still names the field under the old
    // column name: it answers for the field asked about but not for the column, and is left out
    aggregated.removeIf(statsObj -> !columns.contains(statsObj.getColName()));
    return aggregated.size() == colNames.size() ? aggregated : null;
  }

  /**
   * The stored partition entries the given file holds for the partitions the filter admits, each
   * trimmed to the asked columns; a null column set asks for all of them.
   */
  public static Map<String, List<ColumnStatisticsObj>> readPart(Table table, StatisticsFile statsFile,
      Predicate<String> partitionFilter, Set<String> columns, Configuration conf) {
    return readPart(table, statsFile, partitionFilter, columns, fetchVectors(conf));
  }

  /** The same, told outright whether the vector is wanted. */
  public static Map<String, List<ColumnStatisticsObj>> readPart(Table table, StatisticsFile statsFile,
      Predicate<String> partitionFilter, Set<String> columns, boolean withVectors) {
    Map<String, List<ColumnStatisticsObj>> result = Maps.newLinkedHashMap();

    try (PuffinReader reader = Puffin.read(table.io().newInputFile(statsFile.path()))
        .withFileSize(statsFile.fileSizeInBytes())
        .withFooterSize(statsFile.fileFooterSizeInBytes())
        .build()) {

      List<BlobMetadata> blobs = reader.fileMetadata().blobs().stream()
          .filter(metadata -> {
            if (!IcebergColStatsWriter.HIVE_PART_COL_STATS_BLOB_V1.equals(metadata.type())) {
              return false;
            }
            String partName = metadata.properties().get(IcebergColStatsWriter.PARTITION_FIELD);
            return partName != null && (partitionFilter == null || partitionFilter.test(partName));
          })
          .toList();

      LOG.info("Using column stats from: {}", statsFile.path());
      // one stream serves them all, seeking forward: the blobs of the partitions a scan reads sit
      // near one another in the file, and one read spanning the gap between two of them costs less
      // than the seek it saves. Reading each on its own is what makes a scan of many partitions
      // expensive.
      if (!blobs.isEmpty()) {
        InputFile file = table.io().newInputFile(statsFile.path(), statsFile.fileSizeInBytes());
        try (SeekableInputStream in = file.newStream()) {
          readBlobs(in, blobs, columns, withVectors, result, fieldsOf(table, columns));
        }
      }
    } catch (Exception e) {
      // serving no stats degrades the planner to estimates - never wrong
      LOG.warn("Unable to read column stats: {}", e.getMessage());
      result.clear();
    }
    return result;
  }

  /**
   * The blobs a scan reads, taken in as few reads as their places in the file allow. They are
   * written one after another, so the ones a scan asks for are read in runs rather than one at a
   * time; a run ends at a gap wider than the seek it would save, or at the bytes one read may hold.
   *
   * <p>This stands in for what {@link PuffinReader#readAll} leaves as a TODO: it reads one blob
   * per round trip, which a file holding a blob per partition cannot afford. It leaves in favor
   * of Iceberg's reader once that one coalesces runs and takes them in one vectored call.
   */
  static void readBlobs(SeekableInputStream in, List<BlobMetadata> blobs, Set<String> columns,
      boolean withVectors, Map<String, List<ColumnStatisticsObj>> result, IntPredicate fields)
      throws IOException {
    List<BlobMetadata> ordered = blobs.stream()
        .sorted(Comparator.comparingLong(BlobMetadata::offset))
        .toList();

    long minSeek = minSeek(in);
    long maxReadSize = maxReadSize(in);
    List<Run> runs = Lists.newArrayList();

    int cursor = 0;
    while (cursor < ordered.size()) {
      int first = cursor;
      int last = cursor;

      for (int next = cursor + 1; next < ordered.size(); next++) {
        BlobMetadata held = ordered.get(last);
        long gap = ordered.get(next).offset() - (held.offset() + held.length());
        long span = ordered.get(next).offset() + ordered.get(next).length() - ordered.get(first).offset();
        if (gap > minSeek || span > maxReadSize) {
          break;
        }
        last = next;
      }

      runs.add(new Run(first, last));
      cursor = last + 1;
    }

    List<ByteBuffer> held = readRanges(in, ordered, runs);
    for (int r = 0; r < runs.size(); r++) {
      Run run = runs.get(r);
      long start = ordered.get(run.firstBlob()).offset();

      for (int i = run.firstBlob(); i <= run.lastBlob(); i++) {
        BlobMetadata blob = ordered.get(i);
        ByteBuffer part = held.get(r).duplicate();
        part.position((int) (blob.offset() - start));
        part.limit((int) (blob.offset() - start + blob.length()));
        result.put(blob.properties().get(IcebergColStatsWriter.PARTITION_FIELD),
            decodePartBlob(part.slice(), columns, withVectors, fields));
      }
    }
  }

  /** A stretch of consecutive wanted blobs one read takes together, first and last inclusive. */
  private record Run(int firstBlob, int lastBlob) {
  }

  /**
   * The bytes of the ranges the runs mark out, one buffer per run and all held at once - the
   * pruned ask bounds them, not the file. A Hadoop stream takes the ranges in one vectored call,
   * which S3A merges and issues four at a time; any other stream reads them one after another,
   * a round trip each.
   */
  private static List<ByteBuffer> readRanges(SeekableInputStream in, List<BlobMetadata> ordered,
      List<Run> runs) throws IOException {
    Optional<FSDataInputStream> stream = hadoopStream(in);
    if (stream.isPresent()) {
      List<FileRange> ranges = runs.stream()
          .map(run -> {
            long start = ordered.get(run.firstBlob()).offset();
            return FileRange.createFileRange(start,
                Math.toIntExact(ordered.get(run.lastBlob()).offset() + ordered.get(run.lastBlob()).length() - start));
          })
          .toList();
      stream.get().readVectored(ranges, ByteBuffer::allocate);
      List<ByteBuffer> held = Lists.newArrayListWithCapacity(ranges.size());
      for (FileRange range : ranges) {
        held.add(FutureIO.awaitFuture(range.getData()));
      }
      return held;
    }
    List<ByteBuffer> held = Lists.newArrayListWithCapacity(runs.size());
    for (Run run : runs) {
      long start = ordered.get(run.firstBlob()).offset();
      int length = Math.toIntExact(ordered.get(run.lastBlob()).offset() + ordered.get(run.lastBlob()).length() - start);
      byte[] bytes = new byte[length];
      in.seek(start);
      IOUtil.readFully(in, bytes, 0, length);
      held.add(ByteBuffer.wrap(bytes));
    }
    return held;
  }

  /**
   * The asked columns of a partition, out of the entries the blob holds one after another. The
   * name check behind the field-id skip is not the same filter twice: a rename keeps the field,
   * so an entry can pass by id while naming the column as it was called when it was stored.
   */
  static List<ColumnStatisticsObj> decodePartBlob(ByteBuffer blob, Set<String> columns, boolean withVectors) {
    return decodePartBlob(blob, columns, withVectors, null);
  }

  /**
   * The asked columns of a partition. Every entry names the field it is for, so the ones a scan did
   * not ask about are stepped over rather than decoded - and a blob a merge carried from another
   * gather needs hold neither the same columns nor the same order for that to hold.
   */
  static List<ColumnStatisticsObj> decodePartBlob(ByteBuffer blob, Set<String> columns,
      boolean withVectors, IntPredicate fields) {
    List<byte[]> stored = fields == null ?
        IcebergColStatsCodec.decodeBlob(blob) : IcebergColStatsCodec.decodeBlob(blob, fields);

    List<ColumnStatisticsObj> entries = Lists.newArrayListWithCapacity(stored.size());
    for (byte[] entry : stored) {
      ColumnStatisticsObj statsObj = IcebergColStatsCodec.decodeEntry(entry, withVectors);
      if (columns == null || columns.contains(statsObj.getColName())) {
        entries.add(statsObj);
      }
    }
    return entries;
  }

  /**
   * The bytes a read is worth: skipping fewer than these to start another read trades bytes for
   * latency at a loss, so blobs no further apart than this are taken in one read.
   *
   * The store knows this, not us - it is what a filesystem means by the smallest seek worth making,
   * and object stores set it from what their own round trips cost.
   */
  private static long minSeek(SeekableInputStream in) {
    return hadoopStream(in).map(stream -> (long) stream.minSeekForVectorReads())
        .filter(stated -> stated > 0)
        .orElse(DEFAULT_MIN_SEEK);
  }

  /**
   * The bytes one read may hold, so no single read spans a file's worth of them. A store that
   * states nothing states it as zero, which caps nothing and would leave every blob read on
   * its own - the default stands in for it, as it does for a store that is not asked.
   */
  private static long maxReadSize(SeekableInputStream in) {
    return hadoopStream(in).map(stream -> (long) stream.maxReadSizeForVectorReads())
        .filter(stated -> stated > 0)
        .orElse(DEFAULT_MAX_READ_SIZE);
  }

  private static Optional<FSDataInputStream> hadoopStream(SeekableInputStream in) {
    if (in instanceof DelegatingInputStream delegating &&
        delegating.getDelegate() instanceof FSDataInputStream stream) {
      return Optional.of(stream);
    }
    return Optional.empty();
  }

  /**
   * The fields the asked columns are, so a read can step over the entries of the rest. An entry
   * answers by its field id, never by its name alone: a full ask filters by the schema's own
   * fields, so an entry a dropped column left behind is stepped over even when a column added
   * since carries its name.
   */
  private static IntPredicate fieldsOf(Table table, Set<String> columns) {
    Set<Integer> fields = Sets.newHashSet();
    if (columns == null) {
      table.schema().columns().forEach(field -> fields.add(field.fieldId()));
      return fields::contains;
    }
    for (String column : columns) {
      Types.NestedField field = table.schema().caseInsensitiveFindField(column);
      if (field != null) {
        fields.add(field.fieldId());
      }
    }
    return fields::contains;
  }
}
