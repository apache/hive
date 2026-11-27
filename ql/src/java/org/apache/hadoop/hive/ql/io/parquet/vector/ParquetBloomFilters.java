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
package org.apache.hadoop.hive.ql.io.parquet.vector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.Nullable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.common.io.encoded.MemoryBufferOrBuffers;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.RowGroupFilter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * The bloom filter level of row group pruning, which both the vectorized reader and the LLAP native reader
 * run over the row groups that survive statistics.
 *
 * <p>Statistics come from the footer, which is already in memory. Bloom filters live in the data file and
 * need an open reader, so it is opened only when a surviving row group carries a filter for a column the
 * predicate can prune on. With a file key the filters are read through the LLAP metadata cache.
 */
public final class ParquetBloomFilters {

  private static final Logger LOG = LoggerFactory.getLogger(ParquetBloomFilters.class);

  private ParquetBloomFilters() {
  }

  /**
   * Drops the row groups whose bloom filters prove the predicate matches nothing in them.
   *
   * @param blocks the row groups statistics kept
   * @param path the path to open the file by, the inode path when {@code fileKey} is an HDFS inode id
   * @param fileKey the LLAP cache key of the file, or null to read the filters from the file
   * @param tag cache tag of the file, computed from its name path
   */
  public static List<BlockMetaData> filterRowGroups(FilterCompat.Filter filter, List<BlockMetaData> blocks,
      FileMetaData fileMetaData, Path path, JobConf conf, @Nullable Object fileKey, @Nullable CacheTag tag)
      throws IOException {
    if (blocks.isEmpty() || !conf.getBoolean(ParquetInputFormat.BLOOM_FILTERING_ENABLED, true)) {
      return blocks;
    }
    BloomFilterPlan plan = bloomFilterPlan(filter);
    if (!plan.canPrune()) {
      return blocks;
    }
    SortedMap<Long, Integer> ranges = bloomFilterRanges(plan.columnsRead(), blocks);
    if (ranges != null && ranges.isEmpty()) {
      return blocks;
    }
    boolean cacheOnly = fileKey != null && HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_IO_CACHE_ONLY);
    if (ranges == null && cacheOnly) {
      // the filters cannot be served from the cache, and the file may not be read to get them
      return blocks;
    }

    ParquetMetadata metadata = new ParquetMetadata(fileMetaData, blocks);
    // Without a record filter: parquet filters row groups in the constructor otherwise, reading every
    // bloom filter a second time and seeking to chunks that are not among the cached ranges.
    ParquetReadOptions options = HadoopReadOptions.builder(conf, path)
        .withRecordFilter(FilterCompat.NOOP).build();
    try {
      if (fileKey == null || ranges == null) {
        try (ParquetFileReader reader = new ParquetFileReader(conf, path, metadata, options)) {
          return bloomFilterLevel(filter, blocks, reader);
        }
      }
      return LlapProxy.getIo().parquet().readBloomFiltersFromCache(path, conf, tag, fileKey, ranges, cached -> {
        ParquetFilterDataFromCache input = new ParquetFilterDataFromCache(cachedRanges(ranges, cached), path, conf);
        try (ParquetFileReader reader = ParquetFileReader.open(input, metadata, options, input)) {
          return bloomFilterLevel(filter, blocks, reader);
        }
      });
    } catch (Exception e) {
      if (cacheOnly) {
        // a miss is the answer the caller asked for, as it is of every other read this mode makes
        throw e;
      }
      // the row groups these would have dropped are read instead, which costs time and not correctness
      LOG.warn("Skipping the bloom filters of " + path + ", reading the row groups statistics kept", e);
      return blocks;
    }
  }

  private static List<BlockMetaData> bloomFilterLevel(FilterCompat.Filter filter, List<BlockMetaData> blocks,
      ParquetFileReader reader) {
    return RowGroupFilter.filterRowGroups(
        Collections.singletonList(RowGroupFilter.FilterLevel.BLOOMFILTER), filter, blocks, reader);
  }

  private static List<ParquetFilterDataFromCache.Range> cachedRanges(SortedMap<Long, Integer> ranges,
      Map<Long, MemoryBufferOrBuffers> cached) {
    List<ParquetFilterDataFromCache.Range> cachedRanges = new ArrayList<>(ranges.size());
    for (Map.Entry<Long, Integer> range : ranges.entrySet()) {
      cachedRanges.add(new ParquetFilterDataFromCache.Range(range.getKey(), range.getValue(),
          cached.get(range.getKey())));
    }
    return cachedRanges;
  }

  /**
   * The bloom filters of these row groups that the predicate could prune on, as offset to length. Empty
   * when none of the usable columns carries one, so the level can be skipped. Null when one records no
   * length, as files written before Parquet stored it do: those are read from the file rather than cached.
   */
  @VisibleForTesting
  static SortedMap<Long, Integer> bloomFilterRanges(Set<ColumnPath> columns, List<BlockMetaData> blocks) {
    SortedMap<Long, Integer> ranges = new TreeMap<>();
    for (BlockMetaData block : blocks) {
      for (ColumnChunkMetaData column : block.getColumns()) {
        // The path is tested first because reading the offset of an encrypted chunk decrypts its metadata,
        // which throws when the query holds no key for that column.
        if (!columns.contains(column.getPath())) {
          continue;
        }
        long offset = column.getBloomFilterOffset();
        if (offset <= 0) {
          continue;
        }
        int length = column.getBloomFilterLength();
        if (length <= 0) {
          return null;
        }
        ranges.put(offset, length);
      }
    }
    return ranges;
  }

  /**
   * What the bloom filter level can do with a predicate: the columns Parquet may read a filter for, and
   * whether dropping a row group is possible at all. The two differ, and conflating them leaves a read
   * unserved: an OR only drops a row group when both of its sides do, but BloomFilterImpl still reads the
   * filter of a side that can prune, so its column has to be fetched even when the OR itself cannot prune.
   */
  record BloomFilterPlan(Set<ColumnPath> columnsRead, boolean canPrune) {
    private static final BloomFilterPlan NONE = new BloomFilterPlan(Set.of(), false);
  }

  static BloomFilterPlan bloomFilterPlan(FilterCompat.Filter filter) {
    if (!(filter instanceof FilterCompat.FilterPredicateCompat predicateFilter)) {
      return BloomFilterPlan.NONE;
    }
    Set<ColumnPath> columnsRead = new HashSet<>();
    boolean canPrune = prunesWithBloomFilter(predicateFilter.getFilterPredicate(), columnsRead);
    return new BloomFilterPlan(columnsRead, canPrune);
  }

  /**
   * Answers whether this predicate can drop a row group, adding the columns Parquet reads a filter for.
   * A bloom filter only proves a value absent, so it serves equality and set membership and nothing else.
   */
  private static boolean prunesWithBloomFilter(FilterPredicate predicate, Set<ColumnPath> columnsRead) {
    switch (predicate) {
      // eq(col, null) asks for nulls, which a bloom filter says nothing about
      case Operators.Eq<?> eq -> {
        if (eq.getValue() == null) {
          return false;
        }
        columnsRead.add(eq.getColumn().getColumnPath());
        return true;
      }
      case Operators.In<?> in -> {
        columnsRead.add(in.getColumn().getColumnPath());
        return true;
      }
      // both sides are walked whatever they answer, since a side that can prune has its filter read
      // even where the node above it cannot
      case Operators.And and -> {
        boolean left = prunesWithBloomFilter(and.getLeft(), columnsRead);
        boolean right = prunesWithBloomFilter(and.getRight(), columnsRead);
        return left || right;
      }
      case Operators.Or or -> {
        boolean left = prunesWithBloomFilter(or.getLeft(), columnsRead);
        boolean right = prunesWithBloomFilter(or.getRight(), columnsRead);
        return left && right;
      }
      default -> {
        return false;
      }
    }
  }
}
