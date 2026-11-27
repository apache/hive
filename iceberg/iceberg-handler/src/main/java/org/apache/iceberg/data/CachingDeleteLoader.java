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

package org.apache.iceberg.data;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.common.io.FileMetadataCache;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.common.io.encoded.MemoryBufferOrBuffers;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedParquetRecordReader;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.Deletes;
import org.apache.iceberg.deletes.DeletionVectors;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.deletes.PositionDeleteIndexUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mr.hive.DeleteCacheCounters;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.CharSequenceMap;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachingDeleteLoader extends BaseDeleteLoader {
  private static final Logger LOG = LoggerFactory.getLogger(CachingDeleteLoader.class);
  // A put that finds the cache full evicts while that frees memory, then fails instead of waiting for memory.
  private static final AtomicBoolean DO_NOT_WAIT = new AtomicBoolean(true);
  private static final String DIRECTORIES = CachingDeleteLoader.class.getName() + "#directories";

  private final ObjectCache cache;
  private final Function<DeleteFile, InputFile> loadInputFile;
  private final Configuration conf;
  private final TaskAttemptContext context;
  private final FileMetadataCache metadataCache;
  private final Configuration cacheConf;
  private final boolean cachePositionDeleteFiles;
  private final Map<String, Set<String>> directories;

  public CachingDeleteLoader(Function<DeleteFile, InputFile> loadInputFile, Configuration conf) {
    this(loadInputFile, conf, null, null, null);
  }

  /**
   * The context is the task whose counters the metadata cache lookups and puts are counted in.
   */
  public CachingDeleteLoader(Function<DeleteFile, InputFile> loadInputFile, TaskAttemptContext context,
      FileMetadataCache metadataCache, Configuration cacheConf) {
    this(loadInputFile, context.getConfiguration(), context, metadataCache, cacheConf);
  }

  private CachingDeleteLoader(Function<DeleteFile, InputFile> loadInputFile, Configuration conf,
      TaskAttemptContext context, FileMetadataCache metadataCache, Configuration cacheConf) {
    super(loadInputFile);

    String queryId = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_QUERY_ID);
    this.cache = ObjectCacheFactory.getCache(conf, queryId, false);
    this.loadInputFile = loadInputFile;
    this.conf = conf;
    this.context = context;
    String cacheLevel = HiveConf.getVar(conf, HiveConf.ConfVars.LLAP_IO_CACHE_DELETEDELTAS).toLowerCase();
    this.metadataCache = "none".equals(cacheLevel) ? null : metadataCache;
    this.cacheConf = cacheConf;
    this.cachePositionDeleteFiles = "all".equals(cacheLevel);
    this.directories = this.metadataCache != null && cachePositionDeleteFiles ? queryDirectories() : null;
  }

  @Override
  protected boolean canCache(long size) {
    return cache != null;
  }

  @Override
  protected <V> V getOrLoad(String key, Supplier<V> valueSupplier, long valueSize) {
    try {
      return cache.retrieve(key, valueSupplier::get);
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Loads the deletes of a data file through the LLAP metadata cache, when one is injected. Every entry is a
   * deletion vector in its serialized form, keyed by the delete file location and the data file it applies to (delete
   * files are immutable), and deserialized on every hit. Position delete files are cached in the same form, split per
   * data file.
   */
  @Override
  public PositionDeleteIndex loadPositionDeletes(Iterable<DeleteFile> deleteFiles, CharSequence filePath) {
    if (metadataCache != null) {
      if (ContentFileUtil.containsSingleDV(deleteFiles)) {
        return readDV(Iterables.getOnlyElement(deleteFiles), filePath);
      }
      if (cachePositionDeleteFiles) {
        return PositionDeleteIndexUtil.merge(
            Iterables.transform(deleteFiles, deleteFile -> readPosDeletes(deleteFile, filePath)));
      }
    }
    return super.loadPositionDeletes(deleteFiles, filePath);
  }

  private PositionDeleteIndex readDV(DeleteFile dv, CharSequence filePath) {
    validateDV(dv, filePath);
    DeletesKey key = new DeletesKey(dv.location(), filePath.toString());
    byte[] bytes = getCached(key);
    if (bytes != null) {
      counter(DeleteCacheCounters.DELETE_CACHE_HIT).increment(1);
      return PositionDeleteIndex.deserialize(bytes, dv);
    }
    counter(DeleteCacheCounters.DELETE_CACHE_MISS).increment(1);
    bytes = new byte[dv.contentSizeInBytes().intValue()];
    try {
      IOUtil.readFully(loadInputFile.apply(dv), dv.contentOffset(), bytes, 0, bytes.length);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    PositionDeleteIndex index = PositionDeleteIndex.deserialize(bytes, dv);
    putDeletes(key, bytes, dv);
    return index;
  }

  // The checks of BaseDeleteLoader, private there
  private static void validateDV(DeleteFile dv, CharSequence filePath) {
    Preconditions.checkArgument(dv.contentOffset() != null,
        "Invalid DV, offset cannot be null: %s", ContentFileUtil.dvDesc(dv));
    Preconditions.checkArgument(dv.contentSizeInBytes() != null,
        "Invalid DV, length is null: %s", ContentFileUtil.dvDesc(dv));
    Preconditions.checkArgument(dv.contentSizeInBytes() <= Integer.MAX_VALUE,
        "Can't read DV larger than 2GB: %s", dv.contentSizeInBytes());
    Preconditions.checkArgument(filePath.toString().equals(dv.referencedDataFile()),
        "DV is expected to reference %s, not %s", filePath, dv.referencedDataFile());
  }

  /**
   * Loads the deletes of a data file from a position delete file. The cache holds an entry per delete file and data
   * file it references, and, for a partition-scoped delete file, its directory: the locations of the data files it
   * references. A data file the directory does not list gets an empty index without a cache lookup, a decode or an
   * entry of its own; the lookup is counted as UNREFERENCED. Otherwise the deletes come from the data file's entry, or
   * the whole delete file is decoded, once per query on a daemon, and the data file's entry and the directory are
   * cached.
   * <p>
   * A file-scoped delete file is attached to its one data file only, so it has no directory. The query's directories
   * are one map in the query's cache, fetched once per loader. The decoded file has a key of its own in the query's
   * cache: a reader without the metadata cache (BaseDeleteLoader) caches its indexes under the location. Without the
   * LLAP object cache (hive.llap.object.cache.enabled=false) the query's cache keeps nothing: each loader reads the
   * directories from the metadata cache, and a referenced data file decodes the delete file on every miss.
   */
  private PositionDeleteIndex readPosDeletes(DeleteFile deleteFile, CharSequence filePath) {
    String dataFile = filePath.toString();
    if (isPartitionScoped(deleteFile)) {
      Set<String> directory = directory(deleteFile);
      if (directory != null && !directory.contains(dataFile)) {
        counter(DeleteCacheCounters.DELETE_CACHE_UNREFERENCED).increment(1);
        return emptyIndex(deleteFile, filePath);
      }
    }
    DeletesKey key = new DeletesKey(deleteFile.location(), dataFile);
    byte[] bytes = getCached(key);
    if (bytes != null) {
      counter(DeleteCacheCounters.DELETE_CACHE_HIT).increment(1);
      return DeletionVectors.deserialize(bytes, deleteFile);
    }
    bytes = decoded(deleteFile).get(filePath);
    if (bytes == null) {
      // the directory was unknown
      counter(DeleteCacheCounters.DELETE_CACHE_UNREFERENCED).increment(1);
      return emptyIndex(deleteFile, filePath);
    }
    counter(DeleteCacheCounters.DELETE_CACHE_MISS).increment(1);
    putDeletes(key, bytes, deleteFile);
    return DeletionVectors.deserialize(bytes, deleteFile);
  }

  // The directories of the query's partition-scoped delete files, by delete file location. A delete file without one
  // has not been decoded on this daemon in this query, or its directory was evicted from the metadata cache.
  private Map<String, Set<String>> queryDirectories() {
    return canCache(0) ? getOrLoad(DIRECTORIES, ConcurrentHashMap::new, 0) : new ConcurrentHashMap<>();
  }

  // From the query's directories, else from the metadata cache and then kept in the query's directories; null when
  // unknown
  private Set<String> directory(DeleteFile deleteFile) {
    Set<String> directory = directories.get(deleteFile.location());
    if (directory == null) {
      byte[] bytes = getCached(new DirectoryKey(deleteFile.location()));
      if (bytes != null) {
        directory = SerializationUtil.deserializeFromBytes(bytes);
        directories.putIfAbsent(deleteFile.location(), directory);
      }
    }
    return directory;
  }

  // the whole delete file, decoded once per query on a daemon
  private CharSequenceMap<byte[]> decoded(DeleteFile deleteFile) {
    return canCache(deleteFile.recordCount()) ?
        getOrLoad(deleteFile.location() + "#serialized", () -> decodePosDeletes(deleteFile),
            deleteFile.recordCount()) :
        decodePosDeletes(deleteFile);
  }

  /**
   * Decodes a whole position delete file. Serializing optimizes the bitmaps in place, so it has to happen before the
   * indexes are shared.
   * <p>
   * A directory is only published from a complete read, one that read every row of the delete file (its record
   * count): a directory that missed a data file the delete file references would drop that data file's deletes. The
   * reader filters no rows, so a read that does not fail is complete; a read that fails throws before the directory is
   * built.
   */
  private CharSequenceMap<byte[]> decodePosDeletes(DeleteFile deleteFile) {
    PositionDeletesReader.Decoded decoded = PositionDeletesReader.read(deleteFile, loadInputFile.apply(deleteFile));
    CharSequenceMap<byte[]> serialized = CharSequenceMap.create();
    decoded.indexes().forEach((dataFile, index) -> serialized.put(dataFile, index.serialize().array()));
    if (isPartitionScoped(deleteFile) && decoded.rows() == deleteFile.recordCount()) {
      Set<String> directory =
          serialized.keySet().stream().map(CharSequence::toString).collect(Collectors.toUnmodifiableSet());
      directories.putIfAbsent(deleteFile.location(), directory);
      putDirectory(new DirectoryKey(deleteFile.location()), SerializationUtil.serializeToBytes(directory), deleteFile);
    }
    return serialized;
  }

  private static boolean isPartitionScoped(DeleteFile deleteFile) {
    return ContentFileUtil.referencedDataFile(deleteFile) == null;
  }

  private static PositionDeleteIndex emptyIndex(DeleteFile deleteFile, CharSequence filePath) {
    return Deletes.toPositionIndex(filePath, CloseableIterable.<StructLike>empty(), deleteFile);
  }

  private byte[] getCached(Object key) {
    MemoryBufferOrBuffers buffers = metadataCache.getFileMetadata(key);
    if (buffers == null) {
      return null;
    }
    try {
      MemoryBuffer single = buffers.getSingleBuffer();
      MemoryBuffer[] parts = single != null ? new MemoryBuffer[] { single } : buffers.getMultipleBuffers();
      int length = 0;
      for (MemoryBuffer part : parts) {
        length += part.getByteBufferRaw().remaining();
      }
      ByteBuffer bytes = ByteBuffer.allocate(length);
      for (MemoryBuffer part : parts) {
        bytes.put(part.getByteBufferDup());
      }
      return bytes.array();
    } finally {
      metadataCache.decRefBuffer(buffers);
    }
  }

  // PUT and PUT_BYTES count the entries of data files
  private void putDeletes(DeletesKey key, byte[] bytes, DeleteFile deleteFile) {
    if (put(key, bytes, deleteFile)) {
      counter(DeleteCacheCounters.DELETE_CACHE_PUT).increment(1);
      counter(DeleteCacheCounters.DELETE_CACHE_PUT_BYTES).increment(bytes.length);
    }
  }

  private void putDirectory(DirectoryKey key, byte[] bytes, DeleteFile deleteFile) {
    put(key, bytes, deleteFile);
  }

  // Best effort: the deletes are already loaded, so caching them must neither fail nor block the read. A failed put of
  // either kind counts in PUT_FAILED; with task counters, only the first failure of a task is logged at INFO.
  private boolean put(Object key, byte[] bytes, DeleteFile deleteFile) {
    try {
      metadataCache.decRefBuffer(
          metadataCache.putFileMetadata(key, ByteBuffer.wrap(bytes), cacheTag(deleteFile), DO_NOT_WAIT));
      return true;
    } catch (RuntimeException e) {
      counter(DeleteCacheCounters.DELETE_CACHE_PUT_FAILED_BYTES).increment(bytes.length);
      Counter failed = counter(DeleteCacheCounters.DELETE_CACHE_PUT_FAILED);
      failed.increment(1);
      if (failed.getValue() == 1) {
        LOG.info("Could not cache {}; further failures of this task are logged at DEBUG", key, e);
      } else {
        LOG.debug("Could not cache {}", key, e);
      }
      return false;
    }
  }

  // Outside a Tez or MapReduce task there are no task counters, so every failed put is logged at INFO.
  private Counter counter(DeleteCacheCounters name) {
    Counter counter = context.getCounter(name);
    return counter != null ? counter : new Counters.Counter();
  }

  private CacheTag cacheTag(DeleteFile deleteFile) {
    return VectorizedParquetRecordReader.cacheTagOfParquetFile(
        new Path(deleteFile.location()), cacheConf, (JobConf) conf);
  }

  private record DeletesKey(String deleteFile, String dataFile) {
  }

  // the data files a partition-scoped delete file references
  @VisibleForTesting
  record DirectoryKey(String deleteFile) {
  }
}
