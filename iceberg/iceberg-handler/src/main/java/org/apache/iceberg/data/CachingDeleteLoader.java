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
import java.util.function.Function;
import java.util.function.Supplier;
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
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.CharSequenceMap;
import org.apache.iceberg.util.ContentFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachingDeleteLoader extends BaseDeleteLoader {
  private static final Logger LOG = LoggerFactory.getLogger(CachingDeleteLoader.class);

  private final ObjectCache cache;
  private final Function<DeleteFile, InputFile> loadInputFile;
  private final Configuration conf;
  private final TaskAttemptContext context;
  private final FileMetadataCache metadataCache;
  private final Configuration cacheConf;
  private final boolean cachePositionDeleteFiles;

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
    String cacheLevel = HiveConf.getVar(conf, HiveConf.ConfVars.LLAP_IO_CACHE_DELETES);
    this.metadataCache = "none".equals(cacheLevel) ? null : metadataCache;
    this.cacheConf = cacheConf;
    this.cachePositionDeleteFiles = "all".equals(cacheLevel);
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
    DeletesKey key = new DeletesKey(dv.location(), filePath.toString());
    byte[] bytes = getCached(key);
    if (bytes != null) {
      return PositionDeleteIndex.deserialize(bytes, dv);
    }
    bytes = new byte[dv.contentSizeInBytes().intValue()];
    try {
      IOUtil.readFully(loadInputFile.apply(dv), dv.contentOffset(), bytes, 0, bytes.length);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    PositionDeleteIndex index = PositionDeleteIndex.deserialize(bytes, dv);
    putCached(key, bytes, cacheTag(dv));
    return index;
  }

  /**
   * On a miss the whole position delete file is decoded, once per query on a daemon, and only the entry of the
   * requested data file is cached: a daemon caches the deletes of the data files it reads. A data file the delete file
   * does not reference gets an empty entry of its own. The decoded file has a key of its own in the query's cache: a
   * reader without the metadata cache (BaseDeleteLoader) caches its indexes under the location.
   */
  private PositionDeleteIndex readPosDeletes(DeleteFile deleteFile, CharSequence filePath) {
    DeletesKey key = new DeletesKey(deleteFile.location(), filePath.toString());
    byte[] bytes = getCached(key);
    if (bytes == null) {
      CharSequenceMap<byte[]> serialized = canCache(deleteFile.recordCount()) ?
          getOrLoad(deleteFile.location() + "#serialized", () -> decodePosDeletes(deleteFile),
              deleteFile.recordCount()) :
          decodePosDeletes(deleteFile);
      bytes = serialized.get(filePath);
      if (bytes == null) {
        bytes = Deletes.toPositionIndex(filePath, CloseableIterable.<StructLike>empty(), deleteFile)
            .serialize().array();
      }
      putCached(key, bytes, cacheTag(deleteFile));
    }
    return DeletionVectors.deserialize(bytes, deleteFile);
  }

  // Serializing optimizes the bitmaps in place, so it has to happen before the indexes are shared.
  private CharSequenceMap<byte[]> decodePosDeletes(DeleteFile deleteFile) {
    CharSequenceMap<byte[]> serialized = CharSequenceMap.create();
    PositionDeletesReader.read(deleteFile, loadInputFile.apply(deleteFile))
        .forEach((dataFile, index) -> serialized.put(dataFile, index.serialize().array()));
    return serialized;
  }

  private byte[] getCached(DeletesKey key) {
    MemoryBufferOrBuffers buffers = metadataCache.getFileMetadata(key);
    if (buffers == null) {
      counter(DeleteCacheCounters.DELETE_CACHE_MISS).increment(1);
      return null;
    }
    counter(DeleteCacheCounters.DELETE_CACHE_HIT).increment(1);
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

  // Best effort: the deletes are already loaded, so failing to cache them must not fail the read. With task counters,
  // only the first failure of a task is logged at INFO.
  private void putCached(DeletesKey key, byte[] bytes, CacheTag tag) {
    try {
      metadataCache.decRefBuffer(metadataCache.putFileMetadata(key, ByteBuffer.wrap(bytes), tag, null));
      counter(DeleteCacheCounters.DELETE_CACHE_PUT).increment(1);
      counter(DeleteCacheCounters.DELETE_CACHE_PUT_BYTES).increment(bytes.length);
    } catch (RuntimeException e) {
      counter(DeleteCacheCounters.DELETE_CACHE_PUT_FAILED_BYTES).increment(bytes.length);
      Counter failed = counter(DeleteCacheCounters.DELETE_CACHE_PUT_FAILED);
      failed.increment(1);
      if (failed.getValue() == 1) {
        LOG.info("Could not cache the deletes of {} for {}; further failures of this task are logged at DEBUG",
            key.deleteFile(), key.dataFile(), e);
      } else {
        LOG.debug("Could not cache the deletes of {} for {}", key.deleteFile(), key.dataFile(), e);
      }
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
}
