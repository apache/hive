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
package org.apache.hadoop.hive.llap.io.api.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.FileMetadataCache;
import org.apache.hadoop.hive.llap.cache.LlapCacheableBuffer;
import org.apache.hadoop.hive.llap.cache.LlapDataBuffer;
import org.apache.hadoop.hive.llap.cache.LowLevelCachePolicy;
import org.apache.hadoop.hive.llap.cache.PathCache;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.io.encoded.LlapOrcCacheLoader;
import org.apache.hadoop.hive.ql.io.SyntheticFileId;
import org.apache.hadoop.hive.ql.io.orc.encoded.IoTrace;
import org.apache.hive.common.util.FixedSizedObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Internal helper class for extracting the metadata of the cache content and loading data into the cache
 * based on the provided metadata.
 */
final class LlapCacheMetadataSerializer {

  public static final Logger LOG = LoggerFactory.getLogger(LlapCacheMetadataSerializer.class);

  private final FileMetadataCache metadataCache;
  private final DataCache cache;
  private final Configuration conf;
  private final PathCache pathCache;
  private final FixedSizedObjectPool<IoTrace> tracePool;
  private final LowLevelCachePolicy cachePolicy;

  LlapCacheMetadataSerializer(FileMetadataCache fileMetadataCache, DataCache cache, Configuration daemonConf,
      PathCache pathCache, FixedSizedObjectPool<IoTrace> tracePool, LowLevelCachePolicy realCachePolicy) {
    this.metadataCache = fileMetadataCache;
    this.cache = cache;
    this.conf = daemonConf;
    this.pathCache = pathCache;
    this.tracePool = tracePool;
    this.cachePolicy = realCachePolicy;

  }

  public LlapDaemonProtocolProtos.CacheEntryList fetchCachedContentInfo() {
    List<LlapCacheableBuffer> buffers = cachePolicy.getHotBuffers();
    List<LlapDaemonProtocolProtos.CacheEntry> entries = encodeAndConvertHotBuffers(buffers);
    return LlapDaemonProtocolProtos.CacheEntryList.newBuilder().addAllEntries(entries).build();
  }

  private List<LlapDaemonProtocolProtos.CacheEntry> encodeAndConvertHotBuffers(List<LlapCacheableBuffer> buffers) {
    Map<Object, LlapDaemonProtocolProtos.CacheEntry.Builder> entries = encodeAndSortHotBuffersByFileKey(buffers);
    return entries.values().stream().map(v -> v.build()).collect(Collectors.toList());
  }

  private Map<Object, LlapDaemonProtocolProtos.CacheEntry.Builder> encodeAndSortHotBuffersByFileKey(
      List<LlapCacheableBuffer> buffers) {
    Map<Object, LlapDaemonProtocolProtos.CacheEntry.Builder> lookupMap = new HashMap<>();
    for (LlapCacheableBuffer b : buffers) {
      if (b instanceof LlapDataBuffer) {
        LlapDataBuffer db = (LlapDataBuffer) b;
        try {
          Object fileKey = db.getFileKey();
          String path = pathCache.resolve(db.getFileKey());
          if (path != null) {
            LlapDaemonProtocolProtos.CacheEntry.Builder builder =
                lookupOrCreateCacheEntryFromDataBuffer(lookupMap, db, fileKey, path);
            LlapDaemonProtocolProtos.CacheEntryRange.Builder range =
                LlapDaemonProtocolProtos.CacheEntryRange.newBuilder().setStart(db.getStart())
                    .setEnd(db.getStart() + db.declaredCachedLength);
            builder.addRanges(range);
          }
        } catch (IOException ex) {
          // This should never happen. It only happens if we can't decode the fileKey.
          LOG.warn("Skip buffer from CacheEntryList.", ex);
        }
      }
    }
    return lookupMap;
  }

  private static LlapDaemonProtocolProtos.CacheEntry.Builder lookupOrCreateCacheEntryFromDataBuffer(
      Map<Object, LlapDaemonProtocolProtos.CacheEntry.Builder> lookupMap, LlapDataBuffer db, Object fileKey,
      String path) throws IOException {
    LlapDaemonProtocolProtos.CacheEntry.Builder builder = lookupMap.get(fileKey);
    if (builder == null) {
      ByteString encodedFileKey = encodeFileKey(fileKey);
      LlapDaemonProtocolProtos.CacheTag.Builder ctb = encodeCacheTag(db.getTag());
      builder = LlapDaemonProtocolProtos.CacheEntry.newBuilder().setFileKey(encodedFileKey).setCacheTag(ctb)
          .setFilePath(path);

      lookupMap.put(fileKey, builder);
    }
    return builder;
  }

  private static LlapDaemonProtocolProtos.CacheTag.Builder encodeCacheTag(CacheTag cacheTag) {
    LlapDaemonProtocolProtos.CacheTag.Builder ctb =
        LlapDaemonProtocolProtos.CacheTag.newBuilder().setTableName(cacheTag.getTableName());
    if (cacheTag instanceof CacheTag.PartitionCacheTag) {
      CacheTag.PartitionCacheTag partitionCacheTag = (CacheTag.PartitionCacheTag) cacheTag;
      ctb.addAllPartitionDesc(Arrays.asList((partitionCacheTag.getEncodedPartitionDesc())));
    }
    return ctb;
  }

  public void loadData(LlapDaemonProtocolProtos.CacheEntryList data) {
    for (LlapDaemonProtocolProtos.CacheEntry ce : data.getEntriesList()) {
      try {
        loadData(ce);
      } catch (IOException ex) {
        LOG.warn("Skip cache entry load to the cache.", ex);
      }
    }
  }

  private void loadData(LlapDaemonProtocolProtos.CacheEntry ce) throws IOException {
    CacheTag cacheTag = decodeCacheTag(ce.getCacheTag());
    DiskRangeList ranges = decodeRanges(ce.getRangesList());
    Object fileKey = decodeFileKey(ce.getFileKey());
    try (LlapOrcCacheLoader llr = new LlapOrcCacheLoader(new Path(ce.getFilePath()), fileKey, conf, cache,
        metadataCache, cacheTag, tracePool)) {
      llr.init();
      llr.loadFileFooter();
      llr.loadRanges(ranges);
    }
  }

  private static DiskRangeList decodeRanges(List<LlapDaemonProtocolProtos.CacheEntryRange> ranges) {
    DiskRangeList.CreateHelper helper = new DiskRangeList.CreateHelper();
    ranges.stream().sorted(Comparator.comparing(LlapDaemonProtocolProtos.CacheEntryRange::getStart))
        .forEach(r -> helper.addOrMerge(r.getStart(), r.getEnd(), false, false));
    return helper.get();
  }

  private static CacheTag decodeCacheTag(LlapDaemonProtocolProtos.CacheTag ct) {
    return ct.getPartitionDescCount() == 0 ? CacheTag.build(ct.getTableName()) : CacheTag
        .build(ct.getTableName(), ct.getPartitionDescList());
  }

  @VisibleForTesting
  static ByteString encodeFileKey(Object fileKey) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    if (fileKey instanceof SyntheticFileId) {
      SyntheticFileId fk = (SyntheticFileId) fileKey;
      fk.write(dos);
    } else {
      dos.writeLong((Long) fileKey);
    }
    return ByteString.copyFrom(baos.toByteArray());
  }

  /**
   *  If the underlying filesystem supports it, the file key can be a unique file/inode ID represented by a long,
   *  otherwise its a combination of the path hash, the modification time and the length of the file.
   *
   *  @see org.apache.hadoop.hive.llap.io.encoded.OrcEncodedDataReader#determineFileId
   */
  @VisibleForTesting
  static Object decodeFileKey(ByteString encodedFileKey) throws IOException {
    byte[] bytes = encodedFileKey.toByteArray();
    DataInput in = new DataInputStream(new ByteArrayInputStream(bytes));
    Object fileKey;
    if (bytes.length == Long.BYTES) {
      fileKey = in.readLong();
    } else {
      SyntheticFileId fileId = new SyntheticFileId();
      fileId.readFields(in);
      fileKey = fileId;
    }
    return fileKey;
  }
}
