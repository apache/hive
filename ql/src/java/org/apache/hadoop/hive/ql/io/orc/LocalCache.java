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

package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.orc.impl.OrcTail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;

class LocalCache implements OrcInputFormat.FooterCache {
  private static final Logger LOG = LoggerFactory.getLogger(LocalCache.class);
  private static final int DEFAULT_CACHE_INITIAL_CAPACITY = 1024;

  private static final class TailAndFileData {
    public TailAndFileData(long fileLength, long fileModificationTime, ByteBuffer bb) {
      this.fileLength = fileLength;
      this.fileModTime = fileModificationTime;
      this.bb = bb;
    }
    public ByteBuffer bb;
    public long fileLength, fileModTime;

    public int getMemoryUsage() {
      return bb.capacity() + 100; // 100 is for 2 longs, BB and java overheads (semi-arbitrary).
    }
  }

  private final Cache<Path, TailAndFileData> cache;

  LocalCache(int numThreads, long cacheMemSize, boolean useSoftRef) {
    CacheBuilder<Path, TailAndFileData> builder = CacheBuilder.newBuilder()
        .initialCapacity(DEFAULT_CACHE_INITIAL_CAPACITY)
        .concurrencyLevel(numThreads)
        .maximumWeight(cacheMemSize)
        .weigher(new Weigher<Path, TailAndFileData>() {
          @Override
          public int weigh(Path key, TailAndFileData value) {
            return value.getMemoryUsage();
          }
        });

    if (useSoftRef) {
      builder = builder.softValues();
    }
    cache = builder.build();
  }

  public void clear() {
    cache.invalidateAll();
    cache.cleanUp();
  }

  public void put(Path path, OrcTail tail) {
    ByteBuffer bb = tail.getSerializedTail();
    if (bb.capacity() != bb.remaining()) {
      throw new RuntimeException("Bytebuffer allocated for path: " + path + " has remaining: " + bb.remaining() + " != capacity: " + bb.capacity());
    }
    cache.put(path, new TailAndFileData(tail.getFileTail().getFileLength(),
        tail.getFileModificationTime(), bb.duplicate()));
  }

  @Override
  public void getAndValidate(final List<HadoopShims.HdfsFileStatusWithId> files,
      final boolean isOriginal,
      final OrcTail[] result, final ByteBuffer[] ppdResult)
      throws IOException, HiveException {
    // TODO: should local cache also be by fileId? Preserve the original logic for now.
    assert result.length == files.size();
    int i = -1;
    for (HadoopShims.HdfsFileStatusWithId fileWithId : files) {
      ++i;
      FileStatus file = fileWithId.getFileStatus();
      Path path = file.getPath();
      TailAndFileData tfd = cache.getIfPresent(path);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Serialized tail " + (tfd == null ? "not " : "") + "cached for path: " + path);
      }
      if (tfd == null) continue;
      if (file.getLen() == tfd.fileLength && file.getModificationTime() == tfd.fileModTime) {
        result[i] = ReaderImpl.extractFileTail(tfd.bb.duplicate(), tfd.bb.limit(), tfd.fileModTime);
        continue;
      }
      // Invalidate
      cache.invalidate(path);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Meta-Info for : " + path + " changed. CachedModificationTime: "
            + tfd.fileModTime + ", CurrentModificationTime: " + file.getModificationTime()
            + ", CachedLength: " + tfd.fileLength + ", CurrentLength: " + file.getLen());
      }
    }
  }

  @Override
  public boolean hasPpd() {
    return false;
  }

  @Override
  public boolean isBlocking() {
    return false;
  }

  @Override
  public void put(final OrcInputFormat.FooterCacheKey cacheKey, final OrcTail orcTail)
      throws IOException {
    put(cacheKey.getPath(), orcTail);
  }
}
