/**
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

class LocalCache implements OrcInputFormat.FooterCache {
  private static final Logger LOG = LoggerFactory.getLogger(LocalCache.class);
  private static final int DEFAULT_CACHE_INITIAL_CAPACITY = 1024;
  private final Cache<Path, OrcTail> cache;

  LocalCache(int numThreads, int cacheStripeDetailsSize, boolean useSoftRef) {
    CacheBuilder builder = CacheBuilder.newBuilder()
        .initialCapacity(DEFAULT_CACHE_INITIAL_CAPACITY)
        .concurrencyLevel(numThreads)
        .maximumSize(cacheStripeDetailsSize);
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
    cache.put(path, tail);
  }

  public OrcTail get(Path path) {
    return cache.getIfPresent(path);
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
      OrcTail tail = cache.getIfPresent(path);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Serialized tail " + (tail == null ? "not " : "") + "cached for path: " + path);
      }
      if (tail == null) continue;
      if (tail != null && file.getLen() == tail.getFileTail().getFileLength()
          && file.getModificationTime() == tail.getFileModificationTime()) {
        result[i] = tail;
        continue;
      }
      // Invalidate
      cache.invalidate(path);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Meta-Info for : " + path + " changed. CachedModificationTime: "
            + tail.getFileModificationTime() + ", CurrentModificationTime: "
            + file.getModificationTime() + ", CachedLength: " + tail.getFileTail().getFileLength()
            + ", CurrentLength: " + file.getLen());
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