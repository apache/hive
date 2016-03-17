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
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat.FileInfo;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat.FooterCache;
import org.apache.hadoop.hive.shims.HadoopShims.HdfsFileStatusWithId;
import org.apache.orc.FileMetaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/** Local footer cache using Guava. Stores convoluted Java objects. */
class LocalCache implements FooterCache {
  private static final Logger LOG = LoggerFactory.getLogger(LocalCache.class);
  private static boolean isDebugEnabled = LOG.isDebugEnabled();

  private final Cache<Path, FileInfo> cache;

  public LocalCache(int numThreads, int cacheStripeDetailsSize) {
    cache = CacheBuilder.newBuilder()
      .concurrencyLevel(numThreads)
      .initialCapacity(cacheStripeDetailsSize)
      .maximumSize(cacheStripeDetailsSize)
      .softValues()
      .build();
  }

  public void clear() {
    cache.invalidateAll();
    cache.cleanUp();
  }

  public void getAndValidate(List<HdfsFileStatusWithId> files, boolean isOriginal,
      FileInfo[] result, ByteBuffer[] ppdResult) throws IOException {
    // TODO: should local cache also be by fileId? Preserve the original logic for now.
    assert result.length == files.size();
    int i = -1;
    for (HdfsFileStatusWithId fileWithId : files) {
      ++i;
      FileStatus file = fileWithId.getFileStatus();
      Path path = file.getPath();
      Long fileId = fileWithId.getFileId();
      FileInfo fileInfo = cache.getIfPresent(path);
      if (isDebugEnabled) {
        LOG.debug("Info " + (fileInfo == null ? "not " : "") + "cached for path: " + path);
      }
      if (fileInfo == null) continue;
      if ((fileId != null && fileInfo.fileId != null && fileId == fileInfo.fileId)
          || (fileInfo.modificationTime == file.getModificationTime() &&
          fileInfo.size == file.getLen())) {
        result[i] = fileInfo;
        continue;
      }
      // Invalidate
      cache.invalidate(path);
      if (isDebugEnabled) {
        LOG.debug("Meta-Info for : " + path + " changed. CachedModificationTime: "
            + fileInfo.modificationTime + ", CurrentModificationTime: "
            + file.getModificationTime() + ", CachedLength: " + fileInfo.size
            + ", CurrentLength: " + file.getLen());
      }
    }
  }

  public void put(Path path, FileInfo fileInfo) {
    cache.put(path, fileInfo);
  }

  @Override
  public void put(Long fileId, FileStatus file, FileMetaInfo fileMetaInfo, Reader orcReader)
      throws IOException {
    cache.put(file.getPath(), new FileInfo(file.getModificationTime(), file.getLen(),
        orcReader.getStripes(), orcReader.getStripeStatistics(), orcReader.getTypes(),
        orcReader.getOrcProtoFileStatistics(), fileMetaInfo, orcReader.getWriterVersion(),
        fileId));
  }

  @Override
  public boolean isBlocking() {
    return false;
  }

  @Override
  public boolean hasPpd() {
    return false;
  }
}