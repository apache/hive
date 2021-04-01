/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.io;

import org.apache.hadoop.hive.common.io.Allocator;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.llap.cache.LlapCacheableBuffer;
import org.apache.hadoop.hive.llap.cache.LlapDataBuffer;
import org.apache.hadoop.hive.llap.cache.LowLevelCache;
import org.apache.hadoop.hive.llap.cache.LowLevelCacheImpl;
import org.apache.hadoop.hive.llap.cache.LowLevelCachePolicy;
import org.apache.hadoop.hive.ql.io.orc.encoded.CacheChunk;

/**
 * Class for collecting some mocked classes.
 * E.g: DataCache, Allocator.BufferObjectFactory, DiskRangeListFactory
 */
public final class LlapIoMocks {
  private LlapIoMocks() {
    // class should not be instantiated
  }

  public static final class MockDataCache implements DataCache, Allocator.BufferObjectFactory {

    private LowLevelCacheImpl cache;
    private Allocator allocator;
    private LowLevelCachePolicy cachePolicy;

    public MockDataCache(LowLevelCacheImpl cache, Allocator allocator, LowLevelCachePolicy cachePolicy) {
      this.cache = cache;
      this.allocator = allocator;
      this.cachePolicy = cachePolicy;
    }

    @Override public MemoryBuffer create() {
      return new LlapDataBuffer();
    }

    @Override public DiskRangeList getFileData(Object fileKey, DiskRangeList range, long baseOffset,
        DiskRangeListFactory factory, BooleanRef gotAllData) {
      return cache.getFileData(fileKey, range, baseOffset, factory, null, gotAllData);
    }

    @Override public long[] putFileData(Object fileKey, DiskRange[] ranges, MemoryBuffer[] data, long baseOffset) {
      return data != null ? cache
          .putFileData(fileKey, ranges, data, baseOffset, LowLevelCache.Priority.NORMAL, null, null) : null;
    }

    @Override public void releaseBuffer(MemoryBuffer buffer) {
      cachePolicy.notifyUnlock((LlapCacheableBuffer) buffer);
    }

    @Override public void reuseBuffer(MemoryBuffer buffer) {

    }

    @Override public Allocator getAllocator() {
      return allocator;
    }

    @Override public Allocator.BufferObjectFactory getDataBufferFactory() {
      return this;
    }

    @Override public long[] putFileData(Object fileKey, DiskRange[] ranges, MemoryBuffer[] data, long baseOffset,
        CacheTag tag) {
      return data != null ? cache
          .putFileData(fileKey, ranges, data, baseOffset, LowLevelCache.Priority.NORMAL, null, tag) : null;
    }
  }

  public static final class MockDiskRangeListFactory implements DataCache.DiskRangeListFactory {

    @Override public DiskRangeList createCacheChunk(MemoryBuffer buffer, long startOffset, long endOffset) {
      return new CacheChunk(buffer, startOffset, endOffset);
    }

  }
}
