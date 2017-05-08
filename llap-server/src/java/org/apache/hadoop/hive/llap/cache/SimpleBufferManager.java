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
package org.apache.hadoop.hive.llap.cache;

import java.util.List;

import org.apache.hadoop.hive.common.io.Allocator;
import org.apache.hadoop.hive.common.io.DataCache.BooleanRef;
import org.apache.hadoop.hive.common.io.DataCache.DiskRangeListFactory;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;

public class SimpleBufferManager implements BufferUsageManager, LowLevelCache {
  private final Allocator allocator;
  private final LlapDaemonCacheMetrics metrics;

  public SimpleBufferManager(Allocator allocator, LlapDaemonCacheMetrics metrics) {
    LlapIoImpl.LOG.info("Simple buffer manager");
    this.allocator = allocator;
    this.metrics = metrics;
  }

  private boolean lockBuffer(LlapDataBuffer buffer) {
    int rc = buffer.incRef();
    if (rc <= 0) return false;
    metrics.incrCacheNumLockedBuffers();
    return true;
  }

  private void unlockBuffer(LlapDataBuffer buffer) {
    if (buffer.decRef() == 0) {
      if (LlapIoImpl.CACHE_LOGGER.isTraceEnabled()) {
        LlapIoImpl.CACHE_LOGGER.trace("Deallocating {} that was not cached", buffer);
      }
      allocator.deallocate(buffer);
    }
    metrics.decrCacheNumLockedBuffers();
  }

  @Override
  public void decRefBuffer(MemoryBuffer buffer) {
    unlockBuffer((LlapDataBuffer)buffer);
  }

  @Override
  public void decRefBuffers(List<MemoryBuffer> cacheBuffers) {
    for (MemoryBuffer b : cacheBuffers) {
      unlockBuffer((LlapDataBuffer)b);
    }
  }

  @Override
  public boolean incRefBuffer(MemoryBuffer buffer) {
    return lockBuffer((LlapDataBuffer)buffer);
  }

  @Override
  public Allocator getAllocator() {
    return allocator;
  }

  @Override
  public DiskRangeList getFileData(Object fileKey, DiskRangeList range, long baseOffset,
      DiskRangeListFactory factory, LowLevelCacheCounters qfCounters, BooleanRef gotAllData) {
    return range; // Nothing changes - no cache.
  }

  @Override
  public long[] putFileData(Object fileKey, DiskRange[] ranges,
      MemoryBuffer[] chunks, long baseOffset, Priority priority,
      LowLevelCacheCounters qfCounters) {
    for (int i = 0; i < chunks.length; ++i) {
      LlapDataBuffer buffer = (LlapDataBuffer)chunks[i];
      if (LlapIoImpl.LOCKING_LOGGER.isTraceEnabled()) {
        LlapIoImpl.LOCKING_LOGGER.trace("Locking {} at put time (no cache)", buffer);
      }
      boolean canLock = lockBuffer(buffer);
      assert canLock;
    }
    return null;
  }

  @Override
  public void notifyEvicted(MemoryBuffer buffer) {
    throw new UnsupportedOperationException("Buffer manager doesn't have cache");
  }
}
