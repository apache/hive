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

package org.apache.hadoop.hive.llap.io.metadata;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.io.FileMetadataCache;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.common.io.encoded.MemoryBufferOrBuffers;
import org.apache.hadoop.hive.llap.cache.EvictionAwareAllocator;
import org.apache.hadoop.hive.llap.cache.EvictionDispatcher;
import org.apache.hadoop.hive.llap.cache.LlapAllocatorBuffer;
import org.apache.hadoop.hive.llap.cache.LlapOomDebugDump;
import org.apache.hadoop.hive.llap.cache.LowLevelCachePolicy;
import org.apache.hadoop.hive.llap.cache.MemoryManager;
import org.apache.hadoop.hive.llap.cache.LowLevelCache.Priority;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.apache.parquet.io.SeekableInputStream;

// TODO# merge with OrcMetadataCache (and rename) after HIVE-15665. Shares a lot of the code.
public class ParquetMetadataCacheImpl implements LlapOomDebugDump, FileMetadataCache {
  private final ConcurrentHashMap<Object, LlapBufferOrBuffers> metadata =
      new ConcurrentHashMap<>();

  private final MemoryManager memoryManager;
  private final LowLevelCachePolicy policy;
  private final EvictionAwareAllocator allocator;
  private final LlapDaemonCacheMetrics metrics;

  public ParquetMetadataCacheImpl(EvictionAwareAllocator allocator, MemoryManager memoryManager,
      LowLevelCachePolicy policy, LlapDaemonCacheMetrics metrics) {
    this.memoryManager = memoryManager;
    this.allocator = allocator;
    this.policy = policy;
    this.metrics = metrics;
  }

  public void notifyEvicted(LlapFileMetadataBuffer buffer) {
    LlapBufferOrBuffers removed = metadata.remove(buffer.getFileKey());
    if (removed == null) return;
    if (removed.getSingleBuffer() != null) {
      assert removed.getSingleBuffer() == buffer;
      return;
    }
    discardMultiBuffer(removed);
  }

  @Override
  public LlapBufferOrBuffers getFileMetadata(Object fileKey) {
    LlapBufferOrBuffers result = metadata.get(fileKey);
    if (result == null) return null;
    if (!lockBuffer(result, true)) {
      // No need to discard the buffer we cannot lock - eviction takes care of that.
      metadata.remove(fileKey, result);
      return null;
    }
    return result;
  }

  @Override
  public LlapBufferOrBuffers putFileMetadata(
      Object fileKey, int length, InputStream is) throws IOException {
    LlapBufferOrBuffers result = null;
    while (true) { // Overwhelmingly executes once, or maybe twice (replacing stale value).
      LlapBufferOrBuffers oldVal = metadata.get(fileKey);
      if (oldVal == null) {
        result = wrapBbForFile(result, fileKey, length, is);
        if (!lockBuffer(result, false)) {
          throw new AssertionError("Cannot lock a newly created value " + result);
        }
        oldVal = metadata.putIfAbsent(fileKey, result);
        if (oldVal == null) {
          cacheInPolicy(result); // Cached successfully, add to policy.
          return result;
        }
      }
      if (lockOldVal(fileKey, result, oldVal)) {
        return oldVal;
      }
      // We found some old value but couldn't incRef it; remove it.
      metadata.remove(fileKey, oldVal);
    }
  }

  private void cacheInPolicy(LlapBufferOrBuffers buffers) {
    LlapAllocatorBuffer singleBuffer = buffers.getSingleLlapBuffer();
    if (singleBuffer != null) {
      policy.cache(singleBuffer, Priority.HIGH);
      return;
    }
    for (LlapAllocatorBuffer buffer : buffers.getMultipleLlapBuffers()) {
      policy.cache(buffer, Priority.HIGH);
    }
  }

  private <T extends LlapBufferOrBuffers> boolean lockOldVal(Object key, T newVal, T oldVal) {
    if (LlapIoImpl.CACHE_LOGGER.isTraceEnabled()) {
      LlapIoImpl.CACHE_LOGGER.trace("Trying to cache when metadata is already cached for" +
          " {}; old {}, new {}", key, oldVal, newVal);
    }
    if (LlapIoImpl.LOCKING_LOGGER.isTraceEnabled()) {
      LlapIoImpl.LOCKING_LOGGER.trace("Locking {} due to cache collision", oldVal);
    }
    if (lockBuffer(oldVal, true)) {
      // We found an old, valid block for this key in the cache.
      if (LlapIoImpl.LOCKING_LOGGER.isTraceEnabled()) {
        LlapIoImpl.LOCKING_LOGGER.trace("Unlocking {} due to cache collision with {}",
            newVal, oldVal);
      }

      if (newVal != null) {
        unlockBuffer(newVal, false);
      }
      return true;
    }
    return false;
  }

  @Override
  public void decRefBuffer(MemoryBufferOrBuffers buffer) {
    if (!(buffer instanceof LlapBufferOrBuffers)) {
      throw new AssertionError(buffer.getClass());
    }
    unlockBuffer((LlapBufferOrBuffers)buffer, true);
  }

  private LlapBufferOrBuffers wrapBbForFile(LlapBufferOrBuffers result,
      Object fileKey, int length, InputStream stream) throws IOException {
    if (result != null) return result;
    int maxAlloc = allocator.getMaxAllocation();
    LlapFileMetadataBuffer[] largeBuffers = null;
    if (maxAlloc < length) {
      largeBuffers = new LlapFileMetadataBuffer[length / maxAlloc];
      for (int i = 0; i < largeBuffers.length; ++i) {
        largeBuffers[i] = new LlapFileMetadataBuffer(fileKey);
      }
      allocator.allocateMultiple(largeBuffers, maxAlloc, null);
      for (int i = 0; i < largeBuffers.length; ++i) {
        readIntoCacheBuffer(stream, maxAlloc, largeBuffers[i]);
      }
    }
    int smallSize = length % maxAlloc;
    if (smallSize == 0) {
      return new LlapFileMetadataBuffers(largeBuffers);
    } else {
      LlapFileMetadataBuffer[] smallBuffer = new LlapFileMetadataBuffer[1];
      smallBuffer[0] = new LlapFileMetadataBuffer(fileKey);
      allocator.allocateMultiple(smallBuffer, length, null);
      readIntoCacheBuffer(stream, smallSize, smallBuffer[0]);
      if (largeBuffers == null) {
        return smallBuffer[0];
      } else {
        LlapFileMetadataBuffer[] cacheData = new LlapFileMetadataBuffer[largeBuffers.length + 1];
        System.arraycopy(largeBuffers, 0, cacheData, 0, largeBuffers.length);
        cacheData[largeBuffers.length] = smallBuffer[0];
        return new LlapFileMetadataBuffers(largeBuffers);
      }
    }
  }

  private static void readIntoCacheBuffer(
      InputStream stream, int length, MemoryBuffer dest) throws IOException {
    ByteBuffer bb = dest.getByteBufferRaw();
    int pos = bb.position();
    bb.limit(pos + length);
    // TODO: SeekableInputStream.readFully eventually calls a Hadoop method that used to be
    //       buggy in 2.7 and also anyway just does a copy for a direct buffer. Do a copy here.
    // ((SeekableInputStream)stream).readFully(bb);
    FileUtils.readFully(stream, length, bb);
    bb.position(pos);
  }


  private boolean lockBuffer(LlapBufferOrBuffers buffers, boolean doNotifyPolicy) {
    LlapAllocatorBuffer buffer = buffers.getSingleLlapBuffer();
    if (buffer != null) {
      return lockOneBuffer(buffer, doNotifyPolicy);
    }
    LlapAllocatorBuffer[] bufferArray = buffers.getMultipleLlapBuffers();
    for (int i = 0; i < bufferArray.length; ++i) {
      if (lockOneBuffer(bufferArray[i], doNotifyPolicy)) continue;
      for (int j = 0; j < i; ++j) {
        unlockSingleBuffer(buffer, true);
      }
      discardMultiBuffer(buffers);
      return false;
    }
    return true;
  }

  private void discardMultiBuffer(LlapBufferOrBuffers removed) {
    long memoryFreed = 0;
    for (LlapAllocatorBuffer buf : removed.getMultipleLlapBuffers()) {
      long memUsage = buf.getMemoryUsage();
      // We cannot just deallocate the buffer, as it can hypothetically have users.
      int result = buf.invalidate();
      switch (result) {
      case LlapAllocatorBuffer.INVALIDATE_ALREADY_INVALID: continue; // Nothing to do.
      case LlapAllocatorBuffer.INVALIDATE_FAILED: {
        // Someone is using this buffer; eventually, it will be evicted.
        continue;
      }
      case LlapAllocatorBuffer.INVALIDATE_OK: {
        memoryFreed += memUsage;
        allocator.deallocateEvicted(buf);
        break;
      }
      default: throw new AssertionError(result);
      }
    }
    memoryManager.releaseMemory(memoryFreed);
  }

  private boolean lockOneBuffer(LlapAllocatorBuffer buffer, boolean doNotifyPolicy) {
    int rc = buffer.incRef();
    if (rc > 0) {
      metrics.incrCacheNumLockedBuffers();
    }
    if (doNotifyPolicy && rc == 1) {
      // We have just locked a buffer that wasn't previously locked.
      policy.notifyLock(buffer);
    }
    return rc > 0;
  }

  private void unlockBuffer(LlapBufferOrBuffers buffers, boolean isCached) {
    LlapAllocatorBuffer singleBuffer = buffers.getSingleLlapBuffer();
    if (singleBuffer != null) {
      unlockSingleBuffer(singleBuffer, isCached);
      return;
    }
    for (LlapAllocatorBuffer buffer : buffers.getMultipleLlapBuffers()) {
      unlockSingleBuffer(buffer, isCached);
    }
  }

  private void unlockSingleBuffer(LlapAllocatorBuffer buffer, boolean isCached) {
    boolean isLastDecref = (buffer.decRef() == 0);
    if (isLastDecref) {
      if (isCached) {
        policy.notifyUnlock(buffer);
      } else {
        allocator.deallocate(buffer);
      }
    }
    metrics.decrCacheNumLockedBuffers();
  }


  public static interface LlapBufferOrBuffers extends MemoryBufferOrBuffers {
    LlapAllocatorBuffer getSingleLlapBuffer();
    LlapAllocatorBuffer[] getMultipleLlapBuffers();
  }

  public final static class LlapFileMetadataBuffer
      extends LlapAllocatorBuffer implements LlapBufferOrBuffers {
    private final Object fileKey;

    public LlapFileMetadataBuffer(Object fileKey) {
      this.fileKey = fileKey;
    }

    @Override
    public void notifyEvicted(EvictionDispatcher evictionDispatcher) {
      evictionDispatcher.notifyEvicted(this);
    }

    public Object getFileKey() {
      return fileKey;
    }

    @Override
    public LlapAllocatorBuffer getSingleLlapBuffer() {
      return this;
    }

    @Override
    public LlapAllocatorBuffer[] getMultipleLlapBuffers() {
      return null;
    }

    @Override
    public MemoryBuffer getSingleBuffer() {
      return this;
    }

    @Override
    public MemoryBuffer[] getMultipleBuffers() {
      return null;
    }
  }

  public final static class LlapFileMetadataBuffers implements LlapBufferOrBuffers {
    private final LlapFileMetadataBuffer[] buffers;

    public LlapFileMetadataBuffers(LlapFileMetadataBuffer[] buffers) {
      this.buffers = buffers;
    }

    @Override
    public LlapAllocatorBuffer getSingleLlapBuffer() {
      return null;
    }

    @Override
    public LlapAllocatorBuffer[] getMultipleLlapBuffers() {
      return buffers;
    }

    @Override
    public MemoryBuffer getSingleBuffer() {
      return null;
    }

    @Override
    public MemoryBuffer[] getMultipleBuffers() {
      return buffers;
    }
  }

  @Override
  public String debugDumpForOom() {
    // TODO: nothing, will be merged with ORC cache
    return null;
  }

  @Override
  public void debugDumpShort(StringBuilder sb) {
    // TODO: nothing, will be merged with ORC cache
  }
}