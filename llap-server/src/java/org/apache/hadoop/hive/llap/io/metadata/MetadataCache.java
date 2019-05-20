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

package org.apache.hadoop.hive.llap.io.metadata;

import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.io.FileMetadataCache;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.hive.common.io.encoded.MemoryBufferOrBuffers;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.DataCache.BooleanRef;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.llap.cache.BuddyAllocator;
import org.apache.hadoop.hive.llap.cache.EvictionAwareAllocator;
import org.apache.hadoop.hive.llap.cache.EvictionDispatcher;
import org.apache.hadoop.hive.llap.cache.LlapAllocatorBuffer;
import org.apache.hadoop.hive.llap.cache.LlapIoDebugDump;
import org.apache.hadoop.hive.llap.cache.LowLevelCachePolicy;
import org.apache.hadoop.hive.llap.cache.MemoryManager;
import org.apache.hadoop.hive.llap.cache.LowLevelCache.Priority;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.apache.hadoop.hive.ql.io.orc.encoded.OrcBatchKey;
import org.apache.hadoop.hive.ql.io.orc.encoded.StoppableAllocator;

public class MetadataCache implements LlapIoDebugDump, FileMetadataCache {
  private final ConcurrentHashMap<Object, LlapBufferOrBuffers> metadata =
      new ConcurrentHashMap<>();

  private final ConcurrentHashMap<Object, OrcFileEstimateErrors> estimateErrors;
  private final MemoryManager memoryManager;
  private final LowLevelCachePolicy policy;
  private final BuddyAllocator allocator;
  private final LlapDaemonCacheMetrics metrics;

  public MetadataCache(BuddyAllocator allocator, MemoryManager memoryManager,
      LowLevelCachePolicy policy, boolean useEstimateCache, LlapDaemonCacheMetrics metrics) {
    this.memoryManager = memoryManager;
    this.allocator = allocator;
    this.policy = policy;
    this.metrics = metrics;
    this.estimateErrors = useEstimateCache
        ? new ConcurrentHashMap<Object, OrcFileEstimateErrors>() : null;
  }

  public void putIncompleteCbs(Object fileKey, DiskRange[] ranges, long baseOffset, AtomicBoolean isStopped) {
    if (estimateErrors == null) return;
    OrcFileEstimateErrors errorData = estimateErrors.get(fileKey);
    boolean isNew = false;
    // We should technically update memory usage if updating the old object, but we don't do it
    // for now; there is no mechanism to properly notify the cache policy/etc. wrt parallel evicts.
    if (errorData == null) {
      errorData = new OrcFileEstimateErrors(fileKey);
      for (DiskRange range : ranges) {
        errorData.addError(range.getOffset(), range.getLength(), baseOffset);
      }
      long memUsage = errorData.estimateMemoryUsage();
      memoryManager.reserveMemory(memUsage, isStopped);
      OrcFileEstimateErrors old = estimateErrors.putIfAbsent(fileKey, errorData);
      if (old != null) {
        errorData = old;
        memoryManager.releaseMemory(memUsage);
        policy.notifyLock(errorData);
      } else {
        isNew = true;
        policy.cache(errorData, Priority.NORMAL);
      }
    } else {
      policy.notifyLock(errorData);
    }
    if (!isNew) {
      for (DiskRange range : ranges) {
        errorData.addError(range.getOffset(), range.getLength(), baseOffset);
      }
    }
    policy.notifyUnlock(errorData);
  }

  public DiskRangeList getIncompleteCbs(
      Object fileKey, DiskRangeList ranges, long baseOffset, BooleanRef gotAllData) {
    if (estimateErrors == null) return ranges;
    OrcFileEstimateErrors errors = estimateErrors.get(fileKey);
    if (errors == null) return ranges;
    policy.notifyLock(errors);
    policy.notifyUnlock(errors); // Never locked for eviction; Java object.
    return errors.getIncompleteCbs(ranges, baseOffset, gotAllData);
  }

  public void notifyEvicted(LlapMetadataBuffer<?> buffer) {
    LlapBufferOrBuffers removed = metadata.remove(buffer.getKey());
    if (removed == null) return;
    if (removed.getSingleBuffer() != null) {
      assert removed.getSingleBuffer() == buffer;
      return;
    }
    discardMultiBuffer(removed);
  }

  public void notifyEvicted(OrcFileEstimateErrors buffer) {
    estimateErrors.remove(buffer.getFileKey());
  }


  @Override
  public void debugDumpShort(StringBuilder sb) {
    sb.append("\nMetadata cache state: ")
        .append(metadata.size())
        .append(" files and stripes, ")
        .append(metadata.values().parallelStream().mapToLong(value -> {
          if (value.getSingleLlapBuffer() != null) {
            return value.getSingleLlapBuffer().allocSize;
          }
          long sum = 0;
          for (LlapAllocatorBuffer llapMetadataBuffer : value.getMultipleLlapBuffers()) {
            sum += llapMetadataBuffer.allocSize;
          }
          return sum;
        }).sum())
        .append(" total used bytes, ")
        .append(estimateErrors.size())
        .append(" files w/ORC estimate");
  }

  @Override
  public LlapBufferOrBuffers getFileMetadata(Object fileKey) {
    return getInternal(fileKey);
  }

  public LlapBufferOrBuffers getStripeTail(OrcBatchKey stripeKey) {
    return getInternal(new StripeKey(stripeKey.fileKey, stripeKey.stripeIx));
  }

  private LlapBufferOrBuffers getInternal(Object key) {
    LlapBufferOrBuffers result = metadata.get(key);
    if (result == null) return null;
    if (!lockBuffer(result, true)) {
      // No need to discard the buffer we cannot lock - eviction takes care of that.
      metadata.remove(key, result);
      return null;
    }
    return result;
  }

  @Override
  public MemoryBufferOrBuffers putFileMetadata(Object fileKey,
      ByteBuffer tailBuffer) {
    return putInternal(fileKey, tailBuffer, null, null);
  }

  @Override
  public MemoryBufferOrBuffers putFileMetadata(Object fileKey,
      ByteBuffer tailBuffer, String tag) {
    return putInternal(fileKey, tailBuffer, tag, null);
  }

  @Override
  public MemoryBufferOrBuffers putFileMetadata(Object fileKey, int length,
      InputStream is) throws IOException {
    return putFileMetadata(fileKey, length, is, null, null);
  }

  public LlapBufferOrBuffers putStripeTail(
      OrcBatchKey stripeKey, ByteBuffer tailBuffer, String tag, AtomicBoolean isStopped) {
    return putInternal(new StripeKey(stripeKey.fileKey, stripeKey.stripeIx), tailBuffer, tag, isStopped);
  }

  @Override
  public MemoryBufferOrBuffers putFileMetadata(Object fileKey, int length,
      InputStream is, String tag) throws IOException {
    return putFileMetadata(fileKey, length, is, tag, null);
  }


  @Override
  public LlapBufferOrBuffers putFileMetadata(Object fileKey,
      ByteBuffer tailBuffer, String tag, AtomicBoolean isStopped) {
    return putInternal(fileKey, tailBuffer, tag, isStopped);
  }

  @Override
  public LlapBufferOrBuffers putFileMetadata(Object fileKey, int length, InputStream is,
      String tag, AtomicBoolean isStopped) throws IOException {
    LlapBufferOrBuffers result = null;
    while (true) { // Overwhelmingly executes once, or maybe twice (replacing stale value).
      LlapBufferOrBuffers oldVal = metadata.get(fileKey);
      if (oldVal == null) {
        result = wrapBbForFile(result, fileKey, length, is, tag, isStopped);
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


  @SuppressWarnings({ "rawtypes", "unchecked" })
  private LlapBufferOrBuffers wrapBbForFile(LlapBufferOrBuffers result,
      Object fileKey, int length, InputStream stream, String tag, AtomicBoolean isStopped) throws IOException {
    if (result != null) return result;
    int maxAlloc = allocator.getMaxAllocation();
    LlapMetadataBuffer<Object>[] largeBuffers = null;
    if (maxAlloc < length) {
      largeBuffers = new LlapMetadataBuffer[length / maxAlloc];
      for (int i = 0; i < largeBuffers.length; ++i) {
        largeBuffers[i] = new LlapMetadataBuffer<Object>(fileKey, tag);
      }
      allocator.allocateMultiple(largeBuffers, maxAlloc, null, isStopped);
      for (int i = 0; i < largeBuffers.length; ++i) {
        readIntoCacheBuffer(stream, maxAlloc, largeBuffers[i]);
      }
    }
    int smallSize = length % maxAlloc;
    if (smallSize == 0) {
      return new LlapMetadataBuffers(largeBuffers);
    } else {
      LlapMetadataBuffer<Object>[] smallBuffer = new LlapMetadataBuffer[1];
      smallBuffer[0] = new LlapMetadataBuffer(fileKey, tag);
      allocator.allocateMultiple(smallBuffer, length, null, isStopped);
      readIntoCacheBuffer(stream, smallSize, smallBuffer[0]);
      if (largeBuffers == null) {
        return smallBuffer[0]; // This is the overwhelmingly common case.
      } else {
        LlapMetadataBuffer<Object>[] cacheData = new LlapMetadataBuffer[largeBuffers.length + 1];
        System.arraycopy(largeBuffers, 0, cacheData, 0, largeBuffers.length);
        cacheData[largeBuffers.length] = smallBuffer[0];
        return new LlapMetadataBuffers<Object>(cacheData);
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

  private <T> LlapBufferOrBuffers putInternal(T key, ByteBuffer tailBuffer, String tag, AtomicBoolean isStopped) {
    LlapBufferOrBuffers result = null;
    while (true) { // Overwhelmingly executes once, or maybe twice (replacing stale value).
      LlapBufferOrBuffers oldVal = metadata.get(key);
      if (oldVal == null) {
        result = wrapBb(result, key, tailBuffer, tag, isStopped);
        oldVal = metadata.putIfAbsent(key, result);
        if (oldVal == null) {
          cacheInPolicy(result); // Cached successfully, add to policy.
          return result;
        }
      }
      if (lockOldVal(key, result, oldVal)) {
        return oldVal;
      }
      // We found some old value but couldn't incRef it; remove it.
      metadata.remove(key, oldVal);
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

  private <T> LlapBufferOrBuffers wrapBb(
      LlapBufferOrBuffers result, T key, ByteBuffer tailBuffer, String tag, AtomicBoolean isStopped) {
    if (result != null) return result;
    if (tailBuffer.remaining() <= allocator.getMaxAllocation()) {
      // The common case by far.
      return wrapSmallBb(new LlapMetadataBuffer<T>(key, tag), tailBuffer, isStopped);
    } else {
      int allocCount = determineAllocCount(tailBuffer);
      @SuppressWarnings("unchecked")
      LlapMetadataBuffer<T>[] results = new LlapMetadataBuffer[allocCount];
      for (int i = 0; i < allocCount; ++i) {
        results[i] = new LlapMetadataBuffer<T>(key, tag);
      }
      wrapLargeBb(results, tailBuffer, isStopped);
      return new LlapMetadataBuffers<T>(results);
    }
  }

  private <T extends LlapAllocatorBuffer> T wrapSmallBb(T result, ByteBuffer tailBuffer,
      AtomicBoolean isStopped) {
    // Note: we pass in null factory because we allocate objects here. We could also pass a
    //       per-call factory that would set fileKey; or set it after put.
    allocator.allocateMultiple(new MemoryBuffer[] { result }, tailBuffer.remaining(), null, isStopped);
    return putBufferToDest(tailBuffer.duplicate(), result);
  }

  private <T extends LlapAllocatorBuffer> void wrapLargeBb(T[] results, ByteBuffer tailBuffer,
      AtomicBoolean isStopped) {
    // Note: we pass in null factory because we allocate objects here. We could also pass a
    //       per-call factory that would set fileKey; or set it after put.
    allocator.allocateMultiple(results, allocator.getMaxAllocation(), null, isStopped);
    ByteBuffer src = tailBuffer.duplicate();
    int pos = src.position(), remaining = src.remaining();
    for (int i = 0; i < results.length; ++i) {
      T result = results[i];
      int toPut = Math.min(remaining, result.getByteBufferRaw().remaining());
      assert toPut > 0;
      src.position(pos);
      src.limit(pos + toPut);
      pos += toPut;
      remaining -= toPut;
      putBufferToDest(src, result);
    }
  }

  private <T extends LlapAllocatorBuffer> T putBufferToDest(ByteBuffer src, T result) {
    ByteBuffer dest = result.getByteBufferRaw();
    int startPos = dest.position();
    dest.put(src);
    int newPos = dest.position();
    dest.position(startPos);
    dest.limit(newPos);
    boolean canLock = lockOneBuffer(result, false);
    assert canLock;
    return result;
  }

  public int determineAllocCount(ByteBuffer tailBuffer) {
    int total = tailBuffer.remaining(), maxAlloc = allocator.getMaxAllocation();
    return total / maxAlloc + ((total % maxAlloc) > 0 ? 1 : 0);
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

  public boolean lockOneBuffer(LlapAllocatorBuffer buffer, boolean doNotifyPolicy) {
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

  private final static class StripeKey {
    private final Object fileKey;
    private final int stripeIx;

    public StripeKey(Object fileKey, int stripeIx) {
      this.fileKey = fileKey;
      this.stripeIx = stripeIx;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      return (prime + ((fileKey == null) ? 0 : fileKey.hashCode())) * prime + stripeIx;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof StripeKey)) return false;
      StripeKey other = (StripeKey)obj;
      return ((fileKey == null) == (other.fileKey == null))
          && (fileKey == null || fileKey.equals(other.fileKey)) && (stripeIx == other.stripeIx);
    }
  }

  public static interface LlapBufferOrBuffers extends MemoryBufferOrBuffers {
    LlapAllocatorBuffer getSingleLlapBuffer();
    LlapAllocatorBuffer[] getMultipleLlapBuffers();
  }


  public final static class LlapMetadataBuffer<T>
      extends LlapAllocatorBuffer implements LlapBufferOrBuffers {
    private final T key;
    private String tag;

    public LlapMetadataBuffer(T key, String tag) {
      this.key = key;
      this.tag = tag;
    }

    @Override
    public void notifyEvicted(EvictionDispatcher evictionDispatcher) {
      evictionDispatcher.notifyEvicted(this);
    }

    public T getKey() {
      return key;
    }

    @Override
    public LlapAllocatorBuffer getSingleBuffer() {
      return this;
    }

    @Override
    public LlapAllocatorBuffer[] getMultipleBuffers() {
      return null;
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
    public String getTag() {
      return tag;
    }
  }

  public final static class LlapMetadataBuffers<T> implements LlapBufferOrBuffers {
    private final LlapMetadataBuffer<T>[] buffers;

    public LlapMetadataBuffers(LlapMetadataBuffer<T>[] buffers) {
      this.buffers = buffers;
    }

    @Override
    public LlapAllocatorBuffer getSingleBuffer() {
      return null;
    }

    @Override
    public LlapAllocatorBuffer[] getMultipleBuffers() {
      return buffers;
    }

    @Override
    public LlapAllocatorBuffer getSingleLlapBuffer() {
      return null;
    }

    @Override
    public LlapAllocatorBuffer[] getMultipleLlapBuffers() {
      return buffers;
    }
  }
}

