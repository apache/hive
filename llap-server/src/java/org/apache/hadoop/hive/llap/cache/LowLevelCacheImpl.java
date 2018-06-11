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
package org.apache.hadoop.hive.llap.cache;

import org.apache.orc.impl.RecordReaderUtils;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.common.io.Allocator;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.DataCache.BooleanRef;
import org.apache.hadoop.hive.common.io.DataCache.DiskRangeListFactory;
import org.apache.hadoop.hive.common.io.DiskRangeList.MutateHelper;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.apache.hive.common.util.Ref;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;

public class LowLevelCacheImpl implements LowLevelCache, BufferUsageManager, LlapIoDebugDump {
  private static final int DEFAULT_CLEANUP_INTERVAL = 600;
  private final Allocator allocator;
  private final AtomicInteger newEvictions = new AtomicInteger(0);
  private Thread cleanupThread = null;
  // TODO: given the specific data and lookups, perhaps the nested thing should not be a map
  //       In fact, CSLM has slow single-threaded operation, and one file is probably often read
  //       by just one (or few) threads, so a much more simple DS with locking might be better.
  //       Let's use CSLM for now, since it's available.
  private final ConcurrentHashMap<Object,
      FileCache<ConcurrentSkipListMap<Long, LlapDataBuffer>>> cache = new ConcurrentHashMap<>();
  private final LowLevelCachePolicy cachePolicy;
  private final long cleanupInterval;
  private final LlapDaemonCacheMetrics metrics;
  private final boolean doAssumeGranularBlocks;

  private static final Function<Void, ConcurrentSkipListMap<Long, LlapDataBuffer>> CACHE_CTOR =
      new Function<Void, ConcurrentSkipListMap<Long, LlapDataBuffer>>() {
        @Override
        public ConcurrentSkipListMap<Long, LlapDataBuffer> apply(Void input) {
          return new ConcurrentSkipListMap<>();
        }
      };

  public LowLevelCacheImpl(LlapDaemonCacheMetrics metrics, LowLevelCachePolicy cachePolicy,
      Allocator allocator, boolean doAssumeGranularBlocks) {
    this(metrics, cachePolicy, allocator, doAssumeGranularBlocks, DEFAULT_CLEANUP_INTERVAL);
  }

  @VisibleForTesting
  LowLevelCacheImpl(LlapDaemonCacheMetrics metrics, LowLevelCachePolicy cachePolicy,
      Allocator allocator, boolean doAssumeGranularBlocks, long cleanupInterval) {
    LlapIoImpl.LOG.info("Low level cache; cleanup interval {} sec", cleanupInterval);
    this.cachePolicy = cachePolicy;
    this.allocator = allocator;
    this.cleanupInterval = cleanupInterval;
    this.metrics = metrics;
    this.doAssumeGranularBlocks = doAssumeGranularBlocks;
  }

  public void startThreads() {
    if (cleanupInterval < 0) return;
    cleanupThread = new CleanupThread(cache, newEvictions, cleanupInterval);
    cleanupThread.start();
  }

  @Override
  public DiskRangeList getFileData(Object fileKey, DiskRangeList ranges, long baseOffset,
      DiskRangeListFactory factory, LowLevelCacheCounters qfCounters, BooleanRef gotAllData) {
    if (ranges == null) return null;
    DiskRangeList prev = ranges.prev;
    FileCache<ConcurrentSkipListMap<Long, LlapDataBuffer>> subCache = cache.get(fileKey);
    if (subCache == null || !subCache.incRef()) {
      long totalMissed = ranges.getTotalLength();
      metrics.incrCacheRequestedBytes(totalMissed);
      if (qfCounters != null) {
        qfCounters.recordCacheMiss(totalMissed);
      }
      if (prev != null && gotAllData != null) {
        gotAllData.value = false;
      }
      return ranges;
    }
    try {
      if (prev == null) {
        prev = new MutateHelper(ranges);
      }
      if (gotAllData != null) {
        gotAllData.value = true;
      }
      DiskRangeList current = ranges;
      while (current != null) {
        metrics.incrCacheRequestedBytes(current.getLength());
        // We assume ranges in "ranges" are non-overlapping; thus, we will save next in advance.
        DiskRangeList next = current.next;
        getOverlappingRanges(baseOffset, current, subCache.getCache(), factory, gotAllData);
        current = next;
      }
    } finally {
      subCache.decRef();
    }
    boolean isInvalid = false;
    if (qfCounters != null) {
      DiskRangeList current = prev.next;
      long bytesHit = 0, bytesMissed = 0;
      while (current != null) {
        // This assumes no ranges passed to cache to fetch have data beforehand.
        if (current.hasData()) {
          bytesHit += current.getLength();
        } else {
          if (gotAllData.value) {
            isInvalid = true;
          }
          bytesMissed += current.getLength();
        }
        current = current.next;
      }
      qfCounters.recordCacheHit(bytesHit);
      qfCounters.recordCacheMiss(bytesMissed);
    } else if (gotAllData != null && gotAllData.value) {
      DiskRangeList current = prev.next;
      while (current != null) {
        if (!current.hasData()) {
          isInvalid = true;
          break;
        }
        current = current.next;
      }
    }
    if (isInvalid) {
      StringBuilder invalidMsg = new StringBuilder(
          "Internal error - gotAllData=true but the resulting ranges are ").append(
              RecordReaderUtils.stringifyDiskRanges(prev.next));
      subCache = cache.get(fileKey);
      if (subCache != null && subCache.incRef()) {
        try {
          invalidMsg.append("; cache ranges (not necessarily consistent) are ");
          for (Map.Entry<Long, LlapDataBuffer> e : subCache.getCache().entrySet()) {
            long start = e.getKey(), end = start + e.getValue().declaredCachedLength;
            invalidMsg.append("[").append(start).append(", ").append(end).append("), ");
          }
        } finally {
          subCache.decRef();
        }
      } else {
        invalidMsg.append("; cache ranges can no longer be determined");
      }
      String s = invalidMsg.toString();
      LlapIoImpl.LOG.error(s);
      throw new RuntimeException(s);
    }
    return prev.next;
  }

  private void getOverlappingRanges(long baseOffset, DiskRangeList currentNotCached,
      ConcurrentSkipListMap<Long, LlapDataBuffer> cache, DiskRangeListFactory factory,
      BooleanRef gotAllData) {
    long absOffset = currentNotCached.getOffset() + baseOffset;
    if (!doAssumeGranularBlocks) {
      // This currently only happens in tests. See getFileData comment on the interface.
      Long prevOffset = cache.floorKey(absOffset);
      if (prevOffset != null) {
        absOffset = prevOffset;
      }
    }
    Iterator<Map.Entry<Long, LlapDataBuffer>> matches = cache.subMap(
        absOffset, currentNotCached.getEnd() + baseOffset)
        .entrySet().iterator();
    long cacheEnd = -1;
    while (matches.hasNext()) {
      assert currentNotCached != null;
      Map.Entry<Long, LlapDataBuffer> e = matches.next();
      LlapDataBuffer buffer = e.getValue();
      long requestedLength = currentNotCached.getLength();
      // Lock the buffer, validate it and add to results.
      if (LlapIoImpl.LOCKING_LOGGER.isTraceEnabled()) {
        LlapIoImpl.LOCKING_LOGGER.trace("Locking {} during get", buffer);
      }

      if (!lockBuffer(buffer, true)) {
        // If we cannot lock, remove this from cache and continue.
        matches.remove();
        if (gotAllData != null) {
          gotAllData.value = false;
        }
        continue;
      }
      long cacheOffset = e.getKey();
      if (cacheEnd > cacheOffset) { // compare with old cacheEnd
        throw new AssertionError("Cache has overlapping buffers: " + cacheEnd + ") and ["
            + cacheOffset + ", " + (cacheOffset + buffer.declaredCachedLength) + ")");
      }
      cacheEnd = cacheOffset + buffer.declaredCachedLength;
      DiskRangeList currentCached = factory.createCacheChunk(buffer,
          cacheOffset - baseOffset, cacheEnd - baseOffset);
      currentNotCached = addCachedBufferToIter(currentNotCached, currentCached, gotAllData);
      metrics.incrCacheHitBytes(Math.min(requestedLength, currentCached.getLength()));
    }
    if (currentNotCached != null) {
      assert !currentNotCached.hasData(); // Assumes no ranges passed to cache to read have data.
      if (gotAllData != null) {
        gotAllData.value = false;
      }
    }
  }

  /**
   * Adds cached buffer to buffer list.
   * @param currentNotCached Pointer to the list node where we are inserting.
   * @param currentCached The cached buffer found for this node, to insert.
   * @return The new currentNotCached pointer, following the cached buffer insertion.
   */
  private DiskRangeList addCachedBufferToIter(
      DiskRangeList currentNotCached, DiskRangeList currentCached, BooleanRef gotAllData) {
    if (currentNotCached.getOffset() >= currentCached.getOffset()) {
      // Cached buffer has the same (or lower) offset as the requested buffer.
      if (currentNotCached.getEnd() <= currentCached.getEnd()) {
        // Replace the entire current DiskRange with new cached range.
        // In case of an inexact match in either of the below it may throw. We do not currently
        // support the case where the caller requests a single cache buffer via multiple smaller
        // sub-ranges; if that happens, this may throw. Noone does it now, though.
        // TODO: should we actively assert here for cache buffers larger than range?
        currentNotCached.replaceSelfWith(currentCached);
        return null;
      } else {
        // This cache range is a prefix of the requested one; the above also applies.
        // The cache may still contain the rest of the requested range, so don't set gotAllData.
        currentNotCached.insertPartBefore(currentCached);
        return currentNotCached;
      }
    }

    // Some part of the requested range is not cached - the cached offset is past the requested.
    if (gotAllData != null) {
      gotAllData.value = false;
    }
    if (currentNotCached.getEnd() <= currentCached.getEnd()) {
      // The cache buffer comprises the tail of the requested range (and possibly overshoots it).
      // The same as above applies - may throw if cache buffer is larger than the requested range,
      // and there's another range after this that starts in the middle of this cache buffer.
      // Currently, we cache at exact offsets, so the latter should never happen.
      currentNotCached.insertPartAfter(currentCached);
      return null;  // No more matches expected.
    } else {
      // The cached buffer is in the middle of the requested range.
      // The remaining tail of the latter may still be available further.
      DiskRangeList tail = new DiskRangeList(currentCached.getEnd(), currentNotCached.getEnd());
      currentNotCached.insertPartAfter(currentCached);
      currentCached.insertAfter(tail);
      return tail;
    }
  }

  private boolean lockBuffer(LlapDataBuffer buffer, boolean doNotifyPolicy) {
    int rc = buffer.incRef();
    if (rc > 0) {
      metrics.incrCacheNumLockedBuffers();
    }
    if (doNotifyPolicy && rc == 1) {
      // We have just locked a buffer that wasn't previously locked.
      cachePolicy.notifyLock(buffer);
    }
    return rc > 0;
  }

  @Override
  public long[] putFileData(Object fileKey, DiskRange[] ranges, MemoryBuffer[] buffers,
      long baseOffset, Priority priority, LowLevelCacheCounters qfCounters, String tag) {
    long[] result = null;
    assert buffers.length == ranges.length;
    FileCache<ConcurrentSkipListMap<Long, LlapDataBuffer>> subCache =
        FileCache.getOrAddFileSubCache(cache, fileKey, CACHE_CTOR);
    try {
      for (int i = 0; i < ranges.length; ++i) {
        LlapDataBuffer buffer = (LlapDataBuffer)buffers[i];
        if (LlapIoImpl.LOCKING_LOGGER.isTraceEnabled()) {
          LlapIoImpl.LOCKING_LOGGER.trace("Locking {} at put time", buffer);
        }
        boolean canLock = lockBuffer(buffer, false);
        assert canLock;
        long offset = ranges[i].getOffset() + baseOffset;
        assert buffer.declaredCachedLength == LlapDataBuffer.UNKNOWN_CACHED_LENGTH;
        buffer.declaredCachedLength = ranges[i].getLength();
        buffer.setTag(tag);
        while (true) { // Overwhelmingly executes once, or maybe twice (replacing stale value).
          LlapDataBuffer oldVal = subCache.getCache().putIfAbsent(offset, buffer);
          if (oldVal == null) {
            // Cached successfully, add to policy.
            cachePolicy.cache(buffer, priority);
            if (qfCounters != null) {
              qfCounters.recordAllocBytes(buffer.byteBuffer.remaining(), buffer.allocSize);
            }
            break;
          }
          if (LlapIoImpl.CACHE_LOGGER.isTraceEnabled()) {
            LlapIoImpl.CACHE_LOGGER.trace("Trying to cache when the chunk is already cached for" +
                " {}@{} (base {}); old {}, new {}", fileKey, offset, baseOffset, oldVal, buffer);
          }

          if (LlapIoImpl.LOCKING_LOGGER.isTraceEnabled()) {
            LlapIoImpl.LOCKING_LOGGER.trace("Locking {} due to cache collision", oldVal);
          }
          if (lockBuffer(oldVal, true)) {
            // We don't do proper overlap checking because it would cost cycles and we
            // think it will never happen. We do perform the most basic check here.
            if (oldVal.declaredCachedLength != buffer.declaredCachedLength) {
              throw new RuntimeException("Found a block with different length at the same offset: "
                  + oldVal.declaredCachedLength + " vs " + buffer.declaredCachedLength + " @" + offset
                  + " (base " + baseOffset + ")");
            }
            // We found an old, valid block for this key in the cache.
            if (LlapIoImpl.LOCKING_LOGGER.isTraceEnabled()) {
              LlapIoImpl.LOCKING_LOGGER.trace("Unlocking {} due to cache collision with {}",
                  buffer, oldVal);
            }

            unlockBuffer(buffer, false);
            buffers[i] = oldVal;
            if (result == null) {
              result = new long[align64(buffers.length) >>> 6];
            }
            result[i >>> 6] |= (1 << (i & 63)); // indicate that we've replaced the value
            break;
          }
          // We found some old value but couldn't incRef it; remove it.
          subCache.getCache().remove(offset, oldVal);
        }
      }
    } finally {
      subCache.decRef();
    }
    return result;
  }

  private static int align64(int number) {
    return ((number + 63) & ~63);
  }

  @Override
  public void decRefBuffer(MemoryBuffer buffer) {
    unlockBuffer((LlapDataBuffer)buffer, true);
  }

  @Override
  public void decRefBuffers(List<MemoryBuffer> cacheBuffers) {
    for (MemoryBuffer b : cacheBuffers) {
      unlockBuffer((LlapDataBuffer)b, true);
    }
  }

  private void unlockBuffer(LlapDataBuffer buffer, boolean handleLastDecRef) {
    boolean isLastDecref = (buffer.decRef() == 0);
    if (handleLastDecRef && isLastDecref) {
      // This is kind of not pretty, but this is how we detect whether buffer was cached.
      // We would always set this for lookups at put time.
      if (buffer.declaredCachedLength != LlapDataBuffer.UNKNOWN_CACHED_LENGTH) {
        cachePolicy.notifyUnlock(buffer);
      } else {
        if (LlapIoImpl.CACHE_LOGGER.isTraceEnabled()) {
          LlapIoImpl.CACHE_LOGGER.trace("Deallocating {} that was not cached", buffer);
        }
        allocator.deallocate(buffer);
      }
    }
    metrics.decrCacheNumLockedBuffers();
  }

  private static final ByteBuffer fakeBuf = ByteBuffer.wrap(new byte[1]);
  public static LlapDataBuffer allocateFake() {
    LlapDataBuffer fake = new LlapDataBuffer();
    fake.initialize(fakeBuf, 0, 1);
    return fake;
  }

  @Override
  public final void notifyEvicted(MemoryBuffer buffer) {
    newEvictions.incrementAndGet();
  }

  private static final class CleanupThread
    extends FileCacheCleanupThread<ConcurrentSkipListMap<Long, LlapDataBuffer>> {

    public CleanupThread(ConcurrentHashMap<Object,
        FileCache<ConcurrentSkipListMap<Long, LlapDataBuffer>>> fileMap,
        AtomicInteger newEvictions, long cleanupInterval) {
      super("Llap low level cache cleanup thread", fileMap, newEvictions, cleanupInterval);
    }

    @Override
    protected int getCacheSize( FileCache<ConcurrentSkipListMap<Long, LlapDataBuffer>> fc) {
      return fc.getCache().size();
    }

    @Override
    public int cleanUpOneFileCache(
        FileCache<ConcurrentSkipListMap<Long, LlapDataBuffer>> fc,
        int leftToCheck, long endTime, Ref<Boolean> isPastEndTime)
        throws InterruptedException {
      // Iterate thru the file cache. This is best-effort.
      Iterator<Map.Entry<Long, LlapDataBuffer>> subIter = fc.getCache().entrySet().iterator();
      while (subIter.hasNext()) {
        long time = -1;
        isPastEndTime.value = isPastEndTime.value || ((time = System.nanoTime()) >= endTime);
        Thread.sleep(((leftToCheck <= 0) || isPastEndTime.value)
            ? 1 : (endTime - time) / (1000000L * leftToCheck));
        if (subIter.next().getValue().isInvalid()) {
          subIter.remove();
        }
        --leftToCheck;
      }
      return leftToCheck;
    }
  }

  @Override
  public boolean incRefBuffer(MemoryBuffer buffer) {
    // notifyReused implies that buffer is already locked; it's also called once for new
    // buffers that are not cached yet. Don't notify cache policy.
    return lockBuffer(((LlapDataBuffer)buffer), false);
  }

  @Override
  public Allocator getAllocator() {
    return allocator;
  }

  @Override
  public void debugDumpShort(StringBuilder sb) {
    sb.append("\nORC cache state ");
    int allLocked = 0, allUnlocked = 0, allEvicted = 0, allMoving = 0;
    for (Map.Entry<Object, FileCache<ConcurrentSkipListMap<Long, LlapDataBuffer>>> e :
      cache.entrySet()) {
      if (!e.getValue().incRef()) continue;
      try {
        int fileLocked = 0, fileUnlocked = 0, fileEvicted = 0, fileMoving = 0;
        if (e.getValue().getCache().isEmpty()) continue;
        for (Map.Entry<Long, LlapDataBuffer> e2 : e.getValue().getCache().entrySet()) {
          int newRc = e2.getValue().tryIncRef();
          if (newRc < 0) {
            if (newRc == LlapAllocatorBuffer.INCREF_EVICTED) {
              ++fileEvicted;
            } else if (newRc == LlapAllocatorBuffer.INCREF_FAILED) {
              ++fileMoving;
            }
            continue;
          }
          try {
            if (newRc > 1) { // We hold one refcount.
              ++fileLocked;
            } else {
              ++fileUnlocked;
            }
          } finally {
            e2.getValue().decRef();
          }
        }
        allLocked += fileLocked;
        allUnlocked += fileUnlocked;
        allEvicted += fileEvicted;
        allMoving += fileMoving;
        sb.append("\n  file " + e.getKey() + ": " + fileLocked + " locked, " + fileUnlocked
            + " unlocked, " + fileEvicted + " evicted, " + fileMoving + " being moved");
      } finally {
        e.getValue().decRef();
      }
    }
    sb.append("\nORC cache summary: " + allLocked + " locked, " + allUnlocked + " unlocked, "
        + allEvicted + " evicted, " + allMoving + " being moved");
  }
}
