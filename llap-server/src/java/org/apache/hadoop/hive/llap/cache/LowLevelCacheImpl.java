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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.common.DiskRange;
import org.apache.hadoop.hive.common.DiskRangeList;
import org.apache.hadoop.hive.common.DiskRangeList.DiskRangeListMutateHelper;
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.io.api.cache.LlapMemoryBuffer;
import org.apache.hadoop.hive.llap.io.api.cache.LowLevelCache;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;

import com.google.common.annotations.VisibleForTesting;

public class LowLevelCacheImpl implements LowLevelCache, EvictionListener {
  private static final int DEFAULT_CLEANUP_INTERVAL = 600;
  private final Allocator allocator;
  private AtomicInteger newEvictions = new AtomicInteger(0);
  private Thread cleanupThread = null;
  private final ConcurrentHashMap<Long, FileCache> cache =
      new ConcurrentHashMap<Long, FileCache>();
  private final LowLevelCachePolicy cachePolicy;
  private final long cleanupInterval;
  private LlapDaemonCacheMetrics metrics;
  private final boolean doAssumeGranularBlocks;

  public LowLevelCacheImpl(LlapDaemonCacheMetrics metrics, LowLevelCachePolicy cachePolicy,
      Allocator allocator, boolean doAssumeGranularBlocks) {
    this(metrics, cachePolicy, allocator, doAssumeGranularBlocks, DEFAULT_CLEANUP_INTERVAL);
  }

  @VisibleForTesting
  LowLevelCacheImpl(LlapDaemonCacheMetrics metrics, LowLevelCachePolicy cachePolicy,
      Allocator allocator, boolean doAssumeGranularBlocks, long cleanupInterval) {
    if (LlapIoImpl.LOGL.isInfoEnabled()) {
      LlapIoImpl.LOG.info("Low level cache; cleanup interval " + cleanupInterval + "sec");
    }
    this.cachePolicy = cachePolicy;
    this.allocator = allocator;
    this.cleanupInterval = cleanupInterval;
    this.metrics = metrics;
    this.doAssumeGranularBlocks = doAssumeGranularBlocks;
  }

  public void init() {
    if (cleanupInterval < 0) return;
    cleanupThread = new CleanupThread(cleanupInterval);
    cleanupThread.start();
  }

  @Override
  public void allocateMultiple(LlapMemoryBuffer[] dest, int size) {
    allocator.allocateMultiple(dest, size);
  }

  @Override
  public DiskRangeList getFileData(
      long fileId, DiskRangeList ranges, long baseOffset, CacheChunkFactory factory) {
    if (ranges == null) return null;
    FileCache subCache = cache.get(fileId);
    if (subCache == null || !subCache.incRef()) {
      metrics.incrCacheRequestedBytes(ranges.getTotalLength());
      return ranges;
    }
    try {
      DiskRangeList prev = ranges.prev;
      if (prev == null) {
        prev = new DiskRangeListMutateHelper(ranges);
      }
      DiskRangeList current = ranges;
      while (current != null) {
        metrics.incrCacheRequestedBytes(current.getLength());
        // We assume ranges in "ranges" are non-overlapping; thus, we will save next in advance.
        DiskRangeList next = current.next;
        getOverlappingRanges(baseOffset, current, subCache.cache, factory);
        current = next;
      }
      return prev.next;
    } finally {
      subCache.decRef();
    }
  }

  private void getOverlappingRanges(long baseOffset, DiskRangeList currentNotCached,
      ConcurrentSkipListMap<Long, LlapDataBuffer> cache, CacheChunkFactory factory) {
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
      if (DebugUtils.isTraceLockingEnabled()) {
        LlapIoImpl.LOG.info("Locking " + buffer + " during get");
      }

      if (!lockBuffer(buffer, true)) {
        // If we cannot lock, remove this from cache and continue.
        matches.remove();
        continue;
      }
      long cacheOffset = e.getKey();
      if (cacheEnd > cacheOffset) { // compare with old cacheEnd
        throw new AssertionError("Cache has overlapping buffers: " + cacheEnd + ") and ["
            + cacheOffset + ", " + (cacheOffset + buffer.declaredLength) + ")");
      }
      cacheEnd = cacheOffset + buffer.declaredLength;
      DiskRangeList currentCached = factory.createCacheChunk(buffer,
          cacheOffset - baseOffset, cacheEnd - baseOffset);
      currentNotCached = addCachedBufferToIter(currentNotCached, currentCached);
      metrics.incrCacheHitBytes(Math.min(requestedLength, currentCached.getLength()));
    }
  }

  /**
   * Adds cached buffer to buffer list.
   * @param currentNotCached Pointer to the list node where we are inserting.
   * @param currentCached The cached buffer found for this node, to insert.
   * @return The new currentNotCached pointer, following the cached buffer insertion.
   */
  private DiskRangeList addCachedBufferToIter(
      DiskRangeList currentNotCached, DiskRangeList currentCached) {
    if (currentNotCached.getOffset() >= currentCached.getOffset()) {
      if (currentNotCached.getEnd() <= currentCached.getEnd()) {  // we assume it's always "==" now
        // Replace the entire current DiskRange with new cached range.
        currentNotCached.replaceSelfWith(currentCached);
        return null;
      } else {
        // Insert the new cache range before the disk range.
        currentNotCached.insertPartBefore(currentCached);
        return currentNotCached;
      }
    } else {
      assert currentNotCached.getOffset() < currentCached.getOffset()
        || currentNotCached.prev == null
        || currentNotCached.prev.getEnd() <= currentCached.getOffset();
      long endOffset = currentNotCached.getEnd();
      currentNotCached.insertPartAfter(currentCached);
      if (endOffset <= currentCached.getEnd()) { // we assume it's always "==" now
        return null;  // No more matches expected...
      } else {
        // Insert the new disk range after the cache range. TODO: not strictly necessary yet?
        currentNotCached = new DiskRangeList(currentCached.getEnd(), endOffset);
        currentCached.insertAfter(currentNotCached);
        return currentNotCached;
      }
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
  public long[] putFileData(long fileId, DiskRange[] ranges,
      LlapMemoryBuffer[] buffers, long baseOffset, Priority priority) {
    long[] result = null;
    assert buffers.length == ranges.length;
    FileCache subCache = getOrAddFileSubCache(fileId);
    try {
      for (int i = 0; i < ranges.length; ++i) {
        LlapDataBuffer buffer = (LlapDataBuffer)buffers[i];
        if (DebugUtils.isTraceLockingEnabled()) {
          LlapIoImpl.LOG.info("Locking " + buffer + " at put time");
        }
        boolean canLock = lockBuffer(buffer, false);
        assert canLock;
        long offset = ranges[i].getOffset() + baseOffset;
        buffer.declaredLength = ranges[i].getLength();
        while (true) { // Overwhelmingly executes once, or maybe twice (replacing stale value).
          LlapDataBuffer oldVal = subCache.cache.putIfAbsent(offset, buffer);
          if (oldVal == null) {
            // Cached successfully, add to policy.
            cachePolicy.cache(buffer, priority);
            break;
          }
          if (DebugUtils.isTraceCachingEnabled()) {
            LlapIoImpl.LOG.info("Trying to cache when the chunk is already cached for "
                + fileId + "@" + offset  + " (base " + baseOffset + "); old " + oldVal
                + ", new " + buffer);
          }
          if (DebugUtils.isTraceLockingEnabled()) {
            LlapIoImpl.LOG.info("Locking " + oldVal + "  due to cache collision");
          }
          if (lockBuffer(oldVal, true)) {
            // We don't do proper overlap checking because it would cost cycles and we
            // think it will never happen. We do perform the most basic check here.
            if (oldVal.declaredLength != buffer.declaredLength) {
              throw new RuntimeException("Found a block with different length at the same offset: "
                  + oldVal.declaredLength + " vs " + buffer.declaredLength + " @" + offset
                  + " (base " + baseOffset + ")");
            }
            // We found an old, valid block for this key in the cache.
            if (DebugUtils.isTraceLockingEnabled()) {
              LlapIoImpl.LOG.info("Unlocking " + buffer + " due to cache collision with " + oldVal);
            }

            unlockBuffer(buffer);
            buffers[i] = oldVal;
            if (result == null) {
              result = new long[align64(buffers.length) >>> 6];
            }
            result[i >>> 6] |= (1 << (i & 63)); // indicate that we've replaced the value
            break;
          }
          // We found some old value but couldn't incRef it; remove it.
          subCache.cache.remove(offset, oldVal);
        }
      }
    } finally {
      subCache.decRef();
    }
    return result;
  }

  /**
   * All this mess is necessary because we want to be able to remove sub-caches for fully
   * evicted files. It may actually be better to have non-nested map with object keys?
   */
  private FileCache getOrAddFileSubCache(long fileId) {
    FileCache newSubCache = null;
    while (true) { // Overwhelmingly executes once.
      FileCache subCache = cache.get(fileId);
      if (subCache != null) {
        if (subCache.incRef()) return subCache; // Main path - found it, incRef-ed it.
        if (newSubCache == null) {
          newSubCache = new FileCache();
          newSubCache.incRef();
        }
        // Found a stale value we cannot incRef; try to replace it with new value.
        if (cache.replace(fileId, subCache, newSubCache)) return newSubCache;
        continue; // Someone else replaced/removed a stale value, try again.
      }
      // No value found.
      if (newSubCache == null) {
        newSubCache = new FileCache();
        newSubCache.incRef();
      }
      FileCache oldSubCache = cache.putIfAbsent(fileId, newSubCache);
      if (oldSubCache == null) return newSubCache; // Main path 2 - created a new file cache.
      if (oldSubCache.incRef()) return oldSubCache; // Someone created one in parallel.
      // Someone created one in parallel and then it went stale.
      if (cache.replace(fileId, oldSubCache, newSubCache)) return newSubCache;
      // Someone else replaced/removed a parallel-added stale value, try again. Max confusion.
    }
  }

  private static int align64(int number) {
    return ((number + 63) & ~63);
  }

  @Override
  public void releaseBuffer(LlapMemoryBuffer buffer) {
    unlockBuffer((LlapDataBuffer)buffer);
  }

  @Override
  public void releaseBuffers(List<LlapMemoryBuffer> cacheBuffers) {
    for (LlapMemoryBuffer b : cacheBuffers) {
      unlockBuffer((LlapDataBuffer)b);
    }
  }

  public void unlockBuffer(LlapDataBuffer buffer) {
    if (buffer.decRef() == 0) {
      cachePolicy.notifyUnlock(buffer);
    }
    metrics.decrCacheNumLockedBuffers();
  }

  private static final ByteBuffer fakeBuf = ByteBuffer.wrap(new byte[1]);
  public static LlapDataBuffer allocateFake() {
    LlapDataBuffer fake = new LlapDataBuffer();
    fake.initialize(-1, fakeBuf, 0, 1);
    return fake;
  }

  @Override
  public final void notifyEvicted(LlapCacheableBuffer buffer) {
    allocator.deallocate((LlapDataBuffer)buffer);
    newEvictions.incrementAndGet();
  }

  private static class FileCache {
    private static final int EVICTED_REFCOUNT = -1, EVICTING_REFCOUNT = -2;
    // TODO: given the specific data and lookups, perhaps the nested thing should not be a map
    //       In fact, CSLM has slow single-threaded operation, and one file is probably often read
    //       by just one (or few) threads, so a much more simple DS with locking might be better.
    //       Let's use CSLM for now, since it's available.
    private ConcurrentSkipListMap<Long, LlapDataBuffer> cache
      = new ConcurrentSkipListMap<Long, LlapDataBuffer>();
    private AtomicInteger refCount = new AtomicInteger(0);

    boolean incRef() {
      while (true) {
        int value = refCount.get();
        if (value == EVICTED_REFCOUNT) return false;
        if (value == EVICTING_REFCOUNT) continue; // spin until it resolves; extremely rare
        assert value >= 0;
        if (refCount.compareAndSet(value, value + 1)) return true;
      }
    }

    void decRef() {
      int value = refCount.decrementAndGet();
      if (value < 0) {
        throw new AssertionError("Unexpected refCount " + value);
      }
    }

    boolean startEvicting() {
      while (true) {
        int value = refCount.get();
        if (value != 1) return false;
        if (refCount.compareAndSet(value, EVICTING_REFCOUNT)) return true;
      }
    }

    void commitEvicting() {
      boolean result = refCount.compareAndSet(EVICTING_REFCOUNT, EVICTED_REFCOUNT);
      assert result;
    }

    void abortEvicting() {
      boolean result = refCount.compareAndSet(EVICTING_REFCOUNT, 0);
      assert result;
    }
  }

  private final class CleanupThread extends Thread {
    private final long approxCleanupIntervalSec;

    public CleanupThread(long cleanupInterval) {
      super("Llap low level cache cleanup thread");
      this.approxCleanupIntervalSec = cleanupInterval;
      setDaemon(true);
      setPriority(1);
    }

    @Override
    public void run() {
      while (true) {
        try {
          doOneCleanupRound();
        } catch (InterruptedException ex) {
          LlapIoImpl.LOG.warn("Cleanup thread has been interrupted");
          Thread.currentThread().interrupt();
          break;
        } catch (Throwable t) {
          LlapIoImpl.LOG.error("Cleanup has failed; the thread will now exit", t);
          break;
        }
      }
    }

    private void doOneCleanupRound() throws InterruptedException {
      while (true) {
        int evictionsSinceLast = newEvictions.getAndSet(0);
        if (evictionsSinceLast > 0) break;
        synchronized (newEvictions) {
          newEvictions.wait(10000);
        }
      }
      // Duration is an estimate; if the size of the map changes, it can be very different.
      long endTime = System.nanoTime() + approxCleanupIntervalSec * 1000000000L;
      int leftToCheck = 0; // approximate
      for (FileCache fc : cache.values()) {
        leftToCheck += fc.cache.size();
      }
      // Iterate thru all the filecaches. This is best-effort.
      // If these super-long-lived iterators affect the map in some bad way,
      // we'd need to sleep once per round instead.
      Iterator<Map.Entry<Long, FileCache>> iter = cache.entrySet().iterator();
      boolean isPastEndTime = false;
      while (iter.hasNext()) {
        FileCache fc = iter.next().getValue();
        if (!fc.incRef()) {
          throw new AssertionError("Something other than cleanup is removing elements from map");
        }
        // Iterate thru the file cache. This is best-effort.
        Iterator<Map.Entry<Long, LlapDataBuffer>> subIter = fc.cache.entrySet().iterator();
        boolean isEmpty = true;
        while (subIter.hasNext()) {
          long time = -1;
          isPastEndTime = isPastEndTime || ((time = System.nanoTime()) >= endTime);
          Thread.sleep(((leftToCheck <= 0) || isPastEndTime)
              ? 1 : (endTime - time) / (1000000L * leftToCheck));
          if (subIter.next().getValue().isInvalid()) {
            subIter.remove();
          } else {
            isEmpty = false;
          }
          --leftToCheck;
        }
        if (!isEmpty) {
          fc.decRef();
          continue;
        }
        // FileCache might be empty; see if we can remove it. "tryWriteLock"
        if (!fc.startEvicting()) continue;
        if (fc.cache.isEmpty()) {
          fc.commitEvicting();
          iter.remove();
        } else {
          fc.abortEvicting();
        }
      }
    }
  }

  @Override
  public LlapMemoryBuffer createUnallocated() {
    return new LlapDataBuffer();
  }

  @Override
  public boolean notifyReused(LlapMemoryBuffer buffer) {
    // notifyReused implies that buffer is already locked; it's also called once for new
    // buffers that are not cached yet. Don't notify cache policy.
    return lockBuffer(((LlapDataBuffer)buffer), false);
  }

  @Override
  public boolean isDirectAlloc() {
    return allocator.isDirectAlloc();
  }
}
