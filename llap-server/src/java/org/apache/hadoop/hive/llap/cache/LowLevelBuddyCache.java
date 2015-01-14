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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.io.api.cache.LlapMemoryBuffer;
import org.apache.hadoop.hive.llap.io.api.cache.LowLevelCache;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;

// TODO: refactor the cache and allocator parts?
public class LowLevelBuddyCache implements LowLevelCache, EvictionListener {
  private final ArrayList<Arena> arenas;
  private AtomicInteger newEvictions = new AtomicInteger(0);
  private final Thread cleanupThread;
  private final ConcurrentHashMap<String, FileCache> cache =
      new ConcurrentHashMap<String, FileCache>();
  private final LowLevelCachePolicy cachePolicy;

  // Config settings
  private final int minAllocLog2, maxAllocLog2, arenaSizeLog2, maxArenas;

  private final int minAllocation, maxAllocation, arenaSize;
  private final long maxSize;

  public LowLevelBuddyCache(Configuration conf) {
    minAllocation = HiveConf.getIntVar(conf, ConfVars.LLAP_ORC_CACHE_MIN_ALLOC);
    maxAllocation = HiveConf.getIntVar(conf, ConfVars.LLAP_ORC_CACHE_MAX_ALLOC);
    arenaSize = HiveConf.getIntVar(conf, ConfVars.LLAP_ORC_CACHE_ARENA_SIZE);
    maxSize = HiveConf.getLongVar(conf, ConfVars.LLAP_ORC_CACHE_MAX_SIZE);
    if (maxSize < arenaSize || arenaSize < maxAllocation || maxAllocation < minAllocation) {
      throw new AssertionError("Inconsistent sizes of cache, arena and allocations: "
          + minAllocation + ", " + maxAllocation + ", " + arenaSize + ", " + maxSize);
    }
    if ((Integer.bitCount(minAllocation) != 1) || (Integer.bitCount(maxAllocation) != 1)
        || (Long.bitCount(arenaSize) != 1) || (minAllocation == 1)) {
      // TODO: technically, arena size only needs to be divisible by maxAlloc
      throw new AssertionError("Allocation and arena sizes must be powers of two > 1: "
          + minAllocation + ", " + maxAllocation + ", " + arenaSize);
    }
    if ((maxSize % arenaSize) > 0 || (maxSize / arenaSize) > Integer.MAX_VALUE) {
      throw new AssertionError(
          "Cache size not consistent with arena size: " + arenaSize + "," + maxSize);
    }
    minAllocLog2 = 31 - Integer.numberOfLeadingZeros(minAllocation);
    maxAllocLog2 = 31 - Integer.numberOfLeadingZeros(maxAllocation);
    arenaSizeLog2 = 31 - Long.numberOfLeadingZeros(arenaSize);
    maxArenas = (int)(maxSize / arenaSize);
    arenas = new ArrayList<Arena>(maxArenas);
    for (int i = 0; i < maxArenas; ++i) {
      arenas.add(new Arena());
    }
    arenas.get(0).init(arenaSize, maxAllocation, arenaSizeLog2, minAllocLog2, maxAllocLog2);
    cachePolicy = HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_USE_LRFU)
        ? new LowLevelLrfuCachePolicy(conf, minAllocation, maxSize, this)
        : new LowLevelFifoCachePolicy(minAllocation, maxSize, this);
    cleanupThread = new CleanupThread();
    cleanupThread.start();
  }

  // TODO: would it make sense to return buffers asynchronously?
  @Override
  public void allocateMultiple(LlapMemoryBuffer[] dest, int size) {
    assert size > 0;
    int freeListIndex = 31 - Integer.numberOfLeadingZeros(size);
    if (size != (1 << freeListIndex)) ++freeListIndex; // not a power of two, add one more
    freeListIndex = Math.max(freeListIndex - minAllocLog2, 0);
    int allocationSize = 1 << (freeListIndex + minAllocLog2);
    int total = dest.length * allocationSize;
    cachePolicy.reserveMemory(total, true);

    int ix = 0;
    for (int i = 0; i < dest.length; ++i) {
      if (dest[i] != null) continue;
      dest[i] = new LlapCacheableBuffer(null, -1, -1); // TODO: pool of objects?
    }
    // TODO: instead of waiting, loop only ones we haven't tried w/tryLock?
    for (Arena block : arenas) {
      int newIx = allocateFast(block, freeListIndex, dest, ix, allocationSize);
      if (newIx == -1) break;
      if (newIx == dest.length) return;
      ix = newIx;
    }
    // Then try to split bigger blocks.
    for (Arena block : arenas) {
      int newIx = allocateWithSplit(block, freeListIndex, dest, ix, allocationSize);
      if (newIx == -1) break;
      if (newIx == dest.length) return;
      ix = newIx;
    }
    // Then try to allocate memory if we haven't allocated all the way to maxSize yet; very rare.
    for (Arena block : arenas) {
      ix = allocateWithExpand(block, freeListIndex, dest, ix, allocationSize);
      if (ix == dest.length) return;
    }
  }

  private int allocateFast(Arena block,
      int freeListIndex, LlapMemoryBuffer[] dest, int ix, int size) {
    if (block.data == null) return -1; // not allocated yet
    FreeList freeList = block.freeLists[freeListIndex];
    freeList.lock.lock();
    try {
      ix = allocateFromFreeListUnderLock(block, freeList, freeListIndex, dest, ix, size);
    } finally {
      freeList.lock.unlock();
    }
    return ix;
  }

  private int allocateWithSplit(
      Arena arena, int freeListIndex, LlapMemoryBuffer[] dest, int ix, int allocationSize) {
    if (arena.data == null) return -1; // not allocated yet
    FreeList freeList = arena.freeLists[freeListIndex];
    int remaining = -1;
    freeList.lock.lock();
    try {
      ix = allocateFromFreeListUnderLock(arena, freeList, freeListIndex, dest, ix, allocationSize);
      remaining = dest.length - ix;
      if (remaining == 0) return ix;
    } finally {
      freeList.lock.unlock();
    }
    int splitListIndex = freeListIndex;
    byte headerData = (byte)((freeListIndex << 1) | 1);
    while (remaining > 0) {
      ++splitListIndex;
      int splitWays = 1 << (splitListIndex - freeListIndex);
      int headerStep = 1 << splitListIndex;
      int lastSplitBlocksRemaining = -1, lastSplitNextHeader = -1;
      FreeList splitList = arena.freeLists[splitListIndex];
      splitList.lock.lock();
      try {
        int headerIx = splitList.listHead;
        while (headerIx >= 0 && remaining > 0) {
          int origOffset = offsetFromHeaderIndex(headerIx), offset = origOffset;
          int toTake = Math.min(splitWays, remaining);
          remaining -= toTake;
          lastSplitBlocksRemaining = splitWays - toTake;
          for (; toTake > 0; ++ix, --toTake, headerIx += headerStep, offset += allocationSize) {
            arena.headers[headerIx] = headerData;
            dest[ix].initialize(arena.data, offset, allocationSize);
          }
          lastSplitNextHeader = headerIx;
          headerIx = arena.data.getInt(origOffset + 4);
          arena.data.putLong(origOffset, -1); // overwrite list pointers for safety
        }
        splitList.listHead = headerIx;
      } finally {
        splitList.lock.unlock();
      }
      if (remaining == 0) {
        // We have just obtained all we needed by splitting at lastSplitBlockOffset; now
        // we need to put the space remaining from that block into lower free lists.
        // TODO: if we could return blocks asynchronously, we could do this 
        int newListIndex = freeListIndex;
        while (lastSplitBlocksRemaining > 0) {
          if ((lastSplitBlocksRemaining & 1) == 1) {
            arena.headers[lastSplitNextHeader] = (byte)((newListIndex << 1) | 1);
            int offset = offsetFromHeaderIndex(lastSplitNextHeader);
            FreeList newFreeList = arena.freeLists[newListIndex];
            newFreeList.lock.lock();
            try {
              arena.data.putInt(offset, -1);
              arena.data.putInt(offset, newFreeList.listHead);
              newFreeList.listHead = lastSplitNextHeader;
            } finally {
              newFreeList.lock.unlock();
            }
            lastSplitNextHeader += (1 << newListIndex);
          }
          lastSplitBlocksRemaining >>>= 1;
          ++newListIndex;
          continue;
        }
      }
    }
    return ix;
  }

  public int offsetFromHeaderIndex(int lastSplitNextHeader) {
    return lastSplitNextHeader << minAllocLog2;
  }

  public int allocateFromFreeListUnderLock(Arena block, FreeList freeList,
      int freeListIndex, LlapMemoryBuffer[] dest, int ix, int size) {
    int current = freeList.listHead;
    while (current >= 0 && ix < dest.length) {
      int offset = offsetFromHeaderIndex(current);
      block.headers[current] = (byte)((freeListIndex << 1) | 1);
      current = block.data.getInt(offset + 4);
      dest[ix].initialize(block.data, offset, size);
      block.data.putLong(offset, -1); // overwrite list pointers for safety
      ++ix;
    }
    freeList.listHead = current;
    return ix;
  }

  private int allocateWithExpand(
      Arena arena, int freeListIndex, LlapMemoryBuffer[] dest, int ix, int size) {
    if (arena.data != null) return ix; // already allocated
    synchronized (arena) {
      // Never goes from non-null to null, so this is the only place we need sync.
      if (arena.data == null) {
        arena.init(arenaSize, maxAllocation, arenaSizeLog2, minAllocLog2, maxAllocLog2);
      }
    }
    return allocateWithSplit(arena, freeListIndex, dest, ix, size);
  }

  @Override
  public LlapMemoryBuffer[] getFileData(String fileName, long[] offsets) {
    LlapMemoryBuffer[] result = null;
    // TODO: string must be internalized
    FileCache subCache = cache.get(fileName);
    if (subCache == null || !subCache.incRef()) return result;
    try {
      for (int i = 0; i < offsets.length; ++i) {
        while (true) { // Overwhelmingly only runs once.
          long offset = offsets[i];
          LlapCacheableBuffer buffer = subCache.cache.get(offset);
          if (buffer == null) break;
          if (lockBuffer(buffer)) {
            if (result == null) {
              result = new LlapCacheableBuffer[offsets.length];
            }
            result[i] = buffer;
            break;
          }
          if (subCache.cache.remove(offset, buffer)) break;
        }
      }
    } finally {
      subCache.decRef();
    }
    return result;
  }

  private boolean lockBuffer(LlapCacheableBuffer buffer) {
    int rc = buffer.incRef();
    if (rc == 1) {
      cachePolicy.notifyLock(buffer);
    }
    return rc >= 0;
  }

  @Override
  public long[] putFileData(String fileName, long[] offsets, LlapMemoryBuffer[] buffers) {
    long[] result = null;
    assert buffers.length == offsets.length;
    // TODO: string must be internalized
    FileCache subCache = getOrAddFileSubCache(fileName);
    try {
      for (int i = 0; i < offsets.length; ++i) {
        LlapCacheableBuffer buffer = (LlapCacheableBuffer)buffers[i];
        long offset = offsets[i];
        assert buffer.isLocked();
        while (true) { // Overwhelmingly executes once, or maybe twice (replacing stale value).
          LlapCacheableBuffer oldVal = subCache.cache.putIfAbsent(offset, buffer);
          if (oldVal == null) {
            // Cached successfully, add to policy.
            cachePolicy.cache(buffer);
            break;
          }
          if (DebugUtils.isTraceCachingEnabled()) {
            LlapIoImpl.LOG.info("Trying to cache when the chunk is already cached for "
                + fileName + "@" + offset  + "; old " + oldVal + ", new " + buffer);
          }
          if (lockBuffer(oldVal)) {
            // We found an old, valid block for this key in the cache.
            releaseBufferInternal(buffer);
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
  public FileCache getOrAddFileSubCache(String fileName) {
    FileCache newSubCache = null;
    while (true) { // Overwhelmingly executes once.
      FileCache subCache = cache.get(fileName);
      if (subCache != null) {
        if (subCache.incRef()) return subCache; // Main path - found it, incRef-ed it.
        if (newSubCache == null) {
          newSubCache = new FileCache();
          newSubCache.incRef();
        }
        // Found a stale value we cannot incRef; try to replace it with new value.
        if (cache.replace(fileName, subCache, newSubCache)) return newSubCache;
        continue; // Someone else replaced/removed a stale value, try again.
      }
      // No value found.
      if (newSubCache == null) {
        newSubCache = new FileCache();
        newSubCache.incRef();
      }
      FileCache oldSubCache = cache.putIfAbsent(fileName, newSubCache);
      if (oldSubCache == null) return newSubCache; // Main path 2 - created a new file cache.
      if (oldSubCache.incRef()) return oldSubCache; // Someone created one in parallel.
      // Someone created one in parallel and then it went stale.
      if (cache.replace(fileName, oldSubCache, newSubCache)) return newSubCache;
      // Someone else replaced/removed a parallel-added stale value, try again. Max confusion.
    }
  }

  private static int align64(int number) {
    return ((number + 63) & ~63);
  }


  @Override
  public void releaseBuffer(LlapMemoryBuffer buffer) {
    releaseBufferInternal((LlapCacheableBuffer)buffer);
  }

  @Override
  public void releaseBuffers(LlapMemoryBuffer[] cacheBuffers) {
    for (int i = 0; i < cacheBuffers.length; ++i) {
      releaseBufferInternal((LlapCacheableBuffer)cacheBuffers[i]);
    }
  }

  public void releaseBufferInternal(LlapCacheableBuffer buffer) {
    if (buffer.decRef() == 0) {
      cachePolicy.notifyUnlock(buffer);
      unblockEviction();
    }
  }

  public static LlapCacheableBuffer allocateFake() {
    return new LlapCacheableBuffer(null, -1, 1);
  }

  public void unblockEviction() {
    newEvictions.incrementAndGet();
  }

  @Override
  public void notifyEvicted(LlapCacheableBuffer buffer) {
    
  }

  private final class CleanupThread extends Thread {
    private int APPROX_CLEANUP_INTERVAL_SEC = 600;

    public CleanupThread() {
      super("Llap ChunkPool cleanup thread");
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
      long endTime = System.nanoTime() + APPROX_CLEANUP_INTERVAL_SEC * 1000000000L;
      int leftToCheck = 0; // approximate
      for (FileCache fc : cache.values()) {
        leftToCheck += fc.cache.size();
      }
      // TODO: if these super-long-lived iterator affects the map in some bad way,
      //       we'd need to sleep once per round instead.
      // Iterate thru all the filecaches. This is best-effort.
      Iterator<Map.Entry<String, FileCache>> iter = cache.entrySet().iterator();
      while (iter.hasNext()) {
        FileCache fc = iter.next().getValue();
        if (!fc.incRef()) {
          throw new AssertionError("Something other than cleanup is removing elements from map");
        }
        // Iterate thru the file cache. This is best-effort.
        Iterator<Map.Entry<Long, LlapCacheableBuffer>> subIter = fc.cache.entrySet().iterator();
        boolean isEmpty = true;
        while (subIter.hasNext()) {
          Thread.sleep((leftToCheck <= 0)
              ? 1 : (endTime - System.nanoTime()) / (1000000L * leftToCheck));
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

  private static class Arena {
    void init(int arenaSize, int maxAlloc, int arenaSizeLog2, int minAllocLog2, int maxAllocLog2) {
      data = ByteBuffer.allocateDirect(arenaSize);
      int maxMinAllocs = 1 << (arenaSizeLog2 - minAllocLog2);
      headers = new byte[maxMinAllocs];
      int allocLog2Diff = maxAllocLog2 - minAllocLog2;
      freeLists = new FreeList[allocLog2Diff];
      for (int i = 0; i < maxAllocLog2; ++i) {
        freeLists[i] = new FreeList();
      }
      int maxMaxAllocs = 1 << (arenaSizeLog2 - maxAllocLog2),
          headerIndex = 0, headerIncrement = 1 << allocLog2Diff;
      freeLists[maxAllocLog2 - 1].listHead = 0;
      for (int i = 0, offset = 0; i < maxMaxAllocs; ++i, offset += maxAlloc) {
        // TODO: will this cause bugs on large numbers due to some Java sign bit stupidity?
        headers[headerIndex] = (byte)(allocLog2Diff << 1); // Maximum allocation size
        data.putInt(offset, (i == 0) ? -1 : (headerIndex - headerIncrement));
        data.putInt(offset + 4, (i == maxMaxAllocs - 1) ? -1 : (headerIndex + headerIncrement));
        headerIndex += headerIncrement;
      }
    }
    ByteBuffer data;
    // Avoid storing headers with data since we expect binary size allocations.
    // Each headers[i] is a "virtual" byte at i * minAllocation.
    byte[] headers;
    FreeList[] freeLists;
  }

  private static class FreeList {
    ReentrantLock lock = new ReentrantLock(false);
    int listHead = -1; // Index of where the buffer is; in minAllocation units
    // TODO: One possible improvement - store blocks arriving left over from splits, and
    //       blocks requested, to be able to wait for pending splits and reduce fragmentation.
    //       However, we are trying to increase fragmentation now, since we cater to single-size.
  }

  private static class FileCache {
    private static final int EVICTED_REFCOUNT = -1, EVICTING_REFCOUNT = -2;
    // TODO: given the specific data, perhaps the nested thing should not be CHM
    private ConcurrentHashMap<Long, LlapCacheableBuffer> cache
      = new ConcurrentHashMap<Long, LlapCacheableBuffer>();
    private AtomicInteger refCount = new AtomicInteger(0);

    boolean incRef() {
      while (true) {
        int value = refCount.get();
        if (value == EVICTED_REFCOUNT) return false;
        if (value == EVICTING_REFCOUNT) continue; // spin until it resolves
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
}
