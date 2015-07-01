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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.io.api.cache.LlapMemoryBuffer;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;

public final class BuddyAllocator implements EvictionAwareAllocator, BuddyAllocatorMXBean {
  private final Arena[] arenas;
  private AtomicInteger allocatedArenas = new AtomicInteger(0);

  private final MemoryManager memoryManager;

  // Config settings
  private final int minAllocLog2, maxAllocLog2, arenaSizeLog2, maxArenas;
  private final int minAllocation, maxAllocation, arenaSize;
  private final long maxSize;
  private final boolean isDirect;
  private final LlapDaemonCacheMetrics metrics;

  public BuddyAllocator(Configuration conf, MemoryManager memoryManager,
      LlapDaemonCacheMetrics metrics) {
    isDirect = HiveConf.getBoolVar(conf, ConfVars.LLAP_ORC_CACHE_ALLOCATE_DIRECT);
    minAllocation = HiveConf.getIntVar(conf, ConfVars.LLAP_ORC_CACHE_MIN_ALLOC);
    maxAllocation = HiveConf.getIntVar(conf, ConfVars.LLAP_ORC_CACHE_MAX_ALLOC);
    arenaSize = HiveConf.getIntVar(conf, ConfVars.LLAP_ORC_CACHE_ARENA_SIZE);
    long maxSizeVal = HiveConf.getLongVar(conf, ConfVars.LLAP_ORC_CACHE_MAX_SIZE);
    if (LlapIoImpl.LOGL.isInfoEnabled()) {
      LlapIoImpl.LOG.info("Buddy allocator with " + (isDirect ? "direct" : "byte")
          + " buffers; allocation sizes " + minAllocation + " - " + maxAllocation
          + ", arena size " + arenaSize + ". total size " + maxSizeVal);
    }

    if (minAllocation < 8) {
      throw new AssertionError("Min allocation must be at least 8: " + minAllocation);
    }
    if (maxSizeVal < arenaSize || arenaSize < maxAllocation || maxAllocation < minAllocation) {
      throw new AssertionError("Inconsistent sizes of cache, arena and allocations: "
          + minAllocation + ", " + maxAllocation + ", " + arenaSize + ", " + maxSizeVal);
    }
    if ((Integer.bitCount(minAllocation) != 1) || (Integer.bitCount(maxAllocation) != 1)
        || (Long.bitCount(arenaSize) != 1)) {
      // Technically, arena size only needs to be divisible by maxAlloc
      throw new AssertionError("Allocation and arena sizes must be powers of two: "
          + minAllocation + ", " + maxAllocation + ", " + arenaSize);
    }
    if ((maxSizeVal % arenaSize) > 0) {
      long oldMaxSize = maxSizeVal;
      maxSizeVal = (maxSizeVal / arenaSize) * arenaSize;
      LlapIoImpl.LOG.warn("Rounding cache size to " + maxSizeVal + " from " + oldMaxSize
          + " to be divisible by arena size " + arenaSize);
    }
    if ((maxSizeVal / arenaSize) > Integer.MAX_VALUE) {
      throw new AssertionError(
          "Too many arenas needed to allocate the cache: " + arenaSize + "," + maxSizeVal);
    }
    maxSize = maxSizeVal;
    memoryManager.updateMaxSize(maxSize);
    minAllocLog2 = 31 - Integer.numberOfLeadingZeros(minAllocation);
    maxAllocLog2 = 31 - Integer.numberOfLeadingZeros(maxAllocation);
    arenaSizeLog2 = 63 - Long.numberOfLeadingZeros(arenaSize);
    maxArenas = (int)(maxSize / arenaSize);
    arenas = new Arena[maxArenas];
    for (int i = 0; i < maxArenas; ++i) {
      arenas[i] = new Arena();
    }
    arenas[0].init();
    allocatedArenas.set(1);
    this.memoryManager = memoryManager;

    this.metrics = metrics;
    metrics.incrAllocatedArena();
  }

  // TODO: would it make sense to return buffers asynchronously?
  @Override
  public void allocateMultiple(LlapMemoryBuffer[] dest, int size)
      throws LlapCacheOutOfMemoryException {
    assert size > 0 : "size is " + size;
    if (size > maxAllocation) {
      throw new RuntimeException("Trying to allocate " + size + "; max is " + maxAllocation);
    }
    int freeListIx = 31 - Integer.numberOfLeadingZeros(size);
    if (size != (1 << freeListIx)) ++freeListIx; // not a power of two, add one more
    freeListIx = Math.max(freeListIx - minAllocLog2, 0);
    int allocLog2 = freeListIx + minAllocLog2;
    int allocationSize = 1 << allocLog2;
    // TODO: reserving the entire thing is not ideal before we alloc anything. Interleave?
    memoryManager.reserveMemory(dest.length << allocLog2, true);

    int ix = 0;
    for (int i = 0; i < dest.length; ++i) {
      if (dest[i] != null) continue;
      dest[i] = new LlapDataBuffer(); // TODO: pool of objects?
    }
    // First try to quickly lock some of the correct-sized free lists and allocate from them.
    int arenaCount = allocatedArenas.get();
    long threadId = arenaCount > 1 ? Thread.currentThread().getId() : 0;
    {
      int startIndex = (int)(threadId % arenaCount), index = startIndex;
      do {
        int newIx = arenas[index].allocateFast(index, freeListIx, dest, ix, allocationSize);
        if (newIx == dest.length) return;
        if (newIx != -1) {  // TODO: check if it can still happen; count should take care of this.
          ix = newIx;
        }
        ix = newIx;
        if ((++index) == arenaCount) {
          index = 0;
        }
      } while (index != startIndex);
    }

    // TODO: this is very hacky.
    // We called reserveMemory so we know that somewhere in there, there's memory waiting for us.
    // However, we have a class of rare race conditions related to the order of locking/checking of
    // different allocation areas. Simple case - say we have 2 arenas, 256Kb available in arena 2.
    // We look at arena 1; someone deallocs 256Kb from arena 1 and allocs the same from arena 2;
    // we look at arena 2 and find no memory. Or, for single arena, 2 threads reserve 256k each,
    // and a single 1Mb block is available. When the 1st thread locks the 1Mb freelist, the 2nd one
    // might have already examined the 256k and 512k lists, finding nothing. Blocks placed by (1)
    // into smaller lists after its split is done will not be found by (2); given that freelist
    // locks don't overlap, (2) may even run completely between the time (1) takes out the 1Mb
    // block and the time it returns the remaining 768Kb.
    // Two solutions to this are some form of cross-thread helping (threads putting "demand"
    // into some sort of queues that deallocate and split will examine), or having and "actor"
    // allocator thread (or threads per arena).
    // The 2nd one is probably much simpler and will allow us to get rid of a lot of sync code.
    // But for now we will just retry 5 times 0_o
    for (int attempt = 0; attempt < 5; ++attempt) {
      // Try to split bigger blocks. TODO: again, ideally we would tryLock at least once
      for (int i = 0; i < arenaCount; ++i) {
        int newIx = arenas[i].allocateWithSplit(i, freeListIx, dest, ix, allocationSize);
        if (newIx == -1) break; // Shouldn't happen.
        if (newIx == dest.length) return;
        ix = newIx;
      }
      if (attempt == 0) {
        // Try to allocate memory if we haven't allocated all the way to maxSize yet; very rare.
        for (int i = arenaCount; i < arenas.length; ++i) {
          ix = arenas[i].allocateWithExpand(i, freeListIx, dest, ix, allocationSize);
          if (ix == dest.length) return;
        }
      }
      LlapIoImpl.LOG.warn("Failed to allocate despite reserved memory; will retry " + attempt);
    }
    String msg = "Failed to allocate " + size + "; at " + ix + " out of " + dest.length;
    LlapIoImpl.LOG.error(msg + "\nALLOCATOR STATE:\n" + debugDump()
        + "\nPARENT STATE:\n" + memoryManager.debugDumpForOom());
    throw new LlapCacheOutOfMemoryException(msg);
  }

  @Override
  public void deallocate(LlapMemoryBuffer buffer) {
    deallocateInternal(buffer, true);
  }

  @Override
  public void deallocateEvicted(LlapMemoryBuffer buffer) {
    deallocateInternal(buffer, false);
  }

  private void deallocateInternal(LlapMemoryBuffer buffer, boolean doReleaseMemory) {
    LlapDataBuffer buf = (LlapDataBuffer)buffer;
    long memUsage = buf.getMemoryUsage();
    metrics.decrCacheCapacityUsed(buf.byteBuffer.capacity());
    arenas[buf.arenaIndex].deallocate(buf);
    if (doReleaseMemory) {
      memoryManager.releaseMemory(memUsage);
    }
  }

  @Override
  public boolean isDirectAlloc() {
    return isDirect;
  }

  public String debugDump() {
    StringBuilder result = new StringBuilder(
        "NOTE: with multiple threads the dump is not guaranteed to be consistent");
    for (Arena arena : arenas) {
      arena.debugDump(result);
    }
    return result.toString();
  }

  // BuddyAllocatorMXBean
  @Override
  public boolean getIsDirect() {
    return isDirect;
  }

  @Override
  public int getMinAllocation() {
    return minAllocation;
  }

  @Override
  public int getMaxAllocation() {
    return maxAllocation;
  }

  @Override
  public int getArenaSize() {
    return arenaSize;
  }

  @Override
  public long getMaxCacheSize() {
    return maxSize;
  }

  private class Arena {
    private ByteBuffer data;
    // Avoid storing headers with data since we expect binary size allocations.
    // Each headers[i] is a "virtual" byte at i * minAllocation.
    private byte[] headers;
    private FreeList[] freeLists;

    void init() {
      data = isDirect ? ByteBuffer.allocateDirect(arenaSize) : ByteBuffer.allocate(arenaSize);
      int maxMinAllocs = 1 << (arenaSizeLog2 - minAllocLog2);
      headers = new byte[maxMinAllocs];
      int allocLog2Diff = maxAllocLog2 - minAllocLog2, freeListCount = allocLog2Diff + 1;
      freeLists = new FreeList[freeListCount];
      for (int i = 0; i < freeListCount; ++i) {
        freeLists[i] = new FreeList();
      }
      int maxMaxAllocs = 1 << (arenaSizeLog2 - maxAllocLog2),
          headerIndex = 0, headerStep = 1 << allocLog2Diff;
      freeLists[allocLog2Diff].listHead = 0;
      for (int i = 0, offset = 0; i < maxMaxAllocs; ++i, offset += maxAllocation) {
        // TODO: will this cause bugs on large numbers due to some Java sign bit stupidity?
        headers[headerIndex] = makeHeader(allocLog2Diff, false);
        data.putInt(offset, (i == 0) ? -1 : (headerIndex - headerStep));
        data.putInt(offset + 4, (i == maxMaxAllocs - 1) ? -1 : (headerIndex + headerStep));
        headerIndex += headerStep;
      }
    }

    public void debugDump(StringBuilder result) {
      result.append("\nArena: ");
      if (data == null) {
        result.append(" not allocated");
        return;
      }
      // Try to get as consistent view as we can; make copy of the headers.
      byte[] headers = new byte[this.headers.length];
      System.arraycopy(this.headers, 0, headers, 0, headers.length);
      int allocSize = minAllocation;
      for (int i = 0; i < freeLists.length; ++i, allocSize <<= 1) {
        result.append("\n  free list for size " + allocSize + ": ");
        FreeList freeList = freeLists[i];
        freeList.lock.lock();
        try {
          int nextHeaderIx = freeList.listHead;
          while (nextHeaderIx >= 0) {
            result.append(nextHeaderIx + ", ");
            nextHeaderIx = data.getInt(offsetFromHeaderIndex(nextHeaderIx));
          }
        } finally {
          freeList.lock.unlock();
        }
      }
      for (int i = 0; i < headers.length; ++i) {
        byte header = headers[i];
        if (header == 0) continue;
        int freeListIx = freeListFromHeader(header), offset = offsetFromHeaderIndex(i);
        boolean isFree = (header & 1) == 0;
        result.append("\n  block " + i + " at " + offset + ": size "
            + (1 << (freeListIx + minAllocLog2)) + ", " + (isFree ? "free" : "allocated"));
      }
    }

    private int freeListFromHeader(byte header) {
      return (header >> 1) - 1;
    }

    private int allocateFast(
        int arenaIx, int freeListIx, LlapMemoryBuffer[] dest, int ix, int size) {
      if (data == null) return -1; // not allocated yet
      FreeList freeList = freeLists[freeListIx];
      if (!freeList.lock.tryLock()) return ix;
      try {
        return allocateFromFreeListUnderLock(arenaIx, freeList, freeListIx, dest, ix, size);
      } finally {
        freeList.lock.unlock();
      }
    }

    private int allocateWithSplit(int arenaIx, int freeListIx,
        LlapMemoryBuffer[] dest, int ix, int allocationSize) {
      if (data == null) return -1; // not allocated yet
      FreeList freeList = freeLists[freeListIx];
      int remaining = -1;
      freeList.lock.lock();
      // TODO: write some comments for this method
      try {
        ix = allocateFromFreeListUnderLock(
            arenaIx, freeList, freeListIx, dest, ix, allocationSize);
        remaining = dest.length - ix;
        if (remaining == 0) return ix;
      } finally {
        freeList.lock.unlock();
      }
      byte headerData = makeHeader(freeListIx, true);
      int headerStep = 1 << freeListIx;
      int splitListIx = freeListIx + 1;
      while (remaining > 0 && splitListIx < freeLists.length) {
        int splitWaysLog2 = (splitListIx - freeListIx);
        assert splitWaysLog2 > 0;
        int splitWays = 1 << splitWaysLog2; // How many ways each block splits into target size.
        int lastSplitBlocksRemaining = -1; // How many target-sized blocks remain from last split.
        int lastSplitNextHeader = -1; // The header index for the beginning of the remainder.
        FreeList splitList = freeLists[splitListIx];
        splitList.lock.lock();
        try {
          int headerIx = splitList.listHead;
          while (headerIx >= 0 && remaining > 0) {
            int origOffset = offsetFromHeaderIndex(headerIx), offset = origOffset;
            int toTake = Math.min(splitWays, remaining); // We split it splitWays and take toTake.
            remaining -= toTake;
            lastSplitBlocksRemaining = splitWays - toTake; // Whatever remains.
            // Take toTake blocks by splitting the block at origOffset.
            for (; toTake > 0; ++ix, --toTake, headerIx += headerStep, offset += allocationSize) {
              headers[headerIx] = headerData;
              // TODO: this could be done out of the lock, we only need to take the blocks out.
              ((LlapDataBuffer)dest[ix]).initialize(arenaIx, data, offset, allocationSize);
            }
            lastSplitNextHeader = headerIx; // If anything remains, this is where it starts.
            headerIx = data.getInt(origOffset + 4); // Get next item from the free list.
          }
          replaceListHeadUnderLock(splitList, headerIx); // In the end, update free list head.
        } finally {
          splitList.lock.unlock();
        }
        if (remaining == 0) {
          // We have just obtained all we needed by splitting at lastSplitBlockOffset; now
          // we need to put the space remaining from that block into lower free lists.
          int newListIndex = freeListIx;
          while (lastSplitBlocksRemaining > 0) {
            if ((lastSplitBlocksRemaining & 1) == 1) {
              FreeList newFreeList = freeLists[newListIndex];
              newFreeList.lock.lock();
              headers[lastSplitNextHeader] = makeHeader(newListIndex, false);
              try {
                addBlockToFreeListUnderLock(newFreeList, lastSplitNextHeader);
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
        ++splitListIx;
      }
      return ix;
    }

    private void replaceListHeadUnderLock(FreeList freeList, int headerIx) {
      if (headerIx == freeList.listHead) return;
      if (headerIx >= 0) {
        int newHeadOffset = offsetFromHeaderIndex(headerIx);
        data.putInt(newHeadOffset, -1); // Remove backlink.
      }
      freeList.listHead = headerIx;
    }

    private int allocateWithExpand(
        int arenaIx, int freeListIx, LlapMemoryBuffer[] dest, int ix, int size) {
      if (data == null) {
        synchronized (this) {
          // Never goes from non-null to null, so this is the only place we need sync.
          if (data == null) {
            init();
            allocatedArenas.incrementAndGet();
            metrics.incrAllocatedArena();
          }
        }
      }
      return allocateWithSplit(arenaIx, freeListIx, dest, ix, size);
    }

    public int offsetFromHeaderIndex(int lastSplitNextHeader) {
      return lastSplitNextHeader << minAllocLog2;
    }

    public int allocateFromFreeListUnderLock(int arenaIx, FreeList freeList,
        int freeListIx, LlapMemoryBuffer[] dest, int ix, int size) {
      int current = freeList.listHead;
      while (current >= 0 && ix < dest.length) {
        int offset = offsetFromHeaderIndex(current);
        // Noone else has this either allocated or in a different free list; no sync needed.
        headers[current] = makeHeader(freeListIx, true);
        current = data.getInt(offset + 4);
        ((LlapDataBuffer)dest[ix]).initialize(arenaIx, data, offset, size);
        ++ix;
      }
      replaceListHeadUnderLock(freeList, current);
      return ix;
    }

    private byte makeHeader(int freeListIx, boolean isInUse) {
      return (byte)(((freeListIx + 1) << 1) | (isInUse ? 1 : 0));
    }

    public void deallocate(LlapDataBuffer buffer) {
      assert data != null;
      int headerIx = buffer.byteBuffer.position() >>> minAllocLog2,
          freeListIx = freeListFromHeader(headers[headerIx]);
      assert freeListIx == (31 - Integer.numberOfLeadingZeros(buffer.allocSize) - minAllocLog2);
      while (true) {
        FreeList freeList = freeLists[freeListIx];
        int bHeaderIx = headerIx ^ (1 << freeListIx);
        freeList.lock.lock();
        try {
          if ((freeListIx == freeLists.length - 1)
            || headers[bHeaderIx] != makeHeader(freeListIx, false)) {
            // Buddy block is allocated, or it is on higher level of allocation than we are, or we
            // have reached the top level. Add whatever we have got to the current free list.
            addBlockToFreeListUnderLock(freeList, headerIx);
            headers[headerIx] = makeHeader(freeListIx, false);
            break;
          }
          // Buddy block is free and in the same free list we have locked. Take it out for merge.
          removeBlockFromFreeList(freeList, bHeaderIx);
          headers[bHeaderIx] = headers[headerIx] = 0; // Erase both headers of the blocks to merge.
        } finally {
          freeList.lock.unlock();
        }
        ++freeListIx;
        headerIx = Math.min(headerIx, bHeaderIx);
      }
    }

    private void addBlockToFreeListUnderLock(FreeList freeList, int headerIx) {
      if (freeList.listHead >= 0) {
        int oldHeadOffset = offsetFromHeaderIndex(freeList.listHead);
        assert data.getInt(oldHeadOffset) == -1;
        data.putInt(oldHeadOffset, headerIx);
      }
      int offset = offsetFromHeaderIndex(headerIx);
      data.putInt(offset, -1);
      data.putInt(offset + 4, freeList.listHead);
      freeList.listHead = headerIx;
    }

    private void removeBlockFromFreeList(FreeList freeList, int headerIx) {
      int bOffset = offsetFromHeaderIndex(headerIx),
          bpHeaderIx = data.getInt(bOffset), bnHeaderIx = data.getInt(bOffset + 4);
      if (freeList.listHead == headerIx) {
        assert bpHeaderIx == -1;
        freeList.listHead = bnHeaderIx;
      }
      if (bpHeaderIx != -1) {
        data.putInt(offsetFromHeaderIndex(bpHeaderIx) + 4, bnHeaderIx);
      }
      if (bnHeaderIx != -1) {
        data.putInt(offsetFromHeaderIndex(bnHeaderIx), bpHeaderIx);
      }
    }
  }

  private static class Request {

  }
  private static class FreeList {
    ReentrantLock lock = new ReentrantLock(false);
    int listHead = -1; // Index of where the buffer is; in minAllocation units
    // TODO: One possible improvement - store blocks arriving left over from splits, and
    //       blocks requested, to be able to wait for pending splits and reduce fragmentation.
    //       However, we are trying to increase fragmentation now, since we cater to single-size.
  }
}
