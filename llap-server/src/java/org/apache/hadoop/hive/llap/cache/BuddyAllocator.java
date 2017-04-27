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

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;

public final class BuddyAllocator implements EvictionAwareAllocator, BuddyAllocatorMXBean {
  private final Arena[] arenas;
  private final AtomicInteger allocatedArenas = new AtomicInteger(0);

  private final MemoryManager memoryManager;
  private static final long MAX_DUMP_INTERVAL_NS = 300 * 1000000000L; // 5 minutes.
  private final AtomicLong lastLog = new AtomicLong(-1);

  // Config settings
  private final int minAllocLog2, maxAllocLog2, arenaSizeLog2, maxArenas;
  private final int minAllocation, maxAllocation, arenaSize;
  private final long maxSize;
  private final boolean isDirect;
  private final boolean isMapped;
  private final Path cacheDir;
  private final LlapDaemonCacheMetrics metrics;

  // We don't know the acceptable size for Java array, so we'll use 1Gb boundary.
  // That is guaranteed to fit any maximum allocation.
  private static final int MAX_ARENA_SIZE = 1024*1024*1024;
  // Don't try to operate with less than MIN_SIZE allocator space, it will just give you grief.
  private static final int MIN_TOTAL_MEMORY_SIZE = 64*1024*1024;

  private static final FileAttribute<Set<PosixFilePermission>> RWX = PosixFilePermissions
      .asFileAttribute(PosixFilePermissions.fromString("rwx------"));
  private static final FileAttribute<Set<PosixFilePermission>> RW_ = PosixFilePermissions
      .asFileAttribute(PosixFilePermissions.fromString("rw-------"));


  public BuddyAllocator(Configuration conf, MemoryManager mm, LlapDaemonCacheMetrics metrics) {
    this(HiveConf.getBoolVar(conf, ConfVars.LLAP_ALLOCATOR_DIRECT),
        HiveConf.getBoolVar(conf, ConfVars.LLAP_ALLOCATOR_MAPPED),
        (int)HiveConf.getSizeVar(conf, ConfVars.LLAP_ALLOCATOR_MIN_ALLOC),
        (int)HiveConf.getSizeVar(conf, ConfVars.LLAP_ALLOCATOR_MAX_ALLOC),
        HiveConf.getIntVar(conf, ConfVars.LLAP_ALLOCATOR_ARENA_COUNT),
        getMaxTotalMemorySize(conf), 
        HiveConf.getVar(conf, ConfVars.LLAP_ALLOCATOR_MAPPED_PATH),
        mm, metrics);
  }

  private static long getMaxTotalMemorySize(Configuration conf) {
    long maxSize = HiveConf.getSizeVar(conf, ConfVars.LLAP_IO_MEMORY_MAX_SIZE);
    if (maxSize > MIN_TOTAL_MEMORY_SIZE || HiveConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST)) {
      return maxSize;
    }
    throw new RuntimeException("Allocator space is too small for reasonable operation; "
        + ConfVars.LLAP_IO_MEMORY_MAX_SIZE.varname + "=" + maxSize + ", but at least "
        + MIN_TOTAL_MEMORY_SIZE + " is required. If you cannot spare any memory, you can "
        + "disable LLAP IO entirely via " + ConfVars.LLAP_IO_ENABLED.varname + "; or set "
        + ConfVars.LLAP_IO_MEMORY_MODE.varname + " to 'none'");
  }

  @VisibleForTesting
  public BuddyAllocator(boolean isDirectVal, int minAllocVal, int maxAllocVal, int arenaCount,
      long maxSizeVal, MemoryManager memoryManager, LlapDaemonCacheMetrics metrics) {
    this(isDirectVal, false /*isMapped*/,  minAllocVal, maxAllocVal, arenaCount, maxSizeVal, 
        null /* mapping path */, memoryManager, metrics);
  }

  @VisibleForTesting
  public BuddyAllocator(boolean isDirectVal, boolean isMappedVal, int minAllocVal, int maxAllocVal,
      int arenaCount, long maxSizeVal, String mapPath, MemoryManager memoryManager,
      LlapDaemonCacheMetrics metrics) {
    isDirect = isDirectVal;
    isMapped = isMappedVal;
    minAllocation = minAllocVal;
    maxAllocation = maxAllocVal;
    if (isMapped) {
      try {
        cacheDir =
            Files.createTempDirectory(FileSystems.getDefault().getPath(mapPath), "llap-", RWX);
      } catch (IOException ioe) {
        // conf validator already checks this, so it will never trigger usually
        throw new AssertionError("Configured mmap directory should be writable", ioe);
      }
    } else {
      cacheDir = null;
    }
    long arenaSizeVal = (arenaCount == 0) ? MAX_ARENA_SIZE : maxSizeVal / arenaCount;
    // The math.min, and the fact that maxAllocation is an int, ensures we don't overflow.
    arenaSizeVal = Math.max(maxAllocation, Math.min(arenaSizeVal, MAX_ARENA_SIZE));
    if (LlapIoImpl.LOG.isInfoEnabled()) {
      LlapIoImpl.LOG.info("Buddy allocator with " + (isDirect ? "direct" : "byte") + " buffers; "
          + (isMapped ? ("memory mapped off " + cacheDir.toString() + "; ") : "")
          + "allocation sizes " + minAllocation + " - " + maxAllocation
          + ", arena size " + arenaSizeVal + ", total size " + maxSizeVal);
    }

    String minName = ConfVars.LLAP_ALLOCATOR_MIN_ALLOC.varname,
        maxName = ConfVars.LLAP_ALLOCATOR_MAX_ALLOC.varname;
    if (minAllocation < 8) {
      throw new RuntimeException(minName + " must be at least 8 bytes: " + minAllocation);
    }
    if (maxSizeVal < maxAllocation || maxAllocation < minAllocation) {
      throw new RuntimeException("Inconsistent sizes; expecting " + minName + " <= " + maxName
          + " <= " + ConfVars.LLAP_IO_MEMORY_MAX_SIZE.varname + "; configured with min="
          + minAllocation + ", max=" + maxAllocation + " and total=" + maxSizeVal);
    }
    if ((Integer.bitCount(minAllocation) != 1) || (Integer.bitCount(maxAllocation) != 1)) {
      throw new RuntimeException("Allocation sizes must be powers of two; configured with "
          + minName + "=" + minAllocation + ", " + maxName + "=" + maxAllocation);
    }
    if ((arenaSizeVal % maxAllocation) > 0) {
      long oldArenaSize = arenaSizeVal;
      arenaSizeVal = (arenaSizeVal / maxAllocation) * maxAllocation;
      LlapIoImpl.LOG.warn("Rounding arena size to " + arenaSizeVal + " from " + oldArenaSize
          + " to be divisible by allocation size " + maxAllocation);
    }
    arenaSize = (int)arenaSizeVal;
    if ((maxSizeVal % arenaSize) > 0) {
      long oldMaxSize = maxSizeVal;
      maxSizeVal = (maxSizeVal / arenaSize) * arenaSize;
      LlapIoImpl.LOG.warn("Rounding cache size to " + maxSizeVal + " from " + oldMaxSize
          + " to be divisible by arena size " + arenaSize);
    }
    if ((maxSizeVal / arenaSize) > Integer.MAX_VALUE) {
      throw new RuntimeException(
          "Too many arenas needed to allocate the cache: " + arenaSize + ", " + maxSizeVal);
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
  public void allocateMultiple(MemoryBuffer[] dest, int size)
      throws AllocatorOutOfMemoryException {
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
    memoryManager.reserveMemory(dest.length << allocLog2);
    int destAllocIx = 0;
    for (int i = 0; i < dest.length; ++i) {
      if (dest[i] != null) continue;
      dest[i] = createUnallocated(); // TODO: pool of objects?
    }
    // First try to quickly lock some of the correct-sized free lists and allocate from them.
    int arenaCount = allocatedArenas.get();
    if (arenaCount < 0) {
      arenaCount = -arenaCount - 1; // Next arena is being allocated.
    }
    long threadId = arenaCount > 1 ? Thread.currentThread().getId() : 0;
    {
      int startArenaIx = (int)(threadId % arenaCount), index = startArenaIx;
      do {
        int newDestIx = arenas[index].allocateFast(
            index, freeListIx, dest, destAllocIx, allocationSize);
        if (newDestIx == dest.length) return;
        assert newDestIx != -1;
        destAllocIx = newDestIx;
        if ((++index) == arenaCount) {
          index = 0;
        }
      } while (index != startArenaIx);
    }

    // 1) We can get fragmented on large blocks of uncompressed data. The memory might be
    // in there, but it might be in separate small blocks. This is a complicated problem, and
    // several solutions (in order of decreasing ugliness and increasing complexity) are: just
    // ask to evict the exact-sized block (there may be no such block), evict from a particular
    // arena (policy would know allocator internals somewhat), store buffer mapping and ask to
    // evict from specific choice of blocks next to each other or next to already-evicted block,
    // and finally do a compaction (requires a block mapping and complex sync). For now we'd just
    // force-evict some memory and avoid both complexity and ugliness, since large blocks are rare.
    // 2) Fragmentation aside (TODO: and this is a very hacky solution for that),
    // we called reserveMemory so we know that there's memory waiting for us somewhere.
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
    // But for now we will just retry. We will evict more each time.
    long forceReserved = 0;
    int attempt = 0;
    try {
      while (true) {
        // Try to split bigger blocks. TODO: again, ideally we would tryLock at least once
        {
          int startArenaIx = (int)((threadId + attempt) % arenaCount), arenaIx = startArenaIx;
          do {
            int newDestIx = arenas[arenaIx].allocateWithSplit(
                arenaIx, freeListIx, dest, destAllocIx, allocationSize);
            if (newDestIx == dest.length) return;
            assert newDestIx != -1;
            destAllocIx = newDestIx;
            if ((++arenaIx) == arenaCount) {
              arenaIx = 0;
            }
          } while (arenaIx != startArenaIx);
        }

        if (attempt == 0) {
          // Try to allocate memory if we haven't allocated all the way to maxSize yet; very rare.
          for (int arenaIx = arenaCount; arenaIx < arenas.length; ++arenaIx) {
            destAllocIx = arenas[arenaIx].allocateWithExpand(
                arenaIx, freeListIx, dest, destAllocIx, allocationSize);
            if (destAllocIx == dest.length) return;
          }
        }
        int numberToForce = (dest.length - destAllocIx) * (attempt + 1);
        long newReserved = memoryManager.forceReservedMemory(allocationSize, numberToForce);
        forceReserved += newReserved;
        if (newReserved == 0) {
          // Cannot force-evict anything, give up.
          String msg = "Failed to allocate " + size + "; at " + destAllocIx + " out of "
              + dest.length + " (entire cache is fragmented and locked, or an internal issue)";
          logOomErrorMessage(msg);
          throw new AllocatorOutOfMemoryException(msg);
        }
        if (attempt == 0) {
          LlapIoImpl.LOG.warn("Failed to allocate despite reserved memory; will retry");
        }
        ++attempt;
      }
    } finally {
      if (attempt > 4) {
        LlapIoImpl.LOG.warn("Allocation of " + dest.length + " buffers of size " + size
            + " took " + attempt + " attempts to evict enough memory");
      }
      // After we succeed (or fail), release the force-evicted memory to memory manager. We have
      // previously reserved enough to allocate all we need, so we don't take our allocation out
      // of this - as per the comment above, we basically just wasted a bunch of cache (and CPU).
      if (forceReserved > 0) {
        memoryManager.releaseMemory(forceReserved);
      }
    }

  }

  private void logOomErrorMessage(String msg) {
    while (true) {
      long time = System.nanoTime();
      long lastTime = lastLog.get();
      // Magic value usage is invalid with nanoTime, so once in a 1000 years we may log extra.
      boolean shouldLog = (lastTime == -1 || (time - lastTime) > MAX_DUMP_INTERVAL_NS);
      if (shouldLog && !lastLog.compareAndSet(lastTime, time)) {
        continue;
      }
      if (shouldLog) {
        LlapIoImpl.LOG.error(msg + "\nALLOCATOR STATE:\n" + debugDump()
            + "\nPARENT STATE:\n" + memoryManager.debugDumpForOom());
      } else {
        LlapIoImpl.LOG.error(msg);
      }
      return;
    }
  }

  @Override
  public void deallocate(MemoryBuffer buffer) {
    deallocateInternal(buffer, true);
  }

  @Override
  public void deallocateEvicted(MemoryBuffer buffer) {
    deallocateInternal(buffer, false);
  }

  private void deallocateInternal(MemoryBuffer buffer, boolean doReleaseMemory) {
    LlapDataBuffer buf = (LlapDataBuffer)buffer;
    long memUsage = buf.getMemoryUsage();
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

  private ByteBuffer preallocate(int arenaSize) {
    if (isMapped) {
      RandomAccessFile rwf = null;
      File rf = null;
      Preconditions.checkArgument(isDirect, "All memory mapped allocations have to be direct buffers");
      try {
        rf = File.createTempFile("arena-", ".cache", cacheDir.toFile());
        rwf = new RandomAccessFile(rf, "rw");
        rwf.setLength(arenaSize); // truncate (TODO: posix_fallocate?)
        // Use RW, not PRIVATE because the copy-on-write is irrelevant for a deleted file
        // see discussion in YARN-5551 for the memory accounting discussion
        ByteBuffer rwbuf = rwf.getChannel().map(MapMode.READ_WRITE, 0, arenaSize);
        return rwbuf;
      } catch (IOException ioe) {
        LlapIoImpl.LOG.warn("Failed trying to allocate memory mapped arena", ioe);
        // fail similarly when memory allocations fail
        throw new OutOfMemoryError("Failed trying to allocate memory mapped arena: " + ioe.getMessage());
      } finally {
        // A mapping, once established, is not dependent upon the file channel that was used to
        // create it. delete file and hold onto the map
        IOUtils.closeQuietly(rwf);
        if (rf != null) {
          rf.delete();
        }
      }
    }
    return isDirect ? ByteBuffer.allocateDirect(arenaSize) : ByteBuffer.allocate(arenaSize);
  }

  private class Arena {
    private ByteBuffer data;
    // Avoid storing headers with data since we expect binary size allocations.
    // Each headers[i] is a "virtual" byte at i * minAllocation.
    private byte[] headers;
    private FreeList[] freeLists;

    void init() {
      try {
        data = preallocate(arenaSize);
      } catch (OutOfMemoryError oom) {
        throw new OutOfMemoryError("Cannot allocate " + arenaSize + " bytes: " + oom.getMessage()
            + "; make sure your xmx and process size are set correctly.");
      }
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
            nextHeaderIx = getNextFreeListItem(offsetFromHeaderIndex(nextHeaderIx));
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
        int arenaIx, int freeListIx, MemoryBuffer[] dest, int ix, int size) {
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
        MemoryBuffer[] dest, int ix, int allocationSize) {
      if (data == null) return -1; // not allocated yet
      FreeList freeList = freeLists[freeListIx];
      int remaining = -1;
      freeList.lock.lock();
      try {
        // Try to allocate from target-sized free list, maybe we'll get lucky.
        ix = allocateFromFreeListUnderLock(
            arenaIx, freeList, freeListIx, dest, ix, allocationSize);
        remaining = dest.length - ix;
        if (remaining == 0) return ix;
      } finally {
        freeList.lock.unlock();
      }
      byte headerData = makeHeader(freeListIx, true); // Header for newly allocated used blocks.
      int headerStep = 1 << freeListIx; // Number of headers (smallest blocks) per target block.
      int splitListIx = freeListIx + 1; // Next free list from which we will be splitting.
      // Each iteration of this loop tries to split blocks from one level of the free list into
      // target size blocks; if we cannot satisfy the allocation from the free list containing the
      // blocks of a particular size, we'll try to split yet larger blocks, until we run out.
      while (remaining > 0 && splitListIx < freeLists.length) {
        int splitWaysLog2 = (splitListIx - freeListIx);
        assert splitWaysLog2 > 0;
        int splitWays = 1 << splitWaysLog2; // How many ways each block splits into target size.
        int lastSplitBlocksRemaining = -1; // How many target-sized blocks remain from last split.
        int lastSplitNextHeader = -1; // The header index for the beginning of the remainder.
        FreeList splitList = freeLists[splitListIx];
        splitList.lock.lock();
        try {
          int headerIx = splitList.listHead; // Index of the next free block to split.
          while (headerIx >= 0 && remaining > 0) {
            int origOffset = offsetFromHeaderIndex(headerIx), offset = origOffset;
            // We will split the block at headerIx [splitWays] ways, and take [toTake] blocks,
            // which will leave [lastSplitBlocksRemaining] free blocks of target size.
            int toTake = Math.min(splitWays, remaining);
            remaining -= toTake;
            lastSplitBlocksRemaining = splitWays - toTake; // Whatever remains.
            // Take toTake blocks by splitting the block at offset.
            for (; toTake > 0; ++ix, --toTake, headerIx += headerStep, offset += allocationSize) {
              headers[headerIx] = headerData;
              // TODO: this could be done out of the lock, we only need to take the blocks out.
              ((LlapDataBuffer)dest[ix]).initialize(arenaIx, data, offset, allocationSize);
            }
            lastSplitNextHeader = headerIx; // If anything remains, this is where it starts.
            headerIx = getNextFreeListItem(origOffset);
          }
          replaceListHeadUnderLock(splitList, headerIx); // In the end, update free list head.
        } finally {
          splitList.lock.unlock();
        }
        if (remaining == 0) {
          // We have just obtained all we needed by splitting some block; now we need
          // to put the space remaining from that block into lower free lists.
          // We'll put at most one block into each list, since 2 blocks can always be combined
          // to make a larger-level block. Each bit in the remaining target-sized blocks count
          // is one block in a list offset from target-sized list by bit index.
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
        int arenaIx, int freeListIx, MemoryBuffer[] dest, int ix, int size) {
      while (true) {
        int arenaCount = allocatedArenas.get(), allocArenaCount = arenaCount;
        if (arenaCount < 0)  {
          allocArenaCount = -arenaCount - 1; // Someone is allocating an arena.
        }
        if (allocArenaCount > arenaIx) {
          // Someone already allocated this arena; just do the usual thing.
          return allocateWithSplit(arenaIx, freeListIx, dest, ix, size);
        }
        if ((arenaIx + 1) == -arenaCount) {
          // Someone is allocating this arena. Wait a bit and recheck.
          try {
            synchronized (this) {
              this.wait(100);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupt, won't handle here.
          }
          continue;
        }
        // Either this arena is being allocated, or it is already allocated, or it is next. The
        // caller should not try to allocate another arena before waiting for the previous one.
        assert arenaCount == arenaIx :
          "Arena count " + arenaCount + " but " + arenaIx + " is not being allocated";
        if (!allocatedArenas.compareAndSet(arenaCount, -arenaCount - 1)) {
          continue; // CAS race, look again.
        }
        assert data == null;
        init();
        boolean isCommited = allocatedArenas.compareAndSet(-arenaCount - 1, arenaCount + 1);
        assert isCommited;
        synchronized (this) {
          this.notifyAll();
        }
        metrics.incrAllocatedArena();
        return allocateWithSplit(arenaIx, freeListIx, dest, ix, size);
      }
    }

    public int offsetFromHeaderIndex(int lastSplitNextHeader) {
      return lastSplitNextHeader << minAllocLog2;
    }

    public int allocateFromFreeListUnderLock(int arenaIx, FreeList freeList,
        int freeListIx, MemoryBuffer[] dest, int ix, int size) {
      int current = freeList.listHead;
      while (current >= 0 && ix < dest.length) {
        int offset = offsetFromHeaderIndex(current);
        // Noone else has this either allocated or in a different free list; no sync needed.
        headers[current] = makeHeader(freeListIx, true);
        current = getNextFreeListItem(offset);
        ((LlapDataBuffer)dest[ix]).initialize(arenaIx, data, offset, size);
        ++ix;
      }
      replaceListHeadUnderLock(freeList, current);
      return ix;
    }

    private int getPrevFreeListItem(int offset) {
      return data.getInt(offset);
    }

    private int getNextFreeListItem(int offset) {
      return data.getInt(offset + 4);
    }

    private byte makeHeader(int freeListIx, boolean isInUse) {
      return (byte)(((freeListIx + 1) << 1) | (isInUse ? 1 : 0));
    }

    public void deallocate(LlapDataBuffer buffer) {
      assert data != null;
      int headerIx = buffer.byteBuffer.position() >>> minAllocLog2,
          freeListIx = freeListFromHeader(headers[headerIx]);
      assert freeListIx == (31 - Integer.numberOfLeadingZeros(buffer.allocSize) - minAllocLog2)
          : buffer.allocSize + " " + freeListIx;
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
        assert getPrevFreeListItem(oldHeadOffset) == -1;
        data.putInt(oldHeadOffset, headerIx);
      }
      int offset = offsetFromHeaderIndex(headerIx);
      data.putInt(offset, -1);
      data.putInt(offset + 4, freeList.listHead);
      freeList.listHead = headerIx;
    }

    private void removeBlockFromFreeList(FreeList freeList, int headerIx) {
      int bOffset = offsetFromHeaderIndex(headerIx),
          bpHeaderIx = getPrevFreeListItem(bOffset), bnHeaderIx = getNextFreeListItem(bOffset);
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

  private static class FreeList {
    ReentrantLock lock = new ReentrantLock(false);
    int listHead = -1; // Index of where the buffer is; in minAllocation units
    // TODO: One possible improvement - store blocks arriving left over from splits, and
    //       blocks requested, to be able to wait for pending splits and reduce fragmentation.
    //       However, we are trying to increase fragmentation now, since we cater to single-size.
  }

  @Override
  public MemoryBuffer createUnallocated() {
    return new LlapDataBuffer();
  }
}
