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
import java.util.Arrays;
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
import org.apache.hive.common.util.FixedSizedObjectPool;

public final class BuddyAllocator
  implements EvictionAwareAllocator, BuddyAllocatorMXBean, LlapOomDebugDump {
  private final Arena[] arenas;
  private final AtomicInteger allocatedArenas = new AtomicInteger(0);

  private final MemoryManager memoryManager;
  private static final long MAX_DUMP_INTERVAL_NS = 300 * 1000000000L; // 5 minutes.
  private final AtomicLong lastLog = new AtomicLong(-1);
  private final LlapDaemonCacheMetrics metrics;
  private static final int MAX_DISCARD_ATTEMPTS = 10, LOG_DISCARD_ATTEMPTS = 5;

  // Config settings
  private final int minAllocLog2, maxAllocLog2, arenaSizeLog2, maxArenas;
  private final int minAllocation, maxAllocation, arenaSize;
  private final long maxSize;
  private final boolean isDirect;
  private final boolean isMapped;
  private final Path cacheDir;

  // These are only used for tests.
  private boolean enableDefragShortcut = true, oomLogging = true;

  // We don't know the acceptable size for Java array, so we'll use 1Gb boundary.
  // That is guaranteed to fit any maximum allocation.
  private static final int MAX_ARENA_SIZE = 1024*1024*1024;
  // Don't try to operate with less than MIN_SIZE allocator space, it will just give you grief.
  private static final int MIN_TOTAL_MEMORY_SIZE = 64*1024*1024;
  // Maximum reasonable defragmentation headroom. Mostly kicks in on very small caches.
  private static final float MAX_DEFRAG_HEADROOM_FRACTION = 0.01f;

  private static final FileAttribute<Set<PosixFilePermission>> RWX = PosixFilePermissions
      .asFileAttribute(PosixFilePermissions.fromString("rwx------"));
  private final AtomicLong[] defragCounters;
  private final boolean doUseFreeListDiscard, doUseBruteDiscard;
  private final FixedSizedObjectPool<DiscardContext> ctxPool;
  private final static boolean assertsEnabled = areAssertsEnabled();

  public BuddyAllocator(Configuration conf, MemoryManager mm, LlapDaemonCacheMetrics metrics) {
    this(HiveConf.getBoolVar(conf, ConfVars.LLAP_ALLOCATOR_DIRECT),
        HiveConf.getBoolVar(conf, ConfVars.LLAP_ALLOCATOR_MAPPED),
        (int)HiveConf.getSizeVar(conf, ConfVars.LLAP_ALLOCATOR_MIN_ALLOC),
        (int)HiveConf.getSizeVar(conf, ConfVars.LLAP_ALLOCATOR_MAX_ALLOC),
        HiveConf.getIntVar(conf, ConfVars.LLAP_ALLOCATOR_ARENA_COUNT),
        getMaxTotalMemorySize(conf),
        HiveConf.getSizeVar(conf, ConfVars.LLAP_ALLOCATOR_DEFRAG_HEADROOM),
        HiveConf.getVar(conf, ConfVars.LLAP_ALLOCATOR_MAPPED_PATH),
        mm, metrics, HiveConf.getVar(conf, ConfVars.LLAP_ALLOCATOR_DISCARD_METHOD));
  }

  private static boolean areAssertsEnabled() {
    boolean assertsEnabled = false;
    assert assertsEnabled = true;
    return assertsEnabled;
  }

  private static long getMaxTotalMemorySize(Configuration conf) {
    long maxSize = HiveConf.getSizeVar(conf, ConfVars.LLAP_IO_MEMORY_MAX_SIZE);
    if (maxSize > MIN_TOTAL_MEMORY_SIZE || HiveConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST)) {
      return maxSize;
    }
    throw new RuntimeException("Allocator space is too small for reasonable operation; "
        + ConfVars.LLAP_IO_MEMORY_MAX_SIZE.varname + "=" + maxSize + ", but at least "
        + MIN_TOTAL_MEMORY_SIZE + " is required. If you cannot spare any memory, you can "
        + "disable LLAP IO entirely via " + ConfVars.LLAP_IO_ENABLED.varname);
  }

  @VisibleForTesting
  public BuddyAllocator(boolean isDirectVal, boolean isMappedVal, int minAllocVal,
      int maxAllocVal, int arenaCount, long maxSizeVal, long defragHeadroom, String mapPath,
      MemoryManager memoryManager, LlapDaemonCacheMetrics metrics, String discardMethod) {
    isDirect = isDirectVal;
    isMapped = isMappedVal;
    minAllocation = minAllocVal;
    maxAllocation = maxAllocVal;
    if (isMapped) {
      try {
        cacheDir = Files.createTempDirectory(
            FileSystems.getDefault().getPath(mapPath), "llap-", RWX);
      } catch (IOException ioe) {
        // conf validator already checks this, so it will never trigger usually
        throw new AssertionError("Configured mmap directory should be writable", ioe);
      }
    } else {
      cacheDir = null;
    }

    arenaSize = validateAndDetermineArenaSize(arenaCount, maxSizeVal);
    maxSize = validateAndDetermineMaxSize(maxSizeVal);
    memoryManager.updateMaxSize(determineMaxMmSize(defragHeadroom, maxSize));

    minAllocLog2 = 31 - Integer.numberOfLeadingZeros(minAllocation);
    maxAllocLog2 = 31 - Integer.numberOfLeadingZeros(maxAllocation);
    arenaSizeLog2 = 63 - Long.numberOfLeadingZeros(arenaSize);
    maxArenas = (int)(maxSize / arenaSize);
    arenas = new Arena[maxArenas];
    for (int i = 0; i < maxArenas; ++i) {
      arenas[i] = new Arena();
    }
    Arena firstArena = arenas[0];
    firstArena.init(0);
    allocatedArenas.set(1);
    this.memoryManager = memoryManager;
    defragCounters = new AtomicLong[maxAllocLog2 - minAllocLog2 + 1];
    for (int i = 0; i < defragCounters.length; ++i) {
      defragCounters[i] = new AtomicLong(0);
    }
    this.metrics = metrics;
    metrics.incrAllocatedArena();
    boolean isBoth = null == discardMethod || "both".equalsIgnoreCase(discardMethod);
    doUseFreeListDiscard = isBoth || "freelist".equalsIgnoreCase(discardMethod);
    doUseBruteDiscard = isBoth || "brute".equalsIgnoreCase(discardMethod);
    ctxPool = new FixedSizedObjectPool<DiscardContext>(32,
        new FixedSizedObjectPool.PoolObjectHelper<DiscardContext>() {
          @Override
          public DiscardContext create() {
            return new DiscardContext();
          }
          @Override
          public void resetBeforeOffer(DiscardContext t) {
          }
      });
 }

  public long determineMaxMmSize(long defragHeadroom, long maxMmSize) {
    if (defragHeadroom > 0) {
      long maxHeadroom = (long) Math.floor(maxSize * MAX_DEFRAG_HEADROOM_FRACTION);
      defragHeadroom = Math.min(maxHeadroom, defragHeadroom);
      LlapIoImpl.LOG.info("Leaving " + defragHeadroom + " of defragmentation headroom");
      maxMmSize -= defragHeadroom;
    }
    return maxMmSize;
  }

  public long validateAndDetermineMaxSize(long maxSizeVal) {
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
    return maxSizeVal;
  }

  public int validateAndDetermineArenaSize(int arenaCount, long maxSizeVal) {
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
    return (int)arenaSizeVal;
  }

  @Override
  public void allocateMultiple(MemoryBuffer[] dest, int size)
      throws AllocatorOutOfMemoryException {
    allocateMultiple(dest, size, null);
  }

  // TODO: would it make sense to return buffers asynchronously?
  @Override
  public void allocateMultiple(MemoryBuffer[] dest, int size, BufferObjectFactory factory)
      throws AllocatorOutOfMemoryException {
    assert size > 0 : "size is " + size;
    if (size > maxAllocation) {
      throw new RuntimeException("Trying to allocate " + size + "; max is " + maxAllocation);
    }
    int freeListIx = determineFreeListForAllocation(size);
    int allocLog2 = freeListIx + minAllocLog2;
    int allocationSize = 1 << allocLog2;

    // If using async, we could also reserve one by one.
    memoryManager.reserveMemory(dest.length << allocLog2);
    for (int i = 0; i < dest.length; ++i) {
      if (dest[i] != null) continue;
      // Note: this is backward compat only. Should be removed with createUnallocated.
      dest[i] = factory != null ? factory.create() : createUnallocated();
    }

    // First try to quickly lock some of the correct-sized free lists and allocate from them.
    int arenaCount = allocatedArenas.get();
    if (arenaCount < 0) {
      arenaCount = -arenaCount - 1; // Next arena is being allocated.
    }

    // Note: we might want to be smarter if threadId-s are low and there more arenas than threads.
    long threadId = arenaCount > 1 ? Thread.currentThread().getId() : 0;
    int destAllocIx = allocateFast(dest, null, 0, dest.length,
        freeListIx, allocationSize, (int)(threadId % arenaCount), arenaCount);
    if (destAllocIx == dest.length) return;

    // We called reserveMemory so we know that there's memory waiting for us somewhere.
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
    int attempt = 0;
    boolean isFailed = false;
    int memoryForceReleased = 0;
    try {
      int discardFailed = 0;
      while (true) {
        // Try to split bigger blocks.
        int startArenaIx = (int)((threadId + attempt) % arenaCount);
        destAllocIx = allocateWithSplit(dest, null, destAllocIx, dest.length,
            freeListIx, allocationSize, startArenaIx, arenaCount, -1);
        if (destAllocIx == dest.length) return;

        if (attempt == 0) {
          // Try to allocate memory if we haven't allocated all the way to maxSize yet; very rare.
          destAllocIx = allocateWithExpand(
              dest, destAllocIx, freeListIx, allocationSize, arenaCount);
          if (destAllocIx == dest.length) return;
        }

        // Try to force-evict the fragments of the requisite size.
        boolean hasDiscardedAny = false;
        DiscardContext ctx = ctxPool.take();
        try {
          // Brute force may discard up to twice as many buffers.
          int maxListSize = 1 << (doUseBruteDiscard ? freeListIx : (freeListIx - 1));
          int requiredBlocks = dest.length - destAllocIx;
          ctx.init(maxListSize, requiredBlocks);
          // First, try to use the blocks of half size in every arena.
          if (doUseFreeListDiscard && freeListIx > 0) {
            discardBlocksBasedOnFreeLists(freeListIx, startArenaIx, arenaCount, ctx);
            memoryForceReleased += ctx.memoryReleased;
            hasDiscardedAny = ctx.resultCount > 0;
            destAllocIx = allocateFromDiscardResult(
                dest, destAllocIx, freeListIx, allocationSize, ctx);
            if (destAllocIx == dest.length) return;
          }
          // Then, try the brute force search for something to throw away.
          if (doUseBruteDiscard) {
            ctx.resetResults();
            discardBlocksBruteForce(freeListIx, startArenaIx, arenaCount, ctx);
            memoryForceReleased += ctx.memoryReleased;
            hasDiscardedAny = hasDiscardedAny || (ctx.resultCount > 0);
            destAllocIx = allocateFromDiscardResult(
                dest, destAllocIx, freeListIx, allocationSize, ctx);

            if (destAllocIx == dest.length) return;
          }
        } finally {
          ctxPool.offer(ctx);
        }
        if (hasDiscardedAny) {
          discardFailed = 0;
        } else if (++discardFailed > MAX_DISCARD_ATTEMPTS) {
          String msg = "Failed to allocate " + size + "; at " + destAllocIx + " out of "
              + dest.length + " (entire cache is fragmented and locked, or an internal issue)";
          logOomErrorMessage(msg);
          isFailed = true;
          throw new AllocatorOutOfMemoryException(msg);
        }
        ++attempt;
      }
    } finally {
      memoryManager.releaseMemory(memoryForceReleased);
      if (!isFailed && attempt >= LOG_DISCARD_ATTEMPTS) {
        LlapIoImpl.LOG.info("Allocation of " + dest.length + " buffers of size " + size + " took "
            + attempt + " attempts to free enough memory; force-released " + memoryForceReleased);
      }
    }
  }

  /** The context for the forced eviction of buffers. */
  private static final class DiscardContext {
    long[] results;
    int resultCount;
    int memoryReleased;

    /**
     * The headers for blocks we've either locked to move (if allocated), or have taken out
     * of the free lists (if not) so that nobody allocates them while we are freeing space.
     * All the headers will be from the arena currently being processed.
     */
    int[] victimHeaders;
    int victimCount; // The count of the elements of the above that are set.

    /**
     * List-based: the base free buffers that will be paired with the space freed from
     * victimHeaders to create the buffers of allocation size.
     * Brute force: the buffers (that do not exist as separate buffers) composed of victimHeaders
     * buffers; the future result buffers.
     * All the headers will be from the arena currently being processed.
     */
    int[] baseHeaders;
    int baseCount; // The count of the elements of the above that are set.

    /**
     * How many more results (or base headers) do we need to find?
     * This object is reused between arenas; this is the only counter that is preserved.
     */
    int remainingToFind;

    /** The headers from abandoned moved attempts that cannot yet be returned to the
     * free lists, or unlocked due to some lock being held and deadlock potential. */
    int[] abandonedHeaders;
    int abandonedCount;

    void init(int headersPerOneReq, int reqCount) {
      resetResults();
      remainingToFind = reqCount;
      if (results == null || results.length < reqCount) {
        results = new long[reqCount];
        baseHeaders = new int[reqCount];
      }
      int maxVictimCount = headersPerOneReq * reqCount;
      if (victimHeaders == null || victimHeaders.length < maxVictimCount) {
        victimHeaders = new int[maxVictimCount];
      }
    }

    void resetResults() {
      resetBetweenArenas();
      resultCount = memoryReleased = 0;
    }

    void resetBetweenArenas() {
      // Reset everything for the next arena; assume everything has been cleaned.
      victimCount = baseCount = abandonedCount = 0;
    }

    public void addResult(int arenaIx, int freeHeaderIx) {
      results[resultCount] = makeIntPair(arenaIx, freeHeaderIx);
      ++resultCount;
    }

    public void addBaseHeader(int headerIx) {
      baseHeaders[baseCount] = headerIx;
      ++baseCount;
      --remainingToFind;
    }

    @Override
    public String toString() {
      return "[victimHeaders=" + Arrays.toString(victimHeaders) + ", victimCount="
          + victimCount + ", baseHeaders=" + Arrays.toString(baseHeaders) + ", baseCount="
          + baseCount + ", remainingToFind=" + remainingToFind  + "]";
    }
  }

  private void discardBlocksBasedOnFreeLists(
      int freeListIx, int startArenaIx, int arenaCount, DiscardContext ctx) {
    defragCounters[freeListIx].incrementAndGet();
    // The free list level the blocks from which we need to merge.
    final int mergeListIx = freeListIx - 1;

    // Try to allocate using base-buffer approach from each arena.
    int arenaIx = startArenaIx;
    do {
      Arena arena = arenas[arenaIx];
      // Reserve blocks in this arena that would empty the sections of requisite size.
      arena.reserveDiscardBlocksBasedOnFreeList(mergeListIx, ctx);
      // Discard the blocks.
      discardFromCtxBasedOnFreeList(arena, ctx, freeListIx);

      if (ctx.remainingToFind == 0) return; // Reserved as much as we needed.
      ctx.resetBetweenArenas();
      arenaIx = getNextIx(arenaIx, arenaCount, 1);
    } while (arenaIx != startArenaIx);
  }

  private void discardBlocksBruteForce(
      int freeListIx, int startArenaIx, int arenaCount, DiscardContext ctx) {
    // We are going to use this counter as a pseudo-random number for the start of the search.
    // This is to avoid churning at the beginning of the arena all the time.
    long counter = defragCounters[freeListIx].incrementAndGet();
    // How many blocks of this size comprise an arena.
    int positionsPerArena = 1 << (arenaSizeLog2 - (minAllocLog2 + freeListIx));
    // Compute the pseudo-random position from the above, then derive the actual header.
    int startHeaderIx = ((int) (counter % positionsPerArena)) << freeListIx;

    // Try to allocate using brute force approach from each arena.
    int arenaIx = startArenaIx;
    do {
      Arena arena = arenas[arenaIx];
      // Reserve blocks in this arena that would empty the sections of requisite size.
      arena.reserveDiscardBruteForce(freeListIx, ctx, startHeaderIx);
      // Discard the blocks.
      discardFromCtxBruteForce(arena, ctx, freeListIx);

      if (ctx.remainingToFind == 0) return; // Reserved as much as we needed.
      ctx.resetBetweenArenas();
      arenaIx = getNextIx(arenaIx, arenaCount, 1);
    } while (arenaIx != startArenaIx);
  }

  /**
   * Frees up memory by deallocating based on base and victim buffers in MoveContext.
   * @param freeListIx The list for which the blocks are being merged.
   */
  private void discardFromCtxBasedOnFreeList(Arena arena, DiscardContext ctx, int freeListIx) {
    // Discard all the locked blocks.
    discardAllBuffersFromCtx(arena, ctx);
    // Finalize the headers.
    for (int baseIx = ctx.baseCount - 1; baseIx >= 0; --baseIx) {
      int baseHeaderIx = ctx.baseHeaders[baseIx];
      int minHeaderIx = Math.min(baseHeaderIx, getBuddyHeaderIx(freeListIx - 1, baseHeaderIx));
      finalizeDiscardResult(arena, ctx, freeListIx, minHeaderIx);
    }
  }

  /**
   * Frees up memory by deallocating based on base and victim buffers in MoveContext.
   * @param freeListIx The list for which the blocks are being merged.
   */
  private void discardFromCtxBruteForce(Arena arena, DiscardContext ctx, int freeListIx) {
    // Discard all the locked blocks.
    discardAllBuffersFromCtx(arena, ctx);
    // Finalize the headers.
    for (int baseIx = ctx.baseCount - 1; baseIx >= 0; --baseIx) {
      finalizeDiscardResult(arena, ctx, freeListIx, ctx.baseHeaders[baseIx]);
    }
  }

  /**
   * Sets the headers correctly for a newly-freed buffer after discarding stuff.
   */
  private void finalizeDiscardResult(
      Arena arena, DiscardContext ctx, int freeListIx, int newlyFreeHeaderIx) {
    int maxHeaderIx = newlyFreeHeaderIx + (1 << freeListIx);
    if (assertsEnabled) {
      arena.checkHeader(newlyFreeHeaderIx, -1, true);
    }
    arena.unsetHeaders(newlyFreeHeaderIx + 1, maxHeaderIx, CasLog.Src.CLEARED_VICTIM);
    // Set the leftmost header of the base and its buddy (that are now being merged).
    arena.setHeaderNoBufAlloc(newlyFreeHeaderIx, freeListIx, CasLog.Src.NEWLY_CLEARED);
    ctx.addResult(arena.arenaIx, newlyFreeHeaderIx);
  }

  /**
   * Discards all the victim buffers in the context.
   */
  private void discardAllBuffersFromCtx(Arena arena, DiscardContext ctx) {
    for (int victimIx = 0; victimIx < ctx.victimCount; ++victimIx) {
      int victimHeaderIx = ctx.victimHeaders[victimIx];
      // Note: no location check here; the buffer is always locked for move.
      LlapAllocatorBuffer buf = arena.buffers[victimHeaderIx];
      if (buf == null) continue;
      if (assertsEnabled) {
        arena.checkHeader(victimHeaderIx, -1, true);
        byte header = arena.headers[victimHeaderIx];
        assertBufferLooksValid(freeListFromHeader(header), buf, arena.arenaIx, victimHeaderIx);
      }
      // We do not modify the header here; the caller will use this space.
      arena.buffers[victimHeaderIx] = null;
      long memUsage = buf.getMemoryUsage();
      Boolean result = buf.endDiscard();
      if (result == null) {
        ctx.memoryReleased += memUsage; // We have essentially deallocated this.
      } else if (result) {
        // There was a parallel deallocate; it didn't account for the memory.
        memoryManager.releaseMemory(memUsage);
      } else {
        // There was a parallel cache eviction - the evictor is accounting for the memory.
      }
    }
  }

  /**
   * Unlocks the buffer after the discard has been abandoned.
   */
  private void cancelDiscard(LlapAllocatorBuffer buf, int arenaIx, int headerIx) {
    Boolean result = buf.cancelDiscard();
    if (result == null) return;
    // If the result is not null, the buffer was evicted during the move.
    if (result) {
      long memUsage = buf.getMemoryUsage(); // Release memory - simple deallocation.
      arenas[arenaIx].deallocate(buf, true);
      memoryManager.releaseMemory(memUsage);
    } else {
      arenas[arenaIx].deallocate(buf, true); // No need to release memory - cache eviction.
    }
  }

  /**
   * Tries to allocate destCount - destIx blocks, using best-effort fast allocation.
   * @param dest Option 1 - memory allocated is stored in these buffers.
   * @param destHeaders Option 2 - memory allocated is reserved and headers returned via this.
   * @param destIx The start index in either array where allocations are to be saved.
   * @param destCount The end index in either array where allocations are to be saved.
   * @param freeListIx The free list from which to allocate.
   * @param allocSize Allocation size.
   * @param startArenaIx From which arena to start allocating.
   * @param arenaCount The active arena count.
   * @return The index in the array until which the memory has been allocated.
   */
  private int allocateFast(MemoryBuffer[] dest, long[] destHeaders, int destIx, int destCount,
      int freeListIx, int allocSize, int startArenaIx, int arenaCount) {
    int index = startArenaIx;
    do {
      int newDestIx = arenas[index].allocateFast(
          freeListIx, dest, destHeaders, destIx, destCount, allocSize);
      if (newDestIx == destCount) return newDestIx;
      assert newDestIx != -1;
      destIx = newDestIx;
      index = getNextIx(index, arenaCount, 1);
    } while (index != startArenaIx);
    return destIx;
  }

 /**
   * Tries to allocate destCount - destIx blocks by allocating new arenas, if needed. Same args
   * as allocateFast, except the allocations start at arenaCount (the first unallocated arena).
   */
  private int allocateWithExpand(MemoryBuffer[] dest, int destIx,
      int freeListIx, int allocSize, int arenaCount) {
    for (int arenaIx = arenaCount; arenaIx < arenas.length; ++arenaIx) {
      destIx = arenas[arenaIx].allocateWithExpand(
          arenaIx, freeListIx, dest, destIx, allocSize);
      if (destIx == dest.length) return destIx;
    }
    return destIx;
  }

  /**
   * Tries to allocate destCount - destIx blocks, waiting for locks and splitting the larger
   * blocks if the correct sized blocks are not available. Args the same as allocateFast.
   */
  private int allocateWithSplit(MemoryBuffer[] dest, long[] destHeaders, int destIx, int destCount,
      int freeListIx, int allocSize, int startArenaIx, int arenaCount, int maxSplitFreeListIx) {
    int arenaIx = startArenaIx;
    do {
      int newDestIx = arenas[arenaIx].allocateWithSplit(
          freeListIx, dest, destHeaders, destIx, destCount, allocSize, maxSplitFreeListIx);
      if (newDestIx == destCount) return newDestIx;
      assert newDestIx != -1;
      destIx = newDestIx;
      arenaIx = getNextIx(arenaIx, arenaCount, 1);
    } while (arenaIx != startArenaIx);
    return destIx;
  }

  /**
   * Tries to allocate destCount - destIx blocks after the forced eviction of some other buffers.
   * Args the same as allocateFast.
   */
  private int allocateFromDiscardResult(MemoryBuffer[] dest, int destAllocIx,
      int freeListIx, int allocationSize, DiscardContext discardResult) {
    for (int i = 0; i < discardResult.resultCount; ++i) {
      long result = discardResult.results[i];
      destAllocIx = arenas[getFirstInt(result)].allocateFromDiscard(
          dest, destAllocIx, getSecondInt(result), freeListIx, allocationSize);
    }
    return destAllocIx;
  }

  private void logOomErrorMessage(String msg) {
    if (!oomLogging) return;
    while (true) {
      long time = System.nanoTime();
      long lastTime = lastLog.get();
      // Magic value usage is invalid with nanoTime, so once in a 1000 years we may log extra.
      boolean shouldLog = (lastTime == -1 || (time - lastTime) > MAX_DUMP_INTERVAL_NS);
      if (shouldLog && !lastLog.compareAndSet(lastTime, time)) {
        continue;
      }
      if (shouldLog) {
        LlapIoImpl.LOG.error(msg + debugDumpForOomInternal());
      } else {
        LlapIoImpl.LOG.error(msg);
      }
      return;
    }
  }

    /**
   * Arbitrarily, we start getting the state from Allocator. Allocator calls MM which calls
   * the policies that call the eviction dispatcher that calls the caches. See init - these all
   * are connected in a cycle, so we need to make sure the who-calls-whom order is definite.
   */
  @Override
  public void debugDumpShort(StringBuilder sb) {
    memoryManager.debugDumpShort(sb);
    sb.append("\nDefrag counters: ");
    for (int i = 0; i < defragCounters.length; ++i) {
      sb.append(defragCounters[i].get()).append(", ");
    }
    sb.append("\nAllocator state:");
    int unallocCount = 0, fullCount = 0;
    long totalFree = 0;
    for (Arena arena : arenas) {
      Integer result = arena.debugDumpShort(sb);
      if (result == null) {
        ++unallocCount;
      } else if (result == 0) {
        ++fullCount;
      } else {
        totalFree += result;
      }
    }
    sb.append("\nTotal available and allocated: ").append(totalFree).append(
        "; unallocated arenas: ").append(unallocCount).append(
        "; full arenas ").append(fullCount);
    sb.append("\n");
  }

  @Override
  public void deallocate(MemoryBuffer buffer) {
    LlapAllocatorBuffer buf = (LlapAllocatorBuffer)buffer;
    int arenaToRelease = buf.invalidateAndRelease();
    if (arenaToRelease < 0) return; // The block is being moved; the move will release memory.
    long memUsage = buf.getMemoryUsage();
    arenas[arenaToRelease].deallocate(buf, false);
    memoryManager.releaseMemory(memUsage);
  }

  @Override
  public void deallocateEvicted(MemoryBuffer buffer) {
    LlapAllocatorBuffer buf = (LlapAllocatorBuffer)buffer;
    assert buf.isInvalid();
    int arenaToRelease = buf.releaseInvalidated();
    if (arenaToRelease < 0) return; // The block is being moved; the move will release memory.
    arenas[arenaToRelease].deallocate(buf, false);
    // Note: for deallocateEvicted, we do not release the memory to memManager; it may
    // happen that the evictor tries to use the allowance before the move finishes.
    // Retrying/more defrag should take care of that.
  }

  @Override
  public boolean isDirectAlloc() {
    return isDirect;
  }

  private ByteBuffer preallocateArenaBuffer(int arenaSize) {
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
    private static final int FAILED_TO_RESERVE = Integer.MAX_VALUE;

    private int arenaIx;
    private ByteBuffer data;
    // Avoid storing headers with data since we expect binary size allocations.
    // Each headers[i] is a "virtual" byte at i * minAllocation.
    private LlapAllocatorBuffer[] buffers;
    // The TS rule for headers is - a header and buffer array element for some freeList
    // can only be modified if the corresponding freeList lock is held.
    private byte[] headers; // Free list indices of each unallocated block, for quick lookup.
    private FreeList[] freeLists;

    void init(int arenaIx) {
      this.arenaIx = arenaIx;
      try {
        data = preallocateArenaBuffer(arenaSize);
      } catch (OutOfMemoryError oom) {
        throw new OutOfMemoryError("Cannot allocate " + arenaSize + " bytes: " + oom.getMessage()
            + "; make sure your xmx and process size are set correctly.");
      }
      int maxMinAllocs = 1 << (arenaSizeLog2 - minAllocLog2);
      buffers = new LlapAllocatorBuffer[maxMinAllocs];
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
        setHeaderFree(headerIndex, allocLog2Diff, CasLog.Src.CTOR);
        data.putInt(offset, (i == 0) ? -1 : (headerIndex - headerStep));
        data.putInt(offset + 4, (i == maxMaxAllocs - 1) ? -1 : (headerIndex + headerStep));
        headerIndex += headerStep;
      }
    }

    public void checkHeader(int headerIx, int freeListIx, boolean isLocked) {
      checkHeaderByte(arenaIx, headerIx, freeListIx, isLocked, headers[headerIx]);
    }

    /**
     * Reserves the blocks to use to merge into larger blocks.
     * @param freeListIx The free list to reserve for.
     * @param startHeaderIx The header index at which to start looking (to avoid churn at 0).
     */
    public void reserveDiscardBruteForce(int freeListIx, DiscardContext ctx, int startHeaderIx) {
      if (data == null) return; // not allocated yet
      int headerStep = 1 << freeListIx;
      int headerIx = startHeaderIx;
      do {
        long reserveResult = reserveBlockContents(
            freeListIx, headerIx, ctx.victimHeaders, ctx.victimCount, true);
        int reservedCount = getFirstInt(reserveResult), moveSize = getSecondInt(reserveResult);
        if (moveSize == FAILED_TO_RESERVE) {
          for (int i = ctx.victimCount; i < ctx.victimCount + reservedCount; ++i) {
            abandonOneHeaderBeingMoved(ctx.victimHeaders[i], CasLog.Src.ABANDON_MOVE);
          }
        } else {
          ctx.victimCount += reservedCount;
          ctx.addBaseHeader(headerIx);
        }
        headerIx = getNextIx(headerIx, headers.length, headerStep);
      } while (ctx.remainingToFind > 0 && headerIx != startHeaderIx);
    }

    /**
     * Reserves the blocks to use to merge into larger blocks.
     * @param mergeListIx The free list to reserve base blocks from.
     */
    public void reserveDiscardBlocksBasedOnFreeList(int mergeListIx, DiscardContext ctx) {
      if (data == null) return; // not allocated yet
      FreeList freeList = freeLists[mergeListIx];
      freeList.lock.lock();
      try {
        int freeHeaderIx = freeList.listHead;
        while (freeHeaderIx >= 0) {
          boolean reserved = false;
          if (ctx.remainingToFind > 0) {
            int headerToFreeIx = getBuddyHeaderIx(mergeListIx, freeHeaderIx);
            long reserveResult = reserveBlockContents(mergeListIx, headerToFreeIx,
                ctx.victimHeaders, ctx.victimCount, true);
            int reservedCount = getFirstInt(reserveResult), moveSize = getSecondInt(reserveResult);
            reserved = (moveSize != FAILED_TO_RESERVE);
            if (!reserved) {
              // We cannot abandon the attempt here; the concurrent operations might have released
              // all the buffer comprising our buddy block, necessitating a merge into a higher
              // list. That may deadlock with another thread locking its own victims (one can only
              // take list locks separately, or moving DOWN). The alternative would be to release
              // the free list lock before reserving, however iterating the list that way is
              // difficult (we'd have to keep track of things on the main path to avoid re-trying
              // the same headers repeatedly - we'd rather keep track of extra things on failure).
              prepareAbandonUnfinishedMoveAttempt(ctx, reservedCount);
            } else {
              ctx.victimCount += reservedCount;
              ctx.addBaseHeader(freeHeaderIx);
            }
          }
          int nextFreeHeaderIx = getNextFreeListItem(offsetFromHeaderIndex(freeHeaderIx));
          if (reserved) {
            removeBlockFromFreeList(freeList, freeHeaderIx, mergeListIx);
            if (assertsEnabled) {
              checkHeader(freeHeaderIx, mergeListIx, false);
            }
            setHeaderNoBufAlloc(freeHeaderIx, mergeListIx, CasLog.Src.NEW_BASE);
          }
          if (ctx.remainingToFind == 0) break;
          freeHeaderIx = nextFreeHeaderIx;
        }
      } finally {
        freeList.lock.unlock();
      }
      // See the above. Release the headers after unlocking.
      for (int i = 0; i < ctx.abandonedCount; ++i) {
        abandonOneHeaderBeingMoved(ctx.abandonedHeaders[i], CasLog.Src.ABANDON_AT_END);
      }
      ctx.abandonedCount = 0;
    }

    /**
     * Saves the victim headers from a failed reserve into a separate array into the context.
     * See the comment at the call site; this is to prevent deadlocks.
     * @param startIx the victim count before this reserve was started.
     */
    private void prepareAbandonUnfinishedMoveAttempt(DiscardContext ctx, int count) {
      if (count == 0) return; // Nothing to do.
      int startIx = ctx.victimCount, start;
      if (ctx.abandonedHeaders == null) {
        start = 0;
        ctx.abandonedHeaders = new int[count];
      } else {
        start = ctx.abandonedCount;
        int newLen = start + count;
        if (newLen > ctx.abandonedHeaders.length) {
          ctx.abandonedHeaders = Arrays.copyOf(ctx.abandonedHeaders, newLen);
        }
      }
      System.arraycopy(ctx.victimHeaders, startIx, ctx.abandonedHeaders, start, count);
      ctx.abandonedCount += count;
      ctx.victimCount = startIx;
    }

    /**
     * Reserve all the contents of a particular block to merge them together.
     * @param freeListIx The list to which the hypothetical block belongs.
     * @param freeHeaderIx The header of the base block.
     */
    private long reserveBlockContents(int freeListIx, int headerToFreeIx,
        int[] victimHeaders, int victimsOffset, boolean isDiscard) {
      // Try opportunistically for the common case - the same-sized, allocated buddy.
      if (enableDefragShortcut) {
        LlapAllocatorBuffer buffer = buffers[headerToFreeIx];
        byte header = headers[headerToFreeIx];
        if (buffer != null && freeListFromHeader(header) == freeListIx) {
          // Double-check the header under lock.
          FreeList freeList = freeLists[freeListIx];
          freeList.lock.lock();
          try {
            // Noone can take this buffer out and thus change the level after we lock, and if
            // they take it out before we lock, then we will fail to lock (same as
            // prepareOneHeaderForMove).
            if (headers[headerToFreeIx] == header
                && buffer.startMoveOrDiscard(arenaIx, headerToFreeIx, isDiscard)) {
              if (assertsEnabled) {
                assertBufferLooksValid(freeListIx, buffer, arenaIx, headerToFreeIx);
                CasLog.logMove(arenaIx, headerToFreeIx, System.identityHashCode(buffer));
              }
              victimHeaders[victimsOffset] = headerToFreeIx;
              return makeIntPair(1, buffer.allocSize);
            }
          } finally {
            freeList.lock.unlock();
          }
          // We don't bail on failure - try in detail below.
        }
      }
      // Examine the buddy block and its sub-blocks in detail.
      long[] stack = new long[freeListIx + 1]; // Can never have more than this in elements.
      int stackSize = 1;
      // Seed with the buddy of this block (so the first iteration would target this block).
      stack[0] = makeIntPair(freeListIx, getBuddyHeaderIx(freeListIx, headerToFreeIx));
      int victimCount = 0;
      int totalMoveSize = 0;
      // We traverse the leaf nodes of the tree. The stack entries indicate the existing leaf
      // nodes that we need to see siblings for, and sibling levels.
      while (stackSize > 0) {
        --stackSize;
        long next = stack[stackSize];
        int listLevel = getFirstInt(next); // This is not an actual list; see intermList.
        int sourceHeaderIx = getSecondInt(next);
        // Find the buddy of the header at list level. We don't know what list it is actually in.
        int levelBuddyHeaderIx = getBuddyHeaderIx(listLevel, sourceHeaderIx);
        // First, handle the actual thing we found.
        long result = prepareOneHeaderForMove(levelBuddyHeaderIx, isDiscard, freeListIx);
        if (result == -1) {
          // We have failed to reserve a single header. Do not undo the previous ones here,
          // the caller has to handle this to avoid races.
          return makeIntPair(victimCount, FAILED_TO_RESERVE);
        }
        int allocSize = getFirstInt(result);
        totalMoveSize += allocSize;
        victimHeaders[victimsOffset + victimCount] = levelBuddyHeaderIx;
        ++victimCount;
        // Explaining this would really require a picture. Basically if the level is lower than
        // our level, that means (imagine a tree) we are on the leftmost leaf node of the sub-tree
        // under our sibling in the tree. So we'd need to look at the buddies of that leftmost leaf
        // block on all the intermediate levels (aka all intermediate levels of the tree between
        // this guy and our sibling). Including its own buddy on its own level. And so on for every
        // sub-tree where our buddy is not on the same level as us (i.e. does not cover the entire
        // sub-tree).
        int actualBuddyListIx = getSecondInt(result);
        for (int intermListIx = listLevel - 1; intermListIx >= actualBuddyListIx; --intermListIx) {
          stack[stackSize++] = makeIntPair(intermListIx, levelBuddyHeaderIx);
        }
      }
      return makeIntPair(victimCount, totalMoveSize);
    }

    /**
     * Abandons the move attempt for a single header that may be free or allocated.
     */
    private void abandonOneHeaderBeingMoved(int headerIx, CasLog.Src src) {
      byte header = headers[headerIx];
      int freeListIx = freeListFromHeader(header);
      if ((header & 1) != 1) failWithLog("Victim header not in use");
      LlapAllocatorBuffer buf = buffers[headerIx];
      if (buf != null) {
        // Note: no location check; the buffer is always locked for move here.
        if (assertsEnabled) {
          assertBufferLooksValid(freeListIx, buf, arenaIx, headerIx);
        }
        cancelDiscard(buf, arenaIx, headerIx);
      } else {
        if (assertsEnabled) {
          checkHeader(headerIx, -1, true);
        }
        addToFreeListWithMerge(headerIx, freeListIx, null, src);
      }
    }

    /**
     * Prepares victimHeaderIx header to be moved - locks if it's allocated, takes out of
     * the free list if not.
     * @return the list level to which the header belongs if this succeeded, -1 if not.
     */
    private long prepareOneHeaderForMove(int victimHeaderIx, boolean isDiscard, int maxListIx) {
      byte header = headers[victimHeaderIx];
      if (header == 0) return -1;
      int freeListIx = freeListFromHeader(header);
      if (freeListIx > maxListIx) {
        // This can only come from a brute force discard; for now we don't discard blocks larger
        // than the target block. We could discard it and add remainder to free lists.
        // By definition if we are fragmented there should be a smaller buffer somewhere.
        return -1;
      }
      if (buffers[victimHeaderIx] == null && (header & 1) == 1) {
        return -1; // There's no buffer and another move is reserving this.
      }
      FreeList freeList = freeLists[freeListIx];
      freeList.lock.lock();
      try {
        if (headers[victimHeaderIx] != header) {
          // We bail if there are any changes. Note that we don't care about ABA here - all the
          // stuff on the left has been taken out already so noone can touch it, and all the stuff
          // on the right is yet to be seen so we don't care if they changed with this - if it's
          // in the same free list, the processing sequence will remain the same going right.
          return -1;
        }
        LlapAllocatorBuffer buffer = buffers[victimHeaderIx];
         if (buffer == null && (header & 1) == 1) {
          return -1; // The only ABA problem we care about. Ok to have another buffer in there;
                     // not ok to have a location locked by someone else.
        }
        int allocSize = 0;
        if (buffer != null) {
          // The buffer can only be removed after the removed flag has been set. If we are able to
          // lock it here, noone can set the removed flag and thus remove it. That would also mean
          // that the header is not free, and noone will touch the header either.
          if (!buffer.startMoveOrDiscard(arenaIx, victimHeaderIx, isDiscard)) {
            return -1;
          }
          CasLog.logMove(arenaIx, victimHeaderIx, System.identityHashCode(buffer));
          allocSize = allocSizeFromFreeList(freeListIx);
        } else {
          // Take the empty buffer out of the free list.
          setHeaderNoBufAlloc(victimHeaderIx, freeListIx, CasLog.Src.EMPTY_V);
          removeBlockFromFreeList(freeList, victimHeaderIx, freeListIx);
        }
        return makeIntPair(allocSize, freeListIx);
      } finally {
        freeList.lock.unlock();
      }
    }

    /** Allocates into an empty block obtained via a forced eviction. Same args as allocateFast. */
    public int allocateFromDiscard(MemoryBuffer[] dest, int destIx,
        int headerIx, int freeListIx, int allocationSize) {
      LlapAllocatorBuffer buffer = (LlapAllocatorBuffer)dest[destIx];
      initializeNewlyAllocated(buffer, allocationSize, headerIx, offsetFromHeaderIndex(headerIx));
      if (assertsEnabled) {
        checkHeader(headerIx, freeListIx, true);
      }
      setHeaderAlloc(headerIx, freeListIx, buffer, CasLog.Src.ALLOC_DEFRAG);
      return destIx + 1;
    }

    /** Sets the header at an index to refer to an allocated buffer. */
    private void setHeaderAlloc(int headerIx, int freeListIx, LlapAllocatorBuffer alloc,
        CasLog.Src src) {
      assert alloc != null;
      headers[headerIx] = makeHeader(freeListIx, true);
      buffers[headerIx] = alloc;
      CasLog.logSet(src, arenaIx, headerIx, System.identityHashCode(alloc));
    }

    /** Sets the header at an index to refer to free space in a certain free list. */
    private void setHeaderFree(int headerIndex, int freeListIx, CasLog.Src src) {
      headers[headerIndex] = makeHeader(freeListIx, false);
      buffers[headerIndex] = null;
      CasLog.logSetFree(src, arenaIx, headerIndex, allocSizeFromFreeList(freeListIx));
    }

    /** Sets the header at an index to refer to some space in use, without an allocation. */
    private void setHeaderNoBufAlloc(int headerIndex, int freeListIx, CasLog.Src src) {
      headers[headerIndex] = makeHeader(freeListIx, true);
      CasLog.logSetNb(src, arenaIx, headerIndex, allocSizeFromFreeList(freeListIx));
    }

    /** Unsets the header at an index (meaning this does not refer to a buffer). */
    private void unsetHeader(int headerIndex, CasLog.Src src) {
      headers[headerIndex] = 0;
      CasLog.logUnset(src, arenaIx, headerIndex, headerIndex);
    }

    /** Unsets the headers (meaning this does not refer to a buffer). */
    private void unsetHeaders(int fromHeaderIx, int toHeaderIx, CasLog.Src src) {
      Arrays.fill(headers, fromHeaderIx, toHeaderIx, (byte)0);
      CasLog.logUnset(src, arenaIx, fromHeaderIx, toHeaderIx - 1);
    }

    private void debugDump(StringBuilder result) {
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
        boolean isFree = buffers[i] == null;
        result.append("\n  block " + i + " at " + offset + ": size "
            + (1 << (freeListIx + minAllocLog2)) + ", " + (isFree ? "free" : "allocated"));
      }
    }

    public Integer debugDumpShort(StringBuilder result) {
      if (data == null) {
        return null;
      }
      int allocSize = minAllocation;
      int total = 0;
      for (int i = 0; i < freeLists.length; ++i, allocSize <<= 1) {
        FreeList freeList = freeLists[i];
        freeList.lock.lock();
        try {
          int nextHeaderIx = freeList.listHead;
          int count = 0;
          while (nextHeaderIx >= 0) {
            ++count;
            nextHeaderIx = getNextFreeListItem(offsetFromHeaderIndex(nextHeaderIx));
          }
          if (count > 0) {
            if (total == 0) {
              result.append("\nArena with free list lengths by size: ");
            }
            total += (allocSize * count);
            result.append(allocSize).append(" => ").append(count).append(", ");
          }
        } finally {
          freeList.lock.unlock();
        }
      }
      return total;
    }

    private void testDump(StringBuilder result) {
      result.append("{");
      if (data == null) {
        result.append("}, ");
        return;
      }
      // Try to get as consistent view as we can; make copy of the headers.
      byte[] headers = new byte[this.headers.length];
      System.arraycopy(this.headers, 0, headers, 0, headers.length);
      for (int i = 0; i < headers.length; ++i) {
        byte header = headers[i];
        if (header == 0) continue;
        String allocState = ".";
        if (buffers[i] != null) {
          allocState = "*"; // Allocated
        } else if ((header & 1) == 1) {
          allocState = "!"; // Locked for defrag
        }
        int size = 1 << (freeListFromHeader(header) + minAllocLog2);
        result.append("[").append(size).append(allocState).append("@").append(i).append("]");
      }
      result.append("}, ");
    }

    private int allocateFast(int freeListIx, MemoryBuffer[] dest, long[] destHeaders,
        int destIx, int destCount, int allocSize) {
      if (data == null) return -1; // not allocated yet
      FreeList freeList = freeLists[freeListIx];
      if (!freeList.lock.tryLock()) return destIx;
      try {
        return allocateFromFreeListUnderLock(
            freeList, freeListIx, dest, destHeaders, destIx, destCount, allocSize);
      } finally {
        freeList.lock.unlock();
      }
    }

    private int allocateWithSplit(int freeListIx, MemoryBuffer[] dest,
        long[] destHeaders, int destIx, int destCount, int allocSize, int maxSplitFreeListIx) {
      if (data == null) return -1; // not allocated yet
      FreeList freeList = freeLists[freeListIx];
      int remaining = -1;
      freeList.lock.lock();
      try {
        // Try to allocate from target-sized free list, maybe we'll get lucky.
        destIx = allocateFromFreeListUnderLock(
            freeList, freeListIx, dest, destHeaders, destIx, destCount, allocSize);
        remaining = destCount - destIx;
        if (remaining == 0) return destIx;
      } finally {
        freeList.lock.unlock();
      }
      int headerStep = 1 << freeListIx; // Number of headers (smallest blocks) per target block.
      int splitListIx = freeListIx + 1; // Next free list from which we will be splitting.
      // Each iteration of this loop tries to split blocks from one level of the free list into
      // target size blocks; if we cannot satisfy the allocation from the free list containing the
      // blocks of a particular size, we'll try to split yet larger blocks, until we run out.
      if (maxSplitFreeListIx == -1) {
        maxSplitFreeListIx = freeLists.length - 1;
      }
      while (remaining > 0 && splitListIx <= maxSplitFreeListIx) {
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
            for (; toTake > 0; ++destIx, --toTake, headerIx += headerStep, offset += allocSize) {
              if (assertsEnabled) {
                checkHeader(headerIx, -1, false); // Cannot validate the list, it may be unset
              }
              if (dest != null) {
                LlapAllocatorBuffer buffer = (LlapAllocatorBuffer)dest[destIx];
                initializeNewlyAllocated(buffer, allocSize, headerIx, offset);
                setHeaderAlloc(headerIx, freeListIx, buffer, CasLog.Src.ALLOC_SPLIT_BUF);
              } else {
                destHeaders[destIx] = makeIntPair(arenaIx, headerIx);
                setHeaderNoBufAlloc(headerIx, freeListIx, CasLog.Src.ALLOC_SPLIT_DEFRAG);
              }
            }
            lastSplitNextHeader = headerIx; // If anything remains, this is where it starts.
            headerIx = getNextFreeListItem(origOffset);
          }
          replaceListHeadUnderLock(splitList, headerIx, splitListIx); // In the end, update free list head.
        } finally {
          splitList.lock.unlock();
        }
        CasLog.Src src = (dest != null) ? CasLog.Src.SPLIT_AFTER_BUF : CasLog.Src.SPLIT_AFTER_DEFRAG;
        if (remaining == 0) {
          // We have just obtained all we needed by splitting some block; now we need
          // to put the space remaining from that block into lower free lists.
          // We'll put at most one block into each list, since 2 blocks can always be combined
          // to make a larger-level block. Each bit in the remaining target-sized blocks count
          // is one block in a list offset from target-sized list by bit index.
          // We do the merges here too, since the block we just allocated could immediately be
          // moved out, then the resulting free space abandoned.
          int newListIndex = freeListIx;
          while (lastSplitBlocksRemaining > 0) {
            if ((lastSplitBlocksRemaining & 1) == 1) {
              addToFreeListWithMerge(lastSplitNextHeader, newListIndex, null, src);
              lastSplitNextHeader += (1 << newListIndex);
            }
            lastSplitBlocksRemaining >>>= 1;
            ++newListIndex;
            continue;
          }
        }
        ++splitListIx;
      }
      return destIx;
    }

    private void initializeNewlyAllocated(
        LlapAllocatorBuffer buffer, int allocSize, int headerIx, int offset) {
      buffer.initialize(data, offset, allocSize);
      buffer.setNewAllocLocation(arenaIx, headerIx);
    }

    private void replaceListHeadUnderLock(FreeList freeList, int headerIx, int ix) {
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
          return allocateWithSplit(freeListIx, dest, null, ix, dest.length, size, -1);
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
        init(arenaIx);
        boolean isCommited = allocatedArenas.compareAndSet(-arenaCount - 1, arenaCount + 1);
        assert isCommited;
        synchronized (this) {
          this.notifyAll();
        }
        metrics.incrAllocatedArena();
        return allocateWithSplit(freeListIx, dest, null, ix, dest.length, size, -1);
      }
    }

    public int allocateFromFreeListUnderLock(FreeList freeList, int freeListIx,
        MemoryBuffer[] dest, long[] destHeaders, int destIx, int destCount, int allocSize) {
      int current = freeList.listHead;
      assert (dest == null) != (destHeaders == null);
      while (current >= 0 && destIx < destCount) {
        int offset = offsetFromHeaderIndex(current), allocHeaderIx = current;
        current = getNextFreeListItem(offset);
        if (assertsEnabled) {
          checkHeader(allocHeaderIx, freeListIx, false);
        }
        if (dest != null) {
          LlapAllocatorBuffer buffer = (LlapAllocatorBuffer)dest[destIx];
          initializeNewlyAllocated(buffer, allocSize, allocHeaderIx, offset);
          setHeaderAlloc(allocHeaderIx, freeListIx, buffer, CasLog.Src.ALLOC_FREE_BUF);
        } else {
          destHeaders[destIx] = makeIntPair(arenaIx, allocHeaderIx);
          setHeaderNoBufAlloc(allocHeaderIx, freeListIx, CasLog.Src.ALLOC_FREE_DEFRAG);
        }
        ++destIx;
      }
      replaceListHeadUnderLock(freeList, current, freeListIx);
      return destIx;
    }

    private int getPrevFreeListItem(int offset) {
      return data.getInt(offset);
    }

    private int getNextFreeListItem(int offset) {
      return data.getInt(offset + 4);
    }

    public void deallocate(LlapAllocatorBuffer buffer, boolean isAfterMove) {
      assert data != null;
      int pos = buffer.byteBuffer.position();
      // Note: this is called by someone who has ensured the buffer is not going to be moved.
      int headerIx = pos >>> minAllocLog2;
      int freeListIx = freeListFromAllocSize(buffer.allocSize);
      if (assertsEnabled && !isAfterMove) {
        LlapAllocatorBuffer buf = buffers[headerIx];
        if (buf != buffer) {
          failWithLog(arenaIx + ":" + headerIx + " => "
              + toDebugString(buffer) + ", " + toDebugString(buf));
        }
        assertBufferLooksValid(freeListFromHeader(headers[headerIx]), buf, arenaIx, headerIx);
        checkHeader(headerIx, freeListIx, true);
      }
      buffers[headerIx] = null;
      addToFreeListWithMerge(headerIx, freeListIx, buffer, CasLog.Src.DEALLOC);
    }

    private void addToFreeListWithMerge(int headerIx, int freeListIx,
        LlapAllocatorBuffer buffer, CasLog.Src src) {
      while (true) {
        FreeList freeList = freeLists[freeListIx];
        int bHeaderIx = getBuddyHeaderIx(freeListIx, headerIx);
        freeList.lock.lock();
        try {
          if ((freeListIx == freeLists.length - 1)
            || headers[bHeaderIx] != makeHeader(freeListIx, false)) {
            // Buddy block is allocated, or it is on higher level of allocation than we are, or we
            // have reached the top level. Add whatever we have got to the current free list.
            addBlockToFreeListUnderLock(freeList, headerIx, freeListIx);
            setHeaderFree(headerIx, freeListIx, src);
            break;
          }
          // Buddy block is free and in the same free list we have locked. Take it out for merge.
          removeBlockFromFreeList(freeList, bHeaderIx, freeListIx);
          unsetHeader(bHeaderIx, src); // Erase both headers of the blocks to merge.
          unsetHeader(headerIx, src);
        } finally {
          freeList.lock.unlock();
        }
        ++freeListIx;
        headerIx = Math.min(headerIx, bHeaderIx);
      }
    }

    private void addBlockToFreeListUnderLock(FreeList freeList, int headerIx, int ix) {
      CasLog.logAddToList(arenaIx, headerIx, ix, freeList.listHead);
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

    private void removeBlockFromFreeList(FreeList freeList, int headerIx, int ix) {
      int bOffset = offsetFromHeaderIndex(headerIx),
          bpHeaderIx = getPrevFreeListItem(bOffset), bnHeaderIx = getNextFreeListItem(bOffset);
      CasLog.logRemoveFromList(arenaIx, headerIx, ix, freeList.listHead);
      if (freeList.listHead == headerIx) {
        assert bpHeaderIx == -1;
        freeList.listHead = bnHeaderIx;
      }
      // Unnecessary: data.putInt(bOffset, -1); data.putInt(bOffset + 4, -1);
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
    int listHead = -1; // Index of where the buffer is; in minAllocation units (headers array).
  }

  @Override
  @Deprecated
  public MemoryBuffer createUnallocated() {
    return new LlapDataBuffer();
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

  // Various helper methods.
  private static int getBuddyHeaderIx(int freeListIx, int headerIx) {
    return headerIx ^ (1 << freeListIx);
  }

  private static int getNextIx(int ix, int count, int step) {
    ix += step;
    assert ix <= count; // We expect the start at 0 and count divisible by step.
    return ix == count ? 0 : ix;
  }

  private static int freeListFromHeader(byte header) {
    return (header >> 1) - 1;
  }

  private int freeListFromAllocSize(int allocSize) {
    return (31 - Integer.numberOfLeadingZeros(allocSize)) - minAllocLog2;
  }

  private int allocSizeFromFreeList(int freeListIx) {
    return 1 << (freeListIx + minAllocLog2);
  }

  public int offsetFromHeaderIndex(int lastSplitNextHeader) {
    return lastSplitNextHeader << minAllocLog2;
  }

  private static byte makeHeader(int freeListIx, boolean isInUse) {
    return (byte)(((freeListIx + 1) << 1) | (isInUse ? 1 : 0));
  }

  private int determineFreeListForAllocation(int size) {
    int freeListIx = 31 - Integer.numberOfLeadingZeros(size);
    if (size != (1 << freeListIx)) ++freeListIx; // not a power of two, add one more
    return Math.max(freeListIx - minAllocLog2, 0);
  }

  // Utility methods used to store pairs of ints as long.
  private static long makeIntPair(int first, int second) {
    return ((long)first) << 32 | second;
  }
  private static int getFirstInt(long result) {
    return (int) (result >>> 32);
  }
  private static int getSecondInt(long result) {
    return (int) (result & ((1L << 32) - 1));
  }

  // Debug/test related methods.
  private void assertBufferLooksValid(
      int freeListIx, LlapAllocatorBuffer buf, int arenaIx, int headerIx) {
    if (buf.allocSize == allocSizeFromFreeList(freeListIx)) return;
    failWithLog("Race; allocation size " + buf.allocSize + ", not "
        + allocSizeFromFreeList(freeListIx) + " for free list "
        + freeListIx + " at " + arenaIx + ":" + headerIx);
  }

  private static String toDebugString(LlapAllocatorBuffer buffer) {
    return buffer == null ? "null" : buffer.toDebugString();
  }

  private void checkHeaderByte(
      int arenaIx, int headerIx, int freeListIx, boolean isLocked, byte header) {
    if (isLocked != ((header & 1) == 1)) {
      failWithLog("Expected " + arenaIx + ":" + headerIx + " "
          + (isLocked ? "" : "not ") + "locked: " + header);
    }
    if (freeListIx < 0) return;
    if (freeListFromHeader(header) != freeListIx) {
      failWithLog("Expected " + arenaIx + ":" + headerIx + " in list " + freeListIx
          + ": " + freeListFromHeader(header));
    }
  }

  @VisibleForTesting
  void disableDefragShortcutForTest() {
    this.enableDefragShortcut = false;
  }

  @VisibleForTesting
  void setOomLoggingForTest(boolean b) {
    this.oomLogging = b;
  }

  @VisibleForTesting
  String testDump() {
    StringBuilder sb = new StringBuilder();
    for (Arena a : arenas) {
      a.testDump(sb);
    }
    return sb.toString();
  }

  @Override
  public String debugDumpForOom() {
    return "\nALLOCATOR STATE:\n" + debugDumpForOomInternal()
        + "\nPARENT STATE:\n" + memoryManager.debugDumpForOom();
  }

  private String debugDumpForOomInternal() {
    StringBuilder sb = new StringBuilder();
    for (Arena a : arenas) {
      a.debugDump(sb);
    }
    return sb.toString();
  }

  private void failWithLog(String string) {
    CasLog.logError();
    throw new AssertionError(string);
  }

  @VisibleForTesting
  public void dumpTestLog() {
    if (CasLog.casLog != null) {
      CasLog.casLog.dumpLog(true);
    }
  }

  private final static class CasLog {
    // TODO: enable this for production debug, switching between two small buffers?
    private final static CasLog casLog = null; //new CasLog();
    public enum Src {
      NEWLY_CLEARED,
      SPLIT_AFTER_BUF,
      SPLIT_AFTER_DEFRAG,
      ALLOC_SPLIT_DEFRAG,
      ALLOC_SPLIT_BUF,
      ALLOC_DEFRAG,
      EMPTY_V,
      NEW_BASE,
      CTOR,
      MOVE_TO_NESTED,
      MOVE_TO_ALLOC,
      ABANDON_MOVE,
      ABANDON_AT_END,
      ABANDON_BASE,
      CLEARED_BASE,
      CLEARED_VICTIM,
      UNUSABLE_NESTED,
      ABANDON_NESTED,
      DEALLOC,
      ALLOC_FREE_DEFRAG,
      ALLOC_FREE_BUF
    }
    private final int size;
    private final long[] log;
    private final AtomicInteger offset = new AtomicInteger(0);


    public CasLog() {
      size = 50000000;
      log = new long[size];
    }

    public static final int START_MOVE = 0, SET_NB = 1, SET_BUF = 2, SET_FREE = 3,
        ADD_TO_LIST = 4, REMOVE_FROM_LIST = 5, ERROR = 6, UNSET = 7;

    public static void logMove(int arenaIx, int buddyHeaderIx, int identityHashCode) {
      if (casLog == null) return;
      int ix = casLog.offset.addAndGet(3) - 3;
      casLog.log[ix] = makeIntPair(START_MOVE, identityHashCode);
      casLog.log[ix + 1] = Thread.currentThread().getId();
      casLog.log[ix + 2] = makeIntPair(arenaIx, buddyHeaderIx);
    }

    public static void logSetNb(CasLog.Src src, int arenaIx, int headerIndex, int size) {
      if (casLog == null) return;
      int ix = casLog.offset.addAndGet(4) - 4;
      casLog.log[ix] = makeIntPair(SET_NB, src.ordinal());
      casLog.log[ix + 1] = Thread.currentThread().getId();
      casLog.log[ix + 2] = makeIntPair(arenaIx, headerIndex);
      casLog.log[ix + 3] = size;
    }

    public static void logSetFree(CasLog.Src src, int arenaIx, int headerIndex, int size) {
      if (casLog == null) return;
      int ix = casLog.offset.addAndGet(4) - 4;
      casLog.log[ix] = makeIntPair(SET_FREE, src.ordinal());
      casLog.log[ix + 1] = Thread.currentThread().getId();
      casLog.log[ix + 2] = makeIntPair(arenaIx, headerIndex);
      casLog.log[ix + 3] = size;
    }

    public static void logUnset(CasLog.Src src, int arenaIx, int from, int to) {
      if (casLog == null) return;
      if (from > to) return;
      int ix = casLog.offset.addAndGet(4) - 4;
      casLog.log[ix] = makeIntPair(UNSET, src.ordinal());
      casLog.log[ix + 1] = Thread.currentThread().getId();
      casLog.log[ix + 2] = makeIntPair(arenaIx, from);
      casLog.log[ix + 3] = makeIntPair(arenaIx, to);
    }

    public static void logSet(CasLog.Src src, int arenaIx, int headerIndex, int identityHashCode) {
      if (casLog == null) return;
      int ix = casLog.offset.addAndGet(4) - 4;
      casLog.log[ix] = makeIntPair(SET_BUF, src.ordinal());
      casLog.log[ix + 1] = Thread.currentThread().getId();
      casLog.log[ix + 2] = makeIntPair(arenaIx, headerIndex);
      casLog.log[ix + 3] = identityHashCode;
    }

    public static void logRemoveFromList(int arenaIx, int headerIx, int listIx, int listHead) {
      if (casLog == null) return;
      int ix = casLog.offset.addAndGet(4) - 4;
      casLog.log[ix] = makeIntPair(REMOVE_FROM_LIST, listIx);
      casLog.log[ix + 1] = Thread.currentThread().getId();
      casLog.log[ix + 2] = makeIntPair(arenaIx, headerIx);
      casLog.log[ix + 3] = listHead;
    }

    public static void logAddToList(int arenaIx, int headerIx, int listIx, int listHead) {
      if (casLog == null) return;
      int ix = casLog.offset.addAndGet(4) - 4;
      casLog.log[ix] = makeIntPair(ADD_TO_LIST, listIx);
      casLog.log[ix + 1] = Thread.currentThread().getId();
      casLog.log[ix + 2] = makeIntPair(arenaIx, headerIx);
      casLog.log[ix + 3] = listHead;
    }

    public static void logError() {
      if (casLog == null) return;
      int ix = casLog.offset.addAndGet(2) - 2;
      casLog.log[ix] = makeIntPair(ERROR, 0);
      casLog.log[ix + 1] = Thread.currentThread().getId();
    }

    private int dumpOneLine(int ix) {
      int event = getFirstInt(log[ix]);
      switch (event) {
      case START_MOVE: {
        LlapIoImpl.LOG.info(prefix(ix) + " started to move "
            + header(log[ix + 2]) + " " + Integer.toHexString(getSecondInt(log[ix])));
        return ix + 3;
      }
      case SET_NB: {
        LlapIoImpl.LOG.info(prefix(ix) + " " + src(getSecondInt(log[ix])) + " set "
            + header(log[ix + 2]) + " to taken of size " + log[ix + 3]);
        return ix + 4;
      }
      case SET_FREE: {
        LlapIoImpl.LOG.info(prefix(ix) + " " + src(getSecondInt(log[ix])) + " set "
            + header(log[ix + 2]) + " to free of size " + log[ix + 3]);
        return ix + 4;
      }
      case UNSET: {
        LlapIoImpl.LOG.info(prefix(ix) + " " + src(getSecondInt(log[ix])) + " unset ["
            + header(log[ix + 2]) + ", " + header(log[ix + 3]) + "]");
        return ix + 4;
      }
      case SET_BUF: {
        LlapIoImpl.LOG.info(prefix(ix) + " " + src(getSecondInt(log[ix])) + " set "
            + header(log[ix + 2]) + " to " + Integer.toHexString((int)log[ix + 3]));
        return ix + 4;
      }
      case ADD_TO_LIST: {
        //LlapIoImpl.LOG.info(prefix(ix) + " adding " + header(log[ix + 2]) + " to "
        //    + getSecondInt(log[ix]) + " before " + log[ix + 3]);
        return ix + 4;
      }
      case REMOVE_FROM_LIST: {
        // LlapIoImpl.LOG.info(prefix(ix) + " removing " + header(log[ix + 2]) + " from "
        //   + getSecondInt(log[ix]) + " head " + log[ix + 3]);
        return ix + 4;
      }
      case ERROR: {
        LlapIoImpl.LOG.error(prefix(ix) + " failed");
        return ix + 2;
      }
      default: throw new AssertionError("Unknown " + event);
      }
    }

    private String prefix(int ix) {
      return ix + " thread-" + log[ix + 1];
    }

    private String src(int val) {
      return Src.values()[val].name();
    }

    private String header(long l) {
      return getFirstInt(l) + ":" + getSecondInt(l);
    }


    public synchronized void dumpLog(boolean doSleep) {
      if (doSleep) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
        }
      }
      int logSize = (int)offset.get();
      int ix = 0;
      while (ix < logSize) {
        ix = dumpOneLine(ix);
      }
      offset.set(0);
    }
  }
}
