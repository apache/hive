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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hive.common.io.Allocator.AllocatorOutOfMemoryException;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.llap.cache.TestBuddyAllocator.DummyMemoryManager;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This relies on allocations basically being sequential, no internal reordering. Rather,
 * the specific paths it takes do; all the scenarios should work regardless of allocation.
 */
public class TestBuddyAllocatorForceEvict {
  private static final Logger LOG = LoggerFactory.getLogger(TestBuddyAllocatorForceEvict.class);
  private static final DummyMemoryManager MM = new TestBuddyAllocator.DummyMemoryManager();
  private static final LlapDaemonCacheMetrics METRICS = LlapDaemonCacheMetrics.create("test", "1");

  @Test(timeout = 6000)
  public void testSimple() {
    runSimpleTests(false);
    runSimpleTests(true);
  }

  public void runSimpleTests(boolean isBruteOnly) {
    runSimple1to2Discard(create(1024, 1, 1024, true, isBruteOnly), 256);
    runSimple1to2Discard(create(1024, 1, 1024, false, isBruteOnly), 256);
    runSimple1to2Discard(create(512, 2, 1024, false, isBruteOnly), 256);
  }

  @Test(timeout = 6000)
  public void testSmallBlocks() {
    runSmallBlockersTests(false);
    runSmallBlockersTests(true);
  }

  public void runSmallBlockersTests(boolean isBruteOnly) {
    runSmallBlockersDiscard(create(1024, 1, 1024, false, isBruteOnly), 128, false, false);
    runSmallBlockersDiscard(create(1024, 1, 1024, false, isBruteOnly), 128, true, false);
    runSmallBlockersDiscard(create(1024, 1, 1024, false, isBruteOnly), 128, false, true);
    runSmallBlockersDiscard(create(1024, 1, 1024, false, isBruteOnly), 128, true, true);

    runSmallBlockersDiscard(create(512, 2, 1024, false, isBruteOnly), 128, false, false);
    runSmallBlockersDiscard(create(512, 2, 1024, false, isBruteOnly), 128, true, false);
    runSmallBlockersDiscard(create(512, 2, 1024, false, isBruteOnly), 128, false, true);
    runSmallBlockersDiscard(create(512, 2, 1024, false, isBruteOnly), 128, true, true);
  }

  @Test(timeout = 6000)
  public void testZebra() {
    runZebraTests(false);
    runZebraTests(true);
  }

  public void runZebraTests(boolean isBruteOnly) {
    runZebraDiscard(create(1024, 1, 1024, false, isBruteOnly), 32, 16, 1);
    runZebraDiscard(create(1024, 1, 1024, false, isBruteOnly), 64, 8, 1);
    runZebraDiscard(create(1024, 1, 1024, false, isBruteOnly), 32, 16, 2);
    runZebraDiscard(create(1024, 1, 1024, false, isBruteOnly), 32, 16, 4);

    runZebraDiscard(create(512, 2, 1024, false, isBruteOnly), 32, 16, 1);
    runZebraDiscard(create(512, 2, 1024, false, isBruteOnly), 64, 8, 1);
    runZebraDiscard(create(512, 2, 1024, false, isBruteOnly), 32, 16, 2);

    runZebraDiscard(create(256, 4, 1024, false, isBruteOnly), 32, 16, 2);
    runZebraDiscard(create(256, 4, 1024, false, isBruteOnly), 32, 16, 4);
  }

  @Test(timeout = 6000)
  public void testUnevenZebra() {
    runUnevenZebraTests(false);
    runUnevenZebraTests(true);
  }

  public void runUnevenZebraTests(boolean isBruteOnly) {
    runCustomDiscard(create(1024, 1, 1024, false, isBruteOnly),
        new int[] { 256, 256, 128, 128, 128, 128 }, new int[] { 0, 2, 4 }, 512);
    runCustomDiscard(create(1024, 1, 1024, false, isBruteOnly),
        new int[] { 256, 256, 64, 64, 64, 64, 64, 64, 64, 64 },
        new int[] { 0, 2, 4, 6, 8 }, 512);

    runCustomDiscard(create(512, 2, 1024, false, isBruteOnly),
        new int[] { 256, 256, 128, 128, 128, 128 }, new int[] { 0, 2, 4 }, 512);
    runCustomDiscard(create(512, 2, 1024, false, isBruteOnly),
        new int[] { 256, 256, 64, 64, 64, 64, 64, 64, 64, 64 },
        new int[] { 0, 2, 4, 6, 8 }, 512);
  }

  @Test(timeout = 6000)
  public void testComplex1() {
    runComplexTests(false);
    runComplexTests(true);
  }

  public void runComplexTests(boolean isBruteOnly) {
    runCustomDiscard(create(1024, 1, 1024, false, isBruteOnly),
        new int[] { 256, 128, 64, 64, 256, 64, 64, 128 },
        new int[] { 0,            3,            6, 7 }, 512 );
    runCustomDiscard(create(1024, 1, 1024, false, isBruteOnly),
        new int[] { 256, 64, 64, 64, 64, 256, 64, 64, 128 },
        new int[] { 0,                4,           7, 8 }, 512 );

    runCustomDiscard(create(512, 2, 1024, false, isBruteOnly),
        new int[] { 256, 128, 64, 64, 256, 64, 64, 128 },
        new int[] { 0,            3,            6, 7 }, 512 );
    runCustomDiscard(create(512, 2, 1024, false, isBruteOnly),
        new int[] { 256, 64, 64, 64, 64, 256, 64, 64, 128 },
        new int[] { 0,                4,           7, 8 }, 512 );
  }

  static class MttTestCallableResult {
    public int successes, ooms, allocSize;
    @Override
    public String toString() {
      return "allocation size " + allocSize + ": " + successes + " allocations, " + ooms + " OOMs";
    }
  }

  static class MttTestCallable implements Callable<MttTestCallableResult> {
    private final CountDownLatch cdlIn, cdlOut;
    private final int allocSize, allocCount, iterCount;
    private final BuddyAllocator a;

    public MttTestCallable(CountDownLatch cdlIn, CountDownLatch cdlOut, BuddyAllocator a,
        int allocSize, int allocCount, int iterCount) {
      this.cdlIn = cdlIn;
      this.cdlOut = cdlOut;
      this.a = a;
      this.allocSize = allocSize;
      this.allocCount = allocCount;
      this.iterCount = iterCount;
    }

    public MttTestCallableResult call() throws Exception {
      LOG.info(Thread.currentThread().getId() + " thread starts");
      TestBuddyAllocator.syncThreadStart(cdlIn, cdlOut);
      MttTestCallableResult result = new MttTestCallableResult();
      result.allocSize = allocSize;
      List<MemoryBuffer> allocs = new ArrayList<>(allocCount);
      LlapAllocatorBuffer[] dest = new LlapAllocatorBuffer[1];
      for (int i = 0; i < iterCount; ++i) {
        for (int j = 0; j < allocCount; ++j) {
          try {
            dest[0] = null;
            a.allocateMultiple(dest, allocSize);
            LlapAllocatorBuffer buf = dest[0];
            assertTrue(buf.incRef() > 0);
            allocs.add(buf);
            ++result.successes;
            buf.decRef();
          } catch (AllocatorOutOfMemoryException ex) {
            ++result.ooms;
          } catch (Throwable ex) {
            LOG.error("Failed", ex);
            throw new Exception(ex);
          }
        }
        for (MemoryBuffer buf : allocs) {
          try {
            a.deallocate(buf);
          } catch (Throwable ex) {
            LOG.error("Failed", ex);
            throw new Exception(ex);
          }
        }
        allocs.clear();
      }
      return result;
    }
  }

  @Test(timeout = 200000)
  public void testMtt() {
    final int baseAllocSizeLog2 = 3, maxAllocSizeLog2 = 10, totalSize = 8192,
        baseAllocSize = 1 << baseAllocSizeLog2, maxAllocSize = 1 << maxAllocSizeLog2;
    final int threadCount = maxAllocSizeLog2 - baseAllocSizeLog2 + 1;
    final int iterCount = 500;
    final BuddyAllocator a = create(maxAllocSize, 4, totalSize, true, false);
    ExecutorService executor = Executors.newFixedThreadPool(threadCount + 1);
    CountDownLatch cdlIn = new CountDownLatch(threadCount), cdlOut = new CountDownLatch(1);
    @SuppressWarnings("unchecked")
    FutureTask<MttTestCallableResult>[] allocTasks = new FutureTask[threadCount];
    FutureTask<Void> dumpTask = createAllocatorDumpTask(a);
    for (int allocSize = baseAllocSize, i = 0; allocSize <= maxAllocSize; allocSize <<= 1, ++i) {
      allocTasks[i] = new FutureTask<>(new MttTestCallable(
          cdlIn, cdlOut, a, allocSize, totalSize / allocSize, iterCount));
      executor.execute(allocTasks[i]);
    }
    executor.execute(dumpTask);

    runMttTest(a, allocTasks, cdlIn, cdlOut, dumpTask, null, null, totalSize, maxAllocSize);
  }

  public static void runMttTest(BuddyAllocator a, FutureTask<?>[] allocTasks,
      CountDownLatch cdlIn, CountDownLatch cdlOut, FutureTask<Void> dumpTask,
      FutureTask<Void> defragTask, AtomicBoolean defragStopped, int totalSize, int maxAllocSize) {
    Throwable t = null;
    try {
      cdlIn.await(); // Wait for all threads to be ready.
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    cdlOut.countDown(); // Release them at the same time.
    for (int i = 0; i < allocTasks.length; ++i) {
      try {
        Object result = allocTasks[i].get();
        LOG.info("" + result);
      } catch (Throwable tt) {
        LOG.error("Test callable failed", tt);
        if (t == null) {
          a.dumpTestLog();
          t = tt;
        }
      }
    }
    dumpTask.cancel(true);
    if (defragTask != null) {
      defragStopped.set(true);
      try {
        defragTask.get();
      } catch (Throwable tt) {
        LOG.error("Defragmentation thread failed", t);
        if (t == null) {
          a.dumpTestLog();
          t = tt;
        }
      }
    }
    if (t != null) {
      throw new RuntimeException("One of the errors", t);
    }
    // All the tasks should have deallocated their stuff. Make sure we can allocate everything.
    LOG.info("Allocator state after all the tasks: " + a.testDump());
    try {
      allocate(a, totalSize / maxAllocSize, maxAllocSize, 0);
    } catch (Throwable tt) {
      a.dumpTestLog();
      throw tt;
    }
  }

  public static FutureTask<Void> createAllocatorDumpTask(final BuddyAllocator a) {
    return new FutureTask<Void>(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        int logs = 40000; // Prevent excessive logging in case of deadlocks or slowness.
        while ((--logs) >= 0) {
          LOG.info("Allocator state (MTT): " + a.testDump());
          Thread.sleep(10);
        }
        return null;
      }
    });
  }

  private static void runCustomDiscard(BuddyAllocator a, int[] sizes, int[] dealloc, int size) {
    LlapAllocatorBuffer[] initial = prepareCustomFragmentedAllocator(a, sizes, dealloc, true);
    LlapAllocatorBuffer after = allocate(a, 1, size, initial.length + 1)[0];
    LOG.info("After: " + a.testDump());
    for (int i = 0; i < initial.length; ++i) {
      if (initial[i] == null) continue;
      checkTestValue(initial[i], i + 1, null, false);
      a.deallocate(initial[i]);
    }
    checkTestValue(after, initial.length + 1, null, true);
    a.deallocate(after);
  }

  private static void runZebraDiscard(
      BuddyAllocator a, int baseSize, int pairCount, int allocs) {
    LlapAllocatorBuffer[] initial = prepareZebraFragmentedAllocator(a, baseSize, pairCount, true);
    int allocFraction = allocs * 2;
    int bigAllocSize = pairCount * 2 * baseSize / allocFraction;
    LlapAllocatorBuffer[] after = allocate(a, allocs, bigAllocSize, 1 + initial.length);
    LOG.info("After: " + a.testDump());
    for (int i = 0; i < pairCount; ++i) {
      int ix = (i << 1) + 1;
      checkTestValue(initial[ix], ix + 1, null, false);
    }
    checkTestValue(after[0], 1 + initial.length, null, true);
  }

  public static LlapAllocatorBuffer[] prepareZebraFragmentedAllocator(
      BuddyAllocator a, int baseSize, int pairCount, boolean doIncRef) {
    // Allocate 1-1-... xN; free every other one, allocate N/2 (or N/4).
    LlapAllocatorBuffer[] initial = allocate(a, pairCount * 2, baseSize, 1, doIncRef);
    for (int i = 0; i < pairCount; ++i) {
      a.deallocate(initial[i << 1]);
      initial[i << 1] = null;
    }
    LOG.info("Before: " + a.testDump());
    a.setOomLoggingForTest(true);
    return initial;
  }

  private void runSimple1to2Discard(BuddyAllocator a, int baseSize) {
    // Allocate 1-1-1-1; free 0&2; allocate 2
    LlapAllocatorBuffer[] initial = prepareSimpleFragmentedAllocator(a, baseSize, true);
    LlapAllocatorBuffer[] after = allocate(a, 1, baseSize * 2, 1 + initial.length);
    LOG.info("After: " + a.testDump());
    checkInitialValues(initial, 0, 2);
    checkTestValue(after[0], 1 + initial.length, null, true);
    a.deallocate(initial[0]);
    a.deallocate(initial[2]);
    a.deallocate(after[0]);
  }

  public static LlapAllocatorBuffer[] prepareSimpleFragmentedAllocator(
      BuddyAllocator a, int baseSize, boolean doIncRef) {
    LlapAllocatorBuffer[] initial = allocate(a, 4, baseSize, 1, doIncRef);
    checkInitialValues(initial, 0, 2);
    a.deallocate(initial[1]);
    a.deallocate(initial[3]);
    LOG.info("Before: " + a.testDump());
    a.setOomLoggingForTest(true);
    return initial;
  }

  private void runSmallBlockersDiscard(BuddyAllocator a,
      int baseSize, boolean deallocOneFirst, boolean deallocOneSecond) {
    LlapAllocatorBuffer[] initial = prepareAllocatorWithSmallFragments(
        a, baseSize, deallocOneFirst, deallocOneSecond, true);
    int bigAllocSize = baseSize * 4;
    LlapAllocatorBuffer[] after = allocate(a, 1, bigAllocSize, 1 + initial.length);
    LOG.info("After: " + a.testDump());
    checkInitialValues(initial, 2, 4);
    checkTestValue(after[0], 1 + initial.length, null, true);
  }

  public static LlapAllocatorBuffer[] prepareAllocatorWithSmallFragments(BuddyAllocator a,
      int baseSize, boolean deallocOneFirst, boolean deallocOneSecond, boolean doIncRef) {
    // Allocate 2-1-1-2-1-1; free 0,3 and optionally 1 or 5; allocate 4
    int offset = 0;
    LlapAllocatorBuffer[] initial = new LlapAllocatorBuffer[6];
    initial[offset++] = allocate(a, 1, baseSize * 2, offset + 1, doIncRef)[0];
    MemoryBuffer[] tmp = allocate(a, 2, baseSize, offset + 1);
    System.arraycopy(tmp, 0, initial, offset, 2);
    offset += 2;
    initial[offset++] = allocate(a, 1, baseSize * 2, offset + 1, doIncRef)[0];
    tmp = allocate(a, 2, baseSize, offset + 1);
    System.arraycopy(tmp, 0, initial, offset, 2);
    if (deallocOneFirst) {
      a.deallocate(initial[1]);
    }
    if (deallocOneSecond) {
      a.deallocate(initial[5]);
    }
    a.deallocate(initial[0]);
    a.deallocate(initial[3]);
    LOG.info("Before: " + a.testDump());
    a.setOomLoggingForTest(true);
    return initial;
  }

  private static void checkInitialValues(LlapAllocatorBuffer[] bufs, int... indexes) {
    for (int index : indexes) {
      LlapAllocatorBuffer buf = bufs[index];
      if (!incRefIfNotEvicted(buf, false)) continue;
      try {
        checkTestValue(buf, index + 1, null, false);
      } finally {
        buf.decRef();
      }
    }
  }

  private static boolean incRefIfNotEvicted(LlapAllocatorBuffer buf, boolean mustExist) {
    int rc = buf.tryIncRef();
    if (rc == LlapAllocatorBuffer.INCREF_FAILED) {
      fail("Failed to incref (bad state) " + buf);
    }
    if (rc <= 0 && mustExist) {
      fail("Failed to incref (evicted) " + buf);
    }
    return rc > 0; // We expect evicted, but not failed.
  }

  private static void checkTestValue(
      LlapAllocatorBuffer mem, long testValue, String str, boolean mustExist) {
    if (!incRefIfNotEvicted(mem, mustExist)) return;
    try {
      TestBuddyAllocator.checkTestValue(mem, testValue, str);
    } finally {
      mem.decRef();
    }
  }

  public static BuddyAllocator create(int max, int arenas, int total, boolean isShortcut,
      boolean isBruteForceOnly) {
     BuddyAllocator result = new BuddyAllocator(false, false, 8, max, arenas, total, 0,
         null, MM, METRICS, isBruteForceOnly ? "brute" : null, true);
     if (!isShortcut) {
       result.disableDefragShortcutForTest();
     }
     result.setOomLoggingForTest(false);
     return result;
  }

  private static LlapAllocatorBuffer[] allocate(
      BuddyAllocator a, int count, int size, int baseValue) {
    return allocate(a, count, size, baseValue, true);
  }

  public static LlapAllocatorBuffer[] allocate(
      BuddyAllocator a, int count, int size, int baseValue, boolean doIncRef) {
    LlapAllocatorBuffer[] allocs = new LlapAllocatorBuffer[count];
    try {
      a.allocateMultiple(allocs, size);
    } catch (AllocatorOutOfMemoryException ex) {
      LOG.error("Failed to allocate " + allocs.length + " of " + size + "; " + a.testDump());
      throw ex;
    }
    for (int i = 0; i < count; ++i) {
      // Make sure buffers are eligible for discard.
      if (doIncRef) {
        int rc = allocs[i].incRef();
        assertTrue(rc > 0);
      }
      TestBuddyAllocator.putTestValue(allocs[i], baseValue + i);
      if (doIncRef) {
        allocs[i].decRef();
      }
    }
    return allocs;
  }

  public static LlapAllocatorBuffer[] prepareCustomFragmentedAllocator(
      BuddyAllocator a, int[] sizes, int[] dealloc, boolean doIncRef) {
    LlapAllocatorBuffer[] initial = new LlapAllocatorBuffer[sizes.length];
    for (int i = 0; i < sizes.length; ++i) {
      initial[i] = allocate(a, 1, sizes[i], i + 1, doIncRef)[0];
    }
    for (int i = 0; i < dealloc.length; ++i) {
      a.deallocate(initial[dealloc[i]]);
      initial[dealloc[i]] = null;
    }
    LOG.info("Before: " + a.testDump());
    a.setOomLoggingForTest(true);
    return initial;
  }
}
