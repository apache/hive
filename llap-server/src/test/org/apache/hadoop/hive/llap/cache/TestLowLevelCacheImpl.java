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

import static org.apache.hadoop.hive.llap.cache.LlapCacheableBuffer.INVALIDATE_OK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.DataCache.DiskRangeListFactory;
import org.apache.hadoop.hive.common.io.DiskRangeList.CreateHelper;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.llap.cache.LowLevelCache.Priority;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.apache.hadoop.hive.ql.io.orc.encoded.CacheChunk;

import org.junit.Test;

public class TestLowLevelCacheImpl {
  private static final Logger LOG = LoggerFactory.getLogger(TestLowLevelCacheImpl.class);

  private static final DiskRangeListFactory testFactory = new DiskRangeListFactory() {
    public DiskRangeList createCacheChunk(MemoryBuffer buffer, long offset, long end) {
      return new CacheChunk(buffer, offset, end);
    }
  };

  private static class DummyAllocator implements EvictionAwareAllocator {
    @Override
    public void allocateMultiple(MemoryBuffer[] dest, int size) {
      for (int i = 0; i < dest.length; ++i) {
        LlapDataBuffer buf = new LlapDataBuffer();
        buf.initialize(null, -1, size);
        dest[i] = buf;
      }
    }

    @Override
    public void deallocate(MemoryBuffer buffer) {
      if (buffer instanceof LlapCacheableBuffer) {
        ((LlapCacheableBuffer)buffer).invalidate();
      }
    }

    @Override
    public void deallocateEvicted(MemoryBuffer buffer) {
    }

    @Override
    public void deallocateProactivelyEvicted(MemoryBuffer buffer) {
    }

    @Override
    public boolean isDirectAlloc() {
      return false;
    }

    @Override
    public int getMaxAllocation() {
      return 0;
    }

    @Override
    public MemoryBuffer createUnallocated() {
      return new LlapDataBuffer();
    }

    @Override
    public void allocateMultiple(MemoryBuffer[] dest, int size,
        BufferObjectFactory factory) throws AllocatorOutOfMemoryException {
      allocateMultiple(dest, size);
    }
  }

  private static class DummyCachePolicy implements LowLevelCachePolicy {
    public DummyCachePolicy() {
    }

    public void cache(LlapCacheableBuffer buffer, Priority pri) {
    }

    public void notifyLock(LlapCacheableBuffer buffer) {
    }

    public void notifyUnlock(LlapCacheableBuffer buffer) {
    }

    public long evictSomeBlocks(long memoryToReserve) {
      return memoryToReserve;
    }

    public void setEvictionListener(EvictionListener listener) {
    }

    public void setParentDebugDumper(LlapIoDebugDump dumper) {
    }

    @Override
    public long purge() {
      return 0;
    }

    @Override
    public void debugDumpShort(StringBuilder sb) {
    }
  }

/*
Example code to test specific scenarios:
    LowLevelCacheImpl cache = new LowLevelCacheImpl(
        LlapDaemonCacheMetrics.create("test", "1"), new DummyCachePolicy(),
        new DummyAllocator(), true, -1); // no cleanup thread
    final int FILE = 1;
    cache.putFileData(FILE, gaps(3756206, 4261729, 7294767, 7547564), fbs(3), 0, Priority.NORMAL, null, null);
    cache.putFileData(FILE, gaps(7790545, 11051556), fbs(1), 0, Priority.NORMAL, null, null);
    cache.putFileData(FILE, gaps(11864971, 11912961, 13350968, 13393630), fbs(3), 0, Priority.NORMAL, null, null);
    DiskRangeList dr = dr(3756206, 7313562);
    MutateHelper mh = new MutateHelper(dr);
    dr = dr.insertAfter(dr(7790545, 11051556));
    dr = dr.insertAfter(dr(11864971, 13393630));
    BooleanRef g = new BooleanRef();
    dr = cache.getFileData(FILE, mh.next, 0, testFactory, null, g);
*/

  @Test
  public void testGetPut() {
    LowLevelCacheImpl cache = new LowLevelCacheImpl(
        LlapDaemonCacheMetrics.create("test", "1"), new DummyCachePolicy(),
        new DummyAllocator(), true, -1); // no cleanup thread
    long fn1 = 1, fn2 = 2;
    MemoryBuffer[] fakes = new MemoryBuffer[] { fb(), fb(), fb(), fb(), fb(), fb() };
    verifyRefcount(fakes, 1, 1, 1, 1, 1, 1);
    assertNull(cache.putFileData(fn1, drs(1, 2), fbs(fakes, 0, 1), 0, Priority.NORMAL, null, null));
    assertNull(cache.putFileData(fn2, drs(1, 2), fbs(fakes, 2, 3), 0, Priority.NORMAL, null, null));
    verifyCacheGet(cache, fn1, 1, 3, fakes[0], fakes[1]);
    verifyCacheGet(cache, fn2, 1, 3, fakes[2], fakes[3]);
    verifyCacheGet(cache, fn1, 2, 4, fakes[1], dr(3, 4));
    verifyRefcount(fakes, 3, 4, 3, 3, 1, 1);
    MemoryBuffer[] bufsDiff = fbs(fakes, 4, 5);
    long[] mask = cache.putFileData(fn1, drs(3, 1), bufsDiff, 0, Priority.NORMAL, null, null);
    assertEquals(1, mask.length);
    assertEquals(2, mask[0]); // 2nd bit set - element 2 was already in cache.
    assertSame(fakes[0], bufsDiff[1]); // Should have been replaced
    verifyRefcount(fakes, 4, 4, 3, 3, 2, 1);
    verifyCacheGet(cache, fn1, 1, 4, fakes[0], fakes[1], fakes[4]);
    verifyRefcount(fakes, 5, 5, 3, 3, 3, 1);
  }

  private void verifyCacheGet(LowLevelCacheImpl cache, long fileId, Object... stuff) {
    CreateHelper list = new CreateHelper();
    DiskRangeList iter = null;
    int intCount = 0, lastInt = -1;
    int resultCount = stuff.length;
    for (Object obj : stuff) {
      if (obj instanceof Integer) {
        --resultCount;
        assertTrue(intCount >= 0);
        if (intCount == 0) {
          lastInt = (Integer)obj;
          intCount = 1;
        } else {
          list.addOrMerge(lastInt, (Integer)obj, true, true);
          intCount = 0;
        }
        continue;
      } else if (intCount >= 0) {
        assertTrue(intCount == 0);
        intCount = -1;
        iter = cache.getFileData(fileId, list.get(), 0, testFactory, null, null);
        assertEquals(resultCount, iter.listSize());
      }
      assertTrue(iter != null);
      if (obj instanceof MemoryBuffer) {
        assertTrue(iter instanceof CacheChunk);
        assertSame(obj, ((CacheChunk)iter).getBuffer());
      } else {
        assertTrue(iter.equals(obj));
      }
      iter = iter.next;
    }
  }

  @Test
  public void testBitmaskHandling() {
    LowLevelCacheImpl cache = new LowLevelCacheImpl(
            LlapDaemonCacheMetrics.create("test", "1"), new DummyCachePolicy(),
            new DummyAllocator(), true, -1); // no cleanup thread
    long fn1 = 1;

    LlapDataBuffer[] buffs1 = IntStream.range(0, 5).mapToObj(i -> fb()).toArray(LlapDataBuffer[]::new);
    DiskRange[] drs1 = drs(1, 2, 31, 32, 33);

    LlapDataBuffer[] buffs2 = IntStream.range(0, 41).mapToObj(i -> fb()).toArray(LlapDataBuffer[]::new);
    DiskRange[] drs2 = drs(IntStream.range(1, 42).toArray());


    assertNull(cache.putFileData(fn1, drs1, buffs1, 0, Priority.NORMAL, null, null));

    long[] mask = cache.putFileData(fn1, drs2, buffs2, 0, Priority.NORMAL, null, null);
    assertEquals(1, mask.length);
    long expected = Long.parseLong("111000000000000000000000000000011", 2);
    assertEquals(expected, mask[0]);
  }

  @Test
  public void testCacheHydrationMetadata() {
    LowLevelCacheImpl cache = new LowLevelCacheImpl(
        LlapDaemonCacheMetrics.create("test", "1"), new DummyCachePolicy(),
        new DummyAllocator(), true, -1); // no cleanup thread
    long fn1 = 1234;
    long baseOffset = 100;
    LlapDataBuffer[] buffs = IntStream.range(0, 3).mapToObj(i -> fb()).toArray(LlapDataBuffer[]::new);
    DiskRange[] drs = drs(10, 20, 30);

    cache.putFileData(fn1, drs, buffs, baseOffset, Priority.NORMAL, null, null);
    for (int i = 0; i < buffs.length; i++) {
      assertEquals(drs[i].getOffset() + baseOffset, buffs[i].getStart());
    }
  }

  @Test
  public void testDeclaredLengthUnsetForCollidedBuffer() throws Exception {
    LowLevelCacheImpl cache = new LowLevelCacheImpl(
        LlapDaemonCacheMetrics.create("test", "1"), new DummyCachePolicy(),
        new DummyAllocator(), true, -1); // no cleanup thread

    long fileKey = 1;
    long[] putResult;

    // 5 buffers: 0,1 cached in the first go, 2,3,4 cached in the next one; buffers 1 and 4 cover overlapping ranges
    LlapDataBuffer[] buffers = IntStream.range(0, 5).mapToObj(i -> fb()).toArray(LlapDataBuffer[]::new);

    LlapDataBuffer[] oldBufs = new LlapDataBuffer[]{ buffers[0], buffers[1] };
    DiskRange[] oldRanges = drs(0, 15);

    LlapDataBuffer[] newBufs = new LlapDataBuffer[]{ buffers[2], buffers[3], buffers[4] };
    DiskRange[] newRanges = drs(5, 10, 15);

    putResult = cache.putFileData(fileKey, oldRanges, oldBufs, 0, Priority.NORMAL, null, null);
    assertNull(putResult);

    putResult = cache.putFileData(fileKey, newRanges, newBufs, 0, Priority.NORMAL, null, null);
    assertEquals(1, putResult.length);
    assertEquals(Long.parseLong("100", 2), putResult[0]);

    for (int i = 0; i < buffers.length; ++i) {
      // since buffer 4 collided with buffer 1 it should not have cached length set, as it will not even be seen by
      // cache policy before it gets quickly deallocated in processCollisions method
      if (i == buffers.length - 1) {
        assertEquals(LlapDataBuffer.UNKNOWN_CACHED_LENGTH, buffers[i].declaredCachedLength);
      } else {
        assertEquals(1, buffers[i].declaredCachedLength);
      }
    }


  }

  @Test
  public void testProactiveEvictionMark() {
    _testProactiveEvictionMark(false);
  }

  @Test
  public void testProactiveEvictionMarkInstantDeallocation() {
    _testProactiveEvictionMark(true);
  }

  private void _testProactiveEvictionMark(boolean isInstantDeallocation) {
    LowLevelCacheImpl cache = new LowLevelCacheImpl(
        LlapDaemonCacheMetrics.create("test", "1"), new DummyCachePolicy(),
        new DummyAllocator(), true, -1); // no cleanup thread
    long fn1 = 1;
    long fn2 = 2;

    LlapDataBuffer[] buffs1 = IntStream.range(0, 4).mapToObj(i -> fb()).toArray(LlapDataBuffer[]::new);
    DiskRange[] drs1 = drs(IntStream.range(1, 5).toArray());
    CacheTag tag1 = CacheTag.build("default.table1");

    LlapDataBuffer[] buffs2 = IntStream.range(0, 41).mapToObj(i -> fb()).toArray(LlapDataBuffer[]::new);
    DiskRange[] drs2 = drs(IntStream.range(1, 42).toArray());
    CacheTag tag2 = CacheTag.build("default.table2");

    Predicate<CacheTag> predicate = tag -> "default.table1".equals(tag.getTableName());

    cache.putFileData(fn1, drs1, buffs1, 0, Priority.NORMAL, null, tag1);
    cache.putFileData(fn2, drs2, buffs2, 0, Priority.NORMAL, null, tag2);
    Arrays.stream(buffs1).forEach(b -> {b.decRef(); b.decRef();});

    // Simulating eviction on a buffer
    assertEquals(INVALIDATE_OK, buffs1[2].invalidate());

    //buffs1[0,1,3] should be marked, as 2 is already invalidated
    assertEquals(3, cache.markBuffersForProactiveEviction(predicate, isInstantDeallocation));

    for (int i = 0; i < buffs1.length; ++i) {
      LlapDataBuffer buffer = buffs1[i];
      if (i == 2) {
        assertFalse(buffer.isMarkedForEviction());
      } else {
        assertTrue(buffer.isMarkedForEviction());
        assertEquals(isInstantDeallocation, buffer.isInvalid());
      }
    }

    // All buffers for file2 should not be marked as per predicate
    for (LlapDataBuffer buffer : buffs2) {
      assertFalse(buffer.isMarkedForEviction());
    }

  }

  @Test
  public void testMultiMatch() {
    LowLevelCacheImpl cache = new LowLevelCacheImpl(
        LlapDaemonCacheMetrics.create("test", "1"), new DummyCachePolicy(),
        new DummyAllocator(), true, -1); // no cleanup thread
    long fn = 1;
    MemoryBuffer[] fakes = new MemoryBuffer[] { fb(), fb() };
    assertNull(cache.putFileData(
        fn, new DiskRange[] { dr(2, 4), dr(6, 8) }, fakes, 0, Priority.NORMAL, null, null));
    verifyCacheGet(cache, fn, 1, 9, dr(1, 2), fakes[0], dr(4, 6), fakes[1], dr(8, 9));
    verifyCacheGet(cache, fn, 2, 8, fakes[0], dr(4, 6), fakes[1]);
    verifyCacheGet(cache, fn, 1, 5, dr(1, 2), fakes[0], dr(4, 5));
    verifyCacheGet(cache, fn, 1, 3, dr(1, 2), fakes[0]);
    verifyCacheGet(cache, fn, 3, 4, dr(3, 4)); // We don't expect cache requests from the middle.
    verifyCacheGet(cache, fn, 3, 7, dr(3, 6), fakes[1]);
    verifyCacheGet(cache, fn, 0, 2, 4, 6, dr(0, 2), dr(4, 6));
    verifyCacheGet(cache, fn, 2, 4, 6, 8, fakes[0], fakes[1]);
  }

  @Test
  public void testMultiMatchNonGranular() {
    LowLevelCacheImpl cache = new LowLevelCacheImpl(
        LlapDaemonCacheMetrics.create("test", "1"), new DummyCachePolicy(),
        new DummyAllocator(), false, -1); // no cleanup thread
    long fn = 1;
    MemoryBuffer[] fakes = new MemoryBuffer[] { fb(), fb() };
    assertNull(cache.putFileData(
        fn, new DiskRange[] { dr(2, 4), dr(6, 8) }, fakes, 0, Priority.NORMAL, null, null));
    // We expect cache requests from the middle here
    verifyCacheGet(cache, fn, 3, 4, fakes[0]);
    verifyCacheGet(cache, fn, 3, 7, fakes[0], dr(4, 6), fakes[1]);
  }

  @Test
  public void testStaleValueGet() {
    LowLevelCacheImpl cache = new LowLevelCacheImpl(
        LlapDaemonCacheMetrics.create("test", "1"), new DummyCachePolicy(),
        new DummyAllocator(), true, -1); // no cleanup thread
    long fn1 = 1, fn2 = 2;
    MemoryBuffer[] fakes = new MemoryBuffer[] { fb(), fb(), fb() };
    assertNull(cache.putFileData(fn1, drs(1, 2), fbs(fakes, 0, 1), 0, Priority.NORMAL, null, null));
    assertNull(cache.putFileData(fn2, drs(1), fbs(fakes, 2), 0, Priority.NORMAL, null, null));
    verifyCacheGet(cache, fn1, 1, 3, fakes[0], fakes[1]);
    verifyCacheGet(cache, fn2, 1, 2, fakes[2]);
    verifyRefcount(fakes, 3, 3, 3);
    evict(cache, fakes[0]);
    evict(cache, fakes[2]);
    verifyCacheGet(cache, fn1, 1, 3, dr(1, 2), fakes[1]);
    verifyCacheGet(cache, fn2, 1, 2, dr(1, 2));
    verifyRefcount(fakes, 0, 4, 0);
  }

  @Test
  public void testStaleValueReplace() {
    LowLevelCacheImpl cache = new LowLevelCacheImpl(
        LlapDaemonCacheMetrics.create("test", "1"), new DummyCachePolicy(),
        new DummyAllocator(), true, -1); // no cleanup thread
    long fn1 = 1, fn2 = 2;
    MemoryBuffer[] fakes = new MemoryBuffer[] {
        fb(), fb(), fb(), fb(), fb(), fb(), fb(), fb(), fb() };
    assertNull(cache.putFileData(fn1, drs(1, 2, 3), fbs(fakes, 0, 1, 2), 0, Priority.NORMAL, null, null));
    assertNull(cache.putFileData(fn2, drs(1), fbs(fakes, 3), 0, Priority.NORMAL, null, null));
    evict(cache, fakes[0]);
    evict(cache, fakes[3]);
    long[] mask = cache.putFileData(
        fn1, drs(1, 2, 3, 4), fbs(fakes, 4, 5, 6, 7), 0, Priority.NORMAL, null, null);
    assertEquals(1, mask.length);
    assertEquals(6, mask[0]); // Buffers at offset 2 & 3 exist; 1 exists and is stale; 4 doesn't
    assertNull(cache.putFileData(fn2, drs(1), fbs(fakes, 8), 0, Priority.NORMAL, null, null));
    verifyCacheGet(cache, fn1, 1, 5, fakes[4], fakes[1], fakes[2], fakes[7]);
  }

  @Test
  public void testCacheMetrics() {
    CreateHelper list = new CreateHelper();
    list.addOrMerge(0, 100, true, false);
    list.addOrMerge(100, 200, true, false);
    list.addOrMerge(200, 300, true, false);
    list.addOrMerge(300, 400, true, false);
    list.addOrMerge(400, 500, true, false);
    assertEquals(1, list.get().listSize());
    assertEquals(500, list.get().getTotalLength());
    list = new CreateHelper();
    list.addOrMerge(0, 100, false, false);
    list.addOrMerge(100, 200, false, false);
    list.addOrMerge(200, 300, false, false);
    list.addOrMerge(300, 400, false, false);
    list.addOrMerge(400, 500, false, false);
    assertEquals(5, list.get().listSize());
    assertEquals(500, list.get().getTotalLength());
    list = new CreateHelper();
    list.addOrMerge(0, 100, true, false);
    list.addOrMerge(100, 200, true, false);
    list.addOrMerge(200, 300, false, false);
    list.addOrMerge(300, 400, true, false);
    list.addOrMerge(400, 500, true, false);
    assertEquals(2, list.get().listSize());
    assertEquals(500, list.get().getTotalLength());

    LlapDaemonCacheMetrics metrics = LlapDaemonCacheMetrics.create("test", "1");
    LowLevelCacheImpl cache = new LowLevelCacheImpl(metrics,
        new DummyCachePolicy(), new DummyAllocator(), true, -1); // no cleanup thread
    long fn = 1;
    MemoryBuffer[] fakes = new MemoryBuffer[]{fb(), fb(), fb()};
    cache.putFileData(fn, new DiskRange[]{dr(0, 100), dr(300, 500), dr(800, 1000)},
        fakes, 0, Priority.NORMAL, null, null);
    assertEquals(0, metrics.getCacheRequestedBytes());
    assertEquals(0, metrics.getCacheHitBytes());
    list = new CreateHelper();
    list.addOrMerge(0, 1000, true, false);
    cache.getFileData(fn, list.get(), 0, testFactory, null, null);
    assertEquals(1000, metrics.getCacheRequestedBytes());
    assertEquals(500, metrics.getCacheHitBytes());

    list = new CreateHelper();
    list.addOrMerge(0, 100, true, false);
    cache.getFileData(fn, list.get(), 0, testFactory, null, null);
    assertEquals(1100, metrics.getCacheRequestedBytes());
    assertEquals(600, metrics.getCacheHitBytes());

    list = new CreateHelper();
    list.addOrMerge(0, 100, true, false);
    list.addOrMerge(300, 500, true, false);
    list.addOrMerge(800, 1000, true, false);
    cache.getFileData(fn, list.get(), 0, testFactory, null, null);
    assertEquals(1600, metrics.getCacheRequestedBytes());
    assertEquals(1100, metrics.getCacheHitBytes());

    list = new CreateHelper();
    list.addOrMerge(300, 500, true, false);
    list.addOrMerge(1000, 2000, true, false);
    cache.getFileData(fn, list.get(), 0, testFactory, null, null);
    assertEquals(2800, metrics.getCacheRequestedBytes());
    assertEquals(1300, metrics.getCacheHitBytes());
  }

  @Test
  public void testMTTWithCleanup() {
    final LowLevelCacheImpl cache = new LowLevelCacheImpl(LlapDaemonCacheMetrics.create(
        "test", "1"), new DummyCachePolicy(), new DummyAllocator(), true, 1);
    final long fn1 = 1, fn2 = 2;
    final int offsetsToUse = 8;
    final CountDownLatch cdlIn = new CountDownLatch(4), cdlOut = new CountDownLatch(1);
    final AtomicInteger rdmsDone = new AtomicInteger(0);
    Callable<Long> rdmCall = new Callable<Long>() {
      public Long call() {
        int gets = 0, puts = 0;
        try {
          Random rdm = new Random(1234 + Thread.currentThread().getId());
          syncThreadStart(cdlIn, cdlOut);
          for (int i = 0; i < 20000; ++i) {
            boolean isGet = rdm.nextBoolean(), isFn1 = rdm.nextBoolean();
            long fileName = isFn1 ? fn1 : fn2;
            int fileIndex = isFn1 ? 1 : 2;
            int count = rdm.nextInt(offsetsToUse);
            if (isGet) {
              int[] offsets = new int[count];
              count = generateOffsets(offsetsToUse, rdm, offsets);
              CreateHelper list = new CreateHelper();
              for (int j = 0; i < count; ++i) {
                list.addOrMerge(offsets[j], offsets[j] + 1, true, false);
              }

              DiskRangeList iter = cache.getFileData(fileName, list.get(), 0, testFactory, null, null);
              int j = -1;
              while (iter != null) {
                ++j;
                if (!(iter instanceof CacheChunk)) {
                  iter = iter.next;
                  continue;
                }
                ++gets;
                LlapAllocatorBuffer result = (LlapAllocatorBuffer)((CacheChunk)iter).getBuffer();
                assertEquals(makeFakeArenaIndex(fileIndex, offsets[j]), result.getArenaIndex());
                cache.decRefBuffer(result);
                iter = iter.next;
              }
            } else {
              DiskRange[] ranges = new DiskRange[count];
              int[] offsets = new int[count];
              for (int j = 0; j < count; ++j) {
                int next = rdm.nextInt(offsetsToUse);
                ranges[j] = dr(next, next + 1);
                offsets[j] = next;
              }
              MemoryBuffer[] buffers = new MemoryBuffer[count];
              for (int j = 0; j < offsets.length; ++j) {
                LlapDataBuffer buf = LowLevelCacheImpl.allocateFake();
                buf.setNewAllocLocation(makeFakeArenaIndex(fileIndex, offsets[j]), 0);
                buffers[j] = buf;
              }
              long[] mask = cache.putFileData(fileName, ranges, buffers, 0, Priority.NORMAL, null, null);
              puts += buffers.length;
              long maskVal = 0;
              if (mask != null) {
                assertEquals(1, mask.length);
                maskVal = mask[0];
              }
              for (int j = 0; j < offsets.length; ++j) {
                LlapDataBuffer buf = (LlapDataBuffer)(buffers[j]);
                if ((maskVal & 1) == 1) {
                  assertEquals(makeFakeArenaIndex(fileIndex, offsets[j]), buf.getArenaIndex());
                }
                maskVal >>= 1;
                cache.decRefBuffer(buf);
              }
            }
          }
        } finally {
          rdmsDone.incrementAndGet();
        }
        return  (((long)gets) << 32) | puts;
      }

      private int makeFakeArenaIndex(int fileIndex, long offset) {
        return (int)((fileIndex << 12) + offset);
      }
    };

    FutureTask<Integer> evictionTask = new FutureTask<Integer>(new Callable<Integer>() {
      public Integer call() {
        boolean isFirstFile = false;
        Random rdm = new Random(1234 + Thread.currentThread().getId());
        int evictions = 0;
        syncThreadStart(cdlIn, cdlOut);
        while (rdmsDone.get() < 3) {
          DiskRangeList head = new DiskRangeList(0, offsetsToUse + 1);
          isFirstFile = !isFirstFile;
          long fileId = isFirstFile ? fn1 : fn2;
          head = cache.getFileData(fileId, head, 0, testFactory, null, null);
          DiskRange[] results = head.listToArray();
          int startIndex = rdm.nextInt(results.length), index = startIndex;
          LlapDataBuffer victim = null;
          do {
            DiskRange r = results[index];
            if (r instanceof CacheChunk) {
              LlapDataBuffer result = (LlapDataBuffer)((CacheChunk)r).getBuffer();
              cache.decRefBuffer(result);
              if (victim == null && result.invalidate() == INVALIDATE_OK) {
                ++evictions;
                victim = result;
              }
            }
            ++index;
            if (index == results.length) index = 0;
          } while (index != startIndex);
          if (victim == null) continue;
          cache.notifyEvicted(victim);
        }
        return evictions;
      }
    });

    FutureTask<Long> rdmTask1 = new FutureTask<Long>(rdmCall),
        rdmTask2 = new FutureTask<Long>(rdmCall), rdmTask3 = new FutureTask<Long>(rdmCall);
    Executor threadPool = Executors.newFixedThreadPool(4);
    threadPool.execute(rdmTask1);
    threadPool.execute(rdmTask2);
    threadPool.execute(rdmTask3);
    threadPool.execute(evictionTask);
    try {
      cdlIn.await();
      cdlOut.countDown();
      long result1 = rdmTask1.get(), result2 = rdmTask2.get(), result3 = rdmTask3.get();
      int evictions = evictionTask.get();
      LOG.info("MTT test: task 1: " + descRdmTask(result1)  + ", task 2: " + descRdmTask(result2)
          + ", task 3: " + descRdmTask(result3) + "; " + evictions + " evictions");
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  private String descRdmTask(long result) {
    return (result >>> 32) + " successful gets, " + (result & ((1L << 32) - 1)) + " puts";
  }

  private void syncThreadStart(final CountDownLatch cdlIn, final CountDownLatch cdlOut) {
    cdlIn.countDown();
    try {
      cdlOut.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void evict(LowLevelCacheImpl cache, MemoryBuffer fake) {
    LlapDataBuffer victimBuffer = (LlapDataBuffer)fake;
    int refCount = victimBuffer.getRefCount();
    for (int i = 0; i < refCount; ++i) {
      victimBuffer.decRef();
    }
    assertTrue(INVALIDATE_OK == victimBuffer.invalidate());
    cache.notifyEvicted(victimBuffer);
  }

  private void verifyRefcount(MemoryBuffer[] fakes, int... refCounts) {
    for (int i = 0; i < refCounts.length; ++i) {
      assertEquals("At " + i, refCounts[i], ((LlapDataBuffer)fakes[i]).getRefCount());
    }
  }

  private MemoryBuffer[] fbs(MemoryBuffer[] fakes, int... indexes) {
    MemoryBuffer[] rv = new MemoryBuffer[indexes.length];
    for (int i = 0; i < indexes.length; ++i) {
      rv[i] = (indexes[i] == -1) ? null : fakes[indexes[i]];
    }
    return rv;
  }

  private MemoryBuffer[] fbs(int count) {
    MemoryBuffer[] rv = new MemoryBuffer[count];
    for (int i = 0; i < count; ++i) {
      rv[i] = fb();
    }
    return rv;
  }


  private LlapDataBuffer fb() {
    LlapDataBuffer fake = LowLevelCacheImpl.allocateFake();
    fake.incRef();
    return fake;
  }

  private DiskRangeList dr(int from, int to) {
    return new DiskRangeList(from, to);
  }

  private DiskRange[] drs(int... offsets) {
    DiskRange[] result = new DiskRange[offsets.length];
    for (int i = 0; i < offsets.length; ++i) {
      result[i] = new DiskRange(offsets[i], offsets[i] + 1);
    }
    return result;
  }

  private DiskRange[] gaps(int... offsets) {
    DiskRange[] result = new DiskRange[offsets.length - 1];
    for (int i = 0; i < result .length; ++i) {
      result[i] = new DiskRange(offsets[i], offsets[i + 1]);
    }
    return result;
  }

  private int generateOffsets(int offsetsToUse, Random rdm, int[] offsets) {
    for (int j = 0; j < offsets.length; ++j) {
      offsets[j] = rdm.nextInt(offsetsToUse);
    }
    Arrays.sort(offsets);
    // Values should unique (given how we do the checking and "addOrMerge")
    int check = 0, insert = 0, count = offsets.length;
    while (check < (offsets.length - 1)) {
      if (offsets[check] == offsets[check + 1]) {
        --insert;
        --count;
      }
      offsets[++insert] = offsets[++check];
    }
    return count;
  }
}
