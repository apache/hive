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

import static org.junit.Assert.assertEquals;
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.DiskRange;
import org.apache.hadoop.hive.common.DiskRangeList;
import org.apache.hadoop.hive.common.DiskRangeList.DiskRangeListCreateHelper;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.io.api.cache.LlapMemoryBuffer;
import org.apache.hadoop.hive.llap.io.api.cache.LowLevelCache.Priority;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl.CacheChunk;
import org.junit.Test;

public class TestLowLevelCacheImpl {
  private static final Log LOG = LogFactory.getLog(TestLowLevelCacheImpl.class);

  private static class DummyAllocator implements Allocator {
    @Override
    public boolean allocateMultiple(LlapMemoryBuffer[] dest, int size) {
      for (int i = 0; i < dest.length; ++i) {
        LlapDataBuffer buf = new LlapDataBuffer();
        buf.initialize(0, null, -1, size);
        dest[i] = buf;
      }
      return true;
    }

    @Override
    public void deallocate(LlapMemoryBuffer buffer) {
    }

    @Override
    public boolean isDirectAlloc() {
      return false;
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
  }

  @Test
  public void testGetPut() {
    Configuration conf = createConf();
    LowLevelCacheImpl cache = new LowLevelCacheImpl(
        LlapDaemonCacheMetrics.create("test", "1"), new DummyCachePolicy(),
        new DummyAllocator(), -1); // no cleanup thread
    long fn1 = 1, fn2 = 2;
    LlapMemoryBuffer[] fakes = new LlapMemoryBuffer[] { fb(), fb(), fb(), fb(), fb(), fb() };
    verifyRefcount(fakes, 1, 1, 1, 1, 1, 1);
    assertNull(cache.putFileData(fn1, drs(1, 2), fbs(fakes, 0, 1), 0, Priority.NORMAL));
    assertNull(cache.putFileData(fn2, drs(1, 2), fbs(fakes, 2, 3), 0, Priority.NORMAL));
    verifyCacheGet(cache, fn1, 1, 3, fakes[0], fakes[1]);
    verifyCacheGet(cache, fn2, 1, 3, fakes[2], fakes[3]);
    verifyCacheGet(cache, fn1, 2, 4, fakes[1], dr(3, 4));
    verifyRefcount(fakes, 3, 4, 3, 3, 1, 1);
    LlapMemoryBuffer[] bufsDiff = fbs(fakes, 4, 5);
    long[] mask = cache.putFileData(fn1, drs(3, 1), bufsDiff, 0, Priority.NORMAL);
    assertEquals(1, mask.length);
    assertEquals(2, mask[0]); // 2nd bit set - element 2 was already in cache.
    assertSame(fakes[0], bufsDiff[1]); // Should have been replaced
    verifyRefcount(fakes, 4, 4, 3, 3, 2, 1);
    verifyCacheGet(cache, fn1, 1, 4, fakes[0], fakes[1], fakes[4]);
    verifyRefcount(fakes, 5, 5, 3, 3, 3, 1);
  }

  private void verifyCacheGet(LowLevelCacheImpl cache, long fileId, Object... stuff) {
    DiskRangeListCreateHelper list = new DiskRangeListCreateHelper();
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
        iter = cache.getFileData(fileId, list.get(), 0);
        assertEquals(resultCount, iter.listSize());
      }
      assertTrue(iter != null);
      if (obj instanceof LlapMemoryBuffer) {
        assertTrue(iter instanceof CacheChunk);
        assertSame(obj, ((CacheChunk)iter).buffer);
      } else {
        assertTrue(iter.equals(obj));
      }
      iter = iter.next;
    }
  }

  @Test
  public void testMultiMatch() {
    Configuration conf = createConf();
    LowLevelCacheImpl cache = new LowLevelCacheImpl(
        LlapDaemonCacheMetrics.create("test", "1"), new DummyCachePolicy(),
        new DummyAllocator(), -1); // no cleanup thread
    long fn = 1;
    LlapMemoryBuffer[] fakes = new LlapMemoryBuffer[] { fb(), fb() };
    assertNull(cache.putFileData(
        fn, new DiskRange[] { dr(2, 4), dr(6, 8) }, fakes, 0, Priority.NORMAL));
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
  public void testStaleValueGet() {
    Configuration conf = createConf();
    LowLevelCacheImpl cache = new LowLevelCacheImpl(
        LlapDaemonCacheMetrics.create("test", "1"), new DummyCachePolicy(),
        new DummyAllocator(), -1); // no cleanup thread
    long fn1 = 1, fn2 = 2;
    LlapMemoryBuffer[] fakes = new LlapMemoryBuffer[] { fb(), fb(), fb() };
    assertNull(cache.putFileData(fn1, drs(1, 2), fbs(fakes, 0, 1), 0, Priority.NORMAL));
    assertNull(cache.putFileData(fn2, drs(1), fbs(fakes, 2), 0, Priority.NORMAL));
    verifyCacheGet(cache, fn1, 1, 3, fakes[0], fakes[1]);
    verifyCacheGet(cache, fn2, 1, 2, fakes[2]);
    verifyRefcount(fakes, 3, 3, 3);
    evict(cache, fakes[0]);
    evict(cache, fakes[2]);
    verifyCacheGet(cache, fn1, 1, 3, dr(1, 2), fakes[1]);
    verifyCacheGet(cache, fn2, 1, 2, dr(1, 2));
    verifyRefcount(fakes, -1, 4, -1);
  }

  @Test
  public void testStaleValueReplace() {
    Configuration conf = createConf();
    LowLevelCacheImpl cache = new LowLevelCacheImpl(
        LlapDaemonCacheMetrics.create("test", "1"), new DummyCachePolicy(),
        new DummyAllocator(), -1); // no cleanup thread
    long fn1 = 1, fn2 = 2;
    LlapMemoryBuffer[] fakes = new LlapMemoryBuffer[] {
        fb(), fb(), fb(), fb(), fb(), fb(), fb(), fb(), fb() };
    assertNull(cache.putFileData(fn1, drs(1, 2, 3), fbs(fakes, 0, 1, 2), 0, Priority.NORMAL));
    assertNull(cache.putFileData(fn2, drs(1), fbs(fakes, 3), 0, Priority.NORMAL));
    evict(cache, fakes[0]);
    evict(cache, fakes[3]);
    long[] mask = cache.putFileData(
        fn1, drs(1, 2, 3, 4), fbs(fakes, 4, 5, 6, 7), 0, Priority.NORMAL);
    assertEquals(1, mask.length);
    assertEquals(6, mask[0]); // Buffers at offset 2 & 3 exist; 1 exists and is stale; 4 doesn't
    assertNull(cache.putFileData(fn2, drs(1), fbs(fakes, 8), 0, Priority.NORMAL));
    verifyCacheGet(cache, fn1, 1, 5, fakes[4], fakes[1], fakes[2], fakes[7]);
  }

  @Test
  public void testMTTWithCleanup() {
    Configuration conf = createConf();
    final LowLevelCacheImpl cache = new LowLevelCacheImpl(
        LlapDaemonCacheMetrics.create("test", "1"), new DummyCachePolicy(), new DummyAllocator(), 1);
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
              DiskRangeListCreateHelper list = new DiskRangeListCreateHelper();
              for (int j = 0; i < count; ++i) {
                list.addOrMerge(offsets[j], offsets[j] + 1, true, false);
              }

              DiskRangeList iter = cache.getFileData(fileName, list.get(), 0);
              int j = -1;
              while (iter != null) {
                ++j;
                if (!(iter instanceof CacheChunk)) {
                  iter = iter.next;
                  continue;
                }
                ++gets;
                LlapDataBuffer result = (LlapDataBuffer)((CacheChunk)iter).buffer;
                assertEquals(makeFakeArenaIndex(fileIndex, offsets[j]), result.arenaIndex);
                cache.releaseBuffer(result);
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
              LlapMemoryBuffer[] buffers = new LlapMemoryBuffer[count];
              for (int j = 0; j < offsets.length; ++j) {
                LlapDataBuffer buf = LowLevelCacheImpl.allocateFake();
                buf.arenaIndex = makeFakeArenaIndex(fileIndex, offsets[j]);
                buffers[j] = buf;
              }
              long[] mask = cache.putFileData(fileName, ranges, buffers, 0, Priority.NORMAL);
              puts += buffers.length;
              long maskVal = 0;
              if (mask != null) {
                assertEquals(1, mask.length);
                maskVal = mask[0];
              }
              for (int j = 0; j < offsets.length; ++j) {
                LlapDataBuffer buf = (LlapDataBuffer)(buffers[j]);
                if ((maskVal & 1) == 1) {
                  assertEquals(makeFakeArenaIndex(fileIndex, offsets[j]), buf.arenaIndex);
                }
                maskVal >>= 1;
                cache.releaseBuffer(buf);
              }
            }
          }
        } finally {
          rdmsDone.incrementAndGet();
        }
        return  (((long)gets) << 32) | puts;
      }

      private int makeFakeArenaIndex(int fileIndex, long offset) {
        return (int)((fileIndex << 16) + offset);
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
          head = cache.getFileData(fileId, head, 0);
          DiskRange[] results = head.listToArray();
          int startIndex = rdm.nextInt(results.length), index = startIndex;
          LlapDataBuffer victim = null;
          do {
            DiskRange r = results[index];
            if (r instanceof CacheChunk) {
              LlapDataBuffer result = (LlapDataBuffer)((CacheChunk)r).buffer;
              cache.releaseBuffer(result);
              if (victim == null && result.invalidate()) {
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

  private void evict(LowLevelCacheImpl cache, LlapMemoryBuffer fake) {
    LlapDataBuffer victimBuffer = (LlapDataBuffer)fake;
    int refCount = victimBuffer.getRefCount();
    for (int i = 0; i < refCount; ++i) {
      victimBuffer.decRef();
    }
    assertTrue(victimBuffer.invalidate());
    cache.notifyEvicted(victimBuffer);
  }

  private void verifyRefcount(LlapMemoryBuffer[] fakes, int... refCounts) {
    for (int i = 0; i < refCounts.length; ++i) {
      assertEquals("At " + i, refCounts[i], ((LlapDataBuffer)fakes[i]).getRefCount());
    }
  }

  private LlapMemoryBuffer[] fbs(LlapMemoryBuffer[] fakes, int... indexes) {
    LlapMemoryBuffer[] rv = new LlapMemoryBuffer[indexes.length];
    for (int i = 0; i < indexes.length; ++i) {
      rv[i] = (indexes[i] == -1) ? null : fakes[indexes[i]];
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

  private Configuration createConf() {
    Configuration conf = new Configuration();
    conf.setInt(ConfVars.LLAP_ORC_CACHE_MIN_ALLOC.varname, 3);
    conf.setInt(ConfVars.LLAP_ORC_CACHE_MAX_ALLOC.varname, 8);
    conf.setInt(ConfVars.LLAP_ORC_CACHE_ARENA_SIZE.varname, 8);
    conf.setLong(ConfVars.LLAP_ORC_CACHE_MAX_SIZE.varname, 8);
    return conf;
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
