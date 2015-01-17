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

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.RuntimeErrorException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.io.api.cache.LlapMemoryBuffer;
import org.junit.Assume;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestLowLevelCacheImpl {
  private static final Log LOG = LogFactory.getLog(TestLowLevelCacheImpl.class);

  private static class DummyAllocator implements Allocator {
    @Override
    public boolean allocateMultiple(LlapMemoryBuffer[] dest, int size) {
      for (int i = 0; i < dest.length; ++i) {
        LlapCacheableBuffer buf = new LlapCacheableBuffer();
        buf.initialize(0, null, -1, size);
        dest[i] = buf;
      }
      return true;
    }

    @Override
    public void deallocate(LlapMemoryBuffer buffer) {
    }
  }

  private static class DummyCachePolicy extends LowLevelCachePolicyBase {
    public DummyCachePolicy(long maxSize) {
      super(maxSize);
    }

    public void cache(LlapCacheableBuffer buffer) {
    }

    public void notifyLock(LlapCacheableBuffer buffer) {
    }

    public void notifyUnlock(LlapCacheableBuffer buffer) {
    }

    protected long evictSomeBlocks(long memoryToReserve, EvictionListener listener) {
      return memoryToReserve;
    }
  }

  @Test
  public void testGetPut() {
    Configuration conf = createConf();
    LowLevelCacheImpl cache = new LowLevelCacheImpl(
        conf, new DummyCachePolicy(10), new DummyAllocator(), -1); // no cleanup thread
    String fn1 = "file1".intern(), fn2 = "file2".intern();
    LlapMemoryBuffer[] fakes = new LlapMemoryBuffer[] { fb(), fb(), fb(), fb(), fb(), fb() };
    verifyRefcount(fakes, 1, 1, 1, 1, 1, 1);
    assertNull(cache.putFileData(fn1, new long[] { 1, 2 }, fbs(fakes, 0, 1)));
    assertNull(cache.putFileData(fn2, new long[] { 1, 2 }, fbs(fakes, 2, 3)));
    assertArrayEquals(fbs(fakes, 0, 1), cache.getFileData(fn1, new long[] { 1, 2 }));
    assertArrayEquals(fbs(fakes, 2, 3), cache.getFileData(fn2, new long[] { 1, 2 }));
    assertArrayEquals(fbs(fakes, 1, -1), cache.getFileData(fn1, new long[] { 2, 3 }));
    verifyRefcount(fakes, 2, 3, 2, 2, 1, 1);
    LlapMemoryBuffer[] bufsDiff = fbs(fakes, 4, 5);
    long[] mask = cache.putFileData(fn1, new long[] { 3, 1 }, bufsDiff);
    assertEquals(1, mask.length);
    assertEquals(2, mask[0]); // 2nd bit set - element 2 was already in cache.
    assertSame(fakes[0], bufsDiff[1]); // Should have been replaced
    verifyRefcount(fakes, 3, 3, 2, 2, 1, 0);
    assertArrayEquals(fbs(fakes, 0, 1, 4), cache.getFileData(fn1, new long[] { 1, 2, 3 }));
    verifyRefcount(fakes, 4, 4, 2, 2, 2, 0);
  }

  @Test
  public void testStaleValueGet() {
    Configuration conf = createConf();
    LowLevelCacheImpl cache = new LowLevelCacheImpl(
        conf, new DummyCachePolicy(10), new DummyAllocator(), -1); // no cleanup thread
    String fn1 = "file1".intern(), fn2 = "file2".intern();
    LlapMemoryBuffer[] fakes = new LlapMemoryBuffer[] { fb(), fb(), fb() };
    assertNull(cache.putFileData(fn1, new long[] { 1, 2 }, fbs(fakes, 0, 1)));
    assertNull(cache.putFileData(fn2, new long[] { 1 }, fbs(fakes, 2)));
    assertArrayEquals(fbs(fakes, 0, 1), cache.getFileData(fn1, new long[] { 1, 2 }));
    assertArrayEquals(fbs(fakes, 2), cache.getFileData(fn2, new long[] { 1 }));
    verifyRefcount(fakes, 2, 2, 2);
    evict(cache, fakes[0]);
    evict(cache, fakes[2]);
    assertArrayEquals(fbs(fakes, -1, 1), cache.getFileData(fn1, new long[] { 1, 2 }));
    assertNull(cache.getFileData(fn2, new long[] { 1 }));
    verifyRefcount(fakes, -1, 3, -1);
  }

  @Test
  public void testStaleValueReplace() {
    Configuration conf = createConf();
    LowLevelCacheImpl cache = new LowLevelCacheImpl(
        conf, new DummyCachePolicy(10), new DummyAllocator(), -1); // no cleanup thread
    String fn1 = "file1".intern(), fn2 = "file2".intern();
    LlapMemoryBuffer[] fakes = new LlapMemoryBuffer[] {
        fb(), fb(), fb(), fb(), fb(), fb(), fb(), fb(), fb() };
    assertNull(cache.putFileData(fn1, new long[] { 1, 2, 3 }, fbs(fakes, 0, 1, 2)));
    assertNull(cache.putFileData(fn2, new long[] { 1 }, fbs(fakes, 3)));
    evict(cache, fakes[0]);
    evict(cache, fakes[3]);
    long[] mask = cache.putFileData(fn1, new long[] { 1, 2, 3, 4 }, fbs(fakes, 4, 5, 6, 7));
    assertEquals(1, mask.length);
    assertEquals(6, mask[0]); // Buffers at offset 2 & 3 exist; 1 exists and is stale; 4 doesn't
    assertNull(cache.putFileData(fn2, new long[] { 1 }, fbs(fakes, 8)));
    assertArrayEquals(fbs(fakes, 4, 2, 3, 7), cache.getFileData(fn1, new long[] { 1, 2, 3, 4 }));
  }

  @Test
  public void testMTTWithCleanup() {
    Configuration conf = createConf();
    final LowLevelCacheImpl cache = new LowLevelCacheImpl(
        conf, new DummyCachePolicy(10), new DummyAllocator(), 1);
    final String fn1 = "file1".intern(), fn2 = "file2".intern();
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
            String fileName = isFn1 ? fn1 : fn2;
            int fileIndex = isFn1 ? 1 : 2;
            int count = rdm.nextInt(offsetsToUse);
            long[] offsets = new long[count];
            for (int j = 0; j < offsets.length; ++j) {
              offsets[j] = rdm.nextInt(offsetsToUse);
            }
            if (isGet) {
              LlapMemoryBuffer[] results = cache.getFileData(fileName, offsets);
              if (results == null) continue;
              for (int j = 0; j < offsets.length; ++j) {
                if (results[j] == null) continue;
                ++gets;
                LlapCacheableBuffer result = (LlapCacheableBuffer)(results[j]);
                assertEquals(makeFakeArenaIndex(fileIndex, offsets[j]), result.arenaIndex);
                result.decRef();
              }
            } else {
              LlapMemoryBuffer[] buffers = new LlapMemoryBuffer[count];
              for (int j = 0; j < offsets.length; ++j) {
                LlapCacheableBuffer buf = LowLevelCacheImpl.allocateFake();
                buf.incRef();
                buf.arenaIndex = makeFakeArenaIndex(fileIndex, offsets[j]);
                buffers[j] = buf;
              }
              long[] mask = cache.putFileData(fileName, offsets, buffers);
              puts += buffers.length;
              long maskVal = 0;
              if (mask != null) {
                assertEquals(1, mask.length);
                maskVal = mask[0];
              }
              for (int j = 0; j < offsets.length; ++j) {
                LlapCacheableBuffer buf = (LlapCacheableBuffer)(buffers[j]);
                if ((maskVal & 1) == 1) {
                  assertEquals(makeFakeArenaIndex(fileIndex, offsets[j]), buf.arenaIndex);
                }
                maskVal >>= 1;
                buf.decRef();
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
        long[] offsets = new long[offsetsToUse];
        Random rdm = new Random(1234 + Thread.currentThread().getId());
        for (int i = 0; i < offsetsToUse; ++i) {
          offsets[i] = i;
        }
        int evictions = 0;
        syncThreadStart(cdlIn, cdlOut);
        while (rdmsDone.get() < 3) {
          isFirstFile = !isFirstFile;
          String fileName = isFirstFile ? fn1 : fn2;
          LlapMemoryBuffer[] results = cache.getFileData(fileName, offsets);
          if (results == null) continue;
          int startIndex = rdm.nextInt(results.length), index = startIndex;
          LlapCacheableBuffer victim = null;
          do {
            if (results[index] != null) {
              LlapCacheableBuffer result = (LlapCacheableBuffer)results[index];
              result.decRef();
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
    LlapCacheableBuffer victimBuffer = (LlapCacheableBuffer)fake;
    int refCount = victimBuffer.getRefCount();
    for (int i = 0; i < refCount; ++i) {
      victimBuffer.decRef();
    }
    assertTrue(victimBuffer.invalidate());
    cache.notifyEvicted(victimBuffer);
  }

  private void verifyRefcount(LlapMemoryBuffer[] fakes, int... refCounts) {
    for (int i = 0; i < refCounts.length; ++i) {
      assertEquals(refCounts[i], ((LlapCacheableBuffer)fakes[i]).getRefCount());
    }
  }

  private LlapMemoryBuffer[] fbs(LlapMemoryBuffer[] fakes, int... indexes) {
    LlapMemoryBuffer[] rv = new LlapMemoryBuffer[indexes.length];
    for (int i = 0; i < indexes.length; ++i) {
      rv[i] = (indexes[i] == -1) ? null : fakes[indexes[i]];
    }
    return rv;
  }

  private LlapCacheableBuffer fb() {
    LlapCacheableBuffer fake = LowLevelCacheImpl.allocateFake();
    fake.incRef();
    return fake;
  }

  private Configuration createConf() {
    Configuration conf = new Configuration();
    conf.setInt(ConfVars.LLAP_ORC_CACHE_MIN_ALLOC.varname, 3);
    conf.setInt(ConfVars.LLAP_ORC_CACHE_MAX_ALLOC.varname, 8);
    conf.setInt(ConfVars.LLAP_ORC_CACHE_ARENA_SIZE.varname, 8);
    conf.setLong(ConfVars.LLAP_ORC_CACHE_MAX_SIZE.varname, 8);
    return conf;
  }
}
