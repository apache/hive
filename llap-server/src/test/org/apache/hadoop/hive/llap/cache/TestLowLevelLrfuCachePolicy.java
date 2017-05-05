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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.cache.LowLevelCache.Priority;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLowLevelLrfuCachePolicy {
  private static final Logger LOG = LoggerFactory.getLogger(TestLowLevelLrfuCachePolicy.class);

  @Test
  public void testRegression_HIVE_12178() throws Exception {
    LOG.info("Testing wrong list status after eviction");
    EvictionTracker et = new EvictionTracker();
    int memSize = 2;
    Configuration conf = new Configuration();
    // Set lambda to 1 so the heap size becomes 1 (LRU).
    conf.setDouble(HiveConf.ConfVars.LLAP_LRFU_LAMBDA.varname, 1.0f);
    final LowLevelLrfuCachePolicy lrfu = new LowLevelLrfuCachePolicy(1, memSize, conf);
    Field f = LowLevelLrfuCachePolicy.class.getDeclaredField("listLock");
    f.setAccessible(true);
    ReentrantLock listLock = (ReentrantLock)f.get(lrfu);
    LowLevelCacheMemoryManager mm = new LowLevelCacheMemoryManager(memSize, lrfu,
        LlapDaemonCacheMetrics.create("test", "1"));
    lrfu.setEvictionListener(et);
    final LlapDataBuffer buffer1 = LowLevelCacheImpl.allocateFake();
    LlapDataBuffer buffer2 = LowLevelCacheImpl.allocateFake();
    assertTrue(cache(mm, lrfu, et, buffer1));
    assertTrue(cache(mm, lrfu, et, buffer2));
    // buffer2 is now in the heap, buffer1 is in the list. "Use" buffer1 again;
    // before we notify though, lock the list, so lock cannot remove it from the list.
    buffer1.incRef();
    assertEquals(LlapCacheableBuffer.IN_LIST, buffer1.indexInHeap);
    listLock.lock();
    try {
      Thread otherThread = new Thread(new Runnable() {
        public void run() {
          lrfu.notifyLock(buffer1);
        }
      });
      otherThread.start();
      otherThread.join();
    } finally {
      listLock.unlock();
    }
    // Now try to evict with locked buffer still in the list.
    mm.reserveMemory(1, false);
    assertSame(buffer2, et.evicted.get(0));
    unlock(lrfu, buffer1);
  }

  @Test
  public void testHeapSize2() {
    testHeapSize(2);
  }

  @Test
  public void testHeapSize8() {
    testHeapSize(8);
  }

  @Test
  public void testHeapSize30() {
    testHeapSize(30);
  }

  @Test
  public void testHeapSize64() {
    testHeapSize(64);
  }

  private class EvictionTracker implements EvictionListener {
    public List<LlapDataBuffer> evicted = new ArrayList<LlapDataBuffer>();

    @Override
    public void notifyEvicted(LlapCacheableBuffer buffer) {
      evicted.add((LlapDataBuffer)buffer);
    }
  }

  @Test
  public void testLfuExtreme() {
    int heapSize = 4;
    LOG.info("Testing lambda 0 (LFU)");
    Random rdm = new Random(1234);
    Configuration conf = new Configuration();
    ArrayList<LlapDataBuffer> inserted = new ArrayList<LlapDataBuffer>(heapSize);
    conf.setFloat(HiveConf.ConfVars.LLAP_LRFU_LAMBDA.varname, 0.0f);
    EvictionTracker et = new EvictionTracker();
    LowLevelLrfuCachePolicy lfu = new LowLevelLrfuCachePolicy(1, heapSize, conf);
    LowLevelCacheMemoryManager mm = new LowLevelCacheMemoryManager(heapSize, lfu,
        LlapDaemonCacheMetrics.create("test", "1"));
    lfu.setEvictionListener(et);
    for (int i = 0; i < heapSize; ++i) {
      LlapDataBuffer buffer = LowLevelCacheImpl.allocateFake();
      assertTrue(cache(mm, lfu, et, buffer));
      inserted.add(buffer);
    }
    Collections.shuffle(inserted, rdm);
    // LFU extreme, order of accesses should be ignored, only frequency matters.
    // We touch first elements later, but do it less times, so they will be evicted first.
    for (int i = inserted.size() - 1; i >= 0; --i) {
      for (int j = 0; j < i + 1; ++j) {
        lfu.notifyLock(inserted.get(i));
        lfu.notifyUnlock(inserted.get(i));
      }
    }
    verifyOrder(mm, lfu, et, inserted, null);
  }

  @Test
  public void testLruExtreme() {
    int heapSize = 4;
    LOG.info("Testing lambda 1 (LRU)");
    Random rdm = new Random(1234);
    Configuration conf = new Configuration();
    ArrayList<LlapDataBuffer> inserted = new ArrayList<LlapDataBuffer>(heapSize);
    conf.setFloat(HiveConf.ConfVars.LLAP_LRFU_LAMBDA.varname, 1.0f);
    EvictionTracker et = new EvictionTracker();
    LowLevelLrfuCachePolicy lru = new LowLevelLrfuCachePolicy(1, heapSize, conf);
    LowLevelCacheMemoryManager mm = new LowLevelCacheMemoryManager(heapSize, lru,
        LlapDaemonCacheMetrics.create("test", "1"));
    lru.setEvictionListener(et);
    for (int i = 0; i < heapSize; ++i) {
      LlapDataBuffer buffer = LowLevelCacheImpl.allocateFake();
      assertTrue(cache(mm, lru, et, buffer));
      inserted.add(buffer);
    }
    Collections.shuffle(inserted, rdm);
    // LRU extreme, frequency of accesses should be ignored, only order matters.
    for (int i = 0; i < inserted.size(); ++i) {
      for (int j = 0; j < (inserted.size() - i); ++j) {
        lru.notifyLock(inserted.get(i));
        lru.notifyUnlock(inserted.get(i));
      }
    }
    verifyOrder(mm, lru, et, inserted, null);
  }

  @Test
  public void testDeadlockResolution() {
    int heapSize = 4;
    LOG.info("Testing deadlock resolution");
    ArrayList<LlapDataBuffer> inserted = new ArrayList<LlapDataBuffer>(heapSize);
    EvictionTracker et = new EvictionTracker();
    Configuration conf = new Configuration();
    LowLevelLrfuCachePolicy lrfu = new LowLevelLrfuCachePolicy(1, heapSize, conf);
    LowLevelCacheMemoryManager mm = new LowLevelCacheMemoryManager(heapSize, lrfu,
        LlapDaemonCacheMetrics.create("test", "1"));
    lrfu.setEvictionListener(et);
    for (int i = 0; i < heapSize; ++i) {
      LlapDataBuffer buffer = LowLevelCacheImpl.allocateFake();
      assertTrue(cache(mm, lrfu, et, buffer));
      inserted.add(buffer);
    }
    // Lock the lowest priority buffer; try to evict - we'll evict some other buffer.
    LlapDataBuffer locked = inserted.get(0);
    lock(lrfu, locked);
    mm.reserveMemory(1, false);
    LlapDataBuffer evicted = et.evicted.get(0);
    assertNotNull(evicted);
    assertTrue(evicted.isInvalid());
    assertNotSame(locked, evicted);
    unlock(lrfu, locked);
  }
  

  @Test
  public void testForceEvictBySize() {
    int heapSize = 12;
    LOG.info("Testing force-eviction out of order");
    Configuration conf = new Configuration();
    ArrayList<LlapDataBuffer> sizeTwo = new ArrayList<LlapDataBuffer>(4),
        sizeOne = new ArrayList<LlapDataBuffer>(4);
    conf.setFloat(HiveConf.ConfVars.LLAP_LRFU_LAMBDA.varname, 0.45f); // size of LFU heap is 4
    EvictionTracker et = new EvictionTracker();
    LowLevelLrfuCachePolicy lrfu = new LowLevelLrfuCachePolicy(1, heapSize, conf);
    lrfu.setEvictionListener(et);
    for (int i = 0; i < 2; ++i) {
      sizeTwo.add(cacheSizeTwoFake(et, lrfu));
      for (int j = 0; j < 2; ++j) {
        LlapDataBuffer fake = LowLevelCacheImpl.allocateFake();
        assertTrue(cache(null, lrfu, et, fake));
        sizeOne.add(fake);
      }
      sizeTwo.add(cacheSizeTwoFake(et, lrfu));
    }
    // Now we should have two in the heap and two in the list, which is an implementation detail.
    // Evict only big blocks.
    et.evicted.clear();
    assertEquals(8, lrfu.tryEvictContiguousData(2, 4));
    for (int i = 0; i < sizeTwo.size(); ++i) {
      LlapDataBuffer block = et.evicted.get(i);
      assertTrue(block.isInvalid());
      assertSame(sizeTwo.get(i), block);
    }
    et.evicted.clear();
    // Evict small blocks when no big ones are available.
    assertEquals(2, lrfu.tryEvictContiguousData(2, 1));
    for (int i = 0; i < 2; ++i) {
	  LlapDataBuffer block = et.evicted.get(i);
	  assertTrue(block.isInvalid());
	  assertSame(sizeOne.get(i), block);
	}
    et.evicted.clear();
    // Evict the rest.
    assertEquals(2, lrfu.evictSomeBlocks(3));
    for (int i = 2; i < sizeOne.size(); ++i) {
      LlapDataBuffer block = et.evicted.get(i - 2);
      assertTrue(block.isInvalid());
      assertSame(sizeOne.get(i), block);
    }
  }

  private LlapDataBuffer cacheSizeTwoFake(EvictionTracker et, LowLevelLrfuCachePolicy lrfu) {
    LlapDataBuffer fake = new LlapDataBuffer();
    fake.initialize(-1, ByteBuffer.wrap(new byte[2]), 0, 2);
    assertTrue(cache(null, lrfu, et, fake));
    return fake;
  }

  // Buffers in test are fakes not linked to cache; notify cache policy explicitly.
  public boolean cache(LowLevelCacheMemoryManager mm,
      LowLevelLrfuCachePolicy lrfu, EvictionTracker et, LlapDataBuffer buffer) {
    if (mm != null && !mm.reserveMemory(1, false)) {
      return false;
    }
    buffer.incRef();
    lrfu.cache(buffer, Priority.NORMAL);
    buffer.decRef();
    lrfu.notifyUnlock(buffer);
    return true;
  }

  private LlapDataBuffer getOneEvictedBuffer(EvictionTracker et) {
    assertTrue(et.evicted.size() == 0 || et.evicted.size() == 1); // test-specific
    LlapDataBuffer result = et.evicted.isEmpty() ? null : et.evicted.get(0);
    et.evicted.clear();
    return result;
  }

  private static void lock(LowLevelLrfuCachePolicy lrfu, LlapDataBuffer locked) {
    locked.incRef();
    lrfu.notifyLock(locked);
  }

  private static void unlock(LowLevelLrfuCachePolicy lrfu, LlapDataBuffer locked) {
    locked.decRef();
    lrfu.notifyUnlock(locked);
  }

  private static class MetricsMock {
    public MetricsMock(AtomicLong cacheUsed, LlapDaemonCacheMetrics metricsMock) {
      this.cacheUsed = cacheUsed;
      this.metricsMock = metricsMock;
    }
    public AtomicLong cacheUsed;
    public LlapDaemonCacheMetrics metricsMock;
  }

  private MetricsMock createMetricsMock() {
    LlapDaemonCacheMetrics metricsMock = mock(LlapDaemonCacheMetrics.class);
    final AtomicLong cacheUsed = new AtomicLong(0);
    doAnswer(new Answer<Object>() {
      public Object answer(InvocationOnMock invocation) throws Throwable {
        cacheUsed.addAndGet((Long)invocation.getArguments()[0]);
        return null;
      }
    }).when(metricsMock).incrCacheCapacityUsed(anyLong());
    return new MetricsMock(cacheUsed, metricsMock);
  }

  private void testHeapSize(int heapSize) {
    LOG.info("Testing heap size " + heapSize);
    Random rdm = new Random(1234);
    Configuration conf = new Configuration();
    conf.setFloat(HiveConf.ConfVars.LLAP_LRFU_LAMBDA.varname, 0.2f); // very small heap, 14 elements
    EvictionTracker et = new EvictionTracker();
    LowLevelLrfuCachePolicy lrfu = new LowLevelLrfuCachePolicy(1, heapSize, conf);
    MetricsMock m = createMetricsMock();
    LowLevelCacheMemoryManager mm = new LowLevelCacheMemoryManager(heapSize, lrfu, m.metricsMock);
    lrfu.setEvictionListener(et);
    // Insert the number of elements plus 2, to trigger 2 evictions.
    int toEvict = 2;
    ArrayList<LlapDataBuffer> inserted = new ArrayList<LlapDataBuffer>(heapSize);
    LlapDataBuffer[] evicted = new LlapDataBuffer[toEvict];
    Assume.assumeTrue(toEvict <= heapSize);
    for (int i = 0; i < heapSize + toEvict; ++i) {
      LlapDataBuffer buffer = LowLevelCacheImpl.allocateFake();
      assertTrue(cache(mm, lrfu, et, buffer));
      assertEquals((long)Math.min(i + 1, heapSize), m.cacheUsed.get());
      LlapDataBuffer evictedBuf = getOneEvictedBuffer(et);
      if (i < toEvict) {
        evicted[i] = buffer;
      } else {
        if (i >= heapSize) {
          assertSame(evicted[i - heapSize], evictedBuf);
          assertTrue(evictedBuf.isInvalid());
        } else {
          assertNull(evictedBuf);
        }
        inserted.add(buffer);
      }
    }
    LOG.info("Inserted " + dumpInserted(inserted));
    // We will touch all blocks in random order.
    Collections.shuffle(inserted, rdm);
    LOG.info("Touch order " + dumpInserted(inserted));
    // Lock entire heap; heap is still full; we should not be able to evict or insert.
    for (LlapDataBuffer buf : inserted) {
      lock(lrfu, buf);
    }
    assertEquals(heapSize, m.cacheUsed.get());
    assertFalse(mm.reserveMemory(1, false));
    if (!et.evicted.isEmpty()) {
      assertTrue("Got " + et.evicted.get(0), et.evicted.isEmpty());
    }
    for (LlapDataBuffer buf : inserted) {
      unlock(lrfu, buf);
    }
    // To make (almost) sure we get definite order, touch blocks in order large number of times.
    for (LlapDataBuffer buf : inserted) {
      // TODO: this seems to indicate that priorities change too little...
      //       perhaps we need to adjust the policy.
      for (int j = 0; j < 10; ++j) {
        lrfu.notifyLock(buf);
        lrfu.notifyUnlock(buf);
      }
    }
    verifyOrder(mm, lrfu, et, inserted, m.cacheUsed);
  }

  private void verifyOrder(LowLevelCacheMemoryManager mm, LowLevelLrfuCachePolicy lrfu,
      EvictionTracker et, ArrayList<LlapDataBuffer> inserted, AtomicLong cacheUsed) {
    LlapDataBuffer block;
    // Evict all blocks.
    et.evicted.clear();
    for (int i = 0; i < inserted.size(); ++i) {
      assertTrue(mm.reserveMemory(1, false));
      if (cacheUsed != null) {
        assertEquals(inserted.size(), cacheUsed.get());
      }
    }
    // The map should now be empty.
    assertFalse(mm.reserveMemory(1, false));
    if (cacheUsed != null) {
      assertEquals(inserted.size(), cacheUsed.get());
    }
    for (int i = 0; i < inserted.size(); ++i) {
      block = et.evicted.get(i);
      assertTrue(block.isInvalid());
      assertSame(inserted.get(i), block);
    }
    if (cacheUsed != null) {
      mm.releaseMemory(inserted.size());
      assertEquals(0, cacheUsed.get());
    }
  }

  private String dumpInserted(ArrayList<LlapDataBuffer> inserted) {
    String debugStr = "";
    for (int i = 0; i < inserted.size(); ++i) {
      if (i != 0) debugStr += ", ";
      debugStr += inserted.get(i);
    }
    return debugStr;
  }
}
