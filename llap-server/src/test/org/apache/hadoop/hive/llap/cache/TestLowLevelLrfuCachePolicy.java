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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assume;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestLowLevelLrfuCachePolicy {
  private static final Log LOG = LogFactory.getLog(TestLowLevelLrfuCachePolicy.class);

  @Test
  public void testHeapSize2() {
    testHeapSize(2);
  }

  @Test
  public void testHeapSize7() {
    testHeapSize(7);
  }

  @Test
  public void testHeapSize8() {
    testHeapSize(8);
  }

  @Test
  public void testHeapSize30() {
    testHeapSize(30);
  }

  private class EvictionTracker implements EvictionListener {
    public List<LlapCacheableBuffer> evicted = new ArrayList<LlapCacheableBuffer>();

    @Override
    public void notifyEvicted(LlapCacheableBuffer buffer) {
      evicted.add(buffer);
    }
  }

  @Test
  public void testLfuExtreme() {
    int heapSize = 4;
    LOG.info("Testing lambda 0 (LFU)");
    Random rdm = new Random(1234);
    HiveConf conf = new HiveConf();
    ArrayList<LlapCacheableBuffer> inserted = new ArrayList<LlapCacheableBuffer>(heapSize);
    conf.setFloat(HiveConf.ConfVars.LLAP_LRFU_LAMBDA.varname, 0.0f);
    EvictionTracker et = new EvictionTracker();
    LowLevelLrfuCachePolicy lfu = new LowLevelLrfuCachePolicy(conf, 1, heapSize, et);
    for (int i = 0; i < heapSize; ++i) {
      LlapCacheableBuffer buffer = LowLevelBuddyCache.allocateFake();
      assertTrue(cache(lfu, et, buffer));
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
    verifyOrder(lfu, et, inserted);
  }

  @Test
  public void testLruExtreme() {
    int heapSize = 4;
    LOG.info("Testing lambda 1 (LRU)");
    Random rdm = new Random(1234);
    HiveConf conf = new HiveConf();
    ArrayList<LlapCacheableBuffer> inserted = new ArrayList<LlapCacheableBuffer>(heapSize);
    conf.setFloat(HiveConf.ConfVars.LLAP_LRFU_LAMBDA.varname, 1.0f);
    EvictionTracker et = new EvictionTracker();
    LowLevelLrfuCachePolicy lru = new LowLevelLrfuCachePolicy(conf, 1, heapSize, et);
    for (int i = 0; i < heapSize; ++i) {
      LlapCacheableBuffer buffer = LowLevelBuddyCache.allocateFake();
      assertTrue(cache(lru, et, buffer));
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
    verifyOrder(lru, et, inserted);
  }

  @Test
  public void testDeadlockResolution() {
    int heapSize = 4;
    LOG.info("Testing deadlock resolution");
    ArrayList<LlapCacheableBuffer> inserted = new ArrayList<LlapCacheableBuffer>(heapSize);
    EvictionTracker et = new EvictionTracker();
    LowLevelLrfuCachePolicy lrfu = new LowLevelLrfuCachePolicy(new HiveConf(), 1, heapSize, et);
    for (int i = 0; i < heapSize; ++i) {
      LlapCacheableBuffer buffer = LowLevelBuddyCache.allocateFake();
      assertTrue(cache(lrfu, et, buffer));
      inserted.add(buffer);
    }
    // Lock the lowest priority buffer; try to evict - we'll evict some other buffer.
    LlapCacheableBuffer locked = inserted.get(0);
    lock(lrfu, locked);
    lrfu.reserveMemory(1, false);
    LlapCacheableBuffer evicted = et.evicted.get(0);
    assertNotNull(evicted);
    assertTrue(evicted.isInvalid());
    assertNotSame(locked, evicted);
    unlock(lrfu, locked);
  }

  private static final LlapCacheableBuffer CANNOT_EVICT = LowLevelBuddyCache.allocateFake();
  // Buffers in test are fakes not linked to cache; notify cache policy explicitly.
  public boolean cache(
      LowLevelLrfuCachePolicy lrfu, EvictionTracker et, LlapCacheableBuffer buffer) {
    if (!lrfu.reserveMemory(1, false)) {
      return false;
    }
    buffer.incRef();
    lrfu.cache(buffer);
    buffer.decRef();
    lrfu.notifyUnlock(buffer);
    return true;
  }

  private LlapCacheableBuffer getOneEvictedBuffer(EvictionTracker et) {
    assertTrue(et.evicted.size() == 0 || et.evicted.size() == 1); // test-specific
    LlapCacheableBuffer result = et.evicted.isEmpty() ? null : et.evicted.get(0);
    et.evicted.clear();
    return result;
  }

  private static void lock(LowLevelLrfuCachePolicy lrfu, LlapCacheableBuffer locked) {
    locked.incRef();
    lrfu.notifyLock(locked);
  }

  private static void unlock(LowLevelLrfuCachePolicy lrfu, LlapCacheableBuffer locked) {
    locked.decRef();
    lrfu.notifyUnlock(locked);
  }

  private void testHeapSize(int heapSize) {
    LOG.info("Testing heap size " + heapSize);
    Random rdm = new Random(1234);
    HiveConf conf = new HiveConf();
    conf.setFloat(HiveConf.ConfVars.LLAP_LRFU_LAMBDA.varname, 0.05f); // very small heap? TODO#
    EvictionTracker et = new EvictionTracker();
    LowLevelLrfuCachePolicy lrfu = new LowLevelLrfuCachePolicy(conf, 1, heapSize, et);
    // Insert the number of elements plus 2, to trigger 2 evictions.
    int toEvict = 2;
    ArrayList<LlapCacheableBuffer> inserted = new ArrayList<LlapCacheableBuffer>(heapSize);
    LlapCacheableBuffer[] evicted = new LlapCacheableBuffer[toEvict];
    Assume.assumeTrue(toEvict <= heapSize);
    for (int i = 0; i < heapSize + toEvict; ++i) {
      LlapCacheableBuffer buffer = LowLevelBuddyCache.allocateFake();
      assertTrue(cache(lrfu, et, buffer));
      LlapCacheableBuffer evictedBuf = getOneEvictedBuffer(et);
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
    for (LlapCacheableBuffer buf : inserted) {
      lock(lrfu, buf);
    }
    assertFalse(lrfu.reserveMemory(1, false));
    if (!et.evicted.isEmpty()) {
      assertTrue("Got " + et.evicted.get(0), et.evicted.isEmpty());
    }
    for (LlapCacheableBuffer buf : inserted) {
      unlock(lrfu, buf);
    }
    // To make (almost) sure we get definite order, touch blocks in order large number of times.
    for (LlapCacheableBuffer buf : inserted) {
      // TODO: this seems to indicate that priorities change too little...
      //       perhaps we need to adjust the policy.
      for (int j = 0; j < 10; ++j) {
        lrfu.notifyLock(buf);
        lrfu.notifyUnlock(buf);
      }
    }
    verifyOrder(lrfu, et, inserted);
  }

  private void verifyOrder(LowLevelLrfuCachePolicy lrfu,
      EvictionTracker et, ArrayList<LlapCacheableBuffer> inserted) {
    LlapCacheableBuffer block;
    // Evict all blocks.
    et.evicted.clear();
    for (int i = 0; i < inserted.size(); ++i) {
      assertTrue(lrfu.reserveMemory(1, false));
    }
    // The map should now be empty.
    assertFalse(lrfu.reserveMemory(1, false));
    for (int i = 0; i < inserted.size(); ++i) {
      block = et.evicted.get(i);
      assertTrue(block.isInvalid());
      assertSame(inserted.get(i), block);
    }
  }

  private String dumpInserted(ArrayList<LlapCacheableBuffer> inserted) {
    String debugStr = "";
    for (int i = 0; i < inserted.size(); ++i) {
      if (i != 0) debugStr += ", ";
      debugStr += inserted.get(i);
    }
    return debugStr;
  }
}
