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
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.cache.BufferPool.WeakBuffer;
import org.junit.Assume;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestLrfuCachePolicy {
  private static final Log LOG = LogFactory.getLog(TestLrfuCachePolicy.class);

  @Test
  public void testHeapSize1() {
    testHeapSize(1);
  }

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

  @Test
  public void testLfuExtreme() {
    int heapSize = 4;
    LOG.info("Testing lambda 0 (LFU)");
    Random rdm = new Random(1234);
    HiveConf conf = new HiveConf();
    ArrayList<WeakBuffer> inserted = new ArrayList<WeakBuffer>(heapSize);
    conf.setFloat(HiveConf.ConfVars.LLAP_LRFU_LAMBDA.varname, 0.0f);
    LrfuCachePolicy lfu = new LrfuCachePolicy(conf, 1, heapSize);
    for (int i = 0; i < heapSize; ++i) {
      WeakBuffer buffer = BufferPool.allocateFake();
      assertNull(cache(lfu, buffer));
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
    verifyOrder(lfu, inserted);
  }

  @Test
  public void testLruExtreme() {
    int heapSize = 4;
    LOG.info("Testing lambda 1 (LRU)");
    Random rdm = new Random(1234);
    HiveConf conf = new HiveConf();
    ArrayList<WeakBuffer> inserted = new ArrayList<WeakBuffer>(heapSize);
    conf.setFloat(HiveConf.ConfVars.LLAP_LRFU_LAMBDA.varname, 1.0f);
    LrfuCachePolicy lru = new LrfuCachePolicy(conf, 1, heapSize);
    for (int i = 0; i < heapSize; ++i) {
      WeakBuffer buffer = BufferPool.allocateFake();
      assertNull(cache(lru, buffer));
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
    verifyOrder(lru, inserted);
  }

  @Test
  public void testDeadlockResolution() {
    int heapSize = 4;
    LOG.info("Testing deadlock resolution");
    ArrayList<WeakBuffer> inserted = new ArrayList<WeakBuffer>(heapSize);
    LrfuCachePolicy lrfu = new LrfuCachePolicy(new HiveConf(), 1, heapSize);
    for (int i = 0; i < heapSize; ++i) {
      WeakBuffer buffer = BufferPool.allocateFake();
      assertNull(cache(lrfu, buffer));
      inserted.add(buffer);
    }
    // Lock the lowest priority buffer; try to evict - we'll evict some other buffer.
    WeakBuffer locked = inserted.get(0);
    lock(lrfu, locked);
    WeakBuffer evicted = lrfu.evictOneMoreBlock();
    assertNotNull(evicted);
    assertTrue(evicted.isInvalid());
    assertNotSame(locked, evicted);
    unlock(lrfu, locked);
  }

  // Buffers in test are fakes not linked to cache; notify cache policy explicitly.
  public WeakBuffer cache(LrfuCachePolicy lrfu, WeakBuffer buffer) {
    buffer.lock(false);
    WeakBuffer result = lrfu.cache(buffer);
    buffer.unlock();
    if (result != CachePolicy.CANNOT_EVICT) {
      lrfu.notifyUnlock(buffer);
    }
    return result;
  }

  private static void lock(LrfuCachePolicy lrfu, WeakBuffer locked) {
    locked.lock(false);
    lrfu.notifyLock(locked);
  }

  private static void unlock(LrfuCachePolicy lrfu, WeakBuffer locked) {
    locked.unlock();
    lrfu.notifyUnlock(locked);
  }

  private void testHeapSize(int heapSize) {
    LOG.info("Testing heap size " + heapSize);
    Random rdm = new Random(1234);
    HiveConf conf = new HiveConf();
    conf.setFloat(HiveConf.ConfVars.LLAP_LRFU_LAMBDA.varname, 0.01f);
    LrfuCachePolicy lrfu = new LrfuCachePolicy(conf, 1, heapSize);
    // Insert the number of elements plus 2, to trigger 2 evictions.
    int toEvict = 2;
    ArrayList<WeakBuffer> inserted = new ArrayList<WeakBuffer>(heapSize);
    WeakBuffer[] evicted = new WeakBuffer[toEvict];
    Assume.assumeTrue(toEvict <= heapSize);
    for (int i = 0; i < heapSize + toEvict; ++i) {
      WeakBuffer buffer = BufferPool.allocateFake();
      WeakBuffer evictedBuf = cache(lrfu, buffer);
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
    for (WeakBuffer buf : inserted) {
      lock(lrfu, buf);
    }
    WeakBuffer block = lrfu.evictOneMoreBlock();
    assertNull("Got " + block, block);
    assertSame(CachePolicy.CANNOT_EVICT, cache(lrfu, BufferPool.allocateFake()));
    for (WeakBuffer buf : inserted) {
      unlock(lrfu, buf);
    }
    // To make (almost) sure we get definite order, touch blocks in order large number of times.
    for (WeakBuffer buf : inserted) {
      // TODO: this seems to indicate that priorities change too little...
      //       perhaps we need to adjust the policy.
      for (int j = 0; j < 10; ++j) {
        lrfu.notifyLock(buf);
        lrfu.notifyUnlock(buf);
      }
    }
    verifyOrder(lrfu, inserted);
  }

  private void verifyOrder(LrfuCachePolicy lrfu, ArrayList<WeakBuffer> inserted) {
    WeakBuffer block;
    // Evict all blocks.
    for (int i = 0; i < inserted.size(); ++i) {
      block = lrfu.evictOneMoreBlock();
      assertTrue(block.isInvalid());
      assertSame(inserted.get(i), block);
    }
    // The map should now be empty.
    assertNull(lrfu.evictOneMoreBlock());
  }

  private String dumpInserted(ArrayList<WeakBuffer> inserted) {
    String debugStr = "";
    for (int i = 0; i < inserted.size(); ++i) {
      if (i != 0) debugStr += ", ";
      debugStr += inserted.get(i);
    }
    return debugStr;
  }
}
