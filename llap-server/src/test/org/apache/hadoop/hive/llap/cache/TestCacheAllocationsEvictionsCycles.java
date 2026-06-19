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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.io.Allocator;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.io.metadata.MetadataCache;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Test the full cycle of allocation/accounting/eviction interactions.
 */
public class TestCacheAllocationsEvictionsCycles {

  private static final Logger LOG = LoggerFactory.getLogger(TestCacheAllocationsEvictionsCycles.class);
  private static final LlapDaemonCacheMetrics CACHE_METRICS = LlapDaemonCacheMetrics.create("testCache", "testSession");

  private final long maxSize = 1024;
  private final LowLevelCache dataCache = Mockito.mock(LowLevelCache.class);
  private final SerDeLowLevelCacheImpl serdCache = Mockito.mock(SerDeLowLevelCacheImpl.class);
  private final MetadataCache metaDataCache = Mockito.mock(MetadataCache.class);

  private BuddyAllocator allocator;
  private MemoryManager memoryManager;
  private LowLevelCachePolicy cachePolicy;
  private EvictionTracker evictionTracker;

  @Before public void setUp() throws Exception {
    Configuration conf = new Configuration();
    // Set lambda to 1 so the heap size becomes 1 (LRU).
    conf.setDouble(HiveConf.ConfVars.LLAP_LRFU_LAMBDA.varname, 1.0f);
    conf.setInt(HiveConf.ConfVars.LLAP_LRFU_BP_WRAPPER_SIZE.varname, 1);
    int minBufferSize = 1;
    cachePolicy = new LowLevelLrfuCachePolicy(minBufferSize, maxSize, conf);
    memoryManager = new LowLevelCacheMemoryManager(maxSize, cachePolicy, CACHE_METRICS);
    int maxAllocationSize = 1024;
    int minAllocationSize = 8;
    allocator =
        new BuddyAllocator(true,
            false,
            minAllocationSize,
            maxAllocationSize,
            1,
            maxSize,
            0,
            null,
            memoryManager, CACHE_METRICS,
            "no-force-eviction",
            true);
    EvictionDispatcher evictionDispatcher = new EvictionDispatcher(dataCache, serdCache, metaDataCache, allocator);
    evictionTracker = new EvictionTracker(evictionDispatcher);
    cachePolicy.setEvictionListener(evictionTracker);
  }

  @After public void tearDown() throws Exception {
    LOG.info("Purge the cache on tear down");
    cachePolicy.purge();
    allocator = null;
    memoryManager = null;
    cachePolicy = null;
  }

  /**
   * Test case to ensure that deallocate it does merge small blocks into bigger ones.
   */
  @Test(timeout = 6_000L) public void testMergeOfBlocksAfterDeallocate() {
    // allocate blocks of cacheSize/16, Then deallocate then Allocate of size cacheSize/2
    MemoryBuffer[] dest = new MemoryBuffer[16];
    for (MemoryBuffer memoryBuffer : dest) {
      Assert.assertNull(memoryBuffer);
    }
    allocator.allocateMultiple(dest, 64, null);
    //Check that everything is allocated
    Assert.assertEquals(maxSize, ((LowLevelCacheMemoryManager) memoryManager).getCurrentUsedSize());
    for (MemoryBuffer value : dest) {
      cachePolicy.notifyUnlock((LlapCacheableBuffer) value);
    }
    for (MemoryBuffer value : dest) {
      Assert.assertTrue(value instanceof LlapDataBuffer);
      LlapDataBuffer buffer = (LlapDataBuffer) value;
      Assert.assertEquals(buffer.getMemoryUsage(), cachePolicy.evictSomeBlocks(buffer.getMemoryUsage()));
      memoryManager.releaseMemory(buffer.getMemoryUsage());
    }

    // All is deAllocated thus used has to be zero
    Assert.assertEquals(0, ((LowLevelCacheMemoryManager) memoryManager).getCurrentUsedSize());
    MemoryBuffer[] dest2 = new MemoryBuffer[2];
    for (MemoryBuffer memoryBuffer : dest2) {
      Assert.assertNull(memoryBuffer);
    }
    allocator.allocateMultiple(dest2, 512, null);
    Assert.assertEquals(maxSize, ((LowLevelCacheMemoryManager) memoryManager).getCurrentUsedSize());
    for (int i = 0; i < dest2.length; i++) {
      Assert.assertNotNull(dest[i]);
      //we are not calling deallocate evict to avoid extra memory manager free calls
      allocator.deallocate(dest2[i]);
    }
    Assert.assertEquals(0, ((LowLevelCacheMemoryManager) memoryManager).getCurrentUsedSize());
  }

  @Test(timeout = 6_000L) public void testSimpleAllocateThenEvictThenAllocate() {
    // Allocate all the cache 16 * 64
    MemoryBuffer[] dest = new MemoryBuffer[16];
    for (MemoryBuffer memoryBuffer : dest) {
      Assert.assertNull(memoryBuffer);
    }
    allocator.allocateMultiple(dest, 64, null);
    Assert.assertEquals(maxSize, ((LowLevelCacheMemoryManager) memoryManager).getCurrentUsedSize());
    for (MemoryBuffer buffer : dest) {
      cachePolicy.notifyUnlock((LlapCacheableBuffer) buffer);
    }
    // allocate bigger blocks
    dest = new MemoryBuffer[8];
    allocator.allocateMultiple(dest, 128, null);
    Assert.assertEquals(maxSize, ((LowLevelCacheMemoryManager) memoryManager).getCurrentUsedSize());
    for (MemoryBuffer memoryBuffer : dest) {
      Assert.assertNotNull(memoryBuffer);
      allocator.deallocate(memoryBuffer);
    }
  }

  @Test(timeout = 6_000L) public void testRandomFragmentation() {

    MemoryBuffer[] memBuffers8B = new MemoryBuffer[64];
    MemoryBuffer[] memBuffers16B = new MemoryBuffer[16];
    MemoryBuffer[] memBuffers32B = new MemoryBuffer[8];
    for (MemoryBuffer memoryBuffer : memBuffers8B) {
      Assert.assertNull(memoryBuffer);
    }

    allocator.allocateMultiple(memBuffers8B, 8, null);
    allocator.allocateMultiple(memBuffers16B, 16, null);
    allocator.allocateMultiple(memBuffers32B, 32, null);
    //all the cache is allocated with 8 X 128
    Assert.assertEquals(maxSize, ((LowLevelCacheMemoryManager) memoryManager).getCurrentUsedSize());

    for (int i = 0; i < memBuffers8B.length; i++) {
      LlapDataBuffer buffer = (LlapDataBuffer) memBuffers8B[i];
      // this is needed to make sure that the policy adds the buffers to the linked list as buffers ready to be evicted
      cachePolicy.notifyUnlock(buffer);
      // lock some buffers
      if (i % 2 == 0) {
        // lock the even buffers
        buffer.incRef();
      }
    }

    for (int i = 0; i < memBuffers16B.length; i++) {
      LlapDataBuffer buffer = (LlapDataBuffer) memBuffers16B[i];
      // this is needed to make sure that the policy adds the buffers to the linked list as buffers ready to be evicted
      cachePolicy.notifyUnlock(buffer);
      // lock some buffers
      if (i % 2 == 0) {
        // lock the even buffers
        buffer.incRef();
      }
    }

    for (int i = 0; i < memBuffers32B.length; i++) {
      LlapDataBuffer buffer = (LlapDataBuffer) memBuffers32B[i];
      // this is needed to make sure that the policy adds the buffers to the linked list as buffers ready to be evicted
      cachePolicy.notifyUnlock(buffer);
      // lock some buffers
      if (i % 2 == 0) {
        // lock the even buffers
        buffer.incRef();
      }
    }
    Assert.assertEquals(512, ((LowLevelCacheMemoryManager) memoryManager).purge());

    for (MemoryBuffer memoryBuffer : memBuffers32B) {
      LlapDataBuffer buffer = (LlapDataBuffer) memoryBuffer;
      // this is needed to make sure that the policy adds the buffers to the linked list as buffers ready to be evicted
      if (buffer.isLocked()) {
        buffer.decRef();
      }
      cachePolicy.notifyUnlock(buffer);
    }

    for (MemoryBuffer memoryBuffer : memBuffers16B) {
      LlapDataBuffer buffer = (LlapDataBuffer) memoryBuffer;
      // this is needed to make sure that the policy adds the buffers to the linked list as buffers ready to be evicted
      if (buffer.isLocked()) {
        buffer.decRef();
      }
      cachePolicy.notifyUnlock(buffer);
    }

    for (MemoryBuffer memoryBuffer : memBuffers8B) {
      LlapDataBuffer buffer = (LlapDataBuffer) memoryBuffer;
      // this is needed to make sure that the policy adds the buffers to the linked list as buffers ready to be evicted
      if (buffer.isLocked()) {
        buffer.decRef();
      }
      cachePolicy.notifyUnlock(buffer);
    }
    Assert.assertEquals(maxSize / 2, ((LowLevelCacheMemoryManager) memoryManager).getCurrentUsedSize());

    memBuffers8B = new MemoryBuffer[64];
    memBuffers16B = new MemoryBuffer[16];
    memBuffers32B = new MemoryBuffer[8];
    evictionTracker.getEvicted().clear();
    allocator.allocateMultiple(memBuffers16B, 16, null);
    allocator.allocateMultiple(memBuffers8B, 8, null);
    allocator.allocateMultiple(memBuffers32B, 32, null);
    Assert.assertEquals(maxSize, ((LowLevelCacheMemoryManager) memoryManager).getCurrentUsedSize());
    Assert.assertEquals(memBuffers32B.length / 2 + memBuffers16B.length / 2 + memBuffers8B.length / 2, evictionTracker.getEvicted().size());
    for (MemoryBuffer memoryBuffer : memBuffers8B) {
      LlapDataBuffer buffer = (LlapDataBuffer) memoryBuffer;
      allocator.deallocate(buffer);
    }
    for (MemoryBuffer memoryBuffer : memBuffers16B) {
      LlapDataBuffer buffer = (LlapDataBuffer) memoryBuffer;
      allocator.deallocate(buffer);
    }

    for (MemoryBuffer memoryBuffer : memBuffers32B) {
      LlapDataBuffer buffer = (LlapDataBuffer) memoryBuffer;
      allocator.deallocate(buffer);
    }
  }

  @Test(timeout = 6_000L) public void testFragmentation() {
    MemoryBuffer[] dest = new MemoryBuffer[128];
    for (MemoryBuffer memoryBuffer : dest) {
      Assert.assertNull(memoryBuffer);
    }
    allocator.allocateMultiple(dest, 8, null);
    //all the cache is allocated with 8 X 128
    Assert.assertEquals(maxSize, ((LowLevelCacheMemoryManager) memoryManager).getCurrentUsedSize());

    for (int i = 0; i < dest.length; i++) {
      LlapDataBuffer buffer = (LlapDataBuffer) dest[i];
      // this is needed to make sure that the policy adds the buffers to the linked list as buffers ready to be evicted
      cachePolicy.notifyUnlock(buffer);
      // lock some buffers
      if (i % 2 == 0) {
        // lock the even buffers
        buffer.incRef();
      }
    }

    // purge the cache should lead to only evicting the unlocked buffers
    Assert.assertEquals(512, ((LowLevelCacheMemoryManager) memoryManager).purge());
    // After purge the used memory should be aligned to the amount of evicted items
    Assert.assertEquals(512, ((LowLevelCacheMemoryManager) memoryManager).getCurrentUsedSize());

    MemoryBuffer[] dest2 = new MemoryBuffer[1];
    Exception exception = null;
    try {
      allocator.allocateMultiple(dest2, 16, null);
    } catch (Allocator.AllocatorOutOfMemoryException e) {
      // we should fail since we have the extreme case where half of the leaf nodes are locked thus
      // maximum fragmentation case
      exception = e;
    }
    Assert.assertNotNull(exception);
    //We need to make sure that the failed allocation attempt undo the reserved memory.
    //https://issues.apache.org/jira/browse/HIVE-21689
    Assert.assertEquals(512, ((LowLevelCacheMemoryManager) memoryManager).getCurrentUsedSize());
    // unlock one buffer
    Assert.assertTrue(((LlapDataBuffer) dest[0]).isLocked());
    ((LlapDataBuffer) dest[0]).decRef();
    evictionTracker.clear();
    // this is needed since purge has removed the locked ones form the list, the assumption is that when we unlock
    // a buffer we notify the cache policy.
    cachePolicy.notifyUnlock((LlapDataBuffer) dest[0]);
    // we should be able to allocate after some extra eviction
    allocator.allocateMultiple(dest2, 16, null);
    //we have to see that we have force evicted something to make room for the new allocation
    Assert.assertTrue(evictionTracker.getEvicted().size() >= 1);
  }

  private final class EvictionTracker implements EvictionListener {
    private final EvictionListener evictionListener;

    private List<LlapCacheableBuffer> evicted = new ArrayList<>();

    private EvictionTracker(EvictionListener evictionListener) {
      this.evictionListener = evictionListener;
    }

    @Override public void notifyEvicted(LlapCacheableBuffer buffer) {
      evicted.add(buffer);
      evictionListener.notifyEvicted(buffer);
    }

    @Override
    public void notifyProactivelyEvicted(LlapCacheableBuffer buffer) {
      evicted.add(buffer);
      evictionListener.notifyEvicted(buffer);
    }

    public List<LlapCacheableBuffer> getEvicted() {
      return evicted;
    }

    public void clear() {
      evicted.clear();
    }

  }
}
