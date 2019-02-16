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

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.llap.cache.LowLevelCache.Priority;
import org.apache.hadoop.hive.llap.io.metadata.MetadataCache;
import org.apache.hadoop.hive.llap.io.metadata.MetadataCache.LlapBufferOrBuffers;
import org.apache.hadoop.hive.llap.io.metadata.MetadataCache.LlapMetadataBuffer;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.apache.hadoop.hive.ql.io.orc.encoded.IncompleteCb;
import org.junit.Test;

public class TestOrcMetadataCache {
  private static class DummyCachePolicy implements LowLevelCachePolicy {
    int lockCount = 0, unlockCount = 0;

    public void cache(LlapCacheableBuffer buffer, Priority pri) {
      ++lockCount;
    }

    public void notifyLock(LlapCacheableBuffer buffer) {
      ++lockCount;
    }

    public void notifyUnlock(LlapCacheableBuffer buffer) {
      ++unlockCount;
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

    public void verifyEquals(int i) {
      assertEquals(i, lockCount);
      assertEquals(i, unlockCount);
    }

    @Override
    public void debugDumpShort(StringBuilder sb) {
    }
  }

  private static class DummyMemoryManager implements MemoryManager {
    private int allocs;

    @Override
    public void reserveMemory(long memoryToReserve, AtomicBoolean isStopped) {
      ++allocs;
    }

    @Override
    public void releaseMemory(long memUsage) {
    }

    @Override
    public void updateMaxSize(long maxSize) {
    }
  }

  @Test
  public void testBuffers() throws Exception {
    DummyMemoryManager mm = new DummyMemoryManager();
    DummyCachePolicy cp = new DummyCachePolicy();
    final int MAX_ALLOC = 64;
    LlapDaemonCacheMetrics metrics = LlapDaemonCacheMetrics.create("", "");
    BuddyAllocator alloc = new BuddyAllocator(
        false, false, 8, MAX_ALLOC, 1, 4096, 0, null, mm, metrics, null, true);
    MetadataCache cache = new MetadataCache(alloc, mm, cp, true, metrics);
    Object fileKey1 = new Object();
    Random rdm = new Random();

    ByteBuffer smallBuffer = ByteBuffer.allocate(MAX_ALLOC - 1);
    rdm.nextBytes(smallBuffer.array());
    LlapBufferOrBuffers result = cache.putFileMetadata(fileKey1, smallBuffer, null, null);
    cache.decRefBuffer(result);
    ByteBuffer cacheBuf = result.getSingleBuffer().getByteBufferDup();
    assertEquals(smallBuffer, cacheBuf);
    result = cache.putFileMetadata(fileKey1, smallBuffer, null, null);
    cache.decRefBuffer(result);
    cacheBuf = result.getSingleBuffer().getByteBufferDup();
    assertEquals(smallBuffer, cacheBuf);
    result = cache.getFileMetadata(fileKey1);
    cacheBuf = result.getSingleBuffer().getByteBufferDup();
    assertEquals(smallBuffer, cacheBuf);
    cache.decRefBuffer(result);
    cache.notifyEvicted((LlapMetadataBuffer<?>) result.getSingleBuffer());
    result = cache.getFileMetadata(fileKey1);
    assertNull(result);

    ByteBuffer largeBuffer = ByteBuffer.allocate((int)(MAX_ALLOC * 2.5));
    rdm.nextBytes(largeBuffer.array());
    result = cache.putFileMetadata(fileKey1, largeBuffer, null, null);
    cache.decRefBuffer(result);
    assertNull(result.getSingleBuffer());
    assertEquals(largeBuffer, extractResultBbs(result));
    result = cache.getFileMetadata(fileKey1);
    assertNull(result.getSingleBuffer());
    assertEquals(largeBuffer, extractResultBbs(result));
    LlapAllocatorBuffer b0 = result.getMultipleLlapBuffers()[0],
        b1 = result.getMultipleLlapBuffers()[1];
    cache.decRefBuffer(result);
    cache.notifyEvicted((LlapMetadataBuffer<?>) b1);
    result = cache.getFileMetadata(fileKey1);
    assertNull(result);
    assertFalse(b0.incRef() > 0); // Should have also been thrown out.
  }

  public ByteBuffer extractResultBbs(LlapBufferOrBuffers result) {
    int totalLen = 0;
    for (LlapAllocatorBuffer buf : result.getMultipleLlapBuffers()) {
      totalLen += buf.getByteBufferRaw().remaining();
    }
    ByteBuffer combinedBb = ByteBuffer.allocate(totalLen);
    for (LlapAllocatorBuffer buf : result.getMultipleLlapBuffers()) {
      combinedBb.put(buf.getByteBufferDup());
    }
    combinedBb.flip();
    return combinedBb;
  }

  @Test
  public void testIncompleteCbs() throws Exception {
    DummyMemoryManager mm = new DummyMemoryManager();
    DummyCachePolicy cp = new DummyCachePolicy();
    final int MAX_ALLOC = 64;
    LlapDaemonCacheMetrics metrics = LlapDaemonCacheMetrics.create("", "");
    BuddyAllocator alloc = new BuddyAllocator(
        false, false, 8, MAX_ALLOC, 1, 4096, 0, null, mm, metrics, null, true);
    MetadataCache cache = new MetadataCache(alloc, mm, cp, true, metrics);
    DataCache.BooleanRef gotAllData = new DataCache.BooleanRef();
    Object fileKey1 = new Object();

    // Note: incomplete CBs are always an exact match.
    cache.putIncompleteCbs(fileKey1, new DiskRange[] { new DiskRangeList(0, 3) }, 0, null);
    cp.verifyEquals(1);
    DiskRangeList result = cache.getIncompleteCbs(
        fileKey1, new DiskRangeList(0, 3), 0, gotAllData);
    assertTrue(gotAllData.value);
    verifyResult(result, INCOMPLETE, 0, 3);
    cache.putIncompleteCbs(fileKey1, new DiskRange[] { new DiskRangeList(5, 6) }, 0, null);
    cp.verifyEquals(3);
    DiskRangeList ranges = new DiskRangeList(0, 3);
    ranges.insertAfter(new DiskRangeList(4, 6));
    result = cache.getIncompleteCbs(fileKey1, ranges, 0, gotAllData);
    assertFalse(gotAllData.value);
    verifyResult(result, INCOMPLETE, 0, 3, DRL, 4, 6);
    ranges = new DiskRangeList(0, 3);
    ranges.insertAfter(new DiskRangeList(3, 5)).insertAfter(new DiskRangeList(5, 6));
    result = cache.getIncompleteCbs(fileKey1, ranges, 0, gotAllData);
    assertFalse(gotAllData.value);
    verifyResult(result, INCOMPLETE, 0, 3, DRL, 3, 5, INCOMPLETE, 5, 6);
    result = cache.getIncompleteCbs(fileKey1, new DiskRangeList(5, 6), 0, gotAllData);
    assertTrue(gotAllData.value);
    verifyResult(result, INCOMPLETE, 5, 6);
    result = cache.getIncompleteCbs(fileKey1, new DiskRangeList(4, 5), 0, gotAllData);
    assertFalse(gotAllData.value);
    verifyResult(result, DRL, 4, 5);
  }

  private static final int INCOMPLETE = 0, DRL = 1;
  public void verifyResult(DiskRangeList result, long... vals) {
    for (int i = 0; i < vals.length; i += 3) {
      switch ((int)vals[i]) {
      case INCOMPLETE: assertTrue(result instanceof IncompleteCb); break;
      case DRL: assertFalse(result instanceof IncompleteCb); break;
      default: fail();
      }
      assertEquals(vals[i + 1], result.getOffset());
      assertEquals(vals[i + 2], result.getEnd());
      result = result.next;
    }
    assertNull(result);
  }


}
