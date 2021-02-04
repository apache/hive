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
import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.IllegalCacheConfigurationException;
import org.apache.hadoop.hive.llap.cache.LowLevelCache.Priority;
import org.apache.hadoop.hive.llap.io.encoded.OrcEncodedDataReader;
import org.apache.hadoop.hive.llap.io.metadata.MetadataCache;
import org.apache.hadoop.hive.llap.io.metadata.MetadataCache.LlapBufferOrBuffers;
import org.apache.hadoop.hive.llap.io.metadata.MetadataCache.LlapMetadataBuffer;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.apache.hadoop.hive.ql.io.SyntheticFileId;
import org.apache.hadoop.hive.ql.io.orc.encoded.IncompleteCb;
import org.apache.orc.impl.OrcTail;

import org.junit.Assert;
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

    @Override public long evictMemory(long memoryToEvict) {
      return 0;
    }

    @Override
    public void releaseMemory(long memUsage) {
    }

    @Override
    public void updateMaxSize(long maxSize) {
    }
  }

  @Test
  public void testCaseSomePartialBuffersAreEvicted() {
    final DummyMemoryManager mm = new DummyMemoryManager();
    final DummyCachePolicy cp = new DummyCachePolicy();
    final int MAX_ALLOC = 64;
    final LlapDaemonCacheMetrics metrics = LlapDaemonCacheMetrics.create("", "");
    final BuddyAllocator alloc = new BuddyAllocator(false, false, 8, MAX_ALLOC, 1, 4096, 0, null, mm, metrics, null, true);
    final MetadataCache cache = new MetadataCache(alloc, mm, cp, true, metrics);
    final Object fileKey1 = new Object();
    final Random rdm = new Random();
    final ByteBuffer smallBuffer = ByteBuffer.allocate(2 * MAX_ALLOC);
    rdm.nextBytes(smallBuffer.array());
    //put some metadata in the cache that needs multiple buffers (2 * MAX_ALLOC)
    final LlapBufferOrBuffers result = cache.putFileMetadata(fileKey1, smallBuffer, null, null);
    // assert that we have our 2 buffers
    Assert.assertEquals(2, result.getMultipleLlapBuffers().length);
    final LlapAllocatorBuffer[] buffers = result.getMultipleLlapBuffers();
    //test setup where one buffer is evicted and therefore can not be locked
    buffers[1].decRef();
    buffers[1].invalidateAndRelease();
    //Try to get the buffer should lead to cleaning the cache since some part was evicted.
    Assert.assertNull(cache.getFileMetadata(fileKey1));
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

  @Test
  public void testGetOrcTailForPath() throws Exception {
    DummyMemoryManager mm = new DummyMemoryManager();
    DummyCachePolicy cp = new DummyCachePolicy();
    final int MAX_ALLOC = 64;
    LlapDaemonCacheMetrics metrics = LlapDaemonCacheMetrics.create("", "");
    BuddyAllocator alloc = new BuddyAllocator(
        false, false, 8, MAX_ALLOC, 1, 4 * 4096, 0, null, mm, metrics, null, true);
    MetadataCache cache = new MetadataCache(alloc, mm, cp, true, metrics);

    Path path = new Path("../data/files/alltypesorc");
    Configuration jobConf = new Configuration();
    Configuration daemonConf = new Configuration();
    CacheTag tag = CacheTag.build("test-table");
    OrcTail uncached = OrcEncodedDataReader.getOrcTailForPath(path, jobConf, tag, daemonConf, cache, null);
    jobConf.set(HiveConf.ConfVars.LLAP_IO_CACHE_ONLY.varname, "true");
    OrcTail cached = OrcEncodedDataReader.getOrcTailForPath(path, jobConf, tag, daemonConf, cache, null);
    assertEquals(uncached.getSerializedTail(), cached.getSerializedTail());
    assertEquals(uncached.getFileTail(), cached.getFileTail());
  }

  @Test
  public void testGetOrcTailForPathWithFileId() throws Exception {
    DummyMemoryManager mm = new DummyMemoryManager();
    DummyCachePolicy cp = new DummyCachePolicy();
    final int MAX_ALLOC = 64;
    LlapDaemonCacheMetrics metrics = LlapDaemonCacheMetrics.create("", "");
    BuddyAllocator alloc = new BuddyAllocator(
        false, false, 8, MAX_ALLOC, 1, 4 * 4096, 0, null, mm, metrics, null, true);
    MetadataCache cache = new MetadataCache(alloc, mm, cp, true, metrics);

    Path path = new Path("../data/files/alltypesorc");
    Configuration jobConf = new Configuration();
    Configuration daemonConf = new Configuration();
    CacheTag tag = CacheTag.build("test-table");
    FileSystem fs = FileSystem.get(daemonConf);
    FileStatus fileStatus = fs.getFileStatus(path);
    OrcTail uncached = OrcEncodedDataReader.getOrcTailForPath(fileStatus.getPath(), jobConf, tag, daemonConf, cache, new SyntheticFileId(fileStatus));
    jobConf.set(HiveConf.ConfVars.LLAP_IO_CACHE_ONLY.varname, "true");
    // this should work from the cache, by recalculating the same fileId
    OrcTail cached = OrcEncodedDataReader.getOrcTailForPath(fileStatus.getPath(), jobConf, tag, daemonConf, cache, null);
    assertEquals(uncached.getSerializedTail(), cached.getSerializedTail());
    assertEquals(uncached.getFileTail(), cached.getFileTail());
  }

  @Test
  public void testGetOrcTailForPathWithFileIdChange() throws Exception {
    DummyMemoryManager mm = new DummyMemoryManager();
    DummyCachePolicy cp = new DummyCachePolicy();
    final int MAX_ALLOC = 64;
    LlapDaemonCacheMetrics metrics = LlapDaemonCacheMetrics.create("", "");
    BuddyAllocator alloc = new BuddyAllocator(
        false, false, 8, MAX_ALLOC, 1, 4 * 4096, 0, null, mm, metrics, null, true);
    MetadataCache cache = new MetadataCache(alloc, mm, cp, true, metrics);

    Path path = new Path("../data/files/alltypesorc");
    Configuration jobConf = new Configuration();
    Configuration daemonConf = new Configuration();
    CacheTag tag = CacheTag.build("test-table");
    OrcEncodedDataReader.getOrcTailForPath(path, jobConf, tag, daemonConf, cache, new SyntheticFileId(path, 100, 100));
    jobConf.set(HiveConf.ConfVars.LLAP_IO_CACHE_ONLY.varname, "true");
    Exception ex = null;
    try {
      // this should miss the cache, since the fileKey changed
      OrcEncodedDataReader.getOrcTailForPath(path, jobConf, tag, daemonConf, cache, new SyntheticFileId(path, 100, 101));
      fail();
    } catch (IOException e) {
      ex = e;
    }
    Assert.assertTrue(ex.getMessage().contains(HiveConf.ConfVars.LLAP_IO_CACHE_ONLY.varname));
  }

  @Test(expected = IllegalCacheConfigurationException.class)
  public void testGetOrcTailForPathCacheNotReady() throws Exception {
    Path path = new Path("../data/files/alltypesorc");
    Configuration conf = new Configuration();
    OrcEncodedDataReader.getOrcTailForPath(path, conf, null, conf, null, null);
  }

  @Test
  public void testProactiveEvictionMark() throws Exception {
    DummyMemoryManager mm = new DummyMemoryManager();
    DummyCachePolicy cp = new DummyCachePolicy();
    final int MAX_ALLOC = 64;
    LlapDaemonCacheMetrics metrics = LlapDaemonCacheMetrics.create("", "");
    BuddyAllocator alloc = new BuddyAllocator(
        false, false, 8, MAX_ALLOC, 1, 4096, 0, null, mm, metrics, null, true);
    MetadataCache cache = new MetadataCache(alloc, mm, cp, true, metrics);

    long fn1 = 1;
    long fn2 = 2;
    long fn3 = 3;

    AtomicBoolean isStopped = new AtomicBoolean(false);

    // Case for when metadata consists of just 1 buffer (most of the realworld cases)
    ByteBuffer bb = ByteBuffer.wrap("small-meta-data-content".getBytes());
    // Case for when metadata consists of multiple buffers (rare case), (max allocation is 64 hence the test data
    // below is of length 65
    ByteBuffer bb2 = ByteBuffer.wrap("-large-meta-data-content-large-meta-data-content-large-meta-data-".getBytes());

    LlapBufferOrBuffers table1Buffers1 = cache.putFileMetadata(fn1, bb, CacheTag.build("default.table1"), isStopped);
    assertNotNull(table1Buffers1.getSingleLlapBuffer());

    LlapBufferOrBuffers table1Buffers2 = cache.putFileMetadata(fn2, bb2, CacheTag.build("default.table1"), isStopped);
    assertNotNull(table1Buffers2.getMultipleLlapBuffers());
    assertEquals(2, table1Buffers2.getMultipleLlapBuffers().length);

    // Case for when metadata consists of just 1 buffer (most of the realworld cases)
    ByteBuffer bb3 = ByteBuffer.wrap("small-meta-data-content-for-otherFile".getBytes());
    LlapBufferOrBuffers table2Buffers1 = cache.putFileMetadata(fn3, bb3, CacheTag.build("default.table2"), isStopped);
    assertNotNull(table2Buffers1.getSingleLlapBuffer());

    Predicate<CacheTag> predicate = tag -> "default.table1".equals(tag.getTableName());

    // Simulating eviction on some buffers
    table1Buffers2.getMultipleLlapBuffers()[1].decRef();
    assertEquals(INVALIDATE_OK, table1Buffers2.getMultipleLlapBuffers()[1].invalidate());

    // table1Buffers1:27 (allocated as 32) + table1Buffers2[0]:64 (also allocated as 64)
    assertEquals(96, cache.markBuffersForProactiveEviction(predicate, false));

    // Single buffer for file1 should be marked as per predicate
    assertTrue(table1Buffers1.getSingleLlapBuffer().isMarkedForEviction());

    // Multi buffer for file2 should be partially marked as per predicate and prior eviction
    assertTrue(table1Buffers2.getMultipleLlapBuffers()[0].isMarkedForEviction());
    assertFalse(table1Buffers2.getMultipleLlapBuffers()[1].isMarkedForEviction());

    // Single buffer for file3 should not be marked as per predicate
    assertFalse(table2Buffers1.getSingleLlapBuffer().isMarkedForEviction());

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
