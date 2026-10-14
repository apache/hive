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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.llap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.Allocator;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.cache.BuddyAllocator;
import org.apache.hadoop.hive.llap.cache.LlapAllocatorBuffer;
import org.apache.hadoop.hive.llap.cache.LlapCacheableBuffer;
import org.apache.hadoop.hive.llap.cache.LlapDataBuffer;
import org.apache.hadoop.hive.llap.cache.LowLevelCache.Priority;
import org.apache.hadoop.hive.llap.cache.LowLevelCacheImpl;
import org.apache.hadoop.hive.llap.cache.LowLevelLrfuCachePolicy;
import org.apache.hadoop.hive.llap.cache.TestBuddyAllocatorForceEvict;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.apache.hadoop.hive.ql.io.orc.encoded.CacheChunk;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * The stock Parquet reader caches through {@link LlapCacheAwareFs}, the native reader through
 * {@link ParquetCacheLayout}; both cache the same column chunks, so each must find the other's buffers
 * where it would have put its own.
 */
public class TestLlapCacheAwareFsInterop {
  private static final int MAX_ALLOC = 4096;
  private static final ParquetCacheLayout LAYOUT = new ParquetCacheLayout(MAX_ALLOC, 1024);
  private static final LlapDaemonCacheMetrics METRICS =
      LlapDaemonCacheMetrics.create("TestLlapCacheAwareFsInterop", "1");
  private static final Long FILE_KEY = 42L;
  // Two column chunks; the first is cut [4096, 4096, 1024, 784], unlike max-allocation pieces.
  private static final long CHUNK_END = 10_000, FILE_LENGTH = 30_000;

  private final Configuration conf = new HiveConf();
  private java.nio.file.Path dir;
  private Path file;
  private byte[] bytes;
  private LowLevelCacheImpl cache;
  private TrackingDataCache dataCache;

  @Before
  public void setUp() throws IOException {
    // The stream builds its layout from the allocator and the conf registerFile gets: this floor makes it LAYOUT.
    HiveConf.setVar(conf, ConfVars.LLAP_IO_ENCODE_ALLOC_SIZE, "1Kb");
    dir = Files.createTempDirectory("llap-cache-aware-fs");
    bytes = new byte[(int) FILE_LENGTH];
    new Random(7).nextBytes(bytes);
    java.nio.file.Path local = dir.resolve("data");
    Files.write(local, bytes);
    file = new Path(local.toUri());
    BuddyAllocator allocator = TestBuddyAllocatorForceEvict.create(MAX_ALLOC, 2, 8 << 20, true, false);
    cache = new LowLevelCacheImpl(METRICS, new LowLevelLrfuCachePolicy(MAX_ALLOC, 8 << 20, conf), allocator, true);
    dataCache = new TrackingDataCache(cache, allocator);
  }

  @After
  public void tearDown() throws IOException {
    Files.walk(dir).sorted(Collections.reverseOrder()).forEach(p -> p.toFile().delete());
  }

  /** A stock read of a chunk the native reader cached, one piece since evicted, re-caches the gap as that piece. */
  @Test
  public void testGapInANativelyCachedChunkIsRecachedAsItsPiece() throws IOException {
    cacheAsNativeReader(0, CHUNK_END);
    evict(4096 + 4096);

    assertArrayEquals(Arrays.copyOfRange(bytes, 0, (int) CHUNK_END), stockRead(0, CHUNK_END));
    assertCachedAs(0, CHUNK_END);
    dataCache.assertNothingLeaked();
  }

  /** The stock reader caches a chunk the way the native reader would, so the native reader finds its pieces. */
  @Test
  public void testStockReadCachesTheNativeLayout() throws IOException {
    assertArrayEquals(bytes, stockRead(0, FILE_LENGTH));
    assertCachedAs(0, CHUNK_END);
    assertCachedAs(CHUNK_END, FILE_LENGTH);
    dataCache.assertNothingLeaked();
  }

  /** Two readers missing the same chunk at once: the second one's buffers are freed, not leaked or left locked. */
  @Test
  public void testConcurrentMissFreesTheBuffersTheCacheDidNotTake() throws IOException {
    cacheAsNativeReader(0, CHUNK_END);
    dataCache.missOnce = true;

    assertArrayEquals(Arrays.copyOfRange(bytes, 0, (int) CHUNK_END), stockRead(0, CHUNK_END));
    assertCachedAs(0, CHUNK_END);
    dataCache.assertNothingLeaked();
  }

  private byte[] stockRead(long from, long to) throws IOException {
    TreeMap<Long, Long> chunks = new TreeMap<>();
    chunks.put(0L, CHUNK_END);
    chunks.put(CHUNK_END, FILE_LENGTH);
    Path path = LlapCacheAwareFs.registerFile(dataCache, file, FILE_KEY, chunks, conf, null);
    try (FSDataInputStream in = path.getFileSystem(conf).open(path)) {
      byte[] read = new byte[(int) (to - from)];
      in.seek(from);
      in.readFully(read);
      return read;
    } finally {
      LlapCacheAwareFs.unregisterFile(path);
    }
  }

  /** Caches a chunk as the native reader does: one buffer per piece of the layout, put one at a time. */
  private void cacheAsNativeReader(long from, long to) {
    long offset = from;
    for (int size : LAYOUT.bufferSizes(to - from)) {
      MemoryBuffer[] buffer = new MemoryBuffer[1];
      cache.getAllocator().allocateMultiple(buffer, size, LlapDataBuffer::new);
      buffer[0].getByteBufferRaw().duplicate().put(bytes, (int) offset, size);
      assertEquals(null, cache.putFileData(FILE_KEY, new DiskRange[] {new DiskRange(offset, offset + size)},
          buffer, 0, Priority.NORMAL, null, null));
      cache.decRefBuffer(buffer[0]);
      offset += size;
    }
  }

  private void evict(long offset) {
    DiskRangeList hit = cache.getFileData(FILE_KEY, new DiskRangeList(offset, offset + 1), 0,
        CacheChunk::new, null, new DataCache.BooleanRef());
    LlapDataBuffer buffer = (LlapDataBuffer) ((CacheChunk) hit).getBuffer();
    assertEquals(offset, hit.getOffset());
    cache.decRefBuffer(buffer);
    assertEquals(LlapCacheableBuffer.INVALIDATE_OK, buffer.invalidate());
    cache.notifyEvicted(buffer);
  }

  /** The chunk is cached whole, in the pieces of the layout, holding the file's bytes, and none is left locked. */
  private void assertCachedAs(long from, long to) {
    DataCache.BooleanRef gotAll = new DataCache.BooleanRef();
    DiskRangeList hits = cache.getFileData(FILE_KEY, new DiskRangeList(from, to), 0, CacheChunk::new, null, gotAll);
    assertTrue("cached whole", gotAll.value);
    List<Long> pieces = new ArrayList<>(), expected = new ArrayList<>();
    long offset = from;
    for (int size : LAYOUT.bufferSizes(to - from)) {
      expected.add(offset);
      expected.add(offset + size);
      offset += size;
    }
    for (DiskRangeList hit = hits; hit != null; hit = hit.next) {
      pieces.add(hit.getOffset());
      pieces.add(hit.getEnd());
      MemoryBuffer buffer = ((CacheChunk) hit).getBuffer();
      byte[] cached = new byte[(int) hit.getLength()];
      buffer.getByteBufferDup().get(cached);
      assertArrayEquals(Arrays.copyOfRange(bytes, (int) hit.getOffset(), (int) hit.getEnd()), cached);
      cache.decRefBuffer(buffer);
      assertFalse("left locked: " + buffer, ((LlapAllocatorBuffer) buffer).isLocked());
    }
    assertEquals(expected, pieces);
  }

  /**
   * The {@link DataCache} an LLAP daemon gives the stock reader, recording the buffers the reader allocates and
   * frees; {@link #missOnce} makes one lookup miss what the cache holds, as for a reader that looked just before
   * another one cached the chunk.
   */
  private static final class TrackingDataCache implements DataCache, Allocator.BufferObjectFactory, Allocator {
    private final LowLevelCacheImpl cache;
    private final BuddyAllocator allocator;
    private final Set<MemoryBuffer> allocated = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Set<MemoryBuffer> freed = Collections.newSetFromMap(new IdentityHashMap<>());
    boolean missOnce;

    TrackingDataCache(LowLevelCacheImpl cache, BuddyAllocator allocator) {
      this.cache = cache;
      this.allocator = allocator;
    }

    @Override
    public DiskRangeList getFileData(Object fileKey, DiskRangeList range, long baseOffset,
        DiskRangeListFactory factory, BooleanRef gotAllData) {
      if (missOnce) {
        missOnce = false;
        gotAllData.value = false;
        return range;
      }
      return cache.getFileData(fileKey, range, baseOffset, factory, null, gotAllData);
    }

    @Override
    public long[] putFileData(Object fileKey, DiskRange[] ranges, MemoryBuffer[] data, long baseOffset) {
      return putFileData(fileKey, ranges, data, baseOffset, null);
    }

    @Override
    public long[] putFileData(Object fileKey, DiskRange[] ranges, MemoryBuffer[] data, long baseOffset,
        CacheTag tag) {
      return cache.putFileData(fileKey, ranges, data, baseOffset, Priority.NORMAL, null, tag);
    }

    @Override
    public void releaseBuffer(MemoryBuffer buffer) {
      cache.decRefBuffer(buffer);
    }

    @Override
    public void reuseBuffer(MemoryBuffer buffer) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Allocator getAllocator() {
      return this;
    }

    @Override
    public BufferObjectFactory getDataBufferFactory() {
      return this;
    }

    @Override
    public MemoryBuffer create() {
      return new LlapDataBuffer();
    }

    @Override
    public void allocateMultiple(MemoryBuffer[] dest, int size, BufferObjectFactory factory) {
      allocator.allocateMultiple(dest, size, factory);
      Collections.addAll(allocated, dest);
    }

    @Override
    public void deallocate(MemoryBuffer buffer) {
      freed.add(buffer);
      allocator.deallocate(buffer);
    }

    @Override
    public boolean isDirectAlloc() {
      return allocator.isDirectAlloc();
    }

    @Override
    public int getMaxAllocation() {
      return allocator.getMaxAllocation();
    }

    @Deprecated
    @Override
    public void allocateMultiple(MemoryBuffer[] dest, int size) {
      throw new UnsupportedOperationException();
    }

    @Deprecated
    @Override
    public MemoryBuffer createUnallocated() {
      throw new UnsupportedOperationException();
    }

    /** Every buffer the reader allocated is either in the cache, unlocked, or freed. */
    void assertNothingLeaked() {
      for (MemoryBuffer buffer : allocated) {
        if (freed.contains(buffer)) {
          continue;
        }
        long start = ((LlapDataBuffer) buffer).getStart();
        DiskRangeList hit = cache.getFileData(FILE_KEY, new DiskRangeList(start, start + 1), 0, CacheChunk::new,
            null, new BooleanRef());
        assertTrue("neither cached nor freed: " + buffer, hit.hasData());
        MemoryBuffer cached = ((CacheChunk) hit).getBuffer();
        cache.decRefBuffer(cached);
        assertTrue("neither cached nor freed: " + buffer, cached == buffer);
        assertFalse("left locked: " + buffer, ((LlapAllocatorBuffer) buffer).isLocked());
      }
    }
  }
}
