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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.Allocator;
import org.apache.hadoop.hive.common.io.DataCache;
import org.junit.Test;

/** Tests for {@link LlapCacheAwareFs}, the filesystem shim that serves reads through the LLAP cache. */
public class TestLlapCacheAwareFs {
  private static final int MB = 1 << 20;
  /** Not a power of two, as getMaxAllocation() may return; the layout rounds it down to 4MB. */
  private static final int MAX_ALLOC = 6 * MB;

  /** One column chunk at [1000, 1000 + 20MB), the shape VectorizedParquetRecordReader registers. */
  private static LlapCacheAwareFs.CacheAwareInputStream streamOverOneChunk() {
    TreeMap<Long, Long> chunkIndex = new TreeMap<>();
    // ParquetCacheLayout caches it as [0,4) [4,8) [8,12) [12,16) [16,20) MB from the chunk start.
    chunkIndex.put(1000L, 1000L + 20 * MB);
    return streamOver(chunkIndex);
  }

  private static LlapCacheAwareFs.CacheAwareInputStream streamOver(TreeMap<Long, Long> chunkIndex) {
    Allocator allocator = mock(Allocator.class);
    when(allocator.getMaxAllocation()).thenReturn(MAX_ALLOC);
    DataCache cache = mock(DataCache.class);
    when(cache.getAllocator()).thenReturn(allocator);
    return new LlapCacheAwareFs.CacheAwareInputStream(cache, new Configuration(), chunkIndex,
        new Path("/tmp/does-not-matter"), 1L, -1, null, new ParquetCacheLayout(allocator, new Configuration()));
  }

  @Test
  public void aMissOffTheMaxAllocGridIsServed() {
    // A gap need not align to MAX_ALLOC: evicting the second buffer leaves [4,8) MB, and neither
    // end is a multiple of 6MB.
    SortedMap<Long, Long> chunks =
        streamOverOneChunk().getAndValidateMissingChunks(1000L + 4 * MB, 1000L + 8 * MB);
    assertEquals(1, chunks.size());
    assertEquals(1000L, (long) chunks.firstKey());
  }

  @Test
  public void aMissEndingMidChunkIsServed() {
    // A gap may end mid-chunk on any boundary: evicting the fourth buffer leaves [12,16) MB, whose
    // end is not a multiple of 6MB.
    SortedMap<Long, Long> chunks =
        streamOverOneChunk().getAndValidateMissingChunks(1000L + 12 * MB, 1000L + 16 * MB);
    assertEquals(1, chunks.size());
  }

  @Test
  public void theWholeChunkIsServed() {
    assertEquals(1, streamOverOneChunk().getAndValidateMissingChunks(1000L, 1000L + 20 * MB).size());
  }

  // The rejections below should never fire in production: each means a bug. The chunk index holds
  // the projected column chunks of the split's footer, and the Parquet reader asks for ranges from
  // that same footer, so every miss lies inside registered chunks.

  @Test(expected = AssertionError.class)
  public void aMissBelowEveryRegisteredChunkIsRejected() {
    streamOverOneChunk().getAndValidateMissingChunks(0L, 500L);
  }

  @Test(expected = AssertionError.class)
  public void aMissRunningPastTheLastRegisteredChunkIsRejected() {
    streamOverOneChunk().getAndValidateMissingChunks(1000L + 19 * MB, 1000L + 21 * MB);
  }

  @Test(expected = AssertionError.class)
  public void aMissInTheHoleBetweenTwoChunksIsRejected() {
    // The hole is an unprojected column's chunk, which is never registered.
    TreeMap<Long, Long> chunkIndex = new TreeMap<>();
    chunkIndex.put(1000L, 1000L + 20 * MB);
    chunkIndex.put(1000L + 21 * MB, 1000L + 30 * MB);
    streamOver(chunkIndex).getAndValidateMissingChunks(1000L + 20 * MB, 1000L + 20 * MB + 8);
  }
}
