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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.Allocator;
import org.apache.hadoop.hive.common.io.DataCache;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for {@link LlapCacheAwareFs}, the filesystem shim that serves reads through the LLAP cache. */
public class TestLlapCacheAwareFs {
  private static final int MB = 1 << 20;
  /** Not a power of two, as getMaxAllocation() may return; the layout rounds it down to 4MB. */
  private static final int MAX_ALLOC = 6 * MB;

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

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
  public void aMissOffTheMaxAllocGridIsServed() throws IOException {
    // A gap need not align to MAX_ALLOC: evicting the second buffer leaves [4,8) MB, and neither
    // end is a multiple of 6MB.
    try (LlapCacheAwareFs.CacheAwareInputStream stream = streamOverOneChunk()) {
      SortedMap<Long, Long> chunks = stream.getAndValidateMissingChunks(1000L + 4 * MB, 1000L + 8 * MB);
      assertEquals(1, chunks.size());
      assertEquals(1000L, (long) chunks.firstKey());
    }
  }

  @Test
  public void aMissEndingMidChunkIsServed() throws IOException {
    // A gap may end mid-chunk on any boundary: evicting the fourth buffer leaves [12,16) MB, whose
    // end is not a multiple of 6MB.
    try (LlapCacheAwareFs.CacheAwareInputStream stream = streamOverOneChunk()) {
      assertEquals(1, stream.getAndValidateMissingChunks(1000L + 12 * MB, 1000L + 16 * MB).size());
    }
  }

  @Test
  public void theWholeChunkIsServed() throws IOException {
    try (LlapCacheAwareFs.CacheAwareInputStream stream = streamOverOneChunk()) {
      assertEquals(1, stream.getAndValidateMissingChunks(1000L, 1000L + 20 * MB).size());
    }
  }

  @Test
  public void readFullyHonoursTheArrayOffsetAndLength() throws IOException {
    // Hadoop's vectored read fallback lands here as readFully(pos, buf.array(), buf.arrayOffset(), len).
    byte[] content = new byte[100];
    for (int i = 0; i < content.length; ++i) {
      content[i] = (byte) (i + 1);
    }
    File file = tmp.newFile("chunk.bin");
    Files.write(file.toPath(), content);
    TreeMap<Long, Long> chunkIndex = new TreeMap<>();
    chunkIndex.put(0L, (long) content.length);
    DataCache cache = mock(DataCache.class);
    // Nothing cached: the range comes back with no data, and a null fileKey keeps it out of the cache.
    when(cache.getFileData(any(), any(), anyLong(), any(), any())).thenAnswer(inv -> inv.getArgument(1));
    byte[] array = new byte[80];
    Arrays.fill(array, (byte) -1);
    try (LlapCacheAwareFs.CacheAwareInputStream stream = new LlapCacheAwareFs.CacheAwareInputStream(cache,
        new Configuration(), chunkIndex, new Path(file.toURI()), null, 4096, null, null)) {
      stream.readFully(20, array, 10, 50);
    }

    for (int i = 0; i < 10; ++i) {
      assertEquals("array[" + i + "]", -1, array[i]);
    }
    for (int i = 0; i < 50; ++i) {
      assertEquals("array[" + (10 + i) + "]", content[20 + i], array[10 + i]);
    }
    for (int i = 60; i < array.length; ++i) {
      assertEquals("array[" + i + "]", -1, array[i]);
    }
  }

  @Test
  public void aZeroLengthReadDoesNotConsultTheCache() throws IOException {
    DataCache cache = mock(DataCache.class);
    try (LlapCacheAwareFs.CacheAwareInputStream stream = new LlapCacheAwareFs.CacheAwareInputStream(cache,
        new Configuration(), new TreeMap<>(), new Path("/tmp/does-not-matter"), 1L, -1, null, null)) {
      assertEquals(0, stream.read(new byte[4], 2, 0));
    }
    verifyNoInteractions(cache);
  }

  // The rejections below should never fire in production: each means a bug. The chunk index holds
  // the projected column chunks of the split's footer, and the Parquet reader asks for ranges from
  // that same footer, so every miss lies inside registered chunks.

  @Test(expected = AssertionError.class)
  public void aMissBelowEveryRegisteredChunkIsRejected() throws IOException {
    try (LlapCacheAwareFs.CacheAwareInputStream stream = streamOverOneChunk()) {
      stream.getAndValidateMissingChunks(0L, 500L);
    }
  }

  @Test(expected = AssertionError.class)
  public void aMissRunningPastTheLastRegisteredChunkIsRejected() throws IOException {
    try (LlapCacheAwareFs.CacheAwareInputStream stream = streamOverOneChunk()) {
      stream.getAndValidateMissingChunks(1000L + 19 * MB, 1000L + 21 * MB);
    }
  }

  @Test(expected = AssertionError.class)
  public void aMissInTheHoleBetweenTwoChunksIsRejected() throws IOException {
    // The hole is an unprojected column's chunk, which is never registered.
    TreeMap<Long, Long> chunkIndex = new TreeMap<>();
    chunkIndex.put(1000L, 1000L + 20 * MB);
    chunkIndex.put(1000L + 21 * MB, 1000L + 30 * MB);
    try (LlapCacheAwareFs.CacheAwareInputStream stream = streamOver(chunkIndex)) {
      stream.getAndValidateMissingChunks(1000L + 20 * MB, 1000L + 20 * MB + 8);
    }
  }
}
