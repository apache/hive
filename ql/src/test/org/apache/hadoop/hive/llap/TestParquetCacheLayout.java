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

import java.util.Arrays;

import org.junit.Test;

public class TestParquetCacheLayout {
  private static final int MB = 1 << 20;
  /** hive.llap.io.encode.alloc.size at its default, the grain these expectations assume. */
  private static final long FLOOR = 256 * 1024;

  @Test
  public void chunksAreCutIntoPowersOfTwoLargestFirst() {
    ParquetCacheLayout layout = new ParquetCacheLayout(16 * MB, FLOOR);
    assertArrayEquals(new int[] {4 * MB, MB}, layout.bufferSizes(5L * MB));
    assertArrayEquals(new int[] {16 * MB, 4 * MB, MB, 512 * 1024, 100},
        layout.bufferSizes(21L * MB + 512 * 1024 + 100));
    assertArrayEquals(new int[] {16 * MB, 16 * MB}, layout.bufferSizes(32L * MB));
    assertArrayEquals(new int[0], layout.bufferSizes(0));
  }

  @Test
  public void belowTheFloorTheRemainderStaysOneBuffer() {
    assertArrayEquals(new int[] {200 * 1024}, new ParquetCacheLayout(16 * MB, FLOOR).bufferSizes(200 * 1024));
  }

  @Test
  public void aSmallAllocatorCapsEveryBufferAndLowersTheFloorWithIt() {
    assertArrayEquals(new int[] {4096, 4096, 1000}, new ParquetCacheLayout(4096, FLOOR).bufferSizes(9192));
  }

  @Test
  public void anyRunOfBuffersDecomposesIntoItself() {
    // The property the shared layout exists for: a gap left by evicting some of a chunk's buffers
    // must re-cache on the boundaries the original split used, or the bytes are held twice under
    // keys that never match. Lengths cover the floor, both sides of maxAlloc and odd tails.
    long[] lengths = {1, 4095, 4096, 4097, 200 * 1024, FLOOR - 1, FLOOR, FLOOR + 1, MB - 1, MB,
        5L * MB, 15L * MB + 999, 16L * MB, 16L * MB + 1, 21L * MB + 512 * 1024 + 100, 100L * MB - 7};
    for (int maxAlloc : new int[] {4096, MB, 16 * MB}) {
      ParquetCacheLayout layout = new ParquetCacheLayout(maxAlloc, FLOOR);
      for (long length : lengths) {
        int[] sizes = layout.bufferSizes(length);
        if (sizes.length > 48) {
          continue; // a tiny maxAlloc on a long range is thousands of equal buffers; nothing new to learn
        }
        for (int from = 0; from < sizes.length; ++from) {
          long run = 0;
          for (int to = from; to < sizes.length; ++to) {
            run += sizes[to];
            assertArrayEquals("length " + length + " maxAlloc " + maxAlloc + " run [" + from + ", " + to + "]",
                Arrays.copyOfRange(sizes, from, to + 1), layout.bufferSizes(run));
          }
        }
      }
    }
  }

  @Test
  public void theRangeCapNeverFallsBelowTheAllocatorsMaximumNorBelowEightMb() {
    assertEquals(8 * MB, new ParquetCacheLayout(4 * MB, FLOOR).maxRangeBytes());
    assertEquals(8 * MB, new ParquetCacheLayout(8 * MB, FLOOR).maxRangeBytes());
    assertEquals(16 * MB, new ParquetCacheLayout(16 * MB, FLOOR).maxRangeBytes());
    assertEquals(32 * MB, new ParquetCacheLayout(32 * MB, FLOOR).maxRangeBytes());
  }

  @Test
  public void aFloorAboveTheAllocatorsMaximumIsClampedNotOverflowed() {
    // The key is validated as a size but not against the allocator's maximum.
    assertArrayEquals(new int[] {4096, 4096, 1000}, new ParquetCacheLayout(4096, 4L << 30).bufferSizes(9192));
  }
}
