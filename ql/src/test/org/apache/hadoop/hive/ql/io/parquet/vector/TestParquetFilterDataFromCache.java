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
package org.apache.hadoop.hive.ql.io.parquet.vector;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.common.io.encoded.MemoryBufferOrBuffers;
import org.junit.Test;

/**
 * The cached ranges are served at the offsets the file stores them at, so an error in the range or buffer
 * bookkeeping hands Parquet a structurally valid but wrong bloom filter, which prunes the wrong row groups
 * and silently drops rows.
 */
public class TestParquetFilterDataFromCache {

  private static final long FIRST_OFFSET = 1_000L;
  private static final long SECOND_OFFSET = 50_000L;
  private static final long THIRD_OFFSET = 90_000L;

  @Test
  public void testEachRangeIsServedAtItsOwnOffset() throws IOException {
    byte[] first = filled(16, (byte) 1);
    byte[] second = filled(24, (byte) 2);
    byte[] third = filled(8, (byte) 3);
    ParquetFilterDataFromCache input = input(
        range(FIRST_OFFSET, first), range(SECOND_OFFSET, second), range(THIRD_OFFSET, third));

    assertArrayEquals(third, readAt(input, THIRD_OFFSET, third.length));
    assertArrayEquals(first, readAt(input, FIRST_OFFSET, first.length));
    assertArrayEquals(second, readAt(input, SECOND_OFFSET, second.length));
  }

  @Test
  public void testReadCrossesTheBuffersOfOneRange() throws IOException {
    byte[] whole = new byte[40];
    for (int i = 0; i < whole.length; ++i) {
      whole[i] = (byte) i;
    }
    // one range the cache split across three buffers, as it does for anything over the max allocation
    ParquetFilterDataFromCache input = input(range(FIRST_OFFSET,
        Arrays.copyOfRange(whole, 0, 16), Arrays.copyOfRange(whole, 16, 32), Arrays.copyOfRange(whole, 32, 40)));

    assertArrayEquals(whole, readAt(input, FIRST_OFFSET, whole.length));
    // starting inside the second buffer and running into the third
    assertArrayEquals(Arrays.copyOfRange(whole, 20, 36), readAt(input, FIRST_OFFSET + 20, 16));
  }

  @Test
  public void testPositionTracksTheReads() throws IOException {
    ParquetFilterDataFromCache input = input(range(FIRST_OFFSET, filled(16, (byte) 1)));
    input.seek(FIRST_OFFSET + 4);
    assertEquals(FIRST_OFFSET + 4, input.getPos());
    input.readFully(new byte[8], 0, 8);
    assertEquals(FIRST_OFFSET + 12, input.getPos());
  }

  @Test
  public void testSeekOutsideEveryRangeFails() throws IOException {
    ParquetFilterDataFromCache input =
        input(range(FIRST_OFFSET, filled(16, (byte) 1)), range(SECOND_OFFSET, filled(16, (byte) 2)));
    for (long offset : new long[] { 0L, FIRST_OFFSET - 1, FIRST_OFFSET + 16, SECOND_OFFSET + 16 }) {
      try {
        input.seek(offset);
        fail("seek to " + offset + " is outside every cached range and must not be served");
      } catch (IOException expected) {
        assertTrue(expected.getMessage(), expected.getMessage().contains("outside the cached ranges"));
      }
    }
  }

  @Test
  public void testReadPastTheEndOfARangeDoesNotRunIntoTheNext() throws IOException {
    ParquetFilterDataFromCache input =
        input(range(FIRST_OFFSET, filled(16, (byte) 1)), range(SECOND_OFFSET, filled(16, (byte) 2)));
    input.seek(FIRST_OFFSET);
    try {
      input.readFully(new byte[24], 0, 24);
      fail("a read may not continue past the range it started in");
    } catch (EOFException expected) {
      // the ranges are disjoint regions of the file, so the bytes after one are not the next one's
    }
  }

  private static byte[] readAt(ParquetFilterDataFromCache input, long offset, int length)
      throws IOException {
    input.seek(offset);
    byte[] read = new byte[length];
    input.readFully(read, 0, length);
    return read;
  }

  private static byte[] filled(int length, byte value) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, value);
    return bytes;
  }

  private static ParquetFilterDataFromCache input(ParquetFilterDataFromCache.Range... ranges) {
    return new ParquetFilterDataFromCache(List.of(ranges), null, null);
  }

  private static ParquetFilterDataFromCache.Range range(long offset, byte[]... chunks) {
    int length = 0;
    for (byte[] chunk : chunks) {
      length += chunk.length;
    }
    return new ParquetFilterDataFromCache.Range(offset, length, buffers(chunks));
  }

  private static MemoryBufferOrBuffers buffers(byte[]... chunks) {
    MemoryBuffer[] wrapped = new MemoryBuffer[chunks.length];
    for (int i = 0; i < chunks.length; ++i) {
      wrapped[i] = new HeapBuffer(chunks[i]);
    }
    return new MemoryBufferOrBuffers() {
      @Override
      public MemoryBuffer getSingleBuffer() {
        return (wrapped.length == 1) ? wrapped[0] : null;
      }

      @Override
      public MemoryBuffer[] getMultipleBuffers() {
        return (wrapped.length == 1) ? null : wrapped;
      }
    };
  }

  private static final class HeapBuffer implements MemoryBuffer {
    private final ByteBuffer data;

    HeapBuffer(byte[] bytes) {
      this.data = ByteBuffer.wrap(bytes);
    }

    @Override
    public ByteBuffer getByteBufferRaw() {
      return data;
    }

    @Override
    public ByteBuffer getByteBufferDup() {
      return data.duplicate();
    }
  }
}
