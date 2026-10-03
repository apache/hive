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
package org.apache.hadoop.hive.ql.io.parquet.vector;

import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * The hybrid decoder against parquet's own encoder and decoder, over every bit width and random run structure, read
 * in random slice sizes; and definition levels decoded into NULL flags at the widths of one to three levels.
 */
public class TestVectorizedRleValuesReader {

  @Test
  public void testReadIntegers() throws IOException {
    Random rnd = new Random(7);
    for (int bitWidth = 0; bitWidth <= 32; bitWidth++) {
      for (int round = 0; round < 4; round++) {
        int n = 1 + rnd.nextInt(5000);
        int[] values = randomRuns(rnd, bitWidth, n);
        byte[] bytes = encode(bitWidth, values);
        RunLengthBitPackingHybridDecoder reference =
            new RunLengthBitPackingHybridDecoder(bitWidth, new ByteArrayInputStream(bytes));
        VectorizedRleValuesReader reader = new VectorizedRleValuesReader(bitWidth);
        reader.initFromPage(ByteBuffer.wrap(bytes));
        int[] out = new int[n];
        for (int pos = 0; pos < n; ) {
          int slice = Math.min(n - pos, 1 + rnd.nextInt(300));
          reader.readIntegers(slice, out, pos);
          pos += slice;
        }
        for (int k = 0; k < n; k++) {
          int expected = reference.readInt();
          assertEquals(values[k], expected);
          assertEquals("bitWidth " + bitWidth + " round " + round + " value " + k, expected, out[k]);
        }
      }
    }
  }

  /** A dictionary id stream starts with its bit width; an all-NULL page may hold no byte at all. */
  @Test
  public void testDictionaryIds() throws IOException {
    Random rnd = new Random(3);
    VectorizedRleValuesReader reader = new VectorizedRleValuesReader(0);
    for (int bitWidth : new int[] { 3, 11, 1, 0, 20 }) {
      int[] values = randomRuns(rnd, bitWidth, 3000);
      byte[] encoded = encode(bitWidth, values);
      byte[] page = new byte[encoded.length + 1];
      page[0] = (byte) bitWidth;
      System.arraycopy(encoded, 0, page, 1, encoded.length);
      reader.initDictionaryIds(ByteBuffer.wrap(page));
      int[] out = new int[values.length];
      reader.readIntegers(values.length, out, 0);
      for (int k = 0; k < values.length; k++) {
        assertEquals("bitWidth " + bitWidth + " id " + k, values[k], out[k]);
      }
    }
    reader.initDictionaryIds(ByteBuffer.allocate(0));
    reader.readIntegers(0, new int[0], 0);
  }

  /**
   * Definition levels of a column with one to seven levels, read in random slices into NULL flags, with and without
   * the struct field's level array, against the levels the stream holds.
   */
  @Test
  public void testDefinitionLevels() throws IOException {
    Random rnd = new Random(11);
    for (int maxLevel = 1; maxLevel <= 7; maxLevel++) {
      int bitWidth = 32 - Integer.numberOfLeadingZeros(maxLevel);
      for (int round = 0; round < 20; round++) {
        int n = 1 + rnd.nextInt(5000);
        int[] written = randomRuns(rnd, bitWidth, n);
        for (int k = 0; k < n; k++) {
          written[k] = written[k] % (maxLevel + 1);
        }
        byte[] bytes = encode(bitWidth, written);
        // Only a one-bit stream may come without the struct field's level array.
        for (boolean withLevels : bitWidth == 1 ? new boolean[] { false, true } : new boolean[] { true }) {
          VectorizedRleValuesReader reader = new VectorizedRleValuesReader(bitWidth);
          reader.initFromPage(ByteBuffer.wrap(bytes));
          boolean[] isNull = new boolean[n];
          int[] levels = withLevels ? new int[n] : null;
          String label = "maxLevel " + maxLevel + " round " + round + " levels " + withLevels;
          for (int pos = 0; pos < n; ) {
            int slice = Math.min(n - pos, 1 + rnd.nextInt(300));
            int expectedNulls = 0;
            for (int k = pos; k < pos + slice; k++) {
              expectedNulls += written[k] < maxLevel ? 1 : 0;
            }
            assertEquals(label + " nulls at " + pos, expectedNulls,
                reader.readDefinitionLevels(slice, maxLevel, isNull, levels, pos));
            pos += slice;
          }
          for (int k = 0; k < n; k++) {
            assertEquals(label + " isNull " + k, written[k] < maxLevel, isNull[k]);
            if (withLevels) {
              assertEquals(label + " level " + k, written[k], levels[k]);
            }
          }
        }
      }
    }
  }

  /** A bit-packed run whose last group the writer truncated decodes the missing levels as NULL. */
  @Test
  public void testTruncatedGroup() {
    // Two groups announced, one byte present: eight non-NULLs, then the missing group.
    byte[] bytes = { (2 << 1) | 1, (byte) 0xFF };
    VectorizedRleValuesReader reader = new VectorizedRleValuesReader(1);
    reader.initFromPage(ByteBuffer.wrap(bytes));
    boolean[] isNull = new boolean[16];
    assertEquals(3, reader.readDefinitionLevels(11, 1, isNull, null, 0));
    assertEquals(5, reader.readDefinitionLevels(5, 1, isNull, null, 11));
    for (int k = 0; k < 16; k++) {
      assertEquals("level " + k, k >= 8, isNull[k]);
    }
  }

  /** Runs of repeated values and runs of random ones, so the encoder writes both RLE and bit-packed runs. */
  private static int[] randomRuns(Random rnd, int bitWidth, int n) {
    long bound = 1L << bitWidth;
    int[] values = new int[n];
    int i = 0;
    while (i < n) {
      int run = 1 + rnd.nextInt(40);
      boolean repeat = rnd.nextBoolean();
      int v = (int) (rnd.nextLong() & (bound - 1));
      for (int k = 0; k < run && i < n; k++, i++) {
        values[i] = repeat ? v : (int) (rnd.nextLong() & (bound - 1));
      }
    }
    return values;
  }

  private static byte[] encode(int bitWidth, int[] values) throws IOException {
    try (RunLengthBitPackingHybridEncoder encoder =
        new RunLengthBitPackingHybridEncoder(bitWidth, 64, 64 << 10, new HeapByteBufferAllocator())) {
      for (int v : values) {
        encoder.writeInt(v);
      }
      return encoder.toBytes().toByteArray();
    }
  }
}
