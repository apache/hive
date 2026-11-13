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

import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Decoder of Parquet's RLE / bit-packed hybrid encoding, for definition levels and dictionary ids. RLE runs are
 * array fills. Bit-packed runs unpack with one 8-byte load per value, by the byte packer near the end of the stream.
 * Run state survives across calls, so a page is drained in batch slices.
 */
final class VectorizedRleValuesReader {

  private final int[] group = new int[8];
  private int bitWidth;
  private int rleValueBytes;
  private BytePacker packer;
  private byte[] paddedGroup;

  private ByteBuffer buf;
  private int pos;
  private int limit;
  private boolean bitPacked;
  private int runLeft;
  private int rleValue;
  private int groupLeft;

  VectorizedRleValuesReader(int bitWidth) {
    setBitWidth(bitWidth);
  }

  private void setBitWidth(int bitWidth) {
    this.bitWidth = bitWidth;
    this.rleValueBytes = (bitWidth + 7) / 8;
    this.packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    this.paddedGroup = new byte[bitWidth];
  }

  /** A level stream: the whole buffer. */
  void initFromPage(ByteBuffer page) {
    reset(page, page.position(), page.limit());
  }

  /** A dictionary id stream, whose first byte is the bit width; an all-NULL page may carry no byte at all. */
  void initDictionaryIds(ByteBuffer page) {
    int start = page.position();
    int width = page.hasRemaining() ? page.get(start) & 0xFF : 0;
    if (width != bitWidth) {
      setBitWidth(width);
    }
    reset(page, Math.min(start + 1, page.limit()), page.limit());
  }

  private void reset(ByteBuffer page, int start, int end) {
    buf = page.order() == ByteOrder.LITTLE_ENDIAN ? page : page.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    pos = start;
    limit = end;
    runLeft = 0;
    groupLeft = 0;
  }

  /**
   * Decode the next {@code n} definition levels into {@code isNull[offset ..)}, a level below {@code maxLevel} being
   * NULL, and into {@code levels[offset ..)} unless it is null; returns how many are NULL.
   */
  int readDefinitionLevels(int n, int maxLevel, boolean[] isNull, int[] levels, int offset) {
    int end = offset + n;
    if (bitWidth == 1) {
      int nulls = readNulls(isNull, offset, n);
      for (int i = offset; levels != null && i < end; i++) {
        levels[i] = isNull[i] ? 0 : 1;
      }
      return nulls;
    }
    // A column with more than one level is a struct field, which always keeps its levels.
    readIntegers(n, levels, offset);
    int nulls = 0;
    for (int i = offset; i < end; i++) {
      boolean nullValue = levels[i] < maxLevel;
      isNull[i] = nullValue;
      nulls += nullValue ? 1 : 0;
    }
    return nulls;
  }

  /**
   * Decode the next {@code n} dictionary ids into {@code ids[0 .. n)}; returns the id when they are all the same,
   * otherwise -1. A run of one id that starts inside a bit-packed group is still one id.
   */
  int readDictionaryIds(int n, int[] ids) {
    readIntegers(n, ids, 0);
    for (int i = 1; i < n; i++) {
      if (ids[i] != ids[0]) {
        return -1;
      }
    }
    return n > 0 ? ids[0] : -1;
  }

  /**
   * Decode the next {@code n} values into {@code out[offset .. offset + n)}.
   */
  void readIntegers(int n, int[] out, int offset) {
    while (n > 0) {
      if (groupLeft > 0) {
        int k = Math.min(n, groupLeft);
        System.arraycopy(group, 8 - groupLeft, out, offset, k);
        groupLeft -= k;
        offset += k;
        n -= k;
        continue;
      }
      if (runLeft == 0) {
        readRunHeader();
        continue;
      }
      int k = Math.min(n, runLeft);
      if (!bitPacked) {
        Arrays.fill(out, offset, offset + k, rleValue);
        runLeft -= k;
        offset += k;
        n -= k;
        continue;
      }
      int whole = k & ~7;
      // Groups whose 8-byte loads stay inside the stream unpack a word per value; the rest byte-wise.
      int wordGroups = bitWidth == 0 ? 0
          : Math.min(whole >>> 3, Math.max(0, (limit - pos - Long.BYTES) / bitWidth));
      unpackWords(out, offset, wordGroups);
      for (int i = wordGroups * 8; i < whole; i += 8) {
        unpack(out, offset + i);
      }
      runLeft -= whole;
      offset += whole;
      n -= whole;
      k -= whole;
      if (k > 0) {
        unpack(group, 0);
        runLeft -= 8;
        System.arraycopy(group, 0, out, offset, k);
        groupLeft = 8 - k;
        offset += k;
        n -= k;
      }
    }
  }

  /**
   * Decode the next {@code n} values of a bit width 1 stream as NULL flags, value 0 being NULL, into
   * {@code isNull[offset .. offset + n)}; returns how many are NULL. A bit-packed group is one byte.
   */
  private int readNulls(boolean[] isNull, int offset, int n) {
    int nulls = 0;
    while (n > 0) {
      if (groupLeft > 0) {
        int k = Math.min(n, groupLeft);
        for (int i = 8 - groupLeft, end = i + k; i < end; i++, offset++) {
          boolean nullValue = group[i] == 0;
          isNull[offset] = nullValue;
          nulls += nullValue ? 1 : 0;
        }
        groupLeft -= k;
        n -= k;
        continue;
      }
      if (runLeft == 0) {
        readRunHeader();
        continue;
      }
      int k = Math.min(n, runLeft);
      if (!bitPacked) {
        boolean nullRun = rleValue == 0;
        Arrays.fill(isNull, offset, offset + k, nullRun);
        nulls += nullRun ? k : 0;
        runLeft -= k;
        offset += k;
        n -= k;
        continue;
      }
      int bytes = Math.min(k >>> 3, limit - pos);
      for (int end = pos + bytes; pos < end; pos++) {
        int b = buf.get(pos);
        for (int bit = 0; bit < 8; bit++, offset++) {
          isNull[offset] = (b & (1 << bit)) == 0;
        }
        nulls += 8 - Integer.bitCount(b & 0xFF);
      }
      runLeft -= bytes * 8;
      n -= bytes * 8;
      if (bytes * 8 < k) {
        // A partial or truncated last group goes through the group buffer.
        unpack(group, 0);
        runLeft -= 8;
        groupLeft = 8;
      }
    }
    return nulls;
  }

  private void readRunHeader() {
    int header = readUnsignedVarInt();
    bitPacked = (header & 1) == 1;
    if (bitPacked) {
      runLeft = (header >>> 1) * 8;
    } else {
      runLeft = header >>> 1;
      rleValue = 0;
      for (int i = 0; i < rleValueBytes; i++) {
        rleValue |= (buf.get(pos++) & 0xFF) << (8 * i);
      }
    }
  }

  private int readUnsignedVarInt() {
    int value = 0;
    int shift = 0;
    int b;
    do {
      b = buf.get(pos++);
      value |= (b & 0x7F) << shift;
      shift += 7;
    } while ((b & 0x80) != 0);
    return value;
  }

  /**
   * Unpack {@code groups} groups with one little-endian 8-byte load per value, which holds a value of
   * up to 57 bits wherever it starts within its first byte.
   */
  private void unpackWords(int[] out, int offset, int groups) {
    long mask = (1L << bitWidth) - 1;
    int n = groups * 8;
    for (int i = 0, bit = 0; i < n; i++, bit += bitWidth) {
      out[offset + i] = (int) ((buf.getLong(pos + (bit >>> 3)) >>> (bit & 7)) & mask);
    }
    pos += groups * bitWidth;
  }

  /**
   * The writer may truncate the last bit-packed group of a page; the missing bytes decode as zeros.
   */
  private void unpack(int[] out, int offset) {
    if (bitWidth == 0) {
      // A zero-width group packs into no bytes; the unpacker leaves the output untouched.
      Arrays.fill(out, offset, offset + 8, 0);
      return;
    }
    if (pos + bitWidth <= limit) {
      packer.unpack8Values(buf, pos, out, offset);
    } else {
      Arrays.fill(paddedGroup, (byte) 0);
      for (int i = 0; pos + i < limit; i++) {
        paddedGroup[i] = buf.get(pos + i);
      }
      packer.unpack8Values(paddedGroup, 0, out, offset);
    }
    pos += bitWidth;
  }
}
