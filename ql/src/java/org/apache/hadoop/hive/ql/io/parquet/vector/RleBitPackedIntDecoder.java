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
import java.util.Arrays;

/**
 * Parquet RLE / bit-packed hybrid decoder that fills an int[] slice per call: RLE runs are array
 * fills, bit-packed runs are unpacked eight values at a time. Decodes definition levels and
 * dictionary ids of a page; run state survives across calls so a page is drained in batch slices.
 */
final class RleBitPackedIntDecoder {

  private final int bitWidth;
  private final int rleValueBytes;
  private final BytePacker packer;
  private final byte[] paddedGroup;
  private final int[] group = new int[8];

  private ByteBuffer buf;
  private int pos;
  private int limit;
  private boolean bitPacked;
  private int runLeft;
  private int rleValue;
  private int groupLeft;

  RleBitPackedIntDecoder(int bitWidth) {
    this.bitWidth = bitWidth;
    this.rleValueBytes = (bitWidth + 7) / 8;
    this.packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    this.paddedGroup = new byte[bitWidth];
  }

  int bitWidth() {
    return bitWidth;
  }

  void reset(ByteBuffer buf, int pos, int limit) {
    this.buf = buf;
    this.pos = pos;
    this.limit = limit;
    runLeft = 0;
    groupLeft = 0;
  }

  /**
   * Decode the next {@code n} values into {@code out[outPos .. outPos + n)}.
   */
  void read(int[] out, int outPos, int n) {
    while (n > 0) {
      if (groupLeft > 0) {
        int k = Math.min(n, groupLeft);
        System.arraycopy(group, 8 - groupLeft, out, outPos, k);
        groupLeft -= k;
        outPos += k;
        n -= k;
        continue;
      }
      if (runLeft == 0) {
        readRunHeader();
        continue;
      }
      int k = Math.min(n, runLeft);
      if (!bitPacked) {
        Arrays.fill(out, outPos, outPos + k, rleValue);
        runLeft -= k;
        outPos += k;
        n -= k;
        continue;
      }
      int whole = k & ~7;
      for (int i = 0; i < whole; i += 8) {
        unpack(out, outPos + i);
      }
      runLeft -= whole;
      outPos += whole;
      n -= whole;
      k -= whole;
      if (k > 0) {
        unpack(group, 0);
        runLeft -= 8;
        System.arraycopy(group, 0, out, outPos, k);
        groupLeft = 8 - k;
        outPos += k;
        n -= k;
      }
    }
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
   * The writer may truncate the last bit-packed group of a page; the missing bytes decode as zeros.
   */
  private void unpack(int[] out, int outPos) {
    if (bitWidth == 0) {
      // A zero-width group packs into no bytes; the unpacker leaves the output untouched.
      Arrays.fill(out, outPos, outPos + 8, 0);
      return;
    }
    if (pos + bitWidth <= limit) {
      packer.unpack8Values(buf, pos, out, outPos);
    } else {
      Arrays.fill(paddedGroup, (byte) 0);
      for (int i = 0; pos + i < limit; i++) {
        paddedGroup[i] = buf.get(pos + i);
      }
      packer.unpack8Values(paddedGroup, 0, out, outPos);
    }
    pos += bitWidth;
  }
}
