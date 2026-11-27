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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Reads PLAIN values straight from the page buffer: little-endian fixed-width values, LSB-first bit-packed booleans
 * and length-prefixed binaries.
 */
final class VectorizedPlainValuesReader {

  private ByteBuffer buffer;
  private int position;
  private int bit;

  void initFromPage(ByteBuffer page) {
    buffer = page.order(ByteOrder.LITTLE_ENDIAN);
    position = page.position();
    bit = 0;
  }

  void readIntegers(int n, long[] values, int offset) {
    int p = position;
    for (int i = offset; i < offset + n; i++, p += Integer.BYTES) {
      values[i] = buffer.getInt(p);
    }
    position = p;
  }

  void readLongs(int n, long[] values, int offset) {
    int p = position;
    for (int i = offset; i < offset + n; i++, p += Long.BYTES) {
      values[i] = buffer.getLong(p);
    }
    position = p;
  }

  void readFloats(int n, double[] values, int offset) {
    int p = position;
    for (int i = offset; i < offset + n; i++, p += Float.BYTES) {
      values[i] = buffer.getFloat(p);
    }
    position = p;
  }

  void readDoubles(int n, double[] values, int offset) {
    int p = position;
    for (int i = offset; i < offset + n; i++, p += Double.BYTES) {
      values[i] = buffer.getDouble(p);
    }
    position = p;
  }

  void readBooleans(int n, long[] values, int offset) {
    int b = bit;
    for (int i = offset; i < offset + n; i++, b++) {
      values[i] = (buffer.get(position + (b >>> 3)) >>> (b & 7)) & 1;
    }
    bit = b;
  }

  /** Big-endian two's-complement values of {@code width} bytes, as their low 64 bits. */
  void readBigEndianLongs(int n, int width, long[] values, int offset) {
    int p = position;
    for (int i = offset; i < offset + n; i++, p += width) {
      long v = buffer.get(p) < 0 ? -1L : 0L;
      for (int k = 0; k < width; k++) {
        v = (v << 8) | (buffer.get(p + k) & 0xFF);
      }
      values[i] = v;
    }
    position = p;
  }

  /**
   * Length-prefixed values copied into the vector's own buffer, truncated to {@code maxLength} characters when it is
   * positive: the page is a cache buffer the consumer releases after decode, so it is never referenced.
   */
  void readBinary(int n, BytesColumnVector values, int offset, int maxLength) {
    int p = position;
    for (int i = offset; i < offset + n; i++) {
      int length = buffer.getInt(p);
      p += Integer.BYTES;
      values.ensureValPreallocated(length);
      byte[] target = values.getValPreallocatedBytes();
      int start = values.getValPreallocatedStart();
      buffer.get(p, target, start, length);
      p += length;
      values.setValPreallocated(i, maxLength > 0 ? StringExpr.truncate(target, start, length, maxLength) : length);
    }
    position = p;
  }
}
