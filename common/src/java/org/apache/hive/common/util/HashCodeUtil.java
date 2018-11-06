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

package org.apache.hive.common.util;

/*
 * Common hash code routines.
 */
public class HashCodeUtil {

  // Constants for 32 bit variant
  private static final int C1_32 = 0xcc9e2d51;
  private static final int C2_32 = 0x1b873593;
  private static final int R1_32 = 15;
  private static final int R2_32 = 13;
  private static final int M_32 = 5;
  private static final int N_32 = 0xe6546b64;

  private static final int DEFAULT_SEED = 104729;

  public static int calculateIntHashCode(int key) {
    key = ~key + (key << 15); // key = (key << 15) - key - 1;
    key = key ^ (key >>> 12);
    key = key + (key << 2);
    key = key ^ (key >>> 4);
    key = key * 2057; // key = (key + (key << 3)) + (key << 11);
    key = key ^ (key >>> 16);
    return key;
  }
  public static int calculateTwoLongHashCode(long l0, long l1) {
    return hash32(l0, l1, DEFAULT_SEED);
  }

  public static int calculateLongHashCode(long key) {
    return hash32(key, DEFAULT_SEED);
  }

  public static void calculateLongArrayHashCodes(long[] longs, int[] hashCodes, final int count) {
    for (int v = 0; v < count; v++) {
      hashCodes[v] = (int) calculateLongHashCode(longs[v]);
    }
  }

  public static int calculateBytesHashCode(byte[] keyBytes, int keyStart, int keyLength) {
    return murmurHash(keyBytes, keyStart, keyLength);
  }

  public static void calculateBytesArrayHashCodes(byte[][] bytesArrays,
      int[] starts, int[] lengths, int[] valueSelected, int[] hashCodes, final int count) {

    for (int i = 0; i < count; i++) {
      int batchIndex = valueSelected[i];
      hashCodes[i] = murmurHash(bytesArrays[batchIndex], starts[batchIndex],
          lengths[batchIndex]);
    }
  }

  // Lifted from org.apache.hadoop.util.hash.MurmurHash... but supports offset.
  // Must produce the same result as MurmurHash.hash with seed = 0.
  public static int murmurHash(byte[] data, int offset, int length) {
    int m = 0x5bd1e995;
    int r = 24;

    int h = length;

    int len_4 = length >> 2;

    for (int i = 0; i < len_4; i++) {
      int i_4 = offset + (i << 2);
      int k = data[i_4 + 3];
      k = k << 8;
      k = k | (data[i_4 + 2] & 0xff);
      k = k << 8;
      k = k | (data[i_4 + 1] & 0xff);
      k = k << 8;
      k = k | (data[i_4 + 0] & 0xff);
      k *= m;
      k ^= k >>> r;
      k *= m;
      h *= m;
      h ^= k;
    }

    // avoid calculating modulo
    int len_m = len_4 << 2;
    int left = length - len_m;

    if (left != 0) {
      length += offset;
      if (left >= 3) {
        h ^= (int) data[length - 3] << 16;
      }
      if (left >= 2) {
        h ^= (int) data[length - 2] << 8;
      }
      if (left >= 1) {
        h ^= (int) data[length - 1];
      }

      h *= m;
    }

    h ^= h >>> 13;
    h *= m;
    h ^= h >>> 15;

    return h;
  }

  /**
   * Murmur3 32-bit variant.
   * @see Murmur3#hash32(byte[], int, int, int)
   */
  private static int hash32(long l0, long l1, int seed) {
    int hash = seed;

    final byte  b0 = (byte) (l0 >> 56);
    final byte  b1 = (byte) (l0 >> 48);
    final byte  b2 = (byte) (l0 >> 40);
    final byte  b3 = (byte) (l0 >> 32);
    final byte  b4 = (byte) (l0 >> 24);
    final byte  b5 = (byte) (l0 >> 16);
    final byte  b6 = (byte) (l0 >>  8);
    final byte  b7 = (byte) (l0 >>  0);
    final byte  b8 = (byte) (l1 >> 56);
    final byte  b9 = (byte) (l1 >> 48);
    final byte b10 = (byte) (l1 >> 40);
    final byte b11 = (byte) (l1 >> 32);
    final byte b12 = (byte) (l1 >> 24);
    final byte b13 = (byte) (l1 >> 16);
    final byte b14 = (byte) (l1 >>  8);
    final byte b15 = (byte) (l1 >>  0);

    // body
    int k;

    // first 8 bytes
    k = (b0 & 0xff)
        | ((b1 & 0xff) << 8)
        | ((b2 & 0xff) << 16)
        | ((b3 & 0xff) << 24);
    k *= C1_32;
    k = Integer.rotateLeft(k, R1_32);
    k *= C2_32;
    hash ^= k;
    hash = Integer.rotateLeft(hash, R2_32) * M_32 + N_32;

    // second 8 bytes
    k = (b4 & 0xff)
        | ((b5 & 0xff) << 8)
        | ((b6 & 0xff) << 16)
        | ((b7 & 0xff) << 24);
    k *= C1_32;
    k = Integer.rotateLeft(k, R1_32);
    k *= C2_32;
    hash ^= k;
    hash = Integer.rotateLeft(hash, R2_32) * M_32 + N_32;

    // third 8 bytes
    k = (b8 & 0xff)
        | ((b9 & 0xff) << 8)
        | ((b10 & 0xff) << 16)
        | ((b11 & 0xff) << 24);
    k *= C1_32;
    k = Integer.rotateLeft(k, R1_32);
    k *= C2_32;
    hash ^= k;
    hash = Integer.rotateLeft(hash, R2_32) * M_32 + N_32;

    // last 8 bytes
    k = (b12 & 0xff)
        | ((b13 & 0xff) << 8)
        | ((b14 & 0xff) << 16)
        | ((b15 & 0xff) << 24);
    k *= C1_32;
    k = Integer.rotateLeft(k, R1_32);
    k *= C2_32;
    hash ^= k;
    hash = Integer.rotateLeft(hash, R2_32) * M_32 + N_32;

    // finalization
    hash ^= 16;
    hash ^= (hash >>> 16);
    hash *= 0x85ebca6b;
    hash ^= (hash >>> 13);
    hash *= 0xc2b2ae35;
    hash ^= (hash >>> 16);

    return hash;
  }

  /**
   * Murmur3 32-bit variant.
   * @see Murmur3#hash32(byte[], int, int, int)
   */
  private static int hash32(long l0, int seed) {
    int hash = seed;

    final byte b0 = (byte) (l0 >> 56);
    final byte b1 = (byte) (l0 >> 48);
    final byte b2 = (byte) (l0 >> 40);
    final byte b3 = (byte) (l0 >> 32);
    final byte b4 = (byte) (l0 >> 24);
    final byte b5 = (byte) (l0 >> 16);
    final byte b6 = (byte) (l0 >>  8);
    final byte b7 = (byte) (l0 >>  0);

    // body
    int k;

    // first 8 bytes
    k = (b0 & 0xff)
        | ((b1 & 0xff) << 8)
        | ((b2 & 0xff) << 16)
        | ((b3 & 0xff) << 24);
    k *= C1_32;
    k = Integer.rotateLeft(k, R1_32);
    k *= C2_32;
    hash ^= k;
    hash = Integer.rotateLeft(hash, R2_32) * M_32 + N_32;

    // last 8 bytes
    k = (b4 & 0xff)
        | ((b5 & 0xff) << 8)
        | ((b6 & 0xff) << 16)
        | ((b7 & 0xff) << 24);
    k *= C1_32;
    k = Integer.rotateLeft(k, R1_32);
    k *= C2_32;
    hash ^= k;
    hash = Integer.rotateLeft(hash, R2_32) * M_32 + N_32;

    // finalization
    hash ^= 16;
    hash ^= (hash >>> 16);
    hash *= 0x85ebca6b;
    hash ^= (hash >>> 13);
    hash *= 0xc2b2ae35;
    hash ^= (hash >>> 16);

    return hash;
  }
}