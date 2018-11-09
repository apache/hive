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
    return Murmur3.hash32(l0, l1);
  }

  public static int calculateLongHashCode(long key) {
    return Murmur3.hash32(key);
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
}