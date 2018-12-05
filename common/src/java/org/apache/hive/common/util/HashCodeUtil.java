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

  public static int murmurHash(byte[] data, int offset, int length) {
    return Murmur3.hash32(data, offset, length, 0);
  }
}