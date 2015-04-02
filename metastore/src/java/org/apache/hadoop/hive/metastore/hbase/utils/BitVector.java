/**
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

package org.apache.hadoop.hive.metastore.hbase.utils;

import java.util.Arrays;

/**
 * Barebones fixed length bit vector using a byte array
 */
public class BitVector {
  // We'll use this as the bit vector container
  private byte data[];
  public static int ELEMENT_SIZE = Byte.SIZE;

  public BitVector(int size) {
    data = new byte[size/ELEMENT_SIZE];
  }

  /**
   * Total bits -> num elements * size of each element
   *
   */
  public long getSize() {
    return data.length * ELEMENT_SIZE;
  }

  /**
   * Set the bit at the given index to 1
   *
   * @param bitIndex
   */
  public void setBit(int bitIndex) {
    validateBitIndex(bitIndex);
    int dataIndex = bitIndex / ELEMENT_SIZE;
    int elementIndex = ELEMENT_SIZE - bitIndex % ELEMENT_SIZE - 1;
    // Set the elementIndex'th bit of data[dataIndex]'th element
    data[dataIndex] = (byte) (data[dataIndex] | (1 << elementIndex));
  }

  /**
   * Set the bit at the given index to 0
   *
   * @param bitIndex
   */
  public void unSetBit(int bitIndex) {
    validateBitIndex(bitIndex);
    int dataIndex = bitIndex / ELEMENT_SIZE;
    int elementIndex = ELEMENT_SIZE - bitIndex % ELEMENT_SIZE - 1;
    // Unset the elementIndex'th bit of data[dataIndex]'th element
    data[dataIndex] = (byte) (data[dataIndex] & ~(1 << elementIndex));
  }

  /**
   * Check if a bit at the given index is 1
   * @param bitIndex
   */
  public boolean isBitSet(int bitIndex) {
    validateBitIndex(bitIndex);
    int dataIndex = bitIndex / ELEMENT_SIZE;
    int elementIndex = ELEMENT_SIZE - bitIndex % ELEMENT_SIZE - 1;
    if ((data[dataIndex] & (1 << elementIndex)) > 0) {
      return true;
    }
    return false;
  }

  /**
   * Set all bits to 0
   *
   */
  public void clearAll() {
    Arrays.fill(data, (byte) 0x00);
  }

  /**
   * Set all bits to 1
   *
   */
  public void setAll() {
    Arrays.fill(data, (byte) 0xFF);
  }

  /**
   * Prints the bit vector as a string of bit values (e.g. 01010111)
   */
  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    for(byte b : data) {
      str.append(Integer.toBinaryString((b & 0xFF) + 0x100).substring(1));
    }
    return str.toString();
  }

  /**
   * Check if queried bitIndex is in valid range
   * @param bitIndex
   */
  private void validateBitIndex(int bitIndex) {
    if ((bitIndex >= getSize()) || (bitIndex < 0)) {
      throw new IllegalArgumentException("Bit index out of range: " + bitIndex);
    }
  }

}
