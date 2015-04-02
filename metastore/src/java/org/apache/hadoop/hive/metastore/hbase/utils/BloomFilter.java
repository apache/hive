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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Hash;

/**
 * Barebones bloom filter implementation
 */
public class BloomFilter {
  private static final Log LOG = LogFactory.getLog(BloomFilter.class.getName());
  // The cardinality of the set for which we are generating the bloom filter
  // Default size is 10000
  private int setSize = 10000;
  // The probability of false positives we are ready to tolerate
  // Default is 1%
  private double falsePositiveProbability = 0.01;
  // Number of bits used for the filter
  // Formula: -n*ln(p) / (ln(2)^2)
  private int numBits;
  // Number of hashing functions
  // Formula: m/n * ln(2)
  private int numHashFunctions;
  private final Hash hash;
  private BitVector bitVector;

  public BloomFilter(int setSize, double falsePositiveProbability) {
    this.setSize = setSize;
    this.falsePositiveProbability = falsePositiveProbability;
    this.numBits = calculateFilterSize(this.setSize, this.falsePositiveProbability);
    this.numHashFunctions = calculateHashFunctions(this.setSize, this.numBits);
    // Create a bit vector of size numBits
    this.bitVector = new BitVector(numBits);
    // Use MurmurHash3
    hash = Hash.getInstance(Hash.MURMUR_HASH3);
  }

  /**
   * Calculate the number of bits in the filter
   * Also align size to BitVector.HOLDER_SIZE
   * @param setSize
   * @param falsePositiveProbability
   * @return numBits
   */
  private int calculateFilterSize(int setSize, double falsePositiveProbability) {
    int numBits = (int) (-setSize * Math.log(falsePositiveProbability) / (Math.log(2) * Math.log(2)));
    numBits = numBits + (BitVector.ELEMENT_SIZE - (numBits % BitVector.ELEMENT_SIZE));
    LOG.info("Bloom Filter size: " + numBits);
    return numBits;
  }

  /**
   * Calculate the number of hash functions needed by the BloomFilter
   * @param setSize
   * @param numBits
   * @return numHashFunctions
   */
  private int calculateHashFunctions(int setSize, int numBits) {
    int numHashFunctions = Math.max(1, (int) Math.round((double) numBits / setSize * Math.log(2)));
    LOG.info("Number of hashing functions: " + numHashFunctions);
    return numHashFunctions;
  }

  /**
   * @return the underlying BitVector object
   */
  public BitVector getBitVector() {
    return bitVector;
  }

  public int getFilterSize() {
    return numBits;
  }


  public int getNumHashFunctions() {
    return numHashFunctions;
  }

  /**
   * Add an item to the filter
   *
   * @param item to add
   */
  public void addToFilter(byte[] item) {
    int bitIndex;
    // Hash the item numHashFunctions times
    for (int i = 0; i < numHashFunctions; i++) {
      bitIndex = getBitIndex(item, i);
      // Set the bit at this index
      bitVector.setBit(bitIndex);
    }
  }

  /**
   * Check whether the item is present in the filter
   * @param candidate
   * @return hasItem (true if the bloom filter contains the item)
   */
  public boolean contains(byte[] item) {
    int bitIndex;
    boolean hasItem = true;
    // Hash the item numHashFunctions times
    for (int i = 0; i < numHashFunctions; i++) {
      bitIndex = getBitIndex(item, i);
      hasItem = hasItem && bitVector.isBitSet(bitIndex);
      if (!hasItem) {
        return hasItem;
      }
    }
    return hasItem;
  }

  /**
   * Hash the item using the i as the seed and return its potential position in the bit vector
   * Also we negate a negative hash value
   *
   * @param item
   * @param i (for the i-th hash function)
   * @return position of item in unerlying bit vector
   */
  private int getBitIndex(byte[] item, int i) {
    return Math.abs(hash.hash(item, i) % (numBits));
  }
}
