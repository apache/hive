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

import java.io.*;
import java.util.Arrays;

/**
 * BloomFilter is a probabilistic data structure for set membership check. BloomFilters are
 * highly space efficient when compared to using a HashSet. Because of the probabilistic nature of
 * bloom filter false positive (element not present in bloom filter but test() says true) are
 * possible but false negatives are not possible (if element is present then test() will never
 * say false). The false positive probability is configurable (default: 5%) depending on which
 * storage requirement may increase or decrease. Lower the false positive probability greater
 * is the space requirement.
 * Bloom filters are sensitive to number of elements that will be inserted in the bloom filter.
 * During the creation of bloom filter expected number of entries must be specified. If the number
 * of insertions exceed the specified initial number of entries then false positive probability will
 * increase accordingly.
 *
 * Internally, this implementation of bloom filter uses Murmur3 fast non-cryptographic hash
 * algorithm. Although Murmur2 is slightly faster than Murmur3 in Java, it suffers from hash
 * collisions for specific sequence of repeating bytes. Check the following link for more info
 * https://code.google.com/p/smhasher/wiki/MurmurHash2Flaw
 */
public class BloomFilter {
  public static final double DEFAULT_FPP = 0.05;
  protected BitSet bitSet;
  protected int numBits;
  protected int numHashFunctions;

  public BloomFilter() {
  }

  public BloomFilter(long expectedEntries) {
    this(expectedEntries, DEFAULT_FPP);
  }

  static void checkArgument(boolean expression, String message) {
    if (!expression) {
      throw new IllegalArgumentException(message);
    }
  }

  public BloomFilter(long expectedEntries, double fpp) {
    checkArgument(expectedEntries > 0, "expectedEntries should be > 0");
    checkArgument(fpp > 0.0 && fpp < 1.0, "False positive probability should be > 0.0 & < 1.0");
    int nb = optimalNumOfBits(expectedEntries, fpp);
    // make 'm' multiple of 64
    this.numBits = nb + (Long.SIZE - (nb % Long.SIZE));
    this.numHashFunctions = optimalNumOfHashFunctions(expectedEntries, numBits);
    this.bitSet = new BitSet(numBits);
  }

  /**
   * A constructor to support rebuilding the BloomFilter from a serialized representation.
   * @param bits - bits are used as such for bitset and are NOT copied, any changes to bits will affect bloom filter
   * @param numFuncs - number of hash functions
   */
  public BloomFilter(long[] bits, int numFuncs) {
    super();
    // input long[] is set as such without copying, so any modification to the source will affect bloom filter
    this.bitSet = new BitSet(bits);
    this.numBits = bits.length * Long.SIZE;
    this.numHashFunctions = numFuncs;
  }

  static int optimalNumOfHashFunctions(long n, long m) {
    return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
  }

  static int optimalNumOfBits(long n, double p) {
    return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
  }

  public void add(byte[] val) {
    if (val == null) {
      addBytes(val, -1, -1);
    } else {
      addBytes(val, 0, val.length);
    }
  }

  public void addBytes(byte[] val, int offset, int length) {
    // We use the trick mentioned in "Less Hashing, Same Performance: Building a Better Bloom Filter"
    // by Kirsch et.al. From abstract 'only two hash functions are necessary to effectively
    // implement a Bloom filter without any loss in the asymptotic false positive probability'

    // Lets split up 64-bit hashcode into two 32-bit hash codes and employ the technique mentioned
    // in the above paper
    long hash64 = val == null ? Murmur3.NULL_HASHCODE :
        Murmur3.hash64(val, offset, length);
    addHash(hash64);
  }

  private void addHash(long hash64) {
    int hash1 = (int) hash64;
    int hash2 = (int) (hash64 >>> 32);

    for (int i = 1; i <= numHashFunctions; i++) {
      int combinedHash = hash1 + ((i + 1) * hash2);
      // hashcode should be positive, flip all the bits if it's negative
      if (combinedHash < 0) {
        combinedHash = ~combinedHash;
      }
      int pos = combinedHash % numBits;
      bitSet.set(pos);
    }
  }

  public void addString(String val) {
    if (val == null) {
      add(null);
    } else {
      add(val.getBytes());
    }
  }

  public void addLong(long val) {
    addHash(getLongHash(val));
  }

  public void addDouble(double val) {
    addLong(Double.doubleToLongBits(val));
  }

  public boolean test(byte[] val) {
    if (val == null) {
      return testBytes(val, -1, -1);
    }
    return testBytes(val, 0, val.length);
  }

  public boolean testBytes(byte[] val, int offset, int length) {
    long hash64 = val == null ? Murmur3.NULL_HASHCODE :
        Murmur3.hash64(val, offset, length);
    return testHash(hash64);
  }

  private boolean testHash(long hash64) {
    int hash1 = (int) hash64;
    int hash2 = (int) (hash64 >>> 32);

    for (int i = 1; i <= numHashFunctions; i++) {
      int combinedHash = hash1 + ((i + 1) * hash2);
      // hashcode should be positive, flip all the bits if it's negative
      if (combinedHash < 0) {
        combinedHash = ~combinedHash;
      }
      int pos = combinedHash % numBits;
      if (!bitSet.get(pos)) {
        return false;
      }
    }
    return true;
  }

  public boolean testString(String val) {
    if (val == null) {
      return test(null);
    } else {
      return test(val.getBytes());
    }
  }

  public boolean testLong(long val) {
    return testHash(getLongHash(val));
  }

  // Thomas Wang's integer hash function
  // http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm
  private long getLongHash(long key) {
    key = (~key) + (key << 21); // key = (key << 21) - key - 1;
    key = key ^ (key >> 24);
    key = (key + (key << 3)) + (key << 8); // key * 265
    key = key ^ (key >> 14);
    key = (key + (key << 2)) + (key << 4); // key * 21
    key = key ^ (key >> 28);
    key = key + (key << 31);
    return key;
  }

  public boolean testDouble(double val) {
    return testLong(Double.doubleToLongBits(val));
  }

  public long sizeInBytes() {
    return getBitSize() / 8;
  }

  public int getBitSize() {
    return bitSet.getData().length * Long.SIZE;
  }

  public int getNumHashFunctions() {
    return numHashFunctions;
  }

  public long[] getBitSet() {
    return bitSet.getData();
  }

  @Override
  public String toString() {
    return "m: " + numBits + " k: " + numHashFunctions;
  }

  /**
   * Merge the specified bloom filter with current bloom filter.
   *
   * @param that - bloom filter to merge
   */
  public void merge(BloomFilter that) {
    if (this != that && this.numBits == that.numBits && this.numHashFunctions == that.numHashFunctions) {
      this.bitSet.putAll(that.bitSet);
    } else {
      throw new IllegalArgumentException("BloomFilters are not compatible for merging." +
          " this - " + this.toString() + " that - " + that.toString());
    }
  }

  public void reset() {
    this.bitSet.clear();
  }

  /**
   * Serialize a bloom filter
   * @param out output stream to write to
   * @param bloomFilter BloomFilter that needs to be seralized
   */
  public static void serialize(OutputStream out, BloomFilter bloomFilter) throws IOException {
    /**
     * Serialized BloomFilter format:
     * 1 byte for the number of hash functions.
     * 1 big endian int(That is how OutputStream works) for the number of longs in the bitset
     * big endian longs in the BloomFilter bitset
     */
    DataOutputStream dataOutputStream = new DataOutputStream(out);
    dataOutputStream.writeByte(bloomFilter.numHashFunctions);
    dataOutputStream.writeInt(bloomFilter.getBitSet().length);
    for (long value : bloomFilter.getBitSet()) {
      dataOutputStream.writeLong(value);
    }
  }

  /**
   * Deserialize a bloom filter
   * Read a byte stream, which was written by {@linkplain #serialize(OutputStream, BloomFilter)}
   * into a {@code BloomFilter}
   * @param in input bytestream
   * @return deserialized BloomFilter
   */
  public static BloomFilter deserialize(InputStream in) throws IOException {
    if (in == null) {
      throw new IOException("Input stream is null");
    }

    try {
      DataInputStream dataInputStream = new DataInputStream(in);
      int numHashFunc = dataInputStream.readByte();
      int numLongs = dataInputStream.readInt();
      long[] data = new long[numLongs];
      for (int i = 0; i < numLongs; i++) {
        data[i] = dataInputStream.readLong();
      }
      return new BloomFilter(data, numHashFunc);
    } catch (RuntimeException e) {
      IOException io = new IOException( "Unable to deserialize BloomFilter");
      io.initCause(e);
      throw io;
    }
  }

  // Given a byte array consisting of a serialized BloomFilter, gives the offset (from 0)
  // for the start of the serialized long values that make up the bitset.
  // NumHashFunctions (1 byte) + NumBits (4 bytes)
  public static final int START_OF_SERIALIZED_LONGS = 5;

  /**
   * Merges BloomFilter bf2 into bf1.
   * Assumes 2 BloomFilters with the same size/hash functions are serialized to byte arrays
   * @param bf1Bytes
   * @param bf1Start
   * @param bf1Length
   * @param bf2Bytes
   * @param bf2Start
   * @param bf2Length
   */
  public static void mergeBloomFilterBytes(
      byte[] bf1Bytes, int bf1Start, int bf1Length,
      byte[] bf2Bytes, int bf2Start, int bf2Length) {
    if (bf1Length != bf2Length) {
      throw new IllegalArgumentException("bf1Length " + bf1Length + " does not match bf2Length " + bf2Length);
    }

    // Validation on the bitset size/3 hash functions.
    for (int idx = 0; idx < START_OF_SERIALIZED_LONGS; ++idx) {
      if (bf1Bytes[bf1Start + idx] != bf2Bytes[bf2Start + idx]) {
        throw new IllegalArgumentException("bf1 NumHashFunctions/NumBits does not match bf2");
      }
    }

    // Just bitwise-OR the bits together - size/# functions should be the same,
    // rest of the data is serialized long values for the bitset which are supposed to be bitwise-ORed.
    for (int idx = START_OF_SERIALIZED_LONGS; idx < bf1Length; ++idx) {
      bf1Bytes[bf1Start + idx] |= bf2Bytes[bf2Start + idx];
    }
  }

  /**
   * Bare metal bit set implementation. For performance reasons, this implementation does not check
   * for index bounds nor expand the bit set size if the specified index is greater than the size.
   */
  public class BitSet {
    private final long[] data;

    public BitSet(long bits) {
      this(new long[(int) Math.ceil((double) bits / (double) Long.SIZE)]);
    }

    /**
     * Deserialize long array as bit set.
     *
     * @param data - bit array
     */
    public BitSet(long[] data) {
      assert data.length > 0 : "data length is zero!";
      this.data = data;
    }

    /**
     * Sets the bit at specified index.
     *
     * @param index - position
     */
    public void set(int index) {
      data[index >>> 6] |= (1L << index);
    }

    /**
     * Returns true if the bit is set in the specified index.
     *
     * @param index - position
     * @return - value at the bit position
     */
    public boolean get(int index) {
      return (data[index >>> 6] & (1L << index)) != 0;
    }

    /**
     * Number of bits
     */
    public long bitSize() {
      return (long) data.length * Long.SIZE;
    }

    public long[] getData() {
      return data;
    }

    /**
     * Combines the two BitArrays using bitwise OR.
     */
    public void putAll(BitSet array) {
      assert data.length == array.data.length :
          "BitArrays must be of equal length (" + data.length + "!= " + array.data.length + ")";
      for (int i = 0; i < data.length; i++) {
        data[i] |= array.data[i];
      }
    }

    /**
     * Clear the bit set.
     */
    public void clear() {
      Arrays.fill(data, 0);
    }
  }
}
