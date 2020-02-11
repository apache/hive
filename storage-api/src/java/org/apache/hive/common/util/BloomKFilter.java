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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * BloomKFilter is variation of {@link BloomFilter}. Unlike BloomFilter, BloomKFilter will spread
 * 'k' hash bits within same cache line for better L1 cache performance. The way it works is,
 * First hash code is computed from key which is used to locate the block offset (n-longs in bitset constitute a block)
 * Subsequent 'k' hash codes are used to spread hash bits within the block. By default block size is chosen as 8,
 * which is to match cache line size (8 longs = 64 bytes = cache line size).
 * Refer {@link BloomKFilter#addBytes(byte[])} for more info.
 *
 * This implementation has much lesser L1 data cache misses than {@link BloomFilter}.
 */
@SuppressWarnings({ "WeakerAccess", "unused" }) public class BloomKFilter {
  public static final float DEFAULT_FPP = 0.05f;
  private static final int DEFAULT_BLOCK_SIZE = 8;
  private static final int DEFAULT_BLOCK_SIZE_BITS = (int) (Math.log(DEFAULT_BLOCK_SIZE) / Math.log(2));
  private static final int DEFAULT_BLOCK_OFFSET_MASK = DEFAULT_BLOCK_SIZE - 1;
  private static final int DEFAULT_BIT_OFFSET_MASK = Long.SIZE - 1;
  private final BitSet bitSet;
  private final long m;
  private final int k;
  // spread k-1 bits to adjacent longs, default is 8
  // spreading hash bits within blockSize * longs will make bloom filter L1 cache friendly
  // default block size is set to 8 as most cache line sizes are 64 bytes and also AVX512 friendly
  private final int totalBlockCount;

  private static void checkArgument(boolean expression, String message) {
    if (!expression) {
      throw new IllegalArgumentException(message);
    }
  }

  public BloomKFilter(long maxNumEntries) {
    checkArgument(maxNumEntries > 0, "expectedEntries should be > 0");
    long numBits = optimalNumOfBits(maxNumEntries, DEFAULT_FPP);
    this.k = optimalNumOfHashFunctions(maxNumEntries, numBits);
    long nLongs = (long) Math.ceil((double) numBits / (double) Long.SIZE);
    // additional bits to pad long array to block size
    long padLongs = DEFAULT_BLOCK_SIZE - nLongs % DEFAULT_BLOCK_SIZE;
    this.m = (nLongs + padLongs) * Long.SIZE;
    this.bitSet = new BitSet(m);
    checkArgument((bitSet.data.length % DEFAULT_BLOCK_SIZE) == 0, "bitSet has to be block aligned");
    this.totalBlockCount = bitSet.data.length / DEFAULT_BLOCK_SIZE;
  }

  /**
   * A constructor to support rebuilding the BloomFilter from a serialized representation.
   * @param bits BloomK sketch data in form of array of longs.
   * @param numFuncs  Number of functions called as K.
   */
  public BloomKFilter(long[] bits, int numFuncs) {
    super();
    bitSet = new BitSet(bits);
    this.m = bits.length * Long.SIZE;
    this.k = numFuncs;
    checkArgument((bitSet.data.length % DEFAULT_BLOCK_SIZE) == 0, "bitSet has to be block aligned");
    this.totalBlockCount = bitSet.data.length / DEFAULT_BLOCK_SIZE;
  }
  static int optimalNumOfHashFunctions(long n, long m) {
    return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
  }

  static long optimalNumOfBits(long n, double p) {
    return (long) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
  }

  public void add(byte[] val) {
    addBytes(val);
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

  public void addBytes(byte[] val) {
    addBytes(val, 0, val.length);
  }

  private void addHash(long hash64) {
    final int hash1 = (int) hash64;
    final int hash2 = (int) (hash64 >>> 32);

    int firstHash = hash1 + hash2;
    // hashcode should be positive, flip all the bits if it's negative
    if (firstHash < 0) {
      firstHash = ~firstHash;
    }

    // first hash is used to locate start of the block (blockBaseOffset)
    // subsequent K hashes are used to generate K bits within a block of words
    final int blockIdx = firstHash % totalBlockCount;
    final int blockBaseOffset = blockIdx << DEFAULT_BLOCK_SIZE_BITS;
    for (int i = 1; i <= k; i++) {
      int combinedHash = hash1 + ((i + 1) * hash2);
      // hashcode should be positive, flip all the bits if it's negative
      if (combinedHash < 0) {
        combinedHash = ~combinedHash;
      }
      // LSB 3 bits is used to locate offset within the block
      final int absOffset = blockBaseOffset + (combinedHash & DEFAULT_BLOCK_OFFSET_MASK);
      // Next 6 bits are used to locate offset within a long/word
      final int bitPos = (combinedHash >>> DEFAULT_BLOCK_SIZE_BITS) & DEFAULT_BIT_OFFSET_MASK;
      bitSet.data[absOffset] |= (1L << bitPos);
    }
  }

  public void addString(String val) {
    addBytes(val.getBytes());
  }

  public void addByte(byte val) {
    addBytes(new byte[]{val});
  }

  public void addInt(int val) {
    addHash(Murmur3.hash64(val));
  }


  public void addLong(long val) {
    // puts long in little endian order
    addHash(Murmur3.hash64(val));
  }

  public void addFloat(float val) {
    addInt(Float.floatToIntBits(val));
  }

  public void addDouble(double val) {
    addLong(Double.doubleToLongBits(val));
  }

  public boolean test(byte[] val) {
    return testBytes(val);
  }

  public boolean testBytes(byte[] val) {
    return testBytes(val, 0, val.length);
  }

  public boolean testBytes(byte[] val, int offset, int length) {
    long hash64 = val == null ? Murmur3.NULL_HASHCODE :
      Murmur3.hash64(val, offset, length);
    return testHash(hash64);
  }

  private boolean testHash(long hash64) {
    final int hash1 = (int) hash64;
    final int hash2 = (int) (hash64 >>> 32);
    final long[] bits = bitSet.data;

    int firstHash = hash1 + hash2;
    // hashcode should be positive, flip all the bits if it's negative
    if (firstHash < 0) {
      firstHash = ~firstHash;
    }

    // first hash is used to locate start of the block (blockBaseOffset)
    // subsequent K hashes are used to generate K bits within a block of words
    // To avoid branches during probe, a separate masks array is used for each longs/words within a block.
    // data array and masks array are then traversed together and checked for corresponding set bits.
    final int blockIdx = firstHash % totalBlockCount;
    final int blockBaseOffset = blockIdx << DEFAULT_BLOCK_SIZE_BITS;

    // iterate and update masks array
    for (int i = 1; i <= k; i++) {
      int combinedHash = hash1 + ((i + 1)  * hash2);
      // hashcode should be positive, flip all the bits if it's negative
      if (combinedHash < 0) {
        combinedHash = ~combinedHash;
      }
      // LSB 3 bits is used to locate offset within the block
      final int wordOffset = combinedHash & DEFAULT_BLOCK_OFFSET_MASK;
      final int absOffset = blockBaseOffset + wordOffset;
      // Next 6 bits are used to locate offset within a long/word
      final int bitPos = (combinedHash >>> DEFAULT_BLOCK_SIZE_BITS) & DEFAULT_BIT_OFFSET_MASK;
      final long bloomWord = bits[absOffset];
      if (0 == (bloomWord & (1L << bitPos))) {
        return false;
      }
    }

    return true;
  }

  public boolean testString(String val) {
    return testBytes(val.getBytes());
  }

  public boolean testByte(byte val) {
    return testBytes(new byte[]{val});
  }

  public boolean testInt(int val) {
    return testHash(Murmur3.hash64(val));
  }

  public boolean testLong(long val) {
    return testHash(Murmur3.hash64(val));
  }

  public boolean testFloat(float val) {
    return testInt(Float.floatToIntBits(val));
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
    return k;
  }

  public long getNumBits() {
    return m;
  }

  public long[] getBitSet() {
    return bitSet.getData();
  }

  @Override
  public String toString() {
    return "m: " + m + " k: " + k;
  }

  /**
   * Merge the specified bloom filter with current bloom filter.
   *
   * @param that - bloom filter to merge
   */
  public void merge(BloomKFilter that) {
    if (this != that && this.m == that.m && this.k == that.k) {
      this.bitSet.putAll(that.bitSet);
    } else {
      throw new IllegalArgumentException("BloomKFilters are not compatible for merging." +
        " this - " + this.toString() + " that - " + that.toString());
    }
  }

  public void reset() {
    this.bitSet.clear();
  }

  /**
   * Serialize a bloom filter:
   * Serialized BloomKFilter format:
   * 1 byte for the number of hash functions.
   * 1 big endian int(That is how OutputStream works) for the number of longs in the bitset
   * big endian longs in the BloomKFilter bitset
   *
   * @param out         output stream to write to
   * @param bloomFilter BloomKFilter that needs to be serialized
   */
  public static void serialize(OutputStream out, BloomKFilter bloomFilter) throws IOException {
    DataOutputStream dataOutputStream = new DataOutputStream(out);
    dataOutputStream.writeByte(bloomFilter.k);
    dataOutputStream.writeInt(bloomFilter.getBitSet().length);
    for (long value : bloomFilter.getBitSet()) {
      dataOutputStream.writeLong(value);
    }
  }

  /**
   * Deserialize a bloom filter
   * Read a byte stream, which was written by {@linkplain #serialize(OutputStream, BloomKFilter)}
   * into a {@code BloomKFilter}
   *
   * @param in input bytestream
   * @return deserialized BloomKFilter
   */
  public static BloomKFilter deserialize(InputStream in) throws IOException {
    if (in == null) {
      throw new IOException("Input stream is null");
    }

    try {
      DataInputStream dataInputStream = new DataInputStream(in);
      int numHashFunc = dataInputStream.readByte();
      int bitsetArrayLen = dataInputStream.readInt();
      long[] data = new long[bitsetArrayLen];
      for (int i = 0; i < bitsetArrayLen; i++) {
        data[i] = dataInputStream.readLong();
      }
      return new BloomKFilter(data, numHashFunc);
    } catch (RuntimeException e) {
      throw new IOException("Unable to deserialize BloomKFilter", e);
    }
  }

  // Given a byte array consisting of a serialized BloomKFilter, gives the offset (from 0)
  // for the start of the serialized long values that make up the bitset.
  // NumHashFunctions (1 byte) + bitset array length (4 bytes)
  public static final int START_OF_SERIALIZED_LONGS = 5;

  /**
   * Merges BloomKFilter bf2 into bf1.
   * Assumes 2 BloomKFilters with the same size/hash functions are serialized to byte arrays
   *
   * @param bf1Bytes Data of bloom filter 1.
   * @param bf1Start Start index of BF1.
   * @param bf1Length BF1 length.
   * @param bf2Bytes Data of bloom filter 1
   * @param bf2Start Start index of BF2.
   * @param bf2Length BF2 length.
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
  @SuppressWarnings("unused") public static class BitSet {
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
    public int bitSize() {
      return data.length * Long.SIZE;
    }

    public long[] getData() {
      return data;
    }

    /**
     * Combines the two BitArrays using bitwise OR.
     */
    public void putAll(BloomKFilter.BitSet array) {
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
