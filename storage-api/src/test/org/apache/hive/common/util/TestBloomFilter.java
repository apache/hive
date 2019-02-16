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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class TestBloomFilter {
  private static final int COUNT = 100;
  Random rand = new Random(123);

  @Test(expected = IllegalArgumentException.class)
  public void testBloomIllegalArg1() {
    BloomFilter bf = new BloomFilter(0, 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBloomIllegalArg2() {
    BloomFilter bf = new BloomFilter(0, 0.1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBloomIllegalArg3() {
    BloomFilter bf = new BloomFilter(1, 0.0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBloomIllegalArg4() {
    BloomFilter bf = new BloomFilter(1, 1.0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBloomIllegalArg5() {
    BloomFilter bf = new BloomFilter(-1, -1);
  }


  @Test
  public void testBloomNumBits() {
    assertEquals(0, BloomFilter.optimalNumOfBits(0, 0));
    assertEquals(0, BloomFilter.optimalNumOfBits(0, 1));
    assertEquals(0, BloomFilter.optimalNumOfBits(1, 1));
    assertEquals(7, BloomFilter.optimalNumOfBits(1, 0.03));
    assertEquals(72, BloomFilter.optimalNumOfBits(10, 0.03));
    assertEquals(729, BloomFilter.optimalNumOfBits(100, 0.03));
    assertEquals(7298, BloomFilter.optimalNumOfBits(1000, 0.03));
    assertEquals(72984, BloomFilter.optimalNumOfBits(10000, 0.03));
    assertEquals(729844, BloomFilter.optimalNumOfBits(100000, 0.03));
    assertEquals(7298440, BloomFilter.optimalNumOfBits(1000000, 0.03));
    assertEquals(6235224, BloomFilter.optimalNumOfBits(1000000, 0.05));
    assertEquals(1870567268, BloomFilter.optimalNumOfBits(300000000, 0.05));
    assertEquals(1437758756, BloomFilter.optimalNumOfBits(300000000, 0.1));
    assertEquals(432808512, BloomFilter.optimalNumOfBits(300000000, 0.5));
    assertEquals(1393332198, BloomFilter.optimalNumOfBits(3000000000L, 0.8));
    assertEquals(657882327, BloomFilter.optimalNumOfBits(3000000000L, 0.9));
    assertEquals(0, BloomFilter.optimalNumOfBits(3000000000L, 1));
  }

  @Test
  public void testBloomNumHashFunctions() {
    assertEquals(1, BloomFilter.optimalNumOfHashFunctions(-1, -1));
    assertEquals(1, BloomFilter.optimalNumOfHashFunctions(0, 0));
    assertEquals(1, BloomFilter.optimalNumOfHashFunctions(10, 0));
    assertEquals(1, BloomFilter.optimalNumOfHashFunctions(10, 10));
    assertEquals(7, BloomFilter.optimalNumOfHashFunctions(10, 100));
    assertEquals(1, BloomFilter.optimalNumOfHashFunctions(100, 100));
    assertEquals(1, BloomFilter.optimalNumOfHashFunctions(1000, 100));
    assertEquals(1, BloomFilter.optimalNumOfHashFunctions(10000, 100));
    assertEquals(1, BloomFilter.optimalNumOfHashFunctions(100000, 100));
    assertEquals(1, BloomFilter.optimalNumOfHashFunctions(1000000, 100));
  }

  @Test
  public void testBloomFilterBytes() {
    BloomFilter bf = new BloomFilter(10000);
    byte[] val = new byte[]{1, 2, 3};
    byte[] val1 = new byte[]{1, 2, 3, 4};
    byte[] val2 = new byte[]{1, 2, 3, 4, 5};
    byte[] val3 = new byte[]{1, 2, 3, 4, 5, 6};

    assertFalse(bf.test(val));
    assertFalse( bf.test(val1));
    assertFalse( bf.test(val2));
    assertFalse( bf.test(val3));
    bf.add(val);
    assertTrue( bf.test(val));
    assertFalse( bf.test(val1));
    assertFalse( bf.test(val2));
    assertFalse( bf.test(val3));
    bf.add(val1);
    assertTrue( bf.test(val));
    assertTrue( bf.test(val1));
    assertFalse( bf.test(val2));
    assertFalse( bf.test(val3));
    bf.add(val2);
    assertTrue( bf.test(val));
    assertTrue( bf.test(val1));
    assertTrue( bf.test(val2));
    assertFalse( bf.test(val3));
    bf.add(val3);
    assertTrue( bf.test(val));
    assertTrue( bf.test(val1));
    assertTrue( bf.test(val2));
    assertTrue( bf.test(val3));

    byte[] randVal = new byte[COUNT];
    for (int i = 0; i < COUNT; i++) {
      rand.nextBytes(randVal);
      bf.add(randVal);
    }
    // last value should be present
    assertTrue( bf.test(randVal));
    // most likely this value should not exist
    randVal[0] = 0;
    randVal[1] = 0;
    randVal[2] = 0;
    randVal[3] = 0;
    randVal[4] = 0;
    assertFalse( bf.test(randVal));

    assertEquals(7800, bf.sizeInBytes());
  }

  @Test
  public void testBloomFilterByte() {
    BloomFilter bf = new BloomFilter(10000);
    byte val = Byte.MIN_VALUE;
    byte val1 = 1;
    byte val2 = 2;
    byte val3 = Byte.MAX_VALUE;

    assertFalse( bf.testLong(val));
    assertFalse( bf.testLong(val1));
    assertFalse( bf.testLong(val2));
    assertFalse( bf.testLong(val3));
    bf.addLong(val);
    assertTrue( bf.testLong(val));
    assertFalse( bf.testLong(val1));
    assertFalse( bf.testLong(val2));
    assertFalse( bf.testLong(val3));
    bf.addLong(val1);
    assertTrue( bf.testLong(val));
    assertTrue( bf.testLong(val1));
    assertFalse( bf.testLong(val2));
    assertFalse( bf.testLong(val3));
    bf.addLong(val2);
    assertTrue( bf.testLong(val));
    assertTrue( bf.testLong(val1));
    assertTrue( bf.testLong(val2));
    assertFalse( bf.testLong(val3));
    bf.addLong(val3);
    assertTrue( bf.testLong(val));
    assertTrue( bf.testLong(val1));
    assertTrue( bf.testLong(val2));
    assertTrue( bf.testLong(val3));

    byte randVal = 0;
    for (int i = 0; i < COUNT; i++) {
      randVal = (byte) rand.nextInt(Byte.MAX_VALUE);
      bf.addLong(randVal);
    }
    // last value should be present
    assertTrue( bf.testLong(randVal));
    // most likely this value should not exist
    assertFalse( bf.testLong((byte) -120));

    assertEquals(7800, bf.sizeInBytes());
  }

  @Test
  public void testBloomFilterInt() {
    BloomFilter bf = new BloomFilter(10000);
    int val = Integer.MIN_VALUE;
    int val1 = 1;
    int val2 = 2;
    int val3 = Integer.MAX_VALUE;

    assertFalse( bf.testLong(val));
    assertFalse( bf.testLong(val1));
    assertFalse( bf.testLong(val2));
    assertFalse( bf.testLong(val3));
    bf.addLong(val);
    assertTrue( bf.testLong(val));
    assertFalse( bf.testLong(val1));
    assertFalse( bf.testLong(val2));
    assertFalse( bf.testLong(val3));
    bf.addLong(val1);
    assertTrue( bf.testLong(val));
    assertTrue( bf.testLong(val1));
    assertFalse( bf.testLong(val2));
    assertFalse( bf.testLong(val3));
    bf.addLong(val2);
    assertTrue( bf.testLong(val));
    assertTrue( bf.testLong(val1));
    assertTrue( bf.testLong(val2));
    assertFalse( bf.testLong(val3));
    bf.addLong(val3);
    assertTrue( bf.testLong(val));
    assertTrue( bf.testLong(val1));
    assertTrue( bf.testLong(val2));
    assertTrue( bf.testLong(val3));

    int randVal = 0;
    for (int i = 0; i < COUNT; i++) {
      randVal = rand.nextInt();
      bf.addLong(randVal);
    }
    // last value should be present
    assertTrue( bf.testLong(randVal));
    // most likely this value should not exist
    assertFalse( bf.testLong(-120));

    assertEquals(7800, bf.sizeInBytes());
  }

  @Test
  public void testBloomFilterLong() {
    BloomFilter bf = new BloomFilter(10000);
    long val = Long.MIN_VALUE;
    long val1 = 1;
    long val2 = 2;
    long val3 = Long.MAX_VALUE;

    assertFalse( bf.testLong(val));
    assertFalse( bf.testLong(val1));
    assertFalse( bf.testLong(val2));
    assertFalse( bf.testLong(val3));
    bf.addLong(val);
    assertTrue( bf.testLong(val));
    assertFalse( bf.testLong(val1));
    assertFalse( bf.testLong(val2));
    assertFalse( bf.testLong(val3));
    bf.addLong(val1);
    assertTrue( bf.testLong(val));
    assertTrue( bf.testLong(val1));
    assertFalse( bf.testLong(val2));
    assertFalse( bf.testLong(val3));
    bf.addLong(val2);
    assertTrue( bf.testLong(val));
    assertTrue( bf.testLong(val1));
    assertTrue( bf.testLong(val2));
    assertFalse( bf.testLong(val3));
    bf.addLong(val3);
    assertTrue( bf.testLong(val));
    assertTrue( bf.testLong(val1));
    assertTrue( bf.testLong(val2));
    assertTrue( bf.testLong(val3));

    long randVal = 0;
    for (int i = 0; i < COUNT; i++) {
      randVal = rand.nextLong();
      bf.addLong(randVal);
    }
    // last value should be present
    assertTrue( bf.testLong(randVal));
    // most likely this value should not exist
    assertFalse( bf.testLong(-120));

    assertEquals(7800, bf.sizeInBytes());
  }

  @Test
  public void testBloomFilterFloat() {
    BloomFilter bf = new BloomFilter(10000);
    float val = Float.MIN_VALUE;
    float val1 = 1.1f;
    float val2 = 2.2f;
    float val3 = Float.MAX_VALUE;

    assertFalse( bf.testDouble(val));
    assertFalse( bf.testDouble(val1));
    assertFalse( bf.testDouble(val2));
    assertFalse( bf.testDouble(val3));
    bf.addDouble(val);
    assertTrue( bf.testDouble(val));
    assertFalse( bf.testDouble(val1));
    assertFalse( bf.testDouble(val2));
    assertFalse( bf.testDouble(val3));
    bf.addDouble(val1);
    assertTrue( bf.testDouble(val));
    assertTrue( bf.testDouble(val1));
    assertFalse( bf.testDouble(val2));
    assertFalse( bf.testDouble(val3));
    bf.addDouble(val2);
    assertTrue( bf.testDouble(val));
    assertTrue( bf.testDouble(val1));
    assertTrue( bf.testDouble(val2));
    assertFalse( bf.testDouble(val3));
    bf.addDouble(val3);
    assertTrue( bf.testDouble(val));
    assertTrue( bf.testDouble(val1));
    assertTrue( bf.testDouble(val2));
    assertTrue( bf.testDouble(val3));

    float randVal = 0;
    for (int i = 0; i < COUNT; i++) {
      randVal = rand.nextFloat();
      bf.addDouble(randVal);
    }
    // last value should be present
    assertTrue( bf.testDouble(randVal));
    // most likely this value should not exist
    assertFalse( bf.testDouble(-120.2f));

    assertEquals(7800, bf.sizeInBytes());
  }

  @Test
  public void testBloomFilterDouble() {
    BloomFilter bf = new BloomFilter(10000);
    double val = Double.MIN_VALUE;
    double val1 = 1.1d;
    double val2 = 2.2d;
    double val3 = Double.MAX_VALUE;

    assertFalse( bf.testDouble(val));
    assertFalse( bf.testDouble(val1));
    assertFalse( bf.testDouble(val2));
    assertFalse( bf.testDouble(val3));
    bf.addDouble(val);
    assertTrue( bf.testDouble(val));
    assertFalse( bf.testDouble(val1));
    assertFalse( bf.testDouble(val2));
    assertFalse( bf.testDouble(val3));
    bf.addDouble(val1);
    assertTrue( bf.testDouble(val));
    assertTrue( bf.testDouble(val1));
    assertFalse( bf.testDouble(val2));
    assertFalse( bf.testDouble(val3));
    bf.addDouble(val2);
    assertTrue( bf.testDouble(val));
    assertTrue( bf.testDouble(val1));
    assertTrue( bf.testDouble(val2));
    assertFalse( bf.testDouble(val3));
    bf.addDouble(val3);
    assertTrue( bf.testDouble(val));
    assertTrue( bf.testDouble(val1));
    assertTrue( bf.testDouble(val2));
    assertTrue( bf.testDouble(val3));

    double randVal = 0;
    for (int i = 0; i < COUNT; i++) {
      randVal = rand.nextDouble();
      bf.addDouble(randVal);
    }
    // last value should be present
    assertTrue( bf.testDouble(randVal));
    // most likely this value should not exist
    assertFalse( bf.testDouble(-120.2d));

    assertEquals(7800, bf.sizeInBytes());
  }

  @Test
  public void testBloomFilterString() {
    BloomFilter bf = new BloomFilter(100000);
    String val = "bloo";
    String val1 = "bloom fil";
    String val2 = "bloom filter";
    String val3 = "cuckoo filter";

    assertFalse( bf.testString(val));
    assertFalse( bf.testString(val1));
    assertFalse( bf.testString(val2));
    assertFalse( bf.testString(val3));
    bf.addString(val);
    assertTrue( bf.testString(val));
    assertFalse( bf.testString(val1));
    assertFalse( bf.testString(val2));
    assertFalse( bf.testString(val3));
    bf.addString(val1);
    assertTrue( bf.testString(val));
    assertTrue( bf.testString(val1));
    assertFalse( bf.testString(val2));
    assertFalse( bf.testString(val3));
    bf.addString(val2);
    assertTrue( bf.testString(val));
    assertTrue( bf.testString(val1));
    assertTrue( bf.testString(val2));
    assertFalse( bf.testString(val3));
    bf.addString(val3);
    assertTrue( bf.testString(val));
    assertTrue( bf.testString(val1));
    assertTrue( bf.testString(val2));
    assertTrue( bf.testString(val3));

    long randVal = 0;
    for (int i = 0; i < COUNT; i++) {
      randVal = rand.nextLong();
      bf.addString(Long.toString(randVal));
    }
    // last value should be present
    assertTrue( bf.testString(Long.toString(randVal)));
    // most likely this value should not exist
    assertFalse( bf.testString(Long.toString(-120)));

    assertEquals(77944, bf.sizeInBytes());
  }

  @Test
  public void testMerge() {
    BloomFilter bf = new BloomFilter(10000);
    String val = "bloo";
    String val1 = "bloom fil";
    String val2 = "bloom filter";
    String val3 = "cuckoo filter";
    bf.addString(val);
    bf.addString(val1);
    bf.addString(val2);
    bf.addString(val3);

    BloomFilter bf2 = new BloomFilter(10000);
    String v = "2_bloo";
    String v1 = "2_bloom fil";
    String v2 = "2_bloom filter";
    String v3 = "2_cuckoo filter";
    bf2.addString(v);
    bf2.addString(v1);
    bf2.addString(v2);
    bf2.addString(v3);

    assertTrue( bf.testString(val));
    assertTrue( bf.testString(val1));
    assertTrue( bf.testString(val2));
    assertTrue( bf.testString(val3));
    assertFalse( bf.testString(v));
    assertFalse( bf.testString(v1));
    assertFalse( bf.testString(v2));
    assertFalse( bf.testString(v3));

    bf.merge(bf2);

    assertTrue( bf.testString(val));
    assertTrue( bf.testString(val1));
    assertTrue( bf.testString(val2));
    assertTrue( bf.testString(val3));
    assertTrue( bf.testString(v));
    assertTrue( bf.testString(v1));
    assertTrue( bf.testString(v2));
    assertTrue( bf.testString(v3));
  }

  @Test
  public void testSerialize() throws Exception {
    BloomFilter bf1 = new BloomFilter(10000);
    String[] inputs = {
      "bloo",
      "bloom fil",
      "bloom filter",
      "cuckoo filter",
    };

    for (String val : inputs) {
      bf1.addString(val);
    }

    // Serialize/deserialize
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    BloomFilter.serialize(bytesOut, bf1);
    ByteArrayInputStream bytesIn = new ByteArrayInputStream(bytesOut.toByteArray());
    BloomFilter bf2 = BloomFilter.deserialize(bytesIn);

    for (String val : inputs) {
      assertTrue("Testing bf1 with " + val, bf1.testString(val));
      assertTrue("Testing bf2 with " + val, bf2.testString(val));
    }
  }

  @Test
  public void testMergeBloomFilterBytes() throws Exception {
    BloomFilter bf1 = new BloomFilter(10000);
    BloomFilter bf2 = new BloomFilter(10000);

    String[] inputs1 = {
      "bloo",
      "bloom fil",
      "bloom filter",
      "cuckoo filter",
    };

    String[] inputs2 = {
      "2_bloo",
      "2_bloom fil",
      "2_bloom filter",
      "2_cuckoo filter",
    };

    for (String val : inputs1) {
      bf1.addString(val);
    }
    for (String val : inputs2) {
      bf2.addString(val);
    }

    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    BloomFilter.serialize(bytesOut, bf1);
    byte[] bf1Bytes = bytesOut.toByteArray();
    bytesOut.reset();
    BloomFilter.serialize(bytesOut, bf1);
    byte[] bf2Bytes = bytesOut.toByteArray();

    // Merge bytes
    BloomFilter.mergeBloomFilterBytes(
        bf1Bytes, 0, bf1Bytes.length,
        bf2Bytes, 0, bf2Bytes.length);

    // Deserialize and test
    ByteArrayInputStream bytesIn = new ByteArrayInputStream(bf1Bytes, 0, bf1Bytes.length);
    BloomFilter bfMerged = BloomFilter.deserialize(bytesIn);
    // All values should pass test
    for (String val : inputs1) {
      bfMerged.addString(val);
    }
    for (String val : inputs2) {
      bfMerged.addString(val);
    }
  }

  @Test
  public void testMergeBloomFilterBytesFailureCases() throws Exception {
    BloomFilter bf1 = new BloomFilter(1000);
    BloomFilter bf2 = new BloomFilter(200);
    // Create bloom filter with same number of bits, but different # hash functions
    long[] bits = new long[bf1.getBitSet().length];
    BloomFilter bf3 = new BloomFilter(bits, bf1.getNumHashFunctions() + 1);

    // Serialize to bytes
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    BloomFilter.serialize(bytesOut, bf1);
    byte[] bf1Bytes = bytesOut.toByteArray();

    bytesOut.reset();
    BloomFilter.serialize(bytesOut, bf2);
    byte[] bf2Bytes = bytesOut.toByteArray();

    bytesOut.reset();
    BloomFilter.serialize(bytesOut, bf3);
    byte[] bf3Bytes = bytesOut.toByteArray();

    try {
      // this should fail
      BloomFilter.mergeBloomFilterBytes(
          bf1Bytes, 0, bf1Bytes.length,
          bf2Bytes, 0, bf2Bytes.length);
      Assert.fail("Expected exception not encountered");
    } catch (IllegalArgumentException err) {
      // expected
    }

    try {
      // this should fail
      BloomFilter.mergeBloomFilterBytes(
          bf1Bytes, 0, bf1Bytes.length,
          bf3Bytes, 0, bf3Bytes.length);
      Assert.fail("Expected exception not encountered");
    } catch (IllegalArgumentException err) {
      // expected
    }
  }

  @Test
  public void testFpp1K() {
    int size = 1000;
    BloomFilter bf = new BloomFilter(size);
    int fp;
    for (int i = 0; i < size; i++) {
      bf.addLong(i);
    }

    for (int i = 0; i < size; i++) {
      assertTrue(bf.testLong(i));
    }

    fp = getFp(size, bf);

    double actualFpp = (double) fp / (double) size;
    double expectedFpp = BloomFilter.DEFAULT_FPP;
    if (actualFpp < expectedFpp) {
      assertTrue(actualFpp != 0.0);
    } else {
      assertEquals(expectedFpp, actualFpp, 0.005);
    }
  }

  @Test
  public void testFpp10K() {
    int size = 10_000;
    BloomFilter bf = new BloomFilter(size);
    int fp;
    for (int i = 0; i < size; i++) {
      bf.addLong(i);
    }

    for (int i = 0; i < size; i++) {
      assertTrue(bf.testLong(i));
    }

    fp = getFp(size, bf);

    double actualFpp = (double) fp / (double) size;
    double expectedFpp = BloomFilter.DEFAULT_FPP;
    if (actualFpp < expectedFpp) {
      assertTrue(actualFpp != 0.0);
    } else {
      assertEquals(expectedFpp, actualFpp, 0.005);
    }
  }

  @Test
  public void testFpp1M() {
    int size = 1_000_000;
    BloomFilter bf = new BloomFilter(size);
    int fp;
    for (int i = 0; i < size; i++) {
      bf.addLong(i);
    }

    for (int i = 0; i < size; i++) {
      assertTrue(bf.testLong(i));
    }

    fp = getFp(size, bf);

    double actualFpp = (double) fp / (double) size;
    double expectedFpp = BloomFilter.DEFAULT_FPP;
    if (actualFpp < expectedFpp) {
      assertTrue(actualFpp != 0.0);
    } else {
      assertEquals(expectedFpp, actualFpp, 0.005);
    }
  }

  @Test
  public void testFpp10M() {
    int size = 10_000_000;
    BloomFilter bf = new BloomFilter(size);
    int fp;
    for (int i = 0; i < size; i++) {
      bf.addLong(i);
    }

    for (int i = 0; i < size; i++) {
      assertTrue(bf.testLong(i));
    }

    fp = getFp(size, bf);

    double actualFpp = (double) fp / (double) size;
    double expectedFpp = BloomFilter.DEFAULT_FPP;
    if (actualFpp < expectedFpp) {
      assertTrue(actualFpp != 0.0);
    } else {
      assertEquals(expectedFpp, actualFpp, 0.005);
    }
  }

  @SuppressWarnings("Duplicates") private int getFp(int size, BloomFilter bf) {
    int fp = 0;
    for (int i = 0; i < size; i++) {
      int probe = rand.nextInt();
      // out of range probes
      if ((probe > size) || (probe < 0)) {
        if (bf.testLong(probe)) {
          fp++;
        }
      }
    }
    return fp;
  }
}
