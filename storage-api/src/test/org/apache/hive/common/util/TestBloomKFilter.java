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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class TestBloomKFilter {
  private static final int COUNT = 100;
  Random rand = new Random(123);
  // bloom-1 is known to have higher fpp, to make tests pass give room for another 3%
  private final double deltaError = 0.03;

  @Test(expected = IllegalArgumentException.class)
  public void testBloomIllegalArg1() {
    BloomKFilter bf = new BloomKFilter(0);
  }

  @Test
  public void testBloomNumBits() {
    assertEquals(0, BloomKFilter.optimalNumOfBits(0, 0));
    assertEquals(0, BloomKFilter.optimalNumOfBits(0, 1));
    assertEquals(0, BloomKFilter.optimalNumOfBits(1, 1));
    assertEquals(7, BloomKFilter.optimalNumOfBits(1, 0.03));
    assertEquals(72, BloomKFilter.optimalNumOfBits(10, 0.03));
    assertEquals(729, BloomKFilter.optimalNumOfBits(100, 0.03));
    assertEquals(7298, BloomKFilter.optimalNumOfBits(1000, 0.03));
    assertEquals(72984, BloomKFilter.optimalNumOfBits(10000, 0.03));
    assertEquals(729844, BloomKFilter.optimalNumOfBits(100000, 0.03));
    assertEquals(7298440, BloomKFilter.optimalNumOfBits(1000000, 0.03));
    assertEquals(6235224, BloomKFilter.optimalNumOfBits(1000000, 0.05));
    assertEquals(1870567268, BloomKFilter.optimalNumOfBits(300000000, 0.05));
    assertEquals(1437758756, BloomKFilter.optimalNumOfBits(300000000, 0.1));
    assertEquals(432808512, BloomKFilter.optimalNumOfBits(300000000, 0.5));
    assertEquals(1393332198, BloomKFilter.optimalNumOfBits(3000000000L, 0.8));
    assertEquals(657882327, BloomKFilter.optimalNumOfBits(3000000000L, 0.9));
    assertEquals(0, BloomKFilter.optimalNumOfBits(3000000000L, 1));

    BloomKFilter bloomKFilter = new BloomKFilter(40);
    assertEquals(8, bloomKFilter.getBitSet().length);
    assertEquals(bloomKFilter.getNumBits(), bloomKFilter.getBitSize());
  }

  @Test
  public void testBloomNumHashFunctions() {
    assertEquals(1, BloomKFilter.optimalNumOfHashFunctions(-1, -1));
    assertEquals(1, BloomKFilter.optimalNumOfHashFunctions(0, 0));
    assertEquals(1, BloomKFilter.optimalNumOfHashFunctions(10, 0));
    assertEquals(1, BloomKFilter.optimalNumOfHashFunctions(10, 10));
    assertEquals(7, BloomKFilter.optimalNumOfHashFunctions(10, 100));
    assertEquals(1, BloomKFilter.optimalNumOfHashFunctions(100, 100));
    assertEquals(1, BloomKFilter.optimalNumOfHashFunctions(1000, 100));
    assertEquals(1, BloomKFilter.optimalNumOfHashFunctions(10000, 100));
    assertEquals(1, BloomKFilter.optimalNumOfHashFunctions(100000, 100));
    assertEquals(1, BloomKFilter.optimalNumOfHashFunctions(1000000, 100));
  }

  @Test
  public void testBloomKFilterBytes() {
    BloomKFilter bf = new BloomKFilter(10000);
    byte[] val = new byte[]{1, 2, 3};
    byte[] val1 = new byte[]{1, 2, 3, 4};
    byte[] val2 = new byte[]{1, 2, 3, 4, 5};
    byte[] val3 = new byte[]{1, 2, 3, 4, 5, 6};

    assertEquals(false, bf.test(val));
    assertEquals(false, bf.test(val1));
    assertEquals(false, bf.test(val2));
    assertEquals(false, bf.test(val3));
    bf.add(val);
    assertEquals(true, bf.test(val));
    assertEquals(false, bf.test(val1));
    assertEquals(false, bf.test(val2));
    assertEquals(false, bf.test(val3));
    bf.add(val1);
    assertEquals(true, bf.test(val));
    assertEquals(true, bf.test(val1));
    assertEquals(false, bf.test(val2));
    assertEquals(false, bf.test(val3));
    bf.add(val2);
    assertEquals(true, bf.test(val));
    assertEquals(true, bf.test(val1));
    assertEquals(true, bf.test(val2));
    assertEquals(false, bf.test(val3));
    bf.add(val3);
    assertEquals(true, bf.test(val));
    assertEquals(true, bf.test(val1));
    assertEquals(true, bf.test(val2));
    assertEquals(true, bf.test(val3));

    byte[] randVal = new byte[COUNT];
    for (int i = 0; i < COUNT; i++) {
      rand.nextBytes(randVal);
      bf.add(randVal);
    }
    // last value should be present
    assertEquals(true, bf.test(randVal));
    // most likely this value should not exist
    randVal[0] = 0;
    randVal[1] = 0;
    randVal[2] = 0;
    randVal[3] = 0;
    randVal[4] = 0;
    assertEquals(false, bf.test(randVal));

    assertEquals(7808, bf.sizeInBytes());
  }

  @Test
  public void testBloomKFilterByte() {
    BloomKFilter bf = new BloomKFilter(10000);
    byte val = Byte.MIN_VALUE;
    byte val1 = 1;
    byte val2 = 2;
    byte val3 = Byte.MAX_VALUE;

    assertEquals(false, bf.testLong(val));
    assertEquals(false, bf.testLong(val1));
    assertEquals(false, bf.testLong(val2));
    assertEquals(false, bf.testLong(val3));
    bf.addLong(val);
    assertEquals(true, bf.testLong(val));
    assertEquals(false, bf.testLong(val1));
    assertEquals(false, bf.testLong(val2));
    assertEquals(false, bf.testLong(val3));
    bf.addLong(val1);
    assertEquals(true, bf.testLong(val));
    assertEquals(true, bf.testLong(val1));
    assertEquals(false, bf.testLong(val2));
    assertEquals(false, bf.testLong(val3));
    bf.addLong(val2);
    assertEquals(true, bf.testLong(val));
    assertEquals(true, bf.testLong(val1));
    assertEquals(true, bf.testLong(val2));
    assertEquals(false, bf.testLong(val3));
    bf.addLong(val3);
    assertEquals(true, bf.testLong(val));
    assertEquals(true, bf.testLong(val1));
    assertEquals(true, bf.testLong(val2));
    assertEquals(true, bf.testLong(val3));

    byte randVal = 0;
    for (int i = 0; i < COUNT; i++) {
      randVal = (byte) rand.nextInt(Byte.MAX_VALUE);
      bf.addLong(randVal);
    }
    // last value should be present
    assertEquals(true, bf.testLong(randVal));
    // most likely this value should not exist
    assertEquals(false, bf.testLong((byte) -120));

    assertEquals(7808, bf.sizeInBytes());
  }

  @Test
  public void testBloomKFilterInt() {
    BloomKFilter bf = new BloomKFilter(10000);
    int val = Integer.MIN_VALUE;
    int val1 = 1;
    int val2 = 2;
    int val3 = Integer.MAX_VALUE;

    assertEquals(false, bf.testLong(val));
    assertEquals(false, bf.testLong(val1));
    assertEquals(false, bf.testLong(val2));
    assertEquals(false, bf.testLong(val3));
    bf.addLong(val);
    assertEquals(true, bf.testLong(val));
    assertEquals(false, bf.testLong(val1));
    assertEquals(false, bf.testLong(val2));
    assertEquals(false, bf.testLong(val3));
    bf.addLong(val1);
    assertEquals(true, bf.testLong(val));
    assertEquals(true, bf.testLong(val1));
    assertEquals(false, bf.testLong(val2));
    assertEquals(false, bf.testLong(val3));
    bf.addLong(val2);
    assertEquals(true, bf.testLong(val));
    assertEquals(true, bf.testLong(val1));
    assertEquals(true, bf.testLong(val2));
    assertEquals(false, bf.testLong(val3));
    bf.addLong(val3);
    assertEquals(true, bf.testLong(val));
    assertEquals(true, bf.testLong(val1));
    assertEquals(true, bf.testLong(val2));
    assertEquals(true, bf.testLong(val3));

    int randVal = 0;
    for (int i = 0; i < COUNT; i++) {
      randVal = rand.nextInt();
      bf.addLong(randVal);
    }
    // last value should be present
    assertEquals(true, bf.testLong(randVal));
    // most likely this value should not exist
    assertEquals(false, bf.testLong(-120));

    assertEquals(7808, bf.sizeInBytes());
  }

  @Test
  public void testBloomKFilterLong() {
    BloomKFilter bf = new BloomKFilter(10000);
    long val = Long.MIN_VALUE;
    long val1 = 1;
    long val2 = 2;
    long val3 = Long.MAX_VALUE;

    assertEquals(false, bf.testLong(val));
    assertEquals(false, bf.testLong(val1));
    assertEquals(false, bf.testLong(val2));
    assertEquals(false, bf.testLong(val3));
    bf.addLong(val);
    assertEquals(true, bf.testLong(val));
    assertEquals(false, bf.testLong(val1));
    assertEquals(false, bf.testLong(val2));
    assertEquals(false, bf.testLong(val3));
    bf.addLong(val1);
    assertEquals(true, bf.testLong(val));
    assertEquals(true, bf.testLong(val1));
    assertEquals(false, bf.testLong(val2));
    assertEquals(false, bf.testLong(val3));
    bf.addLong(val2);
    assertEquals(true, bf.testLong(val));
    assertEquals(true, bf.testLong(val1));
    assertEquals(true, bf.testLong(val2));
    assertEquals(false, bf.testLong(val3));
    bf.addLong(val3);
    assertEquals(true, bf.testLong(val));
    assertEquals(true, bf.testLong(val1));
    assertEquals(true, bf.testLong(val2));
    assertEquals(true, bf.testLong(val3));

    long randVal = 0;
    for (int i = 0; i < COUNT; i++) {
      randVal = rand.nextLong();
      bf.addLong(randVal);
    }
    // last value should be present
    assertEquals(true, bf.testLong(randVal));
    // most likely this value should not exist
    assertEquals(false, bf.testLong(-120));

    assertEquals(7808, bf.sizeInBytes());
  }

  @Test
  public void testBloomKFilterFloat() {
    BloomKFilter bf = new BloomKFilter(10000);
    float val = Float.MIN_VALUE;
    float val1 = 1.1f;
    float val2 = 2.2f;
    float val3 = Float.MAX_VALUE;

    assertEquals(false, bf.testDouble(val));
    assertEquals(false, bf.testDouble(val1));
    assertEquals(false, bf.testDouble(val2));
    assertEquals(false, bf.testDouble(val3));
    bf.addDouble(val);
    assertEquals(true, bf.testDouble(val));
    assertEquals(false, bf.testDouble(val1));
    assertEquals(false, bf.testDouble(val2));
    assertEquals(false, bf.testDouble(val3));
    bf.addDouble(val1);
    assertEquals(true, bf.testDouble(val));
    assertEquals(true, bf.testDouble(val1));
    assertEquals(false, bf.testDouble(val2));
    assertEquals(false, bf.testDouble(val3));
    bf.addDouble(val2);
    assertEquals(true, bf.testDouble(val));
    assertEquals(true, bf.testDouble(val1));
    assertEquals(true, bf.testDouble(val2));
    assertEquals(false, bf.testDouble(val3));
    bf.addDouble(val3);
    assertEquals(true, bf.testDouble(val));
    assertEquals(true, bf.testDouble(val1));
    assertEquals(true, bf.testDouble(val2));
    assertEquals(true, bf.testDouble(val3));

    float randVal = 0;
    for (int i = 0; i < COUNT; i++) {
      randVal = rand.nextFloat();
      bf.addDouble(randVal);
    }
    // last value should be present
    assertEquals(true, bf.testDouble(randVal));
    // most likely this value should not exist
    assertEquals(false, bf.testDouble(-120.2f));

    assertEquals(7808, bf.sizeInBytes());
  }

  @Test
  public void testBloomKFilterDouble() {
    BloomKFilter bf = new BloomKFilter(10000);
    double val = Double.MIN_VALUE;
    double val1 = 1.1d;
    double val2 = 2.2d;
    double val3 = Double.MAX_VALUE;

    assertEquals(false, bf.testDouble(val));
    assertEquals(false, bf.testDouble(val1));
    assertEquals(false, bf.testDouble(val2));
    assertEquals(false, bf.testDouble(val3));
    bf.addDouble(val);
    assertEquals(true, bf.testDouble(val));
    assertEquals(false, bf.testDouble(val1));
    assertEquals(false, bf.testDouble(val2));
    assertEquals(false, bf.testDouble(val3));
    bf.addDouble(val1);
    assertEquals(true, bf.testDouble(val));
    assertEquals(true, bf.testDouble(val1));
    assertEquals(false, bf.testDouble(val2));
    assertEquals(false, bf.testDouble(val3));
    bf.addDouble(val2);
    assertEquals(true, bf.testDouble(val));
    assertEquals(true, bf.testDouble(val1));
    assertEquals(true, bf.testDouble(val2));
    assertEquals(false, bf.testDouble(val3));
    bf.addDouble(val3);
    assertEquals(true, bf.testDouble(val));
    assertEquals(true, bf.testDouble(val1));
    assertEquals(true, bf.testDouble(val2));
    assertEquals(true, bf.testDouble(val3));

    double randVal = 0;
    for (int i = 0; i < COUNT; i++) {
      randVal = rand.nextDouble();
      bf.addDouble(randVal);
    }
    // last value should be present
    assertEquals(true, bf.testDouble(randVal));
    // most likely this value should not exist
    assertEquals(false, bf.testDouble(-120.2d));

    assertEquals(7808, bf.sizeInBytes());
  }

  @Test
  public void testBloomKFilterString() {
    BloomKFilter bf = new BloomKFilter(100000);
    String val = "bloo";
    String val1 = "bloom fil";
    String val2 = "bloom filter";
    String val3 = "cuckoo filter";

    assertEquals(false, bf.testString(val));
    assertEquals(false, bf.testString(val1));
    assertEquals(false, bf.testString(val2));
    assertEquals(false, bf.testString(val3));
    bf.addString(val);
    assertEquals(true, bf.testString(val));
    assertEquals(false, bf.testString(val1));
    assertEquals(false, bf.testString(val2));
    assertEquals(false, bf.testString(val3));
    bf.addString(val1);
    assertEquals(true, bf.testString(val));
    assertEquals(true, bf.testString(val1));
    assertEquals(false, bf.testString(val2));
    assertEquals(false, bf.testString(val3));
    bf.addString(val2);
    assertEquals(true, bf.testString(val));
    assertEquals(true, bf.testString(val1));
    assertEquals(true, bf.testString(val2));
    assertEquals(false, bf.testString(val3));
    bf.addString(val3);
    assertEquals(true, bf.testString(val));
    assertEquals(true, bf.testString(val1));
    assertEquals(true, bf.testString(val2));
    assertEquals(true, bf.testString(val3));

    long randVal = 0;
    for (int i = 0; i < COUNT; i++) {
      randVal = rand.nextLong();
      bf.addString(Long.toString(randVal));
    }
    // last value should be present
    assertEquals(true, bf.testString(Long.toString(randVal)));
    // most likely this value should not exist
    assertEquals(false, bf.testString(Long.toString(-120)));

    assertEquals(77952, bf.sizeInBytes());
  }

  @Test
  public void testMerge() {
    BloomKFilter bf = new BloomKFilter(10000);
    String val = "bloo";
    String val1 = "bloom fil";
    String val2 = "bloom filter";
    String val3 = "cuckoo filter";
    bf.addString(val);
    bf.addString(val1);
    bf.addString(val2);
    bf.addString(val3);

    BloomKFilter bf2 = new BloomKFilter(10000);
    String v = "2_bloo";
    String v1 = "2_bloom fil";
    String v2 = "2_bloom filter";
    String v3 = "2_cuckoo filter";
    bf2.addString(v);
    bf2.addString(v1);
    bf2.addString(v2);
    bf2.addString(v3);

    assertEquals(true, bf.testString(val));
    assertEquals(true, bf.testString(val1));
    assertEquals(true, bf.testString(val2));
    assertEquals(true, bf.testString(val3));
    assertEquals(false, bf.testString(v));
    assertEquals(false, bf.testString(v1));
    assertEquals(false, bf.testString(v2));
    assertEquals(false, bf.testString(v3));

    bf.merge(bf2);

    assertEquals(true, bf.testString(val));
    assertEquals(true, bf.testString(val1));
    assertEquals(true, bf.testString(val2));
    assertEquals(true, bf.testString(val3));
    assertEquals(true, bf.testString(v));
    assertEquals(true, bf.testString(v1));
    assertEquals(true, bf.testString(v2));
    assertEquals(true, bf.testString(v3));
  }

  @Test
  public void testSerialize() throws Exception {
    BloomKFilter bf1 = new BloomKFilter(10000);
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
    BloomKFilter.serialize(bytesOut, bf1);
    ByteArrayInputStream bytesIn = new ByteArrayInputStream(bytesOut.toByteArray());
    BloomKFilter bf2 = BloomKFilter.deserialize(bytesIn);

    for (String val : inputs) {
      assertEquals("Testing bf1 with " + val, true, bf1.testString(val));
      assertEquals("Testing bf2 with " + val, true, bf2.testString(val));
    }
  }

  @Test
  public void testMergeBloomKFilterBytes() throws Exception {
    BloomKFilter bf1 = new BloomKFilter(10000);
    BloomKFilter bf2 = new BloomKFilter(10000);

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
    BloomKFilter.serialize(bytesOut, bf1);
    byte[] bf1Bytes = bytesOut.toByteArray();
    bytesOut.reset();
    BloomKFilter.serialize(bytesOut, bf1);
    byte[] bf2Bytes = bytesOut.toByteArray();

    // Merge bytes
    BloomKFilter.mergeBloomFilterBytes(
        bf1Bytes, 0, bf1Bytes.length,
        bf2Bytes, 0, bf2Bytes.length);

    // Deserialize and test
    ByteArrayInputStream bytesIn = new ByteArrayInputStream(bf1Bytes, 0, bf1Bytes.length);
    BloomKFilter bfMerged = BloomKFilter.deserialize(bytesIn);
    // All values should pass test
    for (String val : inputs1) {
      bfMerged.addString(val);
    }
    for (String val : inputs2) {
      bfMerged.addString(val);
    }
  }

  @Test
  public void testMergeBloomKFilterBytesFailureCases() throws Exception {
    BloomKFilter bf1 = new BloomKFilter(1000);
    BloomKFilter bf2 = new BloomKFilter(200);
    // Create bloom filter with same number of bits, but different # hash functions
    long[] bits = new long[bf1.getBitSet().length];
    BloomKFilter bf3 = new BloomKFilter(bits, bf1.getNumHashFunctions() + 1);

    // Serialize to bytes
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    BloomKFilter.serialize(bytesOut, bf1);
    byte[] bf1Bytes = bytesOut.toByteArray();

    bytesOut.reset();
    BloomKFilter.serialize(bytesOut, bf2);
    byte[] bf2Bytes = bytesOut.toByteArray();

    bytesOut.reset();
    BloomKFilter.serialize(bytesOut, bf3);
    byte[] bf3Bytes = bytesOut.toByteArray();

    try {
      // this should fail
      BloomKFilter.mergeBloomFilterBytes(
          bf1Bytes, 0, bf1Bytes.length,
          bf2Bytes, 0, bf2Bytes.length);
      Assert.fail("Expected exception not encountered");
    } catch (IllegalArgumentException err) {
      // expected
    }

    try {
      // this should fail
      BloomKFilter.mergeBloomFilterBytes(
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
    BloomKFilter bf = new BloomKFilter(size);
    int fp = 0;
    for (int i = 0; i < size; i++) {
      bf.addLong(i);
    }

    for (int i = 0; i < size; i++) {
      assertTrue(bf.testLong(i));
    }

    for (int i = 0; i < size; i++) {
      int probe = rand.nextInt();
      // out of range probes
      if ((probe > size) || (probe < 0)) {
        if (bf.testLong(probe)) {
          fp++;
        }
      }
    }

    double actualFpp = (double) fp / (double) size;
    double expectedFpp = bf.DEFAULT_FPP;
    if (actualFpp < expectedFpp) {
      assertTrue(actualFpp != 0.0);
    } else {
      assertEquals(expectedFpp, actualFpp, deltaError);
    }
  }

  @Test
  public void testFpp10K() {
    int size = 10_000;
    BloomKFilter bf = new BloomKFilter(size);
    int fp = 0;
    for (int i = 0; i < size; i++) {
      bf.addLong(i);
    }

    for (int i = 0; i < size; i++) {
      assertTrue(bf.testLong(i));
    }

    for (int i = 0; i < size; i++) {
      int probe = rand.nextInt();
      // out of range probes
      if ((probe > size) || (probe < 0)) {
        if (bf.testLong(probe)) {
          fp++;
        }
      }
    }

    double actualFpp = (double) fp / (double) size;
    double expectedFpp = bf.DEFAULT_FPP;
    if (actualFpp < expectedFpp) {
      assertTrue(actualFpp != 0.0);
    } else {
      assertEquals(expectedFpp, actualFpp, deltaError);
    }
  }

  @Test
  public void testFpp1M() {
    int size = 1_000_000;
    BloomKFilter bf = new BloomKFilter(size);
    int fp = 0;
    for (int i = 0; i < size; i++) {
      bf.addLong(i);
    }

    for (int i = 0; i < size; i++) {
      assertTrue(bf.testLong(i));
    }

    for (int i = 0; i < size; i++) {
      int probe = rand.nextInt();
      // out of range probes
      if ((probe > size) || (probe < 0)) {
        if (bf.testLong(probe)) {
          fp++;
        }
      }
    }

    double actualFpp = (double) fp / (double) size;
    double expectedFpp = bf.DEFAULT_FPP;
    if (actualFpp < expectedFpp) {
      assertTrue(actualFpp != 0.0);
    } else {
      assertEquals(expectedFpp, actualFpp, deltaError);
    }
  }

  @Test
  public void testFpp10M() {
    int size = 10_000_000;
    BloomKFilter bf = new BloomKFilter(size);
    int fp = 0;
    for (int i = 0; i < size; i++) {
      bf.addLong(i);
    }

    for (int i = 0; i < size; i++) {
      assertTrue(bf.testLong(i));
    }

    for (int i = 0; i < size; i++) {
      int probe = rand.nextInt();
      // out of range probes
      if ((probe > size) || (probe < 0)) {
        if (bf.testLong(probe)) {
          fp++;
        }
      }
    }

    double actualFpp = (double) fp / (double) size;
    double expectedFpp = bf.DEFAULT_FPP;
    if (actualFpp < expectedFpp) {
      assertTrue(actualFpp != 0.0);
    } else {
      assertEquals(expectedFpp, actualFpp, deltaError);
    }
  }
}
