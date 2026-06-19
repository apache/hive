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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import static org.junit.Assert.*;

import java.util.Random;

import org.junit.Test;

public class TestCuckooSet {

  // maximum table size
  private static int MAX_SIZE = 65437;

  @Test
  public void testSetLong () {

    // Set of values to look for. Include the original blank value Long.MIN_VALUE to make sure
    // the process of choosing a new blank works.
    Long[] values = {1L, 2L, 3L, 1000L, 2000L, 3000L, 8L, 8L, 9L, 13L, 17L,
        22L, 23L, 24L, 25L, -26L, 27L,
        28L, 29L, 30L, 111111111111111L, -444444444444444L, Long.MIN_VALUE};
    Long[] negatives = {0L, 4L, 4000L, -2L, 19L, 222222222222222L, -333333333333333L};
    CuckooSetLong s = new CuckooSetLong(values.length);
    for(Long v : values) {
      s.insert(v);
    }

    // test that the values we added are there
    for(Long v : values) {
      assertTrue(s.lookup(v));
    }

    // test that values that we know are missing are shown to be absent
    for (Long v : negatives) {
      assertFalse(s.lookup(v));
    }

    // Set of values to look for.
    Long[] values2 = {1L, 2L, 3L, 1000L, 2000L, 3000L, 8L, 8L, 9L, 13L, 17L, 22L, 23L,
        24L, 25L, -26L, 27L,
        28L, 29L, 30L, 111111111111111L, -444444444444444L};

    // Include the original blank value Long.MIN_VALUE in the negatives to make sure we get
    // the correct result that the blank value is not there.
    Long[] negatives2 = {0L, 4L, 4000L, -2L, 19L, 222222222222222L,
        -333333333333333L, Long.MIN_VALUE};
    s = new CuckooSetLong(values2.length);
    for(Long v : values2) {
      s.insert(v);
    }

    // test that the values we added are there
    for(Long v : values2) {
      assertTrue(s.lookup(v));
    }

    // test that values that we know are missing are shown to be absent
    for (Long v : negatives2) {
      assertFalse(s.lookup(v));
    }

  }

  // load multiple random sets of Long values
  @Test
  public void testSetLongRandom() {
    long[] values;
    Random gen = new Random(98763537);
    for(int i = 0; i < 200;) {

      // Make a random array of longs
      int size = gen.nextInt() % MAX_SIZE;
      if (size <= 0) {   // ensure size is >= 1, otherwise try again
        continue;
      }
      i++;
      values = new long[size];
      loadRandom(values, gen);

      // load them into a SetLong
      CuckooSetLong s = new CuckooSetLong(size);
      loadSet(s, values);

      // look them up to make sure they are all there
      for (int j = 0; j != size; j++) {
        assertTrue(s.lookup(values[j]));
      }
    }
  }

  @Test
  public void testSetDouble() {

    // Set of values to look for.
    Double[] values = {7021.0D, 5780.0D, 0D, -1D, 1.999e50D};
    Double[] negatives = {7000.0D, -2D, 1.9999e50D};
    CuckooSetDouble s = new CuckooSetDouble(values.length);
    for(Double v : values) {
      s.insert(v);
    }

    // test that the values we added are there
    for(Double v : values) {
      assertTrue(s.lookup(v));
    }

    // test that values that we know are missing are shown to be absent
    for (Double v : negatives) {
      assertFalse(s.lookup(v));
    }
  }

  @Test
  public void testSetBytes() {
    String[] strings = {"foo", "bar", "baz", "a", "", "x1341", "Z"};
    String[] negativeStrings = {"not", "in", "the", "set", "foobar"};
    byte[][] values = getByteArrays(strings);
    byte[][] negatives = getByteArrays(negativeStrings);

    // load set
    CuckooSetBytes s = new CuckooSetBytes(strings.length);
    for(byte[] v : values) {
      s.insert(v);
    }

    // test that the values we added are there
    for(byte[] v : values) {
      assertTrue(s.lookup(v, 0, v.length));
    }

    // test that values that we know are missing are shown to be absent
    for (byte[] v : negatives) {
      assertFalse(s.lookup(v, 0, v.length));
    }

    // Test that we can search correctly using a buffer and pulling
    // a sequence of bytes out of the middle of it. In this case it
    // is the 3 letter sequence "foo".
    byte[] buf = getUTF8Bytes("thewordfooisinhere");
    assertTrue(s.lookup(buf, 7, 3));
  }

  @Test
  public void testSetBytesLargeRandom() {
    byte[][] values;
    Random gen = new Random(98763537);
    for(int i = 0; i < 200;) {

      // Make a random array of byte arrays
      int size = gen.nextInt() % MAX_SIZE;
      if (size <= 0) {   // ensure size is >= 1, otherwise try again
        continue;
      }
      i++;
      values = new byte[size][];
      loadRandomBytes(values, gen);

      // load them into a set
      CuckooSetBytes s = new CuckooSetBytes(size);
      loadSet(s, values);

      // look them up to make sure they are all there
      for (int j = 0; j != size; j++) {
        assertTrue(s.lookup(values[j], 0, values[j].length));
      }
    }
  }

  public void loadRandomBytes(byte[][] values, Random gen) {
    for (int i = 0; i != values.length; i++) {
      values[i] = getUTF8Bytes(Integer.toString(gen.nextInt()));
    }
  }

  private byte[] getUTF8Bytes(String s) {
    byte[] v = null;
    try {
      v = s.getBytes("UTF-8");
    } catch (Exception e) {
      ; // won't happen
    }
    return v;
  }

  // Get an array of UTF-8 byte arrays from an array of strings
  private byte[][] getByteArrays(String[] strings) {
    byte[][] values = new byte[strings.length][];
    for(int i = 0; i != strings.length; i++) {
      try {
        values[i] = strings[i].getBytes("UTF-8");
      } catch (Exception e) {
        ; // can't happen
      }
    }
    return values;
  }

  private void loadSet(CuckooSetLong s, long[] values) {
    for (Long v : values) {
      s.insert(v);
    }
  }

  private void loadSet(CuckooSetBytes s, byte[][] values) {
    for (byte[] v: values) {
      s.insert(v);
    }
  }

  private void loadRandom(long[] a, Random gen)  {
    int size = a.length;
    for(int i = 0; i != size; i++) {
      a[i] = gen.nextLong();
    }
  }
}
