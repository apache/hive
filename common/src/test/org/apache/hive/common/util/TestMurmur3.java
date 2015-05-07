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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.common.util;

import static org.junit.Assert.assertEquals;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

/**
 * Tests for Murmur3 variants.
 */
public class TestMurmur3 {

  @Test
  public void testHashCodesM3_32_string() {
    String key = "test";
    int seed = 123;
    HashFunction hf = Hashing.murmur3_32(seed);
    int hc1 = hf.hashBytes(key.getBytes()).asInt();
    int hc2 = Murmur3.hash32(key.getBytes(), key.getBytes().length, seed);
    assertEquals(hc1, hc2);

    key = "testkey";
    hc1 = hf.hashBytes(key.getBytes()).asInt();
    hc2 = Murmur3.hash32(key.getBytes(), key.getBytes().length, seed);
    assertEquals(hc1, hc2);
  }

  @Test
  public void testHashCodesM3_32_ints() {
    int seed = 123;
    Random rand = new Random(seed);
    HashFunction hf = Hashing.murmur3_32(seed);
    for (int i = 0; i < 1000; i++) {
      int val = rand.nextInt();
      byte[] data = ByteBuffer.allocate(4).putInt(val).array();
      int hc1 = hf.hashBytes(data).asInt();
      int hc2 = Murmur3.hash32(data, data.length, seed);
      assertEquals(hc1, hc2);
    }
  }

  @Test
  public void testHashCodesM3_32_longs() {
    int seed = 123;
    Random rand = new Random(seed);
    HashFunction hf = Hashing.murmur3_32(seed);
    for (int i = 0; i < 1000; i++) {
      long val = rand.nextLong();
      byte[] data = ByteBuffer.allocate(8).putLong(val).array();
      int hc1 = hf.hashBytes(data).asInt();
      int hc2 = Murmur3.hash32(data, data.length, seed);
      assertEquals(hc1, hc2);
    }
  }

  @Test
  public void testHashCodesM3_32_double() {
    int seed = 123;
    Random rand = new Random(seed);
    HashFunction hf = Hashing.murmur3_32(seed);
    for (int i = 0; i < 1000; i++) {
      double val = rand.nextDouble();
      byte[] data = ByteBuffer.allocate(8).putDouble(val).array();
      int hc1 = hf.hashBytes(data).asInt();
      int hc2 = Murmur3.hash32(data, data.length, seed);
      assertEquals(hc1, hc2);
    }
  }

  @Test
  public void testHashCodesM3_128_string() {
    String key = "test";
    int seed = 123;
    HashFunction hf = Hashing.murmur3_128(seed);
    // guava stores the hashcodes in little endian order
    ByteBuffer buf = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
    buf.put(hf.hashBytes(key.getBytes()).asBytes());
    buf.flip();
    long gl1 = buf.getLong();
    long gl2 = buf.getLong(8);
    long[] hc = Murmur3.hash128(key.getBytes(), key.getBytes().length, seed);
    long m1 = hc[0];
    long m2 = hc[1];
    assertEquals(gl1, m1);
    assertEquals(gl2, m2);

    key = "testkey128_testkey128";
    buf = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
    buf.put(hf.hashBytes(key.getBytes()).asBytes());
    buf.flip();
    gl1 = buf.getLong();
    gl2 = buf.getLong(8);
    hc = Murmur3.hash128(key.getBytes(), key.getBytes().length, seed);
    m1 = hc[0];
    m2 = hc[1];
    assertEquals(gl1, m1);
    assertEquals(gl2, m2);
  }

  @Test
  public void testHashCodesM3_128_ints() {
    int seed = 123;
    Random rand = new Random(seed);
    HashFunction hf = Hashing.murmur3_128(seed);
    for (int i = 0; i < 1000; i++) {
      int val = rand.nextInt();
      byte[] data = ByteBuffer.allocate(4).putInt(val).array();
      // guava stores the hashcodes in little endian order
      ByteBuffer buf = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
      buf.put(hf.hashBytes(data).asBytes());
      buf.flip();
      long gl1 = buf.getLong();
      long gl2 = buf.getLong(8);
      long[] hc = Murmur3.hash128(data, data.length, seed);
      long m1 = hc[0];
      long m2 = hc[1];
      assertEquals(gl1, m1);
      assertEquals(gl2, m2);
    }
  }

  @Test
  public void testHashCodesM3_128_longs() {
    int seed = 123;
    Random rand = new Random(seed);
    HashFunction hf = Hashing.murmur3_128(seed);
    for (int i = 0; i < 1000; i++) {
      long val = rand.nextLong();
      byte[] data = ByteBuffer.allocate(8).putLong(val).array();
      // guava stores the hashcodes in little endian order
      ByteBuffer buf = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
      buf.put(hf.hashBytes(data).asBytes());
      buf.flip();
      long gl1 = buf.getLong();
      long gl2 = buf.getLong(8);
      long[] hc = Murmur3.hash128(data, data.length, seed);
      long m1 = hc[0];
      long m2 = hc[1];
      assertEquals(gl1, m1);
      assertEquals(gl2, m2);
    }
  }

  @Test
  public void testHashCodesM3_128_double() {
    int seed = 123;
    Random rand = new Random(seed);
    HashFunction hf = Hashing.murmur3_128(seed);
    for (int i = 0; i < 1000; i++) {
      double val = rand.nextDouble();
      byte[] data = ByteBuffer.allocate(8).putDouble(val).array();
      // guava stores the hashcodes in little endian order
      ByteBuffer buf = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
      buf.put(hf.hashBytes(data).asBytes());
      buf.flip();
      long gl1 = buf.getLong();
      long gl2 = buf.getLong(8);
      long[] hc = Murmur3.hash128(data, data.length, seed);
      long m1 = hc[0];
      long m2 = hc[1];
      assertEquals(gl1, m1);
      assertEquals(gl2, m2);
    }
  }
}
