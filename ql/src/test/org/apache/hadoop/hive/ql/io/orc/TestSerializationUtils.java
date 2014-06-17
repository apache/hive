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
package org.apache.hadoop.hive.ql.io.orc;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.math.BigInteger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestSerializationUtils {

  private InputStream fromBuffer(ByteArrayOutputStream buffer) {
    return new ByteArrayInputStream(buffer.toByteArray());
  }

  @Test
  public void testDoubles() throws Exception {
    double tolerance = 0.0000000000000001;
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    SerializationUtils utils = new SerializationUtils();
    utils.writeDouble(buffer, 1343822337.759);
    assertEquals(1343822337.759, utils.readDouble(fromBuffer(buffer)), tolerance);
    buffer = new ByteArrayOutputStream();
    utils.writeDouble(buffer, 0.8);
    double got = utils.readDouble(fromBuffer(buffer));
    assertEquals(0.8, got, tolerance);
  }

  @Test
  public void testBigIntegers() throws Exception {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    SerializationUtils.writeBigInteger(buffer, BigInteger.valueOf(0));
    assertArrayEquals(new byte[]{0}, buffer.toByteArray());
    assertEquals(0L,
        SerializationUtils.readBigInteger(fromBuffer(buffer)).longValue());
    buffer.reset();
    SerializationUtils.writeBigInteger(buffer, BigInteger.valueOf(1));
    assertArrayEquals(new byte[]{2}, buffer.toByteArray());
    assertEquals(1L,
        SerializationUtils.readBigInteger(fromBuffer(buffer)).longValue());
    buffer.reset();
    SerializationUtils.writeBigInteger(buffer, BigInteger.valueOf(-1));
    assertArrayEquals(new byte[]{1}, buffer.toByteArray());
    assertEquals(-1L,
        SerializationUtils.readBigInteger(fromBuffer(buffer)).longValue());
    buffer.reset();
    SerializationUtils.writeBigInteger(buffer, BigInteger.valueOf(50));
    assertArrayEquals(new byte[]{100}, buffer.toByteArray());
    assertEquals(50L,
        SerializationUtils.readBigInteger(fromBuffer(buffer)).longValue());
    buffer.reset();
    SerializationUtils.writeBigInteger(buffer, BigInteger.valueOf(-50));
    assertArrayEquals(new byte[]{99}, buffer.toByteArray());
    assertEquals(-50L,
        SerializationUtils.readBigInteger(fromBuffer(buffer)).longValue());
    for(int i=-8192; i < 8192; ++i) {
      buffer.reset();
        SerializationUtils.writeBigInteger(buffer, BigInteger.valueOf(i));
      assertEquals("compare length for " + i,
            i >= -64 && i < 64 ? 1 : 2, buffer.size());
      assertEquals("compare result for " + i,
          i, SerializationUtils.readBigInteger(fromBuffer(buffer)).intValue());
    }
    buffer.reset();
    SerializationUtils.writeBigInteger(buffer,
        new BigInteger("123456789abcdef0",16));
    assertEquals(new BigInteger("123456789abcdef0",16),
        SerializationUtils.readBigInteger(fromBuffer(buffer)));
    buffer.reset();
    SerializationUtils.writeBigInteger(buffer,
        new BigInteger("-123456789abcdef0",16));
    assertEquals(new BigInteger("-123456789abcdef0",16),
        SerializationUtils.readBigInteger(fromBuffer(buffer)));
    StringBuilder buf = new StringBuilder();
    for(int i=0; i < 256; ++i) {
      String num = Integer.toHexString(i);
      if (num.length() == 1) {
        buf.append('0');
      }
      buf.append(num);
    }
    buffer.reset();
    SerializationUtils.writeBigInteger(buffer,
        new BigInteger(buf.toString(),16));
    assertEquals(new BigInteger(buf.toString(),16),
        SerializationUtils.readBigInteger(fromBuffer(buffer)));
    buffer.reset();
    SerializationUtils.writeBigInteger(buffer,
        new BigInteger("ff000000000000000000000000000000000000000000ff",16));
    assertEquals(
        new BigInteger("ff000000000000000000000000000000000000000000ff",16),
        SerializationUtils.readBigInteger(fromBuffer(buffer)));
  }

  public static void main(String[] args) throws Exception {
    TestSerializationUtils test = new TestSerializationUtils();
    test.testDoubles();
    test.testBigIntegers();
  }
}
