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
package org.apache.hadoop.hive.serde2.lazy;


import junit.framework.TestCase;

public class TestLazyPrimitive extends TestCase {

  /**
   * Test the LazyByte class.
   */
  public void testLazyByte() throws Throwable {
    try {
      LazyByte b = new LazyByte();
      b.setAll(new byte[]{'0'}, 0, 1);
      assertEquals(Byte.valueOf((byte)0), b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '0'}, 0, 2);
      assertEquals(Byte.valueOf((byte)0), b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '0'}, 0, 2);
      assertEquals(Byte.valueOf((byte)0), b.getPrimitiveObject());
      b.setAll(new byte[]{'a', '1', 'b'}, 1, 1);
      assertEquals(Byte.valueOf((byte)1), b.getPrimitiveObject());
      b.setAll(new byte[]{'a', '-', '1'}, 1, 2);
      assertEquals(Byte.valueOf((byte)-1), b.getPrimitiveObject());
      b.setAll(new byte[]{'a', '+', '1'}, 1, 2);
      assertEquals(Byte.valueOf((byte)1), b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '1', '2', '8'}, 0, 4);
      assertEquals(Byte.valueOf((byte)-128), b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '1', '2', '7'}, 0, 4);
      assertEquals(Byte.valueOf((byte)127), b.getPrimitiveObject());
      
      b.setAll(new byte[]{'a', '1', 'b'}, 1, 2);
      assertNull(b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '1', '2', '8'}, 0, 4);
      assertNull(b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '1', '2', '9'}, 0, 4);
      assertNull(b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '1', '2', '3'}, 0, 1);
      assertNull(b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '1', '2', '3'}, 0, 1);
      assertNull(b.getPrimitiveObject());
     
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }
    
  /**
   * Test the LazyShort class.
   */
  public void testLazyShort() throws Throwable {
    try {
      LazyShort b = new LazyShort();
      b.setAll(new byte[]{'0'}, 0, 1);
      assertEquals(Short.valueOf((short)0), b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '0'}, 0, 2);
      assertEquals(Short.valueOf((short)0), b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '0'}, 0, 2);
      assertEquals(Short.valueOf((short)0), b.getPrimitiveObject());
      b.setAll(new byte[]{'a', '1', 'b'}, 1, 1);
      assertEquals(Short.valueOf((short)1), b.getPrimitiveObject());
      b.setAll(new byte[]{'a', '-', '1'}, 1, 2);
      assertEquals(Short.valueOf((short)-1), b.getPrimitiveObject());
      b.setAll(new byte[]{'a', '+', '1'}, 1, 2);
      assertEquals(Short.valueOf((short)1), b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '1', '2', '8'}, 0, 4);
      assertEquals(Short.valueOf((short)-128), b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '1', '2', '7'}, 0, 4);
      assertEquals(Short.valueOf((short)127), b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '3', '2', '7', '6', '8'}, 0, 6);
      assertEquals(Short.valueOf((short)-32768), b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '3', '2', '7', '6', '7'}, 0, 6);
      assertEquals(Short.valueOf((short)32767), b.getPrimitiveObject());

      b.setAll(new byte[]{'a', '1', 'b'}, 1, 2);
      assertNull(b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '3', '2', '7', '6', '9'}, 0, 6);
      assertNull(b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '3', '2', '7', '6', '8'}, 0, 6);
      assertNull(b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '1', '2', '3'}, 0, 1);
      assertNull(b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '1', '2', '3'}, 0, 1);
      assertNull(b.getPrimitiveObject());

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }
     

  /**
   * Test the LazyInteger class.
   */
  public void testLazyInteger() throws Throwable {
    try {
      LazyInteger b = new LazyInteger();
      b.setAll(new byte[]{'0'}, 0, 1);
      assertEquals(Integer.valueOf((int)0), b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '0'}, 0, 2);
      assertEquals(Integer.valueOf((int)0), b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '0'}, 0, 2);
      assertEquals(Integer.valueOf((int)0), b.getPrimitiveObject());
      b.setAll(new byte[]{'a', '1', 'b'}, 1, 1);
      assertEquals(Integer.valueOf((int)1), b.getPrimitiveObject());
      b.setAll(new byte[]{'a', '-', '1'}, 1, 2);
      assertEquals(Integer.valueOf((int)-1), b.getPrimitiveObject());
      b.setAll(new byte[]{'a', '+', '1'}, 1, 2);
      assertEquals(Integer.valueOf((int)1), b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '1', '2', '8'}, 0, 4);
      assertEquals(Integer.valueOf((int)-128), b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '1', '2', '7'}, 0, 4);
      assertEquals(Integer.valueOf((int)127), b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '3', '2', '7', '6', '8'}, 0, 6);
      assertEquals(Integer.valueOf((int)-32768), b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '3', '2', '7', '6', '7'}, 0, 6);
      assertEquals(Integer.valueOf((int)32767), b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '2', '1', '4', '7', '4', '8', '3', '6', '4', '8'}, 0, 11);
      assertEquals(Integer.valueOf((int)-2147483648), b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '2', '1', '4', '7', '4', '8', '3', '6', '4', '7'}, 0, 11);
      assertEquals(Integer.valueOf((int)2147483647), b.getPrimitiveObject());

      b.setAll(new byte[]{'a', '1', 'b'}, 1, 2);
      assertNull(b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '2', '1', '4', '7', '4', '8', '3', '6', '4', '9'}, 0, 11);
      assertNull(b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '2', '1', '4', '7', '4', '8', '3', '6', '4', '8'}, 0, 11);
      assertNull(b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '1', '2', '3'}, 0, 1);
      assertNull(b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '1', '2', '3'}, 0, 1);
      assertNull(b.getPrimitiveObject());

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }
    
  
  /**
   * Test the LazyLong class.
   */
  public void testLazyLong() throws Throwable {
    try {
      LazyLong b = new LazyLong();
      b.setAll(new byte[]{'0'}, 0, 1);
      assertEquals(Long.valueOf((long)0), b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '0'}, 0, 2);
      assertEquals(Long.valueOf((long)0), b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '0'}, 0, 2);
      assertEquals(Long.valueOf((long)0), b.getPrimitiveObject());
      b.setAll(new byte[]{'a', '1', 'b'}, 1, 1);
      assertEquals(Long.valueOf((long)1), b.getPrimitiveObject());
      b.setAll(new byte[]{'a', '-', '1'}, 1, 2);
      assertEquals(Long.valueOf((long)-1), b.getPrimitiveObject());
      b.setAll(new byte[]{'a', '+', '1'}, 1, 2);
      assertEquals(Long.valueOf((long)1), b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '1', '2', '8'}, 0, 4);
      assertEquals(Long.valueOf((long)-128), b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '1', '2', '7'}, 0, 4);
      assertEquals(Long.valueOf((long)127), b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '3', '2', '7', '6', '8'}, 0, 6);
      assertEquals(Long.valueOf((long)-32768), b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '3', '2', '7', '6', '7'}, 0, 6);
      assertEquals(Long.valueOf((long)32767), b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '2', '1', '4', '7', '4', '8', '3', '6', '4', '8'}, 0, 11);
      assertEquals(Long.valueOf((long)-2147483648), b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '2', '1', '4', '7', '4', '8', '3', '6', '4', '7'}, 0, 11);
      assertEquals(Long.valueOf((long)2147483647), b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '9', '2', '2', '3', '3', '7', '2', '0', '3', '6', '8', '5',
          '4', '7', '7', '5', '8', '0', '8'}, 0, 20);
      assertEquals(Long.valueOf((long)-9223372036854775808L), b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '9', '2', '2', '3', '3', '7', '2', '0', '3', '6', '8', '5',
          '4', '7', '7', '5', '8', '0', '7'}, 0, 20);
      assertEquals(Long.valueOf((long)9223372036854775807L), b.getPrimitiveObject());

      b.setAll(new byte[]{'a', '1', 'b'}, 1, 2);
      assertNull(b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '9', '2', '2', '3', '3', '7', '2', '0', '3', '6', '8', '5',
          '4', '7', '7', '5', '8', '0', '9'}, 0, 20);
      assertNull(b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '9', '2', '2', '3', '3', '7', '2', '0', '3', '6', '8', '5',
          '4', '7', '7', '5', '8', '0', '8'}, 0, 20);
      assertNull(b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '1', '2', '3'}, 0, 1);
      assertNull(b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '1', '2', '3'}, 0, 1);
      assertNull(b.getPrimitiveObject());

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  /**
   * Test the LazyDouble class.
   */
  public void testLazyDouble() throws Throwable {
    try {
      LazyDouble b = new LazyDouble();
      b.setAll(new byte[]{'0'}, 0, 1);
      assertEquals(Double.valueOf((double)0), b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '0'}, 0, 2);
      assertEquals(Double.valueOf((double)0), b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '0'}, 0, 2);
      assertEquals(Double.valueOf((double)-0.0), b.getPrimitiveObject());
      b.setAll(new byte[]{'a', '1', 'b'}, 1, 1);
      assertEquals(Double.valueOf((double)1), b.getPrimitiveObject());
      b.setAll(new byte[]{'a', '-', '1'}, 1, 2);
      assertEquals(Double.valueOf((double)-1), b.getPrimitiveObject());
      b.setAll(new byte[]{'a', '+', '1'}, 1, 2);
      assertEquals(Double.valueOf((double)1), b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '1', '2', '8'}, 0, 4);
      assertEquals(Double.valueOf((double)-128), b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '1', '2', '7'}, 0, 4);
      assertEquals(Double.valueOf((double)127), b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '3', '2', '7', '6', '8'}, 0, 6);
      assertEquals(Double.valueOf((double)-32768), b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '3', '2', '7', '6', '7'}, 0, 6);
      assertEquals(Double.valueOf((double)32767), b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '2', '1', '4', '7', '4', '8', '3', '6', '4', '8'}, 0, 11);
      assertEquals(Double.valueOf((double)-2147483648), b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '2', '1', '4', '7', '4', '8', '3', '6', '4', '7'}, 0, 11);
      assertEquals(Double.valueOf((double)2147483647), b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '9', '2', '2', '3', '3', '7', '2', '0', '3', '6', '8', '5',
          '4', '7', '7', '5', '8', '0', '8'}, 0, 20);
      assertEquals(Double.valueOf((double)-9223372036854775808L), b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '9', '2', '2', '3', '3', '7', '2', '0', '3', '6', '8', '5',
          '4', '7', '7', '5', '8', '0', '7'}, 0, 20);
      assertEquals(Double.valueOf((long)9223372036854775807L), b.getPrimitiveObject());

      b.setAll(new byte[]{'-', '3', '.', '7', '6', '8'}, 0, 6);
      assertEquals(Double.valueOf((double)-3.768), b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '3', '.', '7', '6', '7'}, 0, 6);
      assertEquals(Double.valueOf((double)3.767), b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '2', '.', '4', '7', '4', '8', '3', '6', 'e', '8'}, 0, 11);
      assertEquals(Double.valueOf((double)-2.474836e8), b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '2', '.', '4', '7', '4', '8', '3', 'E', '-', '7'}, 0, 11);
      assertEquals(Double.valueOf((double)2.47483E-7), b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '.', '4', '7', '4', '8', '3', '6', 'e', '8'}, 0, 10);
      assertEquals(Double.valueOf((double)-.474836e8), b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '.', '4', '7', '4', '8', '3', 'E', '-', '7'}, 0, 10);
      assertEquals(Double.valueOf((double).47483E-7), b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '2', '1', '4', '7', '4', '8', '3', '6', '4', '.'}, 0, 11);
      assertEquals(Double.valueOf((double)-214748364.), b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '2', '1', '4', '7', '4', '8', '3', '6', '4', '.'}, 0, 11);
      assertEquals(Double.valueOf((double)+214748364.), b.getPrimitiveObject());

      b.setAll(new byte[]{'.', '0'}, 0, 2);
      assertEquals(Double.valueOf((double).0), b.getPrimitiveObject());
      b.setAll(new byte[]{'0', '.'}, 0, 2);
      assertEquals(Double.valueOf((double)0.), b.getPrimitiveObject());
      
      b.setAll(new byte[]{'a', '1', 'b'}, 1, 2);
      assertNull(b.getPrimitiveObject());
      assertNull(b.getPrimitiveObject());
      b.setAll(new byte[]{'.', '1', '2', '3'}, 0, 1);
      assertNull(b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '1', '2', '3'}, 0, 1);
      assertNull(b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '1', '2', '3'}, 0, 1);
      assertNull(b.getPrimitiveObject());
      
      b.setAll(new byte[]{'-', '1', 'e', '3', '3', '3', '3', '3', '3'}, 0, 9);
      assertEquals(Double.NEGATIVE_INFINITY, b.getPrimitiveObject());
      b.setAll(new byte[]{'+', '1', 'e', '3', '3', '3', '3', '3', '3'}, 0, 9);
      assertEquals(Double.POSITIVE_INFINITY, b.getPrimitiveObject());

      b.setAll(new byte[]{'+', '1', 'e', '-', '3', '3', '3', '3', '3'}, 0, 8);
      assertEquals(Double.valueOf((double)0), b.getPrimitiveObject());
      b.setAll(new byte[]{'-', '1', 'e', '-', '3', '3', '3', '3', '3'}, 0, 8);
      assertEquals(Double.valueOf((double)-0.0), b.getPrimitiveObject());
      
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }
  
  /**
   * Test the LazyString class.
   */
  public void testLazyString() throws Throwable {
    try {
      LazyString b = new LazyString();
      b.setAll(new byte[]{'0'}, 0, 1);
      assertEquals("0", b.getPrimitiveObject());
      b.setAll(new byte[]{'0', '1', '2'}, 1, 1);
      assertEquals("1", b.getPrimitiveObject());
      
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }
}
