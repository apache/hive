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


import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

public class TestLazyPrimitive extends TestCase {

  /**
   * Initialize the LazyObject with the parameters, wrapping the byte[] automatically.
   */
  public static void initLazyObject(LazyObject lo, byte[] data, int start, int length) {
    ByteArrayRef b = new ByteArrayRef();
    b.setData(data);
    lo.init(b, start, length);    
  }
  /**
   * Test the LazyByte class.
   */
  public void testLazyByte() throws Throwable {
    try {
      LazyByte b = new LazyByte();
      initLazyObject(b,new byte[]{'0'}, 0, 0);
      assertNull(b.getObject());
      initLazyObject(b,new byte[]{'0'}, 0, 1);
      assertEquals(new ByteWritable((byte)0), b.getObject());
      initLazyObject(b,new byte[]{'+', '0'}, 0, 2);
      assertEquals(new ByteWritable((byte)0), b.getObject());
      initLazyObject(b,new byte[]{'-', '0'}, 0, 2);
      assertEquals(new ByteWritable((byte)0), b.getObject());
      initLazyObject(b,new byte[]{'a', '1', 'b'}, 1, 1);
      assertEquals(new ByteWritable((byte)1), b.getObject());
      initLazyObject(b,new byte[]{'a', '-', '1'}, 1, 2);
      assertEquals(new ByteWritable((byte)-1), b.getObject());
      initLazyObject(b,new byte[]{'a', '+', '1'}, 1, 2);
      assertEquals(new ByteWritable((byte)1), b.getObject());
      initLazyObject(b,new byte[]{'-', '1', '2', '8'}, 0, 4);
      assertEquals(new ByteWritable((byte)-128), b.getObject());
      initLazyObject(b,new byte[]{'+', '1', '2', '7'}, 0, 4);
      assertEquals(new ByteWritable((byte)127), b.getObject());
      
      initLazyObject(b,new byte[]{'a', '1', 'b'}, 1, 2);
      assertNull(b.getObject());
      initLazyObject(b,new byte[]{'+', '1', '2', '8'}, 0, 4);
      assertNull(b.getObject());
      initLazyObject(b,new byte[]{'-', '1', '2', '9'}, 0, 4);
      assertNull(b.getObject());
      initLazyObject(b,new byte[]{'-', '1', '2', '3'}, 0, 1);
      assertNull(b.getObject());
      initLazyObject(b,new byte[]{'+', '1', '2', '3'}, 0, 1);
      assertNull(b.getObject());
     
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
      initLazyObject(b,new byte[]{'0'}, 0, 0);
      assertNull(b.getObject());
      initLazyObject(b,new byte[]{'0'}, 0, 1);
      assertEquals(new ShortWritable((short)0), b.getObject());
      initLazyObject(b,new byte[]{'+', '0'}, 0, 2);
      assertEquals(new ShortWritable((short)0), b.getObject());
      initLazyObject(b,new byte[]{'-', '0'}, 0, 2);
      assertEquals(new ShortWritable((short)0), b.getObject());
      initLazyObject(b,new byte[]{'a', '1', 'b'}, 1, 1);
      assertEquals(new ShortWritable((short)1), b.getObject());
      initLazyObject(b,new byte[]{'a', '-', '1'}, 1, 2);
      assertEquals(new ShortWritable((short)-1), b.getObject());
      initLazyObject(b,new byte[]{'a', '+', '1'}, 1, 2);
      assertEquals(new ShortWritable((short)1), b.getObject());
      initLazyObject(b,new byte[]{'-', '1', '2', '8'}, 0, 4);
      assertEquals(new ShortWritable((short)-128), b.getObject());
      initLazyObject(b,new byte[]{'+', '1', '2', '7'}, 0, 4);
      assertEquals(new ShortWritable((short)127), b.getObject());
      initLazyObject(b,new byte[]{'-', '3', '2', '7', '6', '8'}, 0, 6);
      assertEquals(new ShortWritable((short)-32768), b.getObject());
      initLazyObject(b,new byte[]{'+', '3', '2', '7', '6', '7'}, 0, 6);
      assertEquals(new ShortWritable((short)32767), b.getObject());

      initLazyObject(b,new byte[]{'a', '1', 'b'}, 1, 2);
      assertNull(b.getObject());
      initLazyObject(b,new byte[]{'-', '3', '2', '7', '6', '9'}, 0, 6);
      assertNull(b.getObject());
      initLazyObject(b,new byte[]{'+', '3', '2', '7', '6', '8'}, 0, 6);
      assertNull(b.getObject());
      initLazyObject(b,new byte[]{'-', '1', '2', '3'}, 0, 1);
      assertNull(b.getObject());
      initLazyObject(b,new byte[]{'+', '1', '2', '3'}, 0, 1);
      assertNull(b.getObject());

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
      initLazyObject(b,new byte[]{'0'}, 0, 0);
      assertNull(b.getObject());
      initLazyObject(b,new byte[]{'0'}, 0, 1);
      assertEquals(new IntWritable((int)0), b.getObject());
      initLazyObject(b,new byte[]{'+', '0'}, 0, 2);
      assertEquals(new IntWritable((int)0), b.getObject());
      initLazyObject(b,new byte[]{'-', '0'}, 0, 2);
      assertEquals(new IntWritable((int)0), b.getObject());
      initLazyObject(b,new byte[]{'a', '1', 'b'}, 1, 1);
      assertEquals(new IntWritable((int)1), b.getObject());
      initLazyObject(b,new byte[]{'a', '-', '1'}, 1, 2);
      assertEquals(new IntWritable((int)-1), b.getObject());
      initLazyObject(b,new byte[]{'a', '+', '1'}, 1, 2);
      assertEquals(new IntWritable((int)1), b.getObject());
      initLazyObject(b,new byte[]{'-', '1', '2', '8'}, 0, 4);
      assertEquals(new IntWritable((int)-128), b.getObject());
      initLazyObject(b,new byte[]{'+', '1', '2', '7'}, 0, 4);
      assertEquals(new IntWritable((int)127), b.getObject());
      initLazyObject(b,new byte[]{'-', '3', '2', '7', '6', '8'}, 0, 6);
      assertEquals(new IntWritable((int)-32768), b.getObject());
      initLazyObject(b,new byte[]{'+', '3', '2', '7', '6', '7'}, 0, 6);
      assertEquals(new IntWritable((int)32767), b.getObject());
      initLazyObject(b,new byte[]{'-', '2', '1', '4', '7', '4', '8', '3', '6', '4', '8'}, 0, 11);
      assertEquals(new IntWritable((int)-2147483648), b.getObject());
      initLazyObject(b,new byte[]{'+', '2', '1', '4', '7', '4', '8', '3', '6', '4', '7'}, 0, 11);
      assertEquals(new IntWritable((int)2147483647), b.getObject());

      initLazyObject(b,new byte[]{'a', '1', 'b'}, 1, 2);
      assertNull(b.getObject());
      initLazyObject(b,new byte[]{'-', '2', '1', '4', '7', '4', '8', '3', '6', '4', '9'}, 0, 11);
      assertNull(b.getObject());
      initLazyObject(b,new byte[]{'+', '2', '1', '4', '7', '4', '8', '3', '6', '4', '8'}, 0, 11);
      assertNull(b.getObject());
      initLazyObject(b,new byte[]{'-', '1', '2', '3'}, 0, 1);
      assertNull(b.getObject());
      initLazyObject(b,new byte[]{'+', '1', '2', '3'}, 0, 1);
      assertNull(b.getObject());

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
      initLazyObject(b,new byte[]{'0'}, 0, 0);
      assertNull(b.getObject());
      initLazyObject(b,new byte[]{'0'}, 0, 1);
      assertEquals(new LongWritable((long)0), b.getObject());
      initLazyObject(b,new byte[]{'+', '0'}, 0, 2);
      assertEquals(new LongWritable((long)0), b.getObject());
      initLazyObject(b,new byte[]{'-', '0'}, 0, 2);
      assertEquals(new LongWritable((long)0), b.getObject());
      initLazyObject(b,new byte[]{'a', '1', 'b'}, 1, 1);
      assertEquals(new LongWritable((long)1), b.getObject());
      initLazyObject(b,new byte[]{'a', '-', '1'}, 1, 2);
      assertEquals(new LongWritable((long)-1), b.getObject());
      initLazyObject(b,new byte[]{'a', '+', '1'}, 1, 2);
      assertEquals(new LongWritable((long)1), b.getObject());
      initLazyObject(b,new byte[]{'-', '1', '2', '8'}, 0, 4);
      assertEquals(new LongWritable((long)-128), b.getObject());
      initLazyObject(b,new byte[]{'+', '1', '2', '7'}, 0, 4);
      assertEquals(new LongWritable((long)127), b.getObject());
      initLazyObject(b,new byte[]{'-', '3', '2', '7', '6', '8'}, 0, 6);
      assertEquals(new LongWritable((long)-32768), b.getObject());
      initLazyObject(b,new byte[]{'+', '3', '2', '7', '6', '7'}, 0, 6);
      assertEquals(new LongWritable((long)32767), b.getObject());
      initLazyObject(b,new byte[]{'-', '2', '1', '4', '7', '4', '8', '3', '6', '4', '8'}, 0, 11);
      assertEquals(new LongWritable((long)-2147483648), b.getObject());
      initLazyObject(b,new byte[]{'+', '2', '1', '4', '7', '4', '8', '3', '6', '4', '7'}, 0, 11);
      assertEquals(new LongWritable((long)2147483647), b.getObject());
      initLazyObject(b,new byte[]{'-', '9', '2', '2', '3', '3', '7', '2', '0', '3', '6', '8', '5',
          '4', '7', '7', '5', '8', '0', '8'}, 0, 20);
      assertEquals(new LongWritable((long)-9223372036854775808L), b.getObject());
      initLazyObject(b,new byte[]{'+', '9', '2', '2', '3', '3', '7', '2', '0', '3', '6', '8', '5',
          '4', '7', '7', '5', '8', '0', '7'}, 0, 20);
      assertEquals(new LongWritable((long)9223372036854775807L), b.getObject());

      initLazyObject(b,new byte[]{'a', '1', 'b'}, 1, 2);
      assertNull(b.getObject());
      initLazyObject(b,new byte[]{'-', '9', '2', '2', '3', '3', '7', '2', '0', '3', '6', '8', '5',
          '4', '7', '7', '5', '8', '0', '9'}, 0, 20);
      assertNull(b.getObject());
      initLazyObject(b,new byte[]{'+', '9', '2', '2', '3', '3', '7', '2', '0', '3', '6', '8', '5',
          '4', '7', '7', '5', '8', '0', '8'}, 0, 20);
      assertNull(b.getObject());
      initLazyObject(b,new byte[]{'-', '1', '2', '3'}, 0, 1);
      assertNull(b.getObject());
      initLazyObject(b,new byte[]{'+', '1', '2', '3'}, 0, 1);
      assertNull(b.getObject());

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
      initLazyObject(b,new byte[]{'0'}, 0, 0);
      assertNull(b.getObject());
      initLazyObject(b,new byte[]{'0'}, 0, 1);
      assertEquals(new DoubleWritable((double)0), b.getObject());
      initLazyObject(b,new byte[]{'+', '0'}, 0, 2);
      assertEquals(new DoubleWritable((double)0), b.getObject());
      initLazyObject(b,new byte[]{'-', '0'}, 0, 2);
      assertEquals(new DoubleWritable((double)-0.0), b.getObject());
      initLazyObject(b,new byte[]{'a', '1', 'b'}, 1, 1);
      assertEquals(new DoubleWritable((double)1), b.getObject());
      initLazyObject(b,new byte[]{'a', '-', '1'}, 1, 2);
      assertEquals(new DoubleWritable((double)-1), b.getObject());
      initLazyObject(b,new byte[]{'a', '+', '1'}, 1, 2);
      assertEquals(new DoubleWritable((double)1), b.getObject());
      initLazyObject(b,new byte[]{'-', '1', '2', '8'}, 0, 4);
      assertEquals(new DoubleWritable((double)-128), b.getObject());
      initLazyObject(b,new byte[]{'+', '1', '2', '7'}, 0, 4);
      assertEquals(new DoubleWritable((double)127), b.getObject());
      initLazyObject(b,new byte[]{'-', '3', '2', '7', '6', '8'}, 0, 6);
      assertEquals(new DoubleWritable((double)-32768), b.getObject());
      initLazyObject(b,new byte[]{'+', '3', '2', '7', '6', '7'}, 0, 6);
      assertEquals(new DoubleWritable((double)32767), b.getObject());
      initLazyObject(b,new byte[]{'-', '2', '1', '4', '7', '4', '8', '3', '6', '4', '8'}, 0, 11);
      assertEquals(new DoubleWritable((double)-2147483648), b.getObject());
      initLazyObject(b,new byte[]{'+', '2', '1', '4', '7', '4', '8', '3', '6', '4', '7'}, 0, 11);
      assertEquals(new DoubleWritable((double)2147483647), b.getObject());
      initLazyObject(b,new byte[]{'-', '9', '2', '2', '3', '3', '7', '2', '0', '3', '6', '8', '5',
          '4', '7', '7', '5', '8', '0', '8'}, 0, 20);
      assertEquals(new DoubleWritable((double)-9223372036854775808L), b.getObject());
      initLazyObject(b,new byte[]{'+', '9', '2', '2', '3', '3', '7', '2', '0', '3', '6', '8', '5',
          '4', '7', '7', '5', '8', '0', '7'}, 0, 20);
      assertEquals(new DoubleWritable((long)9223372036854775807L), b.getObject());

      initLazyObject(b,new byte[]{'-', '3', '.', '7', '6', '8'}, 0, 6);
      assertEquals(new DoubleWritable((double)-3.768), b.getObject());
      initLazyObject(b,new byte[]{'+', '3', '.', '7', '6', '7'}, 0, 6);
      assertEquals(new DoubleWritable((double)3.767), b.getObject());
      initLazyObject(b,new byte[]{'-', '2', '.', '4', '7', '4', '8', '3', '6', 'e', '8'}, 0, 11);
      assertEquals(new DoubleWritable((double)-2.474836e8), b.getObject());
      initLazyObject(b,new byte[]{'+', '2', '.', '4', '7', '4', '8', '3', 'E', '-', '7'}, 0, 11);
      assertEquals(new DoubleWritable((double)2.47483E-7), b.getObject());
      initLazyObject(b,new byte[]{'-', '.', '4', '7', '4', '8', '3', '6', 'e', '8'}, 0, 10);
      assertEquals(new DoubleWritable((double)-.474836e8), b.getObject());
      initLazyObject(b,new byte[]{'+', '.', '4', '7', '4', '8', '3', 'E', '-', '7'}, 0, 10);
      assertEquals(new DoubleWritable((double).47483E-7), b.getObject());
      initLazyObject(b,new byte[]{'-', '2', '1', '4', '7', '4', '8', '3', '6', '4', '.'}, 0, 11);
      assertEquals(new DoubleWritable((double)-214748364.), b.getObject());
      initLazyObject(b,new byte[]{'+', '2', '1', '4', '7', '4', '8', '3', '6', '4', '.'}, 0, 11);
      assertEquals(new DoubleWritable((double)+214748364.), b.getObject());

      initLazyObject(b,new byte[]{'.', '0'}, 0, 2);
      assertEquals(new DoubleWritable((double).0), b.getObject());
      initLazyObject(b,new byte[]{'0', '.'}, 0, 2);
      assertEquals(new DoubleWritable((double)0.), b.getObject());
      
      initLazyObject(b,new byte[]{'a', '1', 'b'}, 1, 2);
      assertNull(b.getObject());
      assertNull(b.getObject());
      initLazyObject(b,new byte[]{'.', '1', '2', '3'}, 0, 1);
      assertNull(b.getObject());
      initLazyObject(b,new byte[]{'-', '1', '2', '3'}, 0, 1);
      assertNull(b.getObject());
      initLazyObject(b,new byte[]{'+', '1', '2', '3'}, 0, 1);
      assertNull(b.getObject());
      
      initLazyObject(b,new byte[]{'-', '1', 'e', '3', '3', '3', '3', '3', '3'}, 0, 9);
      assertEquals(new DoubleWritable(Double.NEGATIVE_INFINITY), b.getObject());
      initLazyObject(b,new byte[]{'+', '1', 'e', '3', '3', '3', '3', '3', '3'}, 0, 9);
      assertEquals(new DoubleWritable(Double.POSITIVE_INFINITY), b.getObject());

      initLazyObject(b,new byte[]{'+', '1', 'e', '-', '3', '3', '3', '3', '3'}, 0, 8);
      assertEquals(new DoubleWritable((double)0), b.getObject());
      initLazyObject(b,new byte[]{'-', '1', 'e', '-', '3', '3', '3', '3', '3'}, 0, 8);
      assertEquals(new DoubleWritable((double)-0.0), b.getObject());
      
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
      initLazyObject(b,new byte[]{'0'}, 0, 0);
      assertEquals(new Text(""), b.getObject());
      initLazyObject(b,new byte[]{'0'}, 0, 1);
      assertEquals(new Text("0"), b.getObject());
      initLazyObject(b,new byte[]{'0', '1', '2'}, 1, 1);
      assertEquals(new Text("1"), b.getObject());
      
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }
  
  public void testLazyIntegerWrite() throws Throwable {
    try {
      ByteStream.Output out = new ByteStream.Output();
      
      int[] tests = {0, -1, 1, -10, 10, -123, 123, 
          Integer.MIN_VALUE, Integer.MIN_VALUE + 1,
          Integer.MAX_VALUE, Integer.MAX_VALUE - 1};
      for (int i=0; i<tests.length; i++) {
        int v = tests[i];
        out.reset();
        LazyInteger.writeUTF8(out, v);
        Text t = new Text();
        t.set(out.getData(), 0, out.getCount());
        assertEquals(String.valueOf(v), t.toString());
      }
      
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }  

  public void testLazyLongWrite() throws Throwable {
    try {
      ByteStream.Output out = new ByteStream.Output();
      
      long[] tests = {0L, -1, 1, -10, 10, -123, 123,
          Long.MIN_VALUE, Long.MIN_VALUE + 1, 
          Long.MAX_VALUE, Long.MAX_VALUE - 1};
      for (int i=0; i<tests.length; i++) {
        long v = tests[i];
        out.reset();
        LazyLong.writeUTF8(out, v);
        Text t = new Text();
        t.set(out.getData(), 0, out.getCount());
        assertEquals(String.valueOf(v), t.toString());
      }
      
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }  

}
