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

import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * TestLazyPrimitive.
 *
 */
public class TestLazyPrimitive extends TestCase {

  /**
   * Initialize the LazyObject with the parameters, wrapping the byte[]
   * automatically.
   */
  public static void initLazyObject(LazyObject lo, byte[] data, int start,
      int length) {
    ByteArrayRef b = new ByteArrayRef();
    b.setData(data);
    lo.init(b, start, length);
  }

  /**
   * Test the LazyByte class.
   */
  public void testLazyByte() throws Throwable {
    try {
      LazyByte b = new LazyByte(
          LazyPrimitiveObjectInspectorFactory.LAZY_BYTE_OBJECT_INSPECTOR);
      initLazyObject(b, new byte[] {'0'}, 0, 0);
      assertNull(b.getWritableObject());
      initLazyObject(b, new byte[] {'0'}, 0, 1);
      assertEquals(new ByteWritable((byte) 0), b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '0'}, 0, 2);
      assertEquals(new ByteWritable((byte) 0), b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '0'}, 0, 2);
      assertEquals(new ByteWritable((byte) 0), b.getWritableObject());
      initLazyObject(b, new byte[] {'a', '1', 'b'}, 1, 1);
      assertEquals(new ByteWritable((byte) 1), b.getWritableObject());
      initLazyObject(b, new byte[] {'a', '-', '1'}, 1, 2);
      assertEquals(new ByteWritable((byte) -1), b.getWritableObject());
      initLazyObject(b, new byte[] {'a', '+', '1'}, 1, 2);
      assertEquals(new ByteWritable((byte) 1), b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '1', '2', '8'}, 0, 4);
      assertEquals(new ByteWritable((byte) -128), b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '1', '2', '7'}, 0, 4);
      assertEquals(new ByteWritable((byte) 127), b.getWritableObject());

      initLazyObject(b, new byte[] {'a', '1', 'b'}, 1, 2);
      assertNull(b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '1', '2', '8'}, 0, 4);
      assertNull(b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '1', '2', '9'}, 0, 4);
      assertNull(b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '1', '2', '3'}, 0, 1);
      assertNull(b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '1', '2', '3'}, 0, 1);
      assertNull(b.getWritableObject());

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
      LazyShort b = new LazyShort(
          LazyPrimitiveObjectInspectorFactory.LAZY_SHORT_OBJECT_INSPECTOR);
      initLazyObject(b, new byte[] {'0'}, 0, 0);
      assertNull(b.getWritableObject());
      initLazyObject(b, new byte[] {'0'}, 0, 1);
      assertEquals(new ShortWritable((short) 0), b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '0'}, 0, 2);
      assertEquals(new ShortWritable((short) 0), b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '0'}, 0, 2);
      assertEquals(new ShortWritable((short) 0), b.getWritableObject());
      initLazyObject(b, new byte[] {'a', '1', 'b'}, 1, 1);
      assertEquals(new ShortWritable((short) 1), b.getWritableObject());
      initLazyObject(b, new byte[] {'a', '-', '1'}, 1, 2);
      assertEquals(new ShortWritable((short) -1), b.getWritableObject());
      initLazyObject(b, new byte[] {'a', '+', '1'}, 1, 2);
      assertEquals(new ShortWritable((short) 1), b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '1', '2', '8'}, 0, 4);
      assertEquals(new ShortWritable((short) -128), b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '1', '2', '7'}, 0, 4);
      assertEquals(new ShortWritable((short) 127), b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '3', '2', '7', '6', '8'}, 0, 6);
      assertEquals(new ShortWritable((short) -32768), b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '3', '2', '7', '6', '7'}, 0, 6);
      assertEquals(new ShortWritable((short) 32767), b.getWritableObject());

      initLazyObject(b, new byte[] {'a', '1', 'b'}, 1, 2);
      assertNull(b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '3', '2', '7', '6', '9'}, 0, 6);
      assertNull(b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '3', '2', '7', '6', '8'}, 0, 6);
      assertNull(b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '1', '2', '3'}, 0, 1);
      assertNull(b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '1', '2', '3'}, 0, 1);
      assertNull(b.getWritableObject());

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
      LazyInteger b = new LazyInteger(
          LazyPrimitiveObjectInspectorFactory.LAZY_INT_OBJECT_INSPECTOR);
      initLazyObject(b, new byte[] {'0'}, 0, 0);
      assertNull(b.getWritableObject());
      initLazyObject(b, new byte[] {'0'}, 0, 1);
      assertEquals(new IntWritable(0), b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '0'}, 0, 2);
      assertEquals(new IntWritable(0), b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '0'}, 0, 2);
      assertEquals(new IntWritable(0), b.getWritableObject());
      initLazyObject(b, new byte[] {'a', '1', 'b'}, 1, 1);
      assertEquals(new IntWritable(1), b.getWritableObject());
      initLazyObject(b, new byte[] {'a', '-', '1'}, 1, 2);
      assertEquals(new IntWritable(-1), b.getWritableObject());
      initLazyObject(b, new byte[] {'a', '+', '1'}, 1, 2);
      assertEquals(new IntWritable(1), b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '1', '2', '8'}, 0, 4);
      assertEquals(new IntWritable(-128), b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '1', '2', '7'}, 0, 4);
      assertEquals(new IntWritable(127), b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '3', '2', '7', '6', '8'}, 0, 6);
      assertEquals(new IntWritable(-32768), b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '3', '2', '7', '6', '7'}, 0, 6);
      assertEquals(new IntWritable(32767), b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '2', '1', '4', '7', '4', '8', '3',
          '6', '4', '8'}, 0, 11);
      assertEquals(new IntWritable(-2147483648), b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '2', '1', '4', '7', '4', '8', '3',
          '6', '4', '7'}, 0, 11);
      assertEquals(new IntWritable(2147483647), b.getWritableObject());

      initLazyObject(b, new byte[] {'a', '1', 'b'}, 1, 2);
      assertNull(b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '2', '1', '4', '7', '4', '8', '3',
          '6', '4', '9'}, 0, 11);
      assertNull(b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '2', '1', '4', '7', '4', '8', '3',
          '6', '4', '8'}, 0, 11);
      assertNull(b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '1', '2', '3'}, 0, 1);
      assertNull(b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '1', '2', '3'}, 0, 1);
      assertNull(b.getWritableObject());

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
      LazyLong b = new LazyLong(
          LazyPrimitiveObjectInspectorFactory.LAZY_LONG_OBJECT_INSPECTOR);
      initLazyObject(b, new byte[] {'0'}, 0, 0);
      assertNull(b.getWritableObject());
      initLazyObject(b, new byte[] {'0'}, 0, 1);
      assertEquals(new LongWritable(0), b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '0'}, 0, 2);
      assertEquals(new LongWritable(0), b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '0'}, 0, 2);
      assertEquals(new LongWritable(0), b.getWritableObject());
      initLazyObject(b, new byte[] {'a', '1', 'b'}, 1, 1);
      assertEquals(new LongWritable(1), b.getWritableObject());
      initLazyObject(b, new byte[] {'a', '-', '1'}, 1, 2);
      assertEquals(new LongWritable(-1), b.getWritableObject());
      initLazyObject(b, new byte[] {'a', '+', '1'}, 1, 2);
      assertEquals(new LongWritable(1), b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '1', '2', '8'}, 0, 4);
      assertEquals(new LongWritable(-128), b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '1', '2', '7'}, 0, 4);
      assertEquals(new LongWritable(127), b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '3', '2', '7', '6', '8'}, 0, 6);
      assertEquals(new LongWritable(-32768), b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '3', '2', '7', '6', '7'}, 0, 6);
      assertEquals(new LongWritable(32767), b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '2', '1', '4', '7', '4', '8', '3',
          '6', '4', '8'}, 0, 11);
      assertEquals(new LongWritable(-2147483648), b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '2', '1', '4', '7', '4', '8', '3',
          '6', '4', '7'}, 0, 11);
      assertEquals(new LongWritable(2147483647), b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '9', '2', '2', '3', '3', '7', '2',
          '0', '3', '6', '8', '5', '4', '7', '7', '5', '8', '0', '8'}, 0, 20);
      assertEquals(new LongWritable(-9223372036854775808L), b
          .getWritableObject());
      initLazyObject(b, new byte[] {'+', '9', '2', '2', '3', '3', '7', '2',
          '0', '3', '6', '8', '5', '4', '7', '7', '5', '8', '0', '7'}, 0, 20);
      assertEquals(new LongWritable(9223372036854775807L), b
          .getWritableObject());

      initLazyObject(b, new byte[] {'a', '1', 'b'}, 1, 2);
      assertNull(b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '9', '2', '2', '3', '3', '7', '2',
          '0', '3', '6', '8', '5', '4', '7', '7', '5', '8', '0', '9'}, 0, 20);
      assertNull(b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '9', '2', '2', '3', '3', '7', '2',
          '0', '3', '6', '8', '5', '4', '7', '7', '5', '8', '0', '8'}, 0, 20);
      assertNull(b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '1', '2', '3'}, 0, 1);
      assertNull(b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '1', '2', '3'}, 0, 1);
      assertNull(b.getWritableObject());

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
      LazyDouble b = new LazyDouble(
          LazyPrimitiveObjectInspectorFactory.LAZY_DOUBLE_OBJECT_INSPECTOR);
      initLazyObject(b, new byte[] {'0'}, 0, 0);
      assertNull(b.getWritableObject());
      initLazyObject(b, new byte[] {'0'}, 0, 1);
      assertEquals(new DoubleWritable(0), b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '0'}, 0, 2);
      assertEquals(new DoubleWritable(0), b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '0'}, 0, 2);
      assertEquals(new DoubleWritable(-0.0), b.getWritableObject());
      initLazyObject(b, new byte[] {'a', '1', 'b'}, 1, 1);
      assertEquals(new DoubleWritable(1), b.getWritableObject());
      initLazyObject(b, new byte[] {'a', '-', '1'}, 1, 2);
      assertEquals(new DoubleWritable(-1), b.getWritableObject());
      initLazyObject(b, new byte[] {'a', '+', '1'}, 1, 2);
      assertEquals(new DoubleWritable(1), b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '1', '2', '8'}, 0, 4);
      assertEquals(new DoubleWritable(-128), b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '1', '2', '7'}, 0, 4);
      assertEquals(new DoubleWritable(127), b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '3', '2', '7', '6', '8'}, 0, 6);
      assertEquals(new DoubleWritable(-32768), b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '3', '2', '7', '6', '7'}, 0, 6);
      assertEquals(new DoubleWritable(32767), b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '2', '1', '4', '7', '4', '8', '3',
          '6', '4', '8'}, 0, 11);
      assertEquals(new DoubleWritable(-2147483648), b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '2', '1', '4', '7', '4', '8', '3',
          '6', '4', '7'}, 0, 11);
      assertEquals(new DoubleWritable(2147483647), b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '9', '2', '2', '3', '3', '7', '2',
          '0', '3', '6', '8', '5', '4', '7', '7', '5', '8', '0', '8'}, 0, 20);
      assertEquals(new DoubleWritable(-9223372036854775808L), b
          .getWritableObject());
      initLazyObject(b, new byte[] {'+', '9', '2', '2', '3', '3', '7', '2',
          '0', '3', '6', '8', '5', '4', '7', '7', '5', '8', '0', '7'}, 0, 20);
      assertEquals(new DoubleWritable(9223372036854775807L), b
          .getWritableObject());

      initLazyObject(b, new byte[] {'-', '3', '.', '7', '6', '8'}, 0, 6);
      assertEquals(new DoubleWritable(-3.768), b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '3', '.', '7', '6', '7'}, 0, 6);
      assertEquals(new DoubleWritable(3.767), b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '2', '.', '4', '7', '4', '8', '3',
          '6', 'e', '8'}, 0, 11);
      assertEquals(new DoubleWritable(-2.474836e8), b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '2', '.', '4', '7', '4', '8', '3',
          'E', '-', '7'}, 0, 11);
      assertEquals(new DoubleWritable(2.47483E-7), b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '.', '4', '7', '4', '8', '3', '6',
          'e', '8'}, 0, 10);
      assertEquals(new DoubleWritable(-.474836e8), b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '.', '4', '7', '4', '8', '3', 'E',
          '-', '7'}, 0, 10);
      assertEquals(new DoubleWritable(.47483E-7), b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '2', '1', '4', '7', '4', '8', '3',
          '6', '4', '.'}, 0, 11);
      assertEquals(new DoubleWritable(-214748364.), b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '2', '1', '4', '7', '4', '8', '3',
          '6', '4', '.'}, 0, 11);
      assertEquals(new DoubleWritable(+214748364.), b.getWritableObject());

      initLazyObject(b, new byte[] {'.', '0'}, 0, 2);
      assertEquals(new DoubleWritable(.0), b.getWritableObject());
      initLazyObject(b, new byte[] {'0', '.'}, 0, 2);
      assertEquals(new DoubleWritable(0.), b.getWritableObject());

      initLazyObject(b, new byte[] {'a', '1', 'b'}, 1, 2);
      assertNull(b.getWritableObject());
      assertNull(b.getWritableObject());
      initLazyObject(b, new byte[] {'.', '1', '2', '3'}, 0, 1);
      assertNull(b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '1', '2', '3'}, 0, 1);
      assertNull(b.getWritableObject());
      initLazyObject(b, new byte[] {'+', '1', '2', '3'}, 0, 1);
      assertNull(b.getWritableObject());

      initLazyObject(b, new byte[] {'-', '1', 'e', '3', '3', '3', '3', '3', '3'}, 0, 9);
      assertEquals(new DoubleWritable(Double.NEGATIVE_INFINITY), b
          .getWritableObject());
      initLazyObject(b, new byte[] {'+', '1', 'e', '3', '3', '3', '3', '3', '3'}, 0, 9);
      assertEquals(new DoubleWritable(Double.POSITIVE_INFINITY), b
          .getWritableObject());

      initLazyObject(b, new byte[] {'+', '1', 'e', '-', '3', '3', '3', '3', '3'}, 0, 8);
      assertEquals(new DoubleWritable(0), b.getWritableObject());
      initLazyObject(b, new byte[] {'-', '1', 'e', '-', '3', '3', '3', '3', '3'}, 0, 8);
      assertEquals(new DoubleWritable(-0.0), b.getWritableObject());

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
      LazyString b = new LazyString(LazyPrimitiveObjectInspectorFactory
          .getLazyStringObjectInspector(false, (byte) 0));
      initLazyObject(b, new byte[] {'0'}, 0, 0);
      assertEquals(new Text(""), b.getWritableObject());
      initLazyObject(b, new byte[] {'0'}, 0, 1);
      assertEquals(new Text("0"), b.getWritableObject());
      initLazyObject(b, new byte[] {'0', '1', '2'}, 1, 1);
      assertEquals(new Text("1"), b.getWritableObject());

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void testLazyBinary() {

    LazyBinary ba = new LazyBinary(LazyPrimitiveObjectInspectorFactory.LAZY_BINARY_OBJECT_INSPECTOR);
    initLazyObject(ba, new byte[]{}, 0, 0);
    assertEquals(new BytesWritable(), ba.getWritableObject());
    initLazyObject(ba, new byte[]{'%'}, 0, 1);
    assertEquals(new BytesWritable(new byte[]{'%'}), ba.getWritableObject());
    initLazyObject(ba, new byte[]{'2','>','3'}, 1, 1);
    assertEquals(new BytesWritable(new byte[]{'>'}), ba.getWritableObject());
    initLazyObject(ba, new byte[]{'2','?','3'}, 0, 3);
    assertEquals(new BytesWritable(new byte[]{'2','?','3'}), ba.getWritableObject());
  }

  public void testLazyIntegerWrite() throws Throwable {
    try {
      ByteStream.Output out = new ByteStream.Output();

      int[] tests = {0, -1, 1, -10, 10, -123, 123, Integer.MIN_VALUE,
          Integer.MIN_VALUE + 1, Integer.MAX_VALUE, Integer.MAX_VALUE - 1};
      for (int v : tests) {
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

      long[] tests = {0L, -1, 1, -10, 10, -123, 123, Long.MIN_VALUE,
          Long.MIN_VALUE + 1, Long.MAX_VALUE, Long.MAX_VALUE - 1};
      for (long v : tests) {
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
