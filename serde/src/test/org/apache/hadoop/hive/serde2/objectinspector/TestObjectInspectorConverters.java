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
package org.apache.hadoop.hive.serde2.objectinspector;

import junit.framework.TestCase;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * TestObjectInspectorConverters.
 *
 */
public class TestObjectInspectorConverters extends TestCase {

  public void testObjectInspectorConverters() throws Throwable {
    try {
      // Boolean
      Converter booleanConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaIntObjectInspector,
          PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
      assertEquals("BooleanConverter", new BooleanWritable(false),
          booleanConverter.convert(Integer.valueOf(0)));
      assertEquals("BooleanConverter", new BooleanWritable(true),
          booleanConverter.convert(Integer.valueOf(1)));
      assertEquals("BooleanConverter", null, booleanConverter.convert(null));

      // Byte
      Converter byteConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaIntObjectInspector,
          PrimitiveObjectInspectorFactory.writableByteObjectInspector);
      assertEquals("ByteConverter", new ByteWritable((byte) 0), byteConverter
          .convert(Integer.valueOf(0)));
      assertEquals("ByteConverter", new ByteWritable((byte) 1), byteConverter
          .convert(Integer.valueOf(1)));
      assertEquals("ByteConverter", null, byteConverter.convert(null));

      // Short
      Converter shortConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaIntObjectInspector,
          PrimitiveObjectInspectorFactory.writableShortObjectInspector);
      assertEquals("ShortConverter", new ShortWritable((short) 0),
          shortConverter.convert(Integer.valueOf(0)));
      assertEquals("ShortConverter", new ShortWritable((short) 1),
          shortConverter.convert(Integer.valueOf(1)));
      assertEquals("ShortConverter", null, shortConverter.convert(null));

      // Int
      Converter intConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaIntObjectInspector,
          PrimitiveObjectInspectorFactory.writableIntObjectInspector);
      assertEquals("IntConverter", new IntWritable(0), intConverter
          .convert(Integer.valueOf(0)));
      assertEquals("IntConverter", new IntWritable(1), intConverter
          .convert(Integer.valueOf(1)));
      assertEquals("IntConverter", null, intConverter.convert(null));

      // Long
      Converter longConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaIntObjectInspector,
          PrimitiveObjectInspectorFactory.writableLongObjectInspector);
      assertEquals("LongConverter", new LongWritable(0), longConverter
          .convert(Integer.valueOf(0)));
      assertEquals("LongConverter", new LongWritable(1), longConverter
          .convert(Integer.valueOf(1)));
      assertEquals("LongConverter", null, longConverter.convert(null));

      // Float
      Converter floatConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaIntObjectInspector,
          PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
      assertEquals("LongConverter", new FloatWritable(0), floatConverter
          .convert(Integer.valueOf(0)));
      assertEquals("LongConverter", new FloatWritable(1), floatConverter
          .convert(Integer.valueOf(1)));
      assertEquals("LongConverter", null, floatConverter.convert(null));

      // Double
      Converter doubleConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaIntObjectInspector,
          PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
      assertEquals("DoubleConverter", new DoubleWritable(0), doubleConverter
          .convert(Integer.valueOf(0)));
      assertEquals("DoubleConverter", new DoubleWritable(1), doubleConverter
          .convert(Integer.valueOf(1)));
      assertEquals("DoubleConverter", null, doubleConverter.convert(null));

      // Text
      Converter textConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaIntObjectInspector,
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      assertEquals("TextConverter", new Text("0"), textConverter
          .convert(Integer.valueOf(0)));
      assertEquals("TextConverter", new Text("1"), textConverter
          .convert(Integer.valueOf(1)));
      assertEquals("TextConverter", null, textConverter.convert(null));

      textConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      assertEquals("TextConverter", new Text("hive"), textConverter
          .convert(new BytesWritable(new byte[]
              {(byte)'h', (byte)'i',(byte)'v',(byte)'e'})));
      assertEquals("TextConverter", null, textConverter.convert(null));

      textConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.writableStringObjectInspector,
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      assertEquals("TextConverter", new Text("hive"), textConverter
	  .convert(new Text("hive")));
      assertEquals("TextConverter", null, textConverter.convert(null));

      textConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector,
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      assertEquals("TextConverter", new Text("hive"), textConverter
	  .convert(new String("hive")));
      assertEquals("TextConverter", null, textConverter.convert(null));

      textConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector,
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      assertEquals("TextConverter", new Text("100.001"), textConverter
	  .convert(HiveDecimal.create("100.001")));
      assertEquals("TextConverter", null, textConverter.convert(null));

      // Binary
      Converter baConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector,
          PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);
      assertEquals("BAConverter", new BytesWritable(new byte[]
          {(byte)'h', (byte)'i',(byte)'v',(byte)'e'}),
          baConverter.convert("hive"));
      assertEquals("BAConverter", null, baConverter.convert(null));

      baConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.writableStringObjectInspector,
          PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);
      assertEquals("BAConverter", new BytesWritable(new byte[]
          {(byte)'h', (byte)'i',(byte)'v',(byte)'e'}),
          baConverter.convert(new Text("hive")));
      assertEquals("BAConverter", null, baConverter.convert(null));
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }

  }

  public void testGetConvertedOI() throws Throwable {
    // Try with types that have type params
    PrimitiveTypeInfo varchar5TI =
        (PrimitiveTypeInfo) TypeInfoFactory.getPrimitiveTypeInfo("varchar(5)");
    PrimitiveTypeInfo varchar10TI =
        (PrimitiveTypeInfo) TypeInfoFactory.getPrimitiveTypeInfo("varchar(10)");
    PrimitiveObjectInspector varchar5OI =
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(varchar5TI);
    PrimitiveObjectInspector varchar10OI =
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(varchar10TI);

    // output OI should have varchar type params
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector)
        ObjectInspectorConverters.getConvertedOI(varchar10OI, varchar5OI);
    VarcharTypeInfo vcParams = (VarcharTypeInfo) poi.getTypeInfo();
    assertEquals("varchar length doesn't match", 5, vcParams.getLength());
  }
}
