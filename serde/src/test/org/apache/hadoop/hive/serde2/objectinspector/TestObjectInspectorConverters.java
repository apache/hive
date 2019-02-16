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
package org.apache.hadoop.hive.serde2.objectinspector;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
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

      // Char
      Converter charConverter = ObjectInspectorConverters.getConverter(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        PrimitiveObjectInspectorFactory.javaHiveCharObjectInspector);
      assertEquals("CharConverter", new HiveChar("TRUE", -1), charConverter
        .convert(Boolean.valueOf(true)));
      assertEquals("CharConverter", new HiveChar("FALSE", -1), charConverter
        .convert(Boolean.valueOf(false)));

      charConverter = ObjectInspectorConverters.getConverter(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        PrimitiveObjectInspectorFactory.writableHiveCharObjectInspector);
      assertEquals("CharConverter", new HiveCharWritable(new HiveChar("TRUE", -1)), charConverter
        .convert(Boolean.valueOf(true)));
      assertEquals("CharConverter", new HiveCharWritable(new HiveChar("FALSE", -1)), charConverter
        .convert(Boolean.valueOf(false)));

      charConverter = ObjectInspectorConverters.getConverter(
        PrimitiveObjectInspectorFactory.javaIntObjectInspector,
        PrimitiveObjectInspectorFactory.javaHiveCharObjectInspector);
      assertEquals("CharConverter", new HiveChar("0", -1), charConverter
        .convert(Integer.valueOf(0)));
      assertEquals("CharConverter", new HiveChar("1", -1), charConverter
        .convert(Integer.valueOf(1)));

      charConverter = ObjectInspectorConverters.getConverter(
        PrimitiveObjectInspectorFactory.javaIntObjectInspector,
        PrimitiveObjectInspectorFactory.writableHiveCharObjectInspector);
      assertEquals("CharConverter", new HiveCharWritable(new HiveChar("0", -1)), charConverter
        .convert(Integer.valueOf(0)));
      assertEquals("CharConverter", new HiveCharWritable(new HiveChar("1", -1)), charConverter
        .convert(Integer.valueOf(1)));

      charConverter = ObjectInspectorConverters.getConverter(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaHiveCharObjectInspector);
      assertEquals("CharConverter", new HiveChar("hive", -1), charConverter
        .convert(String.valueOf("hive")));

      charConverter = ObjectInspectorConverters.getConverter(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.writableHiveCharObjectInspector);
      assertEquals("CharConverter", new HiveCharWritable(new HiveChar("hive", -1)), charConverter
        .convert(String.valueOf("hive")));

      // VarChar
      Converter varcharConverter = ObjectInspectorConverters.getConverter(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        PrimitiveObjectInspectorFactory.javaHiveVarcharObjectInspector);
      assertEquals("VarCharConverter", new HiveVarchar("TRUE", -1), varcharConverter
        .convert(Boolean.valueOf(true)));
      assertEquals("VarCharConverter", new HiveVarchar("FALSE", -1), varcharConverter
        .convert(Boolean.valueOf(false)));

      varcharConverter = ObjectInspectorConverters.getConverter(
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
        PrimitiveObjectInspectorFactory.writableHiveVarcharObjectInspector);
      assertEquals("VarCharConverter", new HiveVarcharWritable(new HiveVarchar("TRUE", -1)), varcharConverter
        .convert(Boolean.valueOf(true)));
      assertEquals("VarCharConverter", new HiveVarcharWritable(new HiveVarchar("FALSE", -1)), varcharConverter
        .convert(Boolean.valueOf(false)));

      varcharConverter = ObjectInspectorConverters.getConverter(
        PrimitiveObjectInspectorFactory.javaIntObjectInspector,
        PrimitiveObjectInspectorFactory.javaHiveVarcharObjectInspector);
      assertEquals("VarCharConverter", new HiveVarchar("0", -1), varcharConverter
        .convert(Integer.valueOf(0)));
      assertEquals("VarCharConverter", new HiveVarchar("1", -1), varcharConverter
        .convert(Integer.valueOf(1)));

      varcharConverter = ObjectInspectorConverters.getConverter(
        PrimitiveObjectInspectorFactory.javaIntObjectInspector,
        PrimitiveObjectInspectorFactory.writableHiveVarcharObjectInspector);
      assertEquals("VarCharConverter", new HiveVarcharWritable(new HiveVarchar("0", -1)), varcharConverter
        .convert(Integer.valueOf(0)));
      assertEquals("VarCharConverter", new HiveVarcharWritable(new HiveVarchar("1", -1)), varcharConverter
        .convert(Integer.valueOf(1)));

      varcharConverter = ObjectInspectorConverters.getConverter(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaHiveVarcharObjectInspector);
      assertEquals("VarCharConverter", new HiveVarchar("hive", -1), varcharConverter
        .convert(String.valueOf("hive")));

      varcharConverter = ObjectInspectorConverters.getConverter(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.writableHiveVarcharObjectInspector);
      assertEquals("VarCharConverter", new HiveVarcharWritable(new HiveVarchar("hive", -1)), varcharConverter
        .convert(String.valueOf("hive")));

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
      assertEquals("TextConverter", new Text("100.001000000000000000"), textConverter
	  .convert(HiveDecimal.create("100.001")));
      assertEquals("TextConverter", null, textConverter.convert(null));

      // Varchar
      PrimitiveTypeInfo varchar5TI =
          (PrimitiveTypeInfo) TypeInfoFactory.getPrimitiveTypeInfo("varchar(5)");
      PrimitiveTypeInfo varchar30TI =
          (PrimitiveTypeInfo) TypeInfoFactory.getPrimitiveTypeInfo("varchar(30)");
      PrimitiveObjectInspector varchar5OI =
          PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(varchar5TI);
      PrimitiveObjectInspector varchar30OI =
          PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(varchar30TI);
      // Value should be truncated to varchar length 5
      varcharConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector,
          varchar5OI);
      assertEquals("VarcharConverter", "100.0",
          varcharConverter.convert(HiveDecimal.create("100.001")).toString());

      varcharConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector,
          varchar30OI);
      assertEquals("VarcharConverter", "100.001000000000000000",
          varcharConverter.convert(HiveDecimal.create("100.001")).toString());

      // Char
      PrimitiveTypeInfo char5TI =
          (PrimitiveTypeInfo) TypeInfoFactory.getPrimitiveTypeInfo("char(5)");
      PrimitiveTypeInfo char30TI =
          (PrimitiveTypeInfo) TypeInfoFactory.getPrimitiveTypeInfo("char(30)");
      PrimitiveObjectInspector char5OI =
          PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(char5TI);
      PrimitiveObjectInspector char30OI =
          PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(char30TI);
      // Value should be truncated to char length 5
      charConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector,
          char5OI);
      assertEquals("CharConverter", "100.0",
          charConverter.convert(HiveDecimal.create("100.001")).toString());
      // Char value should be have space padding to full char length
      charConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector,
          char30OI);
      assertEquals("CharConverter", "100.001000000000000000        ",
          charConverter.convert(HiveDecimal.create("100.001")).toString());

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

      // Union
      ArrayList<String> fieldNames = new ArrayList<String>();
      fieldNames.add("firstInteger");
      fieldNames.add("secondString");
      fieldNames.add("thirdBoolean");
      ArrayList<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>();
      fieldObjectInspectors
          .add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
      fieldObjectInspectors
          .add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      fieldObjectInspectors
          .add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);

      ArrayList<String> fieldNames2 = new ArrayList<String>();
      fieldNames2.add("firstString");
      fieldNames2.add("secondInteger");
      fieldNames2.add("thirdBoolean");
      ArrayList<ObjectInspector> fieldObjectInspectors2 = new ArrayList<ObjectInspector>();
      fieldObjectInspectors2
          .add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      fieldObjectInspectors2
          .add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
      fieldObjectInspectors2
          .add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);

      Converter unionConverter0 = ObjectInspectorConverters.getConverter(ObjectInspectorFactory.getStandardUnionObjectInspector(fieldObjectInspectors),
          ObjectInspectorFactory.getStandardUnionObjectInspector(fieldObjectInspectors2));

      Object convertedObject0 = unionConverter0.convert(new StandardUnion((byte)0, 1));
      StandardUnion expectedObject0 = new StandardUnion();
      expectedObject0.setTag((byte) 0);
      expectedObject0.setObject("1");

      assertEquals(expectedObject0, convertedObject0);

      Converter unionConverter1 = ObjectInspectorConverters.getConverter(ObjectInspectorFactory.getStandardUnionObjectInspector(fieldObjectInspectors),
		  ObjectInspectorFactory.getStandardUnionObjectInspector(fieldObjectInspectors2));

      Object convertedObject1 = unionConverter1.convert(new StandardUnion((byte)1, "1"));
      StandardUnion expectedObject1 = new StandardUnion();
      expectedObject1.setTag((byte) 1);
      expectedObject1.setObject(1);

      assertEquals(expectedObject1, convertedObject1);

      Converter unionConverter2 = ObjectInspectorConverters.getConverter(ObjectInspectorFactory.getStandardUnionObjectInspector(fieldObjectInspectors),
          ObjectInspectorFactory.getStandardUnionObjectInspector(fieldObjectInspectors2));

      Object convertedObject2 = unionConverter2.convert(new StandardUnion((byte)2, true));
      StandardUnion expectedObject2 = new StandardUnion();
      expectedObject2.setTag((byte) 2);
      expectedObject2.setObject(true);

      assertEquals(expectedObject2, convertedObject2);

      // Union (extra fields)
      ArrayList<String> fieldNamesExtra = new ArrayList<String>();
      fieldNamesExtra.add("firstInteger");
      fieldNamesExtra.add("secondString");
      fieldNamesExtra.add("thirdBoolean");
      ArrayList<ObjectInspector> fieldObjectInspectorsExtra = new ArrayList<ObjectInspector>();
      fieldObjectInspectorsExtra
          .add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
      fieldObjectInspectorsExtra
          .add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      fieldObjectInspectorsExtra
          .add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);

      ArrayList<String> fieldNamesExtra2 = new ArrayList<String>();
      fieldNamesExtra2.add("firstString");
      fieldNamesExtra2.add("secondInteger");
      ArrayList<ObjectInspector> fieldObjectInspectorsExtra2 = new ArrayList<ObjectInspector>();
      fieldObjectInspectorsExtra2
          .add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      fieldObjectInspectorsExtra2
          .add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);

      Converter unionConverterExtra = ObjectInspectorConverters.getConverter(ObjectInspectorFactory.getStandardUnionObjectInspector(fieldObjectInspectorsExtra),
          ObjectInspectorFactory.getStandardUnionObjectInspector(fieldObjectInspectorsExtra2));

      Object convertedObjectExtra = unionConverterExtra.convert(new StandardUnion((byte)2, true));
      StandardUnion expectedObjectExtra = new StandardUnion();
      expectedObjectExtra.setTag((byte) -1);
      expectedObjectExtra.setObject(null);

      assertEquals(expectedObjectExtra, convertedObjectExtra); // we should get back null

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