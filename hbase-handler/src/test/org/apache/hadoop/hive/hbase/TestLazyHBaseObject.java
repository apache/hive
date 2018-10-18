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

package org.apache.hadoop.hive.hbase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.hbase.ColumnMappings.ColumnMapping;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * TestLazyHBaseObject is a test for the LazyHBaseXXX classes.
 */
public class TestLazyHBaseObject extends TestCase {
  /**
   * Test the LazyMap class with Integer-to-String.
   * @throws SerDeException
   */
  public void testLazyHBaseCellMap1() throws SerDeException {
    // Map of Integer to String
    Text nullSequence = new Text("\\N");
    ObjectInspector oi = LazyFactory.createLazyObjectInspector(
      TypeInfoUtils.getTypeInfosFromTypeString("map<int,string>").get(0),
      new byte[]{(byte)1, (byte)2}, 0, nullSequence, false, (byte)0);

    LazyHBaseCellMap b = new LazyHBaseCellMap((LazyMapObjectInspector) oi);

    // Initialize a result
    List<Cell> kvs = new ArrayList<Cell>();

    kvs.add(new KeyValue(Bytes.toBytes("test-row"), Bytes.toBytes("cfa"),
        Bytes.toBytes("col1"), Bytes.toBytes("cfacol1")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"), Bytes.toBytes("cfa"),
        Bytes.toBytes("col2"), Bytes.toBytes("cfacol2")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"), Bytes.toBytes("cfb"),
        Bytes.toBytes("2"), Bytes.toBytes("def")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"), Bytes.toBytes("cfb"),
        Bytes.toBytes("-1"), Bytes.toBytes("")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"), Bytes.toBytes("cfb"),
        Bytes.toBytes("0"), Bytes.toBytes("0")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"), Bytes.toBytes("cfb"),
        Bytes.toBytes("8"), Bytes.toBytes("abc")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"), Bytes.toBytes("cfc"),
        Bytes.toBytes("col3"), Bytes.toBytes("cfccol3")));

    Result r = Result.create(kvs);

    List<Boolean> mapBinaryStorage = new ArrayList<Boolean>();
    mapBinaryStorage.add(false);
    mapBinaryStorage.add(false);

    b.init(r, "cfb".getBytes(), mapBinaryStorage);

    assertEquals(
      new Text("def"),
      ((LazyString)b.getMapValueElement(
        new IntWritable(2))).getWritableObject());

    assertNull(b.getMapValueElement(new IntWritable(-1)));

    assertEquals(
      new Text("0"),
      ((LazyString)b.getMapValueElement(
        new IntWritable(0))).getWritableObject());

    assertEquals(
      new Text("abc"),
      ((LazyString)b.getMapValueElement(
        new IntWritable(8))).getWritableObject());

    assertNull(b.getMapValueElement(new IntWritable(12345)));

    assertEquals("{0:'0',2:'def',8:'abc'}".replace('\'', '\"'),
      SerDeUtils.getJSONString(b, oi));
  }

  /**
   * Test the LazyMap class with String-to-String.
   * @throws SerDeException
   */
  public void testLazyHBaseCellMap2() throws SerDeException {
    // Map of String to String
    Text nullSequence = new Text("\\N");
    ObjectInspector oi = LazyFactory.createLazyObjectInspector(
      TypeInfoUtils.getTypeInfosFromTypeString("map<string,string>").get(0),
      new byte[]{(byte)'#', (byte)'\t'}, 0, nullSequence, false, (byte)0);

    LazyHBaseCellMap b = new LazyHBaseCellMap((LazyMapObjectInspector) oi);

    // Initialize a result
    List<Cell> kvs = new ArrayList<Cell>();

    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfa"), Bytes.toBytes("col1"), Bytes.toBytes("cfacol1")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfa"), Bytes.toBytes("col2"), Bytes.toBytes("cfacol2")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfb"), Bytes.toBytes("2"), Bytes.toBytes("d\tf")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfb"), Bytes.toBytes("-1"), Bytes.toBytes("")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfb"), Bytes.toBytes("0"), Bytes.toBytes("0")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfb"), Bytes.toBytes("8"), Bytes.toBytes("abc")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfc"), Bytes.toBytes("col3"), Bytes.toBytes("cfccol3")));

    Result r = Result.create(kvs);
    List<Boolean> mapBinaryStorage = new ArrayList<Boolean>();
    mapBinaryStorage.add(false);
    mapBinaryStorage.add(false);

    b.init(r, "cfb".getBytes(), mapBinaryStorage);

    assertEquals(
      new Text("d\tf"),
      ((LazyString)b.getMapValueElement(
        new Text("2"))).getWritableObject());

    assertNull(b.getMapValueElement(new Text("-1")));

    assertEquals(
      new Text("0"),
      ((LazyString)b.getMapValueElement(
        new Text("0"))).getWritableObject());

    assertEquals(
      new Text("abc"),
      ((LazyString)b.getMapValueElement(
        new Text("8"))).getWritableObject());

    assertNull(b.getMapValueElement(new Text("-")));

    assertEquals(
      "{'0':'0','2':'d\\tf','8':'abc'}".replace('\'', '\"'),
      SerDeUtils.getJSONString(b, oi));
  }

  /**
   * Test the LazyHBaseCellMap class for the case where both the key and the value in the family
   * map are stored in binary format using the appropriate LazyPrimitive objects.
   * @throws SerDeException
   */
  public void testLazyHBaseCellMap3() throws SerDeException {

    Text nullSequence = new Text("\\N");
    TypeInfo mapBinaryIntKeyValue = TypeInfoUtils.getTypeInfoFromTypeString("map<int,int>");
    ObjectInspector oi = LazyFactory.createLazyObjectInspector(
        mapBinaryIntKeyValue, new byte [] {(byte)1, (byte) 2}, 0, nullSequence, false, (byte) 0);
    LazyHBaseCellMap hbaseCellMap = new LazyHBaseCellMap((LazyMapObjectInspector) oi);

    List<Cell> kvs = new ArrayList<Cell>();
    byte [] rowKey = "row-key".getBytes();
    byte [] cfInt = "cf-int".getBytes();
    kvs.add(new KeyValue(rowKey, cfInt, Bytes.toBytes(1), Bytes.toBytes(1)));
    Result result = Result.create(kvs);
    List<Boolean> mapBinaryStorage = new ArrayList<Boolean>();
    mapBinaryStorage.add(true);
    mapBinaryStorage.add(true);
    hbaseCellMap.init(result, cfInt, mapBinaryStorage);
    IntWritable expectedIntValue = new IntWritable(1);
    LazyPrimitive<?, ?> lazyPrimitive =
      (LazyPrimitive<?, ?>) hbaseCellMap.getMapValueElement(expectedIntValue);

    assertEquals(expectedIntValue, lazyPrimitive.getWritableObject());

    kvs.clear();
    kvs.add(new KeyValue(
        rowKey, cfInt, Bytes.toBytes(Integer.MIN_VALUE), Bytes.toBytes(Integer.MIN_VALUE)));
    result = Result.create(kvs);
    hbaseCellMap.init(result, cfInt, mapBinaryStorage);
    expectedIntValue = new IntWritable(Integer.MIN_VALUE);
    lazyPrimitive =
      (LazyPrimitive<?, ?>) hbaseCellMap.getMapValueElement(expectedIntValue);

    assertEquals(expectedIntValue, lazyPrimitive.getWritableObject());

    kvs.clear();
    kvs.add(new KeyValue(
        rowKey, cfInt, Bytes.toBytes(Integer.MAX_VALUE), Bytes.toBytes(Integer.MAX_VALUE)));
    result = Result.create(kvs);
    hbaseCellMap.init(result, cfInt, mapBinaryStorage);
    expectedIntValue = new IntWritable(Integer.MAX_VALUE);
    lazyPrimitive =
      (LazyPrimitive<?, ?>) hbaseCellMap.getMapValueElement(expectedIntValue);

    assertEquals(expectedIntValue, lazyPrimitive.getWritableObject());

    TypeInfo mapBinaryByteKeyValue =
      TypeInfoUtils.getTypeInfoFromTypeString("map<tinyint,tinyint>");
    oi = LazyFactory.createLazyObjectInspector(
        mapBinaryByteKeyValue, new byte [] {(byte) 1, (byte) 2}, 0, nullSequence, false, (byte) 0);
    hbaseCellMap = new LazyHBaseCellMap((LazyMapObjectInspector) oi);
    byte [] cfByte = "cf-byte".getBytes();
    kvs.clear();
    kvs.add(new KeyValue(rowKey, cfByte, new byte [] {(byte) 1}, new byte [] {(byte) 1}));
    result = Result.create(kvs);
    hbaseCellMap.init(result, cfByte, mapBinaryStorage);
    ByteWritable expectedByteValue = new ByteWritable((byte) 1);
    lazyPrimitive =
      (LazyPrimitive<?, ?>) hbaseCellMap.getMapValueElement(expectedByteValue);

    assertEquals(expectedByteValue, lazyPrimitive.getWritableObject());

    kvs.clear();
    kvs.add(new KeyValue(rowKey, cfByte, new byte [] {Byte.MIN_VALUE},
      new byte [] {Byte.MIN_VALUE}));
    result = Result.create(kvs);
    hbaseCellMap.init(result, cfByte, mapBinaryStorage);
    expectedByteValue = new ByteWritable(Byte.MIN_VALUE);
    lazyPrimitive =
      (LazyPrimitive<?, ?>) hbaseCellMap.getMapValueElement(expectedByteValue);

    assertEquals(expectedByteValue, lazyPrimitive.getWritableObject());

    kvs.clear();
    kvs.add(new KeyValue(rowKey, cfByte, new byte [] {Byte.MAX_VALUE},
      new byte [] {Byte.MAX_VALUE}));
    result = Result.create(kvs);
    hbaseCellMap.init(result, cfByte, mapBinaryStorage);
    expectedByteValue = new ByteWritable(Byte.MAX_VALUE);
    lazyPrimitive =
      (LazyPrimitive<?, ?>) hbaseCellMap.getMapValueElement(expectedByteValue);

    assertEquals(expectedByteValue, lazyPrimitive.getWritableObject());

    TypeInfo mapBinaryShortKeyValue =
      TypeInfoUtils.getTypeInfoFromTypeString("map<smallint,smallint>");
    oi = LazyFactory.createLazyObjectInspector(
      mapBinaryShortKeyValue, new byte [] {(byte) 1, (byte) 2}, 0, nullSequence, false, (byte) 0);
    hbaseCellMap = new LazyHBaseCellMap((LazyMapObjectInspector) oi);
    byte [] cfShort = "cf-short".getBytes();
    kvs.clear();
    kvs.add(new KeyValue(rowKey, cfShort, Bytes.toBytes((short) 1), Bytes.toBytes((short) 1)));
    result = Result.create(kvs);
    hbaseCellMap.init(result, cfShort, mapBinaryStorage);
    ShortWritable expectedShortValue = new ShortWritable((short) 1);
    lazyPrimitive =
      (LazyPrimitive<?, ?>) hbaseCellMap.getMapValueElement(expectedShortValue);

    assertEquals(expectedShortValue, lazyPrimitive.getWritableObject());

    kvs.clear();
    kvs.add(new KeyValue(rowKey, cfShort, Bytes.toBytes(Short.MIN_VALUE),
      Bytes.toBytes(Short.MIN_VALUE)));
    result = Result.create(kvs);
    hbaseCellMap.init(result, cfShort, mapBinaryStorage);
    expectedShortValue = new ShortWritable(Short.MIN_VALUE);
    lazyPrimitive =
      (LazyPrimitive<?, ?>) hbaseCellMap.getMapValueElement(expectedShortValue);

    assertEquals(expectedShortValue, lazyPrimitive.getWritableObject());

    kvs.clear();
    kvs.add(new KeyValue(rowKey, cfShort, Bytes.toBytes(Short.MAX_VALUE),
      Bytes.toBytes(Short.MAX_VALUE)));
    result = Result.create(kvs);
    hbaseCellMap.init(result, cfShort, mapBinaryStorage);
    expectedShortValue = new ShortWritable(Short.MAX_VALUE);
    lazyPrimitive =
      (LazyPrimitive<?, ?>) hbaseCellMap.getMapValueElement(expectedShortValue);

    assertEquals(expectedShortValue, lazyPrimitive.getWritableObject());

    TypeInfo mapBinaryLongKeyValue =
      TypeInfoUtils.getTypeInfoFromTypeString("map<bigint,bigint>");
    oi = LazyFactory.createLazyObjectInspector(
        mapBinaryLongKeyValue, new byte [] {(byte) 1, (byte) 2}, 0, nullSequence, false, (byte) 0);
    hbaseCellMap = new LazyHBaseCellMap((LazyMapObjectInspector) oi);
    byte [] cfLong = "cf-long".getBytes();
    kvs.clear();
    kvs.add(new KeyValue(rowKey, cfLong, Bytes.toBytes((long) 1), Bytes.toBytes((long) 1)));
    result = Result.create(kvs);
    hbaseCellMap.init(result, cfLong, mapBinaryStorage);
    LongWritable expectedLongValue = new LongWritable(1);
    lazyPrimitive =
      (LazyPrimitive<?, ?>) hbaseCellMap.getMapValueElement(expectedLongValue);

    assertEquals(expectedLongValue, lazyPrimitive.getWritableObject());

    kvs.clear();
    kvs.add(new KeyValue(rowKey, cfLong, Bytes.toBytes(Long.MIN_VALUE),
      Bytes.toBytes(Long.MIN_VALUE)));
    result = Result.create(kvs);
    hbaseCellMap.init(result, cfLong, mapBinaryStorage);
    expectedLongValue = new LongWritable(Long.MIN_VALUE);
    lazyPrimitive =
      (LazyPrimitive<?, ?>) hbaseCellMap.getMapValueElement(expectedLongValue);

    assertEquals(expectedLongValue, lazyPrimitive.getWritableObject());

    kvs.clear();
    kvs.add(new KeyValue(rowKey, cfLong, Bytes.toBytes(Long.MAX_VALUE),
      Bytes.toBytes(Long.MAX_VALUE)));
    result = Result.create(kvs);
    hbaseCellMap.init(result, cfLong, mapBinaryStorage);
    expectedLongValue = new LongWritable(Long.MAX_VALUE);
    lazyPrimitive =
      (LazyPrimitive<?, ?>) hbaseCellMap.getMapValueElement(expectedLongValue);

    assertEquals(expectedLongValue, lazyPrimitive.getWritableObject());

    TypeInfo mapBinaryFloatKeyValue =
      TypeInfoUtils.getTypeInfoFromTypeString("map<float,float>");
    oi = LazyFactory.createLazyObjectInspector(
        mapBinaryFloatKeyValue, new byte [] {(byte) 1, (byte) 2}, 0, nullSequence, false,
        (byte) 0);
    hbaseCellMap = new LazyHBaseCellMap((LazyMapObjectInspector) oi);
    byte [] cfFloat = "cf-float".getBytes();
    kvs.clear();
    kvs.add(new KeyValue(rowKey, cfFloat, Bytes.toBytes((float) 1.0F),
      Bytes.toBytes((float) 1.0F)));
    result = Result.create(kvs);
    hbaseCellMap.init(result, cfFloat, mapBinaryStorage);
    FloatWritable expectedFloatValue = new FloatWritable(1.0F);
    lazyPrimitive =
      (LazyPrimitive<?, ?>) hbaseCellMap.getMapValueElement(expectedFloatValue);

    assertEquals(expectedFloatValue, lazyPrimitive.getWritableObject());

    kvs.clear();
    kvs.add(new KeyValue(rowKey, cfFloat, Bytes.toBytes((float) Float.MIN_VALUE),
      Bytes.toBytes((float) Float.MIN_VALUE)));
    result = Result.create(kvs);
    hbaseCellMap.init(result, cfFloat, mapBinaryStorage);
    expectedFloatValue = new FloatWritable(Float.MIN_VALUE);
    lazyPrimitive =
      (LazyPrimitive<?, ?>) hbaseCellMap.getMapValueElement(expectedFloatValue);

    assertEquals(expectedFloatValue, lazyPrimitive.getWritableObject());

    kvs.clear();
    kvs.add(new KeyValue(rowKey, cfFloat, Bytes.toBytes((float) Float.MAX_VALUE),
      Bytes.toBytes((float) Float.MAX_VALUE)));
    result = Result.create(kvs);
    hbaseCellMap.init(result, cfFloat, mapBinaryStorage);
    expectedFloatValue = new FloatWritable(Float.MAX_VALUE);
    lazyPrimitive =
      (LazyPrimitive<?, ?>) hbaseCellMap.getMapValueElement(expectedFloatValue);

    assertEquals(expectedFloatValue, lazyPrimitive.getWritableObject());

    TypeInfo mapBinaryDoubleKeyValue =
      TypeInfoUtils.getTypeInfoFromTypeString("map<double,double>");
    oi = LazyFactory.createLazyObjectInspector(
        mapBinaryDoubleKeyValue, new byte [] {(byte) 1, (byte) 2}, 0, nullSequence, false,
        (byte) 0);
    hbaseCellMap = new LazyHBaseCellMap((LazyMapObjectInspector) oi);
    byte [] cfDouble = "cf-double".getBytes();
    kvs.clear();
    kvs.add(new KeyValue(rowKey, cfDouble, Bytes.toBytes(1.0), Bytes.toBytes(1.0)));
    result = Result.create(kvs);
    hbaseCellMap.init(result, cfDouble, mapBinaryStorage);
    DoubleWritable expectedDoubleValue = new DoubleWritable(1.0);
    lazyPrimitive =
      (LazyPrimitive<?, ?>) hbaseCellMap.getMapValueElement(expectedDoubleValue);

    assertEquals(expectedDoubleValue, lazyPrimitive.getWritableObject());

    kvs.clear();
    kvs.add(new KeyValue(rowKey, cfDouble, Bytes.toBytes(Double.MIN_VALUE),
      Bytes.toBytes(Double.MIN_VALUE)));
    result = Result.create(kvs);
    hbaseCellMap.init(result, cfDouble, mapBinaryStorage);
    expectedDoubleValue = new DoubleWritable(Double.MIN_VALUE);
    lazyPrimitive =
      (LazyPrimitive<?, ?>) hbaseCellMap.getMapValueElement(expectedDoubleValue);

    assertEquals(expectedDoubleValue, lazyPrimitive.getWritableObject());

    kvs.clear();
    kvs.add(new KeyValue(rowKey, cfDouble, Bytes.toBytes(Double.MAX_VALUE),
      Bytes.toBytes(Double.MAX_VALUE)));
    result = Result.create(kvs);
    hbaseCellMap.init(result, cfDouble, mapBinaryStorage);
    expectedDoubleValue = new DoubleWritable(Double.MAX_VALUE);
    lazyPrimitive =
      (LazyPrimitive<?, ?>) hbaseCellMap.getMapValueElement(expectedDoubleValue);

    assertEquals(expectedDoubleValue, lazyPrimitive.getWritableObject());

    TypeInfo mapBinaryBooleanKeyValue =
      TypeInfoUtils.getTypeInfoFromTypeString("map<boolean,boolean>");
    oi = LazyFactory.createLazyObjectInspector(
        mapBinaryBooleanKeyValue, new byte [] {(byte) 1, (byte) 2}, 0, nullSequence, false,
        (byte) 0);
    hbaseCellMap = new LazyHBaseCellMap((LazyMapObjectInspector) oi);
    byte [] cfBoolean = "cf-boolean".getBytes();
    kvs.clear();
    kvs.add(new KeyValue(rowKey, cfBoolean, Bytes.toBytes(false), Bytes.toBytes(false)));
    result = Result.create(kvs);
    hbaseCellMap.init(result, cfBoolean, mapBinaryStorage);
    BooleanWritable expectedBooleanValue = new BooleanWritable(false);
    lazyPrimitive =
      (LazyPrimitive<?, ?>) hbaseCellMap.getMapValueElement(expectedBooleanValue);

    assertEquals(expectedBooleanValue, lazyPrimitive.getWritableObject());

    kvs.clear();
    kvs.add(new KeyValue(rowKey, cfBoolean, Bytes.toBytes(true), Bytes.toBytes(true)));
    result = Result.create(kvs);
    hbaseCellMap.init(result, cfBoolean, mapBinaryStorage);
    expectedBooleanValue = new BooleanWritable(true);
    lazyPrimitive =
      (LazyPrimitive<?, ?>) hbaseCellMap.getMapValueElement(expectedBooleanValue);

    assertEquals(expectedBooleanValue, lazyPrimitive.getWritableObject());
  }

  /**
   * Test the LazyHBaseRow class with one-for-one mappings between
   * Hive fields and HBase columns.
   * @throws SerDeException
   */
  public void testLazyHBaseRow1() throws SerDeException {
    List<TypeInfo> fieldTypeInfos =
      TypeInfoUtils.getTypeInfosFromTypeString(
          "string,int,array<string>,map<string,string>,string");
    List<String> fieldNames = Arrays.asList("key", "a", "b", "c", "d");
    Text nullSequence = new Text("\\N");

    String hbaseColsMapping = ":key,cfa:a,cfa:b,cfb:c,cfb:d";
    ColumnMappings columnMappings = null;

    try {
      columnMappings = HBaseSerDe.parseColumnsMapping(hbaseColsMapping);
    } catch (SerDeException e) {
      fail(e.toString());
    }

    for (ColumnMapping colMap : columnMappings) {
      if (!colMap.hbaseRowKey && colMap.qualifierName == null) {
        colMap.binaryStorage.add(false);
        colMap.binaryStorage.add(false);
      } else {
        colMap.binaryStorage.add(false);
      }
    }

    ObjectInspector oi = LazyFactory.createLazyStructInspector(fieldNames,
      fieldTypeInfos, new byte[] {' ', ':', '='},
      nullSequence, false, false, (byte)0);
    LazyHBaseRow o = new LazyHBaseRow((LazySimpleStructObjectInspector) oi, columnMappings);

    List<Cell> kvs = new ArrayList<Cell>();

    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfa"), Bytes.toBytes("a"), Bytes.toBytes("123")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfa"), Bytes.toBytes("b"), Bytes.toBytes("a:b:c")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfb"), Bytes.toBytes("c"), Bytes.toBytes("d=e:f=g")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfb"), Bytes.toBytes("d"), Bytes.toBytes("hi")));

    Result r = Result.create(kvs);
    o.init(r);

    assertEquals(
      ("{'key':'test-row','a':123,'b':['a','b','c'],"
        + "'c':{'d':'e','f':'g'},'d':'hi'}").replace("'", "\""),
      SerDeUtils.getJSONString(o, oi));

    kvs.clear();
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfa"), Bytes.toBytes("a"), Bytes.toBytes("123")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfb"), Bytes.toBytes("c"), Bytes.toBytes("d=e:f=g")));

    r = Result.create(kvs);
    o.init(r);

    assertEquals(
        ("{'key':'test-row','a':123,'b':null,"
          + "'c':{'d':'e','f':'g'},'d':null}").replace("'", "\""),
        SerDeUtils.getJSONString(o, oi));

    kvs.clear();
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfa"), Bytes.toBytes("b"), Bytes.toBytes("a")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfb"), Bytes.toBytes("c"), Bytes.toBytes("d=\\N:f=g:h")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfb"), Bytes.toBytes("d"), Bytes.toBytes("no")));

    r = Result.create(kvs);
    o.init(r);

    assertEquals(
        ("{'key':'test-row','a':null,'b':['a'],"
          + "'c':{'d':null,'f':'g','h':null},'d':'no'}").replace("'", "\""),
        SerDeUtils.getJSONString(o, oi));

    kvs.clear();
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfa"), Bytes.toBytes("b"), Bytes.toBytes(":a::")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfb"), Bytes.toBytes("d"), Bytes.toBytes("no")));

    r = Result.create(kvs);
    o.init(r);

    assertEquals(
      ("{'key':'test-row','a':null,'b':['','a','',''],"
        + "'c':null,'d':'no'}").replace("'", "\""),
      SerDeUtils.getJSONString(o, oi));

    // This is intentionally duplicated because of HIVE-3179
    assertEquals(
      ("{'key':'test-row','a':null,'b':['','a','',''],"
       + "'c':null,'d':'no'}").replace("'", "\""),
      SerDeUtils.getJSONString(o, oi));

    kvs.clear();
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfa"), Bytes.toBytes("a"), Bytes.toBytes("123")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfa"), Bytes.toBytes("b"), Bytes.toBytes("")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfb"), Bytes.toBytes("c"), Bytes.toBytes("")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfb"), Bytes.toBytes("d"), Bytes.toBytes("")));

    r = Result.create(kvs);
    o.init(r);

    assertEquals(
      "{'key':'test-row','a':123,'b':[],'c':{},'d':''}".replace("'", "\""),
      SerDeUtils.getJSONString(o, oi));
  }

  /**
   * Test the LazyHBaseRow class with a mapping from a Hive field to
   * an HBase column family.
   * @throws SerDeException
   */
  public void testLazyHBaseRow2() throws SerDeException {
    // column family is mapped to Map<string,string>
    List<TypeInfo> fieldTypeInfos =
      TypeInfoUtils.getTypeInfosFromTypeString(
        "string,int,array<string>,map<string,string>,string");
    List<String> fieldNames = Arrays.asList(
      new String[]{"key", "a", "b", "c", "d"});
    Text nullSequence = new Text("\\N");
    String hbaseColsMapping = ":key,cfa:a,cfa:b,cfb:,cfc:d";

    ColumnMappings columnMappings = null;

    try {
      columnMappings = HBaseSerDe.parseColumnsMapping(hbaseColsMapping);
    } catch (SerDeException e) {
      fail(e.toString());
    }

    for (ColumnMapping colMap : columnMappings) {
      if (!colMap.hbaseRowKey && colMap.qualifierName == null) {
        colMap.binaryStorage.add(false);
        colMap.binaryStorage.add(false);
      } else {
        colMap.binaryStorage.add(false);
      }
    }

    ObjectInspector oi = LazyFactory.createLazyStructInspector(
      fieldNames,
      fieldTypeInfos,
      new byte[] {' ', ':', '='},
      nullSequence, false, false, (byte) 0);
    LazyHBaseRow o = new LazyHBaseRow((LazySimpleStructObjectInspector) oi, columnMappings);

    List<Cell> kvs = new ArrayList<Cell>();
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfa"), Bytes.toBytes("a"), Bytes.toBytes("123")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfa"), Bytes.toBytes("b"), Bytes.toBytes("a:b:c")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfb"), Bytes.toBytes("d"), Bytes.toBytes("e")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfb"), Bytes.toBytes("f"), Bytes.toBytes("g")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfc"), Bytes.toBytes("d"), Bytes.toBytes("hi")));

    Result r = Result.create(kvs);
    o.init(r);

    assertEquals(
      ("{'key':'test-row','a':123,'b':['a','b','c'],"
        + "'c':{'d':'e','f':'g'},'d':'hi'}").replace("'", "\""),
      SerDeUtils.getJSONString(o, oi));

    kvs.clear();
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfa"), Bytes.toBytes("a"), Bytes.toBytes("123")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfb"), Bytes.toBytes("d"), Bytes.toBytes("e")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfb"), Bytes.toBytes("f"), Bytes.toBytes("g")));

    r = Result.create(kvs);
    o.init(r);

    assertEquals(
      ("{'key':'test-row','a':123,'b':null,"
        + "'c':{'d':'e','f':'g'},'d':null}").replace("'", "\""),
      SerDeUtils.getJSONString(o, oi));

    kvs.clear();
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfa"), Bytes.toBytes("b"), Bytes.toBytes("a")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfb"), Bytes.toBytes("f"), Bytes.toBytes("g")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfc"), Bytes.toBytes("d"), Bytes.toBytes("no")));

    r = Result.create(kvs);
    o.init(r);

    assertEquals(
      ("{'key':'test-row','a':null,'b':['a'],"
        + "'c':{'f':'g'},'d':'no'}").replace("'", "\""),
      SerDeUtils.getJSONString(o, oi));

    kvs.clear();
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfa"), Bytes.toBytes("b"), Bytes.toBytes(":a::")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfc"), Bytes.toBytes("d"), Bytes.toBytes("no")));

    r = Result.create(kvs);
    o.init(r);

    assertEquals(
      ("{'key':'test-row','a':null,'b':['','a','',''],"
        + "'c':{},'d':'no'}").replace("'", "\""),
      SerDeUtils.getJSONString(o, oi));

    kvs.clear();
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfa"), Bytes.toBytes("a"), Bytes.toBytes("123")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfa"), Bytes.toBytes("b"), Bytes.toBytes("")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfc"), Bytes.toBytes("d"), Bytes.toBytes("")));

    r = Result.create(kvs);
    o.init(r);

    assertEquals(
      "{'key':'test-row','a':123,'b':[],'c':{},'d':''}".replace("'", "\""),
      SerDeUtils.getJSONString(o, oi));
  }

  /**
   * Test the LazyHBaseRow class with a one-to-one/onto mapping between Hive columns and
   * HBase column family/column qualifier pairs. The column types are primitive and fields
   * are stored in binary format in HBase.
   * @throws SerDeException
   */
  public void testLazyHBaseRow3() throws SerDeException {

    List<TypeInfo> fieldTypeInfos = TypeInfoUtils.getTypeInfosFromTypeString(
        "string,int,tinyint,smallint,bigint,float,double,string,boolean");
    List<String> fieldNames = Arrays.asList(
        new String [] {"key", "c_int", "c_byte", "c_short", "c_long", "c_float", "c_double",
            "c_string", "c_bool"});
    Text nullSequence = new Text("\\N");
    String hbaseColumnsMapping = ":key#str,cf-int:cq-int#bin,cf-byte:cq-byte#bin,"
      + "cf-short:cq-short#bin,cf-long:cq-long#bin,cf-float:cq-float#bin,cf-double:cq-double#bin,"
      + "cf-string:cq-string#str,cf-bool:cq-bool#bin";
    ColumnMappings columnMappings = null;

    try {
      columnMappings = HBaseSerDe.parseColumnsMapping(hbaseColumnsMapping);
    } catch (SerDeException e) {
      fail(e.toString());
    }

    ColumnMapping[] columnsMapping = columnMappings.getColumnsMapping();
    for (int i = 0; i < columnsMapping.length; i++) {
      ColumnMapping colMap = columnsMapping[i];

      if (i == 0 || i == 7) {
        colMap.binaryStorage.add(false);
      } else {
        colMap.binaryStorage.add(true);
      }
    }

    ObjectInspector oi =
      LazyFactory.createLazyStructInspector(fieldNames, fieldTypeInfos,
          new byte [] {' ', ':', '='}, nullSequence, false, false, (byte) 0);

    LazyHBaseRow o = new LazyHBaseRow((LazySimpleStructObjectInspector) oi, columnMappings);

    byte [] rowKey = "row-key".getBytes();
    List<Cell> kvs = new ArrayList<Cell>();
    byte [] value;

    for (int i = 1; i < columnsMapping.length; i++) {

      switch (i) {

      case 1:
        value = Bytes.toBytes(1);
        break;

      case 2:
        value = new byte[]{(byte)1};
        break;

      case 3:
        value = Bytes.toBytes((short) 1);
        break;

      case 4:
        value = Bytes.toBytes((long) 1);
        break;

      case 5:
        value = Bytes.toBytes((float) 1.0F);
        break;

      case 6:
        value = Bytes.toBytes((double) 1.0);
        break;

      case 7:
        value = "Hadoop, Hive, with HBase storage handler.".getBytes();
        break;

      case 8:
        value = Bytes.toBytes(true);
        break;

      default:
        throw new RuntimeException("Not expected: " + i);
      }

      ColumnMapping colMap = columnsMapping[i];
      kvs.add(new KeyValue(rowKey, colMap.familyNameBytes, colMap.qualifierNameBytes, value));
    }

    Collections.sort(kvs, KeyValue.COMPARATOR);
    Result result = Result.create(kvs);
    o.init(result);
    List<? extends StructField> fieldRefs = ((StructObjectInspector) oi).getAllStructFieldRefs();


    for (int i = 0; i < fieldRefs.size(); i++) {
      Object fieldData = ((StructObjectInspector) oi).getStructFieldData(o, fieldRefs.get(i));

      assert(fieldData != null);
      assert(fieldData instanceof LazyPrimitive<?, ?>);
      Writable writable = ((LazyPrimitive<?, ?>) fieldData).getWritableObject();

      switch (i) {
      case 0:
        Text text = new Text("row-key");
        assertEquals(text, writable);
        break;

      case 1:
        IntWritable iw = new IntWritable(1);
        assertEquals(iw, writable);
        break;

      case 2:
        ByteWritable bw = new ByteWritable((byte) 1);
        assertEquals(bw, writable);
        break;

      case 3:
        ShortWritable sw = new ShortWritable((short) 1);
        assertEquals(sw, writable);
        break;

      case 4:
        LongWritable lw = new LongWritable(1);
        assertEquals(lw, writable);
        break;

      case 5:
        FloatWritable fw = new FloatWritable(1.0F);
        assertEquals(fw, writable);
        break;

      case 6:
        DoubleWritable dw = new DoubleWritable(1.0);
        assertEquals(dw, writable);
        break;

      case 7:
        Text t = new Text("Hadoop, Hive, with HBase storage handler.");
        assertEquals(t, writable);
        break;

      case 8:
        BooleanWritable boolWritable = new BooleanWritable(true);
        assertEquals(boolWritable, writable);
        break;

      default:
        fail("Error: Unanticipated value in deserializing fields for HBaseSerDe.");
        break;
      }
    }
  }
}
