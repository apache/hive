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

package org.apache.hadoop.hive.hbase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * TestLazyHBaseObject is a test for the LazyHBaseXXX classes.
 */
public class TestLazyHBaseObject extends TestCase {
  /**
   * Test the LazyMap class with Integer-to-String.
   */
  public void testLazyHBaseCellMap1() {
    // Map of Integer to String
    Text nullSequence = new Text("\\N");
    ObjectInspector oi = LazyFactory.createLazyObjectInspector(
      TypeInfoUtils.getTypeInfosFromTypeString("map<int,string>").get(0),
      new byte[]{(byte)1, (byte)2}, 0, nullSequence, false, (byte)0);

    LazyHBaseCellMap b = new LazyHBaseCellMap((LazyMapObjectInspector) oi);

    // Initialize a result
    List<KeyValue> kvs = new ArrayList<KeyValue>();

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

    Result r = new Result(kvs);

    b.init(r, "cfb".getBytes());

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
   */
  public void testLazyHBaseCellMap2() {
    // Map of String to String
    Text nullSequence = new Text("\\N");
    ObjectInspector oi = LazyFactory.createLazyObjectInspector(
      TypeInfoUtils.getTypeInfosFromTypeString("map<string,string>").get(0),
      new byte[]{(byte)'#', (byte)'\t'}, 0, nullSequence, false, (byte)0);

    LazyHBaseCellMap b = new LazyHBaseCellMap((LazyMapObjectInspector) oi);

    // Initialize a result
    List<KeyValue> kvs = new ArrayList<KeyValue>();

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

    Result r = new Result(kvs);
    b.init(r, "cfb".getBytes());

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
   * Test the LazyHBaseRow class with one-for-one mappings between
   * Hive fields and HBase columns.
   */
  public void testLazyHBaseRow1() {
    List<TypeInfo> fieldTypeInfos =
      TypeInfoUtils.getTypeInfosFromTypeString(
        "string,int,array<string>,map<string,string>,string");
    List<String> fieldNames = Arrays.asList(
      new String[]{"key", "a", "b", "c", "d"});
    Text nullSequence = new Text("\\N");

    String hbaseColsMapping = ":key,cfa:a,cfa:b,cfb:c,cfb:d";
    List<String> colFamily = new ArrayList<String>();
    List<String> colQual = new ArrayList<String>();
    List<byte []> colFamilyBytes = new ArrayList<byte []>();
    List<byte []> colQualBytes = new ArrayList<byte []>();

    int iKey = -1;

    try {
      iKey = HBaseSerDe.parseColumnMapping(
          hbaseColsMapping, colFamily, colFamilyBytes, colQual, colQualBytes);
    } catch (SerDeException e) {
      fail(e.toString());
    }

    assertEquals(0, iKey);

    ObjectInspector oi = LazyFactory.createLazyStructInspector(fieldNames,
      fieldTypeInfos, new byte[] {' ', ':', '='},
      nullSequence, false, false, (byte)0);
    LazyHBaseRow o = new LazyHBaseRow((LazySimpleStructObjectInspector) oi);

    List<KeyValue> kvs = new ArrayList<KeyValue>();

    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfa"), Bytes.toBytes("a"), Bytes.toBytes("123")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfa"), Bytes.toBytes("b"), Bytes.toBytes("a:b:c")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfb"), Bytes.toBytes("c"), Bytes.toBytes("d=e:f=g")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfb"), Bytes.toBytes("d"), Bytes.toBytes("hi")));

    Result r = new Result(kvs);
    o.init(r, colFamily, colFamilyBytes, colQual, colQualBytes);

    assertEquals(
      ("{'key':'test-row','a':123,'b':['a','b','c'],"
        + "'c':{'d':'e','f':'g'},'d':'hi'}").replace("'", "\""),
      SerDeUtils.getJSONString(o, oi));

    kvs.clear();
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfa"), Bytes.toBytes("a"), Bytes.toBytes("123")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfb"), Bytes.toBytes("c"), Bytes.toBytes("d=e:f=g")));

    r = new Result(kvs);
    o.init(r, colFamily, colFamilyBytes, colQual, colQualBytes);

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

    r = new Result(kvs);
    o.init(r, colFamily, colFamilyBytes, colQual, colQualBytes);

    assertEquals(
        ("{'key':'test-row','a':null,'b':['a'],"
          + "'c':{'d':null,'f':'g','h':null},'d':'no'}").replace("'", "\""),
        SerDeUtils.getJSONString(o, oi));

    kvs.clear();
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfa"), Bytes.toBytes("b"), Bytes.toBytes(":a::")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfb"), Bytes.toBytes("d"), Bytes.toBytes("no")));

    r = new Result(kvs);
    o.init(r, colFamily, colFamilyBytes, colQual, colQualBytes);

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

    r = new Result(kvs);
    o.init(r, colFamily, colFamilyBytes, colQual, colQualBytes);

    assertEquals(
      "{'key':'test-row','a':123,'b':[],'c':{},'d':''}".replace("'", "\""),
      SerDeUtils.getJSONString(o, oi));
  }

  /**
   * Test the LazyHBaseRow class with a mapping from a Hive field to
   * an HBase column family.
   */
  public void testLazyHBaseRow2() {
    // column family is mapped to Map<string,string>
    List<TypeInfo> fieldTypeInfos =
      TypeInfoUtils.getTypeInfosFromTypeString(
        "string,int,array<string>,map<string,string>,string");
    List<String> fieldNames = Arrays.asList(
      new String[]{"key", "a", "b", "c", "d"});
    Text nullSequence = new Text("\\N");

    String hbaseColsMapping = ":key,cfa:a,cfa:b,cfb:,cfc:d";
    List<String> colFamily = new ArrayList<String>();
    List<String> colQual = new ArrayList<String>();
    List<byte []> colFamilyBytes = new ArrayList<byte []>();
    List<byte []> colQualBytes = new ArrayList<byte []>();
    int iKey = -1;
    try {
      iKey = HBaseSerDe.parseColumnMapping(
          hbaseColsMapping, colFamily, colFamilyBytes, colQual, colQualBytes);
    } catch (SerDeException e) {
      fail(e.toString());
    }
    assertEquals(0, iKey);

    ObjectInspector oi = LazyFactory.createLazyStructInspector(
      fieldNames,
      fieldTypeInfos,
      new byte[] {' ', ':', '='},
      nullSequence, false, false, (byte) 0);
    LazyHBaseRow o = new LazyHBaseRow((LazySimpleStructObjectInspector) oi);

    List<KeyValue> kvs = new ArrayList<KeyValue>();
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

    Result r = new Result(kvs);
    o.init(r, colFamily, colFamilyBytes, colQual, colQualBytes);

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

    r = new Result(kvs);
    o.init(r, colFamily, colFamilyBytes, colQual, colQualBytes);

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

    r = new Result(kvs);
    o.init(r, colFamily, colFamilyBytes, colQual, colQualBytes);

    assertEquals(
      ("{'key':'test-row','a':null,'b':['a'],"
        + "'c':{'f':'g'},'d':'no'}").replace("'", "\""),
      SerDeUtils.getJSONString(o, oi));

    kvs.clear();
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfa"), Bytes.toBytes("b"), Bytes.toBytes(":a::")));
    kvs.add(new KeyValue(Bytes.toBytes("test-row"),
        Bytes.toBytes("cfc"), Bytes.toBytes("d"), Bytes.toBytes("no")));

    r = new Result(kvs);
    o.init(r, colFamily, colFamilyBytes, colQual, colQualBytes);

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

    r = new Result(kvs);
    o.init(r, colFamily, colFamilyBytes, colQual, colQualBytes);

    assertEquals(
      "{'key':'test-row','a':123,'b':[],'c':{},'d':''}".replace("'", "\""),
      SerDeUtils.getJSONString(o, oi));
  }
}
