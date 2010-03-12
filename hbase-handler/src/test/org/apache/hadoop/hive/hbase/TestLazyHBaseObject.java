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

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.io.RowResult;
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

import junit.framework.TestCase;

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
        
    // Intialize a row result
    HbaseMapWritable<byte[], Cell> cells = new HbaseMapWritable<byte[], Cell>();
    cells.put("cfa:col1".getBytes(), new Cell("cfacol1".getBytes(), 0));
    cells.put("cfa:col2".getBytes(), new Cell("cfacol2".getBytes(), 0));
    cells.put("cfb:2".getBytes(),    new Cell("def".getBytes(), 0));
    cells.put("cfb:-1".getBytes(),   new Cell("".getBytes(), 0));
    cells.put("cfb:0".getBytes(),    new Cell("0".getBytes(), 0));
    cells.put("cfb:8".getBytes(),    new Cell("abc".getBytes(), 0));
    cells.put("cfc:col3".getBytes(), new Cell("cfccol3".getBytes(), 0));
        
    RowResult rr = new RowResult("test-row".getBytes(), cells);
        
    b.init(rr, "cfb:");
        
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
        
    // Intialize a row result
    HbaseMapWritable<byte[], Cell> cells =
      new HbaseMapWritable<byte[], Cell>();
    cells.put("cfa:col1".getBytes(), new Cell("cfacol1".getBytes(), 0));
    cells.put("cfa:col2".getBytes(), new Cell("cfacol2".getBytes(), 0));
    cells.put("cfb:2".getBytes(),    new Cell("d\tf".getBytes(), 0));
    cells.put("cfb:-1".getBytes(),   new Cell("".getBytes(), 0));
    cells.put("cfb:0".getBytes(),    new Cell("0".getBytes(), 0));
    cells.put("cfb:8".getBytes(),    new Cell("abc".getBytes(), 0));
    cells.put("cfc:col3".getBytes(), new Cell("cfccol3".getBytes(), 0));
        
    RowResult rr = new RowResult("test-row".getBytes(), cells);
        
    b.init(rr, "cfb:");
        
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
        
    List<String> hbaseColumnNames = 
      Arrays.asList(new String[]{"cfa:a", "cfa:b", "cfb:c", "cfb:d"});
        
    ObjectInspector oi = LazyFactory.createLazyStructInspector(fieldNames,
      fieldTypeInfos, new byte[] {' ', ':', '='},
      nullSequence, false, false, (byte)0);
    LazyHBaseRow o = new LazyHBaseRow((LazySimpleStructObjectInspector) oi);

    HbaseMapWritable<byte[], Cell> cells =
      new HbaseMapWritable<byte[], Cell>();
        
    cells.put("cfa:a".getBytes(), new Cell("123".getBytes(), 0));
    cells.put("cfa:b".getBytes(), new Cell("a:b:c".getBytes(), 0));
    cells.put("cfb:c".getBytes(), new Cell("d=e:f=g".getBytes(), 0));
    cells.put("cfb:d".getBytes(), new Cell("hi".getBytes(), 0));
    RowResult rr = new RowResult("test-row".getBytes(), cells);
    o.init(rr, hbaseColumnNames);
    assertEquals(
      ("{'key':'test-row','a':123,'b':['a','b','c'],"
        + "'c':{'d':'e','f':'g'},'d':'hi'}").replace("'", "\""), 
      SerDeUtils.getJSONString(o, oi));

    cells.clear();
    cells.put("cfa:a".getBytes(), new Cell("123".getBytes(), 0));
    cells.put("cfb:c".getBytes(), new Cell("d=e:f=g".getBytes(), 0));
    rr = new RowResult("test-row".getBytes(), cells);
    o.init(rr, hbaseColumnNames);
    assertEquals(
      ("{'key':'test-row','a':123,'b':null,"
        + "'c':{'d':'e','f':'g'},'d':null}").replace("'", "\""),
      SerDeUtils.getJSONString(o, oi));

    cells.clear();
    cells.put("cfa:b".getBytes(), new Cell("a".getBytes(), 0));
    cells.put("cfb:c".getBytes(), new Cell("d=\\N:f=g:h".getBytes(), 0));
    cells.put("cfb:d".getBytes(), new Cell("no".getBytes(), 0));
    rr = new RowResult("test-row".getBytes(), cells);
    o.init(rr, hbaseColumnNames);
    assertEquals(
      ("{'key':'test-row','a':null,'b':['a'],"
        + "'c':{'d':null,'f':'g','h':null},'d':'no'}").replace("'", "\""),
      SerDeUtils.getJSONString(o, oi));

    cells.clear();
    cells.put("cfa:b".getBytes(), new Cell(":a::".getBytes(), 0));
    cells.put("cfb:d".getBytes(), new Cell("no".getBytes(), 0));
    rr = new RowResult("test-row".getBytes(), cells);
    o.init(rr, hbaseColumnNames);
    assertEquals(
      ("{'key':'test-row','a':null,'b':['','a','',''],"
        + "'c':null,'d':'no'}").replace("'", "\""),
      SerDeUtils.getJSONString(o, oi));

    cells.clear();
    cells.put("cfa:a".getBytes(), new Cell("123".getBytes(), 0));
    cells.put("cfa:b".getBytes(), new Cell("".getBytes(), 0));
    cells.put("cfb:c".getBytes(), new Cell("".getBytes(), 0));
    cells.put("cfb:d".getBytes(), new Cell("".getBytes(), 0));
    rr = new RowResult("test-row".getBytes(), cells);
    o.init(rr, hbaseColumnNames);
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
        
    List<String> hbaseColumnNames = 
      Arrays.asList(new String[]{"cfa:a", "cfa:b", "cfb:", "cfc:d"});
        
    ObjectInspector oi = LazyFactory.createLazyStructInspector(
      fieldNames,
      fieldTypeInfos,
      new byte[] {' ', ':', '='},
      nullSequence, false, false, (byte) 0);
    LazyHBaseRow o = new LazyHBaseRow((LazySimpleStructObjectInspector) oi);

    HbaseMapWritable<byte[], Cell> cells =
      new HbaseMapWritable<byte[], Cell>();
        
    cells.put("cfa:a".getBytes(), new Cell("123".getBytes(), 0));
    cells.put("cfa:b".getBytes(), new Cell("a:b:c".getBytes(), 0));
    cells.put("cfb:d".getBytes(), new Cell("e".getBytes(), 0));
    cells.put("cfb:f".getBytes(), new Cell("g".getBytes(), 0));
    cells.put("cfc:d".getBytes(), new Cell("hi".getBytes(), 0));
    RowResult rr = new RowResult("test-row".getBytes(), cells);
    o.init(rr, hbaseColumnNames);
    assertEquals(
      ("{'key':'test-row','a':123,'b':['a','b','c'],"
        + "'c':{'d':'e','f':'g'},'d':'hi'}").replace("'", "\""), 
      SerDeUtils.getJSONString(o, oi));

    cells.clear();
    cells.put("cfa:a".getBytes(), new Cell("123".getBytes(), 0));
    cells.put("cfb:d".getBytes(), new Cell("e".getBytes(), 0));
    cells.put("cfb:f".getBytes(), new Cell("g".getBytes(), 0));
    rr = new RowResult("test-row".getBytes(), cells);
    o.init(rr, hbaseColumnNames);
    assertEquals(
      ("{'key':'test-row','a':123,'b':null,"
        + "'c':{'d':'e','f':'g'},'d':null}").replace("'", "\""),
      SerDeUtils.getJSONString(o, oi));

    cells.clear();
    cells.put("cfa:b".getBytes(), new Cell("a".getBytes(), 0));
    cells.put("cfb:f".getBytes(), new Cell("g".getBytes(), 0));
    cells.put("cfc:d".getBytes(), new Cell("no".getBytes(), 0));
    rr = new RowResult("test-row".getBytes(), cells);
    o.init(rr, hbaseColumnNames);
    assertEquals(
      ("{'key':'test-row','a':null,'b':['a'],"
        + "'c':{'f':'g'},'d':'no'}").replace("'", "\""),
      SerDeUtils.getJSONString(o, oi));

    cells.clear();
    cells.put("cfa:b".getBytes(), new Cell(":a::".getBytes(), 0));
    cells.put("cfc:d".getBytes(), new Cell("no".getBytes(), 0));
    rr = new RowResult("test-row".getBytes(), cells);
    o.init(rr, hbaseColumnNames);
    assertEquals(
      ("{'key':'test-row','a':null,'b':['','a','',''],"
        + "'c':{},'d':'no'}").replace("'", "\""),
      SerDeUtils.getJSONString(o, oi));

    cells.clear();
    cells.put("cfa:a".getBytes(), new Cell("123".getBytes(), 0));
    cells.put("cfa:b".getBytes(), new Cell("".getBytes(), 0));
    cells.put("cfc:d".getBytes(), new Cell("".getBytes(), 0));
    rr = new RowResult("test-row".getBytes(), cells);
    o.init(rr, hbaseColumnNames);
    assertEquals(
      "{'key':'test-row','a':123,'b':[],'c':{},'d':''}".replace("'", "\""),
      SerDeUtils.getJSONString(o, oi));
  }
}
