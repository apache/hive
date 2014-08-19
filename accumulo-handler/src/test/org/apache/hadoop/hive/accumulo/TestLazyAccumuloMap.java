/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.accumulo;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.hive.accumulo.columns.ColumnEncoding;
import org.apache.hadoop.hive.accumulo.columns.HiveAccumuloMapColumnMapping;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class TestLazyAccumuloMap {

  protected byte[] toBytes(int i) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    out.writeInt(i);
    out.close();
    return baos.toByteArray();
  }

  @Test
  public void testStringMapWithProjection() throws SerDeException {
    AccumuloHiveRow row = new AccumuloHiveRow("row");

    row.add("cf1", "foo", "bar".getBytes());
    row.add("cf1", "bar", "foo".getBytes());

    row.add("cf2", "foo1", "bar1".getBytes());
    row.add("cf3", "bar1", "foo1".getBytes());

    HiveAccumuloMapColumnMapping mapping = new HiveAccumuloMapColumnMapping("cf1", null,
        ColumnEncoding.STRING, ColumnEncoding.STRING, "column", TypeInfoFactory.getMapTypeInfo(
            TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo).toString());

    // Map of Integer to String
    Text nullSequence = new Text("\\N");
    ObjectInspector oi = LazyFactory.createLazyObjectInspector(TypeInfoUtils
        .getTypeInfosFromTypeString("map<string,string>").get(0), new byte[] {(byte) 1, (byte) 2},
        0, nullSequence, false, (byte) 0);

    LazyAccumuloMap map = new LazyAccumuloMap((LazyMapObjectInspector) oi);
    map.init(row, mapping);

    Assert.assertEquals(2, map.getMapSize());

    Object o = map.getMapValueElement(new Text("foo"));
    Assert.assertNotNull(o);
    Assert.assertEquals(new Text("bar"), ((LazyString) o).getWritableObject());

    o = map.getMapValueElement(new Text("bar"));
    Assert.assertNotNull(o);
    Assert.assertEquals(new Text("foo"), ((LazyString) o).getWritableObject());
  }

  @Test
  public void testIntMap() throws SerDeException, IOException {
    AccumuloHiveRow row = new AccumuloHiveRow("row");

    row.add(new Text("cf1"), new Text("1"), "2".getBytes());
    row.add(new Text("cf1"), new Text("2"), "4".getBytes());
    row.add(new Text("cf1"), new Text("3"), "6".getBytes());

    HiveAccumuloMapColumnMapping mapping = new HiveAccumuloMapColumnMapping("cf1", null,
        ColumnEncoding.STRING, ColumnEncoding.STRING, "column", TypeInfoFactory.getMapTypeInfo(
            TypeInfoFactory.intTypeInfo, TypeInfoFactory.intTypeInfo).toString());

    // Map of Integer to Integer
    Text nullSequence = new Text("\\N");
    ObjectInspector oi = LazyFactory.createLazyObjectInspector(TypeInfoUtils
        .getTypeInfosFromTypeString("map<int,int>").get(0), new byte[] {(byte) 1, (byte) 2}, 0,
        nullSequence, false, (byte) 0);

    LazyAccumuloMap map = new LazyAccumuloMap((LazyMapObjectInspector) oi);
    map.init(row, mapping);

    Assert.assertEquals(3, map.getMapSize());

    Object o = map.getMapValueElement(new IntWritable(1));
    Assert.assertNotNull(o);
    Assert.assertEquals(new IntWritable(2), ((LazyInteger) o).getWritableObject());

    o = map.getMapValueElement(new IntWritable(2));
    Assert.assertNotNull(o);
    Assert.assertEquals(new IntWritable(4), ((LazyInteger) o).getWritableObject());

    o = map.getMapValueElement(new IntWritable(3));
    Assert.assertNotNull(o);
    Assert.assertEquals(new IntWritable(6), ((LazyInteger) o).getWritableObject());
  }

  @Test
  public void testBinaryIntMap() throws SerDeException, IOException {
    AccumuloHiveRow row = new AccumuloHiveRow("row");

    row.add(new Text("cf1"), new Text(toBytes(1)), toBytes(2));
    row.add(new Text("cf1"), new Text(toBytes(2)), toBytes(4));
    row.add(new Text("cf1"), new Text(toBytes(3)), toBytes(6));

    HiveAccumuloMapColumnMapping mapping = new HiveAccumuloMapColumnMapping("cf1", null,
        ColumnEncoding.BINARY, ColumnEncoding.BINARY, "column", TypeInfoFactory.getMapTypeInfo(
            TypeInfoFactory.intTypeInfo, TypeInfoFactory.intTypeInfo).toString());

    // Map of Integer to String
    Text nullSequence = new Text("\\N");
    ObjectInspector oi = LazyFactory.createLazyObjectInspector(TypeInfoUtils
        .getTypeInfosFromTypeString("map<int,int>").get(0), new byte[] {(byte) 1, (byte) 2}, 0,
        nullSequence, false, (byte) 0);

    LazyAccumuloMap map = new LazyAccumuloMap((LazyMapObjectInspector) oi);
    map.init(row, mapping);

    Assert.assertEquals(3, map.getMapSize());

    Object o = map.getMapValueElement(new IntWritable(1));
    Assert.assertNotNull(o);
    Assert.assertEquals(new IntWritable(2), ((LazyInteger) o).getWritableObject());

    o = map.getMapValueElement(new IntWritable(2));
    Assert.assertNotNull(o);
    Assert.assertEquals(new IntWritable(4), ((LazyInteger) o).getWritableObject());

    o = map.getMapValueElement(new IntWritable(3));
    Assert.assertNotNull(o);
    Assert.assertEquals(new IntWritable(6), ((LazyInteger) o).getWritableObject());
  }

  @Test
  public void testMixedSerializationMap() throws SerDeException, IOException {
    AccumuloHiveRow row = new AccumuloHiveRow("row");

    row.add(new Text("cf1"), new Text(toBytes(1)), "2".getBytes());
    row.add(new Text("cf1"), new Text(toBytes(2)), "4".getBytes());
    row.add(new Text("cf1"), new Text(toBytes(3)), "6".getBytes());

    HiveAccumuloMapColumnMapping mapping = new HiveAccumuloMapColumnMapping("cf1", null,
        ColumnEncoding.BINARY, ColumnEncoding.STRING, "column", TypeInfoFactory.getMapTypeInfo(
            TypeInfoFactory.intTypeInfo, TypeInfoFactory.intTypeInfo).toString());

    // Map of Integer to String
    Text nullSequence = new Text("\\N");
    ObjectInspector oi = LazyFactory.createLazyObjectInspector(TypeInfoUtils
        .getTypeInfosFromTypeString("map<int,int>").get(0), new byte[] {(byte) 1, (byte) 2}, 0,
        nullSequence, false, (byte) 0);

    LazyAccumuloMap map = new LazyAccumuloMap((LazyMapObjectInspector) oi);
    map.init(row, mapping);

    Assert.assertEquals(3, map.getMapSize());

    Object o = map.getMapValueElement(new IntWritable(1));
    Assert.assertNotNull(o);
    Assert.assertEquals(new IntWritable(2), ((LazyInteger) o).getWritableObject());

    o = map.getMapValueElement(new IntWritable(2));
    Assert.assertNotNull(o);
    Assert.assertEquals(new IntWritable(4), ((LazyInteger) o).getWritableObject());

    o = map.getMapValueElement(new IntWritable(3));
    Assert.assertNotNull(o);
    Assert.assertEquals(new IntWritable(6), ((LazyInteger) o).getWritableObject());
  }

}
