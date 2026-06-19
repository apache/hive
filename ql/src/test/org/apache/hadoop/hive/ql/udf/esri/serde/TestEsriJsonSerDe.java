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
package org.apache.hadoop.hive.ql.udf.esri.serde;

import com.esri.core.geometry.Point;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.udf.esri.shims.HiveShims;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Properties;
import java.util.TimeZone;

// Ideally tests to cover:
//  - attributes and/or geometry
//  - null attributes and values to not linger
//  - null geometry
//  - spatial reference preserved

public class TestEsriJsonSerDe extends JsonSerDeTestingBase {

  @Test
  public void TestIntWrite() throws Exception {
    ArrayList<Object> stuff = new ArrayList<Object>();
    Properties proptab = new Properties();
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "num");
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "int");
    AbstractSerDe jserde = mkSerDe(proptab);
    StructObjectInspector rowOI = (StructObjectInspector) jserde.getObjectInspector();

    // {"attributes":{"num":7}}
    addWritable(stuff, 7);
    Writable jsw = jserde.serialize(stuff, rowOI);
    JsonNode jn = new ObjectMapper().readTree(jsw.toString());
    jn = jn.findValue("attributes");
    jn = jn.findValue("num");
    Assert.assertEquals(7, jn.intValue());
  }

  @Test
  public void TestEpochWrite() throws Exception {
    ArrayList<Object> stuff = new ArrayList<Object>();
    Properties proptab = new Properties();
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "when");
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "date");
    AbstractSerDe jserde = mkSerDe(proptab);
    StructObjectInspector rowOI = (StructObjectInspector) jserde.getObjectInspector();

    // {"attributes":{"when":147147147147}}
    long epoch = 147147147147L;
    long zoned = epoch - TimeZone.getDefault().getOffset(epoch);
    java.sql.Date expected = new java.sql.Date(zoned);
    addWritable(stuff, expected);
    Writable jsw = jserde.serialize(stuff, rowOI);
    JsonNode jn = new ObjectMapper().readTree(jsw.toString());
    jn = jn.findValue("attributes");
    jn = jn.findValue("when");
    java.sql.Date actual = new java.sql.Date(jn.longValue());
    long day = 24 * 3600 * 1000;  // DateWritable represents days not milliseconds.
    Assert.assertEquals(epoch / day, jn.longValue() / day);
  }

  @Test
  public void TestTimeWrite() throws Exception {
    ArrayList<Object> stuff = new ArrayList<Object>();
    Properties proptab = new Properties();
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "when");
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "timestamp");
    AbstractSerDe jserde = mkSerDe(proptab);
    StructObjectInspector rowOI = (StructObjectInspector) jserde.getObjectInspector();

    // {"attributes":{"when":147147147147}}
    long epoch = 147147147147L;
    java.sql.Timestamp expected = new java.sql.Timestamp(epoch);
    addWritable(stuff, expected);
    Writable jsw = jserde.serialize(stuff, rowOI);
    JsonNode jn = new ObjectMapper().readTree(jsw.toString());
    jn = jn.findValue("attributes");
    jn = jn.findValue("when");
    java.sql.Timestamp actual = new java.sql.Timestamp(jn.longValue());
    Assert.assertEquals(epoch, actual.getTime());
  }

  @Test
  public void TestPointWrite() throws Exception {
    ArrayList<Object> stuff = new ArrayList<Object>();
    Properties proptab = new Properties();
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "shape");
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "binary");
    AbstractSerDe jserde = mkSerDe(proptab);
    StructObjectInspector rowOI = (StructObjectInspector) jserde.getObjectInspector();

    // {"attributes":{},"geometry":{"x":15.0,"y":5.0}}
    addWritable(stuff, new Point(15.0, 5.0));
    Writable jsw = jserde.serialize(stuff, rowOI);
    String rslt = jsw.toString();
    JsonNode jn = new ObjectMapper().readTree(rslt);
    jn = jn.findValue("geometry");
    Assert.assertNotNull(jn.findValue("x"));
    Assert.assertNotNull(jn.findValue("y"));
  }

  @Test
  public void TestIntParse() throws Exception {
    Configuration config = new Configuration();
    Text value = new Text();

    AbstractSerDe jserde = new EsriJsonSerDe();
    Properties proptab = new Properties();
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "num");
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "int");
    jserde.initialize(config, proptab, null);
    StructObjectInspector rowOI = (StructObjectInspector) jserde.getObjectInspector();

    //value.set("{\"attributes\":{\"num\":7},\"geometry\":null}");
    value.set("{\"attributes\":{\"num\":7}}");
    Object row = jserde.deserialize(value);
    StructField f0 = rowOI.getStructFieldRef("num");
    Object fieldData = rowOI.getStructFieldData(row, f0);
    Assert.assertEquals(7, ((IntWritable) fieldData).get());
    value.set("{\"attributes\":{\"num\":9}}");
    row = jserde.deserialize(value);
    f0 = rowOI.getStructFieldRef("num");
    fieldData = rowOI.getStructFieldData(row, f0);
    Assert.assertEquals(9, ((IntWritable) fieldData).get());
  }

  @Test
  public void TestDateParse() throws Exception {
    // DateWritable#daysToMillis adjusts the numerical/epoch time
    // to midnight in the local time zone - but only prior to Hive-3.1 (HIVE-12192).
    // Raises questions about what the product source code should do,
    // but at least in the meantime the test expectations match that.
    Configuration config = new Configuration();
    Text value = new Text();

    AbstractSerDe jserde = new EsriJsonSerDe();
    Properties proptab = new Properties();
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "when");
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "date");
    jserde.initialize(config, proptab, null);
    StructObjectInspector rowOI = (StructObjectInspector) jserde.getObjectInspector();

    String dateStr = "2020-02-20";
    value.set("{\"attributes\":{\"when\":\"" + dateStr + "\"}}");
    Object row = jserde.deserialize(value);
    StructField f0 = rowOI.getStructFieldRef("when");
    Object fieldData = rowOI.getStructFieldData(row, f0);

    Assert.assertEquals(dateStr, iso8601FromWritable(fieldData));
    dateStr = "2017-05-05";
    value.set("{\"attributes\":{\"when\":\"" + dateStr + "\"}}");
    row = jserde.deserialize(value);
    fieldData = rowOI.getStructFieldData(row, f0);
    Assert.assertEquals(dateStr, iso8601FromWritable(fieldData));
  }

  @Test
  public void TestEpochParse() throws Exception {
    Configuration config = new Configuration();
    Text value = new Text();

    AbstractSerDe jserde = new EsriJsonSerDe();
    Properties proptab = new Properties();
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "when");
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "date");
    jserde.initialize(config, proptab, null);
    StructObjectInspector rowOI = (StructObjectInspector) jserde.getObjectInspector();

    value.set("{\"attributes\":{\"when\":147147147147}}");
    Object row = jserde.deserialize(value);
    StructField f0 = rowOI.getStructFieldRef("when");
    Object fieldData = rowOI.getStructFieldData(row, f0);
    long day = 24 * 3600 * 1000;  // DateWritable represents days not milliseconds.
    long epochExpected = 147147147147L;
    Assert.assertEquals(epochExpected / day, epochFromWritable(fieldData) / day);
    value.set("{\"attributes\":{\"when\":142857142857}}");
    row = jserde.deserialize(value);
    fieldData = rowOI.getStructFieldData(row, f0);
    epochExpected = 142857142857L;
    Assert.assertEquals(epochExpected / day, epochFromWritable(fieldData) / day);
  }

  @Test
  public void TestTimeParse() throws Exception {
    Configuration config = new Configuration();
    Text value = new Text();

    AbstractSerDe jserde = new EsriJsonSerDe();
    Properties proptab = new Properties();
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "when");
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "timestamp");
    jserde.initialize(config, proptab, null);
    StructObjectInspector rowOI = (StructObjectInspector) jserde.getObjectInspector();

    String timeStr = "2020-02-20";
    value.set("{\"attributes\":{\"when\":\"" + timeStr + "\"}}");
    Object row = jserde.deserialize(value);
    StructField f0 = rowOI.getStructFieldRef("when");
    Object fieldData = rowOI.getStructFieldData(row, f0);
    SimpleDateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd");
    dtFmt.setTimeZone(TimeZone.getTimeZone("UTC"));
    long epoch = dtFmt.parse(timeStr).getTime();
    long withOffset = epoch - TimeZone.getDefault().getOffset(epoch);
    long got = epochFromWritable(fieldData);
    Assert.assertEquals(epoch, got);
    timeStr = "2017-05-05 05:05";
    value.set("{\"attributes\":{\"when\":\"" + timeStr + "\"}}");
    row = jserde.deserialize(value);
    fieldData = rowOI.getStructFieldData(row, f0);
    dtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    dtFmt.setTimeZone(TimeZone.getTimeZone("UTC"));
    epoch = dtFmt.parse(timeStr).getTime();
    got = epochFromWritable(fieldData);
    Assert.assertEquals(timeStr, iso8601FromWritable(fieldData).substring(0, 16));
    Assert.assertEquals(epoch, got);

    timeStr = "2017-08-09 10:11:12";
    value.set("{\"attributes\":{\"when\":\"" + timeStr + "\"}}");
    row = jserde.deserialize(value);
    fieldData = rowOI.getStructFieldData(row, f0);
    dtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    dtFmt.setTimeZone(TimeZone.getTimeZone("UTC"));
    epoch = dtFmt.parse(timeStr + 'Z').getTime();
    withOffset = epoch - TimeZone.getDefault().getOffset(epoch);
    got = epochFromWritable(fieldData);
    Assert.assertEquals(timeStr, iso8601FromWritable(fieldData).substring(0, 19));
    Assert.assertEquals(epoch, got);

    timeStr = "2017-06-05 04:03:02.123456789";
    value.set("{\"attributes\":{\"when\":\"" + timeStr + "\"}}");
    row = jserde.deserialize(value);
    fieldData = rowOI.getStructFieldData(row, f0);
    dtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm.SSS");
    dtFmt.setTimeZone(TimeZone.getTimeZone("UTC"));
    dtFmt = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    dtFmt.setTimeZone(TimeZone.getTimeZone("UTC"));
    epoch = dtFmt.parse(timeStr.substring(0, 23)).getTime();
    withOffset = epoch - TimeZone.getDefault().getOffset(epoch);
    got = epochFromWritable(fieldData);
    Assert.assertEquals(epoch, got);  // ns consumed by parser but not checked
  }

  @Test
  public void TestPointParse() throws Exception {
    Configuration config = new Configuration();
    Text value = new Text();

    AbstractSerDe jserde = new EsriJsonSerDe();
    Properties proptab = new Properties();
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "shape");
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "binary");
    jserde.initialize(config, proptab, null);
    StructObjectInspector rowOI = (StructObjectInspector) jserde.getObjectInspector();

    value.set("{\"attributes\":{},\"geometry\":{\"x\":15.0,\"y\":5.0}}");
    Object row = jserde.deserialize(value);
    StructField f0 = rowOI.getStructFieldRef("shape");
    Object fieldData = rowOI.getStructFieldData(row, f0);
    ckPoint(new Point(15.0, 5.0), (BytesWritable) fieldData);

    value.set("{\"attributes\":{},\"geometry\":{\"x\":7.0,\"y\":4.0}}");
    row = jserde.deserialize(value);
    f0 = rowOI.getStructFieldRef("shape");
    fieldData = rowOI.getStructFieldData(row, f0);
    ckPoint(new Point(7.0, 4.0), (BytesWritable) fieldData);
  }

  @Test
  public void TestIntOnly() throws Exception {
    ArrayList<Object> stuff = new ArrayList<Object>();
    Properties proptab = new Properties();
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "num");
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "int");
    AbstractSerDe jserde = mkSerDe(proptab);
    StructObjectInspector rowOI = (StructObjectInspector) jserde.getObjectInspector();

    addWritable(stuff, 7);
    Object row = runSerDe(stuff, jserde, rowOI);
    Object fieldData = getField("num", row, rowOI);
    Assert.assertEquals(7, ((IntWritable) fieldData).get());
    // value.set("{\"attributes\":{\"num\":9}}");
    stuff.clear();
    addWritable(stuff, 9);
    row = runSerDe(stuff, jserde, rowOI);
    fieldData = getField("num", row, rowOI);
    Assert.assertEquals(9, ((IntWritable) fieldData).get());
  }

  @Test
  public void TestPointOnly() throws Exception {
    ArrayList<Object> stuff = new ArrayList<Object>();
    Properties proptab = new Properties();
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "shape");
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "binary");
    AbstractSerDe jserde = mkSerDe(proptab);
    StructObjectInspector rowOI = (StructObjectInspector) jserde.getObjectInspector();

    //value.set("{\"attributes\":{},\"geometry\":{\"x\":15.0,\"y\":5.0}}");
    addWritable(stuff, new Point(15.0, 5.0));
    Object row = runSerDe(stuff, jserde, rowOI);
    Object fieldData = getField("shape", row, rowOI);
    ckPoint(new Point(15.0, 5.0), (BytesWritable) fieldData);

    //value.set("{\"attributes\":{},\"geometry\":{\"x\":7.0,\"y\":4.0}}");
    stuff.clear();
    addWritable(stuff, new Point(7.0, 4.0));
    row = runSerDe(stuff, jserde, rowOI);
    fieldData = getField("shape", row, rowOI);
    ckPoint(new Point(7.0, 4.0), (BytesWritable) fieldData);
  }

  @Test
  public void TestIntPoint() throws Exception {
    ArrayList<Object> stuff = new ArrayList<Object>();
    Properties proptab = new Properties();
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "num,shape");
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "bigint,binary");
    AbstractSerDe jserde = mkSerDe(proptab);
    StructObjectInspector rowOI = (StructObjectInspector) jserde.getObjectInspector();

    //value.set("{\"attributes\":{\"num\":7},\"geometry\":{\"x\":15.0,\"y\":5.0}}");
    addWritable(stuff, 7L);
    addWritable(stuff, new Point(15.0, 5.0));
    Object row = runSerDe(stuff, jserde, rowOI);
    Object fieldData = getField("num", row, rowOI);
    Assert.assertEquals(7, ((LongWritable) fieldData).get());

    //value.set("{\"attributes\":{\"num\":4},\"geometry\":{\"x\":7.0,\"y\":2.0}}");
    stuff.clear();
    addWritable(stuff, 4L);
    addWritable(stuff, new Point(7.0, 2.0));
    row = runSerDe(stuff, jserde, rowOI);
    fieldData = getField("num", row, rowOI);
    Assert.assertEquals(4, ((LongWritable) fieldData).get());
    fieldData = getField("shape", row, rowOI);
    ckPoint(new Point(7.0, 2.0), (BytesWritable) fieldData);
  }

  @Test
  public void TestNullAttr() throws Exception {
    ArrayList<Object> stuff = new ArrayList<Object>();
    Properties proptab = new Properties();
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "num");
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "int");
    AbstractSerDe jserde = mkSerDe(proptab);
    StructObjectInspector rowOI = (StructObjectInspector) jserde.getObjectInspector();

    //value.set("{\"attributes\":{\"num\":7}}");
    addWritable(stuff, 7);
    Object row = runSerDe(stuff, jserde, rowOI);
    Object fieldData = getField("num", row, rowOI);
    Assert.assertEquals(7, ((IntWritable) fieldData).get());
    //value.set("{\"attributes\":{}}");
    stuff.set(0, null);
    row = runSerDe(stuff, jserde, rowOI);
    fieldData = getField("num", row, rowOI);
    Assert.assertNull(fieldData);
  }

  @Test
  public void TestNullGeom() throws Exception {
    ArrayList<Object> stuff = new ArrayList<Object>();
    Properties proptab = new Properties();
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "shape");
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "binary");
    AbstractSerDe jserde = mkSerDe(proptab);
    StructObjectInspector rowOI = (StructObjectInspector) jserde.getObjectInspector();

    //value.set("{\"attributes\":{},\"geometry\":{\"x\":15.0,\"y\":5.0}}");
    addWritable(stuff, new Point(15.0, 5.0));
    Object row = runSerDe(stuff, jserde, rowOI);
    Object fieldData = getField("shape", row, rowOI);
    ckPoint(new Point(15.0, 5.0), (BytesWritable) fieldData);

    //value.set("{\"attributes\":{},\"geometry\":null}");
    stuff.set(0, null);
    row = runSerDe(stuff, jserde, rowOI);
    fieldData = getField("shape", row, rowOI);
    Assert.assertNull(fieldData);
  }

  @Test
  public void TestColumnTypes() throws Exception {
    ArrayList<Object> stuff = new ArrayList<Object>();
    Properties proptab = new Properties();
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "flag,num1,num2,text");
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "boolean,tinyint,smallint,string");
    AbstractSerDe jserde = mkSerDe(proptab);
    StructObjectInspector rowOI = (StructObjectInspector) jserde.getObjectInspector();

    // {"attributes":{"flag":false,"num":"5","text":"Point(15.0 5.0)"}}
    addWritable(stuff, false);
    addWritable(stuff, (byte) 2);
    addWritable(stuff, (short) 5);
    addWritable(stuff, "Point(15.0 5.0)");
    Object row = runSerDe(stuff, jserde, rowOI);
    Object fieldData = getField("flag", row, rowOI);
    Assert.assertEquals(false, ((BooleanWritable) fieldData).get());
    fieldData = getField("num1", row, rowOI);
    Assert.assertEquals((byte) 2, ((ByteWritable) fieldData).get());
    fieldData = getField("num2", row, rowOI);
    Assert.assertEquals((short) 5, ((ShortWritable) fieldData).get());
    fieldData = getField("text", row, rowOI);
    Assert.assertEquals("Point(15.0 5.0)", fieldData.toString());

    stuff.set(0, new BooleanWritable(true));
    stuff.set(1, new ByteWritable((byte) 4));
    stuff.set(2, new ShortWritable((short) 4));
    //stuff.set(3, new Text("other"));
    LazyStringObjectInspector loi = LazyPrimitiveObjectInspectorFactory.
        getLazyStringObjectInspector(false, (byte) '\0');
    LazyString lstr = new LazyString(loi);
    ByteArrayRef bar = new ByteArrayRef();
    bar.setData("other".getBytes());
    lstr.init(bar, 0, 5);
    stuff.set(3, lstr);
    row = runSerDe(stuff, jserde, rowOI);
    fieldData = getField("flag", row, rowOI);
    Assert.assertEquals(true, ((BooleanWritable) fieldData).get());
    fieldData = getField("num1", row, rowOI);
    Assert.assertEquals((byte) 4, ((ByteWritable) fieldData).get());
    fieldData = getField("num2", row, rowOI);
    Assert.assertEquals((short) 4, ((ShortWritable) fieldData).get());
    fieldData = getField("text", row, rowOI);
    Assert.assertEquals("other", fieldData.toString());
  }

  private AbstractSerDe mkSerDe(Properties proptab) throws Exception {
    Configuration config = new Configuration();
    AbstractSerDe jserde = new EsriJsonSerDe();
    jserde.initialize(config, proptab, null);
    return jserde;
  }

}
