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
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Properties;
import java.util.TimeZone;

// Ideally tests to cover:
//  - attributes and/or geometry
//  - null attributes and values to not linger
//  - null geometry
//  - spatial reference preserved

public class TestGeoJsonSerDe extends JsonSerDeTestingBase {

  @Test
  public void TestIntWrite() throws Exception {
    ArrayList<Object> stuff = new ArrayList<Object>();
    Properties proptab = new Properties();
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "num");
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "int");
    AbstractSerDe jserde = mkSerDe(proptab);
    StructObjectInspector rowOI = (StructObjectInspector) jserde.getObjectInspector();

    // {"properties":{"num":7}}
    addWritable(stuff, 7);
    Writable jsw = jserde.serialize(stuff, rowOI);
    JsonNode jn = new ObjectMapper().readTree(jsw.toString());
    jn = jn.findValue("properties");
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

    // {"properties":{"when":147147147147}}
    long epoch = 147147147147L;
    long zoned = epoch - TimeZone.getDefault().getOffset(epoch);
    java.sql.Date expected = new java.sql.Date(zoned);
    addWritable(stuff, expected);
    Writable jsw = jserde.serialize(stuff, rowOI);
    JsonNode jn = new ObjectMapper().readTree(jsw.toString());
    jn = jn.findValue("properties");
    jn = jn.findValue("when");
    java.sql.Date actual = new java.sql.Date(jn.longValue());
    long day = 24 * 3600 * 1000;  // DateWritable represents days not milliseconds.
    Assert.assertEquals(epoch / day, jn.longValue() / day);
  }

  @Test
  public void TestPointWrite() throws Exception {
    ArrayList<Object> stuff = new ArrayList<Object>();
    Properties proptab = new Properties();
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "shape");
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "binary");
    AbstractSerDe jserde = mkSerDe(proptab);
    StructObjectInspector rowOI = (StructObjectInspector) jserde.getObjectInspector();

    // {"properties":{},"geometry":{"type":"Point","coordinates":[15.0,5.0]}}
    addWritable(stuff, new Point(15.0, 5.0));
    Writable jsw = jserde.serialize(stuff, rowOI);
    String rslt = jsw.toString();
    JsonNode jn = new ObjectMapper().readTree(rslt);
    jn = jn.findValue("geometry");
    Assert.assertNotNull(jn.findValue("type"));
    Assert.assertNotNull(jn.findValue("coordinates"));
  }

  @Test
  public void TestIntParse() throws Exception {
    Configuration config = new Configuration();
    Text value = new Text();

    AbstractSerDe jserde = new GeoJsonSerDe();
    Properties proptab = new Properties();
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "num");
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "int");
    jserde.initialize(config, proptab, null);
    StructObjectInspector rowOI = (StructObjectInspector) jserde.getObjectInspector();

    value.set("{\"properties\":{\"num\":7}}");
    Object row = jserde.deserialize(value);
    StructField f0 = rowOI.getStructFieldRef("num");
    Object fieldData = rowOI.getStructFieldData(row, f0);
    Assert.assertEquals(7, ((IntWritable) fieldData).get());
    value.set("{\"properties\":{\"num\":9}}");
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

    AbstractSerDe jserde = new GeoJsonSerDe();
    Properties proptab = new Properties();
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "when");
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "date");
    jserde.initialize(config, proptab, null);
    StructObjectInspector rowOI = (StructObjectInspector) jserde.getObjectInspector();

    String dateStr = "2020-02-20";
    value.set("{\"properties\":{\"when\":\"" + dateStr + "\"}}");
    Object row = jserde.deserialize(value);
    StructField f0 = rowOI.getStructFieldRef("when");
    Object fieldData = rowOI.getStructFieldData(row, f0);
    Assert.assertEquals(dateStr, iso8601FromWritable(fieldData));
    dateStr = "2017-05-05";
    value.set("{\"properties\":{\"when\":\"" + dateStr + "\"}}");
    row = jserde.deserialize(value);
    fieldData = rowOI.getStructFieldData(row, f0);
    Assert.assertEquals(dateStr, iso8601FromWritable(fieldData));
  }

  @Test
  public void TestEpochParse() throws Exception {
    Configuration config = new Configuration();
    Text value = new Text();

    AbstractSerDe jserde = new GeoJsonSerDe();
    Properties proptab = new Properties();
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "when");
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "date");
    jserde.initialize(config, proptab, null);
    StructObjectInspector rowOI = (StructObjectInspector) jserde.getObjectInspector();

    value.set("{\"properties\":{\"when\":147147147147}}");
    Object row = jserde.deserialize(value);
    StructField f0 = rowOI.getStructFieldRef("when");
    Object fieldData = rowOI.getStructFieldData(row, f0);
    long day = 24 * 3600 * 1000;  // DateWritable represents days not milliseconds.
    long epochExpected = 147147147147L;
    Assert.assertEquals(epochExpected / day, epochFromWritable(fieldData) / day);
    value.set("{\"properties\":{\"when\":142857142857}}");
    row = jserde.deserialize(value);
    fieldData = rowOI.getStructFieldData(row, f0);
    epochExpected = 142857142857L;
    Assert.assertEquals(epochExpected / day, epochFromWritable(fieldData) / day);
  }

  @Test
  public void TestPointParse() throws Exception {
    Configuration config = new Configuration();
    Text value = new Text();

    AbstractSerDe jserde = new GeoJsonSerDe();
    Properties proptab = new Properties();
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "shape");
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "binary");
    jserde.initialize(config, proptab, null);
    StructObjectInspector rowOI = (StructObjectInspector) jserde.getObjectInspector();

    value.set("{\"properties\":{},\"geometry\":{\"type\":\"Point\",\"coordinates\":[15.0,5.0]}}");
    Object row = jserde.deserialize(value);
    StructField f0 = rowOI.getStructFieldRef("shape");
    Object fieldData = rowOI.getStructFieldData(row, f0);
    ckPoint(new Point(15.0, 5.0), (BytesWritable) fieldData);

    value.set("{\"properties\":{},\"geometry\":{\"type\":\"Point\",\"type\":\"Point\",\"coordinates\":[7.0,4.0]}}");
    row = jserde.deserialize(value);
    f0 = rowOI.getStructFieldRef("shape");
    fieldData = rowOI.getStructFieldData(row, f0);
    ckPoint(new Point(7.0, 4.0), (BytesWritable) fieldData);
  }

  @Test
  public void TestIntOnly() throws Exception {  // Is this valid for GeoJSON?
    ArrayList<Object> stuff = new ArrayList<Object>();
    Properties proptab = new Properties();
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "num");
    proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "int");
    AbstractSerDe jserde = mkSerDe(proptab);
    StructObjectInspector rowOI = (StructObjectInspector) jserde.getObjectInspector();

    //value.set("{\"properties\":{\"num\":7}}");
    addWritable(stuff, 7);
    Object row = runSerDe(stuff, jserde, rowOI);
    Object fieldData = getField("num", row, rowOI);
    Assert.assertEquals(7, ((IntWritable) fieldData).get());
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

    //value.set("{\"properties\":{},\"geometry\":{\"type\":\"Point\",\"coordinates\":[15.0,5.0]}}");
    addWritable(stuff, new Point(15.0, 5.0));
    Object row = runSerDe(stuff, jserde, rowOI);
    Object fieldData = getField("shape", row, rowOI);
    ckPoint(new Point(15.0, 5.0), (BytesWritable) fieldData);

    //value.set("{\"properties\":{},\"geometry\":{\"type\":\"Point\",\"type\":\"Point\",\"coordinates\":[7.0,4.0]}}");
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

    // value.set("{\"properties\":{\"num\":7},\"geometry\":{\"type\":\"Point\",\"coordinates\":[15.0,5.0]}}");
    addWritable(stuff, 7L);
    addWritable(stuff, new Point(15.0, 5.0));
    Object row = runSerDe(stuff, jserde, rowOI);
    Object fieldData = getField("num", row, rowOI);
    Assert.assertEquals(7, ((LongWritable) fieldData).get());

    //value.set("{\"properties\":{\"num\":4},\"geometry\":{\"type\":\"Point\",\"coordinates\":[7.0,2.0]}}");
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

    //value.set("{\"properties\":{\"num\":7}}");
    addWritable(stuff, 7);
    Object row = runSerDe(stuff, jserde, rowOI);
    Object fieldData = getField("num", row, rowOI);
    Assert.assertEquals(7, ((IntWritable) fieldData).get());
    //value.set("{\"properties\":{}}");
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

    //value.set("{\"properties\":{},\"geometry\":{\"type\":\"Point\",\"coordinates\":[15.0,5.0]}}");
    addWritable(stuff, new Point(15.0, 5.0));
    Object row = runSerDe(stuff, jserde, rowOI);
    Object fieldData = getField("shape", row, rowOI);
    ckPoint(new Point(15.0, 5.0), (BytesWritable) fieldData);

    //value.set("{\"properties\":{},\"coordinates\":null}");
    stuff.set(0, null);
    row = runSerDe(stuff, jserde, rowOI);
    fieldData = getField("shape", row, rowOI);
    Assert.assertNull(fieldData);
  }

  private AbstractSerDe mkSerDe(Properties proptab) throws Exception {
    Configuration config = new Configuration();
    AbstractSerDe jserde = new GeoJsonSerDe();
    jserde.initialize(config, proptab, null);
    return jserde;
  }

}
