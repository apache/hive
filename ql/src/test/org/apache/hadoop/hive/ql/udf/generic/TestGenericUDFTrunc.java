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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.Test;

/**
 * TestGenericUDFTrunc.
 */
public class TestGenericUDFTrunc {

  @Test
  public void testStringToDateWithMonthFormat() throws HiveException {
    GenericUDFTrunc udf = new GenericUDFTrunc();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector[] initArgs = { valueOI0, valueOI1};

    DeferredObject valueObjFmt = new DeferredJavaObject(new Text("MONTH"));

    DeferredObject valueObj0;
    DeferredObject[] evalArgs;

    // test date string
    valueObj0 = new DeferredJavaObject(new Text("2014-01-01"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2014-01-14"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2014-01-31"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2014-02-02"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-02-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2014-02-28"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-02-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2016-02-03"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-02-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2016-02-28"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-02-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2016-02-29"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-02-01", udf, initArgs, evalArgs);

    // test timestamp string
    valueObj0 = new DeferredJavaObject(new Text("2014-01-01 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2014-01-14 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2014-01-31 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2014-02-02 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-02-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2014-02-28 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-02-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2016-02-03 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-02-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2016-02-28 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-02-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2016-02-29 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-02-01", udf, initArgs, evalArgs);
  }

  @Test
  public void testStringToDateWithQuarterFormat() throws HiveException {
    GenericUDFTrunc udf = new GenericUDFTrunc();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector[] initArgs = { valueOI0, valueOI1};

    DeferredObject valueObjFmt = new DeferredJavaObject(new Text("QUARTER"));

    DeferredObject valueObj0;
    DeferredObject[] evalArgs;

    // test date string
    valueObj0 = new DeferredJavaObject(new Text("2014-01-01"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2014-01-14"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2014-01-31"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2014-02-02"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2014-02-28"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2016-02-03"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2016-02-28"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2016-02-29"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2016-05-11"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-04-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2016-07-01"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-07-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2016-12-31"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-10-01", udf, initArgs, evalArgs);

    // test timestamp string
    valueObj0 = new DeferredJavaObject(new Text("2014-01-01 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2014-01-14 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2014-01-31 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2014-02-02 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2014-02-28 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2016-02-03 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2016-02-28 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2016-02-29 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2016-05-11 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-04-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2016-07-01 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-07-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2016-12-31 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-10-01", udf, initArgs, evalArgs);
  }

  @Test
  public void testStringToDateWithYearFormat() throws HiveException {
    GenericUDFTrunc udf = new GenericUDFTrunc();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector[] initArgs = { valueOI0, valueOI1};

    DeferredObject valueObjFmt = new DeferredJavaObject(new Text("YEAR"));

    DeferredObject valueObj0;
    DeferredObject[] evalArgs;

    // test date string
    valueObj0 = new DeferredJavaObject(new Text("2014-01-01"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2014-01-14"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2014-01-31"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2014-02-02"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2014-02-28"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2016-02-03"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2016-02-28"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2016-02-29"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    // test timestamp string
    valueObj0 = new DeferredJavaObject(new Text("2014-01-01 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2014-01-14 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2014-01-31 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2014-02-02 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2014-02-28 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2016-02-03 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2016-02-28 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new Text("2016-02-29 10:30:45"));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);
  }

  @Test
  public void testTimestampToDateWithMonthFormat() throws HiveException {
    GenericUDFTrunc udf = new GenericUDFTrunc();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector[] initArgs = { valueOI0, valueOI1};

    DeferredObject valueObjFmt = new DeferredJavaObject(new Text("MON"));

    DeferredObject valueObj0;
    DeferredObject[] evalArgs;

    // test date string
    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-01-01 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-01-14 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-01-31 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-02-02 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-02-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-02-28 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-02-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2016-02-03 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-02-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2016-02-28 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-02-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2016-02-29 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-02-01", udf, initArgs, evalArgs);

    // test timestamp string
    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-01-01 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-01-14 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-01-31 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-02-02 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-02-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-02-28 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-02-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2016-02-03 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-02-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2016-02-28 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-02-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2016-02-29 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-02-01", udf, initArgs, evalArgs);
  }

  @Test
  public void testTimestampToDateWithQuarterFormat() throws HiveException {
    GenericUDFTrunc udf = new GenericUDFTrunc();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector[] initArgs = { valueOI0, valueOI1};

    DeferredObject valueObjFmt = new DeferredJavaObject(new Text("Q"));

    DeferredObject valueObj0;
    DeferredObject[] evalArgs;

    // test date string
    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-01-01 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-01-14 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-01-31 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-02-02 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-02-28 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2016-02-03 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2016-02-28 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2016-02-29 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2016-05-11 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-04-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2016-07-01 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-07-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2016-12-31 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-10-01", udf, initArgs, evalArgs);

    // test timestamp string
    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-01-01 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-01-14 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-01-31 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-02-02 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-02-28 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2016-02-03 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2016-02-28 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2016-02-29 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2016-05-11 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-04-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2016-07-01 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-07-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2016-12-31 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-10-01", udf, initArgs, evalArgs);
  }

  @Test
  public void testTimestampToDateWithYearFormat() throws HiveException {
    GenericUDFTrunc udf = new GenericUDFTrunc();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector[] initArgs = { valueOI0, valueOI1};

    DeferredObject valueObjFmt = new DeferredJavaObject(new Text("YYYY"));

    DeferredObject valueObj0;
    DeferredObject[] evalArgs;

    // test date string
    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-01-01 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-01-14 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-01-31 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-02-02 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-02-28 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2016-02-03 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2016-02-28 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2016-02-29 00:00:00")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    // test timestamp string
    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-01-01 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-01-14 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-01-31 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-02-02 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2014-02-28 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2016-02-03 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2016-02-28 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf("2016-02-29 10:30:45")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);
  }

  @Test
  public void testDateWritableToDateWithMonthFormat() throws HiveException {
    GenericUDFTrunc udf = new GenericUDFTrunc();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector[] initArgs = { valueOI0, valueOI1};

    DeferredObject valueObjFmt = new DeferredJavaObject(new Text("MM"));

    DeferredObject valueObj0;
    DeferredObject[] evalArgs;

    // test date string
    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2014-01-01")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2014-01-14")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2014-01-31")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2014-02-02")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-02-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2014-02-28")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-02-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2016-02-03")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-02-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2016-02-28")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-02-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2016-02-29")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-02-01", udf, initArgs, evalArgs);
  }

  @Test
  public void testDateWritableToDateWithQuarterFormat() throws HiveException {
    GenericUDFTrunc udf = new GenericUDFTrunc();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector[] initArgs = { valueOI0, valueOI1};

    DeferredObject valueObjFmt = new DeferredJavaObject(new Text("Q"));

    DeferredObject valueObj0;
    DeferredObject[] evalArgs;

    // test date string
    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2014-01-01")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2014-01-14")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2014-01-31")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2014-02-02")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2014-02-28")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2016-02-03")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2016-02-28")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2016-02-29")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2016-05-11")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-04-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2016-07-01")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-07-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2016-12-31")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-10-01", udf, initArgs, evalArgs);
  }

  @Test
  public void testDateWritableToDateWithYearFormat() throws HiveException {
    GenericUDFTrunc udf = new GenericUDFTrunc();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector[] initArgs = { valueOI0, valueOI1};

    DeferredObject valueObjFmt = new DeferredJavaObject(new Text("YY"));

    DeferredObject valueObj0;
    DeferredObject[] evalArgs;

    // test date string
    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2014-01-01")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2014-01-14")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2014-01-31")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2014-02-02")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2014-02-28")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2014-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2016-02-03")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2016-02-28")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);

    valueObj0 = new DeferredJavaObject(new DateWritableV2(Date.valueOf("2016-02-29")));
    evalArgs = new DeferredObject[] { valueObj0, valueObjFmt };
    runAndVerify("2016-01-01", udf, initArgs, evalArgs);
  }

  @Test
  public void testDecimalLargerThanLongWhenScaled() throws HiveException {
    // HIVE-26731: 123456711889.122345897 * 10^8 does not fit in a long
    verifyDecimal("123456711889.12234589", "123456711889.122345897", 21, 9, 8);
    verifyDecimal("-123456711889.12234589", "-123456711889.122345897", 21, 9, 8);
    verifyDecimal("123456711889.122345897", "123456711889.122345897", 21, 9, 19);
    verifyDecimal("0.0000000001", "0.00000000012345678901234567890123456789", 38, 38, 10);
  }

  @Test
  public void testDecimalIntegerPartLargerThanLong() throws HiveException {
    String input = "12345678901234567890123456789.987654321";
    verifyDecimal("12345678901234567890123456789.98765", input, 38, 9, 5);
    verifyDecimal("12345678901234567890123456789", input, 38, 9, 0);
    verifyDecimal("12345678901234567890123450000", input, 38, 9, -4);
    verifyDecimal("-12345678901234567890123450000", "-" + input, 38, 9, -4);
    verifyDecimal("0", input, 38, 9, -29);
    verifyDecimal("0", input, 38, 9, Integer.MIN_VALUE);

    String max = "99999999999999999999999999999999999999";
    verifyDecimal("99999999999999999999999999999999999990", max, 38, 0, -1);
    verifyDecimal("0", max, 38, 0, -38);
    verifyDecimal("0", "-" + max, 38, 0, -39);
    verifyDecimal("0", max, 38, 0, Integer.MIN_VALUE);
  }

  @Test
  public void testDecimalWithoutScale() throws HiveException {
    HiveDecimalWritable input = new HiveDecimalWritable("-12345678901234567890123456789.987654321");
    Object result = evaluate(
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
            TypeInfoFactory.getDecimalTypeInfo(38, 9)), input);
    assertEquals(new HiveDecimalWritable("-12345678901234567890123456789"), result);
    assertEquals("input must not be modified",
        new HiveDecimalWritable("-12345678901234567890123456789.987654321"), input);
  }

  @Test
  public void testDouble() throws HiveException {
    verifyDouble(123456711889.12234, 123456711889.12234, 8);
    verifyDouble(123456711889.122, 123456711889.12234, 3);
    verifyDouble(-123456711889.0, -123456711889.12234, 0);
    verifyDouble(-123456700000.0, -123456711889.12234, -5);
    verifyDouble(1.0E300, 1.0E300, 30);
    verifyDouble(9.9E299, 9.99E299, -298);
    verifyDouble(0.0, 0.5, -1);
    // The shortest decimal representation is truncated, not the exact binary value
    verifyDouble(0.3, 0.3, 1);
    verifyDouble(0.29, 0.29, 2);
    verifyDouble(0.0, -0.5, 0);
    verifyDouble(Double.NaN, Double.NaN, 2);
    verifyDouble(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, 2);
    verifyDouble(Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, 0);
  }

  @Test(timeout = 10000)
  public void testDoubleExtremeScale() throws HiveException {
    verifyDouble(Double.MIN_VALUE, Double.MIN_VALUE, 325);
    verifyDouble(0.0, Double.MIN_VALUE, 323);
    verifyDouble(1.0E308, 1.7976931348623157E308, -308);
    verifyDouble(0.0, -Double.MAX_VALUE, -309);
    // Neither the result nor the work may depend on the magnitude of the scale
    verifyDouble(12.5, 12.5, Integer.MAX_VALUE);
    verifyDouble(0.0, 12.5, Integer.MIN_VALUE);
    for (int i = 0; i < 1000; i++) {
      verifyDouble(12.5, 12.5, 100000000);
      verifyDouble(0.0, 12.5, -100000000);
    }
  }

  @Test
  public void testFloat() throws HiveException {
    verifyFloat(12345.123f, 12345.1234f, 3);
    verifyFloat(12345.1234f, 12345.1234f, 10);
    verifyFloat(-12345f, -12345.1234f, 0);
    verifyFloat(-12300f, -12345.1234f, -2);
  }

  @Test
  public void testIntegerTypes() throws HiveException {
    verifyLong(Long.MAX_VALUE, Long.MAX_VALUE, 2);
    verifyLong(9223372036854775000L, Long.MAX_VALUE, -3);
    verifyLong(-9000000000000000000L, Long.MIN_VALUE, -18);
    verifyLong(0L, Long.MIN_VALUE, -19);
    verifyLong(0L, Long.MAX_VALUE, Integer.MIN_VALUE);

    assertEquals(new IntWritable(-2147483000), evaluate(
        PrimitiveObjectInspectorFactory.writableIntObjectInspector, new IntWritable(-2147483648), -3));
    assertEquals(new IntWritable(2147483647), evaluate(
        PrimitiveObjectInspectorFactory.writableIntObjectInspector, new IntWritable(2147483647), 3));
    assertEquals(new ShortWritable((short) -32700), evaluate(
        PrimitiveObjectInspectorFactory.writableShortObjectInspector,
        new ShortWritable((short) -32768), -2));
    assertEquals(new ByteWritable((byte) 120), evaluate(
        PrimitiveObjectInspectorFactory.writableByteObjectInspector, new ByteWritable((byte) 127), -1));
  }

  @Test
  public void testNonConstantScale() throws HiveException {
    GenericUDFTrunc udf = new GenericUDFTrunc();
    udf.initialize(new ObjectInspector[] {
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
            TypeInfoFactory.getDecimalTypeInfo(21, 9)),
        PrimitiveObjectInspectorFactory.writableIntObjectInspector });

    HiveDecimalWritable input = new HiveDecimalWritable("123456711889.122345897");
    int[] scales = { 8, 0, -4, 19 };
    String[] expected = { "123456711889.12234589", "123456711889", "123456710000",
        "123456711889.122345897" };
    for (int i = 0; i < scales.length; i++) {
      Object result = udf.evaluate(new DeferredObject[] {
          new DeferredJavaObject(input), new DeferredJavaObject(new IntWritable(scales[i])) });
      assertEquals("scale " + scales[i], new HiveDecimalWritable(expected[i]), result);
    }
    assertEquals("input must not be modified", new HiveDecimalWritable("123456711889.122345897"),
        input);
  }

  @Test
  public void testNullInput() throws HiveException {
    assertNull(evaluate(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector, null, 2));
  }

  private static void verifyDecimal(String expected, String input, int precision, int scale,
      int truncScale) throws HiveException {
    Object result = evaluate(
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
            TypeInfoFactory.getDecimalTypeInfo(precision, scale)),
        new HiveDecimalWritable(input), truncScale);
    assertEquals("trunc(" + input + ", " + truncScale + ")",
        HiveDecimal.create(expected), ((HiveDecimalWritable) result).getHiveDecimal());
  }

  private static void verifyDouble(double expected, double input, int scale)
      throws HiveException {
    Object result = evaluate(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
        new DoubleWritable(input), scale);
    assertEquals("trunc(" + input + ", " + scale + ")", expected,
        ((DoubleWritable) result).get(), 0.0);
  }

  private static void verifyFloat(float expected, float input, int scale) throws HiveException {
    Object result = evaluate(PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
        new FloatWritable(input), scale);
    assertEquals("trunc(" + input + ", " + scale + ")", expected,
        ((FloatWritable) result).get(), 0.0f);
  }

  private static void verifyLong(long expected, long input, int scale) throws HiveException {
    Object result = evaluate(PrimitiveObjectInspectorFactory.writableLongObjectInspector,
        new LongWritable(input), scale);
    assertEquals("trunc(" + input + ", " + scale + ")", new LongWritable(expected), result);
  }

  private static Object evaluate(ObjectInspector valueOI, Writable value, int scale)
      throws HiveException {
    GenericUDFTrunc udf = new GenericUDFTrunc();
    IntWritable scaleWritable = new IntWritable(scale);
    udf.initialize(new ObjectInspector[] { valueOI,
        PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
            TypeInfoFactory.intTypeInfo, scaleWritable) });
    return udf.evaluate(new DeferredObject[] {
        new DeferredJavaObject(value), new DeferredJavaObject(scaleWritable) });
  }

  private static Object evaluate(ObjectInspector valueOI, Writable value) throws HiveException {
    GenericUDFTrunc udf = new GenericUDFTrunc();
    udf.initialize(new ObjectInspector[] { valueOI });
    return udf.evaluate(new DeferredObject[] { new DeferredJavaObject(value) });
  }

  private void runAndVerify(String expResult, GenericUDF udf, ObjectInspector[] initArgs,
      DeferredObject[] evalArgs) throws HiveException {
    udf.initialize(initArgs);
    Text output = (Text) udf.evaluate(evalArgs);
    assertEquals("frist_day() test ", expResult, output.toString());
  }
}
