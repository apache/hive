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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

public class TestGenericUDFTrunc extends TestCase {

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

  private void runAndVerify(String expResult, GenericUDF udf, ObjectInspector[] initArgs,
      DeferredObject[] evalArgs) throws HiveException {
    udf.initialize(initArgs);
    Text output = (Text) udf.evaluate(evalArgs);
    assertEquals("frist_day() test ", expResult, output.toString());
  }
}
