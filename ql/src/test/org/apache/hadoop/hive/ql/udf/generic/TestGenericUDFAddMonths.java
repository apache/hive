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



import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * TestGenericUDFAddMonths.
 */
public class TestGenericUDFAddMonths {

  private final Text fmtTextWithTime = new Text("YYYY-MM-dd HH:mm:ss");
  private final Text fmtTextWithTimeAndms = new Text("YYYY-MM-dd HH:mm:ss.SSS");
  private final Text fmtTextWithoutTime = new Text("YYYY-MM-dd");
  private final Text fmtTextInvalid = new Text("YYYY-abcdz");

  @Test
  public void testAddMonthsInt() throws HiveException {
    GenericUDFAddMonths udf = new GenericUDFAddMonths();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    // date str
    runAndVerify("2014-01-14", 1, "2014-02-14", udf);
    runAndVerify("2014-01-31", 1, "2014-02-28", udf);
    runAndVerify("2014-02-28", -1, "2014-01-31", udf);
    runAndVerify("2014-02-28", 2, "2014-04-30", udf);
    runAndVerify("2014-04-30", -2, "2014-02-28", udf);
    runAndVerify("2015-02-28", 12, "2016-02-29", udf);
    runAndVerify("2016-02-29", -12, "2015-02-28", udf);
    runAndVerify("2016-01-29", 1, "2016-02-29", udf);
    runAndVerify("2016-02-29", -1, "2016-01-31", udf);
    runAndVerify("1001-10-05", 1, "1001-11-05", udf);
    runAndVerify("1582-10-05", 1, "1582-11-05", udf);

    // ts str
    runAndVerify("2014-01-14 10:30:00", 1, "2014-02-14", udf);
    runAndVerify("2014-01-31 10:30:00", 1, "2014-02-28", udf);
    runAndVerify("2014-02-28 10:30:00.1", -1, "2014-01-31", udf);
    runAndVerify("2014-02-28 10:30:00.100", 2, "2014-04-30", udf);
    runAndVerify("2014-04-30 10:30:00.001", -2, "2014-02-28", udf);
    runAndVerify("2015-02-28 10:30:00.000000001", 12, "2016-02-29", udf);
    runAndVerify("2016-02-29 10:30:00", -12, "2015-02-28", udf);
    runAndVerify("2016-01-29 10:30:00", 1, "2016-02-29", udf);
    runAndVerify("2016-02-29 10:30:00", -1, "2016-01-31", udf);
    runAndVerify("2016-02-29 10:30:00", -1, fmtTextWithoutTime, "2016-01-31", udf);
  }

  @Test
  public void testAddMonthsStringWithTime() throws HiveException {
    GenericUDFAddMonths udf = new GenericUDFAddMonths();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo,
            fmtTextWithTime);

    ObjectInspector[] arguments = {valueOI0, valueOI1, valueOI2};
    udf.initialize(arguments);
    runAndVerify("2018-05-10 08:15:12", -1, fmtTextWithTime, "2018-04-10 08:15:12", udf);
    runAndVerify("2017-12-31 14:15:16", 2, fmtTextWithTime, "2018-02-28 14:15:16", udf);
    runAndVerify("2017-12-31 14:15:16.001", 2, fmtTextWithTime, "2018-02-28 14:15:16", udf);
  }

  @Test
  public void testAddMonthsInvalidFormatter() throws HiveException {
    GenericUDFAddMonths udf = new GenericUDFAddMonths();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo,
            fmtTextInvalid);

    ObjectInspector[] arguments = {valueOI0, valueOI1, valueOI2};
    try {
      udf.initialize(arguments);
      fail("Expected to throw an exception for invalid DateFormat");
    } catch (IllegalArgumentException e) {
      //test success if exception caught
    }
  }
  @Test
  public void testAddMonthsStringWithTimeWithms() throws HiveException {
    GenericUDFAddMonths udf = new GenericUDFAddMonths();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo,
            fmtTextWithTimeAndms);

    ObjectInspector[] arguments = {valueOI0, valueOI1, valueOI2};
    udf.initialize(arguments);
    runAndVerify("2017-12-31 14:15:16.350", 2, fmtTextWithTimeAndms, "2018-02-28 14:15:16.350",
        udf);
    runAndVerify("2017-12-31 14:15:16.001", 2, fmtTextWithTimeAndms, "2018-02-28 14:15:16.001",
        udf);
    //Try to parse ms where there is no millisecond part in input, expected to return .000 as ms
    runAndVerify("2017-12-31 14:15:16", 2, fmtTextWithTimeAndms, "2018-02-28 14:15:16.000", udf);
  }

  @Test
  public void testAddMonthsWithNullFormatter() throws HiveException {
    GenericUDFAddMonths udf = new GenericUDFAddMonths();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo,
            null);

    ObjectInspector[] arguments = {valueOI0, valueOI1, valueOI2};
    udf.initialize(arguments);
    runAndVerify("2017-12-31 14:15:16.350", 2, null, "2018-02-28",
        udf);
    runAndVerify("2017-12-31", 2, null, "2018-02-28",
        udf);
  }
  @Test
  public void testAddMonthsTimestamp() throws HiveException {
    GenericUDFAddMonths udf = new GenericUDFAddMonths();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableIntObjectInspector;

    ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, fmtTextWithTime);
    ObjectInspector[] arguments = {valueOI0, valueOI1, valueOI2};

    udf.initialize(arguments);
    runAndVerify(Timestamp.valueOf("2018-05-10 08:15:12"), 1, fmtTextWithTime, "2018-06-10 08:15:12", udf);
    runAndVerify(Timestamp.valueOf("2017-12-31 14:15:16"), 2, fmtTextWithTime, "2018-02-28 14:15:16", udf);
  }

  @Test
  public void testWrongDateStr() throws HiveException {
    GenericUDFAddMonths udf = new GenericUDFAddMonths();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);
    runAndVerify("2014-02-30", 1, "2014-04-02", udf);
    runAndVerify("2014-02-32", 1, "2014-04-04", udf);
    runAndVerify("2014-01", 1, null, udf);
  }

  @Test
  public void testWrongTsStr() throws HiveException {
    GenericUDFAddMonths udf = new GenericUDFAddMonths();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerify("2014-02-30 10:30:00", 1, "2014-04-02", udf);
    runAndVerify("2014-02-32 10:30:00", 1, "2014-04-04", udf);
    runAndVerify("2014/01/31 10:30:00", 1, null, udf);
    runAndVerify("2014-01-31T10:30:00", 1, "2014-02-28", udf);
  }

  @Test
  public void testAddMonthsShort() throws HiveException {
    GenericUDFAddMonths udf = new GenericUDFAddMonths();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableShortObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);
    // short
    runAndVerify("2014-01-14", (short) 1, "2014-02-14", udf);
  }

  @Test
  public void testAddMonthsByte() throws HiveException {
    GenericUDFAddMonths udf = new GenericUDFAddMonths();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableByteObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);
    // short
    runAndVerify("2014-01-14", (byte) 1, "2014-02-14", udf);
  }

  @Test
  public void testAddMonthsLong() throws HiveException {
    @SuppressWarnings("resource")
    GenericUDFAddMonths udf = new GenericUDFAddMonths();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    try {
      udf.initialize(arguments);
      assertTrue("add_months exception expected", false);
    } catch (UDFArgumentTypeException e) {
      assertEquals("add_months test",
          "add_months only takes INT/SHORT/BYTE types as 2nd argument, got LONG", e.getMessage());
    }
  }



  private void runAndVerify(String str, int months, String expResult, GenericUDF udf)
      throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(new Text(str));
    DeferredObject valueObj1 = new DeferredJavaObject(new IntWritable(months));
    DeferredObject[] args = { valueObj0, valueObj1 };
    Text output = (Text) udf.evaluate(args);
    assertEquals("add_months() test ", expResult, output != null ? output.toString() : null);
  }

  private void runAndVerify(String str, int months, Text dateFormat, String expResult,
      GenericUDF udf) throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(new Text(str));
    DeferredObject valueObj1 = new DeferredJavaObject(new IntWritable(months));
    DeferredObject valueObj2 = new DeferredJavaObject(dateFormat);
    DeferredObject[] args = {valueObj0, valueObj1, valueObj2};
    Text output = (Text) udf.evaluate(args);
    assertEquals("add_months() test with time part", expResult,
        output != null ? output.toString() : null);
  }

  private void runAndVerify(Timestamp ts, int months, Text dateFormat, String expResult, GenericUDF udf)
      throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(new TimestampWritableV2(ts));
    DeferredObject valueObj1 = new DeferredJavaObject(new IntWritable(months));
    DeferredObject valueObj2 = new DeferredJavaObject(dateFormat);
    DeferredObject[] args = {valueObj0, valueObj1, valueObj2};
    Text output = (Text) udf.evaluate(args);
    assertEquals("add_months() test for timestamp", expResult, output != null ? output.toString() : null);
  }


  private void runAndVerify(String str, short months, String expResult, GenericUDF udf)
      throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(new Text(str));
    DeferredObject valueObj1 = new DeferredJavaObject(new ShortWritable(months));
    DeferredObject[] args = { valueObj0, valueObj1 };
    Text output = (Text) udf.evaluate(args);
    assertEquals("add_months() test ", expResult, output != null ? output.toString() : null);
  }

  private void runAndVerify(String str, byte months, String expResult, GenericUDF udf)
      throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(new Text(str));
    DeferredObject valueObj1 = new DeferredJavaObject(new ByteWritable(months));
    DeferredObject[] args = { valueObj0, valueObj1 };
    Text output = (Text) udf.evaluate(args);
    assertEquals("add_months() test ", expResult, output != null ? output.toString() : null);
  }
}
