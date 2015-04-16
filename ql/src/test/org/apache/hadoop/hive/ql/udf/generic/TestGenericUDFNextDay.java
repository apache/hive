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
package org.apache.hadoop.hive.ql.udf.generic;

import junit.framework.TestCase;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

public class TestGenericUDFNextDay extends TestCase {

  public void testNextDay() throws HiveException {
    GenericUDFNextDay udf = new GenericUDFNextDay();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    // start_date is Sun, 2 letters day name
    runAndVerify("2015-01-11", "su", "2015-01-18", udf);
    runAndVerify("2015-01-11", "MO", "2015-01-12", udf);
    runAndVerify("2015-01-11", "Tu", "2015-01-13", udf);
    runAndVerify("2015-01-11", "wE", "2015-01-14", udf);
    runAndVerify("2015-01-11", "th", "2015-01-15", udf);
    runAndVerify("2015-01-11", "FR", "2015-01-16", udf);
    runAndVerify("2015-01-11", "Sa", "2015-01-17", udf);

    // start_date is Sat, 3 letters day name
    runAndVerify("2015-01-17", "sun", "2015-01-18", udf);
    runAndVerify("2015-01-17", "MON", "2015-01-19", udf);
    runAndVerify("2015-01-17", "Tue", "2015-01-20", udf);
    runAndVerify("2015-01-17", "weD", "2015-01-21", udf);
    runAndVerify("2015-01-17", "tHu", "2015-01-22", udf);
    runAndVerify("2015-01-17", "FrI", "2015-01-23", udf);
    runAndVerify("2015-01-17", "SAt", "2015-01-24", udf);

    // start_date is Wed, full timestamp, full day name
    runAndVerify("2015-01-14 14:04:34", "sunday", "2015-01-18", udf);
    runAndVerify("2015-01-14 14:04:34.1", "Monday", "2015-01-19", udf);
    runAndVerify("2015-01-14 14:04:34.100", "Tuesday", "2015-01-20", udf);
    runAndVerify("2015-01-14 14:04:34.001", "wednesday", "2015-01-21", udf);
    runAndVerify("2015-01-14 14:04:34.000000001", "thursDAY", "2015-01-15", udf);
    runAndVerify("2015-01-14 14:04:34", "FRIDAY", "2015-01-16", udf);
    runAndVerify("2015-01-14 14:04:34", "SATurday", "2015-01-17", udf);

    // null values
    runAndVerify("2015-01-14", null, null, udf);
    runAndVerify(null, "SU", null, udf);
    runAndVerify(null, null, null, udf);

    // not valid values
    runAndVerify("01/14/2015", "TU", null, udf);
    runAndVerify("2015-01-14", "VT", null, udf);
    runAndVerify("2015-02-30", "WE", "2015-03-04", udf);
    runAndVerify("2015-02-32", "WE", "2015-03-11", udf);
    runAndVerify("2015-02-30 10:30:00", "WE", "2015-03-04", udf);
    runAndVerify("2015-02-32 10:30:00", "WE", "2015-03-11", udf);
    runAndVerify("2015/01/14 14:04:34", "SAT", null, udf);
    runAndVerify("2015-01-14T14:04:34", "SAT", "2015-01-17", udf);
  }

  public void testNextDayErrorArg1() throws HiveException {
    @SuppressWarnings("resource")
    GenericUDFNextDay udf = new GenericUDFNextDay();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    try {
      udf.initialize(arguments);
      assertTrue("UDFArgumentException expected", false);
    } catch (UDFArgumentException e) {
      assertEquals(
          "next_day only takes STRING_GROUP, DATE_GROUP, VOID_GROUP types as 1st argument, got LONG",
          e.getMessage());
    }
  }

  public void testNextDayErrorArg2() throws HiveException {
    @SuppressWarnings("resource")
    GenericUDFNextDay udf = new GenericUDFNextDay();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    try {
      udf.initialize(arguments);
      assertTrue("UDFArgumentException expected", false);
    } catch (UDFArgumentException e) {
      assertEquals("next_day only takes STRING_GROUP, VOID_GROUP types as 2nd argument, got INT",
          e.getMessage());
    }
  }

  private void runAndVerify(String date, String dayOfWeek, String expResult, GenericUDF udf)
      throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(date != null ? new Text(date) : null);
    DeferredObject valueObj1 = new DeferredJavaObject(dayOfWeek != null ? new Text(dayOfWeek)
        : null);
    DeferredObject[] args = { valueObj0, valueObj1 };
    Text output = (Text) udf.evaluate(args);
    assertEquals("next_day() test ", expResult, output != null ? output.toString() : null);
  }
}
