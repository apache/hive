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

import junit.framework.TestCase;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class TestGenericUDFAddMonths extends TestCase {

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
    // wrong date str
    runAndVerify("2014-02-30", 1, "2014-04-02", udf);
    runAndVerify("2014-02-32", 1, "2014-04-04", udf);
    runAndVerify("2014-01", 1, null, udf);

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
    // wrong ts str
    runAndVerify("2014-02-30 10:30:00", 1, "2014-04-02", udf);
    runAndVerify("2014-02-32 10:30:00", 1, "2014-04-04", udf);
    runAndVerify("2014/01/31 10:30:00", 1, null, udf);
    runAndVerify("2014-01-31T10:30:00", 1, "2014-02-28", udf);
  }

  public void testAddMonthsShort() throws HiveException {
    GenericUDFAddMonths udf = new GenericUDFAddMonths();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableShortObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);
    // short
    runAndVerify("2014-01-14", (short) 1, "2014-02-14", udf);
  }

  public void testAddMonthsByte() throws HiveException {
    GenericUDFAddMonths udf = new GenericUDFAddMonths();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableByteObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);
    // short
    runAndVerify("2014-01-14", (byte) 1, "2014-02-14", udf);
  }

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
