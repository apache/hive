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

import java.sql.Date;

import junit.framework.TestCase;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class TestGenericUDFLeast extends TestCase {

  public void testOneArg() throws HiveException {
    @SuppressWarnings("resource")
    GenericUDFLeast udf = new GenericUDFLeast();
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = { valueOI1 };

    UDFArgumentException ex = null;
    try {
      udf.initialize(arguments);
    } catch (UDFArgumentException e) {
      ex = e;
    }
    assertNotNull("least() test ", ex);
  }

  public void testDifferentType() throws HiveException {
    @SuppressWarnings("resource")
    GenericUDFLeast udf = new GenericUDFLeast();
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    ObjectInspector[] arguments = { valueOI1, valueOI2 };

    UDFArgumentException ex = null;
    try {
      udf.initialize(arguments);
    } catch (UDFArgumentException e) {
      ex = e;
    }
    assertNotNull("least() test ", ex);
  }

  public void testLeastStr() throws HiveException {
    GenericUDFLeast udf = new GenericUDFLeast();
    ObjectInspector[] arguments = new ObjectInspector[3];
    for (int i = 0; i < arguments.length; i++) {
      arguments[i] = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    udf.initialize(arguments);

    runAndVerify(new String[] { "a", "b", "c" }, "a", udf);
    runAndVerify(new String[] { "C", "a", "B" }, "B", udf);
    runAndVerify(new String[] { "AAA", "AaA", "AAa" }, "AAA", udf);
    runAndVerify(new String[] { "A", "AA", "AAA" }, "A", udf);

    runAndVerify(new String[] { "11", "13", "12" }, "11", udf);
    runAndVerify(new String[] { "11", "2", "12" }, "11", udf);
    runAndVerify(new String[] { "01", "03", "02" }, "01", udf);
    runAndVerify(new String[] { "01", "1", "02" }, "01", udf);

    runAndVerify(new String[] { null, "b", "c" }, "b", udf);
    runAndVerify(new String[] { "a", null, "c" }, "a", udf);
    runAndVerify(new String[] { "a", "b", null }, "a", udf);

    runAndVerify(new String[] { "a", null, null }, "a", udf);
    runAndVerify(new String[] { null, "b", null }, "b", udf);
    runAndVerify(new String[] { null, null, null }, null, udf);
  }

  public void testLeastInt() throws HiveException {
    GenericUDFLeast udf = new GenericUDFLeast();
    ObjectInspector[] arguments = new ObjectInspector[3];
    for (int i = 0; i < arguments.length; i++) {
      arguments[i] = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    }

    udf.initialize(arguments);

    runAndVerify(new Integer[] { 11, 13, 12 }, 11, udf);
    runAndVerify(new Integer[] { 1, 13, 2 }, 1, udf);

    runAndVerify(new Integer[] { -11, -13, -12 }, -13, udf);
    runAndVerify(new Integer[] { 1, -13, 2 }, -13, udf);

    runAndVerify(new Integer[] { null, 1, 2 }, 1, udf);
    runAndVerify(new Integer[] { 1, null, 2 }, 1, udf);
    runAndVerify(new Integer[] { 1, 2, null }, 1, udf);

    runAndVerify(new Integer[] { null, null, null }, null, udf);
  }

  public void testLeastDouble() throws HiveException {
    GenericUDFLeast udf = new GenericUDFLeast();
    ObjectInspector[] arguments = new ObjectInspector[3];
    for (int i = 0; i < arguments.length; i++) {
      arguments[i] = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    }

    udf.initialize(arguments);

    runAndVerify(new Double[] { 11.4, 11.5, 11.2 }, 11.2, udf);
    runAndVerify(new Double[] { 1.0, 13.3, 2.0 }, 1.0, udf);

    runAndVerify(new Double[] { -11.4, -13.1, -12.2 }, -13.1, udf);
    runAndVerify(new Double[] { 1.0, -13.3, 2.2 }, -13.3, udf);

    runAndVerify(new Double[] { null, 1.1, 2.2 }, 1.1, udf);
    runAndVerify(new Double[] { 1.1, null, 2.2 }, 1.1, udf);
    runAndVerify(new Double[] { 1.1, 2.2, null }, 1.1, udf);

    runAndVerify(new Double[] { null, null, null }, null, udf);
  }

  public void testLeastDate() throws HiveException {
    GenericUDFLeast udf = new GenericUDFLeast();
    ObjectInspector[] arguments = new ObjectInspector[3];
    for (int i = 0; i < arguments.length; i++) {
      arguments[i] = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    }

    udf.initialize(arguments);

    Date d1 = Date.valueOf("2015-03-20");
    Date d2 = Date.valueOf("2015-03-21");
    Date d3 = Date.valueOf("2014-03-20");

    runAndVerify(new Date[] { d1, d2, d3 }, d3, udf);

    runAndVerify(new Date[] { null, d2, d3 }, d3, udf);
    runAndVerify(new Date[] { d1, null, d3 }, d3, udf);
    runAndVerify(new Date[] { d1, d2, null }, d1, udf);

    runAndVerify(new Date[] { null, null, null }, null, udf);
  }

  private void runAndVerify(String[] v, String expResult, GenericUDF udf) throws HiveException {
    DeferredObject[] args = new DeferredObject[v.length];
    for (int i = 0; i < v.length; i++) {
      args[i] = new DeferredJavaObject(v[i] != null ? new Text(v[i]) : null);
    }
    Text output = (Text) udf.evaluate(args);
    assertEquals("least() test ", expResult, output != null ? output.toString() : null);
  }

  private void runAndVerify(Integer[] v, Integer expResult, GenericUDF udf) throws HiveException {
    DeferredObject[] args = new DeferredObject[v.length];
    for (int i = 0; i < v.length; i++) {
      args[i] = new DeferredJavaObject(v[i] != null ? new IntWritable(v[i]) : null);
    }
    IntWritable output = (IntWritable) udf.evaluate(args);
    Integer res = output != null ? Integer.valueOf(output.get()) : null;
    assertEquals("least() test ", expResult, res);
  }

  private void runAndVerify(Double[] v, Double expResult, GenericUDF udf) throws HiveException {
    DeferredObject[] args = new DeferredObject[v.length];
    for (int i = 0; i < v.length; i++) {
      args[i] = new DeferredJavaObject(v[i] != null ? new DoubleWritable(v[i]) : null);
    }
    DoubleWritable output = (DoubleWritable) udf.evaluate(args);
    Double res = output != null ? Double.valueOf(output.get()) : null;
    assertEquals("least() test ", expResult, res);
  }

  private void runAndVerify(Date[] v, Date expResult, GenericUDF udf) throws HiveException {
    DeferredObject[] args = new DeferredObject[v.length];
    for (int i = 0; i < v.length; i++) {
      args[i] = new DeferredJavaObject(v[i] != null ? new DateWritable(v[i]) : null);
    }
    DateWritable output = (DateWritable) udf.evaluate(args);
    Date res = output != null ? output.get() : null;
    assertEquals("least() test ", expResult, res);
  }
}
