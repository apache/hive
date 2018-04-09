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
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TestGenericUDFGreatest extends TestCase {

  public void testOneArg() throws HiveException {
    @SuppressWarnings("resource")
    GenericUDFGreatest udf = new GenericUDFGreatest();
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = { valueOI1 };

    UDFArgumentException ex = null;
    try {
      udf.initialize(arguments);
    } catch (UDFArgumentException e) {
      ex = e;
    }
    assertNotNull("greatest() test ", ex);
  }

  public void testVoids() throws HiveException {
    GenericUDFGreatest udf = new GenericUDFGreatest();
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableVoidObjectInspector;
    ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    ObjectInspector valueOI3 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = { valueOI1, valueOI2, valueOI3 };
    udf.initialize(arguments);
    runAndVerify(new Object[] { null, 1, "test"}, null, udf);
  }

  public void testGreatestMixed() throws HiveException {
    GenericUDFGreatest udf = new GenericUDFGreatest();
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    ObjectInspector valueOI3 = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    ObjectInspector valueOI4 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = { valueOI1, valueOI2, valueOI3, valueOI4 };
    udf.initialize(arguments);
    runAndVerify(new Object[] { 1, 11.1, Date.valueOf("2015-03-20"), "test"}, "test", udf);  //string comparisons
  }


  public void testGreatestStr() throws HiveException {
    GenericUDFGreatest udf = new GenericUDFGreatest();
    ObjectInspector[] arguments = new ObjectInspector[3];
    for (int i = 0; i < arguments.length; i++) {
      arguments[i] = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    udf.initialize(arguments);

    runAndVerify(new String[] { "a", "b", "c" }, "c", udf);
    runAndVerify(new String[] { "C", "a", "B" }, "a", udf);
    runAndVerify(new String[] { "AAA", "AaA", "AAa" }, "AaA", udf);
    runAndVerify(new String[] { "A", "AA", "AAA" }, "AAA", udf);

    runAndVerify(new String[] { "11", "13", "12" }, "13", udf);
    runAndVerify(new String[] { "11", "2", "12" }, "2", udf);
    runAndVerify(new String[] { "01", "03", "02" }, "03", udf);
    runAndVerify(new String[] { "01", "1", "02" }, "1", udf);

    runAndVerify(new String[] { null, "b", "c" }, null, udf);
    runAndVerify(new String[] { "a", null, "c" }, null, udf);
    runAndVerify(new String[] { "a", "b", null }, null, udf);

    runAndVerify(new String[] { "a", null, null }, null, udf);
    runAndVerify(new String[] { null, "b", null }, null, udf);
    runAndVerify(new String[] { null, null, null }, null, udf);
  }

  public void testGreatestInt() throws HiveException {
    GenericUDFGreatest udf = new GenericUDFGreatest();
    ObjectInspector[] arguments = new ObjectInspector[3];
    for (int i = 0; i < arguments.length; i++) {
      arguments[i] = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    }

    udf.initialize(arguments);

    runAndVerify(new Integer[] { 11, 13, 12 }, 13, udf);
    runAndVerify(new Integer[] { 1, 13, 2 }, 13, udf);

    runAndVerify(new Integer[] { -11, -13, -12 }, -11, udf);
    runAndVerify(new Integer[] { 1, -13, 2 }, 2, udf);

    runAndVerify(new Integer[] { null, 1, 2 }, null, udf);
    runAndVerify(new Integer[] { 1, null, 2 }, null, udf);
    runAndVerify(new Integer[] { 1, 2, null }, null, udf);

    runAndVerify(new Integer[] { null, null, null }, null, udf);
  }

  public void testGreatestDouble() throws HiveException {
    GenericUDFGreatest udf = new GenericUDFGreatest();
    ObjectInspector[] arguments = new ObjectInspector[3];
    for (int i = 0; i < arguments.length; i++) {
      arguments[i] = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    }

    udf.initialize(arguments);

    runAndVerify(new Double[] { 11.4, 11.5, 11.2 }, 11.5, udf);
    runAndVerify(new Double[] { 1.0, 13.3, 2.0 }, 13.3, udf);

    runAndVerify(new Double[] { -11.4, -13.1, -12.2 }, -11.4, udf);
    runAndVerify(new Double[] { 1.0, -13.3, 2.2 }, 2.2, udf);

    runAndVerify(new Double[] { null, 1.1, 2.2 }, null, udf);
    runAndVerify(new Double[] { 1.1, null, 2.2 }, null, udf);
    runAndVerify(new Double[] { 1.1, 2.2, null }, null, udf);

    runAndVerify(new Double[] { null, null, null }, null, udf);
  }

  public void testGreatestDate() throws HiveException {
    GenericUDFGreatest udf = new GenericUDFGreatest();
    ObjectInspector[] arguments = new ObjectInspector[3];
    for (int i = 0; i < arguments.length; i++) {
      arguments[i] = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    }

    udf.initialize(arguments);

    Date d1 = Date.valueOf("2015-03-20");
    Date d2 = Date.valueOf("2015-03-21");
    Date d3 = Date.valueOf("2014-03-20");

    runAndVerify(new Date[] { d1, d2, d3 }, d2, udf);

    runAndVerify(new Date[] { null, d2, d3 }, null, udf);
    runAndVerify(new Date[] { d1, null, d3 }, null, udf);
    runAndVerify(new Date[] { d1, d2, null }, null, udf);

    runAndVerify(new Date[] { null, null, null }, null, udf);
  }

  public void testGreatestIntTypes() throws HiveException {
    GenericUDFGreatest udf = new GenericUDFGreatest();
    ObjectInspector[] arguments = new ObjectInspector[4];

    arguments[0] = PrimitiveObjectInspectorFactory.writableByteObjectInspector;
    arguments[1] = PrimitiveObjectInspectorFactory.writableShortObjectInspector;
    arguments[2] = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    arguments[3] = PrimitiveObjectInspectorFactory.writableLongObjectInspector;


    udf.initialize(arguments);

    runAndVerify(new Object[] { (byte) 11, (short) 13, 12, 14L }, 14L, udf);
    runAndVerify(new Object[] { (byte) 1, (short) 13, 2, 0L }, 13L, udf);

    runAndVerify(new Object[] { (byte) -11, (short) -13, -12, 0L }, 0L, udf);
    runAndVerify(new Object[] { (byte) 1, (short) -13, 2, 0L}, 2L, udf);

    runAndVerify(new Object[] { null, (short) 1, 2, 0L }, null, udf);
    runAndVerify(new Object[] { (byte) 1, null, 2, -1L }, null, udf);
    runAndVerify(new Object[] { (byte) 1, (short) 2, null, -1L }, null, udf);

    runAndVerify(new Integer[] { null, null, null, null }, null, udf);
  }

  private void runAndVerify(Object[] v, Object expResult, GenericUDF udf) throws HiveException {
    DeferredObject[] args = new DeferredObject[v.length];
    for (int i = 0; i < v.length; i++) {
      args[i] = new DeferredJavaObject(getWritable(v[i]));
    }
    Object output = udf.evaluate(args);
    output = parseOutput(output);
    assertEquals("greatest() test ", expResult, output != null ? output : null);
  }

  private Object getWritable(Object o) {
    if (o instanceof String) {
      return o != null ? new Text((String) o) : null;
    } else if (o instanceof Integer) {
      return o != null ? new IntWritable((Integer) o) : null;
    } else if (o instanceof Double) {
      return o != null ? new DoubleWritable((Double) o) : null;
    } else if (o instanceof Date) {
      return o != null ? new DateWritableV2((Date) o) : null;
    } else if (o instanceof Byte) {
      return o != null ? new ByteWritable((Byte) o): null;
    } else if (o instanceof Short) {
      return o != null ? new ShortWritable((Short) o) : null;
    } else if (o instanceof Long) {
      return o != null ? new LongWritable((Long) o) : null;
    }
    return null;
  }

  private Object parseOutput(Object o) {
    if (o == null) {
      return null;
    }
    if (o instanceof Text) {
      return o.toString();
    } else if (o instanceof IntWritable) {
      return ((IntWritable) o).get();
    } else if (o instanceof DoubleWritable) {
      return ((DoubleWritable) o).get();
    } else if (o instanceof DateWritableV2) {
      return ((DateWritableV2) o).get();
    } else if (o instanceof ByteWritable) {
      return ((ByteWritable) o).get();
    } else if (o instanceof ShortWritable) {
      return ((ShortWritable) o).get();
    } else if (o instanceof LongWritable) {
      return ((LongWritable) o).get();
    }
    return null;
  }
}
