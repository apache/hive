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

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TestGenericUDFAbs extends TestCase {

  public void testInt() throws HiveException {
    GenericUDFAbs udf = new GenericUDFAbs();
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    ObjectInspector[] arguments = {valueOI};

    udf.initialize(arguments);
    DeferredObject valueObj = new DeferredJavaObject(new IntWritable(107));
    DeferredObject[] args = {valueObj};
    IntWritable output = (IntWritable) udf.evaluate(args);

    assertEquals("abs() test for INT failed ", 107, output.get());

    valueObj = new DeferredJavaObject(new IntWritable(-107));
    args[0] = valueObj;
    output = (IntWritable) udf.evaluate(args);

    assertEquals("abs() test for INT failed ", 107, output.get());
  }

  public void testLong() throws HiveException {
    GenericUDFAbs udf = new GenericUDFAbs();
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    ObjectInspector[] arguments = {valueOI};

    udf.initialize(arguments);
    DeferredObject valueObj = new DeferredJavaObject(new LongWritable(107L));
    DeferredObject[] args = {valueObj};
    LongWritable output = (LongWritable) udf.evaluate(args);

    assertEquals("abs() test for LONG failed ", 107, output.get());

    valueObj = new DeferredJavaObject(new LongWritable(-107L));
    args[0] = valueObj;
    output = (LongWritable) udf.evaluate(args);

    assertEquals("abs() test for LONG failed ", 107, output.get());
  }

  public void testDouble() throws HiveException {
    GenericUDFAbs udf = new GenericUDFAbs();
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    ObjectInspector[] arguments = {valueOI};

    udf.initialize(arguments);
    DeferredObject valueObj = new DeferredJavaObject(new DoubleWritable(107.78));
    DeferredObject[] args = {valueObj};
    DoubleWritable output = (DoubleWritable) udf.evaluate(args);

    assertEquals("abs() test for Double failed ", 107.78, output.get());

    valueObj = new DeferredJavaObject(new DoubleWritable(-107.78));
    args[0] = valueObj;
    output = (DoubleWritable) udf.evaluate(args);

    assertEquals("abs() test for Double failed ", 107.78, output.get());
  }

  public void testFloat() throws HiveException {
    GenericUDFAbs udf = new GenericUDFAbs();
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
    ObjectInspector[] arguments = {valueOI};

    udf.initialize(arguments);
    DeferredObject valueObj = new DeferredJavaObject(new FloatWritable(107.78f));
    DeferredObject[] args = {valueObj};
    DoubleWritable output = (DoubleWritable) udf.evaluate(args);

    // Make sure flow and double equality compare works
    assertTrue("abs() test for Float failed ", Math.abs(107.78 - output.get()) < 0.0001);

    valueObj = new DeferredJavaObject(new FloatWritable(-107.78f));
    args[0] = valueObj;
    output = (DoubleWritable) udf.evaluate(args);

    assertTrue("abs() test for Float failed ", Math.abs(107.78 - output.get()) < 0.0001);
  }


  public void testText() throws HiveException {
    GenericUDFAbs udf = new GenericUDFAbs();
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = {valueOI};

    udf.initialize(arguments);
    DeferredObject valueObj = new DeferredJavaObject(new Text("123.45"));
    DeferredObject[] args = {valueObj};
    DoubleWritable output = (DoubleWritable) udf.evaluate(args);

    assertEquals("abs() test for String failed ", "123.45", output.toString());

    valueObj = new DeferredJavaObject(new Text("-123.45"));
    args[0] = valueObj;
    output = (DoubleWritable) udf.evaluate(args);

    assertEquals("abs() test for String failed ", "123.45", output.toString());

    valueObj = new DeferredJavaObject(new Text("foo"));
    args[0] = valueObj;
    output = (DoubleWritable) udf.evaluate(args);

    assertEquals("abs() test for String failed ", null, output);
  }

  public void testHiveDecimal() throws HiveException {
    GenericUDFAbs udf = new GenericUDFAbs();
    int prec = 12;
    int scale = 9;
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
        TypeInfoFactory.getDecimalTypeInfo(prec, scale));
    ObjectInspector[] arguments = {valueOI};

    PrimitiveObjectInspector outputOI = (PrimitiveObjectInspector) udf.initialize(arguments);
    // Make sure result precision/scale matches the input prec/scale
    assertEquals("result precision for abs()", prec, outputOI.precision());
    assertEquals("result scale for abs()", scale, outputOI.scale());

    DeferredObject valueObj = new DeferredJavaObject(new HiveDecimalWritable(HiveDecimal.create(
        "107.123456789")));
    DeferredObject[] args = {valueObj};
    HiveDecimalWritable output = (HiveDecimalWritable) udf.evaluate(args);

    assertEquals("abs() test for HiveDecimal failed ", 107.123456789, output.getHiveDecimal()
        .doubleValue());

    valueObj = new DeferredJavaObject(new HiveDecimalWritable(HiveDecimal.create("-107.123456789")));
    args[0] = valueObj;
    output = (HiveDecimalWritable) udf.evaluate(args);

    assertEquals("abs() test for HiveDecimal failed ", 107.123456789, output.getHiveDecimal()
        .doubleValue());

    // null input
    args[0] = new DeferredJavaObject(null);
    output = (HiveDecimalWritable) udf.evaluate(args);
    assertEquals("abs(null)", null, output);

    // if value too large, should also be null
    args[0] = new DeferredJavaObject(new HiveDecimalWritable(HiveDecimal.create("-1000.123456")));
    output = (HiveDecimalWritable) udf.evaluate(args);
    assertEquals("abs() of too large decimal value", null, output);
  }
}
