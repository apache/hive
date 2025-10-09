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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

public class TestGenericUDFArrayPosition {
  private final GenericUDFArrayPosition udf = new GenericUDFArrayPosition();

  @Test public void testPrimitive() throws HiveException {
    ObjectInspector[] inputOIs = { ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.writableIntObjectInspector),
        PrimitiveObjectInspectorFactory.writableIntObjectInspector };
    udf.initialize(inputOIs);

    Object i1 = new IntWritable(3);
    Object i2 = new IntWritable(1);
    Object i3 = new IntWritable(2);
    Object i4 = new IntWritable(1);
    Object i5 = new IntWritable(5);

    runAndVerify(asList(i1, i2, i3, i4), i2, 2);
    runAndVerify(asList(i1, i2, i3, i4), i5, 0);

    ObjectInspector[] inputOIfs = { ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.writableFloatObjectInspector),
        PrimitiveObjectInspectorFactory.writableFloatObjectInspector };
    udf.initialize(inputOIfs);

    i1 = new FloatWritable(3.3f);
    i2 = new FloatWritable(1.1f);
    i3 = new FloatWritable(3.3f);
    i4 = new FloatWritable(2.20f);
    runAndVerify(asList(i1, i2, i3, i4), i1, 1);
    runAndVerify(asList(i1, i2, i3, i4),null,null); //Test null element
  }

  @Test public void testList() throws HiveException {
    ObjectInspector[] inputOIs = { ObjectInspectorFactory.getStandardListObjectInspector(
        ObjectInspectorFactory.getStandardListObjectInspector(
            PrimitiveObjectInspectorFactory.writableStringObjectInspector)),
        ObjectInspectorFactory.getStandardListObjectInspector(
            PrimitiveObjectInspectorFactory.writableStringObjectInspector) };
    udf.initialize(inputOIs);

    Object i1 = asList(new Text("aa1"), new Text("dd"), new Text("cc"), new Text("bb"));
    Object i2 = asList(new Text("aa2"), new Text("cc"), new Text("ba"), new Text("dd"));
    Object i3 = asList(new Text("aa3"), new Text("cc"), new Text("dd"), new Text("ee"), new Text("bb"));
    Object i4 = asList(new Text("aa4"), new Text("cc"), new Text("ddd"), new Text("bb"));
    runAndVerify(asList(i1, i2, i2, i3, i4, i4), i2, 2);
  }

  @Test public void testStruct() throws HiveException {
    ObjectInspector[] inputOIs = { ObjectInspectorFactory.getStandardListObjectInspector(
        ObjectInspectorFactory.getStandardStructObjectInspector(asList("f1", "f2", "f3", "f4"),
            asList(PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDateObjectInspector,
                ObjectInspectorFactory.getStandardListObjectInspector(
                    PrimitiveObjectInspectorFactory.writableIntObjectInspector)))),
        ObjectInspectorFactory.getStandardStructObjectInspector(asList("f1", "f2", "f3", "f4"),
            asList(PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDateObjectInspector,
                ObjectInspectorFactory.getStandardListObjectInspector(
                    PrimitiveObjectInspectorFactory.writableIntObjectInspector))) };
    udf.initialize(inputOIs);

    Object i1 = asList(new Text("a"), new DoubleWritable(3.1415), new DateWritableV2(Date.of(2015, 5, 26)),
        asList(new IntWritable(1), new IntWritable(3), new IntWritable(2), new IntWritable(4)));

    Object i2 = asList(new Text("b"), new DoubleWritable(3.14), new DateWritableV2(Date.of(2015, 5, 26)),
        asList(new IntWritable(1), new IntWritable(3), new IntWritable(2), new IntWritable(4)));

    Object i3 = asList(new Text("a"), new DoubleWritable(3.1415), new DateWritableV2(Date.of(2015, 5, 25)),
        asList(new IntWritable(1), new IntWritable(3), new IntWritable(2), new IntWritable(5)));

    Object i4 = asList(new Text("a"), new DoubleWritable(3.1415), new DateWritableV2(Date.of(2015, 5, 25)),
        asList(new IntWritable(1), new IntWritable(3), new IntWritable(2), new IntWritable(4)));

    runAndVerify(asList(i1, i3, i2, i3, i4, i2), i2, 3);
  }

  @Test public void testMap() throws HiveException {
    ObjectInspector[] inputOIs = { ObjectInspectorFactory.getStandardListObjectInspector(
        ObjectInspectorFactory.getStandardMapObjectInspector(
            PrimitiveObjectInspectorFactory.writableStringObjectInspector,
            PrimitiveObjectInspectorFactory.writableIntObjectInspector)),
        ObjectInspectorFactory.getStandardMapObjectInspector(
            PrimitiveObjectInspectorFactory.writableStringObjectInspector,
            PrimitiveObjectInspectorFactory.writableIntObjectInspector) };
    udf.initialize(inputOIs);

    Map<Text, IntWritable> m1 = new HashMap<>();
    m1.put(new Text("a"), new IntWritable(4));
    m1.put(new Text("b"), new IntWritable(3));
    m1.put(new Text("c"), new IntWritable(1));
    m1.put(new Text("d"), new IntWritable(2));

    Map<Text, IntWritable> m2 = new HashMap<>();
    m2.put(new Text("d"), new IntWritable(4));
    m2.put(new Text("b"), new IntWritable(3));
    m2.put(new Text("a"), new IntWritable(1));
    m2.put(new Text("c"), new IntWritable(2));

    Map<Text, IntWritable> m3 = new HashMap<>();
    m3.put(new Text("d"), new IntWritable(4));
    m3.put(new Text("b"), new IntWritable(3));
    m3.put(new Text("a"), new IntWritable(1));

    runAndVerify(asList(m1, m3, m2, m3, m1), m1, 1);
  }

  private void runAndVerify(List<Object> actual, Object element, Object expected)
      throws HiveException {
    GenericUDF.DeferredJavaObject[] args = { new GenericUDF.DeferredJavaObject(actual), new GenericUDF.DeferredJavaObject(element) };
    Object result = udf.evaluate(args);
    if(expected == null){
      Assert.assertNull(result);
    }
    else {
      Assert.assertEquals("index value", expected, ((IntWritable)result).get());
    }
  }
}
