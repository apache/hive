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
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

public class TestGenericUDFArrayIntersect {
  private final GenericUDFArrayIntersect udf = new GenericUDFArrayIntersect();

  @Test
  public void testPrimitive() throws HiveException {
    ObjectInspector intObjectInspector = ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.writableIntObjectInspector);
    ObjectInspector floatObjectInspector = ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
    ObjectInspector doubleObjectInspector = ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
    ObjectInspector longObjectInspector = ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.writableLongObjectInspector);
    ObjectInspector stringObjectInspector = ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.writableStringObjectInspector);

    Object i1 = new IntWritable(1);
    Object i2 = new IntWritable(2);
    Object i3 = new IntWritable(4);
    Object i4 = new IntWritable(5);
    Object i5 = new IntWritable(1);
    Object i6 = new IntWritable(3);
    Object i7 = new IntWritable(2);
    Object i8 = new IntWritable(9);
    List<Object> inputList = new ArrayList<>();
    inputList.add(i1);
    inputList.add(i2);
    inputList.add(i3);
    inputList.add(i4);

    udf.initialize(new ObjectInspector[] { intObjectInspector, intObjectInspector });
    runAndVerify(inputList, asList(i5, i6, i7, i8), asList(i1, i2));

    i1 = new FloatWritable(3.3f);
    i2 = new FloatWritable(1.1f);
    i3 = new FloatWritable(4.3f);
    i4 = new FloatWritable(2.22f);
    i5 = new FloatWritable(3.3f);
    i6 = new FloatWritable(1.1f);
    i7 = new FloatWritable(2.28f);
    i8 = new FloatWritable(2.20f);
    List<Object> inputFloatList = new ArrayList<>();
    inputFloatList.add(i1);
    inputFloatList.add(i2);
    inputFloatList.add(i3);
    inputFloatList.add(i4);

    udf.initialize(new ObjectInspector[] { floatObjectInspector, floatObjectInspector });
    runAndVerify(new ArrayList<>(inputFloatList), asList(i5, i6, i7, i8), asList(i1, i2));

    Object s1 = new Text("1");
    Object s2 = new Text("2");
    Object s3 = new Text("4");
    Object s4 = new Text("5");
    List<Object> inputStringList = new ArrayList<>();
    inputStringList.add(s1);
    inputStringList.add(s2);
    inputStringList.add(s3);
    inputStringList.add(s4);

    udf.initialize(new ObjectInspector[] { stringObjectInspector, stringObjectInspector });
    runAndVerify(inputStringList,asList(s1,s3),asList(s1,s3));
    // Empty array output
    runAndVerify(inputStringList,inputStringList,inputStringList);
    runAndVerify(inputStringList,asList(),asList());
    // Empty input arrays
    runAndVerify(asList(),asList(),asList());
    // Int & float arrays
    UDFArgumentTypeException exception = Assert.assertThrows(UDFArgumentTypeException.class, () -> udf.initialize(new ObjectInspector[] { floatObjectInspector, intObjectInspector }));
    Assert.assertEquals(GenericUDFArrayIntersect.ERROR_NOT_COMPARABLE,exception.getMessage());
    // float and string arrays
    exception = Assert.assertThrows(UDFArgumentTypeException.class, () -> udf.initialize(new ObjectInspector[] { floatObjectInspector, stringObjectInspector }));
    Assert.assertEquals(GenericUDFArrayIntersect.ERROR_NOT_COMPARABLE,exception.getMessage());
    // long and double arrays
    exception = Assert.assertThrows(UDFArgumentTypeException.class, () -> udf.initialize(new ObjectInspector[] { longObjectInspector, doubleObjectInspector }));
    Assert.assertEquals(GenericUDFArrayIntersect.ERROR_NOT_COMPARABLE,exception.getMessage());
  }

  @Test public void testList() throws HiveException {
    ObjectInspector[] inputOIs = { ObjectInspectorFactory.getStandardListObjectInspector(
        ObjectInspectorFactory.getStandardListObjectInspector(
            PrimitiveObjectInspectorFactory.writableStringObjectInspector)),
        ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardListObjectInspector(
            PrimitiveObjectInspectorFactory.writableStringObjectInspector)) };
    udf.initialize(inputOIs);

    Object i1 = asList(new Text("aa1"), new Text("dd"), new Text("cc"), new Text("bb"));
    Object i2 = asList(new Text("aa2"), new Text("cc"), new Text("ba"), new Text("dd"));
    Object i3 = asList(new Text("aa3"), new Text("cc"), new Text("dd"), new Text("ee"), new Text("bb"));
    Object i4 = asList(new Text("aa4"), new Text("cc"), new Text("ddd"), new Text("bb"));
    List<Object> inputList = new ArrayList<>();
    inputList.add(i1);
    inputList.add(i2);
    inputList.add(i3);
    inputList.add(i4);
    runAndVerify(inputList, asList(i1, i2, i2), asList(i1, i2));
  }

  @Test public void testStruct() throws HiveException {
    ObjectInspector[] inputOIs = { ObjectInspectorFactory.getStandardListObjectInspector(
        ObjectInspectorFactory.getStandardStructObjectInspector(asList("f1", "f2", "f3", "f4"),
            asList(PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                PrimitiveObjectInspectorFactory.writableDateObjectInspector,
                ObjectInspectorFactory.getStandardListObjectInspector(
                    PrimitiveObjectInspectorFactory.writableIntObjectInspector)))),
        ObjectInspectorFactory.getStandardListObjectInspector(
            ObjectInspectorFactory.getStandardStructObjectInspector(asList("f1", "f2", "f3", "f4"),
                asList(PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                    PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                    PrimitiveObjectInspectorFactory.writableDateObjectInspector,
                    ObjectInspectorFactory.getStandardListObjectInspector(
                        PrimitiveObjectInspectorFactory.writableIntObjectInspector)))) };
    udf.initialize(inputOIs);

    Object i1 = asList(new Text("a"), new DoubleWritable(3.1415), new DateWritableV2(Date.of(2015, 5, 26)),
        asList(new IntWritable(1), new IntWritable(3), new IntWritable(2), new IntWritable(4)));

    Object i2 = asList(new Text("b"), new DoubleWritable(3.14), new DateWritableV2(Date.of(2015, 5, 26)),
        asList(new IntWritable(1), new IntWritable(3), new IntWritable(2), new IntWritable(4)));

    Object i3 = asList(new Text("a"), new DoubleWritable(3.1415), new DateWritableV2(Date.of(2015, 5, 25)),
        asList(new IntWritable(1), new IntWritable(3), new IntWritable(2), new IntWritable(5)));

    Object i4 = asList(new Text("a"), new DoubleWritable(3.1415), new DateWritableV2(Date.of(2015, 5, 25)),
        asList(new IntWritable(1), new IntWritable(3), new IntWritable(2), new IntWritable(4)));

    List<Object> inputList = new ArrayList<>();
    inputList.add(i1);
    inputList.add(i2);
    inputList.add(i3);
    inputList.add(i4);
    runAndVerify(inputList, asList(i1, i3), asList(i1, i3));
  }

  @Test public void testMap() throws HiveException {
    ObjectInspector[] inputOIs = { ObjectInspectorFactory.getStandardListObjectInspector(
        ObjectInspectorFactory.getStandardMapObjectInspector(
            PrimitiveObjectInspectorFactory.writableStringObjectInspector,
            PrimitiveObjectInspectorFactory.writableIntObjectInspector)),
        ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardMapObjectInspector(
            PrimitiveObjectInspectorFactory.writableStringObjectInspector,
            PrimitiveObjectInspectorFactory.writableIntObjectInspector)) };
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

    Map<Text, IntWritable> m4 = new HashMap<>();
    m3.put(new Text("e"), new IntWritable(4));
    m3.put(new Text("b"), new IntWritable(3));
    m3.put(new Text("a"), new IntWritable(1));

    List<Object> inputList = new ArrayList<>();
    inputList.add(m1);
    inputList.add(m3);
    inputList.add(m2);
    inputList.add(m4);
    inputList.add(m1);

    runAndVerify(inputList, asList(m1, m3), asList(m1, m3));
  }

  private void runAndVerify(List<Object> actual, List<Object> actual2, List<Object> expected) throws HiveException {
    GenericUDF.DeferredJavaObject[] args =
        { new GenericUDF.DeferredJavaObject(actual), new GenericUDF.DeferredJavaObject(actual2) };
    List<?> result = (List<?>) udf.evaluate(args);
    Assert.assertArrayEquals("Check content", expected.toArray(), result.toArray());
  }
}
