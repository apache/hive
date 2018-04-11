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

import java.sql.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static java.util.Arrays.asList;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class TestGenericUDFSortArray {
  private final GenericUDFSortArray udf = new GenericUDFSortArray();

  @Test
  public void testSortPrimitive() throws HiveException {
    ObjectInspector[] inputOIs = {
        ObjectInspectorFactory.getStandardListObjectInspector(
            PrimitiveObjectInspectorFactory.writableIntObjectInspector)
    };
    udf.initialize(inputOIs);

    Object i1 = new IntWritable(3);
    Object i2 = new IntWritable(4);
    Object i3 = new IntWritable(2);
    Object i4 = new IntWritable(1);

    runAndVerify(asList(i1,i2,i3,i4), asList(i4,i3,i1,i2));
  }

  @Test
  public void testSortList() throws HiveException {
    ObjectInspector[] inputOIs = {
        ObjectInspectorFactory.getStandardListObjectInspector(
            ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.writableStringObjectInspector
            )
        )
    };
    udf.initialize(inputOIs);

    Object i1 = asList(new Text("aa"),new Text("dd"),new Text("cc"),new Text("bb"));
    Object i2 = asList(new Text("aa"),new Text("cc"),new Text("ba"),new Text("dd"));
    Object i3 = asList(new Text("aa"),new Text("cc"),new Text("dd"),new Text("ee"), new Text("bb"));
    Object i4 = asList(new Text("aa"),new Text("cc"),new Text("ddd"),new Text("bb"));

    runAndVerify(asList(i1,i2,i3,i4), asList(i2,i3,i4,i1));
  }

  @Test
  public void testSortStruct() throws HiveException {
    ObjectInspector[] inputOIs = {
        ObjectInspectorFactory.getStandardListObjectInspector(
            ObjectInspectorFactory.getStandardStructObjectInspector(
                asList("f1", "f2", "f3", "f4"),
                asList(
                    PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                    PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
                    PrimitiveObjectInspectorFactory.writableDateObjectInspector,
                    ObjectInspectorFactory.getStandardListObjectInspector(
                        PrimitiveObjectInspectorFactory.writableIntObjectInspector
                    )
                )
            )
        )
    };
    udf.initialize(inputOIs);

    Object i1 = asList(new Text("a"), new DoubleWritable(3.1415),
        new DateWritable(new Date(2015, 5, 26)),
        asList(new IntWritable(1), new IntWritable(3),
            new IntWritable(2), new IntWritable(4)));

    Object i2 = asList(new Text("b"), new DoubleWritable(3.14),
        new DateWritable(new Date(2015, 5, 26)),
        asList(new IntWritable(1), new IntWritable(3),
            new IntWritable(2), new IntWritable(4)));

    Object i3 = asList(new Text("a"), new DoubleWritable(3.1415),
        new DateWritable(new Date(2015, 5, 25)),
        asList(new IntWritable(1), new IntWritable(3),
            new IntWritable(2), new IntWritable(5)));

    Object i4 = asList(new Text("a"), new DoubleWritable(3.1415),
        new DateWritable(new Date(2015, 5, 25)),
        asList(new IntWritable(1), new IntWritable(3),
            new IntWritable(2), new IntWritable(4)));

    runAndVerify(asList(i1,i2,i3,i4), asList(i4,i3,i1,i2));
  }

  @Test
  public void testSortMap() throws HiveException {
    ObjectInspector[] inputOIs = {
        ObjectInspectorFactory.getStandardListObjectInspector(
            ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableIntObjectInspector
            )
        )
    };
    udf.initialize(inputOIs);

    Map<Text, IntWritable> m1 = new HashMap<Text, IntWritable>();
    m1.put(new Text("a"), new IntWritable(4));
    m1.put(new Text("b"), new IntWritable(3));
    m1.put(new Text("c"), new IntWritable(1));
    m1.put(new Text("d"), new IntWritable(2));

    Map<Text, IntWritable> m2 = new HashMap<Text, IntWritable>();
    m2.put(new Text("d"), new IntWritable(4));
    m2.put(new Text("b"), new IntWritable(3));
    m2.put(new Text("a"), new IntWritable(1));
    m2.put(new Text("c"), new IntWritable(2));

    Map<Text, IntWritable> m3 = new HashMap<Text, IntWritable>();
    m3.put(new Text("d"), new IntWritable(4));
    m3.put(new Text("b"), new IntWritable(3));
    m3.put(new Text("a"), new IntWritable(1));

    runAndVerify(asList((Object)m1, m2, m3), asList((Object)m3, m2, m1));
  }

  private void runAndVerify(List<Object> actual, List<Object> expected)
      throws HiveException {
    GenericUDF.DeferredJavaObject[] args = { new GenericUDF.DeferredJavaObject(actual) };
    List<Object> result = (List<Object>) udf.evaluate(args);

    Assert.assertEquals("Check size", expected.size(), result.size());
    Assert.assertArrayEquals("Check content", expected.toArray(), result.toArray());
  }
}
