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

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;

public class TestGenericUDFArrayJoin {
  private final GenericUDFArrayJoin udf = new GenericUDFArrayJoin();

  @Test public void testPrimitive() throws HiveException {
    ObjectInspector[] inputOIs = { ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.writableIntObjectInspector),
        PrimitiveObjectInspectorFactory.writableStringObjectInspector,
        PrimitiveObjectInspectorFactory.writableStringObjectInspector };
    udf.initialize(inputOIs);

    Object i1 = new IntWritable(3);
    Object i2 = new IntWritable(1);
    Object i3 = new IntWritable(2);
    Object i4 = new IntWritable(1);
    runAndVerify(asList(i1, i2, i3, i4), ",", null, i1 + "," + i2 + "," + i3 + "," + i4);

    i1 = new FloatWritable(3.3f);
    i2 = new FloatWritable(1.1f);
    i3 = new FloatWritable(3.3f);
    i4 = new FloatWritable(2.20f);
    runAndVerify(asList(i1, null, i2, i3, null, i4), ",", ":", i1 + ",:," + i2 + "," + i3 + ",:," + i4);

    i1 = new Text("aa1");
    i2 = new Text("aa2");
    i3 = new Text("aa3");
    i4 = new Text("aa4");
    runAndVerify(asList(i1, null, i2, i3, null, i4), ":", null, i1 + ":" + i2 + ":" + i3 + ":" + i4);

  }

  private void runAndVerify(List<Object> actual, String separator, String replaceNull, String expected)
      throws HiveException {
    GenericUDF.DeferredJavaObject[] args = { new GenericUDF.DeferredJavaObject(actual),
        new GenericUDF.DeferredJavaObject(separator != null ? new Text(separator) : null),
        new GenericUDF.DeferredJavaObject(replaceNull != null ? new Text(replaceNull) : null) };
    Text result = (Text) udf.evaluate(args);
    Assert.assertEquals("Not equal", expected, result.toString());
  }
}

