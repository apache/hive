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



import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import org.junit.Test;

/**
 * TestGenericUDFLevenshtein.
 */
public class TestGenericUDFLevenshtein {

  @Test
  public void testLevenshtein() throws HiveException {
    GenericUDFLevenshtein udf = new GenericUDFLevenshtein();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);
    runAndVerify("kitten", "sitting", 3, udf);
    runAndVerify("Test String1", "Test String2", 1, udf);
    runAndVerify("Test String1", "test String2", 2, udf);

    runAndVerify("Test String1", "", 12, udf);
    runAndVerify("", "Test String2", 12, udf);

    runAndVerify(null, "sitting", null, udf);
    runAndVerify("kitten", null, null, udf);
    runAndVerify(null, null, null, udf);
  }

  @Test
  public void testLevenshteinWrongType0() throws HiveException {
    @SuppressWarnings("resource")
    GenericUDFLevenshtein udf = new GenericUDFLevenshtein();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    try {
      udf.initialize(arguments);
      assertTrue("levenshtein test. UDFArgumentTypeException is expected", false);
    } catch (UDFArgumentTypeException e) {
      assertEquals("levenshtein test",
          "levenshtein only takes STRING_GROUP, VOID_GROUP types as 1st argument, got INT", e.getMessage());
    }
  }

  @Test
  public void testLevenshteinWrongType1() throws HiveException {
    @SuppressWarnings("resource")
    GenericUDFLevenshtein udf = new GenericUDFLevenshtein();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableHiveVarcharObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    try {
      udf.initialize(arguments);
      assertTrue("levenshtein test. UDFArgumentTypeException is expected", false);
    } catch (UDFArgumentTypeException e) {
      assertEquals("levenshtein test",
          "levenshtein only takes STRING_GROUP, VOID_GROUP types as 2nd argument, got FLOAT", e.getMessage());
    }
  }

  @Test
  public void testLevenshteinWrongLength() throws HiveException {
    @SuppressWarnings("resource")
    GenericUDFLevenshtein udf = new GenericUDFLevenshtein();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableHiveVarcharObjectInspector;
    ObjectInspector[] arguments = { valueOI0 };

    try {
      udf.initialize(arguments);
      assertTrue("levenshtein test. UDFArgumentLengthException is expected", false);
    } catch (UDFArgumentLengthException e) {
      assertEquals("levenshtein test", "levenshtein requires 2 argument(s), got 1", e.getMessage());
    }
  }

  private void runAndVerify(String str0, String str1, Integer expResult, GenericUDF udf)
      throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(str0 != null ? new Text(str0) : null);
    DeferredObject valueObj1 = new DeferredJavaObject(str1 != null ? new Text(str1) : null);
    DeferredObject[] args = { valueObj0, valueObj1 };
    IntWritable output = (IntWritable) udf.evaluate(args);
    if (expResult == null) {
      assertNull("levenshtein test ", output);
    } else {
      assertNotNull("levenshtein test", output);
      assertEquals("levenshtein test", expResult.intValue(), output.get());
    }
  }
}
