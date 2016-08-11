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

import static org.junit.Assert.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

public class TestGenericUDFStringToMap {

  @Test
  public void testStringToMapWithCustomDelimiters() throws HiveException {
    GenericUDFStringToMap udf = new GenericUDFStringToMap();
    initGenericUDF(udf);
    Map<String, String> expResult = new LinkedHashMap<String, String>();
    expResult.put("key1", "value1");
    expResult.put("key2", "value2");
    expResult.put("key3", "value3");
    runAndVerify("key1=value1;key2=value2;key3=value3", ";", "=", expResult, udf);
  }

  @Test
  public void testStringToMapWithDefaultDelimiters() throws HiveException {
    GenericUDFStringToMap udf = new GenericUDFStringToMap();
    initGenericUDFWithNoDelimiters(udf);
    Map<String, String> expResult = new LinkedHashMap<String, String>();
    expResult.put("key1", "value1");
    expResult.put("key2", "value2");
    expResult.put("key3", "value3");
    runAndVerify("key1:value1,key2:value2,key3:value3", expResult, udf);
  }

  @Test
  public void testStringToMapWithNullDelimiters() throws HiveException {
    GenericUDFStringToMap udf = new GenericUDFStringToMap();
    initGenericUDF(udf);
    Map<String, String> expResult = new LinkedHashMap<String, String>();
    expResult.put("key1", "value1");
    expResult.put("key2", "value2");
    expResult.put("key3", "value3");
    runAndVerify("key1:value1,key2:value2,key3:value3", null, null, expResult, udf);
  }

  @Test
  public void testStringToMapWithNullText() throws HiveException {
    GenericUDFStringToMap udf = new GenericUDFStringToMap();
    initGenericUDFWithNoDelimiters(udf);
    Map<String, String> expResult = new LinkedHashMap<String, String>();
    runAndVerify(null, expResult, udf);
  }

  @Test
  public void testStringToMapWithEmptyText() throws HiveException {
    GenericUDFStringToMap udf = new GenericUDFStringToMap();
    initGenericUDFWithNoDelimiters(udf);
    Map<String, String> expResult = new LinkedHashMap<String, String>();
    expResult.put("", null);
    runAndVerify("", expResult, udf);
  }

  @Test
  public void testStringToMapNoKey() throws HiveException {
    GenericUDFStringToMap udf = new GenericUDFStringToMap();
    initGenericUDFWithNoDelimiters(udf);
    Map<String, String> expResult = new LinkedHashMap<String, String>();
    expResult.put("", "value");
    runAndVerify(":value", expResult, udf);
  }

  @Test
  public void testStringToMapNoValue() throws HiveException {
    GenericUDFStringToMap udf = new GenericUDFStringToMap();
    initGenericUDFWithNoDelimiters(udf);
    Map<String, String> expResult = new LinkedHashMap<String, String>();
    expResult.put("key", "");
    runAndVerify("key:", expResult, udf);
  }

  @Test
  public void testStringToMapNotMatchingDelimiter() throws HiveException {
    GenericUDFStringToMap udf = new GenericUDFStringToMap();
    initGenericUDFWithNoDelimiters(udf);
    Map<String, String> expResult = new LinkedHashMap<String, String>();
    expResult.put("key=value", null);
    runAndVerify("key=value", expResult, udf);
  }

  private void initGenericUDF(GenericUDFStringToMap udf)
      throws UDFArgumentException {

    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1, valueOI2 };
    udf.initialize(arguments);
  }

  private void initGenericUDFWithNoDelimiters(GenericUDFStringToMap udf)
      throws UDFArgumentException {

    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector[] arguments = { valueOI0 };
    udf.initialize(arguments);
  }

  private void runAndVerify(String text, String delimiter1, String delimiter2,
      Map<String, String> expResult, GenericUDF udf) throws HiveException {

    DeferredObject valueObj0 = new DeferredJavaObject(text);
    DeferredObject valueObj1 = new DeferredJavaObject(delimiter1);
    DeferredObject valueObj2 = new DeferredJavaObject(delimiter2);
    DeferredObject[] args = { valueObj0, valueObj1, valueObj2 };

    @SuppressWarnings("unchecked")
    LinkedHashMap<Object, Object> output = (LinkedHashMap<Object, Object>) udf.evaluate(args);
    assertTrue("str_to_map() test", expResult.equals(output));
  }

  private void runAndVerify(String text, Map<String, String> expResult,
      GenericUDF udf) throws HiveException {

    DeferredObject valueObj0 = new DeferredJavaObject(text);
    DeferredObject[] args = { valueObj0 };
    @SuppressWarnings("unchecked")
    LinkedHashMap<Object, Object> output = (LinkedHashMap<Object, Object>) udf.evaluate(args);
    assertTrue("str_to_map() test", expResult.equals(output));
  }
}