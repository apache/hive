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
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * TestGenericUDFSubstringIndex.
 */
public class TestGenericUDFSubstringIndex {

  @Test
  public void testSubstringIndex() throws HiveException {
    GenericUDFSubstringIndex udf = new GenericUDFSubstringIndex();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1, valueOI2 };

    udf.initialize(arguments);

    runAndVerify("www.apache.org", ".", 3, "www.apache.org", udf);
    runAndVerify("www.apache.org", ".", 2, "www.apache", udf);
    runAndVerify("www.apache.org", ".", 1, "www", udf);
    runAndVerify("www.apache.org", ".", 0, "", udf);
    runAndVerify("www.apache.org", ".", -1, "org", udf);
    runAndVerify("www.apache.org", ".", -2, "apache.org", udf);
    runAndVerify("www.apache.org", ".", -3, "www.apache.org", udf);

    // str is empty string
    runAndVerify("", ".", 1, "", udf);
    // empty string delim
    runAndVerify("www.apache.org", "", 1, "", udf);
    // delim does not exist in str
    runAndVerify("www.apache.org", "-", 2, "www.apache.org", udf);
    // delim is 2 chars
    runAndVerify("www||apache||org", "||", 2, "www||apache", udf);

    // null
    runAndVerify(null, ".", 2, null, udf);
    runAndVerify("www.apache.org", null, 2, null, udf);
    runAndVerify("www.apache.org", ".", null, null, udf);
  }

  @Test
  public void testSubstringIndexConst() throws HiveException {
    GenericUDFSubstringIndex udf = new GenericUDFSubstringIndex();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    Text delim = new Text(".");
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, delim);
    IntWritable count = new IntWritable(2);
    ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, count);
    ObjectInspector[] arguments = { valueOI0, valueOI1, valueOI2 };

    udf.initialize(arguments);

    runAndVerifyConst("www.apache.org", "www.apache", udf);
  }

  private void runAndVerify(String str, String delim, Integer count, String expResult,
      GenericUDF udf) throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(str != null ? new Text(str) : null);
    DeferredObject valueObj1 = new DeferredJavaObject(delim != null ? new Text(delim) : delim);
    DeferredObject valueObj2 = new DeferredJavaObject(count != null ? new IntWritable(count) : null);
    DeferredObject[] args = { valueObj0, valueObj1, valueObj2 };
    Text output = (Text) udf.evaluate(args);
    assertEquals("substring_index() test ", expResult, output != null ? output.toString() : null);
  }

  private void runAndVerifyConst(String str, String expResult, GenericUDF udf) throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(str != null ? new Text(str) : null);
    DeferredObject[] args = { valueObj0 };
    Text output = (Text) udf.evaluate(args);
    assertEquals("substring_index() test ", expResult, output != null ? output.toString() : null);
  }
}
