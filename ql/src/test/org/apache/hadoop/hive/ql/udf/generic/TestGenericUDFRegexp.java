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

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

public class TestGenericUDFRegexp extends TestCase {

  public void testConstant() throws HiveException {
    GenericUDFRegExp udf = new GenericUDFRegExp();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    Text regexText = new Text("^fo");
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, regexText);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyConst("fofo", regexText, true, udf);
    runAndVerifyConst("fofofo", regexText, true, udf);
    runAndVerifyConst("fobar", regexText, true, udf);
    runAndVerifyConst("barfobar", regexText, false, udf);
    // null
    runAndVerifyConst(null, regexText, null, udf);
  }

  public void testEmptyConstant() throws HiveException {
    GenericUDFRegExp udf = new GenericUDFRegExp();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    Text regexText = new Text("");
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, regexText);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    // empty regex (should be one WARN message)
    runAndVerifyConst("foo", regexText, false, udf);
    runAndVerifyConst("bar", regexText, false, udf);
    // null
    runAndVerifyConst(null, regexText, null, udf);
  }

  public void testNullConstant() throws HiveException {
    GenericUDFRegExp udf = new GenericUDFRegExp();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    Text regexText = null;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, regexText);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);
    // null
    runAndVerifyConst("fofo", regexText, null, udf);
    runAndVerifyConst("fofofo", regexText, null, udf);
    runAndVerifyConst("fobar", regexText, null, udf);
    runAndVerifyConst(null, regexText, null, udf);
  }

  public void testNonConstant() throws HiveException {
    GenericUDFRegExp udf = new GenericUDFRegExp();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerify("fofo", "^fo", true, udf);
    runAndVerify("fo\no", "^fo\no$", true, udf);
    runAndVerify("Bn", "^Ba*n", true, udf);
    runAndVerify("afofo", "fo", true, udf);
    runAndVerify("afofo", "^fo", false, udf);
    runAndVerify("Baan", "^Ba?n", false, udf);
    runAndVerify("axe", "pi|apa", false, udf);
    runAndVerify("pip", "^(pi)*$", false, udf);
    // empty regex (should be one WARN message)
    runAndVerify("bar", "", false, udf);
    runAndVerify("foo", "", false, udf);
    // null
    runAndVerify(null, "^fo", null, udf);
    runAndVerify("fofo", null, null, udf);
  }

  private void runAndVerifyConst(String str, Text regexText, Boolean expResult, GenericUDF udf)
      throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(str != null ? new Text(str) : null);
    DeferredObject valueObj1 = new DeferredJavaObject(regexText);
    DeferredObject[] args = { valueObj0, valueObj1 };
    BooleanWritable output = (BooleanWritable) udf.evaluate(args);
    if (expResult == null) {
      assertNull(output);
    } else {
      assertNotNull(output);
      assertEquals("regexp() const test ", expResult.booleanValue(), output.get());
    }
  }

  private void runAndVerify(String str, String regex, Boolean expResult, GenericUDF udf)
      throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(str != null ? new Text(str) : null);
    DeferredObject valueObj1 = new DeferredJavaObject(regex != null ? new Text(regex) : null);
    DeferredObject[] args = { valueObj0, valueObj1 };
    BooleanWritable output = (BooleanWritable) udf.evaluate(args);
    if (expResult == null) {
      assertNull(output);
    } else {
      assertNotNull(output);
      assertEquals("regexp() test ", expResult.booleanValue(), output.get());
    }
  }
}
