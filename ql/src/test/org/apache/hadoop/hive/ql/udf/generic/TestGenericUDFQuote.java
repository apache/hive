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
import org.apache.hadoop.io.Text;

import junit.framework.TestCase;
/**
 * Source for TestGenericUDFQuote.
 */
public class TestGenericUDFQuote extends TestCase {
  public TestGenericUDFQuote() {}
  public void testQuote() throws HiveException {
    GenericUDFQuote udf = new GenericUDFQuote();
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = {valueOI};

    udf.initialize(arguments);
    runAndVerify("", "''", udf);
    runAndVerify(" ", "' '", udf);
    runAndVerify("'", "'\\''", udf);
    runAndVerify("DONT", "'DONT'", udf);
    runAndVerify(" DON'T", "' DON\\'T'", udf);
    runAndVerify("DON\\'T", "'DON\\\\'T'", udf);
  }

  private void runAndVerify(String str, String expResult, GenericUDF udf) throws HiveException {
    DeferredObject valueObj = new DeferredJavaObject(new Text(str));
    DeferredObject[] args = {valueObj};
    Text output = (Text) udf.evaluate(args);
    if (expResult != null) {
      assertEquals("quote() test ", expResult, output.toString());
    } else {
      assertNull("quote() test ", output.toString());
    }
  }
}
