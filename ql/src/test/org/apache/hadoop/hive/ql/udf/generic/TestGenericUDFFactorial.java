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

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class TestGenericUDFFactorial extends TestCase {

  public void testFactorial() throws HiveException {
    GenericUDFFactorial udf = new GenericUDFFactorial();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    ObjectInspector[] arguments = { valueOI0 };

    udf.initialize(arguments);

    // date str
    runAndVerify(5, 120L, udf);
    runAndVerify(0, 1L, udf);
    runAndVerify(20, 2432902008176640000L, udf);
    // outside of [0..20] range
    runAndVerify(-1, null, udf);
    runAndVerify(21, null, udf);
    // null input
    runAndVerify(null, null, udf);
  }

  public void testWrongInputType() throws HiveException {
    @SuppressWarnings("resource")
    GenericUDFFactorial udf = new GenericUDFFactorial();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    ObjectInspector[] arguments = { valueOI0 };

    try {
      udf.initialize(arguments);
      assertTrue("GenericUDFFactorial.initialize() shold throw UDFArgumentTypeException", false);
    } catch (UDFArgumentTypeException e) {
      // UDFArgumentTypeException is expected
    }
  }

  private void runAndVerify(Integer in, Long expResult, GenericUDF udf) throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(in != null ? new IntWritable(in) : null);
    DeferredObject[] args = { valueObj0 };
    LongWritable output = (LongWritable) udf.evaluate(args);
    if (expResult == null) {
      assertNull("factorial() test ", output);
    } else {
      assertNotNull("factorial() test ", output);
      assertEquals("factorial() test ", expResult.longValue(), output.get());
    }
  }
}
