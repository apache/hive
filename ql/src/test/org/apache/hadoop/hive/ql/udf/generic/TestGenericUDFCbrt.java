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

import junit.framework.TestCase;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class TestGenericUDFCbrt extends TestCase {

  public void testCbrt() throws HiveException {
    GenericUDFCbrt udf = new GenericUDFCbrt();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    ObjectInspector[] arguments = { valueOI0 };

    udf.initialize(arguments);

    runAndVerify(0.0, 0.0, udf);
    runAndVerify(1.0, 1.0, udf);
    runAndVerify(-1.0, -1.0, udf);
    runAndVerify(27.0, 3.0, udf);
    runAndVerify(-27.0, -3.0, udf);
    runAndVerify(87860583272930481.0, 444561.0, udf);
    runAndVerify(null, null, udf);
  }

  private void runAndVerify(Double in, Double expResult, GenericUDF udf) throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(in != null ? new DoubleWritable(in) : null);
    DeferredObject[] args = { valueObj0 };
    DoubleWritable output = (DoubleWritable) udf.evaluate(args);
    if (expResult == null) {
      assertNull("cbrt() test", output);
    } else {
      assertNotNull("cbrt() test", output);
      assertEquals("cbrt() test", expResult.doubleValue(), output.get(), 1E-10);
    }
  }
}
