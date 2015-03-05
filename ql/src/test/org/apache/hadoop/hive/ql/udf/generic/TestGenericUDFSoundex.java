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

import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

public class TestGenericUDFSoundex extends TestCase {

  public void testSoundex() throws HiveException {
    GenericUDFSoundex udf = new GenericUDFSoundex();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = { valueOI0 };

    udf.initialize(arguments);
    runAndVerify("Miller", "M460", udf);
    runAndVerify("miler", "M460", udf);
    runAndVerify("myller", "M460", udf);
    runAndVerify("muller", "M460", udf);
    runAndVerify("m", "M000", udf);
    runAndVerify("mu", "M000", udf);
    runAndVerify("mil", "M400", udf);

    runAndVerify("Peterson", "P362", udf);
    runAndVerify("Pittersen", "P362", udf);

    runAndVerify("", "", udf);

    runAndVerify(null, null, udf);
    runAndVerify("\u3500\u3501\u3502\u3503", null, udf);
  }

  public void testSoundexWrongType0() throws HiveException {
    @SuppressWarnings("resource")
    GenericUDFSoundex udf = new GenericUDFSoundex();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    ObjectInspector[] arguments = { valueOI0 };

    try {
      udf.initialize(arguments);
      assertTrue("soundex test. UDFArgumentTypeException is expected", false);
    } catch (UDFArgumentTypeException e) {
      assertEquals("soundex test",
          "soundex only takes STRING_GROUP types as 1st argument, got INT", e.getMessage());
    }
  }

  public void testSoundexWrongLength() throws HiveException {
    @SuppressWarnings("resource")
    GenericUDFSoundex udf = new GenericUDFSoundex();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableHiveVarcharObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableHiveVarcharObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    try {
      udf.initialize(arguments);
      assertTrue("soundex test. UDFArgumentLengthException is expected", false);
    } catch (UDFArgumentLengthException e) {
      assertEquals("soundex test", "soundex requires 1 argument, got 2", e.getMessage());
    }
  }

  private void runAndVerify(String str0, String expResult, GenericUDF udf) throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(str0 != null ? new Text(str0) : null);
    DeferredObject[] args = { valueObj0 };
    Text output = (Text) udf.evaluate(args);
    if (expResult == null) {
      assertNull("soundex test ", output);
    } else {
      assertNotNull("soundex test", output);
      assertEquals("soundex test", expResult, output.toString());
    }
  }
}
