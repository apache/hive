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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.Test;

/**
 * TestGenericUDFRpad.
 */
public class TestGenericUDFRpad {

  @Test
  public void testRpad() throws HiveException {
    GenericUDFRpad udf = new GenericUDFRpad();
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    ObjectInspector valueOI3 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = { valueOI1, valueOI2, valueOI3 };

    udf.initialize(arguments);
    runAndVerify("hi", 5, "??", "hi???", udf);
    runAndVerify("hi", 1, "??", "h", udf);
    runAndVerify("ｈｉ", 5, "？？", "ｈｉ？？？", udf);
    runAndVerify("ｈｉ", 1, "？？", "ｈ", udf);
    runAndVerify("hi", 3, "", null, udf);
  }

  private void runAndVerify(String str, int len, String pad, String expResult, GenericUDF udf)
      throws HiveException {
    DeferredObject valueObj1 = new DeferredJavaObject(new Text(str));
    DeferredObject valueObj2 = new DeferredJavaObject(new IntWritable(len));
    DeferredObject valueObj3 = new DeferredJavaObject(new Text(pad));
    DeferredObject[] args = { valueObj1, valueObj2, valueObj3 };
    Object output = udf.evaluate(args);
    if(expResult != null) {
      assertEquals("rpad() test ", expResult, output.toString());
    } else {
      assertNull("rpad() test ", output);
    }
  }
}
