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

package org.apache.hadoop.hive.ql.udf;



import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDateFormat;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToInteger;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.Test;

/**
 * TestToInteger.
 */
public class TestToInteger{

  @Test
  public void testTextToInteger() throws Exception{
    GenericUDFToInteger udf = new GenericUDFToInteger();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = {valueOI0};
    udf.initialize(arguments);

    runAndVerifyStr("-1",-1,udf);
    runAndVerifyStr("0",0,udf);
    runAndVerifyStr("1.1",1,udf);

    runAndVerifyNull("A",udf);
  }

  private void runAndVerifyStr(String str, int expResult, GenericUDF udf)
      throws HiveException {
    GenericUDF.DeferredObject valueObj0 = new GenericUDF.DeferredJavaObject(str != null ? new Text(str) : null);
    GenericUDF.DeferredObject[] args = { valueObj0 };
    int output = (int) udf.evaluate(args);
    assertEquals("Cast ( str as INT) ", expResult, output);
  }

  private void runAndVerifyNull(String str, GenericUDF udf)
      throws HiveException {
    GenericUDF.DeferredObject valueObj0 = new GenericUDF.DeferredJavaObject(str != null ? new Text(str) : null);
    GenericUDF.DeferredObject[] args = { valueObj0 };
    int output = (int) udf.evaluate(args);
    assertNull("Cast ( str as INT) ", output);
  }
}
