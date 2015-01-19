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

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

public class TestGenericUDFAddMonths extends TestCase {

  public void testAddMonths() throws HiveException {
    GenericUDFAddMonths udf = new GenericUDFAddMonths();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);
    runAndVerify("2014-01-14", 1, "2014-02-14", udf);
    runAndVerify("2014-01-31", 1, "2014-02-28", udf);
    runAndVerify("2014-02-28", -1, "2014-01-31", udf);
    runAndVerify("2014-02-28", 2, "2014-04-30", udf);
    runAndVerify("2014-04-30", -2, "2014-02-28", udf);
    runAndVerify("2015-02-28", 12, "2016-02-29", udf);
    runAndVerify("2016-02-29", -12, "2015-02-28", udf);
    runAndVerify("2016-01-29", 1, "2016-02-29", udf);
    runAndVerify("2016-02-29", -1, "2016-01-31", udf);
  }

  private void runAndVerify(String str, int months, String expResult, GenericUDF udf)
      throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(new Text(str));
    DeferredObject valueObj1 = new DeferredJavaObject(new IntWritable(months));
    DeferredObject[] args = { valueObj0, valueObj1 };
    Text output = (Text) udf.evaluate(args);
    assertEquals("add_months() test ", expResult, output.toString());
  }
}
