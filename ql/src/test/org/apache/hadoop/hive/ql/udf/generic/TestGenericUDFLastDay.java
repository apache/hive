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
import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

public class TestGenericUDFLastDay extends TestCase {

  public void testLastDay() throws HiveException {
    GenericUDFLastDay udf = new GenericUDFLastDay();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = { valueOI0 };

    udf.initialize(arguments);
    runAndVerify("2014-01-01", "2014-01-31", udf);
    runAndVerify("2014-01-14", "2014-01-31", udf);
    runAndVerify("2014-01-31", "2014-01-31", udf);
    runAndVerify("2014-02-02", "2014-02-28", udf);
    runAndVerify("2014-02-28", "2014-02-28", udf);
    runAndVerify("2016-02-03", "2016-02-29", udf);
    runAndVerify("2016-02-28", "2016-02-29", udf);
    runAndVerify("2016-02-29", "2016-02-29", udf);

    runAndVerify("2014-01-01 10:30:45", "2014-01-31", udf);
    runAndVerify("2014-01-14 10:30:45", "2014-01-31", udf);
    runAndVerify("2014-01-31 10:30:45", "2014-01-31", udf);
    runAndVerify("2014-02-02 10:30:45", "2014-02-28", udf);
    runAndVerify("2014-02-28 10:30:45", "2014-02-28", udf);
    runAndVerify("2016-02-03 10:30:45", "2016-02-29", udf);
    runAndVerify("2016-02-28 10:30:45", "2016-02-29", udf);
    runAndVerify("2016-02-29 10:30:45", "2016-02-29", udf);
  }

  private void runAndVerify(String str, String expResult, GenericUDF udf)
      throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(new Text(str));
    DeferredObject[] args = { valueObj0 };
    Text output = (Text) udf.evaluate(args);
    assertEquals("last_day() test ", expResult, output.toString());
  }
}
