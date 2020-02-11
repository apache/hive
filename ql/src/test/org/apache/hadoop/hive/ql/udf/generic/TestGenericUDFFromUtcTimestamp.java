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

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;


import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * TestGenericUDFFromUtcTimestamp.
 */
public class TestGenericUDFFromUtcTimestamp {
  public static void runAndVerify(GenericUDF udf,
      Object arg1, Object arg2, Object expected) throws HiveException {
    DeferredObject[] args = { new DeferredJavaObject(arg1), new DeferredJavaObject(arg2) };
    Object result = udf.evaluate(args);

    if (expected == null) {
      assertNull(result);
    } else {
      assertEquals(expected.toString(), result.toString());
    }
  }

  @Test
  public void testFromUtcTimestamp() throws Exception {
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    GenericUDFFromUtcTimestamp udf = new GenericUDFFromUtcTimestamp();
    ObjectInspector[] args2 = {valueOI, valueOI};
    udf.initialize(args2);

    runAndVerify(udf,
        new Text("2015-03-28 17:00:00"), new Text("Europe/London"),
        Timestamp.valueOf("2015-03-28 17:00:00"));
    runAndVerify(udf,
        new Text("2015-03-28 18:00:00"), new Text("Europe/London"),
        Timestamp.valueOf("2015-03-28 18:00:00"));
    runAndVerify(udf,
        new Text("2015-03-28 19:00:00"), new Text("Europe/London"),
        Timestamp.valueOf("2015-03-28 19:00:00"));

    // Make sure nanos are preserved
    runAndVerify(udf,
        new Text("2015-03-28 18:00:00.123456789"), new Text("Europe/London"),
        Timestamp.valueOf("2015-03-28 18:00:00.123456789"));
  }

  @Test
  public void testToUtcTimestamp() throws Exception {
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    GenericUDFToUtcTimestamp udf = new GenericUDFToUtcTimestamp();
    ObjectInspector[] args2 = {valueOI, valueOI};
    udf.initialize(args2);

    runAndVerify(udf,
        new Text("2015-03-28 17:00:00"), new Text("Europe/London"),
        Timestamp.valueOf("2015-03-28 17:00:00"));
    runAndVerify(udf,
        new Text("2015-03-28 18:00:00"), new Text("Europe/London"),
        Timestamp.valueOf("2015-03-28 18:00:00"));
    runAndVerify(udf,
        new Text("2015-03-28 19:00:00"), new Text("Europe/London"),
        Timestamp.valueOf("2015-03-28 19:00:00"));

    // Make sure nanos are preserved
    runAndVerify(udf,
        new Text("2015-03-28 18:00:00.123456789"), new Text("Europe/London"),
        Timestamp.valueOf("2015-03-28 18:00:00.123456789"));
  }
}
