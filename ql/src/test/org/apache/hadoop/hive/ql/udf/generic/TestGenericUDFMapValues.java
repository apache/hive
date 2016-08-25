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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

public class TestGenericUDFMapValues {

  @Test
  public void testNullMap() throws HiveException, IOException {
    ObjectInspector[] inputOIs = {
        ObjectInspectorFactory.getStandardMapObjectInspector(
            PrimitiveObjectInspectorFactory.writableStringObjectInspector,
            PrimitiveObjectInspectorFactory.writableStringObjectInspector),
    };

    Map<String, String> input = null;
    DeferredObject[] args = {
        new DeferredJavaObject(input)
    };

  GenericUDFMapValues udf = new GenericUDFMapValues();
    StandardListObjectInspector oi = (StandardListObjectInspector) udf.initialize(inputOIs);
    Object res = udf.evaluate(args);
    Assert.assertTrue(oi.getList(res).isEmpty());
    udf.close();
  }

}
