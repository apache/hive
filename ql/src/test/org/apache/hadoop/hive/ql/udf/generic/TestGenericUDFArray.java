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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestGenericUDFArray {
  protected AbstractGenericUDFArrayBase udf = null;

  protected void runAndVerify(List<Object> actual, List<Object> expected) throws HiveException {
    GenericUDF.DeferredJavaObject[] args = { new GenericUDF.DeferredJavaObject(actual) };
    if (udf != null) {
      List<?> result = (List<?>) udf.evaluate(args);
      if ((null == actual)) {
        Assert.assertEquals(actual, result);
      } else {
        Assert.assertArrayEquals("Check content", expected.toArray(), result.toArray());
      }
    }
  }

  @Test public void testNullAndEmptyArray() throws HiveException {
    ObjectInspector[] inputOIs = { ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.writableVoidObjectInspector) };
    if (udf != null) {
      udf.initialize(inputOIs);
    }
    runAndVerify(null, null);
  }
}
