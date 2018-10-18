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
package org.apache.hive.hplsql;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;

public class TestHplsqlUdf {
  StringObjectInspector queryOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  ObjectInspector argOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

  /**
   * test evaluate for exec init and setParameters
   */
  @Test
  public void testEvaluateWithoutRun() throws HiveException {
    // init udf
    Udf udf = new Udf();
    ObjectInspector[] initArguments = {queryOI, argOI};
    udf.initialize(initArguments);
    //set arguments
    DeferredObject queryObj = new DeferredJavaObject("hello(:1)");
      DeferredObject argObj = new DeferredJavaObject("name");
      DeferredObject[] argumentsObj = {queryObj, argObj};
      
      // init exec and set parameters, included
      udf.initExec(argumentsObj);
      udf.setParameters(argumentsObj);
      
      // checking var exists and its value is right
      Var var = udf.exec.findVariable(":1");
      Assert.assertNotNull(var);
      String val = (String) var.value;
      Assert.assertEquals(val, "name");
  }
}

