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

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;

import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;
import org.junit.Before;
import org.junit.Test;

public class TestGenericUDFMacro {

  private String name;
  private GenericUDFMacro udf;
  private List<String> colNames;
  private List<TypeInfo> colTypes;
  private ObjectInspector[] inspectors;
  private DeferredObject[] arguments;
  private IntWritable x;
  private IntWritable y;
  private ExprNodeConstantDesc bodyDesc;
  private int expected;
  @Before
  public void setup() throws Exception {
    name = "fixed_number";
    colNames = new ArrayList<String>();
    colTypes = new ArrayList<TypeInfo>();
    colNames.add("x");
    colTypes.add(TypeInfoFactory.intTypeInfo);
    colNames.add("y");
    colTypes.add(TypeInfoFactory.intTypeInfo);
    x = new IntWritable(1);
    y = new IntWritable(2);
    expected = x.get() + y.get();
    bodyDesc = new ExprNodeConstantDesc(expected);
    inspectors = new ObjectInspector[] {
        PrimitiveObjectInspectorFactory.
          getPrimitiveWritableConstantObjectInspector(
              TypeInfoFactory.intTypeInfo, x),
        PrimitiveObjectInspectorFactory.
          getPrimitiveWritableConstantObjectInspector(
              TypeInfoFactory.intTypeInfo, y),
    };
    arguments = new DeferredObject[] {
        new DeferredJavaObject(x),
        new DeferredJavaObject(y)
    };
  }

  @Test
  public void testUDF() throws Exception {
    udf = new GenericUDFMacro(name, bodyDesc, colNames, colTypes);
    udf.initialize(inspectors);
    Object actual = udf.evaluate(arguments);
    Assert.assertEquals(bodyDesc.getValue(), ((IntWritable)actual).get());
    Assert.assertTrue(udf.isDeterministic());
    Assert.assertFalse(udf.isStateful());
    Assert.assertEquals(name, udf.getMacroName());
    Assert.assertEquals(bodyDesc, udf.getBody());
    Assert.assertEquals(colNames, udf.getColNames());
    Assert.assertEquals(colTypes, udf.getColTypes());
    Assert.assertEquals(name + "(x, y)", udf.getDisplayString(new String[] { "x", "y"}));
  }
  @Test
  public void testNoArgsContructor() throws Exception {
    udf = new GenericUDFMacro();
    Assert.assertTrue(udf.isDeterministic());
    Assert.assertFalse(udf.isStateful());
  }
}
