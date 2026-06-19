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

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;

import org.junit.Assert;
import org.junit.Test;

public class TestGenericUDFPrintf {

  @Test
  public void testCharVarcharArgs() throws HiveException {
    GenericUDFPrintf udf = new GenericUDFPrintf();

    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableStringObjectInspector,
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getCharTypeInfo(5)),
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getVarcharTypeInfo(7))
    };

    HiveCharWritable argChar = new HiveCharWritable();
    argChar.set("hello");
    HiveVarcharWritable argVarchar = new HiveVarcharWritable();
    argVarchar.set("world");
    DeferredObject[] args = {
        new DeferredJavaObject(new Text("1st: %s, 2nd: %s")),
        new DeferredJavaObject(argChar),
        new DeferredJavaObject(argVarchar)
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(PrimitiveObjectInspectorFactory.writableStringObjectInspector, oi);
    Text res = (Text) udf.evaluate(args);
    Assert.assertEquals("1st: hello, 2nd: world", res.toString());
  }

  @Test
  public void testCharFormat() throws HiveException {
    GenericUDFPrintf udf = new GenericUDFPrintf();

    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getCharTypeInfo(10)),
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getVarcharTypeInfo(7))
    };

    HiveCharWritable formatChar = new HiveCharWritable();
    formatChar.set("arg1=%s");
    HiveVarcharWritable argVarchar = new HiveVarcharWritable();
    argVarchar.set("world");
    DeferredObject[] args = {
        new DeferredJavaObject(formatChar),
        new DeferredJavaObject(argVarchar)
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(PrimitiveObjectInspectorFactory.writableStringObjectInspector, oi);
    Text res = (Text) udf.evaluate(args);
    Assert.assertEquals("arg1=world", res.toString());
  }

  @Test
  public void testVarcharFormat() throws HiveException {
    GenericUDFPrintf udf = new GenericUDFPrintf();

    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getVarcharTypeInfo(7)),
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getCharTypeInfo(5))
    };

    HiveCharWritable argChar = new HiveCharWritable();
    argChar.set("hello");
    HiveVarcharWritable formatVarchar = new HiveVarcharWritable();
    formatVarchar.set("arg1=%s");
    DeferredObject[] args = {
        new DeferredJavaObject(formatVarchar),
        new DeferredJavaObject(argChar)
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(PrimitiveObjectInspectorFactory.writableStringObjectInspector, oi);
    Text res = (Text) udf.evaluate(args);
    Assert.assertEquals("arg1=hello", res.toString());
  }

  @Test
  public void testDecimalArgs() throws HiveException {
    GenericUDFPrintf udf = new GenericUDFPrintf();

    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableStringObjectInspector,
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(5, 2)),
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(3, 2))
    };

    HiveDecimalWritable argDec1 = new HiveDecimalWritable();
    argDec1.set(HiveDecimal.create("234.789"));
    HiveDecimalWritable argDec2 = new HiveDecimalWritable();
    argDec2.set(HiveDecimal.create("3.5"));

    DeferredObject[] args = {
        new DeferredJavaObject(new Text("1st: %s, 2nd: %s")),
        new DeferredJavaObject(argDec1),
        new DeferredJavaObject(argDec2)
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(PrimitiveObjectInspectorFactory.writableStringObjectInspector, oi);
    Text res = (Text) udf.evaluate(args);
    Assert.assertEquals("1st: 234.79, 2nd: 3.5", res.toString());
  }

}
