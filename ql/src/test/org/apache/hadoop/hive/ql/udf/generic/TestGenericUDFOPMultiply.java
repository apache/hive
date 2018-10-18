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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Assert;
import org.junit.Test;

public class TestGenericUDFOPMultiply extends AbstractTestGenericUDFOPNumeric {

  @Test
  public void testByteTimesShort() throws HiveException {
    GenericUDFOPMultiply udf = new GenericUDFOPMultiply();

    ByteWritable left = new ByteWritable((byte) 4);
    ShortWritable right = new ShortWritable((short) 6);
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableByteObjectInspector,
        PrimitiveObjectInspectorFactory.writableShortObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(oi.getTypeInfo(), TypeInfoFactory.shortTypeInfo);
    ShortWritable res = (ShortWritable) udf.evaluate(args);
    Assert.assertEquals(24, res.get());
  }

  @Test
  public void testVarcharTimesInt() throws HiveException {
    GenericUDFOPMultiply udf = new GenericUDFOPMultiply();

    HiveVarcharWritable left = new HiveVarcharWritable();
    left.set("123");
    IntWritable right = new IntWritable(456);
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableHiveVarcharObjectInspector,
        PrimitiveObjectInspectorFactory.writableIntObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(oi.getTypeInfo(), TypeInfoFactory.doubleTypeInfo);
    DoubleWritable res = (DoubleWritable) udf.evaluate(args);
    Assert.assertEquals(new Double(123 * 456), new Double(res.get()));
  }

  @Test
  public void testDoubleTimesLong() throws HiveException {
    GenericUDFOPMultiply udf = new GenericUDFOPMultiply();

    DoubleWritable left = new DoubleWritable(4.5);
    LongWritable right = new LongWritable(10);
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
        PrimitiveObjectInspectorFactory.writableLongObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.doubleTypeInfo, oi.getTypeInfo());
    DoubleWritable res = (DoubleWritable) udf.evaluate(args);
    Assert.assertEquals(new Double(45.0), new Double(res.get()));
  }

  @Test
  public void testLongTimesDecimal() throws HiveException {
    GenericUDFOPMultiply udf = new GenericUDFOPMultiply();

    LongWritable left = new LongWritable(104);
    HiveDecimalWritable right = new HiveDecimalWritable(HiveDecimal.create("234.97"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableLongObjectInspector,
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(9, 4))
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.getDecimalTypeInfo(29,4), oi.getTypeInfo());
    HiveDecimalWritable res = (HiveDecimalWritable) udf.evaluate(args);
    Assert.assertEquals(HiveDecimal.create("24436.88"), res.getHiveDecimal());
  }

  @Test
  public void testFloatTimesFloat() throws HiveException {
    GenericUDFOPMultiply udf = new GenericUDFOPMultiply();

    FloatWritable f1 = new FloatWritable(4.5f);
    FloatWritable f2 = new FloatWritable(0.0f);
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
        PrimitiveObjectInspectorFactory.writableFloatObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(f1),
        new DeferredJavaObject(f2),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(oi.getTypeInfo(), TypeInfoFactory.floatTypeInfo);
    FloatWritable res = (FloatWritable) udf.evaluate(args);
    Assert.assertEquals(new Float(0.0), new Float(res.get()));
  }

  @Test
  public void testDouleTimesDecimal() throws HiveException {
    GenericUDFOPMultiply udf = new GenericUDFOPMultiply();

    DoubleWritable left = new DoubleWritable(74.52);
    HiveDecimalWritable right = new HiveDecimalWritable(HiveDecimal.create("234.97"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(5, 2))
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.doubleTypeInfo, oi.getTypeInfo());
    DoubleWritable res = (DoubleWritable) udf.evaluate(args);
    Assert.assertEquals(new Double(17509.9644), new Double(res.get()));
  }

  @Test
  public void testDecimalTimesDecimal() throws HiveException {
    GenericUDFOPMultiply udf = new GenericUDFOPMultiply();

    HiveDecimalWritable left = new HiveDecimalWritable(HiveDecimal.create("14.5"));
    HiveDecimalWritable right = new HiveDecimalWritable(HiveDecimal.create("234.97"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(3, 1)),
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(5, 2))
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.getDecimalTypeInfo(9,3), oi.getTypeInfo());
    HiveDecimalWritable res = (HiveDecimalWritable) udf.evaluate(args);
    Assert.assertEquals(HiveDecimal.create("3407.065"), res.getHiveDecimal());
  }

  @Test
  public void testDecimalTimesDecimalSameParams() throws HiveException {
    GenericUDFOPMultiply udf = new GenericUDFOPMultiply();

    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(5, 2)),
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(5, 2))
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.getDecimalTypeInfo(11, 4), oi.getTypeInfo());
  }


  @Test
  public void testReturnTypeBackwardCompat() throws Exception {
    // Disable ansi sql arithmetic changes
    SessionState.get().getConf().setVar(HiveConf.ConfVars.HIVE_COMPAT, "0.12");

    verifyReturnType(new GenericUDFOPMultiply(), "int", "int", "int");
    verifyReturnType(new GenericUDFOPMultiply(), "int", "float", "float");
    verifyReturnType(new GenericUDFOPMultiply(), "int", "double", "double");
    verifyReturnType(new GenericUDFOPMultiply(), "int", "decimal(10,2)", "decimal(21,2)");

    verifyReturnType(new GenericUDFOPMultiply(), "float", "float", "float");
    verifyReturnType(new GenericUDFOPMultiply(), "float", "double", "double");
    verifyReturnType(new GenericUDFOPMultiply(), "float", "decimal(10,2)", "float");

    verifyReturnType(new GenericUDFOPMultiply(), "double", "double", "double");
    verifyReturnType(new GenericUDFOPMultiply(), "double", "decimal(10,2)", "double");

    verifyReturnType(new GenericUDFOPMultiply(), "decimal(10,2)", "decimal(10,2)", "decimal(21,4)");

    // Most tests are done with ANSI SQL mode enabled, set it back to true
    SessionState.get().getConf().setVar(HiveConf.ConfVars.HIVE_COMPAT, "latest");
  }

  @Test
  public void testReturnTypeAnsiSql() throws Exception {
    SessionState.get().getConf().setVar(HiveConf.ConfVars.HIVE_COMPAT, "latest");

    verifyReturnType(new GenericUDFOPMultiply(), "int", "int", "int");
    verifyReturnType(new GenericUDFOPMultiply(), "int", "float", "float");
    verifyReturnType(new GenericUDFOPMultiply(), "int", "double", "double");
    verifyReturnType(new GenericUDFOPMultiply(), "int", "decimal(10,2)", "decimal(21,2)");

    verifyReturnType(new GenericUDFOPMultiply(), "float", "float", "float");
    verifyReturnType(new GenericUDFOPMultiply(), "float", "double", "double");
    verifyReturnType(new GenericUDFOPMultiply(), "float", "decimal(10,2)", "float");

    verifyReturnType(new GenericUDFOPMultiply(), "double", "double", "double");
    verifyReturnType(new GenericUDFOPMultiply(), "double", "decimal(10,2)", "double");

    verifyReturnType(new GenericUDFOPMultiply(), "decimal(10,2)", "decimal(10,2)", "decimal(21,4)");

    verifyReturnType(new GenericUDFOPMultiply(), "decimal(38,18)", "decimal(38,18)", "decimal(38,6)");
    verifyReturnType(new GenericUDFOPMultiply(), "decimal(38,38)", "decimal(38,38)", "decimal(38,37)");
    verifyReturnType(new GenericUDFOPMultiply(), "decimal(38,0)", "decimal(38,0)", "decimal(38,0)");
    verifyReturnType(new GenericUDFOPMultiply(), "decimal(38,38)", "decimal(38,0)", "decimal(38,6)");
    verifyReturnType(new GenericUDFOPMultiply(), "decimal(20,2)", "decimal(20,0)", "decimal(38,2)");
  }
}
