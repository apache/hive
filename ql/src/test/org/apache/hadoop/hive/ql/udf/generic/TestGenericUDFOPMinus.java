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

public class TestGenericUDFOPMinus extends TestGenericUDFOPNumeric {

  @Test
  public void testByteMinusShort() throws HiveException {
    GenericUDFOPMinus udf = new GenericUDFOPMinus();

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
    Assert.assertEquals(-2, res.get());
  }

  @Test
  public void testVarcharMinusInt() throws HiveException {
    GenericUDFOPMinus udf = new GenericUDFOPMinus();

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
    Assert.assertEquals(new Double(-333.0), new Double(res.get()));
  }

  @Test
  public void testDoubleMinusLong() throws HiveException {
    GenericUDFOPMinus udf = new GenericUDFOPMinus();

    // Int
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
    Assert.assertEquals(new Double(-5.5), new Double(res.get()));
  }

  @Test
  public void testLongMinusDecimal() throws HiveException {
    GenericUDFOPMinus udf = new GenericUDFOPMinus();

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
    Assert.assertEquals(TypeInfoFactory.getDecimalTypeInfo(24,4), oi.getTypeInfo());
    HiveDecimalWritable res = (HiveDecimalWritable) udf.evaluate(args);
    Assert.assertEquals(HiveDecimal.create("-130.97"), res.getHiveDecimal());
  }

  @Test
  public void testFloatMinusFloat() throws HiveException {
    GenericUDFOPMinus udf = new GenericUDFOPMinus();

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
    Assert.assertEquals(new Float(4.5), new Float(res.get()));
  }

  @Test
  public void testDouleMinusDecimal() throws HiveException {
    GenericUDFOPMinus udf = new GenericUDFOPMinus();

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
    Assert.assertEquals(new Double(-160.45), new Double(res.get()));
  }

  @Test
  public void testDecimalMinusDecimal() throws HiveException {
    GenericUDFOPMinus udf = new GenericUDFOPMinus();

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
    Assert.assertEquals(TypeInfoFactory.getDecimalTypeInfo(6,2), oi.getTypeInfo());
    HiveDecimalWritable res = (HiveDecimalWritable) udf.evaluate(args);
    Assert.assertEquals( HiveDecimal.create("-220.47"), res.getHiveDecimal());
  }

  @Test
  public void testDecimalMinusDecimalSameParams() throws HiveException {
    GenericUDFOPMinus udf = new GenericUDFOPMinus();

    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(5, 2)),
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(5, 2))
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.getDecimalTypeInfo(6, 2), oi.getTypeInfo());
  }

  @Test
  public void testReturnTypeBackwardCompat() throws Exception {
    // Disable ansi sql arithmetic changes
    SessionState.get().getConf().setVar(HiveConf.ConfVars.HIVE_COMPAT, "0.12");

    verifyReturnType(new GenericUDFOPMinus(), "int", "int", "int");
    verifyReturnType(new GenericUDFOPMinus(), "int", "float", "float");
    verifyReturnType(new GenericUDFOPMinus(), "int", "double", "double");
    verifyReturnType(new GenericUDFOPMinus(), "int", "decimal(10,2)", "decimal(13,2)");

    verifyReturnType(new GenericUDFOPMinus(), "float", "float", "float");
    verifyReturnType(new GenericUDFOPMinus(), "float", "double", "double");
    verifyReturnType(new GenericUDFOPMinus(), "float", "decimal(10,2)", "double");

    verifyReturnType(new GenericUDFOPMinus(), "double", "double", "double");
    verifyReturnType(new GenericUDFOPMinus(), "double", "decimal(10,2)", "double");

    verifyReturnType(new GenericUDFOPMinus(), "decimal(10,2)", "decimal(10,2)", "decimal(11,2)");

    // Most tests are done with ANSI SQL mode enabled, set it back to true
    SessionState.get().getConf().setVar(HiveConf.ConfVars.HIVE_COMPAT, "latest");
  }

  @Test
  public void testReturnTypeAnsiSql() throws Exception {
    SessionState.get().getConf().setVar(HiveConf.ConfVars.HIVE_COMPAT, "latest");

    verifyReturnType(new GenericUDFOPMinus(), "int", "int", "int");
    verifyReturnType(new GenericUDFOPMinus(), "int", "float", "float");
    verifyReturnType(new GenericUDFOPMinus(), "int", "double", "double");
    verifyReturnType(new GenericUDFOPMinus(), "int", "decimal(10,2)", "decimal(13,2)");

    verifyReturnType(new GenericUDFOPMinus(), "float", "float", "float");
    verifyReturnType(new GenericUDFOPMinus(), "float", "double", "double");
    verifyReturnType(new GenericUDFOPMinus(), "float", "decimal(10,2)", "double");

    verifyReturnType(new GenericUDFOPMinus(), "double", "double", "double");
    verifyReturnType(new GenericUDFOPMinus(), "double", "decimal(10,2)", "double");

    verifyReturnType(new GenericUDFOPMinus(), "decimal(10,2)", "decimal(10,2)", "decimal(11,2)");
  }
}
