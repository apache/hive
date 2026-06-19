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

public class TestGenericUDFOPDivide extends AbstractTestGenericUDFOPNumeric {

  private static final double EPSILON = 1E-6;

  @Test
  public void testByteDivideShort() throws HiveException {
    GenericUDFOPDivide udf = new GenericUDFOPDivide();

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
    Assert.assertEquals(oi.getTypeInfo(), TypeInfoFactory.getDecimalTypeInfo(9, 6));
    HiveDecimalWritable res = (HiveDecimalWritable) udf.evaluate(args);
    Assert.assertEquals(HiveDecimal.create("0.666667"), res.getHiveDecimal());
  }

  @Test
  public void testVarcharDivideInt() throws HiveException {
    GenericUDFOPDivide udf = new GenericUDFOPDivide();

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
    Assert.assertEquals(123.0 / 456.0, res.get(), EPSILON);
  }

  @Test
  public void testDoubleDivideLong() throws HiveException {
    GenericUDFOPDivide udf = new GenericUDFOPDivide();

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
    Assert.assertEquals(0.45, res.get(), EPSILON);
  }

  @Test
  public void testLongDivideDecimal() throws HiveException {
    GenericUDFOPDivide udf = new GenericUDFOPDivide();

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
    Assert.assertEquals(TypeInfoFactory.getDecimalTypeInfo(33, 10), oi.getTypeInfo());
    HiveDecimalWritable res = (HiveDecimalWritable) udf.evaluate(args);
    Assert.assertEquals(HiveDecimal.create("0.4426096949"), res.getHiveDecimal());
  }

  @Test
  public void testFloatDivideFloat() throws HiveException {
    GenericUDFOPDivide udf = new GenericUDFOPDivide();

    FloatWritable f1 = new FloatWritable(4.5f);
    FloatWritable f2 = new FloatWritable(1.5f);
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
        PrimitiveObjectInspectorFactory.writableFloatObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(f1),
        new DeferredJavaObject(f2),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(oi.getTypeInfo(), TypeInfoFactory.doubleTypeInfo);
    DoubleWritable res = (DoubleWritable) udf.evaluate(args);
    Assert.assertEquals(3.0, res.get(), EPSILON);
  }

  @Test
  public void testDouleDivideDecimal() throws HiveException {
    GenericUDFOPDivide udf = new GenericUDFOPDivide();

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
    Assert.assertEquals(74.52 / 234.97, res.get(), EPSILON);
  }

  @Test
  public void testDecimalDivideDecimal() throws HiveException {
    GenericUDFOPDivide udf = new GenericUDFOPDivide();

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
    Assert.assertEquals(TypeInfoFactory.getDecimalTypeInfo(11, 7), oi.getTypeInfo());
    HiveDecimalWritable res = (HiveDecimalWritable) udf.evaluate(args);
    Assert.assertEquals(HiveDecimal.create("0.06171"), res.getHiveDecimal());
  }

  @Test
  public void testDecimalDivideDecimal2() throws HiveException {
    GenericUDFOPDivide udf = new GenericUDFOPDivide();

    HiveDecimalWritable left = new HiveDecimalWritable(HiveDecimal.create("5"));
    HiveDecimalWritable right = new HiveDecimalWritable(HiveDecimal.create("25"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(1, 0)),
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(2, 0))
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.getDecimalTypeInfo(7, 6), oi.getTypeInfo());
    HiveDecimalWritable res = (HiveDecimalWritable) udf.evaluate(args);
    Assert.assertEquals(HiveDecimal.create("0.2"), res.getHiveDecimal());
  }

  @Test
  public void testDecimalDivideDecimalSameParams() throws HiveException {
    GenericUDFOPDivide udf = new GenericUDFOPDivide();

    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(5, 2)),
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(5, 2))
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.getDecimalTypeInfo(13, 8), oi.getTypeInfo());
  }

  @Test
  public void testDecimalDivisionResultType() throws HiveException {
    testDecimalDivisionResultType(5, 2, 3, 2, 11, 6);
    testDecimalDivisionResultType(38, 18, 38, 18, 38, 6);
    testDecimalDivisionResultType(38, 18, 20, 0, 38, 18);
    testDecimalDivisionResultType(20, 0, 8, 5, 34, 9);
    testDecimalDivisionResultType(10, 0, 10, 0, 21, 11);
    testDecimalDivisionResultType(5, 2, 5, 5, 16, 8);
    testDecimalDivisionResultType(10, 10, 5, 0, 16, 16);
    testDecimalDivisionResultType(10, 10, 5, 5, 21, 16);
    testDecimalDivisionResultType(38, 38, 38, 38, 38, 6);
    testDecimalDivisionResultType(38, 0, 38, 0, 38, 6);
  }

  private void testDecimalDivisionResultType(int prec1, int scale1, int prec2, int scale2, int prec3, int scale3)
      throws HiveException {
    GenericUDFOPDivide udf = new GenericUDFOPDivide();

    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(prec1, scale1)),
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(prec2, scale2))
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.getDecimalTypeInfo(prec3, scale3), oi.getTypeInfo());
  }

  @Test
  public void testReturnTypeBackwardCompat() throws Exception {
    // Disable ansi sql arithmetic changes
    SessionState.get().getConf().setVar(HiveConf.ConfVars.HIVE_COMPAT, "0.12");

    verifyReturnType(new GenericUDFOPDivide(), "int", "int", "double"); // different from sql compat mode
    verifyReturnType(new GenericUDFOPDivide(), "int", "float", "double");
    verifyReturnType(new GenericUDFOPDivide(), "int", "double", "double");
    verifyReturnType(new GenericUDFOPDivide(), "int", "decimal(10,2)", "decimal(23,11)");

    verifyReturnType(new GenericUDFOPDivide(), "float", "float", "double");
    verifyReturnType(new GenericUDFOPDivide(), "float", "double", "double");
    verifyReturnType(new GenericUDFOPDivide(), "float", "decimal(10,2)", "double");

    verifyReturnType(new GenericUDFOPDivide(), "double", "double", "double");
    verifyReturnType(new GenericUDFOPDivide(), "double", "decimal(10,2)", "double");

    verifyReturnType(new GenericUDFOPDivide(), "decimal(10,2)", "decimal(10,2)", "decimal(23,13)");

    // Most tests are done with ANSI SQL mode enabled, set it back to true
    SessionState.get().getConf().setVar(HiveConf.ConfVars.HIVE_COMPAT, "latest");
  }

  @Test
  public void testReturnTypeAnsiSql() throws Exception {
    SessionState.get().getConf().setVar(HiveConf.ConfVars.HIVE_COMPAT, "latest");

    verifyReturnType(new GenericUDFOPDivide(), "int", "int", "decimal(21,11)");
    verifyReturnType(new GenericUDFOPDivide(), "int", "float", "double");
    verifyReturnType(new GenericUDFOPDivide(), "int", "double", "double");
    verifyReturnType(new GenericUDFOPDivide(), "int", "decimal(10,2)", "decimal(23,11)");

    verifyReturnType(new GenericUDFOPDivide(), "float", "float", "double");
    verifyReturnType(new GenericUDFOPDivide(), "float", "double", "double");
    verifyReturnType(new GenericUDFOPDivide(), "float", "decimal(10,2)", "double");

    verifyReturnType(new GenericUDFOPDivide(), "double", "double", "double");
    verifyReturnType(new GenericUDFOPDivide(), "double", "decimal(10,2)", "double");

    verifyReturnType(new GenericUDFOPDivide(), "decimal(10,2)", "decimal(10,2)", "decimal(23,13)");
  }
}
