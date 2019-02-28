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

public class TestGenericUDFPower {

  private static final double EPSILON = 1E-10;

  @Test
  public void testBytePowerShort() throws HiveException {
    GenericUDFPower udf = new GenericUDFPower();

    ByteWritable left = new ByteWritable((byte) 2);
    ShortWritable right = new ShortWritable((short) 4);
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableByteObjectInspector,
        PrimitiveObjectInspectorFactory.writableShortObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.doubleTypeInfo, oi.getTypeInfo());
    DoubleWritable res = (DoubleWritable) udf.evaluate(args);
    Assert.assertEquals(16, res.get(), EPSILON);
  }

  @Test
  public void testVarcharPowerInt() throws HiveException {
    GenericUDFPower udf = new GenericUDFPower();

    HiveVarcharWritable left = new HiveVarcharWritable();
    left.set("3.14");
    IntWritable right = new IntWritable(2);
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
    Assert.assertEquals(3.14 * 3.14, res.get(), EPSILON);
  }

  @Test
  public void testDoublePowerLong() throws HiveException {
    GenericUDFPower udf = new GenericUDFPower();

    DoubleWritable left = new DoubleWritable(4.5);
    LongWritable right = new LongWritable(4);
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
    Assert.assertEquals(4.5 * 4.5 * 4.5 * 4.5, res.get(), EPSILON);
  }

  @Test
  public void testLongPowerDecimal() throws HiveException {
    GenericUDFPower udf = new GenericUDFPower();

    LongWritable left = new LongWritable(10);
    HiveDecimalWritable right = new HiveDecimalWritable(HiveDecimal.create("3.14"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableLongObjectInspector,
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(9, 4))
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.doubleTypeInfo, oi.getTypeInfo());
    DoubleWritable res = (DoubleWritable) udf.evaluate(args);
    Assert.assertEquals(1380.3842646028852, res.get(), EPSILON);
   }

  @Test
  public void testFloatPowerFloat() throws HiveException {
    GenericUDFPower udf = new GenericUDFPower();

    FloatWritable f1 = new FloatWritable(4.5f);
    FloatWritable f2 = new FloatWritable(-1.5f);
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
    Assert.assertEquals(0.10475656017578482, res.get(), EPSILON);
  }

  @Test
  public void testShortPowerFloat() throws HiveException {
    GenericUDFPower udf = new GenericUDFPower();

    ShortWritable base = new ShortWritable((short) 23);
    FloatWritable power = new FloatWritable(-1.5f);
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableShortObjectInspector,
        PrimitiveObjectInspectorFactory.writableFloatObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(base),
        new DeferredJavaObject(power),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(oi.getTypeInfo(), TypeInfoFactory.doubleTypeInfo);
    DoubleWritable res = (DoubleWritable) udf.evaluate(args);
    Assert.assertEquals(0.009065844089438033, res.get(), EPSILON);
  }

  @Test
  public void testDoulePowerDecimal() throws HiveException {
    GenericUDFPower udf = new GenericUDFPower();

    DoubleWritable left = new DoubleWritable(-4.52);
    HiveDecimalWritable right = new HiveDecimalWritable(HiveDecimal.create("3"));
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
    Assert.assertEquals("Unexpected result", -4.52 * 4.52 * 4.52, res.get(), EPSILON);
  }

  @Test
  public void testDecimalPowerDecimal() throws HiveException {
    GenericUDFPower udf = new GenericUDFPower();

    HiveDecimalWritable left = new HiveDecimalWritable(HiveDecimal.create("14.5"));
    HiveDecimalWritable right = new HiveDecimalWritable(HiveDecimal.create("-3.2"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(3, 1)),
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(5, 2))
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.doubleTypeInfo, oi.getTypeInfo());
    DoubleWritable res = (DoubleWritable) udf.evaluate(args);
    Assert.assertEquals(1.9214203800477838E-4, res.get(), EPSILON);
  }

}
