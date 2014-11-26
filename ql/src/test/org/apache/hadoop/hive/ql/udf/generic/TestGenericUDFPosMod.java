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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
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

public class TestGenericUDFPosMod extends AbstractTestGenericUDFOPNumeric {

  @Test
  public void testPosModByZero1() throws HiveException {
    GenericUDFPosMod udf = new GenericUDFPosMod();

    // Byte
    ByteWritable b1 = new ByteWritable((byte) 4);
    ByteWritable b2 = new ByteWritable((byte) 0);
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableByteObjectInspector,
        PrimitiveObjectInspectorFactory.writableByteObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(b1),
        new DeferredJavaObject(b2),
    };

    udf.initialize(inputOIs);
    ByteWritable b3 = (ByteWritable) udf.evaluate(args);
    Assert.assertNull(b3);
  }

  @Test
  public void testPosModByZero2() throws HiveException {
    GenericUDFPosMod udf = new GenericUDFPosMod();

    // Short
    ShortWritable s1 = new ShortWritable((short) 4);
    ShortWritable s2 = new ShortWritable((short) 0);
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableShortObjectInspector,
        PrimitiveObjectInspectorFactory.writableShortObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(s1),
        new DeferredJavaObject(s2),
    };

    udf.initialize(inputOIs);
    ShortWritable s3 = (ShortWritable) udf.evaluate(args);
    Assert.assertNull(s3);
  }

  @Test
  public void testPosModByZero3() throws HiveException {
    GenericUDFPosMod udf = new GenericUDFPosMod();

    // Int
    IntWritable i1 = new IntWritable(4);
    IntWritable i2 = new IntWritable(0);
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableIntObjectInspector,
        PrimitiveObjectInspectorFactory.writableIntObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(i1),
        new DeferredJavaObject(i2),
    };

    udf.initialize(inputOIs);
    IntWritable i3 = (IntWritable) udf.evaluate(args);
    Assert.assertNull(i3);
  }

  @Test
  public void testPosModByZero4() throws HiveException {
    GenericUDFPosMod udf = new GenericUDFPosMod();

    // Long
    LongWritable l1 = new LongWritable(4);
    LongWritable l2 = new LongWritable(0L);
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableLongObjectInspector,
        PrimitiveObjectInspectorFactory.writableLongObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(l1),
        new DeferredJavaObject(l2),
    };

    udf.initialize(inputOIs);
    LongWritable l3 = (LongWritable) udf.evaluate(args);
    Assert.assertNull(l3);
  }

  @Test
  public void testPosModByZero5() throws HiveException {
    GenericUDFPosMod udf = new GenericUDFPosMod();

    // Float
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

    udf.initialize(inputOIs);
    FloatWritable f3 = (FloatWritable) udf.evaluate(args);
    Assert.assertNull(f3);
  }

  @Test
  public void testPosModByZero6() throws HiveException {
    GenericUDFPosMod udf = new GenericUDFPosMod();

    // Double
    DoubleWritable d1 = new DoubleWritable(4.5);
    DoubleWritable d2 = new DoubleWritable(0.0);
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(d1),
        new DeferredJavaObject(d2),
    };

    udf.initialize(inputOIs);
    DoubleWritable d3 = (DoubleWritable) udf.evaluate(args);
    Assert.assertNull(d3);
  }

  @Test
  public void testPosModByZero8() throws HiveException {
    GenericUDFPosMod udf = new GenericUDFPosMod();

    // Decimal
    HiveDecimalWritable dec1 = new HiveDecimalWritable(HiveDecimal.create("4.5"));
    HiveDecimalWritable dec2 = new HiveDecimalWritable(HiveDecimal.create("0"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(2, 1)),
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(1, 0))
    };
    DeferredObject[] args = {
        new DeferredJavaObject(dec1),
        new DeferredJavaObject(dec2),
    };

    udf.initialize(inputOIs);
    HiveDecimalWritable dec3 = (HiveDecimalWritable) udf.evaluate(args);
    Assert.assertNull(dec3);
  }

  @Test
  public void testDecimalPosModDecimal() throws HiveException {
    GenericUDFPosMod udf = new GenericUDFPosMod();

    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(3, 1)),
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(5, 2))
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.getDecimalTypeInfo(5, 2), oi.getTypeInfo());
  }

  @Test
  public void testDecimalPosModDecimalSameParams() throws HiveException {
    GenericUDFPosMod udf = new GenericUDFPosMod();

    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(5, 2)),
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(5, 2))
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.getDecimalTypeInfo(5, 2), oi.getTypeInfo());
  }

}
