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

import static java.math.BigDecimal.ROUND_HALF_EVEN;

import java.math.BigDecimal;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.expressions.MathExpr;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.junit.Assert;
import org.junit.Test;

public class TestGenericUDFBRound {

  @Test
  public void testDouble() throws HiveException {
    GenericUDFBRound udf = new GenericUDFBRound();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;

    IntWritable scale = new IntWritable(0);
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, scale);

    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runDouble(2.5, scale, 2.0, udf);
    runDouble(3.5, scale, 4.0, udf);

    runDouble(2.49, scale, 2.0, udf);
    runDouble(3.49, scale, 3.0, udf);

    runDouble(2.51, scale, 3.0, udf);
    runDouble(3.51, scale, 4.0, udf);

    runDouble(2.4, scale, 2.0, udf);
    runDouble(3.4, scale, 3.0, udf);

    runDouble(2.6, scale, 3.0, udf);
    runDouble(3.6, scale, 4.0, udf);
  }

  @Test
  public void testDoubleScaleMinus1() throws HiveException {
    GenericUDFBRound udf = new GenericUDFBRound();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;

    IntWritable scale = new IntWritable(-1);
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, scale);

    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runDouble(55.0, scale, 60.0, udf);
    runDouble(45.0, scale, 40.0, udf);

    runDouble(54.9, scale, 50.0, udf);
    runDouble(44.9, scale, 40.0, udf);

    runDouble(55.1, scale, 60.0, udf);
    runDouble(45.1, scale, 50.0, udf);

    runDouble(-55.0, scale, -60.0, udf);
    runDouble(-45.0, scale, -40.0, udf);

    runDouble(-54.9, scale, -50.0, udf);
    runDouble(-44.9, scale, -40.0, udf);

    runDouble(-55.1, scale, -60.0, udf);
    runDouble(-45.1, scale, -50.0, udf);
  }

  @Test
  public void testFloat() throws HiveException {
    GenericUDFBRound udf = new GenericUDFBRound();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableFloatObjectInspector;

    IntWritable scale = new IntWritable(0);
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, scale);

    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runFloat(2.5f, scale, 2.0f, udf);
    runFloat(3.5f, scale, 4.0f, udf);

    runFloat(2.49f, scale, 2.0f, udf);
    runFloat(3.49f, scale, 3.0f, udf);

    runFloat(2.51f, scale, 3.0f, udf);
    runFloat(3.51f, scale, 4.0f, udf);

    runFloat(2.4f, scale, 2.0f, udf);
    runFloat(3.4f, scale, 3.0f, udf);

    runFloat(2.6f, scale, 3.0f, udf);
    runFloat(3.6f, scale, 4.0f, udf);
  }

  @Test
  public void testDecimal() throws HiveException {
    GenericUDFBRound udf = new GenericUDFBRound();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector;

    IntWritable scale = new IntWritable(0);
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, scale);

    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runDecimal(2.5, scale, 2.0, udf);
    runDecimal(3.5, scale, 4.0, udf);

    runDecimal(2.49, scale, 2.0, udf);
    runDecimal(3.49, scale, 3.0, udf);

    runDecimal(2.51, scale, 3.0, udf);
    runDecimal(3.51, scale, 4.0, udf);

    runDecimal(2.4, scale, 2.0, udf);
    runDecimal(3.4, scale, 3.0, udf);

    runDecimal(2.6, scale, 3.0, udf);
    runDecimal(3.6, scale, 4.0, udf);
  }

  @Test
  public void testMathExprBround() throws HiveException {
    double[] vArr = { 1.5, 2.5, -1.5, -2.5, 1.49, 1.51 };
    for (double v : vArr) {
      double v1 = RoundUtils.bround(v, 0);
      double v2 = MathExpr.bround(v);
      Assert.assertEquals(v1, v2, 0.00001);

      double v3 = BigDecimal.valueOf(v).setScale(0, ROUND_HALF_EVEN).doubleValue();
      Assert.assertEquals(v3, v2, 0.00001);
    }
  }

  private void runDouble(double v, IntWritable scale, Double expV, GenericUDF udf)
      throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(new DoubleWritable(v));
    DeferredObject valueObj1 = new DeferredJavaObject(scale);
    DeferredObject[] args = { valueObj0, valueObj1 };
    DoubleWritable output = (DoubleWritable) udf.evaluate(args);
    Assert.assertEquals("bround() test ", expV.doubleValue(), output.get(), 0.00001);
  }

  private void runFloat(float v, IntWritable scale, Float expV, GenericUDF udf)
      throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(new FloatWritable(v));
    DeferredObject valueObj1 = new DeferredJavaObject(scale);
    DeferredObject[] args = { valueObj0, valueObj1 };
    FloatWritable output = (FloatWritable) udf.evaluate(args);
    Assert.assertEquals("bround() test ", expV.floatValue(), output.get(), 0.001f);
  }

  private void runDecimal(double v, IntWritable scale, Double expV, GenericUDF udf)
      throws HiveException {
    HiveDecimal hd = HiveDecimal.create(BigDecimal.valueOf(v));
    DeferredObject valueObj0 = new DeferredJavaObject(new HiveDecimalWritable(hd));
    DeferredObject valueObj1 = new DeferredJavaObject(scale);
    DeferredObject[] args = { valueObj0, valueObj1 };
    HiveDecimalWritable output = (HiveDecimalWritable) udf.evaluate(args);
    Assert.assertEquals("bround() test ", expV.doubleValue(),
        output.getHiveDecimal().doubleValue(), 0.00001);
  }
}
