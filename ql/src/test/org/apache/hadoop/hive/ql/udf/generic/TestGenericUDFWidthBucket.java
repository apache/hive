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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class TestGenericUDFWidthBucket {

  @Test
  public void testExprLessThanMinValue() throws HiveException {
    assertEquals(0, testWidthBucketWithValues(99L, 100L, 5000L, 10).get());
  }

  @Test
  public void testExprEqualsMinValue() throws HiveException {
    assertEquals(1, testWidthBucketWithValues(100L, 100L, 5000L, 10).get());
  }

  @Test
  public void testExprEqualsBoundaryValue() throws HiveException {
    assertEquals(2, testWidthBucketWithValues(590L, 100L, 5000L, 10).get());
  }

  @Test
  public void testExprEqualsMaxValue() throws HiveException {
    assertEquals(11, testWidthBucketWithValues(5000L, 100L, 5000L, 10).get());
  }

  @Test
  public void testExprAboveMaxValue() throws HiveException {
    assertEquals(11, testWidthBucketWithValues(6000L, 100L, 5000L, 10).get());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeBucketValue() throws HiveException {
    testWidthBucketWithValues(100L, 100L, 5000L, -1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testZeroBucketValue() throws HiveException {
    testWidthBucketWithValues(100L, 100L, 5000L, 0);
  }

  private IntWritable testWidthBucketWithValues(Long expr, Long minValue, Long maxValue, Integer numBuckets) throws HiveException {
    GenericUDFWidthBucket udf = new GenericUDFWidthBucket();
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
    ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
    ObjectInspector valueOI3 = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
    ObjectInspector valueOI4 = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    ObjectInspector[] arguments = {valueOI1, valueOI2, valueOI3, valueOI4};

    udf.initialize(arguments);

    GenericUDF.DeferredObject valueObj1 = new GenericUDF.DeferredJavaObject(expr);
    GenericUDF.DeferredObject valueObj2 = new GenericUDF.DeferredJavaObject(minValue);
    GenericUDF.DeferredObject valueObj3 = new GenericUDF.DeferredJavaObject(maxValue);
    GenericUDF.DeferredObject valueObj4 = new GenericUDF.DeferredJavaObject(numBuckets);
    GenericUDF.DeferredObject[] args = {valueObj1, valueObj2, valueObj3, valueObj4};

    return (IntWritable) udf.evaluate(args);
  }
}
