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

import java.io.IOException;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Test;

public class TestGenericUDFOPOr {
  @Test
  public void testTrueOrTrue() throws HiveException, IOException {
    GenericUDFOPOr udf = new GenericUDFOPOr();

    BooleanWritable left = new BooleanWritable(true);
    BooleanWritable right = new BooleanWritable(true);

    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
        PrimitiveObjectInspectorFactory.writableBooleanObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(oi.getTypeInfo(), TypeInfoFactory.booleanTypeInfo);
    BooleanWritable res = (BooleanWritable) udf.evaluate(args);
    Assert.assertEquals(true, res.get());
    udf.close();
  }

  @Test
  public void testTrueOrFalse() throws HiveException, IOException {
    GenericUDFOPOr udf = new GenericUDFOPOr();

    BooleanWritable left = new BooleanWritable(true);
    BooleanWritable right = new BooleanWritable(false);

    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
        PrimitiveObjectInspectorFactory.writableBooleanObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(oi.getTypeInfo(), TypeInfoFactory.booleanTypeInfo);
    BooleanWritable res = (BooleanWritable) udf.evaluate(args);
    Assert.assertEquals(true, res.get());
    udf.close();
  }

  @Test
  public void testFalseOrFalse() throws HiveException, IOException {
    GenericUDFOPOr udf = new GenericUDFOPOr();

    BooleanWritable left = new BooleanWritable(false);
    BooleanWritable right = new BooleanWritable(false);

    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
        PrimitiveObjectInspectorFactory.writableBooleanObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(oi.getTypeInfo(), TypeInfoFactory.booleanTypeInfo);
    BooleanWritable res = (BooleanWritable) udf.evaluate(args);
    Assert.assertEquals(false, res.get());
    udf.close();
  }

  @Test
  public void testTrueOrNull() throws HiveException, IOException {
    GenericUDFOPOr udf = new GenericUDFOPOr();

    BooleanWritable left = new BooleanWritable(true);
    Writable right = null;

    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
        PrimitiveObjectInspectorFactory.writableVoidObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(oi.getTypeInfo(), TypeInfoFactory.booleanTypeInfo);
    BooleanWritable res = (BooleanWritable) udf.evaluate(args);
    Assert.assertEquals(true, res.get());
    udf.close();
  }

  @Test
  public void testFalseOrNull() throws HiveException, IOException {
    GenericUDFOPOr udf = new GenericUDFOPOr();

    BooleanWritable left = new BooleanWritable(false);
    Writable right = null;

    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
        PrimitiveObjectInspectorFactory.writableVoidObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(oi.getTypeInfo(), TypeInfoFactory.booleanTypeInfo);
    BooleanWritable res = (BooleanWritable) udf.evaluate(args);
    Assert.assertEquals(null, res);
    udf.close();
  }

  @Test
  public void testNullOrNull() throws HiveException, IOException {
    GenericUDFOPOr udf = new GenericUDFOPOr();

    Writable left = null;
    Writable right = null;

    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableVoidObjectInspector,
        PrimitiveObjectInspectorFactory.writableVoidObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(oi.getTypeInfo(), TypeInfoFactory.booleanTypeInfo);
    BooleanWritable res = (BooleanWritable) udf.evaluate(args);
    Assert.assertEquals(null, res);
    udf.close();
  }
}
