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

import static java.util.Arrays.asList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class TestGenericUDFNullif {

  @Test
  public void testByteTypeEq() throws HiveException {
    GenericUDFNullif udf = new GenericUDFNullif();

    ObjectInspector[] inputOIs = { PrimitiveObjectInspectorFactory.writableByteObjectInspector,
        PrimitiveObjectInspectorFactory.writableByteObjectInspector };
    DeferredObject[] args = { new DeferredJavaObject(new ByteWritable((byte) 4)),
        new DeferredJavaObject(new ByteWritable((byte) 4)) };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.byteTypeInfo, oi.getTypeInfo());
    ByteWritable res = (ByteWritable) udf.evaluate(args);
    Assert.assertEquals(null, res);
  }

  @Test
  public void testByteNeq() throws HiveException {
    GenericUDFNullif udf = new GenericUDFNullif();

    ObjectInspector[] inputOIs = { PrimitiveObjectInspectorFactory.writableByteObjectInspector,
        PrimitiveObjectInspectorFactory.writableByteObjectInspector };
    DeferredObject[] args = { new DeferredJavaObject(new ByteWritable((byte) 4)),
        new DeferredJavaObject(new ByteWritable((byte) 1)) };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.byteTypeInfo, oi.getTypeInfo());
    ByteWritable res = (ByteWritable) udf.evaluate(args);
    Assert.assertEquals(4, res.get());
  }

  @Test(expected = UDFArgumentException.class)
  public void testConversionIsPrevented1() throws HiveException {
    GenericUDFNullif udf = new GenericUDFNullif();

    ObjectInspector[] inputOIs = { PrimitiveObjectInspectorFactory.writableStringObjectInspector,
        PrimitiveObjectInspectorFactory.writableByteObjectInspector };
    DeferredObject[] args = { new DeferredJavaObject(new Text("4")),
        new DeferredJavaObject(new ByteWritable((byte) 4)) };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
  }

  @Test
  public void testConversionInSameGroup() throws HiveException {
    GenericUDFNullif udf = new GenericUDFNullif();

    ObjectInspector[] inputOIs = { PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
        PrimitiveObjectInspectorFactory.writableByteObjectInspector };
    DeferredObject[] args = { new DeferredJavaObject(new DoubleWritable(4.0)),
        new DeferredJavaObject(new ByteWritable((byte) 4)) };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
  }

  @Test(expected = UDFArgumentException.class)
  public void testConversionIsPrevented2() throws HiveException {
    GenericUDFNullif udf = new GenericUDFNullif();

    ObjectInspector[] inputOIs = { PrimitiveObjectInspectorFactory.writableByteObjectInspector,
        PrimitiveObjectInspectorFactory.writableStringObjectInspector };
    DeferredObject[] args = { new DeferredJavaObject(new ByteWritable((byte) 4)),
        new DeferredJavaObject(new Text("4")) };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
  }

  @Test(expected = UDFArgumentException.class)
  public void testNotSupportedArgumentMix() throws HiveException {
    GenericUDFNullif udf = new GenericUDFNullif();

    ObjectInspector[] inputOIs = { PrimitiveObjectInspectorFactory.writableDateObjectInspector,
        PrimitiveObjectInspectorFactory.writableByteObjectInspector };
    DeferredObject[] args = { new DeferredJavaObject(new DateWritableV2(4)),
        new DeferredJavaObject(new ByteWritable((byte) 4)) };

    udf.initialize(inputOIs);
  }

  @Test
  public void testDateCompareEq() throws HiveException {
    GenericUDFNullif udf = new GenericUDFNullif();

    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableDateObjectInspector,
        PrimitiveObjectInspectorFactory.writableDateObjectInspector };
    DeferredObject[] args = {
        new DeferredJavaObject(new DateWritableV2(4)),
        new DeferredJavaObject(new DateWritableV2(4))
        };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.dateTypeInfo, oi.getTypeInfo());
    Assert.assertEquals(null, udf.evaluate(args));
  }

  @Test
  public void testLazy() throws HiveException {
    GenericUDFNullif udf = new GenericUDFNullif();

    ObjectInspector[] inputOIs = { LazyPrimitiveObjectInspectorFactory.LAZY_INT_OBJECT_INSPECTOR,
        LazyPrimitiveObjectInspectorFactory.LAZY_INT_OBJECT_INSPECTOR };
    LazyInteger a1 = new LazyInteger(LazyPrimitiveObjectInspectorFactory.LAZY_INT_OBJECT_INSPECTOR);
    LazyInteger a2 = new LazyInteger(LazyPrimitiveObjectInspectorFactory.LAZY_INT_OBJECT_INSPECTOR);
    a1.getWritableObject().set(1);
    a2.getWritableObject().set(1);

    DeferredObject[] args = { new DeferredJavaObject(a1), new DeferredJavaObject(a2) };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.intTypeInfo, oi.getTypeInfo());
    Assert.assertEquals(null, udf.evaluate(args));
  }

  @Test
  public void testArrayNull() throws Exception {
    GenericUDFNullif udf = new GenericUDFNullif();

    ObjectInspector[] inputOIs = {
        ObjectInspectorFactory
            .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector),
        ObjectInspectorFactory
            .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector) };

    Object i1 = asList(new Text("aa"), new Text("dd"), new Text("cc"), new Text("bb"));
    Object i2 = asList(new Text("aa"), new Text("dd"), new Text("cc"), new Text("bb"));

    DeferredObject[] args = { new DeferredJavaObject(i1), new DeferredJavaObject(i2) };

    StandardListObjectInspector oi = (StandardListObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals("array<string>", oi.getTypeName());
    Assert.assertEquals(null, udf.evaluate(args));

  }

  @Test
  public void testArray1() throws Exception {
    GenericUDFNullif udf = new GenericUDFNullif();

    ObjectInspector[] inputOIs = {
        ObjectInspectorFactory
            .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector),
        ObjectInspectorFactory
            .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector) };

    Object i1 = asList(new Text("aa"), new Text("dd"), new Text("cc"), new Text("bb"));
    Object i2 = asList(new Text("aa"), new Text("dd"), new Text("cc"), new Text("xx"));

    DeferredObject[] args = { new DeferredJavaObject(i1), new DeferredJavaObject(i2) };

    StandardListObjectInspector oi = (StandardListObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals("array<string>", oi.getTypeName());
    Assert.assertEquals(i1, udf.evaluate(args));

  }

}
