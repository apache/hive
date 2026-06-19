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

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class TestGenericUDFCeil {

  @Test
  public void testByte() throws HiveException {
    GenericUDFCeil udf = new GenericUDFCeil();

    ByteWritable input = new ByteWritable((byte) 4);
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableByteObjectInspector,
    };
    DeferredObject[] args = {
        new DeferredJavaObject(input)
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.longTypeInfo, oi.getTypeInfo());
    LongWritable res = (LongWritable) udf.evaluate(args);
    Assert.assertEquals(4L, res.get());
  }

  @Test
  public void testShort() throws HiveException {
    GenericUDFCeil udf = new GenericUDFCeil();

    ShortWritable input = new ShortWritable((short) -74);
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableShortObjectInspector,
    };
    DeferredObject[] args = {
        new DeferredJavaObject(input)
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.longTypeInfo, oi.getTypeInfo());
    LongWritable res = (LongWritable) udf.evaluate(args);
    Assert.assertEquals(-74L, res.get());
  }

  @Test
  public void testInt() throws HiveException {
    GenericUDFCeil udf = new GenericUDFCeil();

    IntWritable input = new IntWritable(747);
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableIntObjectInspector,
    };
    DeferredObject[] args = {
        new DeferredJavaObject(input)
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.longTypeInfo, oi.getTypeInfo());
    LongWritable res = (LongWritable) udf.evaluate(args);
    Assert.assertEquals(747L, res.get());
  }

  @Test
  public void testLong() throws HiveException {
    GenericUDFCeil udf = new GenericUDFCeil();

    LongWritable input = new LongWritable(3234747);
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableLongObjectInspector,
    };
    DeferredObject[] args = {
        new DeferredJavaObject(input)
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.longTypeInfo, oi.getTypeInfo());
    LongWritable res = (LongWritable) udf.evaluate(args);
    Assert.assertEquals(3234747L, res.get());
  }

  @Test
  public void testFloat() throws HiveException {
    GenericUDFCeil udf = new GenericUDFCeil();

    FloatWritable input = new FloatWritable(323.4747f);
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
    };
    DeferredObject[] args = {
        new DeferredJavaObject(input)
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.longTypeInfo, oi.getTypeInfo());
    LongWritable res = (LongWritable) udf.evaluate(args);
    Assert.assertEquals(324L, res.get());
  }

  @Test
  public void testDouble() throws HiveException {
    GenericUDFCeil udf = new GenericUDFCeil();

    DoubleWritable input = new DoubleWritable(32300.004747);
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
    };
    DeferredObject[] args = {
        new DeferredJavaObject(input)
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.longTypeInfo, oi.getTypeInfo());
    LongWritable res = (LongWritable) udf.evaluate(args);
    Assert.assertEquals(32301L, res.get());
  }

  @Test
  public void testDecimal() throws HiveException {
    GenericUDFCeil udf = new GenericUDFCeil();

    HiveDecimalWritable input = new HiveDecimalWritable(HiveDecimal.create("32300.004747"));
    DecimalTypeInfo inputTypeInfo = TypeInfoFactory.getDecimalTypeInfo(11, 6);
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(inputTypeInfo),
    };
    DeferredObject[] args = {
        new DeferredJavaObject(input)
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.getDecimalTypeInfo(6, 0), oi.getTypeInfo());
    HiveDecimalWritable res = (HiveDecimalWritable) udf.evaluate(args);
    Assert.assertEquals(HiveDecimal.create("32301"), res.getHiveDecimal());
  }

  @Test
  public void testString() throws HiveException {
    GenericUDFCeil udf = new GenericUDFCeil();

    Text input = new Text("32300.004747");
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableStringObjectInspector,
    };
    DeferredObject[] args = {
        new DeferredJavaObject(input)
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.longTypeInfo, oi.getTypeInfo());
    LongWritable res = (LongWritable) udf.evaluate(args);
    Assert.assertEquals(32301L, res.get());
  }

  @Test
  public void testVarchar() throws HiveException {
    GenericUDFCeil udf = new GenericUDFCeil();

    HiveVarchar vc = new HiveVarchar("32300.004747", 12);
    HiveVarcharWritable input = new HiveVarcharWritable(vc);
    VarcharTypeInfo inputTypeInfo = TypeInfoFactory.getVarcharTypeInfo(12);
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(inputTypeInfo),
    };
    DeferredObject[] args = {
        new DeferredJavaObject(input)
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.longTypeInfo, oi.getTypeInfo());
    LongWritable res = (LongWritable) udf.evaluate(args);
    Assert.assertEquals(32301L, res.get());
  }

  @Test
  public void testChar() throws HiveException {
    GenericUDFCeil udf = new GenericUDFCeil();

    HiveChar vc = new HiveChar("-32300.004747", 12);
    HiveCharWritable input = new HiveCharWritable(vc);
    CharTypeInfo inputTypeInfo = TypeInfoFactory.getCharTypeInfo(12);
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(inputTypeInfo),
    };
    DeferredObject[] args = {
        new DeferredJavaObject(input)
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.longTypeInfo, oi.getTypeInfo());
    LongWritable res = (LongWritable) udf.evaluate(args);
    Assert.assertEquals(-32300L, res.get());
  }

}
