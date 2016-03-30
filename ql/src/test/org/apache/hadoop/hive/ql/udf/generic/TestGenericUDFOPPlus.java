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

import java.sql.Date;
import java.sql.Timestamp;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Assert;
import org.junit.Test;

public class TestGenericUDFOPPlus extends AbstractTestGenericUDFOPNumeric {

  @Test
  public void testBytePlusShort() throws HiveException {
    GenericUDFOPPlus udf = new GenericUDFOPPlus();

    // Byte
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
    Assert.assertEquals(10, res.get());
  }

  @Test
  public void testVarcharPlusInt() throws HiveException {
    GenericUDFOPPlus udf = new GenericUDFOPPlus();

    // Short
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
    Assert.assertEquals(new Double(579.0), new Double(res.get()));
  }

  @Test
  public void testDoublePlusLong() throws HiveException {
    GenericUDFOPPlus udf = new GenericUDFOPPlus();

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
    Assert.assertEquals(new Double(14.5), new Double(res.get()));
  }

  @Test
  public void testLongPlusDecimal() throws HiveException {
    GenericUDFOPPlus udf = new GenericUDFOPPlus();

    // Long
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
    Assert.assertEquals( HiveDecimal.create("338.97"), res.getHiveDecimal());
  }

  @Test
  public void testFloatPlusFloat() throws HiveException {
    GenericUDFOPPlus udf = new GenericUDFOPPlus();

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

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(oi.getTypeInfo(), TypeInfoFactory.floatTypeInfo);
    FloatWritable res = (FloatWritable) udf.evaluate(args);
    Assert.assertEquals(new Float(4.5), new Float(res.get()));
  }

  @Test
  public void testDoulePlusDecimal() throws HiveException {
    GenericUDFOPPlus udf = new GenericUDFOPPlus();

    // Double
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
    Assert.assertEquals(new Double(309.49), new Double(res.get()));
  }

  @Test
  public void testDecimalPlusDecimal() throws HiveException {
    GenericUDFOPPlus udf = new GenericUDFOPPlus();

    // Decimal
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
    Assert.assertEquals(HiveDecimal.create("249.47"), res.getHiveDecimal());
  }

  @Test
  public void testDecimalPlusDecimalSameParams() throws HiveException {
    GenericUDFOPPlus udf = new GenericUDFOPPlus();

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

    verifyReturnType(new GenericUDFOPPlus(), "int", "int", "int");
    verifyReturnType(new GenericUDFOPPlus(), "int", "float", "float");
    verifyReturnType(new GenericUDFOPPlus(), "int", "double", "double");
    verifyReturnType(new GenericUDFOPPlus(), "int", "decimal(10,2)", "decimal(13,2)");

    verifyReturnType(new GenericUDFOPPlus(), "float", "float", "float");
    verifyReturnType(new GenericUDFOPPlus(), "float", "double", "double");
    verifyReturnType(new GenericUDFOPPlus(), "float", "decimal(10,2)", "float");

    verifyReturnType(new GenericUDFOPPlus(), "double", "double", "double");
    verifyReturnType(new GenericUDFOPPlus(), "double", "decimal(10,2)", "double");

    verifyReturnType(new GenericUDFOPPlus(), "decimal(10,2)", "decimal(10,2)", "decimal(11,2)");

    // Most tests are done with ANSI SQL mode enabled, set it back to true
    SessionState.get().getConf().setVar(HiveConf.ConfVars.HIVE_COMPAT, "latest");
  }

  @Test
  public void testReturnTypeAnsiSql() throws Exception {
    SessionState.get().getConf().setVar(HiveConf.ConfVars.HIVE_COMPAT, "latest");

    verifyReturnType(new GenericUDFOPPlus(), "int", "int", "int");
    verifyReturnType(new GenericUDFOPPlus(), "int", "float", "float");
    verifyReturnType(new GenericUDFOPPlus(), "int", "double", "double");
    verifyReturnType(new GenericUDFOPPlus(), "int", "decimal(10,2)", "decimal(13,2)");

    verifyReturnType(new GenericUDFOPPlus(), "float", "float", "float");
    verifyReturnType(new GenericUDFOPPlus(), "float", "double", "double");
    verifyReturnType(new GenericUDFOPPlus(), "float", "decimal(10,2)", "float");

    verifyReturnType(new GenericUDFOPPlus(), "double", "double", "double");
    verifyReturnType(new GenericUDFOPPlus(), "double", "decimal(10,2)", "double");

    verifyReturnType(new GenericUDFOPPlus(), "decimal(10,2)", "decimal(10,2)", "decimal(11,2)");
  }

  @Test
  public void testIntervalYearMonthPlusIntervalYearMonth() throws Exception {
    GenericUDFOPPlus udf = new GenericUDFOPPlus();

    HiveIntervalYearMonthWritable left =
        new HiveIntervalYearMonthWritable(HiveIntervalYearMonth.valueOf("1-2"));
    HiveIntervalYearMonthWritable right =
        new HiveIntervalYearMonthWritable(HiveIntervalYearMonth.valueOf("1-11"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableHiveIntervalYearMonthObjectInspector,
        PrimitiveObjectInspectorFactory.writableHiveIntervalYearMonthObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.intervalYearMonthTypeInfo, oi.getTypeInfo());
    HiveIntervalYearMonthWritable res = (HiveIntervalYearMonthWritable) udf.evaluate(args);
    Assert.assertEquals(HiveIntervalYearMonth.valueOf("3-1"), res.getHiveIntervalYearMonth());
  }

  @Test
  public void testIntervalYearMonthPlusDate() throws Exception {
    GenericUDFOPPlus udf = new GenericUDFOPPlus();

    HiveIntervalYearMonthWritable left =
        new HiveIntervalYearMonthWritable(HiveIntervalYearMonth.valueOf("2-8"));
    DateWritable right =
        new DateWritable(Date.valueOf("2001-06-15"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableHiveIntervalYearMonthObjectInspector,
        PrimitiveObjectInspectorFactory.writableDateObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.dateTypeInfo, oi.getTypeInfo());
    DateWritable res = (DateWritable) udf.evaluate(args);
    Assert.assertEquals(Date.valueOf("2004-02-15"), res.get());
  }

  @Test
  public void testDatePlusIntervalYearMonth() throws Exception {
    GenericUDFOPPlus udf = new GenericUDFOPPlus();

    DateWritable left =
        new DateWritable(Date.valueOf("2001-06-15"));
    HiveIntervalYearMonthWritable right =
        new HiveIntervalYearMonthWritable(HiveIntervalYearMonth.valueOf("2-8"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableDateObjectInspector,
        PrimitiveObjectInspectorFactory.writableHiveIntervalYearMonthObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.dateTypeInfo, oi.getTypeInfo());
    DateWritable res = (DateWritable) udf.evaluate(args);
    Assert.assertEquals(Date.valueOf("2004-02-15"), res.get());
  }

  @Test
  public void testIntervalYearMonthPlusTimestamp() throws Exception {
    GenericUDFOPPlus udf = new GenericUDFOPPlus();

    HiveIntervalYearMonthWritable left =
        new HiveIntervalYearMonthWritable(HiveIntervalYearMonth.valueOf("2-2"));
    TimestampWritable right =
        new TimestampWritable(Timestamp.valueOf("2001-11-15 01:02:03.123456789"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableHiveIntervalYearMonthObjectInspector,
        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.timestampTypeInfo, oi.getTypeInfo());
    TimestampWritable res = (TimestampWritable) udf.evaluate(args);
    Assert.assertEquals(Timestamp.valueOf("2004-01-15 01:02:03.123456789"), res.getTimestamp());
  }

  @Test
  public void testTimestampPlusIntervalYearMonth() throws Exception {
    GenericUDFOPPlus udf = new GenericUDFOPPlus();

    TimestampWritable left =
        new TimestampWritable(Timestamp.valueOf("2001-11-15 01:02:03.123456789"));
    HiveIntervalYearMonthWritable right =
        new HiveIntervalYearMonthWritable(HiveIntervalYearMonth.valueOf("2-2"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector,
        PrimitiveObjectInspectorFactory.writableHiveIntervalYearMonthObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.timestampTypeInfo, oi.getTypeInfo());
    TimestampWritable res = (TimestampWritable) udf.evaluate(args);
    Assert.assertEquals(Timestamp.valueOf("2004-01-15 01:02:03.123456789"), res.getTimestamp());
  }

  @Test
  public void testIntervalDayTimePlusIntervalDayTime() throws Exception {
    GenericUDFOPPlus udf = new GenericUDFOPPlus();

    HiveIntervalDayTimeWritable left =
        new HiveIntervalDayTimeWritable(HiveIntervalDayTime.valueOf("1 0:0:0.567"));
    HiveIntervalDayTimeWritable right =
        new HiveIntervalDayTimeWritable(HiveIntervalDayTime.valueOf("1 2:3:4"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableHiveIntervalDayTimeObjectInspector,
        PrimitiveObjectInspectorFactory.writableHiveIntervalDayTimeObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.intervalDayTimeTypeInfo, oi.getTypeInfo());
    HiveIntervalDayTimeWritable res = (HiveIntervalDayTimeWritable) udf.evaluate(args);
    Assert.assertEquals(HiveIntervalDayTime.valueOf("2 2:3:4.567"), res.getHiveIntervalDayTime());
  }

  @Test
  public void testIntervalDayTimePlusTimestamp() throws Exception {
    GenericUDFOPPlus udf = new GenericUDFOPPlus();

    HiveIntervalDayTimeWritable left =
        new HiveIntervalDayTimeWritable(HiveIntervalDayTime.valueOf("1 2:3:4.567"));
    TimestampWritable right =
        new TimestampWritable(Timestamp.valueOf("2001-01-01 00:00:00"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableHiveIntervalDayTimeObjectInspector,
        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.timestampTypeInfo, oi.getTypeInfo());
    TimestampWritable res = (TimestampWritable) udf.evaluate(args);
    Assert.assertEquals(Timestamp.valueOf("2001-01-02 2:3:4.567"), res.getTimestamp());
  }

  @Test
  public void testTimestampPlusIntervalDayTime() throws Exception {
    GenericUDFOPPlus udf = new GenericUDFOPPlus();

    TimestampWritable left =
        new TimestampWritable(Timestamp.valueOf("2001-01-01 00:00:00"));
    HiveIntervalDayTimeWritable right =
        new HiveIntervalDayTimeWritable(HiveIntervalDayTime.valueOf("1 2:3:4.567"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector,
        PrimitiveObjectInspectorFactory.writableHiveIntervalDayTimeObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.timestampTypeInfo, oi.getTypeInfo());
    TimestampWritable res = (TimestampWritable) udf.evaluate(args);
    Assert.assertEquals(Timestamp.valueOf("2001-01-02 2:3:4.567"), res.getTimestamp());
  }

  @Test
  public void testIntervalDayTimePlusDate() throws Exception {
    GenericUDFOPPlus udf = new GenericUDFOPPlus();

    HiveIntervalDayTimeWritable left =
        new HiveIntervalDayTimeWritable(HiveIntervalDayTime.valueOf("1 2:3:4.567"));
    DateWritable right =
        new DateWritable(Date.valueOf("2001-01-01"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableHiveIntervalDayTimeObjectInspector,
        PrimitiveObjectInspectorFactory.writableDateObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    // Date + day-time interval = timestamp
    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.timestampTypeInfo, oi.getTypeInfo());
    TimestampWritable res = (TimestampWritable) udf.evaluate(args);
    Assert.assertEquals(Timestamp.valueOf("2001-01-02 2:3:4.567"), res.getTimestamp());
  }

  @Test
  public void testDatePlusIntervalDayTime() throws Exception {
    GenericUDFOPPlus udf = new GenericUDFOPPlus();

    DateWritable left =
        new DateWritable(Date.valueOf("2001-01-01"));
    HiveIntervalDayTimeWritable right =
        new HiveIntervalDayTimeWritable(HiveIntervalDayTime.valueOf("1 2:3:4.567"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableDateObjectInspector,
        PrimitiveObjectInspectorFactory.writableHiveIntervalDayTimeObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    // Date + day-time interval = timestamp
    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.timestampTypeInfo, oi.getTypeInfo());
    TimestampWritable res = (TimestampWritable) udf.evaluate(args);
    Assert.assertEquals(Timestamp.valueOf("2001-01-02 2:3:4.567"), res.getTimestamp());
  }
}
