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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.util.DateTimeMath;
import org.apache.hadoop.hive.serde2.RandomTypeUtil;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.util.TimestampUtils;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;

/**
 * Test VectorExpression classes for vectorized implementations of type casts.
 */
public class TestVectorTypeCasts {

  @Test
  public void testVectorCastLongToDouble() throws HiveException {
    VectorizedRowBatch b = TestVectorMathFunctions.getVectorizedRowBatchLongInDoubleOut();
    DoubleColumnVector resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new CastLongToDouble(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(2.0, resultV.vector[4], Double.MIN_VALUE);
  }

  @Test
  public void testVectorCastDoubleToLong() throws HiveException {
    VectorizedRowBatch b = TestVectorMathFunctions.getVectorizedRowBatchDoubleInLongOut();
    LongColumnVector resultV = (LongColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new CastDoubleToLong(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(1, resultV.vector[6]);
  }

  @Test
  public void testCastDateToString() throws HiveException {
    int[] intValues = new int[100];
    VectorizedRowBatch b = TestVectorMathFunctions.getVectorizedRowBatchDateInStringOut(intValues);
    BytesColumnVector resultV = (BytesColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new CastDateToString(0, 1);
    expr.evaluate(b);

    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
    formatter.setCalendar(DateTimeMath.getProlepticGregorianCalendarUTC());

    String expected, result;
    for (int i = 0; i < intValues.length; i++) {
      expected = formatter.format(new java.sql.Date(DateWritableV2.daysToMillis(intValues[i])));
      byte[] subbyte = Arrays.copyOfRange(resultV.vector[i], resultV.start[i],
          resultV.start[i] + resultV.length[i]);
      result = new String(subbyte, StandardCharsets.UTF_8);

      Assert.assertEquals("Index: " + i + " Epoch day value: " + intValues[i], expected, result);
    }
  }

  @Test
  public void testCastDateToTimestamp() throws HiveException {
    int[] intValues = new int[500];
    VectorizedRowBatch b = TestVectorMathFunctions.getVectorizedRowBatchDateInTimestampOut(intValues);
    TimestampColumnVector resultV = (TimestampColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new CastDateToTimestamp(0, 1);
    expr.evaluate(b);
    for (int i = 0; i < intValues.length; i++) {
      Timestamp timestamp = resultV.asScratchTimestamp(i);
      long actual = DateWritableV2.millisToDays(timestamp.getTime());
      assertEquals(actual, intValues[i]);
    }
  }

  @Test
  public void testCastDoubleToBoolean() throws HiveException {
    VectorizedRowBatch b = TestVectorMathFunctions.getVectorizedRowBatchDoubleInLongOut();
    LongColumnVector resultV = (LongColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new CastDoubleToBooleanViaDoubleToLong(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(0, resultV.vector[3]);
    Assert.assertEquals(1, resultV.vector[4]);
  }

  @Test
  public void testCastDoubleToTimestamp() throws HiveException {
    VectorizedRowBatch b = TestVectorMathFunctions.getVectorizedRowBatchDoubleInTimestampOut();
    TimestampColumnVector resultV = (TimestampColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new CastDoubleToTimestamp(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(0.0, TimestampUtils.getDouble(resultV.asScratchTimestamp(3)), Double.MIN_VALUE);
    Assert.assertEquals(0.5d, TimestampUtils.getDouble(resultV.asScratchTimestamp(4)), Double.MIN_VALUE);
  }

  @Test
  public void testCastLongToBoolean() throws HiveException {
    VectorizedRowBatch b = TestVectorMathFunctions.getVectorizedRowBatchLongInLongOut();
    LongColumnVector inV = (LongColumnVector) b.cols[0];
    inV.vector[0] = 0;  // make one entry produce false in result
    LongColumnVector resultV = (LongColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new CastLongToBooleanViaLongToLong(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(0, resultV.vector[0]);
    Assert.assertEquals(1, resultV.vector[1]);
  }

  @Test
  public void testCastStringToBoolean() throws HiveException {
    VectorizedRowBatch b = TestVectorMathFunctions.getVectorizedRowBatchStringInLongOut();
    LongColumnVector resultV = (LongColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new CastStringToBoolean(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(1, resultV.vector[0]); // true
    Assert.assertEquals(1, resultV.vector[1]); // true
    Assert.assertEquals(1, resultV.vector[2]); // true
    Assert.assertEquals(0, resultV.vector[3]); // false
    Assert.assertEquals(0, resultV.vector[4]); // false
    Assert.assertEquals(0, resultV.vector[5]); // false
    Assert.assertEquals(0, resultV.vector[6]); // false
    Assert.assertEquals(1, resultV.vector[7]); // true
  }

  @Test
  public void testCastLongToTimestamp() throws HiveException {
    long[] longValues = new long[500];
    VectorizedRowBatch b = TestVectorMathFunctions.getVectorizedRowBatchLongInTimestampOut(longValues);
    TimestampColumnVector resultV = (TimestampColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new CastLongToTimestamp(0, 1);
    expr.evaluate(b);
    for (int i = 0; i < longValues.length; i++) {
      Timestamp timestamp = resultV.asScratchTimestamp(i);
      long actual = TimestampWritableV2.getLong(
          org.apache.hadoop.hive.common.type.Timestamp.ofEpochMilli(timestamp.getTime(), timestamp.getNanos()));
      assertEquals(actual, longValues[i]);
    }
  }

  @Test
  public void testCastTimestampToLong() throws HiveException {
    long[] longValues = new long[500];
    VectorizedRowBatch b = TestVectorMathFunctions.getVectorizedRowBatchTimestampInLongOut(longValues);
    TimestampColumnVector inV = (TimestampColumnVector) b.cols[0];
    LongColumnVector resultV = (LongColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new CastTimestampToLong(0, 1);
    expr.setOutputTypeInfo(TypeInfoFactory.longTypeInfo);
    expr.setOutputDataTypePhysicalVariation(DataTypePhysicalVariation.NONE);
    expr.transientInit();
    expr.evaluate(b);
    for (int i = 0; i < longValues.length; i++) {
      long actual = resultV.vector[i];
      long timestampLong = inV.getTimestampAsLong(i);
      if (actual != timestampLong) {
        assertTrue(false);
      }
    }
  }

  @Test
  public void testCastTimestampToDouble() throws HiveException {
    double[] doubleValues = new double[500];
    VectorizedRowBatch b = TestVectorMathFunctions.getVectorizedRowBatchTimestampInDoubleOut(doubleValues);
    TimestampColumnVector inV = (TimestampColumnVector) b.cols[0];
    DoubleColumnVector resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new CastTimestampToDouble(0, 1);
    expr.evaluate(b);
    for (int i = 0; i < doubleValues.length; i++) {
      double actual = resultV.vector[i];
      double doubleValue = TimestampUtils.getDouble(inV.asScratchTimestamp(i));
      assertEquals(actual, doubleValue, 0.000000001F);
    }
  }

  @Test
  public void testCastTimestampToString() throws HiveException {
    int numberToTest = 100;
    long[] epochSecondValues = new long[numberToTest];
    int[] nanoValues = new int[numberToTest];
    VectorizedRowBatch b =
        TestVectorMathFunctions.getVectorizedRowBatchTimestampInStringOut(epochSecondValues, nanoValues);
    BytesColumnVector resultV = (BytesColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new CastTimestampToString(0, 1);
    expr.evaluate(b);

    String expected, result;
    for (int i = 0; i < numberToTest; i++) {
      expected = org.apache.hadoop.hive.common.type.Timestamp
          .ofEpochSecond(epochSecondValues[i], nanoValues[i]).toString();
      byte[] subbyte = Arrays.copyOfRange(resultV.vector[i], resultV.start[i],
          resultV.start[i] + resultV.length[i]);
      result = new String(subbyte, StandardCharsets.UTF_8);
      Assert.assertEquals("Index: " +  i + " Seconds since epoch: " + epochSecondValues[i] +
              " nanoseconds: " + nanoValues[i],
          expected, result);
    }
  }

  public byte[] toBytes(String s) {
    byte[] b = null;
    try {
      b = s.getBytes("UTF-8");
    } catch (Exception e) {
      throw new RuntimeException("Could not convert string to UTF-8 byte array.");
    }
    return b;
  }

  @Test
  public void testCastLongToString() throws HiveException {
    VectorizedRowBatch b = TestVectorMathFunctions.getBatchForStringMath();
    BytesColumnVector resultV = (BytesColumnVector) b.cols[2];
    b.cols[1].noNulls = true;
    VectorExpression expr = new CastLongToString(1, 2);
    expr.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.longTypeInfo});
    expr.transientInit();
    expr.evaluate(b);
    byte[] num255 = toBytes("255");
    Assert.assertEquals(0,
        StringExpr.compare(num255, 0, num255.length,
            resultV.vector[1], resultV.start[1], resultV.length[1]));
  }

  @Test
  public void testCastBooleanToString() throws HiveException {
    byte[] t = toBytes("TRUE");
    byte[] f = toBytes("FALSE");
    VectorizedRowBatch b = TestVectorMathFunctions.getBatchForStringMath();
    LongColumnVector inV = (LongColumnVector) b.cols[1];
    BytesColumnVector resultV = (BytesColumnVector) b.cols[2];
    inV.vector[1] = 1;
    VectorExpression expr = new CastBooleanToStringViaLongToString(1, 2);
    expr.evaluate(b);
    Assert.assertEquals(0,
        StringExpr.compare(f, 0, f.length,
            resultV.vector[0], resultV.start[0], resultV.length[0]));
    Assert.assertEquals(0,
        StringExpr.compare(t, 0, t.length,
            resultV.vector[1], resultV.start[1], resultV.length[1]));
  }

  @Test
  public void testCastDecimalToLong() throws HiveException {

    // test basic case
    VectorizedRowBatch b = getBatchDecimalLong();
    VectorExpression expr = new CastDecimalToLong(0, 1);

    // With the integer type range checking, we need to know the Hive data type.
    expr.setOutputTypeInfo(TypeInfoFactory.longTypeInfo);
    expr.transientInit();
    expr.evaluate(b);
    LongColumnVector r = (LongColumnVector) b.cols[1];
    assertEquals(1, r.vector[0]);
    assertEquals(-2, r.vector[1]);
    assertEquals(9999999999999999L, r.vector[2]);

    // test with nulls in input
    b = getBatchDecimalLong();
    b.cols[0].noNulls = false;
    b.cols[0].isNull[1] = true;
    expr.evaluate(b);
    r = (LongColumnVector) b.cols[1];
    assertFalse(r.noNulls);
    assertTrue(r.isNull[1]);
    assertFalse(r.isNull[0]);
    assertEquals(1, r.vector[0]);

    // test repeating case
    b = getBatchDecimalLong();
    b.cols[0].isRepeating = true;
    expr.evaluate(b);
    r = (LongColumnVector) b.cols[1];
    assertTrue(r.isRepeating);
    assertEquals(1, r.vector[0]);

    // test repeating nulls case
    b = getBatchDecimalLong();
    b.cols[0].isRepeating = true;
    b.cols[0].noNulls = false;
    b.cols[0].isNull[0] = true;
    expr.evaluate(b);
    r = (LongColumnVector) b.cols[1];
    assertTrue(r.isRepeating);
    assertTrue(r.isNull[0]);
  }

  @Test
  /**
   * Just spot check the basic case because code path is the same as
   * for cast of decimal to long due to inheritance.
   */
  public void testCastDecimalToBoolean() throws HiveException {
    VectorizedRowBatch b = getBatchDecimalLong();
    VectorExpression expr = new CastDecimalToBoolean(0, 1);
    expr.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.decimalTypeInfo});
    expr.setOutputTypeInfo(TypeInfoFactory.booleanTypeInfo);
    expr.transientInit();
    DecimalColumnVector in = (DecimalColumnVector) b.cols[0];
    in.vector[1].set(HiveDecimal.create(0));
    expr.evaluate(b);
    LongColumnVector r = (LongColumnVector) b.cols[1];
    assertEquals(1, r.vector[0]);
    assertEquals(0, r.vector[1]);
    assertEquals(1, r.vector[2]);
  }

  private VectorizedRowBatch getBatchDecimalLong() {
    VectorizedRowBatch b = new VectorizedRowBatch(2);
    DecimalColumnVector dv;
    short scale = 2;
    b.cols[0] = dv = new DecimalColumnVector(18, scale);
    b.cols[1] = new LongColumnVector();

    b.size = 3;

    dv.vector[0].set(HiveDecimal.create("1.1"));
    dv.vector[1].set(HiveDecimal.create("-2.2"));
    dv.vector[2].set(HiveDecimal.create("9999999999999999.00"));

    return b;
  }

  @Test
  public void testCastDecimalToDouble() throws HiveException {

    final double eps = 0.000001d; // tolerance to check double equality

    // test basic case
    VectorizedRowBatch b = getBatchDecimalDouble();
    VectorExpression expr = new CastDecimalToDouble(0, 1);
    expr.evaluate(b);
    DoubleColumnVector r = (DoubleColumnVector) b.cols[1];
    assertEquals(1.1d, r.vector[0], eps);
    assertEquals(-2.2d, r.vector[1], eps);
    assertEquals(9999999999999999.0d, r.vector[2], eps);

    // test with nulls in input
    b = getBatchDecimalDouble();
    b.cols[0].noNulls = false;
    b.cols[0].isNull[1] = true;
    expr.evaluate(b);
    r = (DoubleColumnVector) b.cols[1];
    assertFalse(r.noNulls);
    assertTrue(r.isNull[1]);
    assertFalse(r.isNull[0]);
    assertEquals(1.1d, r.vector[0], eps);

    // test repeating case
    b = getBatchDecimalDouble();
    b.cols[0].isRepeating = true;
    expr.evaluate(b);
    r = (DoubleColumnVector) b.cols[1];
    assertTrue(r.isRepeating);
    assertEquals(1.1d, r.vector[0], eps);

    // test repeating nulls case
    b = getBatchDecimalDouble();
    b.cols[0].isRepeating = true;
    b.cols[0].noNulls = false;
    b.cols[0].isNull[0] = true;
    expr.evaluate(b);
    r = (DoubleColumnVector) b.cols[1];
    assertTrue(r.isRepeating);
    assertTrue(r.isNull[0]);
  }

  private VectorizedRowBatch getBatchDecimalDouble() {
    VectorizedRowBatch b = new VectorizedRowBatch(2);
    DecimalColumnVector dv;
    short scale = 2;
    b.cols[0] = dv = new DecimalColumnVector(18, scale);
    b.cols[1] = new DoubleColumnVector();

    b.size = 3;

    dv.vector[0].set(HiveDecimal.create("1.1"));
    dv.vector[1].set(HiveDecimal.create("-2.2"));
    dv.vector[2].set(HiveDecimal.create("9999999999999999.00"));

    return b;
  }

  @Test
  public void testCastDecimalToFloat() throws HiveException {

    final double eps = 0.00000000000001d; // tolerance to check float equality

    double f1 = HiveDecimal.create("1.1").floatValue();
    double f2 = HiveDecimal.create("-2.2").floatValue();
    double f3 = HiveDecimal.create("9999999999999999.00").floatValue();

    // test basic case
    VectorizedRowBatch b = getBatchDecimalDouble();
    VectorExpression expr = new CastDecimalToFloat(0, 1);
    expr.evaluate(b);
    DoubleColumnVector r = (DoubleColumnVector) b.cols[1];
    assertEquals(f1, r.vector[0], eps);
    assertEquals(f2, r.vector[1], eps);
    assertEquals(f3, r.vector[2], eps);

    // test with nulls in input
    b = getBatchDecimalDouble();
    b.cols[0].noNulls = false;
    b.cols[0].isNull[1] = true;
    expr.evaluate(b);
    r = (DoubleColumnVector) b.cols[1];
    assertFalse(r.noNulls);
    assertTrue(r.isNull[1]);
    assertFalse(r.isNull[0]);
    assertEquals(f1, r.vector[0], eps);

    // test repeating case
    b = getBatchDecimalDouble();
    b.cols[0].isRepeating = true;
    expr.evaluate(b);
    r = (DoubleColumnVector) b.cols[1];
    assertTrue(r.isRepeating);
    assertEquals(f1, r.vector[0], eps);

    // test repeating nulls case
    b = getBatchDecimalDouble();
    b.cols[0].isRepeating = true;
    b.cols[0].noNulls = false;
    b.cols[0].isNull[0] = true;
    expr.evaluate(b);
    r = (DoubleColumnVector) b.cols[1];
    assertTrue(r.isRepeating);
    assertTrue(r.isNull[0]);
  }

  @Test
  public void testCastDecimalToString() throws HiveException {
    VectorizedRowBatch b = getBatchDecimalString();
    VectorExpression expr = new CastDecimalToString(0, 1);
    expr.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.decimalTypeInfo});
    expr.transientInit();
    expr.evaluate(b);
    BytesColumnVector r = (BytesColumnVector) b.cols[1];

    // As of HIVE-8745, these decimal values should be trimmed of trailing zeros.
    byte[] v = toBytes("1.10");
    assertTrue(((Integer) v.length).toString() + " " + r.length[0], v.length == r.length[0]);
    Assert.assertEquals(0,
        StringExpr.compare(v, 0, v.length,
            r.vector[0], r.start[0], r.length[0]));

    v = toBytes("-2.20");
    Assert.assertEquals(0,
        StringExpr.compare(v, 0, v.length,
            r.vector[1], r.start[1], r.length[1]));

    v = toBytes("9999999999999999.00");
    Assert.assertEquals(0,
        StringExpr.compare(v, 0, v.length,
            r.vector[2], r.start[2], r.length[2]));
  }

  private VectorizedRowBatch getBatchDecimalString() {
    VectorizedRowBatch b = new VectorizedRowBatch(2);
    DecimalColumnVector dv;
    short scale = 2;
    b.cols[0] = dv = new DecimalColumnVector(18, scale);
    b.cols[1] = new BytesColumnVector();

    b.size = 3;

    dv.vector[0].set(HiveDecimal.create("1.1"));
    dv.vector[1].set(HiveDecimal.create("-2.2"));
    dv.vector[2].set(HiveDecimal.create("9999999999999999.00"));

    return b;
  }

  @Test
  public void testCastDecimalToTimestamp() throws HiveException {
    double[] doubleValues = new double[500];
    VectorizedRowBatch b = getBatchDecimalTimestamp(doubleValues);
    VectorExpression expr = new CastDecimalToTimestamp(0, 1);
    expr.evaluate(b);
    TimestampColumnVector r = (TimestampColumnVector) b.cols[1];
    for (int i = 0; i < doubleValues.length; i++) {
      Timestamp timestamp = r.asScratchTimestamp(i);
      double asDouble = TimestampUtils.getDouble(timestamp);
      double expectedDouble = doubleValues[i];
      if (expectedDouble != asDouble) {
        assertTrue(false);
      }
    }
  }

  private VectorizedRowBatch getBatchDecimalLong2() {
    VectorizedRowBatch b = new VectorizedRowBatch(2);
    DecimalColumnVector dv;
    short scale = 9;
    b.cols[0] = dv = new DecimalColumnVector(18, scale);
    b.cols[1] = new LongColumnVector();

    b.size = 3;

    dv.vector[0].set(HiveDecimal.create("1.111111111"));
    dv.vector[1].set(HiveDecimal.create("-2.222222222"));
    dv.vector[2].set(HiveDecimal.create("31536000.999999999"));

    return b;
  }

  private VectorizedRowBatch getBatchDecimalTimestamp(double[] doubleValues) {
    VectorizedRowBatch b = new VectorizedRowBatch(2);
    DecimalColumnVector dv;
    b.cols[0] = dv = new DecimalColumnVector(doubleValues.length, HiveDecimal.SYSTEM_DEFAULT_PRECISION, HiveDecimal.SYSTEM_DEFAULT_SCALE);
    b.cols[1] = new TimestampColumnVector(doubleValues.length);
    dv.noNulls = true;
    Random r = new Random(94830);
    for (int i = 0; i < doubleValues.length; i++) {
      long millis = RandomTypeUtil.randomMillis(r);
      Timestamp ts = new Timestamp(millis);
      int nanos = RandomTypeUtil.randomNanos(r);
      ts.setNanos(nanos);
      TimestampWritableV2 tsw = new TimestampWritableV2(
          org.apache.hadoop.hive.common.type.Timestamp.ofEpochMilli(ts.getTime(), ts.getNanos()));
      double asDouble = tsw.getDouble();
      doubleValues[i] = asDouble;
      HiveDecimal hiveDecimal = HiveDecimal.create(new BigDecimal(asDouble));
      dv.set(i, hiveDecimal);
    }
    b.size = doubleValues.length;
    return b;
  }

  @Test
  public void testCastLongToDecimal() throws HiveException {
    VectorizedRowBatch b = getBatchLongDecimal();
    VectorExpression expr = new CastLongToDecimal(0, 1);
    expr.evaluate(b);
    DecimalColumnVector r = (DecimalColumnVector) b.cols[1];
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("0")));
    assertTrue(r.vector[1].getHiveDecimal().equals(HiveDecimal.create("-1")));
    assertTrue(r.vector[2].getHiveDecimal().equals(HiveDecimal.create("99999999999999")));
  }

  private VectorizedRowBatch getBatchLongDecimal() {
    VectorizedRowBatch b = new VectorizedRowBatch(2);
    LongColumnVector lv;
    b.cols[0] = lv = new LongColumnVector();
    b.cols[1] = new DecimalColumnVector(18, 2);
    lv.vector[0] = 0;
    lv.vector[1] = -1;
    lv.vector[2] = 99999999999999L;
    return b;
  }


  public static final long NANOSECONDS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
  public static final long MILLISECONDS_PER_SECOND = TimeUnit.SECONDS.toMillis(1);
  public static final long NANOSECONDS_PER_MILLISSECOND = TimeUnit.MILLISECONDS.toNanos(1);

  private VectorizedRowBatch getBatchTimestampDecimal(HiveDecimal[] hiveDecimalValues) {
    Random r = new Random(994);
    VectorizedRowBatch b = new VectorizedRowBatch(2);
    TimestampColumnVector tcv;
    b.cols[0] = tcv = new TimestampColumnVector(hiveDecimalValues.length);
    b.cols[1] = new DecimalColumnVector(hiveDecimalValues.length, HiveDecimal.SYSTEM_DEFAULT_PRECISION, HiveDecimal.SYSTEM_DEFAULT_SCALE);
    for (int i = 0; i < hiveDecimalValues.length; i++) {
      int optionalNanos = 0;
      switch (r.nextInt(4)) {
      case 0:
        // No nanos.
        break;
      case 1:
        optionalNanos = r.nextInt((int) NANOSECONDS_PER_SECOND);
        break;
      case 2:
        // Limit to milliseconds only...
        optionalNanos = r.nextInt((int) MILLISECONDS_PER_SECOND) * (int) NANOSECONDS_PER_MILLISSECOND;
        break;
      case 3:
        // Limit to below milliseconds only...
        optionalNanos = r.nextInt((int) NANOSECONDS_PER_MILLISSECOND);
        break;
      }
      long millis = RandomTypeUtil.randomMillis(r);
      Timestamp ts = new Timestamp(millis);
      ts.setNanos(optionalNanos);
      TimestampWritableV2 tsw = new TimestampWritableV2(
          org.apache.hadoop.hive.common.type.Timestamp.ofEpochMilli(ts.getTime(), ts.getNanos()));
      hiveDecimalValues[i] = tsw.getHiveDecimal();

      tcv.set(i, ts);
    }
    b.size = hiveDecimalValues.length;
    return b;
  }

  @Test
  public void testCastDoubleToDecimal() throws HiveException {
    VectorizedRowBatch b = getBatchDoubleDecimal();
    VectorExpression expr = new CastDoubleToDecimal(0, 1);
    expr.evaluate(b);
    DecimalColumnVector r = (DecimalColumnVector) b.cols[1];

    HiveDecimal hd0 = HiveDecimal.create("0.0");
    if (!hd0.equals(r.vector[0].getHiveDecimal())) {
      assertTrue(false);
    }
    HiveDecimal hd1 = HiveDecimal.create("-1.0");
    if (!hd1.equals(r.vector[1].getHiveDecimal())) {
      assertTrue(false);
    }
    HiveDecimal hd2 = HiveDecimal.create("99999999999999");
    if (!hd2.equals(r.vector[2].getHiveDecimal())) {
      assertTrue(false);
    }
  }

  private VectorizedRowBatch getBatchDoubleDecimal() {
    VectorizedRowBatch b = new VectorizedRowBatch(2);
    DoubleColumnVector dv;
    short scale = 2;
    b.cols[0] = dv = new DoubleColumnVector();
    b.cols[1] = new DecimalColumnVector(18, scale);

    b.size = 3;

    dv.vector[0] = 0d;
    dv.vector[1] = -1d;
    dv.vector[2] = 99999999999999.0d;

    return b;
  }

  @Test
  public void testCastStringToDecimal() throws HiveException {
    VectorizedRowBatch b = getBatchStringDecimal();
    VectorExpression expr = new CastStringToDecimal(0, 1);
    expr.evaluate(b);
    DecimalColumnVector r = (DecimalColumnVector) b.cols[1];
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("1.10")));
    assertTrue(r.vector[1].getHiveDecimal().equals(HiveDecimal.create("-2.20")));
    assertTrue(r.vector[2].getHiveDecimal().equals(HiveDecimal.create("99999999999999.0")));
  }

  private VectorizedRowBatch getBatchStringDecimal() {
    VectorizedRowBatch b = new VectorizedRowBatch(2);
    BytesColumnVector bv;
    b.cols[0] = bv = new BytesColumnVector();
    b.cols[1] = new DecimalColumnVector(18, 2);

    bv.initBuffer();

    byte[] x0 = toBytes("1.10");
    byte[] x1 = toBytes("-2.20");
    byte[] x2 = toBytes("99999999999999.0");

    bv.setVal(0, x0, 0, x0.length);
    bv.setVal(1, x1, 0, x1.length);
    bv.setVal(2, x2, 0, x2.length);

    return b;
  }

  @Test
  public void testCastTimestampToDecimal() throws HiveException {

    // The input timestamps are stored as long values
    // measured in nanoseconds from the epoch.
    HiveDecimal[] hiveDecimalValues = new HiveDecimal[500];
    VectorizedRowBatch b = getBatchTimestampDecimal(hiveDecimalValues);
    VectorExpression expr = new CastTimestampToDecimal(0, 1);

    expr.evaluate(b);
    DecimalColumnVector r = (DecimalColumnVector) b.cols[1];
    for (int i = 0; i < hiveDecimalValues.length; i++) {
      HiveDecimal hiveDecimal = r.vector[i].getHiveDecimal();
      HiveDecimal expectedHiveDecimal = hiveDecimalValues[i];
      if (!hiveDecimal.equals(expectedHiveDecimal)) {
        assertTrue(false);
      }
    }

    // Try again with a value that won't fit in 5 digits, to make
    // sure that NULL is produced.
    b.cols[1] = r = new DecimalColumnVector(hiveDecimalValues.length, 5, 2);
    expr.evaluate(b);
    r = (DecimalColumnVector) b.cols[1];
    for (int i = 0; i < hiveDecimalValues.length; i++) {
      HiveDecimal hiveDecimal = r.vector[i].getHiveDecimal();
      HiveDecimal expectedHiveDecimal = hiveDecimalValues[i];
      if (HiveDecimal.enforcePrecisionScale(expectedHiveDecimal, 5, 2) == null) {
        assertTrue(r.isNull[i]);
      } else {
        assertTrue(!r.isNull[i]);
        if (!hiveDecimal.equals(expectedHiveDecimal)) {
          assertTrue(false);
        }
      }
    }
  }

  /**
   * This batch has output decimal column precision 5 and scale 2.
   * The goal is to allow testing of input long values that, when
   * converted to decimal, will not fit in the given precision.
   * Then it will be possible to check that the results are NULL.
   */
  private VectorizedRowBatch getBatchLongDecimalPrec5Scale2() {
    VectorizedRowBatch b = new VectorizedRowBatch(2);
    LongColumnVector lv;
    b.cols[0] = lv = new LongColumnVector();
    b.cols[1] = new DecimalColumnVector(5, 2);
    lv.vector[0] = 0;
    lv.vector[1] = -1;
    lv.vector[2] = 99999999999999L;
    return b;
  }

  private VectorizedRowBatch getBatchDecimalDecimal() {
    VectorizedRowBatch b = new VectorizedRowBatch(2);

    DecimalColumnVector v0, v1;
    b.cols[0] = v0 = new DecimalColumnVector(18, 4);
    b.cols[1] = v1 = new DecimalColumnVector(5, 2);

    v0.vector[0].set(HiveDecimal.create("10.0001"));
    v0.vector[1].set(HiveDecimal.create("-9999999.9999"));

    v1.vector[0].set(HiveDecimal.create("100.01"));
    v1.vector[1].set(HiveDecimal.create("-200.02"));

    b.size = 2;
    return b;
  }
}
