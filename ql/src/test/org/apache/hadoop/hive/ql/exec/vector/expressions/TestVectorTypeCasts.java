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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import junit.framework.Assert;

import org.apache.hadoop.hive.common.type.Decimal128;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.*;
import org.apache.hadoop.hive.ql.exec.vector.expressions.*;
import org.junit.Test;

/**
 * Test VectorExpression classes for vectorized implementations of type casts.
 */
public class TestVectorTypeCasts {

  // Number of nanoseconds in one second
  private static final long NANOS_PER_SECOND = 1000000000;

  // Number of microseconds in one second
  private static final long MICROS_PER_SECOND = 1000000;

  @Test
  public void testVectorCastLongToDouble() {
    VectorizedRowBatch b = TestVectorMathFunctions.getVectorizedRowBatchLongInDoubleOut();
    DoubleColumnVector resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new CastLongToDouble(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(2.0, resultV.vector[4]);
  }

  @Test
  public void testVectorCastDoubleToLong() {
    VectorizedRowBatch b = TestVectorMathFunctions.getVectorizedRowBatchDoubleInLongOut();
    LongColumnVector resultV = (LongColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new CastDoubleToLong(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(1, resultV.vector[6]);
  }

  @Test
  public void testCastDoubleToBoolean() {
    VectorizedRowBatch b = TestVectorMathFunctions.getVectorizedRowBatchDoubleInLongOut();
    LongColumnVector resultV = (LongColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new CastDoubleToBooleanViaDoubleToLong(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(0, resultV.vector[3]);
    Assert.assertEquals(1, resultV.vector[4]);
  }

  @Test
  public void testCastDoubleToTimestamp() {
    VectorizedRowBatch b = TestVectorMathFunctions.getVectorizedRowBatchDoubleInLongOut();
    LongColumnVector resultV = (LongColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new CastDoubleToTimestampViaDoubleToLong(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(0, resultV.vector[3]);
    Assert.assertEquals((long) (0.5d * NANOS_PER_SECOND), resultV.vector[4]);
  }

  @Test
  public void testCastLongToBoolean() {
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
  public void testCastLongToTimestamp() {
    VectorizedRowBatch b = TestVectorMathFunctions.getVectorizedRowBatchLongInLongOut();
    LongColumnVector resultV = (LongColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new CastLongToTimestampViaLongToLong(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(-2 * MICROS_PER_SECOND, resultV.vector[0]);
    Assert.assertEquals(2 * MICROS_PER_SECOND, resultV.vector[1]);
  }

  @Test
  public void testCastTimestampToLong() {
    VectorizedRowBatch b = TestVectorMathFunctions.getVectorizedRowBatchLongInLongOut();
    LongColumnVector inV = (LongColumnVector) b.cols[0];
    inV.vector[0] = NANOS_PER_SECOND;  // Make one entry produce interesting result
      // (1 sec after epoch).

    LongColumnVector resultV = (LongColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new CastTimestampToLongViaLongToLong(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(1, resultV.vector[0]);
  }

  @Test
  public void testCastTimestampToDouble() {
    VectorizedRowBatch b = TestVectorMathFunctions.getVectorizedRowBatchLongInDoubleOut();
    LongColumnVector inV = (LongColumnVector) b.cols[0];
    DoubleColumnVector resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new CastTimestampToDoubleViaLongToDouble(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(-1E-9D , resultV.vector[1]);
    Assert.assertEquals(1E-9D, resultV.vector[3]);
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
  public void testCastLongToString() {
    VectorizedRowBatch b = TestVectorMathFunctions.getBatchForStringMath();
    BytesColumnVector resultV = (BytesColumnVector) b.cols[2];
    b.cols[1].noNulls = true;
    VectorExpression expr = new CastLongToString(1, 2);
    expr.evaluate(b);
    byte[] num255 = toBytes("255");
    Assert.assertEquals(0,
        StringExpr.compare(num255, 0, num255.length,
            resultV.vector[1], resultV.start[1], resultV.length[1]));
  }

  @Test
  public void testCastBooleanToString() {
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
  public void testCastDecimalToLong() {

    // test basic case
    VectorizedRowBatch b = getBatchDecimalLong();
    VectorExpression expr = new CastDecimalToLong(0, 1);
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
  /* Just spot check the basic case because code path is the same as
   * for cast of decimal to long due to inheritance.
   */
  public void testCastDecimalToBoolean() {
    VectorizedRowBatch b = getBatchDecimalLong();
    VectorExpression expr = new CastDecimalToBoolean(0, 1);
    DecimalColumnVector in = (DecimalColumnVector) b.cols[0];
    in.vector[1].update(0);
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

    dv.vector[0].update("1.1", scale);
    dv.vector[1].update("-2.2", scale);
    dv.vector[2].update("9999999999999999.00", scale);

    return b;
  }

  @Test
  public void testCastDecimalToDouble() {

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

    dv.vector[0].update("1.1", scale);
    dv.vector[1].update("-2.2", scale);
    dv.vector[2].update("9999999999999999.00", scale);

    return b;
  }

  @Test
  public void testCastDecimalToString() {
    VectorizedRowBatch b = getBatchDecimalString();
    VectorExpression expr = new CastDecimalToString(0, 1);
    expr.evaluate(b);
    BytesColumnVector r = (BytesColumnVector) b.cols[1];

    byte[] v = toBytes("1.1");
    Assert.assertEquals(0,
        StringExpr.compare(v, 0, v.length,
            r.vector[0], r.start[0], r.length[0]));

    v = toBytes("-2.2");
    Assert.assertEquals(0,
        StringExpr.compare(v, 0, v.length,
            r.vector[1], r.start[1], r.length[1]));

    v = toBytes("9999999999999999");
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

    dv.vector[0].update("1.1", scale);
    dv.vector[1].update("-2.2", scale);
    dv.vector[2].update("9999999999999999.00", scale);

    return b;
  }

  @Test
  public void testCastDecimalToTimestamp() {
    VectorizedRowBatch b = getBatchDecimalLong2();
    VectorExpression expr = new CastDecimalToTimestamp(0, 1);
    expr.evaluate(b);
    LongColumnVector r = (LongColumnVector) b.cols[1];
    assertEquals(1111111111L, r.vector[0]);
    assertEquals(-2222222222L, r.vector[1]);
    assertEquals(31536000999999999L, r.vector[2]);
  }

  private VectorizedRowBatch getBatchDecimalLong2() {
    VectorizedRowBatch b = new VectorizedRowBatch(2);
    DecimalColumnVector dv;
    short scale = 9;
    b.cols[0] = dv = new DecimalColumnVector(18, scale);
    b.cols[1] = new LongColumnVector();

    b.size = 3;

    dv.vector[0].update("1.111111111", scale);
    dv.vector[1].update("-2.222222222", scale);
    dv.vector[2].update("31536000.999999999", scale);

    return b;
  }

  @Test
  public void testCastLongToDecimal() {
    VectorizedRowBatch b = getBatchLongDecimal();
    VectorExpression expr = new CastLongToDecimal(0, 1);
    expr.evaluate(b);
    DecimalColumnVector r = (DecimalColumnVector) b.cols[1];
    assertTrue(r.vector[0].equals(new Decimal128(0, (short) 2)));
    assertTrue(r.vector[1].equals(new Decimal128(-1, (short) 2)));
    assertTrue(r.vector[2].equals(new Decimal128(99999999999999L, (short) 2)));
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

  @Test
  public void testCastDoubleToDecimal() {
    VectorizedRowBatch b = getBatchDoubleDecimal();
    VectorExpression expr = new CastDoubleToDecimal(0, 1);
    expr.evaluate(b);
    DecimalColumnVector r = (DecimalColumnVector) b.cols[1];

    assertTrue(r.vector[0].equals(new Decimal128(0, r.scale)));
    assertTrue(r.vector[1].equals(new Decimal128(-1, r.scale)));
    assertTrue(r.vector[2].equals(new Decimal128("99999999999999.0", r.scale)));
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
  public void testCastStringToDecimal() {
    VectorizedRowBatch b = getBatchStringDecimal();
    VectorExpression expr = new CastStringToDecimal(0, 1);
    expr.evaluate(b);
    DecimalColumnVector r = (DecimalColumnVector) b.cols[1];
    assertTrue(r.vector[0].equals(new Decimal128("1.10", r.scale)));
    assertTrue(r.vector[1].equals(new Decimal128("-2.20", r.scale)));
    assertTrue(r.vector[2].equals(new Decimal128("99999999999999.0", r.scale)));
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
  public void testCastTimestampToDecimal() {

    // The input timestamps are stored as long values
    // measured in nanoseconds from the epoch.
    VectorizedRowBatch b = getBatchLongDecimal();
    VectorExpression expr = new CastTimestampToDecimal(0, 1);
    LongColumnVector inL = (LongColumnVector) b.cols[0];
    inL.vector[1] = -1990000000L;
    expr.evaluate(b);
    DecimalColumnVector r = (DecimalColumnVector) b.cols[1];
    assertTrue(r.vector[0].equals(new Decimal128(0, (short) 2)));
    assertTrue(r.vector[1].equals(new Decimal128("-1.99", (short) 2)));
    assertTrue(r.vector[2].equals(new Decimal128("100000.00", (short) 2)));

    // Try again with a value that won't fit in 5 digits, to make
    // sure that NULL is produced.
    b = getBatchLongDecimalPrec5Scale2();
    expr.evaluate(b);
    r = (DecimalColumnVector) b.cols[1];
    assertFalse(r.noNulls);
    assertFalse(r.isNull[0]);
    assertFalse(r.isNull[1]);
    assertTrue(r.isNull[2]);
  }

  /* This batch has output decimal column precision 5 and scale 2.
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

  @Test
  public void testCastDecimalToDecimal() {

    // test casting from one precision and scale to another.
    VectorizedRowBatch b = getBatchDecimalDecimal();
    VectorExpression expr = new CastDecimalToDecimal(0, 1);
    expr.evaluate(b);
    DecimalColumnVector r = (DecimalColumnVector) b.cols[1];
    assertTrue(r.vector[0].equals(new Decimal128("10.00", (short) 2)));
    assertFalse(r.noNulls);
    assertTrue(r.isNull[1]);

    // test an increase in precision/scale
    b = getBatchDecimalDecimal();
    expr = new CastDecimalToDecimal(1, 0);
    expr.evaluate(b);
    r = (DecimalColumnVector) b.cols[0];
    assertTrue(r.vector[0].equals(new Decimal128("100.01", (short) 4)));
    assertTrue(r.vector[1].equals(new Decimal128("-200.02", (short) 4)));
    assertTrue(r.noNulls);
  }

  private VectorizedRowBatch getBatchDecimalDecimal() {
    VectorizedRowBatch b = new VectorizedRowBatch(2);

    DecimalColumnVector v0, v1;
    b.cols[0] = v0 = new DecimalColumnVector(18, 4);
    b.cols[1] = v1 = new DecimalColumnVector(5, 2);

    v0.vector[0].update(new Decimal128("10.0001", (short) 4));
    v0.vector[1].update(new Decimal128("-9999999.9999", (short) 4));

    v1.vector[0].update(new Decimal128("100.01", (short) 2));
    v1.vector[1].update(new Decimal128("-200.02", (short) 2));

    b.size = 2;
    return b;
  }
}
