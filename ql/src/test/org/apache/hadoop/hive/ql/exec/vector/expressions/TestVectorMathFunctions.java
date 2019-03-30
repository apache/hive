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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import org.junit.Assert;

import org.apache.hadoop.hive.serde2.RandomTypeUtil;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncACosDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncASinDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncATanDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncAbsDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncAbsLongToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncCeilDoubleToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncCosDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncDegreesDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncExpDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncFloorDoubleToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncLnDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncLnLongToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncLog10DoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncLog10LongToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncLog2DoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncLog2LongToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncRadiansDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncRoundDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncSignDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncSignLongToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncSinDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncSqrtDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncTanDoubleToDouble;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;


public class TestVectorMathFunctions {

  private static final double eps = 1.0e-7;
  private static boolean equalsWithinTolerance(double a, double b) {
    return Math.abs(a - b) < eps;
  }

  @Test
  public void testVectorRound() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatchDoubleInDoubleOut();
    VectorExpression expr = new FuncRoundDoubleToDouble(0, 1);
    DoubleColumnVector resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    expr.evaluate(b);
    Assert.assertEquals(-2d, resultV.vector[0], Double.MIN_VALUE);
    Assert.assertEquals(-1d, resultV.vector[1], Double.MIN_VALUE);
    Assert.assertEquals(0d, resultV.vector[2], Double.MIN_VALUE);
    Assert.assertEquals(0d, resultV.vector[3], Double.MIN_VALUE);
    Assert.assertEquals(1d, resultV.vector[4], Double.MIN_VALUE);
    Assert.assertEquals(1d, resultV.vector[5], Double.MIN_VALUE);
    Assert.assertEquals(2d, resultV.vector[6], Double.MIN_VALUE);

    // spot check null propagation
    b.cols[0].noNulls = false;
    b.cols[0].isNull[3] = true;
    resultV.noNulls = true;
    expr.evaluate(b);
    Assert.assertEquals(true, resultV.isNull[3]);
    Assert.assertEquals(false, resultV.noNulls);

    // check isRepeating propagation
    b.cols[0].isRepeating = true;
    resultV.isRepeating = false;
    expr.evaluate(b);
    Assert.assertEquals(-2d, resultV.vector[0], Double.MIN_VALUE);
    Assert.assertEquals(true, resultV.isRepeating);

    resultV.isRepeating = false;
    b.cols[0].noNulls = true;
    expr.evaluate(b);
    Assert.assertEquals(-2d, resultV.vector[0], Double.MIN_VALUE);
    Assert.assertEquals(true, resultV.isRepeating);
  }

  @Test
  public void testRoundToDecimalPlaces() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatchDoubleInDoubleOut();
    VectorExpression expr = new RoundWithNumDigitsDoubleToDouble(0, 4, 1);
    expr.evaluate(b);
    DoubleColumnVector resultV = (DoubleColumnVector) b.cols[1];

    // Verify result is rounded to 4 digits
    Assert.assertEquals(1.2346d, resultV.vector[7], Double.MIN_VALUE);
  }

  static int DAYS_LIMIT = 365 * 9999;

  public static VectorizedRowBatch getVectorizedRowBatchDateInTimestampOut(int[] intValues) {
    Random r = new Random(12099);
    VectorizedRowBatch batch = new VectorizedRowBatch(2);
    LongColumnVector inV;
    TimestampColumnVector outV;
    inV = new LongColumnVector();
    outV = new TimestampColumnVector();

    for (int i = 0; i < intValues.length; i++) {
      intValues[i] = r.nextInt() % DAYS_LIMIT;
      inV.vector[i] = intValues[i];
    }

    batch.cols[0] = inV;
    batch.cols[1] = outV;

    batch.size = intValues.length;
    return batch;
  }

  public static VectorizedRowBatch getVectorizedRowBatchDoubleInLongOut() {
    VectorizedRowBatch batch = new VectorizedRowBatch(2);
    LongColumnVector lcv;
    DoubleColumnVector dcv;
    lcv = new LongColumnVector();
    dcv = new DoubleColumnVector();
    dcv.vector[0] = -1.5d;
    dcv.vector[1] = -0.5d;
    dcv.vector[2] = -0.1d;
    dcv.vector[3] = 0d;
    dcv.vector[4] = 0.5d;
    dcv.vector[5] = 0.7d;
    dcv.vector[6] = 1.5d;

    batch.cols[0] = dcv;
    batch.cols[1] = lcv;

    batch.size = 7;
    return batch;
  }

  public static VectorizedRowBatch getVectorizedRowBatchDoubleInTimestampOut() {
    VectorizedRowBatch batch = new VectorizedRowBatch(2);
    TimestampColumnVector tcv;
    DoubleColumnVector dcv;
    tcv = new TimestampColumnVector();
    dcv = new DoubleColumnVector();
    dcv.vector[0] = -1.5d;
    dcv.vector[1] = -0.5d;
    dcv.vector[2] = -0.1d;
    dcv.vector[3] = 0d;
    dcv.vector[4] = 0.5d;
    dcv.vector[5] = 0.7d;
    dcv.vector[6] = 1.5d;

    batch.cols[0] = dcv;
    batch.cols[1] = tcv;

    batch.size = 7;
    return batch;
  }

  public static VectorizedRowBatch getVectorizedRowBatchDoubleInDoubleOut() {
    VectorizedRowBatch batch = new VectorizedRowBatch(2);
    DoubleColumnVector inV;
    DoubleColumnVector outV;
    outV = new DoubleColumnVector();
    inV = new DoubleColumnVector();
    inV.vector[0] = -1.5d;
    inV.vector[1] = -0.5d;
    inV.vector[2] = -0.1d;
    inV.vector[3] = 0d;
    inV.vector[4] = 0.5d;
    inV.vector[5] = 0.7d;
    inV.vector[6] = 1.5d;
    inV.vector[7] = 1.2345678d;

    batch.cols[0] = inV;
    batch.cols[1] = outV;

    batch.size = 8;
    return batch;
  }

  public static VectorizedRowBatch getVectorizedRowBatchLongInDoubleOut() {
    VectorizedRowBatch batch = new VectorizedRowBatch(2);
    LongColumnVector lcv;
    DoubleColumnVector dcv;
    lcv = new LongColumnVector();
    dcv = new DoubleColumnVector();
    lcv.vector[0] = -2;
    lcv.vector[1] = -1;
    lcv.vector[2] = 0;
    lcv.vector[3] = 1;
    lcv.vector[4] = 2;

    batch.cols[0] = lcv;
    batch.cols[1] = dcv;

    batch.size = 5;
    return batch;
  }

  public static VectorizedRowBatch getVectorizedRowBatchTimestampInDoubleOut(double[] doubleValues) {
    Random r = new Random(45993);
    VectorizedRowBatch batch = new VectorizedRowBatch(2);
    TimestampColumnVector tcv;
    DoubleColumnVector dcv;
    tcv = new TimestampColumnVector(doubleValues.length);
    dcv = new DoubleColumnVector(doubleValues.length);
    for (int i = 0; i < doubleValues.length; i++) {
      doubleValues[i] = r.nextDouble() % (double) SECONDS_LIMIT;
      dcv.vector[i] = doubleValues[i];
    }

    batch.cols[0] = tcv;
    batch.cols[1] = dcv;

    batch.size = doubleValues.length;
    return batch;
  }

  public static VectorizedRowBatch getVectorizedRowBatchLongInLongOut() {
    VectorizedRowBatch batch = new VectorizedRowBatch(2);
    LongColumnVector inV, outV;
    inV = new LongColumnVector();
    outV = new LongColumnVector();
    inV.vector[0] = -2;
    inV.vector[1] = 2;

    batch.cols[0] = inV;
    batch.cols[1] = outV;

    batch.size = 2;
    return batch;
  }

  public static VectorizedRowBatch getVectorizedRowBatchStringInLongOut() {
    VectorizedRowBatch batch = new VectorizedRowBatch(2);
    BytesColumnVector inV;
    LongColumnVector outV;
    inV = new BytesColumnVector();
    outV = new LongColumnVector();
    inV.initBuffer();
    inV.setVal(0, StandardCharsets.UTF_8.encode("true").array());
    inV.setVal(1, StandardCharsets.UTF_8.encode("TRUE").array());
    inV.setVal(2, StandardCharsets.UTF_8.encode("TrUe").array());
    inV.setVal(3, StandardCharsets.UTF_8.encode("false").array());
    inV.setVal(4, StandardCharsets.UTF_8.encode("FALSE").array());
    inV.setVal(5, StandardCharsets.UTF_8.encode("FaLsE").array());
    inV.setVal(6, StandardCharsets.UTF_8.encode("").array());
    inV.setVal(7, StandardCharsets.UTF_8.encode("Other").array());

    batch.cols[0] = inV;
    batch.cols[1] = outV;

    batch.size = 8;
    return batch;
  }

  public static VectorizedRowBatch getVectorizedRowBatchTimestampInLongOut(long[] longValues) {
    Random r = new Random(345);
    VectorizedRowBatch batch = new VectorizedRowBatch(2);
    TimestampColumnVector inV;
    LongColumnVector outV;
    inV = new TimestampColumnVector(longValues.length);
    outV = new LongColumnVector(longValues.length);
    for (int i = 0; i < longValues.length; i++) {
      Timestamp randTimestamp = RandomTypeUtil.getRandTimestamp(r);
      longValues[i] = TimestampWritableV2.getLong(randTimestamp);
      inV.set(0, randTimestamp.toSqlTimestamp());
    }

    batch.cols[0] = inV;
    batch.cols[1] = outV;

    batch.size = longValues.length;
    return batch;
  }

  static long SECONDS_LIMIT = 60L * 24L * 365L * 9999L;

  public static VectorizedRowBatch getVectorizedRowBatchLongInTimestampOut(long[] longValues) {
    Random r = new Random(12099);
    VectorizedRowBatch batch = new VectorizedRowBatch(2);
    LongColumnVector inV;
    TimestampColumnVector outV;
    inV = new LongColumnVector();
    outV = new TimestampColumnVector();

    for (int i = 0; i < longValues.length; i++) {
      longValues[i] = r.nextLong() % SECONDS_LIMIT;
      inV.vector[i] = longValues[i];
    }

    batch.cols[0] = inV;
    batch.cols[1] = outV;

    batch.size = longValues.length;
    return batch;
  }

  public static VectorizedRowBatch getBatchForStringMath() {
    VectorizedRowBatch batch = new VectorizedRowBatch(3);
    LongColumnVector inL;
    BytesColumnVector inS, outS;
    inL = new LongColumnVector();
    inS = new BytesColumnVector();
    outS = new BytesColumnVector();
    inL.vector[0] = 0;
    inL.vector[1] = 255;
    inL.vector[2] = 0;
    inS.initBuffer();

    inS.setVal(0, "00".getBytes(StandardCharsets.UTF_8), 0, 2);
    inS.setVal(1, "3232".getBytes(StandardCharsets.UTF_8), 0, 4);
    byte[] bad = "bad data".getBytes(StandardCharsets.UTF_8);
    inS.setVal(2, bad, 0, bad.length);


    batch.cols[0] = inS;
    batch.cols[1] = inL;
    batch.cols[2] = outS;

    batch.size = 3;
    return batch;
  }

  /*
   * The following tests spot-check that vectorized functions with signature
   * DOUBLE func(DOUBLE) that came from template ColumnUnaryFunc.txt
   * get the right result. Null propagation, isRepeating
   * propagation will be checked once for a single expansion of the template
   * (for FuncRoundDoubleToDouble).
   */
  @Test
  public void testVectorSin() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatchDoubleInDoubleOut();
    DoubleColumnVector resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new FuncSinDoubleToDouble(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(Math.sin(0.5d), resultV.vector[4], Double.MIN_VALUE);
  }

  @Test
  public void testVectorCos() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatchDoubleInDoubleOut();
    DoubleColumnVector resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new FuncCosDoubleToDouble(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(Math.cos(0.5d), resultV.vector[4], Double.MIN_VALUE);
  }

  @Test
  public void testVectorTan() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatchDoubleInDoubleOut();
    DoubleColumnVector resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new FuncTanDoubleToDouble(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(Math.tan(0.5d), resultV.vector[4], Double.MIN_VALUE);
  }

  @Test
  public void testVectorASin() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatchDoubleInDoubleOut();
    DoubleColumnVector resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new FuncASinDoubleToDouble(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(Math.asin(0.5d), resultV.vector[4], Double.MIN_VALUE);
  }

  @Test
  public void testVectorACos() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatchDoubleInDoubleOut();
    DoubleColumnVector resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new FuncACosDoubleToDouble(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(Math.acos(0.5d), resultV.vector[4], Double.MIN_VALUE);
  }

  @Test
  public void testVectorATan() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatchDoubleInDoubleOut();
    DoubleColumnVector resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new FuncATanDoubleToDouble(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(Math.atan(0.5d), resultV.vector[4], Double.MIN_VALUE);
  }

  @Test
  public void testVectorDegrees() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatchDoubleInDoubleOut();
    DoubleColumnVector resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new FuncDegreesDoubleToDouble(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(Math.toDegrees(0.5d), resultV.vector[4], Double.MIN_VALUE);
  }

  @Test
  public void testVectorRadians() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatchDoubleInDoubleOut();
    DoubleColumnVector resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new FuncRadiansDoubleToDouble(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(Math.toRadians(0.5d), resultV.vector[4], Double.MIN_VALUE);
  }

  @Test
  public void testVectorFloor() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatchDoubleInLongOut();
    LongColumnVector resultV = (LongColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new FuncFloorDoubleToLong(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(-2, resultV.vector[0]);
    Assert.assertEquals(1, resultV.vector[6]);
  }

  @Test
  public void testVectorCeil() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatchDoubleInLongOut();
    LongColumnVector resultV = (LongColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new FuncCeilDoubleToLong(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(-1, resultV.vector[0]);
    Assert.assertEquals(2, resultV.vector[6]);
  }

  @Test
  public void testVectorExp() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatchDoubleInDoubleOut();
    DoubleColumnVector resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new FuncExpDoubleToDouble(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(Math.exp(0.5d), resultV.vector[4], Double.MIN_VALUE);
  }

  @Test
  public void testVectorLn() throws HiveException {

    // test double->double version
    VectorizedRowBatch b = getVectorizedRowBatchDoubleInDoubleOut();
    DoubleColumnVector resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new FuncLnDoubleToDouble(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(Math.log(0.5), resultV.vector[4], Double.MIN_VALUE);

    // test long->double version
    b = getVectorizedRowBatchLongInDoubleOut();
    resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    expr = new FuncLnLongToDouble(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(Math.log(2), resultV.vector[4], Double.MIN_VALUE);
  }

  @Test
  public void testVectorLog2() throws HiveException {

    // test double->double version
    VectorizedRowBatch b = getVectorizedRowBatchDoubleInDoubleOut();
    DoubleColumnVector resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new FuncLog2DoubleToDouble(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(Math.log(0.5d) / Math.log(2), resultV.vector[4], Double.MIN_VALUE);

    // test long->double version
    b = getVectorizedRowBatchLongInDoubleOut();
    resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    expr = new FuncLog2LongToDouble(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(Math.log(1) / Math.log(2), resultV.vector[3], Double.MIN_VALUE);
  }

  @Test
  public void testVectorLog10() throws HiveException {

    // test double->double version
    VectorizedRowBatch b = getVectorizedRowBatchDoubleInDoubleOut();
    DoubleColumnVector resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new FuncLog10DoubleToDouble(0, 1);
    expr.evaluate(b);
    Assert.assertTrue(equalsWithinTolerance(Math.log(0.5d) / Math.log(10), resultV.vector[4]));

    // test long->double version
    b = getVectorizedRowBatchLongInDoubleOut();
    resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    expr = new FuncLog10LongToDouble(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(Math.log(1) / Math.log(10), resultV.vector[3], Double.MIN_VALUE);
  }

  @Test
  public void testVectorRand() throws HiveException {
    VectorizedRowBatch b = new VectorizedRowBatch(1);
    DoubleColumnVector v = new DoubleColumnVector();
    b.cols[0] = v;
    b.size = VectorizedRowBatch.DEFAULT_SIZE;
    int n = b.size;
    v.noNulls = true;
    VectorExpression expr = new FuncRandNoSeed(0);
    expr.evaluate(b);
    double sum = 0;
    for(int i = 0; i != n; i++) {
      sum += v.vector[i];
      Assert.assertTrue(v.vector[i] >= 0.0 && v.vector[i] <= 1.0);
    }
    double avg = sum / n;

    /* The random values must be between 0 and 1, distributed uniformly.
     * So the average value of a large set should be about 0.5. Verify it is
     * close to this value.
     */
    Assert.assertTrue(avg > 0.3 && avg < 0.7);

    // Now, test again with a seed.
    Arrays.fill(v.vector, 0);
    expr = new FuncRand(99999, 0);
    expr.evaluate(b);
    sum = 0;
    for(int i = 0; i != n; i++) {
      sum += v.vector[i];
      Assert.assertTrue(v.vector[i] >= 0.0 && v.vector[i] <= 1.0);
    }
    avg = sum / n;
    Assert.assertTrue(avg > 0.3 && avg < 0.7);
  }

  @Test
  public void testVectorLogBase() throws HiveException {

    // test double->double version
    VectorizedRowBatch b = getVectorizedRowBatchDoubleInDoubleOut();
    DoubleColumnVector resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new FuncLogWithBaseDoubleToDouble(10.0, 0, 1);
    expr.evaluate(b);
    Assert.assertTrue(equalsWithinTolerance(Math.log(0.5d) / Math.log(10), resultV.vector[4]));
  }

  @Test
  public void testVectorPosMod() throws HiveException {

    // test double->double version
    VectorizedRowBatch b = getVectorizedRowBatchDoubleInDoubleOut();
    DoubleColumnVector inV = (DoubleColumnVector) b.cols[0];
    DoubleColumnVector resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    inV.vector[4] = -4.0;
    VectorExpression expr = new PosModDoubleToDouble(0, 0.3d, 1);
    expr.setOutputTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo("double"));
    expr.evaluate(b);
    Assert.assertTrue(equalsWithinTolerance(((-4.0d % 0.3d) + 0.3d) % 0.3d, resultV.vector[4]));

    // test long->long version
    b = getVectorizedRowBatchLongInLongOut();
    LongColumnVector resV2 = (LongColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    expr = new PosModLongToLong(0, 3, 1);
    expr.setOutputTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo("tinyint"));
    //((ISetLongArg) expr).setArg(3);
    expr.evaluate(b);
    Assert.assertEquals(((-2 % 3) + 3) % 3, resV2.vector[0]);
    //use smallint as outputTypeInfo
    expr = new PosModLongToLong(0, 3, 1);
    expr.setOutputTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo("smallint"));
    //((ISetLongArg) expr).setArg(3);
    expr.evaluate(b);
    Assert.assertEquals(((-2 % 3) + 3) % 3, resV2.vector[0]);
    //use int as outputTypeInfo
    expr = new PosModLongToLong(0, 3, 1);
    expr.setOutputTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo("int"));
    //((ISetLongArg) expr).setArg(3);
    expr.evaluate(b);
    Assert.assertEquals(((-2 % 3) + 3) % 3, resV2.vector[0]);
    //use bigint
    expr = new PosModLongToLong(0, 3, 1);
    expr.setOutputTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo("bigint"));
    //((ISetLongArg) expr).setArg(3);
    expr.evaluate(b);
    Assert.assertEquals(((-2 % 3) + 3) % 3, resV2.vector[0]);
  }

  @Test
  public void testVectorPosModWithFloatOutputType() throws HiveException {

    // test double->double version
    VectorizedRowBatch b = getVectorizedRowBatchDoubleInDoubleOut();
    DoubleColumnVector inV = (DoubleColumnVector) b.cols[0];
    DoubleColumnVector resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    inV.vector[4] = -4.0;
    VectorExpression expr = new PosModDoubleToDouble(0, 0.3d, 1);
    expr.setOutputTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo("float"));
    expr.evaluate(b);
    Assert.assertTrue(equalsWithinTolerance(((-4.0f % 0.3f) + 0.3f) % 0.3f, resultV.vector[4]));

    // test long->long version
    b = getVectorizedRowBatchLongInLongOut();
    LongColumnVector resV2 = (LongColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    expr = new PosModLongToLong(0, 3, 1);
    //((ISetLongArg) expr).setArg(3);
    expr.evaluate(b);
    Assert.assertEquals(((-2 % 3) + 3) % 3, resV2.vector[0]);
  }

  @Test
  public void testVectorPower() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatchDoubleInDoubleOut();
    DoubleColumnVector resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new FuncPowerDoubleToDouble(0, 2.0, 1);
    expr.evaluate(b);
    Assert.assertTrue(equalsWithinTolerance(0.5d * 0.5d, resultV.vector[4]));
  }

  @Test
  public void testVectorSqrt() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatchDoubleInDoubleOut();
    DoubleColumnVector resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new FuncSqrtDoubleToDouble(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(Math.sqrt(0.5d), resultV.vector[4], Double.MIN_VALUE);
  }

  @Test
  public void testVectorAbs() throws HiveException {

    // test double->double version
    VectorizedRowBatch b = getVectorizedRowBatchDoubleInDoubleOut();
    DoubleColumnVector resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new FuncAbsDoubleToDouble(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(1.5, resultV.vector[0], Double.MIN_VALUE);
    Assert.assertEquals(0.5, resultV.vector[4], Double.MIN_VALUE);

    // test long->long version
    b = getVectorizedRowBatchLongInLongOut();
    LongColumnVector resultVLong = (LongColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    expr = new FuncAbsLongToLong(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(2, resultVLong.vector[0]);
    Assert.assertEquals(2, resultVLong.vector[1]);
  }

  @Test
  public void testVectorSign() throws HiveException {

    // test double->double version
    VectorizedRowBatch b = getVectorizedRowBatchDoubleInDoubleOut();
    DoubleColumnVector resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    VectorExpression expr = new FuncSignDoubleToDouble(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(-1.0d, resultV.vector[0], Double.MIN_VALUE);
    Assert.assertEquals(1.0d, resultV.vector[4], Double.MIN_VALUE);

    // test long->double version
    b = getVectorizedRowBatchLongInDoubleOut();
    resultV = (DoubleColumnVector) b.cols[1];
    b.cols[0].noNulls = true;
    expr = new FuncSignLongToDouble(0, 1);
    expr.evaluate(b);
    Assert.assertEquals(-1.0d, resultV.vector[0], Double.MIN_VALUE);
    Assert.assertEquals(1.0d, resultV.vector[4], Double.MIN_VALUE);
  }

  @Test
  public void testVectorBin() throws HiveException {

    // test conversion of long->string
    VectorizedRowBatch b = getBatchForStringMath();
    BytesColumnVector resultV = (BytesColumnVector) b.cols[2];
    b.cols[0].noNulls = true;
    VectorExpression expr = new FuncBin(1, 2);
    expr.transientInit();
    expr.evaluate(b);
    String s = new String(resultV.vector[1], resultV.start[1], resultV.length[1]);
    Assert.assertEquals("11111111", s);
  }

  @Test
  public void testVectorHex() throws HiveException {

    // test long->string version
    VectorizedRowBatch b = getBatchForStringMath();
    BytesColumnVector resultV = (BytesColumnVector) b.cols[2];
    b.cols[1].noNulls = true;
    VectorExpression expr = new FuncHex(1, 2);
    expr.transientInit();
    expr.evaluate(b);
    String s = new String(resultV.vector[1], resultV.start[1], resultV.length[1]);
    Assert.assertEquals("FF", s);

    // test string->string version
    b = getBatchForStringMath();
    resultV = (BytesColumnVector) b.cols[2];
    b.cols[0].noNulls = true;
    expr = new StringHex(0, 2);
    expr.evaluate(b);
    s = new String(resultV.vector[1], resultV.start[1], resultV.length[1]);
    Assert.assertEquals("33323332", s);
  }
}
