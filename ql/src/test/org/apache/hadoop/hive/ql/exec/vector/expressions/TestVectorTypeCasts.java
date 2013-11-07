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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
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
}
