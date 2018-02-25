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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import junit.framework.Assert;

import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TestVectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongScalarModuloLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongScalarSubtractLongColumn;
import org.junit.Test;

/**
 * Test vectorized expression handling for the case where there is a scalar on
 * the left and a column vector on the right.
 */
public class TestVectorScalarColArithmetic {

  /* Testing for equality of doubles after a math operation is
   * not always reliable so use this as a tolerance.
   */
  private final double EPS = 1e-7d;

  private VectorizedRowBatch getVectorizedRowBatchSingleLongVector(int size) {
    VectorizedRowBatch batch = new VectorizedRowBatch(2, size);
    LongColumnVector lcv = new LongColumnVector(size);
    for (int i = 0; i < size; i++) {
      lcv.vector[i] = i * 37;
    }
    batch.cols[0] = lcv;
    batch.cols[1] = new LongColumnVector(size);
    batch.size = size;
    return batch;
  }

  private VectorizedRowBatch getBatchSingleLongVectorPositiveNonZero() {
    VectorizedRowBatch batch = new VectorizedRowBatch(2);
    final int size = VectorizedRowBatch.DEFAULT_SIZE;
    LongColumnVector lcv = new LongColumnVector();
    for (int i = 0; i < size; i++) {
      lcv.vector[i] = (i + 1) * 37;
    }
    batch.cols[0] = lcv;
    batch.cols[1] = new LongColumnVector();
    batch.size = size;
    return batch;
  }

  @Test
  public void testLongScalarModuloLongColNoNulls()  {
    VectorizedRowBatch batch = getBatchSingleLongVectorPositiveNonZero();
    LongScalarModuloLongColumn expr = new LongScalarModuloLongColumn(100, 0, 1);
    expr.evaluate(batch);

    // verify
    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      Assert.assertEquals(100 % ((i + 1) * 37), ((LongColumnVector) batch.cols[1]).vector[i]);
    }
    Assert.assertTrue(((LongColumnVector)batch.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector)batch.cols[1]).isRepeating);
  }

  @Test
  public void testLongScalarSubtractLongColNoNulls()  {
    VectorizedRowBatch batch = getVectorizedRowBatchSingleLongVector(
        VectorizedRowBatch.DEFAULT_SIZE);
    LongScalarSubtractLongColumn expr = new LongScalarSubtractLongColumn(100, 0, 1);
    expr.evaluate(batch);

    //verify
    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      Assert.assertEquals(100 - i * 37, ((LongColumnVector) batch.cols[1]).vector[i]);
    }
    Assert.assertTrue(((LongColumnVector)batch.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector)batch.cols[1]).isRepeating);
  }

  @Test
  public void testLongScalarSubtractLongColWithNulls()  {
    VectorizedRowBatch batch = getVectorizedRowBatchSingleLongVector(
        VectorizedRowBatch.DEFAULT_SIZE);
    LongColumnVector lcv = (LongColumnVector) batch.cols[0];
    TestVectorizedRowBatch.addRandomNulls(lcv);
    LongScalarSubtractLongColumn expr = new LongScalarSubtractLongColumn(100, 0, 1);
    expr.evaluate(batch);

    //verify
    for (int i=0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      if (!lcv.isNull[i]) {
        Assert.assertEquals(100 - i * 37, ((LongColumnVector)batch.cols[1]).vector[i]);
      } else {
        Assert.assertTrue(((LongColumnVector)batch.cols[1]).isNull[i]);
      }
    }
    Assert.assertFalse(((LongColumnVector)batch.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector)batch.cols[1]).isRepeating);
    TestVectorArithmeticExpressions.verifyLongNullDataVectorEntries(
        (LongColumnVector) batch.cols[1], batch.selected, batch.selectedInUse, batch.size);
  }

  @Test
  public void testLongScalarSubtractLongColWithRepeating() {
    LongColumnVector in, out;
    VectorizedRowBatch batch;
    LongScalarSubtractLongColumn expr;

    // Case 1: is repeating, no nulls
    batch = getVectorizedRowBatchSingleLongVector(
        VectorizedRowBatch.DEFAULT_SIZE);
    in = (LongColumnVector) batch.cols[0];
    in.isRepeating = true;
    out = (LongColumnVector) batch.cols[1];
    out.isRepeating = false;
    expr = new LongScalarSubtractLongColumn(100, 0, 1);
    expr.evaluate(batch);

    // verify
    Assert.assertTrue(out.isRepeating);
    Assert.assertTrue(out.noNulls);
    Assert.assertEquals(out.vector[0], 100 - 0 * 37);

    // Case 2: is repeating, has nulls
    batch = getVectorizedRowBatchSingleLongVector(
        VectorizedRowBatch.DEFAULT_SIZE);
    in = (LongColumnVector) batch.cols[0];
    in.isRepeating = true;
    in.noNulls = false;
    in.isNull[0] = true;

    out = (LongColumnVector) batch.cols[1];
    out.isRepeating = false;
    out.isNull[0] = false;
    out.noNulls = true;
    expr = new LongScalarSubtractLongColumn(100, 0, 1);
    expr.evaluate(batch);

    // verify
    Assert.assertTrue(out.isRepeating);
    Assert.assertFalse(out.noNulls);
    Assert.assertEquals(true, out.isNull[0]);
    TestVectorArithmeticExpressions.verifyLongNullDataVectorEntries(
        out, batch.selected, batch.selectedInUse, batch.size);
  }

  private boolean equalsWithinTolerance(double a, double b) {
    return Math.abs(a - b) < EPS;
  }

  @Test
  public void testLongScalarDivide() {
    VectorizedRowBatch batch =
        TestVectorArithmeticExpressions.getVectorizedRowBatch2LongInDoubleOut();
    LongColDivideLongScalar expr = new LongColDivideLongScalar(0, 100, 2);
    batch.cols[0].isNull[0] = true;
    batch.cols[0].noNulls = false;
    DoubleColumnVector out = (DoubleColumnVector) batch.cols[2];
    out.noNulls = true;     // set now so we can verify it changed
    out.isRepeating = true;
    expr.evaluate(batch);

    // verify NULL output in entry 0 is correct
    assertFalse(out.noNulls);
    assertTrue(out.isNull[0]);
    assertTrue(Double.isNaN(out.vector[0]));

    // check entries beyond first one
    for (int i = 1; i != batch.size; i++) {
      assertTrue(equalsWithinTolerance((i * 37) / 100d, out.vector[i]));
    }
    assertFalse(out.isRepeating);
  }

  @Test
  public void testScalarLongDivide() {
    VectorizedRowBatch batch =
        TestVectorArithmeticExpressions.getVectorizedRowBatch2LongInDoubleOut();
    LongScalarDivideLongColumn expr = new LongScalarDivideLongColumn(100, 0, 2);
    batch.cols[0].isNull[1] = true;
    batch.cols[0].noNulls = false;
    DoubleColumnVector out = (DoubleColumnVector) batch.cols[2];
    out.noNulls = true;     // set now so we can verify it changed
    out.isRepeating = true;
    expr.evaluate(batch);

    // verify zero-divide result for position 0
    assertTrue(out.isNull[0]);
    assertTrue(Double.isNaN(out.vector[0]));

    // verify NULL output in entry 1 is correct
    assertTrue(out.isNull[1]);
    assertTrue(Double.isNaN(out.vector[1]));

    // check entries beyond 2nd one
    for (int i = 2; i != batch.size; i++) {
      assertTrue(equalsWithinTolerance(100d / (i * 37), out.vector[i]));
    }
    assertFalse(out.noNulls);
    assertFalse(out.isRepeating);
  }

  @Test
  public void testBooleanValuedLongIn() {
    VectorizedRowBatch batch = getBatch();
    long[] a = new long[2];
    a[0] = 20;
    a[1] = 1000;
    batch.size = 2;
    VectorExpression expr = (new LongColumnInList(0, 1));
    ((LongColumnInList) expr).setInListValues(a);
    expr.evaluate(batch);
    LongColumnVector out = (LongColumnVector) batch.cols[1];
    Assert.assertEquals(0, out.vector[0]);
    Assert.assertEquals(1, out.vector[1]);
  }

  private VectorizedRowBatch getBatch() {
    VectorizedRowBatch b = new VectorizedRowBatch(2);
    LongColumnVector v = new LongColumnVector();
    v.vector[0] = 10;
    v.vector[1] = 20;
    b.cols[0] = v;
    b.cols[1] = new LongColumnVector();
    return b;
  }
}
