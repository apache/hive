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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.junit.Assert;
import org.junit.Test;

/**
* Unit tests for logical expressions AND, OR, NOT, IsNull etc.
*/
public class TestVectorLogicalExpressions {

  private static final int BOOLEAN_COLUMN_TEST_SIZE = 9;

  @Test
  public void testLongColOrLongCol() {
    VectorizedRowBatch batch = getBatchThreeBooleanCols();
    ColOrCol expr = new ColOrCol(0, 1, 2);
    LongColumnVector outCol = (LongColumnVector) batch.cols[2];
    expr.evaluate(batch);
    // verify
    Assert.assertEquals(0, outCol.vector[0]);
    Assert.assertEquals(1, outCol.vector[1]);
    Assert.assertEquals(1, outCol.vector[2]);
    Assert.assertEquals(1, outCol.vector[3]);
    Assert.assertFalse(outCol.isNull[3]);
    Assert.assertTrue(outCol.isNull[4]);
    Assert.assertEquals(1, outCol.vector[5]);
    Assert.assertTrue(outCol.isNull[6]);
    Assert.assertEquals(1, outCol.vector[7]);
    Assert.assertTrue(outCol.isNull[8]);

    Assert.assertEquals(batch.size, 9);
    Assert.assertFalse(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);

    // try non-null path
    batch = getBatchThreeBooleanCols();
    batch.cols[0].noNulls = true;
    batch.cols[1].noNulls = true;
    batch.cols[2].noNulls = false;
    outCol = (LongColumnVector) batch.cols[2];
    expr.evaluate(batch);

    // spot check
    Assert.assertTrue(outCol.noNulls);
    Assert.assertEquals(0, outCol.vector[0]);
    Assert.assertEquals(1, outCol.vector[1]);
    Assert.assertEquals(1, outCol.vector[2]);
    Assert.assertEquals(1, outCol.vector[3]);

    // try isRepeating path (left input only), no nulls
    batch = getBatchThreeBooleanCols();
    batch.cols[0].noNulls = true;
    batch.cols[0].isRepeating = true;
    batch.cols[1].noNulls = true;
    batch.cols[1].isRepeating = false;
    batch.cols[2].noNulls = false;
    batch.cols[2].isRepeating = true;
    outCol = (LongColumnVector) batch.cols[2];
    expr.evaluate(batch);

    // spot check
    Assert.assertFalse(outCol.isRepeating);
    Assert.assertEquals(0, outCol.vector[0]);
    Assert.assertEquals(1, outCol.vector[1]);
    Assert.assertEquals(0, outCol.vector[2]);
    Assert.assertEquals(1, outCol.vector[3]);
  }

  /**
   * Get a batch with three boolean (long) columns.
   */
  private VectorizedRowBatch getBatchThreeBooleanCols() {
    VectorizedRowBatch batch = new VectorizedRowBatch(3, VectorizedRowBatch.DEFAULT_SIZE);
    LongColumnVector v0, v1, v2;
    v0 = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    v1 = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    v2 = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    batch.cols[0] = v0;
    batch.cols[1] = v1;
    batch.cols[2] = v2;

    // add some data and nulls
    int i;
    i = 0; v0.vector[i] = 0; v0.isNull[i] = false; v1.vector[i] = 0; v1.isNull[i] = false;  // 0 0
    i = 1; v0.vector[i] = 0; v0.isNull[i] = false; v1.vector[i] = 1; v1.isNull[i] = false;  // 0 1
    i = 2; v0.vector[i] = 1; v0.isNull[i] = false; v1.vector[i] = 0; v1.isNull[i] = false;  // 1 0
    i = 3; v0.vector[i] = 1; v0.isNull[i] = false; v1.vector[i] = 1; v1.isNull[i] = false;  // 1 1
    i = 4; v0.vector[i] = 0; v0.isNull[i] = true; v1.vector[i] = 0; v1.isNull[i] = false;  // NULL 0
    i = 5; v0.vector[i] = 0; v0.isNull[i] = true; v1.vector[i] = 1; v1.isNull[i] = false;  // NULL 1
    i = 6; v0.vector[i] = 0; v0.isNull[i] = false; v1.vector[i] = 0; v1.isNull[i] = true;  // 0 NULL
    i = 7; v0.vector[i] = 1; v0.isNull[i] = false; v1.vector[i] = 1; v1.isNull[i] = true;  // 1 NULL
    i = 8; v0.vector[i] = 1; v0.isNull[i] = true; v1.vector[i] = 1; v1.isNull[i] = true; // NULL NULL

    v0.noNulls = false;
    v1.noNulls = false;
    v0.isRepeating = false;
    v1.isRepeating = false;

    v2.isRepeating = true; // this value should get over-written with correct value
    v2.noNulls = true; // ditto

    batch.size = BOOLEAN_COLUMN_TEST_SIZE;
    return batch;
  }

  @Test
  public void testBooleanNot() {
    VectorizedRowBatch batch = getBatchThreeBooleanCols();
    NotCol expr = new NotCol(0, 2);
    LongColumnVector outCol = (LongColumnVector) batch.cols[2];
    expr.evaluate(batch);

    // Case with nulls
    Assert.assertFalse(outCol.isRepeating);
    Assert.assertEquals(1, outCol.vector[0]);
    Assert.assertFalse(outCol.isNull[0]);
    Assert.assertEquals(0, outCol.vector[2]);
    Assert.assertFalse(outCol.isNull[0]);
    Assert.assertTrue(outCol.isNull[4]);

    // No nulls case
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    Assert.assertFalse(outCol.isRepeating);
    Assert.assertTrue(outCol.noNulls);
    Assert.assertEquals(1, outCol.vector[0]);
    Assert.assertEquals(0, outCol.vector[2]);

    // isRepeating, and there are nulls
    batch = getBatchThreeBooleanCols();
    outCol = (LongColumnVector) batch.cols[2];
    batch.cols[0].isRepeating = true;
    batch.cols[0].isNull[0] = true;
    expr.evaluate(batch);
    Assert.assertTrue(outCol.isRepeating);
    Assert.assertTrue(outCol.isNull[0]);

    // isRepeating, and no nulls
    batch = getBatchThreeBooleanCols();
    outCol = (LongColumnVector) batch.cols[2];
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    Assert.assertTrue(outCol.isRepeating);
    Assert.assertTrue(outCol.noNulls);
    Assert.assertEquals(1, outCol.vector[0]);
  }

  @Test
  public void testIsNullExpr() {
    // has nulls, not repeating
    VectorizedRowBatch batch = getBatchThreeBooleanCols();
    IsNull expr = new IsNull(0, 2);
    LongColumnVector outCol = (LongColumnVector) batch.cols[2];
    expr.evaluate(batch);
    Assert.assertEquals(0, outCol.vector[0]);
    Assert.assertEquals(1, outCol.vector[4]);
    Assert.assertTrue(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);

    // No nulls case, not repeating
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    Assert.assertTrue(outCol.isRepeating);
    Assert.assertTrue(outCol.noNulls);
    Assert.assertEquals(0, outCol.vector[0]);

    // isRepeating, and there are nulls
    batch = getBatchThreeBooleanCols();
    outCol = (LongColumnVector) batch.cols[2];
    batch.cols[0].isRepeating = true;
    batch.cols[0].isNull[0] = true;
    expr.evaluate(batch);
    Assert.assertTrue(outCol.isRepeating);
    Assert.assertEquals(1, outCol.vector[0]);
    Assert.assertTrue(outCol.noNulls);

    // isRepeating, and no nulls
    batch = getBatchThreeBooleanCols();
    outCol = (LongColumnVector) batch.cols[2];
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    Assert.assertTrue(outCol.isRepeating);
    Assert.assertTrue(outCol.noNulls);
    Assert.assertEquals(0, outCol.vector[0]);
  }

  @Test
  public void testIsNotNullExpr() {
    // has nulls, not repeating
    VectorizedRowBatch batch = getBatchThreeBooleanCols();
    IsNotNull expr = new IsNotNull(0, 2);
    LongColumnVector outCol = (LongColumnVector) batch.cols[2];
    expr.evaluate(batch);
    Assert.assertEquals(1, outCol.vector[0]);
    Assert.assertEquals(0, outCol.vector[4]);
    Assert.assertTrue(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);

    // No nulls case, not repeating
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    Assert.assertTrue(outCol.isRepeating);
    Assert.assertTrue(outCol.noNulls);
    Assert.assertEquals(1, outCol.vector[0]);

    // isRepeating, and there are nulls
    batch = getBatchThreeBooleanCols();
    outCol = (LongColumnVector) batch.cols[2];
    batch.cols[0].isRepeating = true;
    batch.cols[0].isNull[0] = true;
    expr.evaluate(batch);
    Assert.assertTrue(outCol.isRepeating);
    Assert.assertEquals(0, outCol.vector[0]);
    Assert.assertTrue(outCol.noNulls);

    // isRepeating, and no nulls
    batch = getBatchThreeBooleanCols();
    outCol = (LongColumnVector) batch.cols[2];
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    Assert.assertTrue(outCol.isRepeating);
    Assert.assertTrue(outCol.noNulls);
    Assert.assertEquals(1, outCol.vector[0]);
  }

  @Test
  public void testBooleanFiltersOnColumns() {
    VectorizedRowBatch batch = getBatchThreeBooleanCols();

    SelectColumnIsTrue expr = new SelectColumnIsTrue(0);
    expr.evaluate(batch);
    assertEquals(3, batch.size);
    assertEquals(2, batch.selected[0]);
    assertEquals(3, batch.selected[1]);
    assertEquals(7, batch.selected[2]);

    batch = getBatchThreeBooleanCols();
    SelectColumnIsFalse expr1 = new SelectColumnIsFalse(1);
    expr1.evaluate(batch);
    assertEquals(3, batch.size);
    assertEquals(0, batch.selected[0]);
    assertEquals(2, batch.selected[1]);
    assertEquals(4, batch.selected[2]);
  }

  @Test
  public void testSelectColumnIsNull() {
    // has nulls, not repeating
    VectorizedRowBatch batch = getBatchThreeBooleanCols();
    SelectColumnIsNull expr = new SelectColumnIsNull(0);
    expr.evaluate(batch);
    assertEquals(3, batch.size);
    assertEquals(4, batch.selected[0]);
    assertEquals(5, batch.selected[1]);
    assertEquals(8, batch.selected[2]);

    // No nulls case, not repeating
    batch = getBatchThreeBooleanCols();
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(0, batch.size);

    // isRepeating, and there are nulls
    batch = getBatchThreeBooleanCols();
    batch.cols[0].isRepeating = true;
    batch.cols[0].isNull[0] = true;
    int initialSize = batch.size;
    expr.evaluate(batch);
    Assert.assertEquals(initialSize, batch.size);

    // isRepeating, and no nulls
    batch = getBatchThreeBooleanCols();
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(0, batch.size);
  }

  @Test
  public void testSelectColumnIsNotNull() {
    // has nulls, not repeating
    VectorizedRowBatch batch = getBatchThreeBooleanCols();
    SelectColumnIsNotNull expr = new SelectColumnIsNotNull(0);
    expr.evaluate(batch);
    assertEquals(6, batch.size);
    assertEquals(0, batch.selected[0]);
    assertEquals(1, batch.selected[1]);
    assertEquals(2, batch.selected[2]);
    assertEquals(3, batch.selected[3]);
    assertEquals(6, batch.selected[4]);
    assertEquals(7, batch.selected[5]);

    // No nulls case, not repeating
    batch = getBatchThreeBooleanCols();
    batch.cols[0].noNulls = true;
    int initialSize = batch.size;
    expr.evaluate(batch);
    Assert.assertEquals(initialSize, batch.size);

    // isRepeating, and there are nulls
    batch = getBatchThreeBooleanCols();
    batch.cols[0].isRepeating = true;
    batch.cols[0].isNull[0] = true;
    expr.evaluate(batch);
    Assert.assertEquals(0, batch.size);

    // isRepeating, and no nulls
    batch = getBatchThreeBooleanCols();
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = true;
    initialSize = batch.size;
    expr.evaluate(batch);
    Assert.assertEquals(initialSize, batch.size);
  }

  @Test
  public void testFilterExprOrExpr() {
    VectorizedRowBatch batch1 = getBatchThreeBooleanCols();
    VectorizedRowBatch batch2 = getBatchThreeBooleanCols();

    SelectColumnIsTrue expr1 = new SelectColumnIsTrue(0);
    SelectColumnIsFalse expr2 = new SelectColumnIsFalse(1);

    FilterExprOrExpr orExpr = new FilterExprOrExpr();
    orExpr.setChildExpressions(new VectorExpression[] {expr1, expr2});

    orExpr.evaluate(batch1);
    orExpr.evaluate(batch2);

    assertEquals(batch1.size, batch2.size);
    for (int j = 0; j < batch1.size; j++) {
      assertEquals(batch1.selected[j], batch2.selected[j]);
      int i = j;
      assertEquals((((LongColumnVector) batch1.cols[0]).vector[i]),
          (((LongColumnVector) batch2.cols[0]).vector[i]));
    }

    assertEquals(5, batch1.size);
    assertEquals(0, batch1.selected[0]);
    assertEquals(2, batch1.selected[1]);
    assertEquals(3, batch1.selected[2]);
    assertEquals(4, batch1.selected[3]);
    assertEquals(7, batch1.selected[4]);

    // Repeat the expression on the same batch,
    // the result must be unchanged.
    orExpr.evaluate(batch1);

    assertEquals(5, batch1.size);
    assertEquals(0, batch1.selected[0]);
    assertEquals(2, batch1.selected[1]);
    assertEquals(3, batch1.selected[2]);
    assertEquals(4, batch1.selected[3]);
    assertEquals(7, batch1.selected[4]);
  }

  @Test
  public void testFilterExprOrExprWithBatchReuse() {
    VectorizedRowBatch batch1 = getBatchThreeBooleanCols();

    SelectColumnIsTrue expr1 = new SelectColumnIsTrue(0);
    SelectColumnIsFalse expr2 = new SelectColumnIsFalse(1);

    FilterExprOrExpr orExpr = new FilterExprOrExpr();
    orExpr.setChildExpressions(new VectorExpression[] {expr1, expr2});

    orExpr.evaluate(batch1);

    // Now re-initialize batch1 to simulate batch-object re-use.
    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      batch1.selected[i] = 0;
    }
    batch1.size = BOOLEAN_COLUMN_TEST_SIZE;
    batch1.selectedInUse = false;

    // Swap column vectors to simulate change in data
    ColumnVector tmp = batch1.cols[0];
    batch1.cols[0] = batch1.cols[1];
    batch1.cols[1] = tmp;

    orExpr.evaluate(batch1);

    assertEquals(5, batch1.size);
    assertEquals(0, batch1.selected[0]);
    assertEquals(1, batch1.selected[1]);
    assertEquals(3, batch1.selected[2]);
    assertEquals(5, batch1.selected[3]);
    assertEquals(6, batch1.selected[4]);
  }

  @Test
  public void testFilterExprOrExprWithSelectInUse() {
    VectorizedRowBatch batch1 = getBatchThreeBooleanCols();

    SelectColumnIsTrue expr1 = new SelectColumnIsTrue(0);
    SelectColumnIsFalse expr2 = new SelectColumnIsFalse(1);

    FilterExprOrExpr orExpr = new FilterExprOrExpr();
    orExpr.setChildExpressions(new VectorExpression[] {expr1, expr2});

    // Evaluate batch1 so that temporary arrays in the expression
    // have residual values to interfere in later computation
    orExpr.evaluate(batch1);

    // Swap column vectors, but keep selected vector unchanged
    ColumnVector tmp = batch1.cols[0];
    batch1.cols[0] = batch1.cols[1];
    batch1.cols[1] = tmp;
    // Make sure row-7 is in the output.
    batch1.cols[1].isNull[7] = false;
    ((LongColumnVector) batch1.cols[1]).vector[7] = 0;

    orExpr.evaluate(batch1);

    assertEquals(3, batch1.size);
    assertEquals(0, batch1.selected[0]);
    assertEquals(3, batch1.selected[1]);
    assertEquals(7, batch1.selected[2]);
  }

  @Test
  public void testFilterExprAndExpr() {
    VectorizedRowBatch batch1 = getBatchThreeBooleanCols();

    SelectColumnIsTrue expr1 = new SelectColumnIsTrue(0);
    SelectColumnIsFalse expr2 = new SelectColumnIsFalse(1);

    FilterExprAndExpr andExpr = new FilterExprAndExpr();
    andExpr.setChildExpressions(new VectorExpression[] {expr1, expr2});

    andExpr.evaluate(batch1);

    assertEquals(1, batch1.size);

    assertEquals(2, batch1.selected[0]);
  }

  @Test
  public void testLongInExpr() {

    // check basic operation
    VectorizedRowBatch b = TestVectorMathFunctions.getVectorizedRowBatchLongInLongOut();
    LongColumnVector outV = (LongColumnVector) b.cols[1];
    long[] inVals = new long[2];
    inVals[0] = 0;
    inVals[1] = -2;
    LongColumnInList expr = new LongColumnInList(0, 1);
    expr.setInListValues(inVals);
    expr.evaluate(b);
    assertEquals(1, outV.vector[0]);
    assertEquals(0, outV.vector[1]);

    // check null handling
    b.cols[0].noNulls = false;
    b.cols[0].isNull[0] = true;
    expr.evaluate(b);
    assertEquals(true, !outV.noNulls && outV.isNull[0]);
    assertEquals(0, outV.vector[1]);

    // check isRepeating handling
    b = TestVectorMathFunctions.getVectorizedRowBatchLongInLongOut();
    outV = (LongColumnVector) b.cols[1];
    b.cols[0].isRepeating = true;
    expr.evaluate(b);
    assertEquals(true, outV.isRepeating);
    assertEquals(1, outV.vector[0]);
  }

  @Test
  public void testDoubleInExpr() {

    // check basic operation
    VectorizedRowBatch b = TestVectorMathFunctions.getVectorizedRowBatchDoubleInLongOut();
    LongColumnVector outV = (LongColumnVector) b.cols[1];
    double[] inVals = new double[2];
    inVals[0] = -1.5d;
    inVals[1] = 30d;
    b.size = 2;
    DoubleColumnInList expr = new DoubleColumnInList(0, 1);
    expr.setInListValues(inVals);
    expr.evaluate(b);
    assertEquals(1, outV.vector[0]);
    assertEquals(0, outV.vector[1]);

    // check null handling
    b.cols[0].noNulls = false;
    b.cols[0].isNull[0] = true;
    expr.evaluate(b);
    assertEquals(true, !outV.noNulls && outV.isNull[0]);
    assertEquals(0, outV.vector[1]);

    // check isRepeating handling
    b = TestVectorMathFunctions.getVectorizedRowBatchDoubleInLongOut();
    outV = (LongColumnVector) b.cols[1];
    b.cols[0].isRepeating = true;
    expr.evaluate(b);
    assertEquals(true, outV.isRepeating);
    assertEquals(1, outV.vector[0]);
  }
}
