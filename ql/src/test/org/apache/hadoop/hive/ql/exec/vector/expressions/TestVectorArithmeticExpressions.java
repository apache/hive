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
import static org.junit.Assert.assertNull;
import org.junit.Assert;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TestVectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalColDivideDecimalScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalScalarDivideDecimalColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalScalarModuloDecimalColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalColAddDecimalColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalColDivideDecimalColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalColModuloDecimalColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalColModuloDecimalScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalColMultiplyDecimalScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalColSubtractDecimalScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalColMultiplyDecimalColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalColSubtractDecimalColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColAddLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColAddLongColumnChecked;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColAddLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalColSubtractDecimalColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalColAddDecimalColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalColMultiplyDecimalColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalColAddDecimalScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalColSubtractDecimalScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalColMultiplyDecimalScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalScalarAddDecimalColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalScalarSubtractDecimalColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalScalarMultiplyDecimalColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColAddLongScalarChecked;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColModuloLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.util.VectorizedRowGroupGenUtil;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;

/**
 * Unit tests for vectorized arithmetic expressions.
 */
public class TestVectorArithmeticExpressions {

  @Test
  public void testLongColAddLongScalarNoNulls() throws HiveException {
    longColAddLongScalarNoNulls(false);
  }

  @Test
  public void testLongColAddLongScalarCheckedNoNulls() throws HiveException {
    longColAddLongScalarNoNulls(true);
  }

  private void longColAddLongScalarNoNulls(boolean checked) throws HiveException {
    VectorizedRowBatch vrg = getVectorizedRowBatchSingleLongVector(VectorizedRowBatch.DEFAULT_SIZE);
    VectorExpression expr;
    if (checked) {
      expr = new LongColAddLongScalarChecked(0, 23, 1);
      expr.setOutputTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo("bigint"));
    } else {
      expr = new LongColAddLongScalar(0, 23, 1);
    }
    expr.evaluate(vrg);
    //verify
    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      Assert.assertEquals(i * 37 + 23, ((LongColumnVector) vrg.cols[1]).vector[i]);
    }
    Assert.assertTrue(((LongColumnVector)vrg.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector)vrg.cols[1]).isRepeating);
  }

  private VectorizedRowBatch getVectorizedRowBatchSingleLongVector(int size) {
    VectorizedRowBatch vrg = new VectorizedRowBatch(2, size);
    LongColumnVector lcv = new LongColumnVector(size);
    for (int i=0; i < size; i++) {
      lcv.vector[i] = i*37;
    }
    vrg.cols[0] = lcv;
    vrg.cols[1] = new LongColumnVector(size);
    vrg.size = size;
    return vrg;
  }

  public static VectorizedRowBatch getVectorizedRowBatch2LongInDoubleOut() {
    VectorizedRowBatch batch = new VectorizedRowBatch(3);
    LongColumnVector lcv, lcv2;
    lcv = new LongColumnVector();
    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      lcv.vector[i] = i * 37;
    }
    batch.cols[0] = lcv;
    lcv2 = new LongColumnVector();
    batch.cols[1] = lcv2;
    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      lcv2.vector[i] = i * 37;
    }
    batch.cols[2] = new DoubleColumnVector();
    batch.size = VectorizedRowBatch.DEFAULT_SIZE;
    return batch;
  }

  @Test
  public void testLongColAddLongScalarWithNulls() throws HiveException {
    longColAddLongScalarCheckedWithNulls(false);
  }

  @Test
  public void testLongColAddLongScalarCheckedWithNulls() throws HiveException {
    longColAddLongScalarCheckedWithNulls(true);
  }

  private void longColAddLongScalarCheckedWithNulls(boolean isChecked) throws HiveException {
    VectorizedRowBatch batch = getVectorizedRowBatchSingleLongVector(
        VectorizedRowBatch.DEFAULT_SIZE);
    LongColumnVector lcv = (LongColumnVector) batch.cols[0];
    LongColumnVector lcvOut = (LongColumnVector) batch.cols[1];
    TestVectorizedRowBatch.addRandomNulls(lcv);
    VectorExpression expr;
    if (isChecked) {
      expr = new LongColAddLongScalarChecked(0, 23, 1);
      expr.setOutputTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo("bigint"));
    } else {
      expr = new LongColAddLongScalar(0, 23, 1);
    }
    expr.evaluate(batch);

    // verify
    for (int i=0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      if (!lcv.isNull[i]) {
        Assert.assertEquals(i*37+23, lcvOut.vector[i]);
      } else {
        Assert.assertTrue(lcvOut.isNull[i]);
      }
    }
    Assert.assertFalse(lcvOut.noNulls);
    Assert.assertFalse(lcvOut.isRepeating);
    verifyLongNullDataVectorEntries(lcvOut, batch.selected, batch.selectedInUse, batch.size);
  }

  @Test
  public void testLongColAddLongScalarWithRepeating() throws HiveException {
    longColAddLongScalarWithRepeatingUtil(false);
  }

  @Test
  public void testLongColAddLongScalarCheckedWithRepeating() throws HiveException {
    longColAddLongScalarWithRepeatingUtil(true);
  }

  private void longColAddLongScalarWithRepeatingUtil(boolean isChecked) throws HiveException {
    LongColumnVector in, out;
    VectorizedRowBatch batch;
    VectorExpression expr;

    // Case 1: is repeating, no nulls
    batch = getVectorizedRowBatchSingleLongVector(VectorizedRowBatch.DEFAULT_SIZE);
    in = (LongColumnVector) batch.cols[0];
    in.isRepeating = true;
    out = (LongColumnVector) batch.cols[1];
    out.isRepeating = false;
    if(isChecked) {
      expr = new LongColAddLongScalarChecked(0, 23, 1);
      expr.setOutputTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo("bigint"));
    } else {
      expr = new LongColAddLongScalar(0, 23, 1);
    }

    expr.evaluate(batch);
    // verify
    Assert.assertTrue(out.isRepeating);
    Assert.assertTrue(out.noNulls);
    Assert.assertEquals(out.vector[0], 0 * 37 + 23);

    // Case 2: is repeating, has nulls
    batch = getVectorizedRowBatchSingleLongVector(VectorizedRowBatch.DEFAULT_SIZE);
    in = (LongColumnVector) batch.cols[0];
    in.isRepeating = true;
    in.noNulls = false;
    in.isNull[0] = true;

    out = (LongColumnVector) batch.cols[1];
    out.isRepeating = false;
    out.isNull[0] = false;
    out.noNulls = true;
    if (isChecked) {
      expr = new LongColAddLongScalarChecked(0, 23, 1);
      expr.setOutputTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo("bigint"));
    } else {
      expr = new LongColAddLongScalar(0, 23, 1);
    }

    expr.evaluate(batch);
    // verify
    Assert.assertTrue(out.isRepeating);
    Assert.assertFalse(out.noNulls);
    Assert.assertEquals(true, out.isNull[0]);
    verifyLongNullDataVectorEntries(out, batch.selected, batch.selectedInUse, batch.size);
  }

  /* Make sure all the NULL entries in this long column output vector have their data vector
   * element set to the correct value, as per the specification, to prevent later arithmetic
   * errors (e.g. zero-divide).
   */
  public static void verifyLongNullDataVectorEntries(
      LongColumnVector v, int[] sel, boolean selectedInUse, int n) {
    if (n == 0 || v.noNulls) {
      return;
    } else if (v.isRepeating) {
      if (v.isNull[0]) {
        assertEquals(LongColumnVector.NULL_VALUE, v.vector[0]);
      }
    } else if (selectedInUse) {
      for (int j = 0; j != n; j++) {
        int i = sel[j];
        if (v.isNull[i]) {
          assertEquals(LongColumnVector.NULL_VALUE, v.vector[i]);
        }
      }
    } else {
      for (int i = 0; i != n; i++) {
        if (v.isNull[i]) {
          assertEquals(LongColumnVector.NULL_VALUE, v.vector[i]);
        }
      }
    }
  }

  @Test
  public void testLongColAddLongColumn() throws HiveException {
    longColAddLongColumnUtil(false);
  }

  @Test
  public void testLongColAddLongColumnChecked() throws HiveException {
    longColAddLongColumnUtil(true);
  }

  private void longColAddLongColumnUtil(boolean isChecked) throws HiveException {
    int seed = 17;
    VectorizedRowBatch vrg = VectorizedRowGroupGenUtil.getVectorizedRowBatch(
        VectorizedRowBatch.DEFAULT_SIZE,
        6, seed);
    LongColumnVector lcv0 = (LongColumnVector) vrg.cols[0];
    LongColumnVector lcv1 = (LongColumnVector) vrg.cols[1];
    LongColumnVector lcv2 = (LongColumnVector) vrg.cols[2];
    LongColumnVector lcv3 = (LongColumnVector) vrg.cols[3];
    LongColumnVector lcv4 = (LongColumnVector) vrg.cols[4];
    LongColumnVector lcv5 = (LongColumnVector) vrg.cols[5];
    VectorExpression expr;
    if (isChecked) {
      expr = new LongColAddLongColumnChecked(0, 1, 2);
      expr.setOutputTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo("bigint"));
    } else {
      expr = new LongColAddLongColumn(0, 1, 2);
    }

    expr.evaluate(vrg);
    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      assertEquals((i+1) * seed * 3, lcv2.vector[i]);
    }
    assertTrue(lcv2.noNulls);

    // Now set one column nullable
    lcv1.noNulls = false;
    lcv1.isNull[1] = true;
    lcv2.isRepeating = true;   // set output isRepeating to true to make sure it gets over-written
    lcv2.noNulls = true;       // similarly with noNulls
    expr.evaluate(vrg);
    assertTrue(lcv2.isNull[1]);
    assertFalse(lcv2.noNulls);
    assertFalse(lcv2.isRepeating);
    verifyLongNullDataVectorEntries(lcv2, vrg.selected, vrg.selectedInUse, vrg.size);

    // Now set other column nullable too
    lcv0.noNulls = false;
    lcv0.isNull[1] = true;
    lcv0.isNull[3] = true;
    expr.evaluate(vrg);
    assertTrue(lcv2.isNull[1]);
    assertTrue(lcv2.isNull[3]);
    assertFalse(lcv2.noNulls);
    verifyLongNullDataVectorEntries(lcv2, vrg.selected, vrg.selectedInUse, vrg.size);

    // Now test with repeating flag
    lcv3.isRepeating = true;
    VectorExpression expr2;
    if (isChecked) {
      expr2 = new LongColAddLongColumnChecked(3, 4, 5);
      expr2.setOutputTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo("bigint"));
    } else {
      expr2 = new LongColAddLongColumn(3, 4, 5);
    }
    expr2.evaluate(vrg);
    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      assertEquals(seed * (4 + 5*(i+1)), lcv5.vector[i]);
    }

    // Repeating with other as nullable
    lcv4.noNulls = false;
    lcv4.isNull[0] = true;
    expr2.evaluate(vrg);
    assertTrue(lcv5.isNull[0]);
    assertFalse(lcv5.noNulls);
    verifyLongNullDataVectorEntries(lcv5, vrg.selected, vrg.selectedInUse, vrg.size);

    // Repeating null value
    lcv3.isRepeating = true;
    lcv3.noNulls = false;
    lcv3.isNull[0] = true;
    expr2.evaluate(vrg);
    assertFalse(lcv5.noNulls);
    assertTrue(lcv5.isRepeating);
    assertTrue(lcv5.isNull[0]);
    verifyLongNullDataVectorEntries(lcv5, vrg.selected, vrg.selectedInUse, vrg.size);

    // Neither input has nulls. Verify that this propagates to output.
    vrg.selectedInUse = false;
    lcv0.noNulls = true;
    lcv1.noNulls = true;
    lcv0.isRepeating = false;
    lcv1.isRepeating = false;
    lcv2.reset();
    expr.evaluate(vrg);
    assertTrue(lcv2.noNulls);
    assertFalse(lcv2.isRepeating);
  }

  @Test
  public void testLongColDivideLongColumn() throws HiveException {
    /* Testing for equality of doubles after a math operation is
     * not always reliable so use this as a tolerance.
     */
    final double eps = 1e-7d;
    VectorizedRowBatch batch = getVectorizedRowBatch2LongInDoubleOut();
    LongColDivideLongColumn expr = new LongColDivideLongColumn(0, 1, 2);
    batch.cols[0].isNull[1] = true;
    batch.cols[0].noNulls = false;
    batch.cols[1].noNulls = false;
    DoubleColumnVector out = (DoubleColumnVector) batch.cols[2];

    // Set so we can verify they are reset by operation
    out.noNulls = true;
    out.isRepeating = true;

    expr.evaluate(batch);

    // 0/0 for entry 0 should work but generate NaN
    assertFalse(out.noNulls);
    assertTrue(out.isNull[0]);
    assertTrue(Double.isNaN(out.vector[0]));

    // verify NULL output in entry 1 is correct
    assertTrue(out.isNull[1]);
    assertTrue(Double.isNaN(out.vector[1]));

    // check entries beyond first 2
    for (int i = 2; i != batch.size; i++) {
      assertTrue(out.vector[i] > 1.0d - eps && out.vector[i] < 1.0d + eps);
    }
    assertFalse(out.noNulls);
    assertFalse(out.isRepeating);
  }

  @Test
  public void testLongColModuloLongColumn() throws HiveException {
    VectorizedRowBatch batch = getVectorizedRowBatch2LongInLongOut();
    LongColModuloLongColumn expr = new LongColModuloLongColumn(0, 1, 2);
    batch.cols[0].isNull[1] = true;
    batch.cols[0].noNulls = false;
    batch.cols[1].noNulls = false;
    LongColumnVector out = (LongColumnVector) batch.cols[2];

    // Set so we can verify they are reset by operation
    out.noNulls = true;
    out.isRepeating = true;

    expr.evaluate(batch);

    // 0/0 for entry 0 should be set as NULL
    assertFalse(out.noNulls);
    assertTrue(out.isNull[0]);

    // verify NULL output in entry 1 is correct
    assertTrue(out.isNull[1]);

    // check entries beyond first 2
    for (int i = 2; i != batch.size; i++) {
      assertTrue(out.vector[i] == 0L);
    }
    assertFalse(out.noNulls);
    assertFalse(out.isRepeating);
  }

  private VectorizedRowBatch getVectorizedRowBatch2LongInLongOut() {
    VectorizedRowBatch batch = new VectorizedRowBatch(3);
    LongColumnVector lcv, lcv2;
    lcv = new LongColumnVector();
    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      lcv.vector[i] = i * 37;
    }
    batch.cols[0] = lcv;
    lcv2 = new LongColumnVector();
    batch.cols[1] = lcv2;
    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      lcv2.vector[i] = i * 37;
    }
    batch.cols[2] = new LongColumnVector();
    batch.size = VectorizedRowBatch.DEFAULT_SIZE;
    return batch;
  }

  @Test
  public void testDecimalColAddDecimalColumn() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatch3DecimalCols();
    VectorExpression expr = new DecimalColAddDecimalColumn(0, 1, 2);
    DecimalColumnVector r = (DecimalColumnVector) b.cols[2];

    // test without nulls
    expr.evaluate(b);
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("2.20")));
    assertTrue(r.vector[1].getHiveDecimal().equals(HiveDecimal.create("-2.30")));
    assertTrue(r.vector[2].getHiveDecimal().equals(HiveDecimal.create("1.00")));

    // test nulls propagation
    b = getVectorizedRowBatch3DecimalCols();
    DecimalColumnVector c0 = (DecimalColumnVector) b.cols[0];
    c0.noNulls = false;
    c0.isNull[0] = true;
    r = (DecimalColumnVector) b.cols[2];
    expr.evaluate(b);
    assertTrue(!r.noNulls && r.isNull[0]);

    // Verify null output data entry is not 0, but rather the value specified by design,
    // which is the minimum non-0 value, 0.01 in this case.
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("0.01")));

    // test that overflow produces NULL
    b = getVectorizedRowBatch3DecimalCols();
    c0 = (DecimalColumnVector) b.cols[0];
    c0.vector[0].set(HiveDecimal.create("9999999999999999.99")); // set to max possible value
    r = (DecimalColumnVector) b.cols[2];
    expr.evaluate(b); // will cause overflow for result at position 0, must yield NULL
    assertTrue(!r.noNulls && r.isNull[0]);

    // verify proper null output data value
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("0.01")));

    // test left input repeating
    b = getVectorizedRowBatch3DecimalCols();
    c0 = (DecimalColumnVector) b.cols[0];
    c0.isRepeating = true;
    r = (DecimalColumnVector) b.cols[2];
    expr.evaluate(b);
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("2.20")));
    assertTrue(r.vector[1].getHiveDecimal().equals(HiveDecimal.create("2.20")));
    assertTrue(r.vector[2].getHiveDecimal().equals(HiveDecimal.create("2.20")));

    // test both inputs repeating
    DecimalColumnVector c1 = (DecimalColumnVector) b.cols[1];
    c1.isRepeating = true;
    expr.evaluate(b);
    assertTrue(r.isRepeating);
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("2.20")));

    // test right input repeating
    b = getVectorizedRowBatch3DecimalCols();
    c1 = (DecimalColumnVector) b.cols[1];
    c1.isRepeating = true;
    c1.vector[0].set(HiveDecimal.create("2.00"));
    r = (DecimalColumnVector) b.cols[2];
    expr.evaluate(b);
    assertTrue(r.vector[2].getHiveDecimal().equals(HiveDecimal.create("2.00")));
  }

  // Spot check decimal column-column subtract
  @Test
  public void testDecimalColSubtractDecimalColumn() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatch3DecimalCols();
    VectorExpression expr = new DecimalColSubtractDecimalColumn(0, 1, 2);
    DecimalColumnVector r = (DecimalColumnVector) b.cols[2];

    // test without nulls
    expr.evaluate(b);
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("0.20")));
    assertTrue(r.vector[1].getHiveDecimal().equals(HiveDecimal.create("-4.30")));
    assertTrue(r.vector[2].getHiveDecimal().equals(HiveDecimal.create("-1.00")));

    // test that underflow produces NULL
    b = getVectorizedRowBatch3DecimalCols();
    DecimalColumnVector c0 = (DecimalColumnVector) b.cols[0];
    c0.vector[0].set(HiveDecimal.create("-9999999999999999.99")); // set to min possible value
    r = (DecimalColumnVector) b.cols[2];
    expr.evaluate(b); // will cause underflow for result at position 0, must yield NULL
    assertTrue(!r.noNulls && r.isNull[0]);
  }

  // Spot check decimal column-column multiply
  @Test
  public void testDecimalColMultiplyDecimalColumn() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatch3DecimalCols();
    VectorExpression expr = new DecimalColMultiplyDecimalColumn(0, 1, 2);
    DecimalColumnVector r = (DecimalColumnVector) b.cols[2];

    // test without nulls
    expr.evaluate(b);
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("1.20")));
    assertTrue(r.vector[1].getHiveDecimal().equals(HiveDecimal.create("-3.30")));
    assertTrue(r.vector[2].getHiveDecimal().equals(HiveDecimal.create("0.00")));

    // test that underflow produces NULL
    b = getVectorizedRowBatch3DecimalCols();
    DecimalColumnVector c0 = (DecimalColumnVector) b.cols[0];
    c0.vector[0].set(HiveDecimal.create("9999999999999999.99")); // set to max possible value
    DecimalColumnVector c1 = (DecimalColumnVector) b.cols[1];
    c1.vector[0].set(HiveDecimal.create("2.00"));
    r = (DecimalColumnVector) b.cols[2];
    expr.evaluate(b); // will cause overflow for result at position 0, must yield NULL
    assertTrue(!r.noNulls && r.isNull[0]);
  }

  /* Test decimal column to decimal scalar addition. This is used to cover all the
   * cases used in the source code template ColumnArithmeticScalarDecimal.txt.
   */
  @Test
  public void testDecimalColAddDecimalScalar() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatch3DecimalCols();
    HiveDecimal d = HiveDecimal.create(1);
    VectorExpression expr = new DecimalColAddDecimalScalar(0, d, 2);

    // test without nulls
    expr.evaluate(b);
    DecimalColumnVector r = (DecimalColumnVector) b.cols[2];
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("2.20")));
    assertTrue(r.vector[1].getHiveDecimal().equals(HiveDecimal.create("-2.30")));
    assertTrue(r.vector[2].getHiveDecimal().equals(HiveDecimal.create("1")));

    // test null propagation
    b = getVectorizedRowBatch3DecimalCols();
    DecimalColumnVector in = (DecimalColumnVector) b.cols[0];
    r = (DecimalColumnVector) b.cols[2];
    in.noNulls = false;
    in.isNull[0] = true;
    expr.evaluate(b);
    assertTrue(!r.noNulls);
    assertTrue(r.isNull[0]);

    // test repeating case, no nulls
    b = getVectorizedRowBatch3DecimalCols();
    in = (DecimalColumnVector) b.cols[0];
    in.isRepeating = true;
    expr.evaluate(b);
    r = (DecimalColumnVector) b.cols[2];
    assertTrue(r.isRepeating);
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("2.20")));

    // test repeating case for null value
    b = getVectorizedRowBatch3DecimalCols();
    in = (DecimalColumnVector) b.cols[0];
    in.isRepeating = true;
    in.isNull[0] = true;
    in.noNulls = false;
    expr.evaluate(b);
    r = (DecimalColumnVector) b.cols[2];
    assertTrue(r.isRepeating);
    assertTrue(!r.noNulls);
    assertTrue(r.isNull[0]);

    // test that overflow produces null
    b = getVectorizedRowBatch3DecimalCols();
    in = (DecimalColumnVector) b.cols[0];
    in.vector[0].set(HiveDecimal.create("9999999999999999.99")); // set to max possible value
    expr.evaluate(b);
    r = (DecimalColumnVector) b.cols[2];
    assertFalse(r.noNulls);
    assertTrue(r.isNull[0]);
  }

  /* Test decimal column to decimal scalar division. This is used to cover all the
   * cases used in the source code template ColumnDivideScalarDecimal.txt.
   * The template is used for division and modulo.
   */
  @Test
  public void testDecimalColDivideDecimalScalar() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatch3DecimalCols();
    HiveDecimal d = HiveDecimal.create("2.00");
    VectorExpression expr = new DecimalColDivideDecimalScalar(0, d, 2);


    // test without nulls
    expr.evaluate(b);
    DecimalColumnVector r = (DecimalColumnVector) b.cols[2];
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("0.6")));
    assertTrue(r.vector[1].getHiveDecimal().equals(HiveDecimal.create("-1.65")));
    assertTrue(r.vector[2].getHiveDecimal().equals(HiveDecimal.create("0")));

    // test null propagation
    b = getVectorizedRowBatch3DecimalCols();
    DecimalColumnVector in = (DecimalColumnVector) b.cols[0];
    r = (DecimalColumnVector) b.cols[2];
    in.noNulls = false;
    in.isNull[0] = true;
    expr.evaluate(b);
    assertTrue(!r.noNulls);
    assertTrue(r.isNull[0]);

    // test repeating case, no nulls
    b = getVectorizedRowBatch3DecimalCols();
    in = (DecimalColumnVector) b.cols[0];
    in.isRepeating = true;
    expr.evaluate(b);
    r = (DecimalColumnVector) b.cols[2];
    assertTrue(r.isRepeating);
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("0.6")));

    // test repeating case for null value
    b = getVectorizedRowBatch3DecimalCols();
    in = (DecimalColumnVector) b.cols[0];
    in.isRepeating = true;
    in.isNull[0] = true;
    in.noNulls = false;
    expr.evaluate(b);
    r = (DecimalColumnVector) b.cols[2];
    assertTrue(r.isRepeating);
    assertTrue(!r.noNulls);
    assertTrue(r.isNull[0]);

    // test that zero-divide produces null for all output values
    b = getVectorizedRowBatch3DecimalCols();
    in = (DecimalColumnVector) b.cols[0];
    expr = new DecimalColDivideDecimalScalar(0, HiveDecimal.create("0"), 2);
    expr.evaluate(b);
    r = (DecimalColumnVector) b.cols[2];
    assertFalse(r.noNulls);
    assertTrue(r.isNull[0]);
    assertTrue(r.isRepeating);
  }

  /* Test decimal scalar divided column. This tests the primary logic
   * for template ScalarDivideColumnDecimal.txt.
   */
  @Test
  public void testDecimalScalarDivideDecimalColumn() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatch3DecimalCols();
    HiveDecimal d = HiveDecimal.create("3.96");  // 1.20 * 3.30
    VectorExpression expr = new DecimalScalarDivideDecimalColumn(d, 0, 2);

    // test without nulls
    expr.evaluate(b);
    DecimalColumnVector r = (DecimalColumnVector) b.cols[2];
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("3.3")));
    assertTrue(r.vector[1].getHiveDecimal().equals(HiveDecimal.create("-1.2")));
    assertFalse(r.noNulls); // entry 2 is null due to zero-divide
    assertTrue(r.isNull[2]);

    // test null propagation
    b = getVectorizedRowBatch3DecimalCols();
    DecimalColumnVector in = (DecimalColumnVector) b.cols[0];
    r = (DecimalColumnVector) b.cols[2];
    in.noNulls = false;
    in.isNull[0] = true;
    expr.evaluate(b);
    assertTrue(!r.noNulls);
    assertTrue(r.isNull[0]);

    // test repeating case, no nulls
    b = getVectorizedRowBatch3DecimalCols();
    in = (DecimalColumnVector) b.cols[0];
    in.isRepeating = true;
    expr.evaluate(b);
    r = (DecimalColumnVector) b.cols[2];
    assertTrue(r.isRepeating);
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("3.3")));

    // test repeating case for null value
    b = getVectorizedRowBatch3DecimalCols();
    in = (DecimalColumnVector) b.cols[0];
    in.isRepeating = true;
    in.isNull[0] = true;
    in.noNulls = false;
    expr.evaluate(b);
    r = (DecimalColumnVector) b.cols[2];
    assertTrue(r.isRepeating);
    assertTrue(!r.noNulls);
    assertTrue(r.isNull[0]);
  }

  // Spot check Decimal Col-Scalar Modulo
  @Test
  public void testDecimalColModuloDecimalScalar() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatch3DecimalCols();
    HiveDecimal d = HiveDecimal.create("2.00");
    VectorExpression expr = new DecimalColModuloDecimalScalar(0, d, 2);

    // test without nulls
    expr.evaluate(b);
    DecimalColumnVector r = (DecimalColumnVector) b.cols[2];
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("1.20")));
    assertTrue(r.vector[1].getHiveDecimal().equals(HiveDecimal.create("-1.30")));
    assertTrue(r.vector[2].getHiveDecimal().equals(HiveDecimal.create("0")));

    // try again with some different data values and divisor
    DecimalColumnVector in = (DecimalColumnVector) b.cols[0];
    in.vector[0].set(HiveDecimal.create("15.40"));
    in.vector[1].set(HiveDecimal.create("-17.20"));
    in.vector[2].set(HiveDecimal.create("70.00"));
    d = HiveDecimal.create("4.75");
    expr = new DecimalColModuloDecimalScalar(0, d, 2);

    expr.evaluate(b);
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("1.15")));
    assertTrue(r.vector[1].getHiveDecimal().equals(HiveDecimal.create("-2.95")));
    assertTrue(r.vector[2].getHiveDecimal().equals(HiveDecimal.create("3.50")));

    // try a zero-divide to show a repeating NULL is produced
    d = HiveDecimal.create("0.00");
    expr = new DecimalColModuloDecimalScalar(0, d, 2);
    expr.evaluate(b);
    assertFalse(r.noNulls);
    assertTrue(r.isNull[0]);
    assertTrue(r.isRepeating);
  }

  // Spot check decimal scalar-column modulo
  @Test
  public void testDecimalScalarModuloDecimalColumn() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatch3DecimalCols();
    HiveDecimal d = HiveDecimal.create("2.00");
    VectorExpression expr = new DecimalScalarModuloDecimalColumn(d, 0, 2);

    // test without nulls
    expr.evaluate(b);
    DecimalColumnVector r = (DecimalColumnVector) b.cols[2];
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("0.80")));
    assertTrue(r.vector[1].getHiveDecimal().equals(HiveDecimal.create("2.00")));
    assertFalse(r.noNulls); // entry 2 will be null due to zero-divide
    assertTrue(r.isNull[2]);

    // try again with some different data values
    DecimalColumnVector in = (DecimalColumnVector) b.cols[0];
    expr = new DecimalScalarModuloDecimalColumn(d, 0, 2);
    in.vector[0].set(HiveDecimal.create("0.50"));
    in.vector[1].set(HiveDecimal.create("0.80"));
    in.vector[2].set(HiveDecimal.create("0.70"));

    expr.evaluate(b);
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("0.00")));
    assertTrue(r.vector[1].getHiveDecimal().equals(HiveDecimal.create("0.40")));
    assertTrue(r.vector[2].getHiveDecimal().equals(HiveDecimal.create("0.60")));
  }

  @Test
  public void testDecimalColDivideDecimalColumn() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatch3DecimalCols();
    DecimalColumnVector in1 = (DecimalColumnVector) b.cols[1];
    for (int i = 0; i < 3; i++) {
      in1.vector[i].set(HiveDecimal.create("0.50"));
    }
    VectorExpression expr = new DecimalColDivideDecimalColumn(0, 1, 2);
    expr.evaluate(b);
    DecimalColumnVector r = (DecimalColumnVector) b.cols[2];

    // all divides are by 0.50 so the result column is 2 times col 0.
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("2.4")));
    assertTrue(r.vector[1].getHiveDecimal().equals(HiveDecimal.create("-6.6")));
    assertTrue(r.vector[2].getHiveDecimal().equals(HiveDecimal.create("0")));

    // test null on left
    b.cols[0].noNulls = false;
    b.cols[0].isNull[0] = true;
    expr.evaluate(b);
    assertFalse(r.noNulls);
    assertTrue(r.isNull[0]);

    // test null on right
    b = getVectorizedRowBatch3DecimalCols();
    b.cols[1].noNulls = false;
    b.cols[1].isNull[0] = true;
    r = (DecimalColumnVector) b.cols[2];
    expr.evaluate(b);
    assertFalse(r.noNulls);
    assertTrue(r.isNull[0]);

    // test null on both sides
    b = getVectorizedRowBatch3DecimalCols();
    b.cols[0].noNulls = false;
    b.cols[0].isNull[0] = true;
    b.cols[1].noNulls = false;
    b.cols[1].isNull[0] = true;
    expr.evaluate(b);
    assertFalse(r.noNulls);
    assertTrue(r.isNull[0]);
    assertFalse(r.isNull[1]);
    assertFalse(r.isNull[2]);

    // test repeating on left
    b = getVectorizedRowBatch3DecimalCols();
    b.cols[0].isRepeating = true;
    expr.evaluate(b);
    r = (DecimalColumnVector) b.cols[2];
    assertTrue(r.vector[2].getHiveDecimal().equals(HiveDecimal.create("1.2")));

    // test repeating on right
    b = getVectorizedRowBatch3DecimalCols();
    b.cols[1].isRepeating = true;
    expr.evaluate(b);
    r = (DecimalColumnVector) b.cols[2];
    assertTrue(r.vector[2].getHiveDecimal().equals(HiveDecimal.create("0")));

    // test both repeating
    b = getVectorizedRowBatch3DecimalCols();
    b.cols[0].isRepeating = true;
    b.cols[1].isRepeating = true;
    expr.evaluate(b);
    r = (DecimalColumnVector) b.cols[2];
    assertTrue(r.isRepeating);
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("1.2")));

    // test zero-divide to show it results in NULL
    b = getVectorizedRowBatch3DecimalCols();
    ((DecimalColumnVector) b.cols[1]).vector[0].set(HiveDecimal.create("0"));
    expr.evaluate(b);
    r = (DecimalColumnVector) b.cols[2];
    assertFalse(r.noNulls);
    assertTrue(r.isNull[0]);
  }

  // Spot check decimal column modulo decimal column
  @Test
  public void testDecimalColModuloDecimalColumn() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatch3DecimalCols();
    DecimalColumnVector in1 = (DecimalColumnVector) b.cols[1];
    for (int i = 0; i < 3; i++) {
      in1.vector[i].set(HiveDecimal.create("0.50"));
    }
    VectorExpression expr = new DecimalColModuloDecimalColumn(0, 1, 2);
    expr.evaluate(b);
    DecimalColumnVector r = (DecimalColumnVector) b.cols[2];

    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("0.20")));
    assertTrue(r.vector[1].getHiveDecimal().equals(HiveDecimal.create("-0.30")));
    assertTrue(r.vector[2].getHiveDecimal().equals(HiveDecimal.create("0")));
  }

  /* Spot check correctness of decimal column subtract decimal scalar. The case for
   * addition checks all the cases for the template, so don't do that redundantly here.
   */
  @Test
  public void testDecimalColSubtractDecimalScalar() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatch3DecimalCols();
    HiveDecimal d = HiveDecimal.create(1);
    VectorExpression expr = new DecimalColSubtractDecimalScalar(0, d, 2);

    // test without nulls
    expr.evaluate(b);
    DecimalColumnVector r = (DecimalColumnVector) b.cols[2];
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("0.20")));
    assertTrue(r.vector[1].getHiveDecimal().equals(HiveDecimal.create("-4.30")));
    assertTrue(r.vector[2].getHiveDecimal().equals(HiveDecimal.create("-1")));

    // test that underflow produces null
    b = getVectorizedRowBatch3DecimalCols();
    DecimalColumnVector in = (DecimalColumnVector) b.cols[0];
    in.vector[0].set(HiveDecimal.create("-9999999999999999.99")); // set to min possible value
    expr.evaluate(b);
    r = (DecimalColumnVector) b.cols[2];
    assertFalse(r.noNulls);
    assertTrue(r.isNull[0]);
  }

  /* Spot check correctness of decimal column multiply decimal scalar. The case for
   * addition checks all the cases for the template, so don't do that redundantly here.
   */
  @Test
  public void testDecimalColMultiplyDecimalScalar() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatch3DecimalCols();
    HiveDecimal d = HiveDecimal.create(2);
    VectorExpression expr = new DecimalColMultiplyDecimalScalar(0, d, 2);

    // test without nulls
    expr.evaluate(b);
    DecimalColumnVector r = (DecimalColumnVector) b.cols[2];
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("2.40")));
    assertTrue(r.vector[1].getHiveDecimal().equals(HiveDecimal.create("-6.60")));
    assertTrue(r.vector[2].getHiveDecimal().equals(HiveDecimal.create("0")));

    // test that overflow produces null
    b = getVectorizedRowBatch3DecimalCols();
    DecimalColumnVector in = (DecimalColumnVector) b.cols[0];
    in.vector[0].set(HiveDecimal.create("9999999999999999.99")); // set to max possible value
    expr.evaluate(b);
    r = (DecimalColumnVector) b.cols[2];
    assertFalse(r.noNulls);
    assertTrue(r.isNull[0]);
  }

  /* Test decimal scalar to decimal column addition. This is used to cover all the
   * cases used in the source code template ScalarArithmeticColumnDecimal.txt.
   */
  @Test
  public void testDecimalScalarAddDecimalColumn() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatch3DecimalCols();
    HiveDecimal d = HiveDecimal.create(1);
    VectorExpression expr = new DecimalScalarAddDecimalColumn(d, 0, 2);

    // test without nulls
    expr.evaluate(b);
    DecimalColumnVector r = (DecimalColumnVector) b.cols[2];
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("2.20")));
    assertTrue(r.vector[1].getHiveDecimal().equals(HiveDecimal.create("-2.30")));
    assertTrue(r.vector[2].getHiveDecimal().equals(HiveDecimal.create("1")));

    // test null propagation
    b = getVectorizedRowBatch3DecimalCols();
    DecimalColumnVector in = (DecimalColumnVector) b.cols[0];
    r = (DecimalColumnVector) b.cols[2];
    in.noNulls = false;
    in.isNull[0] = true;
    expr.evaluate(b);
    assertTrue(!r.noNulls);
    assertTrue(r.isNull[0]);

    // test repeating case, no nulls
    b = getVectorizedRowBatch3DecimalCols();
    in = (DecimalColumnVector) b.cols[0];
    in.isRepeating = true;
    expr.evaluate(b);
    r = (DecimalColumnVector) b.cols[2];
    assertTrue(r.isRepeating);
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("2.20")));

    // test repeating case for null value
    b = getVectorizedRowBatch3DecimalCols();
    in = (DecimalColumnVector) b.cols[0];
    in.isRepeating = true;
    in.isNull[0] = true;
    in.noNulls = false;
    expr.evaluate(b);
    r = (DecimalColumnVector) b.cols[2];
    assertTrue(r.isRepeating);
    assertTrue(!r.noNulls);
    assertTrue(r.isNull[0]);

    // test that overflow produces null
    b = getVectorizedRowBatch3DecimalCols();
    in = (DecimalColumnVector) b.cols[0];
    in.vector[0].set(HiveDecimal.create("9999999999999999.99")); // set to max possible value
    expr.evaluate(b);
    r = (DecimalColumnVector) b.cols[2];
    assertFalse(r.noNulls);
    assertTrue(r.isNull[0]);
  }

  /* Spot check correctness of decimal scalar subtract decimal column. The case for
   * addition checks all the cases for the template, so don't do that redundantly here.
   */
  @Test
  public void testDecimalScalarSubtractDecimalColumn() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatch3DecimalCols();
    HiveDecimal d = HiveDecimal.create(1);
    VectorExpression expr = new DecimalScalarSubtractDecimalColumn(d, 0, 2);

    // test without nulls
    expr.evaluate(b);
    DecimalColumnVector r = (DecimalColumnVector) b.cols[2];
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("-0.20")));
    assertTrue(r.vector[1].getHiveDecimal().equals(HiveDecimal.create("4.30")));
    assertTrue(r.vector[2].getHiveDecimal().equals(HiveDecimal.create("1")));

    // test that overflow produces null
    b = getVectorizedRowBatch3DecimalCols();
    DecimalColumnVector in = (DecimalColumnVector) b.cols[0];
    in.vector[0].set(HiveDecimal.create("-9999999999999999.99")); // set to min possible value
    expr.evaluate(b);
    r = (DecimalColumnVector) b.cols[2];
    assertFalse(r.noNulls);
    assertTrue(r.isNull[0]);
  }

  /* Spot check correctness of decimal scalar multiply decimal column. The case for
   * addition checks all the cases for the template, so don't do that redundantly here.
   */

  @Test
  public void testDecimalScalarMultiplyDecimalColumn() throws HiveException {
    VectorizedRowBatch b = getVectorizedRowBatch3DecimalCols();
    HiveDecimal d = HiveDecimal.create(2);
    VectorExpression expr = new DecimalScalarMultiplyDecimalColumn(d, 0, 2);

    // test without nulls
    expr.evaluate(b);
    DecimalColumnVector r = (DecimalColumnVector) b.cols[2];
    assertTrue(r.vector[0].getHiveDecimal().equals(HiveDecimal.create("2.40")));
    assertTrue(r.vector[1].getHiveDecimal().equals(HiveDecimal.create("-6.60")));
    assertTrue(r.vector[2].getHiveDecimal().equals(HiveDecimal.create("0")));

    // test that overflow produces null
    b = getVectorizedRowBatch3DecimalCols();
    DecimalColumnVector in = (DecimalColumnVector) b.cols[0];
    in.vector[0].set(HiveDecimal.create("9999999999999999.99")); // set to max possible value
    expr.evaluate(b);
    r = (DecimalColumnVector) b.cols[2];
    assertFalse(r.noNulls);
    assertTrue(r.isNull[0]);
  }

  // Make a decimal batch with three columns, including two for inputs and one for the result.
  private VectorizedRowBatch getVectorizedRowBatch3DecimalCols() {
    VectorizedRowBatch b = new VectorizedRowBatch(3);
    DecimalColumnVector v0, v1;
    b.cols[0] = v0 = new DecimalColumnVector(18, 2);
    b.cols[1] = v1 = new DecimalColumnVector(18, 2);
    b.cols[2] = new DecimalColumnVector(18, 2);
    v0.vector[0].set(HiveDecimal.create("1.20"));
    v0.vector[1].set(HiveDecimal.create("-3.30"));
    v0.vector[2].set(HiveDecimal.create("0"));

    v1.vector[0].set(HiveDecimal.create("1.00"));
    v1.vector[1].set(HiveDecimal.create("1.00"));
    v1.vector[2].set(HiveDecimal.create("1.00"));

    b.size = 3;

    return b;
  }
}
