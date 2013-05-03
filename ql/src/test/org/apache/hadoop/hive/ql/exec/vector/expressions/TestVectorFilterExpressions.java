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

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColEqualLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColGreaterLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColGreaterLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColLessLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColAddLongScalar;
import org.junit.Assert;
import org.junit.Test;

public class TestVectorFilterExpressions {

  @Test
  public void testFilterLongColEqualLongScalar() {
    VectorizedRowBatch vrg =
        VectorizedRowGroupGenUtil.getVectorizedRowBatch(1024, 1, 23);
    FilterLongColEqualLongScalar expr = new FilterLongColEqualLongScalar(0, 46);
    expr.evaluate(vrg);
    assertEquals(1, vrg.size);
    assertEquals(1, vrg.selected[0]);
  }

  @Test
  public void testFilterLongColEqualLongColumn() {
    int seed = 17;
    VectorizedRowBatch vrg = VectorizedRowGroupGenUtil.getVectorizedRowBatch(
        VectorizedRowBatch.DEFAULT_SIZE,
        2, seed);
    LongColumnVector lcv0 = (LongColumnVector) vrg.cols[0];
    LongColumnVector lcv1 = (LongColumnVector) vrg.cols[1];
    FilterLongColGreaterLongColumn expr = new FilterLongColGreaterLongColumn(0, 1);

    //Basic case
    lcv0.vector[1] = 23;
    lcv1.vector[1] = 19;
    lcv0.vector[5] = 23;
    lcv1.vector[5] = 19;
    expr.evaluate(vrg);
    assertEquals(2, vrg.size);
    assertEquals(1, vrg.selected[0]);
    assertEquals(5, vrg.selected[1]);

    //handle null
    lcv0.noNulls = false;
    lcv0.isNull[1] = true;
    expr.evaluate(vrg);
    assertEquals(1, vrg.size);
    assertEquals(5, vrg.selected[0]);
  }

  @Test
  public void testColOpScalarNumericFilterNullAndRepeatingLogic()
  {
    // No nulls, not repeating
    FilterLongColGreaterLongScalar f = new FilterLongColGreaterLongScalar(0, 1);
    VectorizedRowBatch batch = this.getSimpleLongBatch();

    batch.cols[0].noNulls = true;
    batch.cols[0].isRepeating = false;
    f.evaluate(batch);
    // only last 2 rows qualify
    Assert.assertEquals(2, batch.size);
    // show that their positions are recorded
    Assert.assertTrue(batch.selectedInUse);
    Assert.assertEquals(2, batch.selected[0]);
    Assert.assertEquals(3, batch.selected[1]);

    // make everything qualify and ensure selected is not in use
    f = new FilterLongColGreaterLongScalar(0, -1); // col > -1
    batch = getSimpleLongBatch();
    f.evaluate(batch);
    Assert.assertFalse(batch.selectedInUse);
    Assert.assertEquals(4, batch.size);

    // has nulls, not repeating
    batch = getSimpleLongBatch();
    f = new FilterLongColGreaterLongScalar(0, 1); // col > 1
    batch.cols[0].noNulls = false;
    batch.cols[0].isRepeating = false;
    batch.cols[0].isNull[3] = true;
    f.evaluate(batch);
    Assert.assertTrue(batch.selectedInUse);
    Assert.assertEquals(1, batch.size);
    Assert.assertEquals(2, batch.selected[0]);

    // no nulls, is repeating
    batch = getSimpleLongBatch();
    f = new FilterLongColGreaterLongScalar(0, -1); // col > -1
    batch.cols[0].noNulls = true;
    batch.cols[0].isRepeating = true;
    f.evaluate(batch);
    Assert.assertFalse(batch.selectedInUse);
    Assert.assertEquals(4, batch.size); // everything qualifies (4 rows, all with value -1)

    // has nulls, is repeating
    batch = getSimpleLongBatch();
    batch.cols[0].noNulls = false;
    batch.cols[0].isRepeating = true;
    batch.cols[0].isNull[0] = true;
    f.evaluate(batch);
    Assert.assertEquals(0, batch.size); // all values are null so none qualify
  }

  private VectorizedRowBatch getSimpleLongBatch() {
    VectorizedRowBatch batch = VectorizedRowGroupGenUtil
        .getVectorizedRowBatch(4, 1, 1);
    LongColumnVector lcv0 = (LongColumnVector) batch.cols[0];

    lcv0.vector[0] = 0;
    lcv0.vector[1] = 1;
    lcv0.vector[2] = 2;
    lcv0.vector[3] = 3;
    return batch;
  }

  @Test
  public void testFilterLongColLessLongColumn() {
    int seed = 17;
    VectorizedRowBatch vrg = VectorizedRowGroupGenUtil.getVectorizedRowBatch(
        5, 3, seed);
    LongColumnVector lcv0 = (LongColumnVector) vrg.cols[0];
    LongColumnVector lcv1 = (LongColumnVector) vrg.cols[1];
    LongColumnVector lcv2 = (LongColumnVector) vrg.cols[2];
    FilterLongColLessLongColumn expr = new FilterLongColLessLongColumn(2, 1);

    LongColAddLongScalar childExpr = new LongColAddLongScalar(0, 10, 2);

    expr.setChildExpressions(new VectorExpression[] {childExpr});

    //Basic case
    lcv0.vector[0] = 10;
    lcv0.vector[1] = 20;
    lcv0.vector[2] = 10;
    lcv0.vector[3] = 20;
    lcv0.vector[4] = 10;

    lcv1.vector[0] = 20;
    lcv1.vector[1] = 10;
    lcv1.vector[2] = 20;
    lcv1.vector[3] = 10;
    lcv1.vector[4] = 20;

    childExpr.evaluate(vrg);

    assertEquals(20, lcv2.vector[0]);
    assertEquals(30, lcv2.vector[1]);
    assertEquals(20, lcv2.vector[2]);
    assertEquals(30, lcv2.vector[3]);
    assertEquals(20, lcv2.vector[4]);

    expr.evaluate(vrg);

    assertEquals(0, vrg.size);
  }
}
