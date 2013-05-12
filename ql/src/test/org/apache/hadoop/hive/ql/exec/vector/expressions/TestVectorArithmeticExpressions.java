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
import junit.framework.Assert;

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TestVectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColAddLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColAddLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.util.VectorizedRowGroupGenUtil;
import org.junit.Test;

public class TestVectorArithmeticExpressions {

  @Test
  public void testLongColAddLongScalarNoNulls()  {
    VectorizedRowBatch vrg = getVectorizedRowBatchSingleLongVector
        (VectorizedRowBatch.DEFAULT_SIZE);
    LongColAddLongScalar expr = new LongColAddLongScalar(0, 23, 1);
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

  @Test
  public void testLongColAddLongScalarWithNulls()  {
    VectorizedRowBatch vrg = getVectorizedRowBatchSingleLongVector
        (VectorizedRowBatch.DEFAULT_SIZE);
    LongColumnVector lcv = (LongColumnVector) vrg.cols[0];
    TestVectorizedRowBatch.addRandomNulls(lcv);
    LongColAddLongScalar expr = new LongColAddLongScalar(0, 23, 1);
    expr.evaluate(vrg);
    //verify
    for (int i=0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      if (!lcv.isNull[i]) {
        Assert.assertEquals(i*37+23, ((LongColumnVector)vrg.cols[1]).vector[i]);
      } else {
        Assert.assertTrue(((LongColumnVector)vrg.cols[1]).isNull[i]);
      }
    }
    Assert.assertFalse(((LongColumnVector)vrg.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector)vrg.cols[1]).isRepeating);
  }

  @Test
  public void testLongColAddLongScalarWithRepeating() {
    LongColumnVector in, out;
    VectorizedRowBatch batch;
    LongColAddLongScalar expr;

    // Case 1: is repeating, no nulls
    batch = getVectorizedRowBatchSingleLongVector
        (VectorizedRowBatch.DEFAULT_SIZE);
    in = (LongColumnVector) batch.cols[0];
    in.isRepeating = true;
    out = (LongColumnVector) batch.cols[1];
    out.isRepeating = false;
    expr = new LongColAddLongScalar(0, 23, 1);
    expr.evaluate(batch);
    // verify
    Assert.assertTrue(out.isRepeating);
    Assert.assertTrue(out.noNulls);
    Assert.assertEquals(out.vector[0], 0 * 37 + 23);

    // Case 2: is repeating, has nulls
    batch = getVectorizedRowBatchSingleLongVector
        (VectorizedRowBatch.DEFAULT_SIZE);
    in = (LongColumnVector) batch.cols[0];
    in.isRepeating = true;
    in.noNulls = false;
    in.isNull[0] = true;

    out = (LongColumnVector) batch.cols[1];
    out.isRepeating = false;
    out.isNull[0] = false;
    out.noNulls = true;
    expr = new LongColAddLongScalar(0, 23, 1);
    expr.evaluate(batch);
    // verify
    Assert.assertTrue(out.isRepeating);
    Assert.assertFalse(out.noNulls);
    Assert.assertEquals(true, out.isNull[0]);
  }

  @Test
  public void testLongColAddLongColumn() {
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
    LongColAddLongColumn expr = new LongColAddLongColumn(0, 1, 2);
    expr.evaluate(vrg);
    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      assertEquals((i+1) * seed * 3, lcv2.vector[i]);
    }
    assertTrue(lcv2.noNulls);

    //Now set one column nullable
    lcv1.noNulls = false;
    lcv1.isNull[1] = true;
    lcv2.isRepeating = true;   // set output isRepeating to true to make sure it gets over-written
    lcv2.noNulls = true;       // similarly with noNulls
    expr.evaluate(vrg);
    assertTrue(lcv2.isNull[1]);
    assertFalse(lcv2.noNulls);
    assertFalse(lcv2.isRepeating);

    //Now set other column nullable too
    lcv0.noNulls = false;
    lcv0.isNull[1] = true;
    lcv0.isNull[3] = true;
    expr.evaluate(vrg);
    assertTrue(lcv2.isNull[1]);
    assertTrue(lcv2.isNull[3]);
    assertFalse(lcv2.noNulls);

    //Now test with repeating flag
    lcv3.isRepeating = true;
    LongColAddLongColumn expr2 = new LongColAddLongColumn(3, 4, 5);
    expr2.evaluate(vrg);
    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      assertEquals(seed * ( 4 + 5*(i+1)), lcv5.vector[i]);
    }

    //Repeating with other as nullable
    lcv4.noNulls = false;
    lcv4.isNull[0] = true;
    expr2.evaluate(vrg);
    assertTrue(lcv5.isNull[0]);
    assertFalse(lcv5.noNulls);

    //Repeating null value
    lcv3.isRepeating = true;
    lcv3.noNulls = false;
    lcv3.isNull[0] = true;
    expr2.evaluate(vrg);
    assertFalse(lcv5.noNulls);
    assertTrue(lcv5.isRepeating);
    assertTrue(lcv5.isNull[0]);
  }
}
