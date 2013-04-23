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
}
