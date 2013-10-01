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
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.util.VectorizedRowGroupGenUtil;
import org.junit.Test;

/**
 * Test vector expressions with constants.
 */
public class TestConstantVectorExpression {

  @Test
  public void testConstantExpression() {
    ConstantVectorExpression longCve = new ConstantVectorExpression(0, 17);
    ConstantVectorExpression doubleCve = new ConstantVectorExpression(1, 17.34);
    ConstantVectorExpression bytesCve = new ConstantVectorExpression(2, "alpha".getBytes());

    int size = 20;
    VectorizedRowBatch vrg = VectorizedRowGroupGenUtil.getVectorizedRowBatch(size, 3, 0);

    LongColumnVector lcv = (LongColumnVector) vrg.cols[0];
    DoubleColumnVector dcv = new DoubleColumnVector(size);
    BytesColumnVector bcv = new BytesColumnVector(size);
    vrg.cols[1] = dcv;
    vrg.cols[2] = bcv;

    longCve.evaluate(vrg);
    doubleCve.evaluate(vrg);
    bytesCve.evaluate(vrg);

    assertTrue(lcv.isRepeating);
    assertTrue(dcv.isRepeating);
    assertTrue(bcv.isRepeating);
    assertEquals(17, lcv.vector[0]);
    assertTrue(17.34 == dcv.vector[0]);
    assertTrue(Arrays.equals("alpha".getBytes(), bcv.vector[0]));
  }

}
