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

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColUnaryMinus;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColUnaryMinusChecked;
import org.apache.hadoop.hive.ql.exec.vector.util.VectorizedRowGroupGenUtil;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;

/**
 * Unit tests for unary minus.
 */
public class TestUnaryMinus {

  @Test
  public void testUnaryMinus() {
    VectorizedRowBatch vrg = VectorizedRowGroupGenUtil.getVectorizedRowBatch(1024, 2, 23);
    LongColUnaryMinus expr = new LongColUnaryMinus(0, 1);
    expr.evaluate(vrg);
    //verify
    long[] inVector = ((LongColumnVector) vrg.cols[0]).vector;
    long[] outVector = ((LongColumnVector) vrg.cols[1]).vector;
    for (int i = 0; i < outVector.length; i++) {
      assertEquals(0, inVector[i]+outVector[i]);
    }
  }


  @Test
  public void testUnaryMinusCheckedOverflow() {
    VectorizedRowBatch vrg = VectorizedRowGroupGenUtil.getVectorizedRowBatch(1, 2, 0);
    //set value to MIN_VALUE so that -MIN_VALUE overflows and gets set to MIN_VALUE again
    ((LongColumnVector)vrg.cols[0]).vector[0] = Integer.MIN_VALUE;
    LongColUnaryMinusChecked expr = new LongColUnaryMinusChecked(0, 1);
    expr.setOutputTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo("int"));
    expr.evaluate(vrg);
    //verify
    long[] inVector = ((LongColumnVector) vrg.cols[0]).vector;
    long[] outVector = ((LongColumnVector) vrg.cols[1]).vector;
    for (int i = 0; i < outVector.length; i++) {
      assertEquals(Integer.MIN_VALUE, outVector[i]);
    }
  }

  @Test
  public void testUnaryMinusChecked() {
    VectorizedRowBatch vrg = VectorizedRowGroupGenUtil.getVectorizedRowBatch(1024, 2, 23);
    LongColUnaryMinusChecked expr = new LongColUnaryMinusChecked(0, 1);
    expr.setOutputTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo("bigint"));
    expr.evaluate(vrg);
    //verify
    long[] inVector = ((LongColumnVector) vrg.cols[0]).vector;
    long[] outVector = ((LongColumnVector) vrg.cols[1]).vector;
    for (int i = 0; i < outVector.length; i++) {
      assertEquals(0, inVector[i]+outVector[i]);
    }
  }
}
