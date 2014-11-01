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

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
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
    String str = "alpha";
    ConstantVectorExpression bytesCve = new ConstantVectorExpression(2, str.getBytes());
    HiveDecimal decVal = HiveDecimal.create("25.8");
    ConstantVectorExpression decimalCve = new ConstantVectorExpression(3, decVal);
    ConstantVectorExpression nullCve = new ConstantVectorExpression(4, "string", true);
    
    int size = 20;
    VectorizedRowBatch vrg = VectorizedRowGroupGenUtil.getVectorizedRowBatch(size, 5, 0);

    LongColumnVector lcv = (LongColumnVector) vrg.cols[0];
    DoubleColumnVector dcv = new DoubleColumnVector(size);
    BytesColumnVector bcv = new BytesColumnVector(size);
    DecimalColumnVector dv = new DecimalColumnVector(5, 1);
    BytesColumnVector bcvn = new BytesColumnVector(size);
    vrg.cols[1] = dcv;
    vrg.cols[2] = bcv;
    vrg.cols[3] = dv;
    vrg.cols[4] = bcvn;

    longCve.evaluate(vrg);
    doubleCve.evaluate(vrg);
    bytesCve.evaluate(vrg);  
    decimalCve.evaluate(vrg);
    nullCve.evaluate(vrg);
    assertTrue(lcv.isRepeating);
    assertTrue(dcv.isRepeating);
    assertTrue(bcv.isRepeating);
    assertEquals(17, lcv.vector[0]);
    assertTrue(17.34 == dcv.vector[0]);
    
    assertTrue(bcvn.isRepeating);
    assertTrue(bcvn.isNull[0]);
    assertTrue(!bcvn.noNulls);
    
    byte[] alphaBytes = "alpha".getBytes();
    assertTrue(bcv.length[0] == alphaBytes.length);
    assertTrue(sameFirstKBytes(alphaBytes, bcv.vector[0], alphaBytes.length)); 
    // Evaluation of the bytes Constant Vector Expression after the vector is
    // modified. 
    ((BytesColumnVector) (vrg.cols[2])).vector[0] = "beta".getBytes();
    bytesCve.evaluate(vrg);  
    assertTrue(bcv.length[0] == alphaBytes.length);
    assertTrue(sameFirstKBytes(alphaBytes, bcv.vector[0], alphaBytes.length)); 

    assertTrue(25.8 == dv.vector[0].getHiveDecimal().doubleValue());
    // Evaluation of the decimal Constant Vector Expression after the vector is
    // modified.    
    ((DecimalColumnVector) (vrg.cols[3])).vector[0].set(HiveDecimal.create("39.7"));
    decimalCve.evaluate(vrg);
    assertTrue(25.8 == dv.vector[0].getHiveDecimal().doubleValue());    
  }
  
  private boolean sameFirstKBytes(byte[] o1, byte[] o2, int k) {
    for (int i = 0; i != k; i++) {
      if (o1[i] != o2[i]) {
        return false;
      } 
    }
    return true;
  }

}
