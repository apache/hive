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

import junit.framework.Assert;
import org.apache.hadoop.hive.common.type.Decimal128;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.junit.Test;

/**
 * Unit tests for DecimalUtil.
 */
public class TestDecimalUtil {

  @Test
  public void testFloor() {
    DecimalColumnVector dcv = new DecimalColumnVector(4 ,20, 13);
    Decimal128 d1 = new Decimal128(19.56778, (short) 5);
    Decimal128 expected1 = new Decimal128(19, (short)0);
    DecimalUtil.floor(0, d1, dcv);
    Assert.assertEquals(0, expected1.compareTo(dcv.vector[0]));

    Decimal128 d2 = new Decimal128(23.0, (short) 5);
    Decimal128 expected2 = new Decimal128(23, (short)0);
    DecimalUtil.floor(0, d2, dcv);
    Assert.assertEquals(0, expected2.compareTo(dcv.vector[0]));

    Decimal128 d3 = new Decimal128(-25.34567, (short) 5);
    Decimal128 expected3 = new Decimal128(-26, (short)0);
    DecimalUtil.floor(0, d3, dcv);
    Assert.assertEquals(0, expected3.compareTo(dcv.vector[0]));

    Decimal128 d4 = new Decimal128(-17, (short) 5);
    Decimal128 expected4 = new Decimal128(-17, (short)0);
    DecimalUtil.floor(0, d4, dcv);
    Assert.assertEquals(0, expected4.compareTo(dcv.vector[0]));

    Decimal128 d5 = new Decimal128(-0.3, (short) 5);
    Decimal128 expected5 = new Decimal128(-1, (short)0);
    DecimalUtil.floor(0, d5, dcv);
    Assert.assertEquals(0, expected5.compareTo(dcv.vector[0]));

    Decimal128 d6 = new Decimal128(0.3, (short) 5);
    Decimal128 expected6 = new Decimal128(0, (short)0);
    DecimalUtil.floor(0, d6, dcv);
    Assert.assertEquals(0, expected6.compareTo(dcv.vector[0]));
  }

  @Test
  public void testCeiling() {
    DecimalColumnVector dcv = new DecimalColumnVector(4 ,20, 13);
    Decimal128 d1 = new Decimal128(19.56778, (short) 5);
    Decimal128 expected1 = new Decimal128(20, (short)0);
    DecimalUtil.ceiling(0, d1, dcv);
    Assert.assertEquals(0, expected1.compareTo(dcv.vector[0]));

    Decimal128 d2 = new Decimal128(23.0, (short) 5);
    Decimal128 expected2 = new Decimal128(23, (short)0);
    DecimalUtil.ceiling(0, d2, dcv);
    Assert.assertEquals(0, expected2.compareTo(dcv.vector[0]));

    Decimal128 d3 = new Decimal128(-25.34567, (short) 5);
    Decimal128 expected3 = new Decimal128(-25, (short)0);
    DecimalUtil.ceiling(0, d3, dcv);
    Assert.assertEquals(0, expected3.compareTo(dcv.vector[0]));

    Decimal128 d4 = new Decimal128(-17, (short) 5);
    Decimal128 expected4 = new Decimal128(-17, (short)0);
    DecimalUtil.ceiling(0, d4, dcv);
    Assert.assertEquals(0, expected4.compareTo(dcv.vector[0]));

    Decimal128 d5 = new Decimal128(-0.3, (short) 5);
    Decimal128 expected5 = new Decimal128(0, (short)0);
    DecimalUtil.ceiling(0, d5, dcv);
    Assert.assertEquals(0, expected5.compareTo(dcv.vector[0]));

    Decimal128 d6 = new Decimal128(0.3, (short) 5);
    Decimal128 expected6 = new Decimal128(1, (short)0);
    DecimalUtil.ceiling(0, d6, dcv);
    Assert.assertEquals(0, expected6.compareTo(dcv.vector[0]));
  }

  @Test
  public void testAbs() {
    DecimalColumnVector dcv = new DecimalColumnVector(4 ,20, 13);
    Decimal128 d1 = new Decimal128(19.56778, (short) 5);
    DecimalUtil.abs(0, d1, dcv);
    Assert.assertEquals(0, d1.compareTo(dcv.vector[0]));

    Decimal128 d2 = new Decimal128(-25.34567, (short) 5);
    Decimal128 expected2 = new Decimal128(25.34567, (short)5);
    DecimalUtil.abs(0, d2, dcv);
    Assert.assertEquals(0, expected2.compareTo(dcv.vector[0]));
  }

  @Test
  public void testRound() {
    DecimalColumnVector dcv = new DecimalColumnVector(4 ,20, 0);
    Decimal128 d1 = new Decimal128(19.56778, (short) 5);
    Decimal128 expected1 = new Decimal128(20, (short)0);
    DecimalUtil.round(0, d1, dcv);
    Assert.assertEquals(0, expected1.compareTo(dcv.vector[0]));

    Decimal128 d2 = new Decimal128(23.0, (short) 5);
    Decimal128 expected2 = new Decimal128(23, (short)0);
    DecimalUtil.round(0, d2, dcv);
    Assert.assertEquals(0, expected2.compareTo(dcv.vector[0]));

    Decimal128 d3 = new Decimal128(-25.34567, (short) 5);
    Decimal128 expected3 = new Decimal128(-25, (short)0);
    DecimalUtil.round(0, d3, dcv);
    Assert.assertEquals(0, expected3.compareTo(dcv.vector[0]));

    Decimal128 d4 = new Decimal128(-17, (short) 5);
    Decimal128 expected4 = new Decimal128(-17, (short)0);
    DecimalUtil.round(0, d4, dcv);
    Assert.assertEquals(0, expected4.compareTo(dcv.vector[0]));

    Decimal128 d5 = new Decimal128(19.36778, (short) 5);
    Decimal128 expected5 = new Decimal128(19, (short)0);
    DecimalUtil.round(0, d5, dcv);
    Assert.assertEquals(0, expected5.compareTo(dcv.vector[0]));

    Decimal128 d6 = new Decimal128(-25.54567, (short) 5);
    Decimal128 expected6 = new Decimal128(-26, (short)0);
    DecimalUtil.round(0, d6, dcv);
    Assert.assertEquals(0, expected6.compareTo(dcv.vector[0]));
  }

  @Test
  public void testRoundWithDigits() {
    DecimalColumnVector dcv = new DecimalColumnVector(4 ,20, 3);
    Decimal128 d1 = new Decimal128(19.56778, (short) 5);
    Decimal128 expected1 = new Decimal128(19.568, (short)3);
    DecimalUtil.round(0, d1, dcv);
    Assert.assertEquals(0, expected1.compareTo(dcv.vector[0]));

    Decimal128 d2 = new Decimal128(23.567, (short) 5);
    Decimal128 expected2 = new Decimal128(23.567, (short)3);
    DecimalUtil.round(0, d2, dcv);
    Assert.assertEquals(0, expected2.compareTo(dcv.vector[0]));

    Decimal128 d3 = new Decimal128(-25.34567, (short) 5);
    Decimal128 expected3 = new Decimal128(-25.346, (short)3);
    DecimalUtil.round(0, d3, dcv);
    Assert.assertEquals(0, expected3.compareTo(dcv.vector[0]));

    Decimal128 d4 = new Decimal128(-17.234, (short) 5);
    Decimal128 expected4 = new Decimal128(-17.234, (short)3);
    DecimalUtil.round(0, d4, dcv);
    Assert.assertEquals(0, expected4.compareTo(dcv.vector[0]));

    Decimal128 d5 = new Decimal128(19.36748, (short) 5);
    Decimal128 expected5 = new Decimal128(19.367, (short)3);
    DecimalUtil.round(0, d5, dcv);
    Assert.assertEquals(0, expected5.compareTo(dcv.vector[0]));

    Decimal128 d6 = new Decimal128(-25.54537, (short) 5);
    Decimal128 expected6 = new Decimal128(-25.545, (short)3);
    DecimalUtil.round(0, d6, dcv);
    Assert.assertEquals(0, expected6.compareTo(dcv.vector[0]));
  }

  @Test
  public void testNegate() {
    DecimalColumnVector dcv = new DecimalColumnVector(4 ,20, 13);
    Decimal128 d1 = new Decimal128(19.56778, (short) 5);
    Decimal128 expected1 = new Decimal128(-19.56778, (short)5);
    DecimalUtil.negate(0, d1, dcv);
    Assert.assertEquals(0, expected1.compareTo(dcv.vector[0]));

    Decimal128 d2 = new Decimal128(-25.34567, (short) 5);
    Decimal128 expected2 = new Decimal128(25.34567, (short)5);
    DecimalUtil.negate(0, d2, dcv);
    Assert.assertEquals(0, expected2.compareTo(dcv.vector[0]));

    Decimal128 d3 = new Decimal128(0, (short) 5);
    Decimal128 expected3 = new Decimal128(0, (short)0);
    DecimalUtil.negate(0, d3, dcv);
    Assert.assertEquals(0, expected3.compareTo(dcv.vector[0]));
  }

  @Test
  public void testSign() {
    LongColumnVector lcv = new LongColumnVector(4);
    Decimal128 d1 = new Decimal128(19.56778, (short) 5);
    DecimalUtil.sign(0, d1, lcv);
    Assert.assertEquals(1, lcv.vector[0]);

    Decimal128 d2 = new Decimal128(-25.34567, (short) 5);
    DecimalUtil.sign(0, d2, lcv);
    Assert.assertEquals(-1, lcv.vector[0]);

    Decimal128 d3 = new Decimal128(0, (short) 5);
    DecimalUtil.sign(0, d3, lcv);
    Assert.assertEquals(0, lcv.vector[0]);
  }
}