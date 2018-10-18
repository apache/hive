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

import junit.framework.Assert;

import org.apache.hadoop.hive.common.type.HiveDecimal;
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
    HiveDecimal d1 = HiveDecimal.create("19.56778");
    HiveDecimal expected1 = HiveDecimal.create("19");
    DecimalUtil.floor(0, d1, dcv);
    Assert.assertEquals(0, expected1.compareTo(dcv.vector[0].getHiveDecimal()));

    // As of HIVE-8745, these decimal values should be trimmed of trailing zeros.
    HiveDecimal d2 = HiveDecimal.create("23.00000");
    Assert.assertEquals(0, d2.scale());
    HiveDecimal expected2 = HiveDecimal.create("23");
    DecimalUtil.floor(0, d2, dcv);
    Assert.assertEquals(0, expected2.compareTo(dcv.vector[0].getHiveDecimal()));

    HiveDecimal d3 = HiveDecimal.create("-25.34567");
    HiveDecimal expected3 = HiveDecimal.create("-26");
    DecimalUtil.floor(0, d3, dcv);
    Assert.assertEquals(0, expected3.compareTo(dcv.vector[0].getHiveDecimal()));

    HiveDecimal d4 = HiveDecimal.create("-17.00000");
    Assert.assertEquals(0, d4.scale());
    HiveDecimal expected4 = HiveDecimal.create("-17");
    DecimalUtil.floor(0, d4, dcv);
    Assert.assertEquals(0, expected4.compareTo(dcv.vector[0].getHiveDecimal()));

    HiveDecimal d5 = HiveDecimal.create("-0.30000");
    Assert.assertEquals(1, d5.scale());
    HiveDecimal expected5 = HiveDecimal.create("-1");
    DecimalUtil.floor(0, d5, dcv);
    Assert.assertEquals(0, expected5.compareTo(dcv.vector[0].getHiveDecimal()));

    HiveDecimal d6 = HiveDecimal.create("0.30000");
    Assert.assertEquals(1, d6.scale());
    HiveDecimal expected6 = HiveDecimal.create("0");
    DecimalUtil.floor(0, d6, dcv);
    Assert.assertEquals(0, expected6.compareTo(dcv.vector[0].getHiveDecimal()));
  }

  @Test
  public void testCeiling() {
    DecimalColumnVector dcv = new DecimalColumnVector(4 ,20, 13);
    HiveDecimal d1 = HiveDecimal.create("19.56778");
    HiveDecimal expected1 = HiveDecimal.create("20");
    DecimalUtil.ceiling(0, d1, dcv);
    Assert.assertEquals(0, expected1.compareTo(dcv.vector[0].getHiveDecimal()));

    // As of HIVE-8745, these decimal values should be trimmed of trailing zeros.
    HiveDecimal d2 = HiveDecimal.create("23.00000");
    Assert.assertEquals(0, d2.scale());
    HiveDecimal expected2 = HiveDecimal.create("23");
    DecimalUtil.ceiling(0, d2, dcv);
    Assert.assertEquals(0, expected2.compareTo(dcv.vector[0].getHiveDecimal()));

    HiveDecimal d3 = HiveDecimal.create("-25.34567");
    HiveDecimal expected3 = HiveDecimal.create("-25");
    DecimalUtil.ceiling(0, d3, dcv);
    Assert.assertEquals(0, expected3.compareTo(dcv.vector[0].getHiveDecimal()));

    HiveDecimal d4 = HiveDecimal.create("-17.00000");
    Assert.assertEquals(0, d4.scale());
    HiveDecimal expected4 = HiveDecimal.create("-17");
    DecimalUtil.ceiling(0, d4, dcv);
    Assert.assertEquals(0, expected4.compareTo(dcv.vector[0].getHiveDecimal()));

    HiveDecimal d5 = HiveDecimal.create("-0.30000");
    Assert.assertEquals(1, d5.scale());
    HiveDecimal expected5 = HiveDecimal.create("0");
    DecimalUtil.ceiling(0, d5, dcv);
    Assert.assertEquals(0, expected5.compareTo(dcv.vector[0].getHiveDecimal()));

    HiveDecimal d6 = HiveDecimal.create("0.30000");
    Assert.assertEquals(1, d6.scale());
    HiveDecimal expected6 = HiveDecimal.create("1");
    DecimalUtil.ceiling(0, d6, dcv);
    Assert.assertEquals(0, expected6.compareTo(dcv.vector[0].getHiveDecimal()));
  }

  @Test
  public void testAbs() {
    DecimalColumnVector dcv = new DecimalColumnVector(4 ,20, 13);
    HiveDecimal d1 = HiveDecimal.create("19.56778");
    DecimalUtil.abs(0, d1, dcv);
    Assert.assertEquals(0, d1.compareTo(dcv.vector[0].getHiveDecimal()));

    HiveDecimal d2 = HiveDecimal.create("-25.34567");
    HiveDecimal expected2 = HiveDecimal.create("25.34567");
    DecimalUtil.abs(0, d2, dcv);
    Assert.assertEquals(0, expected2.compareTo(dcv.vector[0].getHiveDecimal()));
  }

  @Test
  public void testRound() {
    DecimalColumnVector dcv = new DecimalColumnVector(4 ,20, 0);
    HiveDecimal d1 = HiveDecimal.create("19.56778");
    HiveDecimal expected1 = HiveDecimal.create("20");
    DecimalUtil.round(0, d1, dcv);
    Assert.assertEquals(0, expected1.compareTo(dcv.vector[0].getHiveDecimal()));

    // As of HIVE-8745, these decimal values should be trimmed of trailing zeros.
    HiveDecimal d2 = HiveDecimal.create("23.00000");
    Assert.assertEquals(0, d2.scale());
    HiveDecimal expected2 = HiveDecimal.create("23");
    DecimalUtil.round(0, d2, dcv);
    Assert.assertEquals(0, expected2.compareTo(dcv.vector[0].getHiveDecimal()));

    HiveDecimal d3 = HiveDecimal.create("-25.34567");
    HiveDecimal expected3 = HiveDecimal.create("-25");
    DecimalUtil.round(0, d3, dcv);
    Assert.assertEquals(0, expected3.compareTo(dcv.vector[0].getHiveDecimal()));

    HiveDecimal d4 = HiveDecimal.create("-17.00000");
    Assert.assertEquals(0, d4.scale());
    HiveDecimal expected4 = HiveDecimal.create("-17");
    DecimalUtil.round(0, d4, dcv);
    Assert.assertEquals(0, expected4.compareTo(dcv.vector[0].getHiveDecimal()));

    HiveDecimal d5 = HiveDecimal.create("19.36778");
    HiveDecimal expected5 = HiveDecimal.create("19");
    DecimalUtil.round(0, d5, dcv);
    Assert.assertEquals(0, expected5.compareTo(dcv.vector[0].getHiveDecimal()));

    HiveDecimal d6 = HiveDecimal.create("-25.54567");
    HiveDecimal expected6 = HiveDecimal.create("-26");
    DecimalUtil.round(0, d6, dcv);
    Assert.assertEquals(0, expected6.compareTo(dcv.vector[0].getHiveDecimal()));
  }

  @Test
  public void testRoundWithDigits() {
    DecimalColumnVector dcv = new DecimalColumnVector(4 ,20, 3);
    HiveDecimal d1 = HiveDecimal.create("19.56778");
    HiveDecimal expected1 = HiveDecimal.create("19.568");
    DecimalUtil.round(0, d1, dcv);
    Assert.assertEquals(0, expected1.compareTo(dcv.vector[0].getHiveDecimal()));

    // As of HIVE-8745, these decimal values should be trimmed of trailing zeros.
    HiveDecimal d2 = HiveDecimal.create("23.56700");
    Assert.assertEquals(3, d2.scale());
    HiveDecimal expected2 = HiveDecimal.create("23.567");
    DecimalUtil.round(0, d2, dcv);
    Assert.assertEquals(0, expected2.compareTo(dcv.vector[0].getHiveDecimal()));

    HiveDecimal d3 = HiveDecimal.create("-25.34567");
    HiveDecimal expected3 = HiveDecimal.create("-25.346");
    DecimalUtil.round(0, d3, dcv);
    Assert.assertEquals(0, expected3.compareTo(dcv.vector[0].getHiveDecimal()));

    HiveDecimal d4 = HiveDecimal.create("-17.23400");
    Assert.assertEquals(3, d4.scale());
    HiveDecimal expected4 = HiveDecimal.create("-17.234");
    DecimalUtil.round(0, d4, dcv);
    Assert.assertEquals(0, expected4.compareTo(dcv.vector[0].getHiveDecimal()));

    HiveDecimal d5 = HiveDecimal.create("19.36748");
    HiveDecimal expected5 = HiveDecimal.create("19.367");
    DecimalUtil.round(0, d5, dcv);
    Assert.assertEquals(0, expected5.compareTo(dcv.vector[0].getHiveDecimal()));

    HiveDecimal d6 = HiveDecimal.create("-25.54537");
    HiveDecimal expected6 = HiveDecimal.create("-25.545");
    DecimalUtil.round(0, d6, dcv);
    Assert.assertEquals(0, expected6.compareTo(dcv.vector[0].getHiveDecimal()));
  }

  @Test
  public void testNegate() {
    DecimalColumnVector dcv = new DecimalColumnVector(4 ,20, 13);
    HiveDecimal d1 = HiveDecimal.create("19.56778");
    HiveDecimal expected1 = HiveDecimal.create("-19.56778");
    DecimalUtil.negate(0, d1, dcv);
    Assert.assertEquals(0, expected1.compareTo(dcv.vector[0].getHiveDecimal()));

    HiveDecimal d2 = HiveDecimal.create("-25.34567");
    HiveDecimal expected2 = HiveDecimal.create("25.34567");
    DecimalUtil.negate(0, d2, dcv);
    Assert.assertEquals(0, expected2.compareTo(dcv.vector[0].getHiveDecimal()));

    // As of HIVE-8745, these decimal values should be trimmed of trailing zeros.
    HiveDecimal d3 = HiveDecimal.create("0.00000");
    Assert.assertEquals(0, d3.scale());
    HiveDecimal expected3 = HiveDecimal.create("0");
    DecimalUtil.negate(0, d3, dcv);
    Assert.assertEquals(0, expected3.compareTo(dcv.vector[0].getHiveDecimal()));
  }

  @Test
  public void testSign() {
    LongColumnVector lcv = new LongColumnVector(4);
    HiveDecimal d1 = HiveDecimal.create("19.56778");
    DecimalUtil.sign(0, d1, lcv);
    Assert.assertEquals(1, lcv.vector[0]);

    HiveDecimal d2 = HiveDecimal.create("-25.34567");
    DecimalUtil.sign(0, d2, lcv);
    Assert.assertEquals(-1, lcv.vector[0]);

    HiveDecimal d3 = HiveDecimal.create("0.00000");
    Assert.assertEquals(0, d3.scale());
    DecimalUtil.sign(0, d3, lcv);
    Assert.assertEquals(0, lcv.vector[0]);
  }
}