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
package org.apache.hadoop.hive.common.type;

import java.sql.Timestamp;
import java.util.Random;
import java.util.Arrays;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.hadoop.hive.serde2.io.HiveDecimalWritableV1;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.common.type.RandomTypeUtil;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.util.TimestampUtils;

import org.junit.*;

import static org.junit.Assert.*;

public class TestHiveDecimal extends HiveDecimalTestBase {

  @Test
  public void testInvalidStringInput() {

    HiveDecimalV1 oldDec;
    HiveDecimalV1 resultOldDec;
    HiveDecimal dec;
    HiveDecimal resultDec;

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create("-");
    Assert.assertTrue(oldDec == null);
    //---------------------------------------------------
    dec = HiveDecimal.create("-");
    Assert.assertTrue(dec == null);

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create("+");
    Assert.assertTrue(oldDec == null);
    //---------------------------------------------------
    dec = HiveDecimal.create("+");
    Assert.assertTrue(dec == null);

    // Naked dot.
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(".");
    Assert.assertTrue(oldDec == null);
    //---------------------------------------------------
    dec = HiveDecimal.create(".");
    Assert.assertTrue(dec == null);

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create("-.");
    Assert.assertTrue(oldDec == null);
    //---------------------------------------------------
    dec = HiveDecimal.create("-.");
    Assert.assertTrue(dec == null);

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create("+.");
    Assert.assertTrue(oldDec == null);
    //---------------------------------------------------
    dec = HiveDecimal.create("+.");
    Assert.assertTrue(dec == null);

    // Naked E/e.
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create("E");
    Assert.assertTrue(oldDec == null);
    //---------------------------------------------------
    dec = HiveDecimal.create("E");
    Assert.assertTrue(dec == null);

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(".E");
    Assert.assertTrue(oldDec == null);
    //---------------------------------------------------
    dec = HiveDecimal.create(".E");
    Assert.assertTrue(dec == null);

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create("-E");
    Assert.assertTrue(oldDec == null);
    //---------------------------------------------------
    dec = HiveDecimal.create("-E");
    Assert.assertTrue(dec == null);

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create("+E");
    Assert.assertTrue(oldDec == null);
    //---------------------------------------------------
    dec = HiveDecimal.create("+E");
    Assert.assertTrue(dec == null);
 
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create("e");
    Assert.assertTrue(oldDec == null);
    //---------------------------------------------------
    dec = HiveDecimal.create("e");
    Assert.assertTrue(dec == null);

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(".e");
    Assert.assertTrue(oldDec == null);
    //---------------------------------------------------
    dec = HiveDecimal.create(".e");
    Assert.assertTrue(dec == null);

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create("-e");
    Assert.assertTrue(oldDec == null);
    //---------------------------------------------------
    dec = HiveDecimal.create("-e");
    Assert.assertTrue(dec == null);

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create("+e");
    Assert.assertTrue(oldDec == null);
    //---------------------------------------------------
    dec = HiveDecimal.create("+e");
    Assert.assertTrue(dec == null);

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create("error");
    Assert.assertTrue(oldDec == null);
    //---------------------------------------------------
    dec = HiveDecimal.create("error");
    Assert.assertTrue(dec == null);

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create("0x0");
    Assert.assertTrue(oldDec == null);
    //---------------------------------------------------
    dec = HiveDecimal.create("0x0");
    Assert.assertTrue(dec == null);

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create("0e");
    Assert.assertTrue(oldDec == null);
    //---------------------------------------------------
    dec = HiveDecimal.create("0e");
    Assert.assertTrue(dec == null);

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create("7e");
    Assert.assertTrue(oldDec == null);
    //---------------------------------------------------
    dec = HiveDecimal.create("7e");
    Assert.assertTrue(dec == null);

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create("233e-");
    Assert.assertTrue(oldDec == null);
    //---------------------------------------------------
    dec = HiveDecimal.create("233e-");
    Assert.assertTrue(dec == null);

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create("32e+");
    Assert.assertTrue(oldDec == null);
    //---------------------------------------------------
    dec = HiveDecimal.create("32e+");
    Assert.assertTrue(dec == null);

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(".0e");
    Assert.assertTrue(oldDec == null);
    //---------------------------------------------------
    dec = HiveDecimal.create(".0e");
    Assert.assertTrue(dec == null);

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(".4e");
    Assert.assertTrue(oldDec == null);
    //---------------------------------------------------
    dec = HiveDecimal.create(".4e");
    Assert.assertTrue(dec == null);
  }

  @Test
  public void testVariousCases() {

    HiveDecimalV1 oldDec;
    HiveDecimalV1 resultOldDec;
    HiveDecimal dec;
    HiveDecimal resultDec;

    BigDecimal bigDecimal = new BigDecimal("-99999999999999999999999999999999999999.99999999999999999");

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(bigDecimal);
    Assert.assertEquals("-100000000000000000000000000000000000000", oldDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(bigDecimal);
    Assert.assertTrue(dec == null);

    // One less integer digit...
    bigDecimal = new BigDecimal("-9999999999999999999999999999999999999.99999999999999999");

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(bigDecimal);
    Assert.assertEquals("-10000000000000000000000000000000000000", oldDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(bigDecimal);
    Assert.assertEquals("-10000000000000000000000000000000000000", dec.toString());

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create("101");
    resultOldDec = HiveDecimalV1.enforcePrecisionScale(oldDec, 10, 0);
    Assert.assertEquals("101", resultOldDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create("101");
    resultDec = HiveDecimal.enforcePrecisionScale(dec, 10, 0);
    Assert.assertEquals("101", resultDec.toString());

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create("1");
    resultOldDec = oldDec.scaleByPowerOfTen(-99);
    Assert.assertEquals("0", resultOldDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create("1");
    resultDec = dec.scaleByPowerOfTen(-99);
    Assert.assertEquals("0", resultDec.toString());
  }

  @Test
  public void testCreateFromBigIntegerRounding() {

    BigInteger bigInt;
    HiveDecimalV1 oldDec;
    HiveDecimal dec;

    // 1786135888657847525803324040144343378.09799306448796128931113691624
    bigInt = new BigInteger(
                        "178613588865784752580332404014434337809799306448796128931113691624");
    Assert.assertEquals("178613588865784752580332404014434337809799306448796128931113691624", bigInt.toString());
    //                   12345678901234567890123456789012345678
    //                            1         2         3
    //                                                        12345678901234567890123456789
    dec = HiveDecimal.create(bigInt, 29);
    Assert.assertEquals("1786135888657847525803324040144343378.1", dec.toString());

    // 8.090000000000000000000000000000000000000123456
    bigInt = new BigInteger(
                        "8090000000000000000000000000000000000000123456");
    //                   123456789012345678901234567890123456789012345
    //                             1         2         3         4
    Assert.assertEquals("8090000000000000000000000000000000000000123456", bigInt.toString());
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(bigInt, 45);
    Assert.assertEquals("8.09", oldDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(bigInt, 45);
    Assert.assertEquals("8.09", dec.toString());

    // 99999999.99999999999999999999999999999949999
    // MAX_DECIMAL 9's WITH NO ROUND (longer than 38 digits)
    bigInt = new BigInteger(
                        "9999999999999999999999999999999999999949999");
    //                   12345678901234567890123456789012345678
    //                             1         2         3
    //                   99999999.99999999999999999999999999999949999
    Assert.assertEquals("9999999999999999999999999999999999999949999", bigInt.toString());
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(bigInt, 35);
    Assert.assertEquals("99999999.999999999999999999999999999999", oldDec.toString());
    //---------------------------------------------------
    // Without the round, this conversion fails.
    dec = HiveDecimal.create(bigInt, 35);
    Assert.assertEquals("99999999.999999999999999999999999999999", dec.toString());

    // MAX_DECIMAL 9's WITH ROUND.
    bigInt = new BigInteger(
                        "9999999999999999999999999999999999999979999");
    //                   12346678.901234667890123466789012346678
    //                             1         2         3
    Assert.assertEquals("9999999999999999999999999999999999999979999", bigInt.toString());
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(bigInt, 35);
    Assert.assertEquals("100000000", oldDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(bigInt, 35);
    Assert.assertEquals("100000000", dec.toString());
  }

  @Test
  public void testCreateFromBigDecimal() {

    BigDecimal bigDec;
    HiveDecimalV1 oldDec;
    HiveDecimal dec;

    bigDec = new BigDecimal("0");
    Assert.assertEquals("0", bigDec.toString());
    dec = HiveDecimal.create(bigDec);
    Assert.assertEquals("0", dec.toString());

    bigDec = new BigDecimal("1");
    Assert.assertEquals("1", bigDec.toString());
    dec = HiveDecimal.create(bigDec);
    Assert.assertEquals("1", dec.toString());

    bigDec = new BigDecimal("0.999");
    Assert.assertEquals("0.999", bigDec.toString());
    dec = HiveDecimal.create(bigDec);
    Assert.assertEquals("0.999", dec.toString());

    // HiveDecimal suppresses trailing zeroes.
    bigDec = new BigDecimal("0.9990");
    Assert.assertEquals("0.9990", bigDec.toString());
    dec = HiveDecimal.create(bigDec);
    Assert.assertEquals("0.999", dec.toString());
  }

  @Test
  public void testCreateFromBigDecimalRounding() {

    BigDecimal bigDec;
    HiveDecimalV1 oldDec;
    HiveDecimal dec;

    bigDec = new BigDecimal(
                        "1786135888657847525803324040144343378.09799306448796128931113691624");
    Assert.assertEquals("1786135888657847525803324040144343378.09799306448796128931113691624", bigDec.toString());
    //                   1234567890123456789012345678901234567.8
    //                            1         2         3
    // Without the round, this conversion fails.
    dec = HiveDecimal.create(bigDec, false);
    Assert.assertTrue(dec == null);
    dec = HiveDecimal.create(bigDec, true);
    Assert.assertEquals("1786135888657847525803324040144343378.1", dec.toString());

    bigDec = new BigDecimal(
                        "8.090000000000000000000000000000000000000123456");
    //                   1.23456789012345678901234567890123456789012345
    //                             1         2         3         4
    Assert.assertEquals("8.090000000000000000000000000000000000000123456", bigDec.toString());
    //---------------------------------------------------
    HiveDecimalV1 oldDec4 = HiveDecimalV1.create(bigDec, false);
    Assert.assertTrue(oldDec4 == null);
    oldDec4 = HiveDecimalV1.create(bigDec, true);
    Assert.assertEquals("8.09", oldDec4.toString());
    //---------------------------------------------------
    // Without the round, this conversion fails.
    dec = HiveDecimal.create(bigDec, false);
    Assert.assertTrue(dec == null);
    dec = HiveDecimal.create(bigDec, true);
    Assert.assertEquals("8.09", dec.toString());

    // MAX_DECIMAL 9's WITH NO ROUND (longer than 38 digits)
    bigDec = new BigDecimal(
                        "99999999.99999999999999999999999999999949999");
    //                   12345678.901234567890123456789012345678
    //                             1         2         3
    Assert.assertEquals("99999999.99999999999999999999999999999949999", bigDec.toString());
    //---------------------------------------------------
    HiveDecimalV1 oldDec5 = HiveDecimalV1.create(bigDec, false);
    Assert.assertTrue(oldDec5 == null);
    oldDec5 = HiveDecimalV1.create(bigDec, true);
    Assert.assertEquals("99999999.999999999999999999999999999999", oldDec5.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(bigDec, false);
    Assert.assertTrue(dec == null);
    dec = HiveDecimal.create(bigDec, true);
    Assert.assertEquals("99999999.999999999999999999999999999999", dec.toString());

    // MAX_DECIMAL 9's WITH ROUND.
    bigDec = new BigDecimal(
                        "99999999.99999999999999999999999999999979999");
    //                   12346678.901234667890123466789012346678
    //                             1         2         3
    Assert.assertEquals("99999999.99999999999999999999999999999979999", bigDec.toString());
    //---------------------------------------------------
    HiveDecimalV1 oldDec6 = HiveDecimalV1.create(bigDec, false);
    Assert.assertTrue(oldDec6 == null);
    oldDec6 = HiveDecimalV1.create(bigDec, true);
    Assert.assertEquals("100000000", oldDec6.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(bigDec, false);
    Assert.assertTrue(dec == null);
    dec = HiveDecimal.create(bigDec, true);
    Assert.assertEquals("100000000", dec.toString());
  }


  @Test
  public void testPrecisionScaleEnforcement() {

    HiveDecimalV1 oldDec;
    HiveDecimalV1 oldResultDec;

    HiveDecimal dec;
    HiveDecimal resultDec;

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create("0.02538461538461538461538461538461538462");
    Assert.assertEquals("0.02538461538461538461538",
        HiveDecimalV1.enforcePrecisionScale(oldDec, 38, 23).toString());
    //---------------------------------------------------
    dec = HiveDecimal.create("0.02538461538461538461538461538461538462");
    Assert.assertEquals("0.02538461538461538461538",
        HiveDecimal.enforcePrecisionScale(dec, 38, 23).toString());

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create("005.34000");
    Assert.assertEquals(oldDec.precision(), 3);  // 1 integer digit; 2 fraction digits.
    Assert.assertEquals(oldDec.scale(), 2);      // Trailing zeroes are suppressed.
    //---------------------------------------------------
    dec = HiveDecimal.create("005.34000");
    Assert.assertEquals(dec.precision(), 3);  // 1 integer digit; 2 fraction digits.
    Assert.assertEquals(dec.scale(), 2);      // Trailing zeroes are suppressed.

    dec = HiveDecimal.create("178613588865784752580332404014434337809799306448796128931113691624");
    Assert.assertNull(dec);

    // Rounding numbers that increase int digits
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create("9.5");
    Assert.assertEquals("10",
        HiveDecimalV1.enforcePrecisionScale(oldDec, 2, 0).toString());
    Assert.assertNull(
        HiveDecimalV1.enforcePrecisionScale(oldDec, 1, 0));
    oldDec = HiveDecimalV1.create("9.4");
    Assert.assertEquals("9",
        HiveDecimalV1.enforcePrecisionScale(oldDec, 1, 0).toString());
    //---------------------------------------------------
    dec = HiveDecimal.create("9.5");
    Assert.assertEquals("10",
        HiveDecimal.enforcePrecisionScale(dec, 2, 0).toString());
    Assert.assertNull(
        HiveDecimal.enforcePrecisionScale(dec, 1, 0));
    dec = HiveDecimal.create("9.4");
    Assert.assertEquals("9",
        HiveDecimal.enforcePrecisionScale(dec, 1, 0).toString());
  }

  @Test
  public void testPrecisionScaleEnforcementEdgeCond() {

    // Since HiveDecimal now uses FastHiveDecimal which stores 16 decimal digits per long,
    // lets test edge conditions here.

    HiveDecimal fifteenFractionalNinesDec = HiveDecimal.create("0.999999999999999");
    Assert.assertNotNull(fifteenFractionalNinesDec);
    Assert.assertEquals("0.999999999999999",
        HiveDecimal.enforcePrecisionScale(fifteenFractionalNinesDec, 15, 15).toString());

    HiveDecimal sixteenFractionalNines = HiveDecimal.create("0.9999999999999999");
    Assert.assertNotNull(sixteenFractionalNines);
    Assert.assertEquals("0.9999999999999999",
        HiveDecimal.enforcePrecisionScale(sixteenFractionalNines, 16, 16).toString());

    HiveDecimal seventeenFractionalNines = HiveDecimal.create("0.99999999999999999");
    Assert.assertNotNull(seventeenFractionalNines);
    Assert.assertEquals("0.99999999999999999",
        HiveDecimal.enforcePrecisionScale(seventeenFractionalNines, 17, 17).toString());

  }

  @Test
  public void testTrailingZeroRemovalAfterEnforcement() {
    String decStr = "8.090000000000000000000000000000000000000123456";
    //                 123456789012345678901234567890123456789012345
    //                          1         2         3         4
    HiveDecimal dec = HiveDecimal.create(decStr);
    Assert.assertEquals("8.09", dec.toString());
  }

  @Test
  public void testMultiply() {

    // This multiply produces more than 38 digits --> overflow.
    //---------------------------------------------------
    HiveDecimalV1 oldDec1 = HiveDecimalV1.create("0.00001786135888657847525803");
    HiveDecimalV1 oldDec2 = HiveDecimalV1.create("3.0000123456789");
    HiveDecimalV1 oldResult = oldDec1.multiply(oldDec2);
    Assert.assertTrue(oldResult == null);
    //---------------------------------------------------
    HiveDecimal dec1 = HiveDecimal.create("0.00001786135888657847525803");
    HiveDecimal dec2 = HiveDecimal.create("3.0000123456789");
    HiveDecimal result = dec1.multiply(dec2);
    Assert.assertTrue(result == null);

    dec1 = HiveDecimal.create("178613588865784752580323232232323444.4");
    dec2 = HiveDecimal.create("178613588865784752580302323232.3");
    Assert.assertNull(dec1.multiply(dec2));   // i.e. Overflow.

    dec1 = HiveDecimal.create("47.324");
    dec2 = HiveDecimal.create("9232.309");
    Assert.assertEquals("436909.791116", dec1.multiply(dec2).toString());

    dec1 = HiveDecimal.create("3.140");
    dec2 = HiveDecimal.create("1.00");
    Assert.assertEquals("3.14", dec1.multiply(dec2).toString());

    dec1 = HiveDecimal.create("43.010");
    dec2 = HiveDecimal.create("2");
    Assert.assertEquals("86.02", dec1.multiply(dec2).toString());
  }

  @Test
  public void testMultiply2() {
    // 0.09765625BD * 0.09765625BD * 0.0125BD * 578992BD
    HiveDecimal dec1 = HiveDecimal.create("0.09765625");
    HiveDecimal dec2 = HiveDecimal.create("0.09765625");
    HiveDecimal dec3 = HiveDecimal.create("0.0125");
    HiveDecimal dec4 = HiveDecimal.create("578992");
    HiveDecimal result1 = dec1.multiply(dec2);
    Assert.assertNotNull(result1);
    HiveDecimal result2 = result1.multiply(dec3);
    Assert.assertNotNull(result2);
    HiveDecimal result = result2.multiply(dec4);
    Assert.assertNotNull(result);
    Assert.assertEquals("69.0212249755859375", result.toString());
  }

  @Test
  public void testPow() {

    HiveDecimal dec;

    dec = HiveDecimal.create("3.00001415926");
    HiveDecimal decPow2 = dec.pow(2);
    HiveDecimal decMultiplyTwice = dec.multiply(dec);
    Assert.assertEquals(decPow2, decMultiplyTwice);

    dec = HiveDecimal.create("0.000017861358882");
    dec = dec.pow(3);
    Assert.assertNull(dec);

    dec = HiveDecimal.create("3.140");
    Assert.assertEquals("9.8596", dec.pow(2).toString());
  }

  @Test
  public void testScaleByPowerOfTen() {

    HiveDecimalV1 oldDec;
    HiveDecimal dec;
    HiveDecimalV1 oldResultDec;
    HiveDecimal resultDec;

    //**********************************************************************************************

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(
        "1");
    Assert.assertEquals(0, oldDec.scale());
    oldResultDec = oldDec.scaleByPowerOfTen(2);
    Assert.assertEquals(
        "100", oldResultDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(
        "1");
    Assert.assertEquals(0, dec.scale());
    // resultDec = dec.scaleByPowerOfTen(2);
    // Assert.assertEquals(
    //     "100", resultDec.toString());

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(
        "0.00000000000000000000000000000000000001");
    Assert.assertEquals(38, oldDec.scale());
    oldResultDec = oldDec.scaleByPowerOfTen(2);
    Assert.assertEquals(
        "0.000000000000000000000000000000000001", oldResultDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(
        "0.00000000000000000000000000000000000001");
    Assert.assertEquals(38, dec.scale());
    resultDec = dec.scaleByPowerOfTen(2);
    Assert.assertEquals(
        "0.000000000000000000000000000000000001", resultDec.toString());

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(
        "0.00000000000000000000000000000000000001");
    Assert.assertEquals(38, oldDec.scale());
    oldResultDec = oldDec.scaleByPowerOfTen(38);
    Assert.assertEquals(
        "1", oldResultDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(
        "0.00000000000000000000000000000000000001");
    Assert.assertEquals(38, dec.scale());
    resultDec = dec.scaleByPowerOfTen(38);
    Assert.assertEquals(
        "1", resultDec.toString());

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(
        "0.00000000000000000000000000000000000001");
    Assert.assertEquals(38, oldDec.scale());
    oldResultDec = oldDec.scaleByPowerOfTen(2 * 38 - 1);
    Assert.assertEquals(
        "10000000000000000000000000000000000000", oldResultDec.toString());
    Assert.assertEquals(0, oldResultDec.scale());
    //---------------------------------------------------
    dec = HiveDecimal.create(
        "0.00000000000000000000000000000000000001");
    Assert.assertEquals(38, dec.scale());
    resultDec = dec.scaleByPowerOfTen(2 * 38 - 1);
    Assert.assertEquals(
        "10000000000000000000000000000000000000", resultDec.toString());
    Assert.assertEquals(0, resultDec.scale());

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(
        "0.00000000000000000000000000000000000001");
    Assert.assertEquals(38, oldDec.scale());
    oldResultDec = oldDec.scaleByPowerOfTen(2 * 38);
    Assert.assertTrue(oldResultDec == null);
    //---------------------------------------------------
    dec = HiveDecimal.create(
        "0.00000000000000000000000000000000000001");
    Assert.assertEquals(38, dec.scale());
    resultDec = dec.scaleByPowerOfTen(2 * 38);
    Assert.assertTrue(resultDec == null);


    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(
        "0.00000000000000000000000000000000000022");
    Assert.assertEquals(38, oldDec.scale());
    oldResultDec = oldDec.scaleByPowerOfTen(38);
    Assert.assertEquals(
        "22", oldResultDec.toString());
    Assert.assertEquals(0, oldResultDec.scale());
    //---------------------------------------------------
    dec = HiveDecimal.create(
        "0.00000000000000000000000000000000000022");
    Assert.assertEquals(38, dec.scale());
    resultDec = dec.scaleByPowerOfTen(38);
    Assert.assertEquals(
        "22", resultDec.toString());
    Assert.assertEquals(0, resultDec.scale());

    //---------------------------------------------------
    oldDec = HiveDecimalV1.create("3.00001415926");
    Assert.assertEquals(11, oldDec.scale());
    oldResultDec = oldDec.scaleByPowerOfTen(2);
    Assert.assertEquals("300.001415926", oldResultDec.toString());
    Assert.assertEquals(9, oldResultDec.scale());
    oldResultDec = oldDec.scaleByPowerOfTen(5);
    Assert.assertEquals("300001.415926", oldResultDec.toString());
    Assert.assertEquals(6, oldResultDec.scale());
    oldResultDec = oldDec.scaleByPowerOfTen(18);
    Assert.assertEquals("3000014159260000000", oldResultDec.toString());
    Assert.assertEquals(0, oldResultDec.scale());
    oldResultDec = oldDec.scaleByPowerOfTen(35);
    Assert.assertEquals("300001415926000000000000000000000000", oldResultDec.toString());
    Assert.assertEquals(0, oldResultDec.scale());
    oldResultDec = oldDec.scaleByPowerOfTen(37);
    Assert.assertEquals("30000141592600000000000000000000000000", oldResultDec.toString());
    Assert.assertEquals(0, oldResultDec.scale());
    //---------------------------------------------------
    dec = HiveDecimal.create("3.00001415926");
    Assert.assertEquals(11, dec.scale());
    Assert.assertEquals(1, dec.integerDigitCount());
    resultDec = dec.scaleByPowerOfTen(2);
    Assert.assertEquals("300.001415926", resultDec.toString());
    Assert.assertEquals(9, resultDec.scale());
    Assert.assertEquals(3, resultDec.integerDigitCount());
    resultDec = dec.scaleByPowerOfTen(5);
    Assert.assertEquals("300001.415926", resultDec.toString());
    Assert.assertEquals(6, resultDec.scale());
    Assert.assertEquals(6, resultDec.integerDigitCount());
    resultDec = dec.scaleByPowerOfTen(18);
    Assert.assertEquals("3000014159260000000", resultDec.toString());
    Assert.assertEquals(0, resultDec.scale());
    Assert.assertEquals(19, resultDec.integerDigitCount());
    resultDec = dec.scaleByPowerOfTen(35);
    Assert.assertEquals("300001415926000000000000000000000000", resultDec.toString());
    Assert.assertEquals(0, resultDec.scale());
    Assert.assertEquals(36, resultDec.integerDigitCount());
    resultDec = dec.scaleByPowerOfTen(37);
    Assert.assertEquals("30000141592600000000000000000000000000", resultDec.toString());
    Assert.assertEquals(0, resultDec.scale());
    Assert.assertEquals(38, resultDec.integerDigitCount());
  }

  @Test
  public void testSingleWordDivision() {
 
    HiveDecimalV1 oldDec1;
    HiveDecimalV1 oldDec2;
    HiveDecimalV1 oldResultDec;

    HiveDecimal dec1;
    HiveDecimal dec2;
    HiveDecimal resultDec;

    //---------------------------------------------------
    oldDec1 = HiveDecimalV1.create("839293");
    oldDec2 = HiveDecimalV1.create("8");
    oldResultDec = oldDec1.divide(oldDec2);
    Assert.assertEquals("104911.625", oldResultDec.toString());
    //---------------------------------------------------
    dec1 = HiveDecimal.create("839293");
    dec2 = HiveDecimal.create("8");
    resultDec = dec1.divide(dec2);
    Assert.assertEquals("104911.625", resultDec.toString());  // UNDONE

    //---------------------------------------------------
    oldDec1 = HiveDecimalV1.create("1");
    oldDec2 = HiveDecimalV1.create("3");
    oldResultDec = oldDec1.divide(oldDec2);
    Assert.assertEquals("0.33333333333333333333333333333333333333", oldResultDec.toString());
    //---------------------------------------------------
    dec1 = HiveDecimal.create("1");
    dec2 = HiveDecimal.create("3");
    resultDec = dec1.divide(dec2);
    Assert.assertEquals("0.33333333333333333333333333333333333333", resultDec.toString());  // UNDONE

    //---------------------------------------------------
    oldDec1 = HiveDecimalV1.create("1");
    oldDec2 = HiveDecimalV1.create("9");
    oldResultDec = oldDec1.divide(oldDec2);
    Assert.assertEquals("0.11111111111111111111111111111111111111", oldResultDec.toString());
    //---------------------------------------------------
    dec1 = HiveDecimal.create("1");
    dec2 = HiveDecimal.create("9");
    resultDec = dec1.divide(dec2);
    Assert.assertEquals("0.11111111111111111111111111111111111111", resultDec.toString());  // UNDONE

    //---------------------------------------------------
    oldDec1 = HiveDecimalV1.create("22");
    oldDec2 = HiveDecimalV1.create("7");
    oldResultDec = oldDec1.divide(oldDec2);
    Assert.assertEquals("3.1428571428571428571428571428571428571", oldResultDec.toString());
    //---------------------------------------------------
    dec1 = HiveDecimal.create("22");
    dec2 = HiveDecimal.create("7");
    resultDec = dec1.divide(dec2);
    Assert.assertEquals("3.1428571428571428571428571428571428571", resultDec.toString());  // UNDONE

    //---------------------------------------------------
    oldDec1 = HiveDecimalV1.create("1");
    oldDec2 = HiveDecimalV1.create("81");
    oldResultDec = oldDec1.divide(oldDec2);
    Assert.assertEquals("0.01234567901234567901234567901234567901", oldResultDec.toString());
    //---------------------------------------------------
    dec1 = HiveDecimal.create("1");
    dec2 = HiveDecimal.create("81");
    resultDec = dec1.divide(dec2);
    Assert.assertEquals("0.01234567901234567901234567901234567901", resultDec.toString());  // UNDONE

    //---------------------------------------------------
    oldDec1 = HiveDecimalV1.create("425");
    oldDec2 = HiveDecimalV1.create("1000000000000000");
    oldResultDec = oldDec1.divide(oldDec2);
    Assert.assertEquals("0.000000000000425", oldResultDec.toString());
    //---------------------------------------------------
    dec1 = HiveDecimal.create("425");
    dec2 = HiveDecimal.create("1000000000000000");
    resultDec = dec1.divide(dec2);
    Assert.assertEquals("0.000000000000425", resultDec.toString());  // UNDONE

    //---------------------------------------------------
    oldDec1 = HiveDecimalV1.create("0.000000000088");
    oldDec2 = HiveDecimalV1.create("1000000000000000");
    oldResultDec = oldDec1.divide(oldDec2);
    Assert.assertEquals("0.000000000000000000000000088", oldResultDec.toString());
    Assert.assertEquals(27, oldResultDec.scale());
    //---------------------------------------------------
    dec1 = HiveDecimal.create("0.000000000088");
    dec2 = HiveDecimal.create("1000000000000000");
    resultDec = dec1.divide(dec2);
    Assert.assertEquals("0.000000000000000000000000088", resultDec.toString());  // UNDONE
    Assert.assertEquals(27, resultDec.scale());
   }

  @Test
  public void testDivide() {
    HiveDecimal dec1 = HiveDecimal.create("3.14");
    HiveDecimal dec2 = HiveDecimal.create("3");
    Assert.assertNotNull(dec1.divide(dec2));

    dec1 = HiveDecimal.create("15");
    dec2 = HiveDecimal.create("5");
    Assert.assertEquals("3", dec1.divide(dec2).toString());

    dec1 = HiveDecimal.create("3.140");
    dec2 = HiveDecimal.create("1.00");
    Assert.assertEquals("3.14", dec1.divide(dec2).toString());
  }

  @Test
  public void testPlus() {

    HiveDecimalV1 oldDec;
    HiveDecimalV1 oldDec2;
    HiveDecimalV1 oldResultDec;

    HiveDecimal dec;
    HiveDecimal dec1;
    HiveDecimal dec2;
    HiveDecimal resultDec;

    String decStr;
    String decStr2;

    dec1 = HiveDecimal.create("3.140");
    dec1.validate();
    dec2 = HiveDecimal.create("1.00");
    dec2.validate();
    resultDec = dec1.add(dec2);
    resultDec.validate();
    Assert.assertEquals("4.14", resultDec.toString());

    decStr = "3.140";
    decStr2 = "1.00";
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(decStr);
    oldDec2 = HiveDecimalV1.create(decStr2);
    oldResultDec = oldDec.add(oldDec2);
    Assert.assertEquals("4.14", oldResultDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(decStr);
    assertTrue(dec != null);
    dec.validate();
    dec2 = HiveDecimal.create(decStr2);
    assertTrue(dec2 != null);
    dec2.validate();
    resultDec = dec.add(dec2);
    assertTrue(resultDec != null);
    resultDec.validate();
    Assert.assertEquals("4.14", resultDec.toString());
    Assert.assertEquals(1, resultDec.integerDigitCount());

    decStr = "3.140";
    decStr2 = "1.00000008733";
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(decStr);
    oldDec2 = HiveDecimalV1.create(decStr2);
    oldResultDec = oldDec.add(oldDec2);
    Assert.assertEquals("4.14000008733", oldResultDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(decStr);
    assertTrue(dec != null);
    dec.validate();
    dec2 = HiveDecimal.create(decStr2);
    assertTrue(dec2 != null);
    dec2.validate();
    resultDec = dec.add(dec2);
    assertTrue(resultDec != null);
    resultDec.validate();
    Assert.assertEquals("4.14000008733", resultDec.toString());
    Assert.assertEquals(1, resultDec.integerDigitCount());

    decStr = "3.140";
    decStr2 = "1.00000000000000000008733";
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(decStr);
    oldDec2 = HiveDecimalV1.create(decStr2);
    oldResultDec = oldDec.add(oldDec2);
    Assert.assertEquals("4.14000000000000000008733", oldResultDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(decStr);
    assertTrue(dec != null);
    dec.validate();
    dec2 = HiveDecimal.create(decStr2);
    assertTrue(dec2 != null);
    dec2.validate();
    resultDec = dec.add(dec2);
    assertTrue(resultDec != null);
    resultDec.validate();
    Assert.assertEquals("4.14000000000000000008733", resultDec.toString());
    Assert.assertEquals(1, resultDec.integerDigitCount());

    decStr = "30000000000.140";
    decStr2 = "1.00000000000000000008733";
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(decStr);
    oldDec2 = HiveDecimalV1.create(decStr2);
    oldResultDec = oldDec.add(oldDec2);
    Assert.assertEquals("30000000001.14000000000000000008733", oldResultDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(decStr);
    assertTrue(dec != null);
    dec.validate();
    dec2 = HiveDecimal.create(decStr2);
    assertTrue(dec2 != null);
    dec2.validate();
    resultDec = dec.add(dec2);
    assertTrue(resultDec != null);
    resultDec.validate();
    Assert.assertEquals("30000000001.14000000000000000008733", resultDec.toString());
    // Assert.assertEquals(1, resultDec.integerDigitCount());

    decStr = "300000000000000.140";
    decStr2 = "1.00000000000000000008733";
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(decStr);
    oldDec2 = HiveDecimalV1.create(decStr2);
    oldResultDec = oldDec.add(oldDec2);
    Assert.assertEquals("300000000000001.14000000000000000008733", oldResultDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(decStr);
    assertTrue(dec != null);
    dec.validate();
    dec2 = HiveDecimal.create(decStr2);
    assertTrue(dec2 != null);
    dec2.validate();
    resultDec = dec.add(dec2);
    assertTrue(resultDec != null);
    resultDec.validate();
    Assert.assertEquals("300000000000001.14000000000000000008733", resultDec.toString());
    // Assert.assertEquals(1, resultDec.integerDigitCount());

    // Edge case?
    decStr = "3000000000000000.140";
    decStr2 = "1.00000000000000000008733";
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(decStr);
    oldDec2 = HiveDecimalV1.create(decStr2);
    oldResultDec = oldDec.add(oldDec2);
    Assert.assertEquals("3000000000000001.1400000000000000000873", oldResultDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(decStr);
    assertTrue(dec != null);
    dec.validate();
    dec2 = HiveDecimal.create(decStr2);
    assertTrue(dec2 != null);
    dec2.validate();
    resultDec = dec.add(dec2);
    assertTrue(resultDec != null);
    resultDec.validate();
    Assert.assertEquals("3000000000000001.1400000000000000000873", resultDec.toString());
    // Assert.assertEquals(1, resultDec.integerDigitCount());

    decStr = "300000000000000000000000.14";
    decStr2 = "0.0000055555555550008733";
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(decStr);
    oldDec2 = HiveDecimalV1.create(decStr2);
    oldResultDec = oldDec.add(oldDec2);
    Assert.assertEquals("300000000000000000000000.14000555555556", oldResultDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(decStr);
    assertTrue(dec != null);
    dec.validate();
    dec2 = HiveDecimal.create(decStr2);
    assertTrue(dec2 != null);
    dec2.validate();
    resultDec = dec.add(dec2);
    assertTrue(resultDec != null);
    resultDec.validate();
    Assert.assertEquals("300000000000000000000000.14000555555556", resultDec.toString());
    // Assert.assertEquals(1, resultDec.integerDigitCount());

    decStr = "300000000000000000000000.14";
    decStr2 = "0.000005555555555000873355";
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(decStr);
    oldDec2 = HiveDecimalV1.create(decStr2);
    oldResultDec = oldDec.add(oldDec2);
    Assert.assertEquals("300000000000000000000000.14000555555556", oldResultDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(decStr);
    assertTrue(dec != null);
    dec.validate();
    dec2 = HiveDecimal.create(decStr2);
    assertTrue(dec2 != null);
    dec2.validate();
    resultDec = dec.add(dec2);
    assertTrue(resultDec != null);
    resultDec.validate();
    Assert.assertEquals("300000000000000000000000.14000555555556", resultDec.toString());
    // Assert.assertEquals(1, resultDec.integerDigitCount());



    // Example from HiveDecimal.add header comments.
    decStr = "598575157855521918987423259.94094";
    decStr2 = "0.0000000000006711991169422033";
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(decStr);
    oldDec2 = HiveDecimalV1.create(decStr2);
    oldResultDec = oldDec.add(oldDec2);
    Assert.assertEquals("598575157855521918987423259.94094", oldResultDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(decStr);
    assertTrue(dec != null);
    dec.validate();
    dec2 = HiveDecimal.create(decStr2);
    assertTrue(dec2 != null);
    dec2.validate();
    resultDec = dec.add(dec2);
    assertTrue(resultDec != null);
    resultDec.validate();
    Assert.assertEquals("598575157855521918987423259.94094", resultDec.toString());
    Assert.assertEquals(27, resultDec.integerDigitCount());

    decStr = "598575157855521918987423259.94094";
    decStr2 = "0.5555555555556711991169422033";
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(decStr);
    oldDec2 = HiveDecimalV1.create(decStr2);
    oldResultDec = oldDec.add(oldDec2);
    Assert.assertEquals("598575157855521918987423260.49649555556", oldResultDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(decStr);
    assertTrue(dec != null);
    dec.validate();
    dec2 = HiveDecimal.create(decStr2);
    assertTrue(dec2 != null);
    dec2.validate();
    resultDec = dec.add(dec2);
    assertTrue(resultDec != null);
    resultDec.validate();
    Assert.assertEquals("598575157855521918987423260.49649555556", resultDec.toString());
    Assert.assertEquals(27, resultDec.integerDigitCount());

    decStr = "199999999.99995";
    decStr2 = "100000000.00005";
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(decStr);
    oldDec2 = HiveDecimalV1.create(decStr2);
    oldResultDec = oldDec.add(oldDec2);
    Assert.assertEquals("300000000", oldResultDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(decStr);
    assertTrue(dec != null);
    dec.validate();
    dec2 = HiveDecimal.create(decStr2);
    assertTrue(dec2 != null);
    dec2.validate();
    resultDec = dec.add(dec2);
    assertTrue(resultDec != null);
    resultDec.validate();
    Assert.assertEquals("300000000", resultDec.toString());
    Assert.assertEquals(9, resultDec.integerDigitCount());

    dec1 = HiveDecimal.create("99999999999999999999999999999999999999");
    dec1.validate();
    Assert.assertEquals(38, dec1.integerDigitCount());
    dec2 = HiveDecimal.create("1");
    dec2.validate();
    Assert.assertNull(dec1.add(dec2));
  }

  @Test
  public void testAdd() {

    HiveDecimalV1 oldDec;
    HiveDecimalV1 oldDec2;
    HiveDecimalV1 oldResultDec;

    HiveDecimal dec;
    HiveDecimal dec2;
    HiveDecimal resultDec;

    // Use the example from HIVE-13423 where the integer digits of the result exceed the
    // enforced precision/scale.
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create("98765432109876543210.12345");
    oldResultDec = oldDec.add(oldDec);
    Assert.assertEquals("197530864219753086420.2469", oldResultDec.toString());
    oldResultDec = HiveDecimalV1.enforcePrecisionScale(oldResultDec, 38, 18);
    Assert.assertTrue(oldResultDec == null);
    //---------------------------------------------------
    dec = HiveDecimal.create("98765432109876543210.12345");
    assertTrue(dec != null);
    dec.validate();
    resultDec = dec.add(dec);
    assertTrue(resultDec != null);
    resultDec.validate();
    Assert.assertEquals("197530864219753086420.2469", resultDec.toString());
    // Assert.assertEquals(21, resultDec.integerDigitCount());
    resultDec = HiveDecimal.enforcePrecisionScale(resultDec, 38, 18);
    Assert.assertTrue(resultDec == null);
 
    // Make sure zero trimming doesn't extend into the integer digits.
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create("199999999.99995");
    oldDec2 = HiveDecimalV1.create("100000000.00005");
    oldResultDec = oldDec.add(oldDec2);
    Assert.assertEquals("300000000", oldResultDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create("199999999.99995");
    assertTrue(dec != null);
    dec.validate();
    dec2 = HiveDecimal.create("100000000.00005");
    assertTrue(dec2 != null);
    dec2.validate();
    resultDec = dec.add(dec2);
    assertTrue(resultDec != null);
    resultDec.validate();
    Assert.assertEquals("300000000", resultDec.toString());
    Assert.assertEquals(9, resultDec.integerDigitCount());
   }

  @Test
  public void testMinus() {

    HiveDecimalV1 oldDec;
    HiveDecimalV1 oldDec2;
    HiveDecimalV1 oldResultDec;

    HiveDecimal dec;
    HiveDecimal dec1;
    HiveDecimal dec2;
    HiveDecimal resultDec;

    String decStr;
    String decStr2;

    dec1 = HiveDecimal.create("3.140");
    dec1.validate();
    dec2 = HiveDecimal.create("1.00");
    dec2.validate();
    resultDec = dec1.add(dec2);
    resultDec.validate();
    Assert.assertEquals("4.14", resultDec.toString());

    decStr = "3.140";
    decStr2 = "1.00";
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(decStr);
    oldDec2 = HiveDecimalV1.create(decStr2);
    oldResultDec = oldDec.subtract(oldDec2);
    Assert.assertEquals("2.14", oldResultDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(decStr);
    assertTrue(dec != null);
    dec.validate();
    dec2 = HiveDecimal.create(decStr2);
    assertTrue(dec2 != null);
    dec2.validate();
    resultDec = dec.subtract(dec2);
    assertTrue(resultDec != null);
    resultDec.validate();
    Assert.assertEquals("2.14", resultDec.toString());
    // Assert.assertEquals(1, resultDec.integerDigitCount());

    decStr = "3.140";
    decStr2 = "1.00000008733";
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(decStr);
    oldDec2 = HiveDecimalV1.create(decStr2);
    oldResultDec = oldDec.subtract(oldDec2);
    Assert.assertEquals("2.13999991267", oldResultDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(decStr);
    assertTrue(dec != null);
    dec.validate();
    dec2 = HiveDecimal.create(decStr2);
    assertTrue(dec2 != null);
    dec2.validate();
    resultDec = dec.subtract(dec2);
    assertTrue(resultDec != null);
    resultDec.validate();
    Assert.assertEquals("2.13999991267", resultDec.toString());
    // Assert.assertEquals(1, resultDec.integerDigitCount());

    decStr = "3.140";
    decStr2 = "1.00000000000000000008733";
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(decStr);
    oldDec2 = HiveDecimalV1.create(decStr2);
    oldResultDec = oldDec.subtract(oldDec2);
    Assert.assertEquals("2.13999999999999999991267", oldResultDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(decStr);
    assertTrue(dec != null);
    dec.validate();
    dec2 = HiveDecimal.create(decStr2);
    assertTrue(dec2 != null);
    dec2.validate();
    resultDec = dec.subtract(dec2);
    assertTrue(resultDec != null);
    resultDec.validate();
    Assert.assertEquals("2.13999999999999999991267", resultDec.toString());
    // Assert.assertEquals(1, resultDec.integerDigitCount());

    decStr = "30000000000.140";
    decStr2 = "1.00000000000000000008733";
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(decStr);
    oldDec2 = HiveDecimalV1.create(decStr2);
    oldResultDec = oldDec.subtract(oldDec2);
    Assert.assertEquals("29999999999.13999999999999999991267", oldResultDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(decStr);
    assertTrue(dec != null);
    dec.validate();
    dec2 = HiveDecimal.create(decStr2);
    assertTrue(dec2 != null);
    dec2.validate();
    resultDec = dec.subtract(dec2);
    assertTrue(resultDec != null);
    resultDec.validate();
    Assert.assertEquals("29999999999.13999999999999999991267", resultDec.toString());
    // Assert.assertEquals(1, resultDec.integerDigitCount());

    decStr = "300000000000000.140";
    decStr2 = "1.00000000000000000008733";
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(decStr);
    oldDec2 = HiveDecimalV1.create(decStr2);
    oldResultDec = oldDec.subtract(oldDec2);
    Assert.assertEquals("299999999999999.13999999999999999991267", oldResultDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(decStr);
    assertTrue(dec != null);
    dec.validate();
    dec2 = HiveDecimal.create(decStr2);
    assertTrue(dec2 != null);
    dec2.validate();
    resultDec = dec.subtract(dec2);
    assertTrue(resultDec != null);
    resultDec.validate();
    Assert.assertEquals("299999999999999.13999999999999999991267", resultDec.toString());
    // Assert.assertEquals(1, resultDec.integerDigitCount());

    // Edge case?
    decStr = "3000000000000000.140";
    decStr2 = "1.00000000000000000008733";
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(decStr);
    oldDec2 = HiveDecimalV1.create(decStr2);
    oldResultDec = oldDec.subtract(oldDec2);
    Assert.assertEquals("2999999999999999.1399999999999999999127", oldResultDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(decStr);
    assertTrue(dec != null);
    dec.validate();
    dec2 = HiveDecimal.create(decStr2);
    assertTrue(dec2 != null);
    dec2.validate();
    resultDec = dec.subtract(dec2);
    assertTrue(resultDec != null);
    resultDec.validate();
    Assert.assertEquals("2999999999999999.1399999999999999999127", resultDec.toString());
    // Assert.assertEquals(1, resultDec.integerDigitCount());

    decStr = "300000000000000000000000.14";
    decStr2 = "0.0000055555555550008733";
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(decStr);
    oldDec2 = HiveDecimalV1.create(decStr2);
    oldResultDec = oldDec.subtract(oldDec2);
    Assert.assertEquals("300000000000000000000000.13999444444444", oldResultDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(decStr);
    assertTrue(dec != null);
    dec.validate();
    dec2 = HiveDecimal.create(decStr2);
    assertTrue(dec2 != null);
    dec2.validate();
    resultDec = dec.subtract(dec2);
    assertTrue(resultDec != null);
    resultDec.validate();
    Assert.assertEquals("300000000000000000000000.13999444444444", resultDec.toString());
    // Assert.assertEquals(1, resultDec.integerDigitCount());

    decStr = "300000000000000000000000.14";
    decStr2 = "0.000005555555555000873355";
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(decStr);
    oldDec2 = HiveDecimalV1.create(decStr2);
    oldResultDec = oldDec.subtract(oldDec2);
    Assert.assertEquals("300000000000000000000000.13999444444444", oldResultDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(decStr);
    assertTrue(dec != null);
    dec.validate();
    dec2 = HiveDecimal.create(decStr2);
    assertTrue(dec2 != null);
    dec2.validate();
    resultDec = dec.subtract(dec2);
    assertTrue(resultDec != null);
    resultDec.validate();
    Assert.assertEquals("300000000000000000000000.13999444444444", resultDec.toString());
    // Assert.assertEquals(1, resultDec.integerDigitCount());

    // Example from HiveDecimal.subtract header comments.
    decStr = "598575157855521918987423259.94094";
    decStr2 = "0.0000000000006711991169422033";
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(decStr);
    oldDec2 = HiveDecimalV1.create(decStr2);
    oldResultDec = oldDec.subtract(oldDec2);
    Assert.assertEquals("598575157855521918987423259.94094", oldResultDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(decStr);
    assertTrue(dec != null);
    dec.validate();
    dec2 = HiveDecimal.create(decStr2);
    assertTrue(dec2 != null);
    dec2.validate();
    resultDec = dec.subtract(dec2);
    assertTrue(resultDec != null);
    resultDec.validate();
    Assert.assertEquals("598575157855521918987423259.94094", resultDec.toString());
    // Assert.assertEquals(1, resultDec.integerDigitCount());

    decStr = "598575157855521918987423259.94094";
    decStr2 = "0.5555555555556711991169422033";
    //---------------------------------------------------
    oldDec = HiveDecimalV1.create(decStr);
    oldDec2 = HiveDecimalV1.create(decStr2);
    oldResultDec = oldDec.subtract(oldDec2);
    Assert.assertEquals("598575157855521918987423259.38538444444", oldResultDec.toString());
    //---------------------------------------------------
    dec = HiveDecimal.create(decStr);
    assertTrue(dec != null);
    dec.validate();
    dec2 = HiveDecimal.create(decStr2);
    assertTrue(dec2 != null);
    dec2.validate();
    resultDec = dec.subtract(dec2);
    assertTrue(resultDec != null);
    resultDec.validate();
    Assert.assertEquals("598575157855521918987423259.38538444444", resultDec.toString());
    // Assert.assertEquals(1, resultDec.integerDigitCount());
  }

  @Test
  public void testSubtract() {
    HiveDecimal dec1 = HiveDecimal.create("3.140");
    assertTrue(dec1 != null);
    dec1.validate();
    HiveDecimal dec2 = HiveDecimal.create("1.00");
    assertTrue(dec2 != null);
    dec2.validate();
    HiveDecimal result = dec1.subtract(dec2);
    assertTrue(result != null);
    result.validate();
    Assert.assertEquals("2.14", result.toString());

    dec1 = HiveDecimal.create("0.00001786135888657847525803");
    assertTrue(dec1 != null);
    dec1.validate();
    dec2 = HiveDecimal.create("3.0000123456789");
    assertTrue(dec2 != null);
    dec2.validate();
    result = dec1.subtract(dec2);
    assertTrue(result != null);
    result.validate();
    Assert.assertEquals("-2.99999448432001342152474197", result.toString());
  }

  @Test
  public void testPosMod() {
    HiveDecimal hd1 = HiveDecimal.create("-100.91");
    assertTrue(hd1 != null);
    hd1.validate();
    HiveDecimal hd2 = HiveDecimal.create("9.8");
    assertTrue(hd2 != null);
    hd2.validate();
    HiveDecimal dec = hd1.remainder(hd2).add(hd2).remainder(hd2);
    assertTrue(dec != null);
    dec.validate();
    Assert.assertEquals("6.89", dec.toString());
  }

  @Test
  public void testHashCode() {
      Assert.assertEquals(HiveDecimal.create("9").hashCode(), HiveDecimal.create("9.00").hashCode());
      Assert.assertEquals(HiveDecimal.create("0").hashCode(), HiveDecimal.create("0.00").hashCode());
  }

  @Test
  public void testException() {
    HiveDecimal dec = HiveDecimal.create("3.1415.926");
    Assert.assertNull(dec);
    dec = HiveDecimal.create("3abc43");
    Assert.assertNull(dec);
  }

  @Test
  public void testBinaryConversion() {
    Random r = new Random(2399);
    for (String decString : specialDecimalStrings) {
      doTestBinaryConversion(decString, r);
    }
  }

  private void doTestBinaryConversion(String num, Random r) {
    int scale = r.nextInt(HiveDecimal.MAX_SCALE);
    HiveDecimal dec = HiveDecimal.create(num);
    if (dec == null) {
      return;
    }
    byte[] d = dec.setScale(scale).unscaledValue().toByteArray();
    HiveDecimal roundedDec = dec.setScale(scale, HiveDecimal.ROUND_HALF_UP);
    Assert.assertEquals(roundedDec, HiveDecimal.create(new BigInteger(d), scale));
  }

//------------------------------------------------------------------------------------------------

  @Test
  public void testDecimalsWithOneOne() {
    doTestDecimalsWithPrecisionScale(decimal_1_1_txt, 1, 1);
  }

  @Test
  public void testDecimalsWithKv7Keys() {
    doTestDecimalsWithPrecisionScale(kv7_txt_keys, 38, 18);
  }

  public void doTestDecimalsWithPrecisionScale(String[] decStrings, int precision, int scale) {

    HiveDecimalV1 oldSum = HiveDecimalV1.create(0);
    HiveDecimalWritable sum = new HiveDecimalWritable(0);

    for (int i = 0; i < decStrings.length; i++) {

      String string = decStrings[i];

      HiveDecimalV1 oldDec = HiveDecimalV1.create(string);

      HiveDecimalV1 resultOldDec;
      if (oldDec == null) {
        resultOldDec = null;
      } else {
        resultOldDec = HiveDecimalV1.enforcePrecisionScale(oldDec, precision, scale);
      }

      HiveDecimal dec = HiveDecimal.create(string);

      if (oldDec == null) {
        Assert.assertTrue(dec == null);
        continue;
      }
      HiveDecimal resultDec = HiveDecimal.enforcePrecisionScale(dec, precision, scale);
      if (resultOldDec == null) {
        Assert.assertTrue(resultDec == null);
        continue;
      }

      Assert.assertEquals(resultOldDec.toString(), resultDec.toString());
      Assert.assertEquals(resultOldDec.toFormatString(scale), resultDec.toFormatString(scale));

      oldSum = oldSum.add(resultOldDec);
      sum.mutateAdd(resultDec);
    }

    Assert.assertEquals(oldSum.toString(), sum.toString());
  }

//------------------------------------------------------------------------------------------------

  @Test
  public void testDecimalsWithOneOneWritable() {
    doTestDecimalsWithPrecisionScaleWritable(decimal_1_1_txt, 1, 1);
  }

  @Test
  public void testDecimalsWithKv7KeysWritable() {
    doTestDecimalsWithPrecisionScaleWritable(kv7_txt_keys, 38, 18);
  }

  public void doTestDecimalsWithPrecisionScaleWritable(String[] decStrings, int precision, int scale) {

    HiveDecimalV1 oldSum = HiveDecimalV1.create(0);
    HiveDecimalWritable sum = new HiveDecimalWritable(0);

    for (int i = 0; i < decStrings.length; i++) {
      String string = decStrings[i];

      HiveDecimalV1 oldDec = HiveDecimalV1.create(string);
      HiveDecimalV1 resultOldDec;
      if (oldDec == null) {
        resultOldDec = null;
      } else {
        resultOldDec = HiveDecimalV1.enforcePrecisionScale(oldDec, precision, scale);
      }

      HiveDecimalWritable decWritable = new HiveDecimalWritable(string);
      if (oldDec == null) {
        Assert.assertTrue(!decWritable.isSet());
        continue;
      }
      decWritable.mutateEnforcePrecisionScale(precision, scale);;
      if (resultOldDec == null) {
        Assert.assertTrue(!decWritable.isSet());
        continue;
      }

      Assert.assertEquals(resultOldDec.toString(), decWritable.toString());
      Assert.assertEquals(resultOldDec.toFormatString(scale), decWritable.toFormatString(scale));

      oldSum = oldSum.add(resultOldDec);
      sum.mutateAdd(decWritable);
    }

    Assert.assertEquals(oldSum.toString(), sum.toString());
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testSort() {
    doTestSort(decimal_1_1_txt);
  }

  @Test
  public void testSortSpecial() {
    doTestSort(specialDecimalStrings);
  }

  @Test
  public void testSortRandom() {
    Random r = new Random(14434);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestSortRandom(r, standardAlphabet, bigDecimalFlavor);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestSortRandom(r, sparseAlphabet, bigDecimalFlavor);
      }
    }
  }

  public void doTestSortRandom(Random r, String digitAlphabet, BigDecimalFlavor bigDecimalFlavor) {
    String[] randomStrings = new String[POUND_FACTOR];

    for (int i = 0; i < POUND_FACTOR; i++) {
      BigDecimal bigDecimal = randHiveBigDecimal(r, digitAlphabet, bigDecimalFlavor);

      randomStrings[i] = bigDecimal.toString();
    }

    doTestSort(randomStrings);
  }

  public void doTestSort(String[] decStrings) {

    HiveDecimalV1[] oldDecSortArray = new HiveDecimalV1[decStrings.length];
    HiveDecimal[] decSortArray = new HiveDecimal[decStrings.length];

    int count = 0;
    for (int i = 0; i < decStrings.length; i++) {
      String string = decStrings[i];

      HiveDecimalV1 oldDec = HiveDecimalV1.create(string);
      if (oldDec == null) {
        continue;
      }
      if (isTenPowerBug(oldDec.toString())) {
        continue;
      }
      oldDecSortArray[count] = oldDec;

      HiveDecimal dec = HiveDecimal.create(string);
      if (dec == null) {
        Assert.fail();
      }
      decSortArray[count] = dec;
      count++;
    }

    oldDecSortArray = Arrays.copyOf(oldDecSortArray,  count);
    decSortArray = Arrays.copyOf(decSortArray, count);

    Arrays.sort(oldDecSortArray);
    Arrays.sort(decSortArray);

    for (int i = 0; i < count; i++) {
      String oldDecString = oldDecSortArray[i].toString();
      String decString = decSortArray[i].toString();

      if (!oldDecString.equals(decString)) {
        Assert.fail();
      }
    }
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomCreateFromBigDecimal() {
    Random r = new Random(14434);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestRandomCreateFromBigDecimal(r, standardAlphabet, bigDecimalFlavor);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestRandomCreateFromBigDecimal(r, sparseAlphabet, bigDecimalFlavor);
      }
    }
  }

  private void doTestRandomCreateFromBigDecimal(Random r, String digitAlphabet, BigDecimalFlavor bigDecimalFlavor) {

    for (int i = 0; i < POUND_FACTOR; i++) {
      BigDecimal bigDecimal = randHiveBigDecimal(r, digitAlphabet, bigDecimalFlavor);

      doTestCreateFromBigDecimal(bigDecimal);
    }
  }

  @Test
  public void testCreateFromBigDecimalSpecial() {
    for (BigDecimal bigDecimal : specialBigDecimals) {
      doTestCreateFromBigDecimal(bigDecimal);
    }
  }

  private void doTestCreateFromBigDecimal(BigDecimal bigDecimal) {

    HiveDecimalV1 oldDec = HiveDecimalV1.create(bigDecimal);
    if (oldDec != null && isTenPowerBug(oldDec.toString())) {
      return;
    }
    HiveDecimal dec = HiveDecimal.create(bigDecimal);
    if (oldDec == null) {
      assertTrue(dec == null);
      return;
    }
    assertTrue(dec != null);
    dec.validate();

    Assert.assertEquals(oldDec.toString(), dec.toString());
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomCreateFromBigDecimalNoRound() {
    Random r = new Random(14434);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestRandomCreateFromBigDecimalNoRound(r, standardAlphabet, bigDecimalFlavor);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestRandomCreateFromBigDecimalNoRound(r, sparseAlphabet, bigDecimalFlavor);
      }
    }
  }

  private void doTestRandomCreateFromBigDecimalNoRound(Random r, String digitAlphabet, BigDecimalFlavor bigDecimalFlavor) {

    for (int i = 0; i < POUND_FACTOR; i++) {
      BigDecimal bigDecimal = randHiveBigDecimal(r, digitAlphabet, bigDecimalFlavor);

      doTestCreateFromBigDecimalNoRound(bigDecimal);
    }
  }

  @Test
  public void testCreateFromBigDecimalNoRoundSpecial() {
    for (BigDecimal bigDecimal : specialBigDecimals) {
      doTestCreateFromBigDecimalNoRound(bigDecimal);
    }
  }

  private void doTestCreateFromBigDecimalNoRound(BigDecimal bigDecimal) {

    HiveDecimalV1 oldDec = HiveDecimalV1.create(bigDecimal, /* allowRounding */ false);
    HiveDecimal dec = HiveDecimal.create(bigDecimal, /* allowRounding */ false);
    if (oldDec == null) {
      assertTrue(dec == null);
      return;
    }
    if (dec == null) {
      Assert.fail();
    }
    dec.validate();

    Assert.assertEquals(oldDec.toString(), dec.toString());

  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testCreateFromBigDecimalNegativeScaleSpecial() {
    Random r = new Random(223965);
    for (BigDecimal bigDecimal : specialBigDecimals) {
      int negativeScale = -(0 + r.nextInt(38 + 1));
      bigDecimal = bigDecimal.setScale(negativeScale, BigDecimal.ROUND_HALF_UP);
      doTestCreateFromBigDecimalNegativeScale(bigDecimal);
    }
  }

  private void doTestCreateFromBigDecimalNegativeScale(BigDecimal bigDecimal) {

    HiveDecimalV1 oldDec = HiveDecimalV1.create(bigDecimal);
    HiveDecimal dec = HiveDecimal.create(bigDecimal);
    if (oldDec == null) {
      assertTrue(dec == null);
      return;
    }
    assertTrue(dec != null);
    dec.validate();

    Assert.assertEquals(oldDec.toString(), dec.toString());
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomCreateFromBigInteger() {
    doTestRandomCreateFromBigInteger(standardAlphabet);
  }

  @Test
  public void testRandomCreateFromBigIntegerSparse() {
    for (String digitAlphabet : sparseAlphabets) {
      doTestRandomCreateFromBigInteger(digitAlphabet);
    }
  }

  private void doTestRandomCreateFromBigInteger(String digitAlphabet) {

    Random r = new Random(11241);
    for (int i = 0; i < POUND_FACTOR; i++) {
      BigInteger bigInteger = randHiveBigInteger(r, digitAlphabet);

      doTestCreateFromBigInteger(bigInteger);
    }
  }

  @Test
  public void testCreateFromBigIntegerSpecial() {
    for (BigDecimal bigDecimal : specialBigDecimals) {
      doTestCreateFromBigInteger(bigDecimal.unscaledValue());
    }
  }

  private void doTestCreateFromBigInteger(BigInteger bigInteger) {

    HiveDecimalV1 oldDec = HiveDecimalV1.create(bigInteger);
    HiveDecimal dec = HiveDecimal.create(bigInteger);
    if (oldDec == null) {
      assertTrue(dec == null);
      return;
    }
    assertTrue(dec != null);
    dec.validate();

    Assert.assertEquals(oldDec.toString(), dec.toString());

  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomCreateFromBigIntegerScale() {
    doTestRandomCreateFromBigIntegerScale(standardAlphabet, false);
  }

  @Test
  public void testRandomCreateFromBigIntegerScaleFractionsOnly() {
    doTestRandomCreateFromBigIntegerScale(standardAlphabet, true);
  }

  @Test
  public void testRandomCreateFromBigIntegerScaleSparse() {
    for (String digitAlphabet : sparseAlphabets) {
      doTestRandomCreateFromBigIntegerScale(digitAlphabet, false);
    }
  }

  private void doTestRandomCreateFromBigIntegerScale(String digitAlphabet, boolean fractionsOnly) {

    Random r = new Random(4448);
    for (int i = 0; i < POUND_FACTOR; i++) {
      BigInteger bigInteger = randHiveBigInteger(r, digitAlphabet);

      int scale;
      if (fractionsOnly) {
        scale = 1 + r.nextInt(38);
      } else {
        scale = 0 + r.nextInt(38 + 1);
      }

      doTestCreateFromBigIntegerScale(bigInteger, scale);
    }
  }

  @Test
  public void testCreateFromBigIntegerScaleSpecial() {
    for (BigDecimal bigDecimal : specialBigDecimals) {
      doTestCreateFromBigIntegerScale(bigDecimal.unscaledValue(), bigDecimal.scale());
    }
  }

  private void doTestCreateFromBigIntegerScale(BigInteger bigInteger, int scale) {

    HiveDecimalV1 oldDec = HiveDecimalV1.create(bigInteger, scale);
    HiveDecimal dec = HiveDecimal.create(bigInteger, scale);
    if (oldDec == null) {
      assertTrue(dec == null);
      return;
    }
    assertTrue(dec != null);
    dec.validate();

    Assert.assertEquals(oldDec.toString(), dec.toString());

  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomSetFromDouble() {
    Random r = new Random(14434);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestRandomSetFromDouble(r, standardAlphabet, bigDecimalFlavor);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestRandomSetFromDouble(r, sparseAlphabet, bigDecimalFlavor);
      }
    }
  }

  private void doTestRandomSetFromDouble(Random r, String digitAlphabet, BigDecimalFlavor bigDecimalFlavor) {

    for (int i = 0; i < POUND_FACTOR; i++) {
      BigDecimal bigDecimal = randHiveBigDecimal(r, digitAlphabet, bigDecimalFlavor);

      doTestSetFromDouble(bigDecimal.doubleValue());
    }
  }

  private void doTestRandomSetFromDouble() {

    Random r = new Random(94762);
    for (int i = 0; i < POUND_FACTOR; i++) {
      double randomDouble = r.nextDouble();
 
      doTestSetFromDouble(randomDouble);
    }
  }

  @Test
  public void testSetFromDoubleSpecial() {

    for (String specialString : specialDecimalStrings) {
      double specialDouble = Double.valueOf(specialString);
      doTestSetFromDouble(specialDouble);
    }
  }

  private void doTestSetFromDouble(double doubleValue) {

    HiveDecimalV1 oldDec = HiveDecimalV1.create(Double.toString(doubleValue));
    if (oldDec == null) {
      return;
    }
    HiveDecimal dec = HiveDecimal.create(doubleValue);
    if (dec == null) {
      Assert.fail();
    }
    dec.validate();
    if (!oldDec.toString().equals(dec.toString())) {
      BigDecimal bigDecimal = new BigDecimal(dec.toString());
      for (int i = 16; i < 18;i++) {
        BigDecimal trial = bigDecimal.setScale(i, HiveDecimal.ROUND_HALF_UP);
      }
    }
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomCreateFromString() {
    Random r = new Random(1221);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestRandomCreateFromString(r, standardAlphabet, bigDecimalFlavor);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestRandomCreateFromString(r, sparseAlphabet, bigDecimalFlavor);
      }
    }
  }

  private void doTestRandomCreateFromString(Random r, String digitAlphabet, BigDecimalFlavor bigDecimalFlavor) {

    for (int i = 0; i < POUND_FACTOR; i++) {
      BigDecimal bigDecimal = randHiveBigDecimal(r, digitAlphabet, bigDecimalFlavor);

      doTestCreateFromString(bigDecimal);
    }
  }

  @Test
  public void testCreateFromStringSpecial() {
    for (BigDecimal bigDecimal : specialBigDecimals) {
      doTestCreateFromString(bigDecimal);
    }
  }

  private void doTestCreateFromString(BigDecimal bigDecimal) {

    String decString = bigDecimal.toPlainString();

    HiveDecimalV1 oldDec = HiveDecimalV1.create(decString);
    if (oldDec != null && isTenPowerBug(oldDec.toString())) {
      return;
    }
    HiveDecimal dec = HiveDecimal.create(decString);
    if (oldDec == null) {
      assertTrue(dec == null);
      return;
    }
    assertTrue(dec != null);
    dec.validate();

    Assert.assertEquals(oldDec.toString(), dec.toString());
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomCreateFromStringPadded() {
    Random r = new Random(9774);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestRandomCreateFromStringPadded(r, standardAlphabet, bigDecimalFlavor);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestRandomCreateFromStringPadded(r, sparseAlphabet, bigDecimalFlavor);
      }
    }
  }

  private void doTestRandomCreateFromStringPadded(Random r, String digitAlphabet, BigDecimalFlavor bigDecimalFlavor) {

    for (int i = 0; i < POUND_FACTOR; i++) {
      BigDecimal bigDecimal = randHiveBigDecimal(r, digitAlphabet, bigDecimalFlavor);

      doTestCreateFromStringPadded(bigDecimal);
    }
  }

  @Test
  public void testCreateFromStringPaddedSpecial() {
    for (BigDecimal bigDecimal : specialBigDecimals) {
      doTestCreateFromStringPadded(bigDecimal);
    }
  }

  private void doTestCreateFromStringPadded(BigDecimal bigDecimal) {

    String decString = bigDecimal.toPlainString();
    String decString1 = " " + decString;
    String decString2 = decString + " ";
    String decString3 = " " + decString + " ";
    String decString4 = "  " + decString;
    String decString5 = decString + "  ";
    String decString6 = "  " + decString + "  ";

    HiveDecimalV1 oldDec;
    HiveDecimal dec;

    oldDec = HiveDecimalV1.create(decString);
    if (oldDec != null && isTenPowerBug(oldDec.toString())) {
      return;
    }

    dec = HiveDecimal.create(decString1, true);
    if (oldDec == null) {
      assertTrue(dec == null);
    } else {
      assertTrue(dec != null);
      dec.validate();

      Assert.assertEquals(oldDec.toString(), dec.toString());
    }

    dec = HiveDecimal.create(decString2, true);
    if (oldDec == null) {
      assertTrue(dec == null);
    } else {
      assertTrue(dec != null);
      dec.validate();

      Assert.assertEquals(oldDec.toString(), dec.toString());
    }

    dec = HiveDecimal.create(decString3, true);
    if (oldDec == null) {
      assertTrue(dec == null);
    } else {
      assertTrue(dec != null);
      dec.validate();

      Assert.assertEquals(oldDec.toString(), dec.toString());
    }

    dec = HiveDecimal.create(decString4, true);
    if (oldDec == null) {
      assertTrue(dec == null);
    } else {
      assertTrue(dec != null);
      dec.validate();

      Assert.assertEquals(oldDec.toString(), dec.toString());
    }

    dec = HiveDecimal.create(decString5, true);
    if (oldDec == null) {
      assertTrue(dec == null);
    } else {
      assertTrue(dec != null);
      dec.validate();

      Assert.assertEquals(oldDec.toString(), dec.toString());
    }

    dec = HiveDecimal.create(decString6, true);
    if (oldDec == null) {
      assertTrue(dec == null);
    } else {
      assertTrue(dec != null);
      dec.validate();

      Assert.assertEquals(oldDec.toString(), dec.toString());
    }
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomCreateFromStringExponent() {
    Random r = new Random(297111);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestRandomCreateFromStringPadded(r, standardAlphabet, bigDecimalFlavor);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestRandomCreateFromStringPadded(r, sparseAlphabet, bigDecimalFlavor);
      }
    }
  }

  private void doTestRandomCreateFromStringExponent(Random r, String digitAlphabet, BigDecimalFlavor bigDecimalFlavor) {

    for (int i = 0; i < POUND_FACTOR; i++) {
      BigDecimal bigDecimal = randHiveBigDecimal(r, digitAlphabet, bigDecimalFlavor);

      doTestCreateFromStringExponent(bigDecimal);
    }
  }

  @Test
  public void testCreateFromStringExponentSpecial() {
    for (BigDecimal bigDecimal : specialBigDecimals) {
      doTestCreateFromStringExponent(bigDecimal);
    }
  }

  private void doTestCreateFromStringExponent(BigDecimal bigDecimal) {

    // Use toString which will have exponents instead of toPlainString.
    String decString = bigDecimal.toString();

    HiveDecimalV1 oldDec = HiveDecimalV1.create(decString);
    HiveDecimal dec = HiveDecimal.create(decString);
    if (oldDec == null) {
      assertTrue(dec == null);
      return;
    }
    assertTrue(dec != null);
    dec.validate();

    Assert.assertEquals(oldDec.toString(), dec.toString());
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomLongValue() {
    Random r = new Random(73293);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestRandomLongValue(r, standardAlphabet, bigDecimalFlavor);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestRandomLongValue(r, sparseAlphabet, bigDecimalFlavor);
      }
    }
  }

  private void doTestRandomLongValue(Random r, String digitAlphabet, BigDecimalFlavor bigDecimalFlavor) {

    for (int i = 0; i < POUND_FACTOR; i++) {
      BigDecimal bigDecimal = randHiveBigDecimal(r, digitAlphabet, bigDecimalFlavor);

      doTestLongValue(bigDecimal);
    }
  }

  @Test
  public void testLongValueSpecial() {
    for (BigDecimal bigDecimal : specialBigDecimals) {
      doTestLongValue(bigDecimal);
    }
  }

  private void doTestLongValue(BigDecimal bigDecimal) {

    HiveDecimalV1 oldDec = HiveDecimalV1.create(bigDecimal);
    if (oldDec != null && isTenPowerBug(oldDec.toString())) {
      return;
    }
    HiveDecimal dec = HiveDecimal.create(bigDecimal);
    if (oldDec == null) {
      assertTrue(dec == null);
      return;
    }
    assertTrue(dec != null);
    dec.validate();

    BigDecimal bigDecimalOldDec = oldDec.bigDecimalValue();
    BigDecimal bigDecimalDec = dec.bigDecimalValue();
    Assert.assertEquals(bigDecimalOldDec, bigDecimalDec);

    BigDecimal bigDecimalFloor = bigDecimalDec.setScale(0, BigDecimal.ROUND_DOWN);
    long longValueBigDecimalFloor = bigDecimalFloor.longValue();
    boolean isLongExpected =
        bigDecimalFloor.equals(bigDecimalDec.valueOf(longValueBigDecimalFloor));

    boolean decIsLong = dec.isLong();
    long oldDecLong = oldDec.longValue();
    long decLong = dec.longValue();
    if (isLongExpected != decIsLong) {
      Assert.fail();
    }

    if (decIsLong) {
      if (oldDecLong != decLong) {
        Assert.fail();
      }
    }
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomIntValue() {
    Random r = new Random(98333);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestRandomIntValue(r, standardAlphabet, bigDecimalFlavor);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestRandomIntValue(r, sparseAlphabet, bigDecimalFlavor);
      }
    }
  }

  private void doTestRandomIntValue(Random r, String digitAlphabet, BigDecimalFlavor bigDecimalFlavor) {

    for (int i = 0; i < POUND_FACTOR; i++) {
      BigDecimal bigDecimal = randHiveBigDecimal(r, digitAlphabet, bigDecimalFlavor);

      doTestIntValue(bigDecimal);
    }
  }

  @Test
  public void testIntValueSpecial() {
    for (BigDecimal bigDecimal : specialBigDecimals) {
      doTestIntValue(bigDecimal);
    }
  }

  private void doTestIntValue(BigDecimal bigDecimal) {

    HiveDecimalV1 oldDec = HiveDecimalV1.create(bigDecimal);
    if (oldDec != null && isTenPowerBug(oldDec.toString())) {
      return;
    }
    HiveDecimal dec = HiveDecimal.create(bigDecimal);
    if (oldDec == null) {
      assertTrue(dec == null);
      return;
    }
    assertTrue(dec != null);
    dec.validate();

    BigDecimal bigDecimalOldDec = oldDec.bigDecimalValue();
    BigDecimal bigDecimalDec = dec.bigDecimalValue();
    Assert.assertEquals(bigDecimalOldDec, bigDecimalDec);

    BigDecimal bigDecimalFloor = bigDecimalDec.setScale(0, BigDecimal.ROUND_DOWN);
    int intValueBigDecimalFloor = bigDecimalFloor.intValue();
    boolean isIntExpected =
        bigDecimalFloor.equals(bigDecimalDec.valueOf(intValueBigDecimalFloor));

    boolean decIsInt = dec.isInt();
    int oldDecInt = oldDec.intValue();
    int decInt = dec.intValue();
    if (isIntExpected != decIsInt) {
      Assert.fail();
    }

    if (decIsInt) {
      if (oldDecInt != decInt) {
        Assert.fail();
      }
    }
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomShortValue() {
    Random r = new Random(15);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestRandomShortValue(r, standardAlphabet, bigDecimalFlavor);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestRandomShortValue(r, sparseAlphabet, bigDecimalFlavor);
      }
    }
  }

  private void doTestRandomShortValue(Random r, String digitAlphabet, BigDecimalFlavor bigDecimalFlavor) {
    for (int i = 0; i < POUND_FACTOR; i++) {
      BigDecimal bigDecimal = randHiveBigDecimal(r, digitAlphabet, bigDecimalFlavor);

      doTestShortValue(bigDecimal);
    }
  }

  @Test
  public void testShortValueSpecial() {
    for (BigDecimal bigDecimal : specialBigDecimals) {
      doTestShortValue(bigDecimal);
    }
  }

  private void doTestShortValue(BigDecimal bigDecimal) {

    HiveDecimalV1 oldDec = HiveDecimalV1.create(bigDecimal);
    if (oldDec != null && isTenPowerBug(oldDec.toString())) {
      return;
    }
    HiveDecimal dec = HiveDecimal.create(bigDecimal);
    if (oldDec == null) {
      assertTrue(dec == null);
      return;
    }
    assertTrue(dec != null);
    dec.validate();

    BigDecimal bigDecimalOldDec = oldDec.bigDecimalValue();
    BigDecimal bigDecimalDec = dec.bigDecimalValue();
    Assert.assertEquals(bigDecimalOldDec, bigDecimalDec);

    BigDecimal bigDecimalFloor = bigDecimalDec.setScale(0, BigDecimal.ROUND_DOWN);
    short shortValueBigDecimalFloor = bigDecimalFloor.shortValue();
    boolean isShortExpected =
        bigDecimalFloor.equals(bigDecimalDec.valueOf(shortValueBigDecimalFloor));

    boolean decIsShort = dec.isShort();
    short oldDecShort = oldDec.shortValue();
    short decShort = dec.shortValue();
    if (isShortExpected != decIsShort) {
      Assert.fail();
    }

    if (decIsShort) {
      if (oldDecShort != decShort) {
        Assert.fail();
      }
    }
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomByteValue() {
    Random r = new Random(9292);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestRandomByteValue(r, standardAlphabet, bigDecimalFlavor);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestRandomByteValue(r, sparseAlphabet, bigDecimalFlavor);
      }
    }
  }

  private void doTestRandomByteValue(Random r, String digitAlphabet, BigDecimalFlavor bigDecimalFlavor) {

    for (int i = 0; i < POUND_FACTOR; i++) {
      BigDecimal bigDecimal = randHiveBigDecimal(r, digitAlphabet, bigDecimalFlavor);

      doTestByteValue(bigDecimal);
    }
  }

  @Test
  public void testByteValueSpecial() {
    for (BigDecimal bigDecimal : specialBigDecimals) {
      doTestByteValue(bigDecimal);
    }
  }

  private void doTestByteValue(BigDecimal bigDecimal) {

    HiveDecimalV1 oldDec = HiveDecimalV1.create(bigDecimal);
    if (oldDec != null && isTenPowerBug(oldDec.toString())) {
      return;
    }
    HiveDecimal dec = HiveDecimal.create(bigDecimal);
    if (oldDec == null) {
      assertTrue(dec == null);
      return;
    }
    assertTrue(dec != null);
    dec.validate();

    BigDecimal bigDecimalOldDec = oldDec.bigDecimalValue();
    BigDecimal bigDecimalDec = dec.bigDecimalValue();
    Assert.assertEquals(bigDecimalOldDec, bigDecimalDec);

    BigDecimal bigDecimalFloor = bigDecimalDec.setScale(0, BigDecimal.ROUND_DOWN);
    byte byteValueBigDecimalFloor = bigDecimalFloor.byteValue();
    boolean isByteExpected =
        bigDecimalFloor.equals(bigDecimalDec.valueOf(byteValueBigDecimalFloor));

    boolean decIsByte = dec.isByte();
    byte oldDecByte = oldDec.byteValue();
    byte decByte = dec.byteValue();
    if (isByteExpected != decIsByte) {
      Assert.fail();
    }

    if (decIsByte) {
      if (oldDecByte != decByte) {
        Assert.fail();
      }
    }
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomTimestamp() {
    Random r = new Random(5476);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestRandomTimestamp(r, standardAlphabet, bigDecimalFlavor);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestRandomTimestamp(r, sparseAlphabet, bigDecimalFlavor);
      }
    }
  }

  private void doTestRandomTimestamp(Random r, String digitAlphabet, BigDecimalFlavor bigDecimalFlavor) {
    for (int i = 0; i < POUND_FACTOR; i++) {
      BigDecimal bigDecimal = randHiveBigDecimal(r, digitAlphabet, bigDecimalFlavor);

      doTestTimestamp(bigDecimal);
    }
  }

  @Test
  public void testTimestampSpecial() {
    for (BigDecimal bigDecimal : specialBigDecimals) {
      doTestTimestamp(bigDecimal);
    }
  }

  private void doTestTimestamp(BigDecimal bigDecimal) {

    HiveDecimalV1 oldDec = HiveDecimalV1.create(bigDecimal);
    if (oldDec != null && isTenPowerBug(oldDec.toString())) {
      return;
    }
    HiveDecimal dec = HiveDecimal.create(bigDecimal);
    if (oldDec == null) {
      assertTrue(dec == null);
      return;
    }
    assertTrue(dec != null);
    dec.validate();

    Timestamp timestampOldDec = TimestampUtils.decimalToTimestamp(oldDec);
    Timestamp timestampDec = TimestampUtils.decimalToTimestamp(dec);
    if (timestampOldDec == null) {
      Assert.assertTrue(timestampDec == null);
      return;
    }
    if (timestampDec == null) {
      return;
    }
    Assert.assertEquals(timestampOldDec, timestampDec);
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomBigIntegerBytes() {
    Random r = new Random(1050);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestRandomBigIntegerBytes(r, standardAlphabet, bigDecimalFlavor);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestRandomBigIntegerBytes(r, sparseAlphabet, bigDecimalFlavor);
      }
    }
  }

  private void doTestRandomBigIntegerBytes(Random r, String digitAlphabet, BigDecimalFlavor bigDecimalFlavor) {
    for (int i = 0; i < POUND_FACTOR; i++) {
      BigDecimal bigDecimal = randHiveBigDecimal(r, digitAlphabet, bigDecimalFlavor);

      doTestBigIntegerBytes(bigDecimal);
    }
  }

  @Test
  public void testBigIntegerBytesSpecial() {
    Random r = new Random(1050);
    for (BigDecimal bigDecimal : specialBigDecimals) {
      int negativeScale = -(0 + r.nextInt(38 + 1));
      bigDecimal = bigDecimal.setScale(negativeScale, BigDecimal.ROUND_HALF_UP);
      doTestBigIntegerBytes(bigDecimal);
    }
  }

  private void doTestBigIntegerBytes(BigDecimal bigDecimal) {

    HiveDecimalV1 oldDec = HiveDecimalV1.create(bigDecimal);
    if (oldDec != null && isTenPowerBug(oldDec.toString())) {
      return;
    }
    if (oldDec == null) {
      return;
    }
    HiveDecimal dec = HiveDecimal.create(bigDecimal);
    assertTrue(dec != null);
    dec.validate();

    //---------------------------------------------------
    BigInteger oldBigInteger = oldDec.unscaledValue();
    int oldScale = oldDec.scale();
    //---------------------------------------------------

    BigInteger bigInteger = dec.unscaledValue();
    int scale = dec.scale();

    long[] scratchLongs = new long[HiveDecimal.SCRATCH_LONGS_LEN];
    byte[] scratchBuffer = new byte[HiveDecimal.SCRATCH_BUFFER_LEN_BIG_INTEGER_BYTES];

    int which = 0;
    try {
      which = 1;
      int byteLength = dec.bigIntegerBytes(scratchLongs, scratchBuffer);
      byte[] bytes = null;
      if (byteLength == 0) {
        Assert.fail();
      } else {
        bytes = Arrays.copyOf(scratchBuffer, byteLength);
      }

      which = 2;
      byte[] bytesExpected = bigInteger.toByteArray();
      String bytesExpectedString = displayBytes(bytesExpected, 0, bytesExpected.length);

      if (!StringExpr.equal(bytes, 0, bytes.length, bytesExpected, 0, bytesExpected.length)) {
        fail();
      }

      which = 3;
      HiveDecimal createFromBigIntegerBytesDec =
          HiveDecimal.createFromBigIntegerBytesAndScale(
              bytes, 0, bytes.length, scale);
      if (!createFromBigIntegerBytesDec.equals(dec)) {
        fail();
      }

    } catch (Exception e) {
      fail();
    }
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomToFormatString() {
    Random r = new Random(1051);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestRandomToFormatString(r, standardAlphabet, bigDecimalFlavor);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestRandomToFormatString(r, sparseAlphabet, bigDecimalFlavor);
      }
    }
  }

  private void doTestRandomToFormatString(Random r, String digitAlphabet, BigDecimalFlavor bigDecimalFlavor) {
    for (int i = 0; i < POUND_FACTOR; i++) {
      BigDecimal bigDecimal = randHiveBigDecimal(r, digitAlphabet, bigDecimalFlavor);

      doTestToFormatString(r, bigDecimal);
    }
  }

  @Test
  public void testToFormatStringSpecial() {
    Random r = new Random(1050);
    for (BigDecimal bigDecimal : specialBigDecimals) {
      doTestToFormatString(r, bigDecimal);
    }
  }

  private void doTestToFormatString(Random r, BigDecimal bigDecimal) {

    HiveDecimalV1 oldDec = HiveDecimalV1.create(bigDecimal);
    if (oldDec != null && isTenPowerBug(oldDec.toString())) {
      return;
    }
    HiveDecimal dec;
    if (oldDec == null) {
      dec = HiveDecimal.create(bigDecimal);
      if (dec != null) {
        Assert.fail();
      }
      return;
    } else {
      dec = HiveDecimal.create(bigDecimal);
      if (dec == null) {
        if (isTenPowerBug(oldDec.toString())) {
          return;
        }
        Assert.fail();
      }
    }
    dec.validate();

    // UNDONE: Does this random range need to go as high as 38?
    int formatScale = 0 + r.nextInt(38);

    String oldDecFormatString = oldDec.toFormatString(formatScale);
    String decFormatString;
    if (oldDecFormatString == null) {
      decFormatString = dec.toFormatString(formatScale);
      if (decFormatString != null) {
        Assert.fail();
      }
      return;
    } else {
      decFormatString = dec.toFormatString(formatScale);
      if (decFormatString == null) {
        if (isTenPowerBug(oldDecFormatString)) {
          return;
        }
        Assert.fail();
      }
    }

    if (!oldDecFormatString.equals(decFormatString)) {
      fail();
    }
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomScaleByPowerOfTen() {
    Random r = new Random(1052);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestRandomScaleByPowerOfTen(r, standardAlphabet, bigDecimalFlavor);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestRandomScaleByPowerOfTen(r, sparseAlphabet, bigDecimalFlavor);
      }
    }
  }

  private void doTestRandomScaleByPowerOfTen(Random r, String digitAlphabet, BigDecimalFlavor bigDecimalFlavor) {
    for (int i = 0; i < POUND_FACTOR; i++) {
      BigDecimal bigDecimal = randHiveBigDecimal(r, digitAlphabet, bigDecimalFlavor);

      doTestScaleByPowerOfTen(r, bigDecimal);
    }
  }

  @Test
  public void testScaleByPowerOfTenSpecial() {
    Random r = new Random(1050);
    for (BigDecimal bigDecimal : specialBigDecimals) {
      doTestScaleByPowerOfTen(r, bigDecimal);
    }
  }

  private void doTestScaleByPowerOfTen(Random r, BigDecimal bigDecimal) {

    HiveDecimalV1 oldDec = HiveDecimalV1.create(bigDecimal);
    if (oldDec != null && isTenPowerBug(oldDec.toString())) {
      return;
    }
    HiveDecimal dec = HiveDecimal.create(bigDecimal);
    if (oldDec == null) {
      assertTrue(dec == null);
      return;
    }
    assertTrue(dec != null);
    dec.validate();

    HiveDecimalV1 oldPowerDec;
    HiveDecimal powerDec;

    for (int power = -(2 * HiveDecimal.MAX_SCALE + 1);
         power <= 2 * HiveDecimal.MAX_SCALE + 1;
         power++) {

      oldPowerDec = oldDec.scaleByPowerOfTen(power);
      boolean isEqual;
      if (oldPowerDec == null) {
        powerDec = dec.scaleByPowerOfTen(power);
        if (powerDec != null) {
          Assert.fail();
        }
        return;
      } else {
        String oldPowerDecString = oldPowerDec.toString();
        powerDec = dec.scaleByPowerOfTen(power);
        if (powerDec == null) {
          if (isTenPowerBug(oldPowerDec.toString())) {
            return;
          }
          Assert.fail();
          continue;
        }
        powerDec.validate();
        String powerDecString = powerDec.toString();
        isEqual = oldPowerDecString.equals(powerDecString);
        if (!isEqual) {
          if (oldPowerDecString.equals("0.00000000000000000000000000000000000001") ||
              oldPowerDecString.equals("-0.00000000000000000000000000000000000001")) {
            continue;
          }
          Assert.fail();
        }
      }
    }
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomWriteReadFields() throws Exception {
    Random r = new Random(1052);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestRandomWriteReadFields(r, standardAlphabet, bigDecimalFlavor);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestRandomWriteReadFields(r, sparseAlphabet, bigDecimalFlavor);
      }
    }
  }

  private void doTestRandomWriteReadFields(Random r, String digitAlphabet, BigDecimalFlavor bigDecimalFlavor) throws Exception {
    for (int i = 0; i < POUND_FACTOR; i++) {
      BigDecimal bigDecimal = randHiveBigDecimal(r, digitAlphabet, bigDecimalFlavor);

      doTestWriteReadFields(r, bigDecimal);
    }
  }

  @Test
  public void testWriteReadFieldsSpecial() throws Exception {
    Random r = new Random(1050);
    for (BigDecimal bigDecimal : specialBigDecimals) {
      doTestWriteReadFields(r, bigDecimal);
    }
  }

  private void doTestWriteReadFields(Random r, BigDecimal bigDecimal) throws IOException {

    HiveDecimalV1 oldDec = HiveDecimalV1.create(bigDecimal);
    if (oldDec != null && isTenPowerBug(oldDec.toString())) {
      return;
    }
    HiveDecimal dec = HiveDecimal.create(bigDecimal);
    if (oldDec == null) {
      assertTrue(dec == null);
      return;
    }
    assertTrue(dec != null);
    dec.validate();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);

    HiveDecimalWritable decimalWritableOut = new HiveDecimalWritable(dec);
    decimalWritableOut.write(out);

    byte[] valueBytes = baos.toByteArray();

    ByteArrayInputStream bais = new ByteArrayInputStream(valueBytes);
    DataInputStream in = new DataInputStream(bais);
    HiveDecimalWritable decimalWritableIn = new HiveDecimalWritable();
    decimalWritableIn.readFields(in);

    Assert.assertEquals(dec, decimalWritableIn.getHiveDecimal());
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomBigIntegerBytesScaled() throws Exception {
    Random r = new Random(1052);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestRandomBigIntegerBytesScaled(r, standardAlphabet, bigDecimalFlavor);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestRandomBigIntegerBytesScaled(r, sparseAlphabet, bigDecimalFlavor);
      }
    }
  }

  private void doTestRandomBigIntegerBytesScaled(Random r, String digitAlphabet, BigDecimalFlavor bigDecimalFlavor) throws Exception {
    for (int i = 0; i < POUND_FACTOR; i++) {
      BigDecimal bigDecimal = randHiveBigDecimal(r, digitAlphabet, bigDecimalFlavor);

      doTestBigIntegerBytesScaled(r, bigDecimal);
    }
  }

  @Test
  public void testBigIntegerBytesScaledSpecial() throws Exception {
    Random r = new Random(1050);
    for (BigDecimal bigDecimal : specialBigDecimals) {
      doTestBigIntegerBytesScaled(r, bigDecimal);
    }
  }

  private void doTestBigIntegerBytesScaled(Random r, BigDecimal bigDecimal) throws IOException {

    HiveDecimalV1 oldDec = HiveDecimalV1.create(bigDecimal);
    if (oldDec != null && isTenPowerBug(oldDec.toString())) {
      return;
    }
    HiveDecimal dec = HiveDecimal.create(bigDecimal);
    if (oldDec == null) {
      assertTrue(dec == null);
      return;
    }
    assertTrue(dec != null);
    dec.validate();

    int scale = oldDec.scale();
    int newScale;
    if (scale == HiveDecimal.MAX_SCALE) {
      newScale = scale;
    } else {
      newScale = scale + r.nextInt(HiveDecimal.MAX_SCALE - scale);
    }

    HiveDecimalV1 oldDecScaled = oldDec.setScale(newScale);
    HiveDecimalWritableV1 oldDecScaledWritable = new HiveDecimalWritableV1(oldDecScaled);

    byte[] bytesExpected = oldDecScaledWritable.getInternalStorage();

    byte[] bytes = dec.bigIntegerBytesScaled(newScale);

    if (!StringExpr.equal(bytes, 0, bytes.length, bytesExpected, 0, bytesExpected.length)) {
      Assert.fail();
    }

    HiveDecimalWritableV1 oldDecWritableRetrieve = new HiveDecimalWritableV1(bytesExpected, newScale);
    HiveDecimalV1 oldDecRetrieve = oldDecWritableRetrieve.getHiveDecimal();
    Assert.assertTrue(oldDecRetrieve != null);

    HiveDecimal decRetrieve = HiveDecimal.createFromBigIntegerBytesAndScale(bytes, newScale);
    Assert.assertTrue(decRetrieve != null);

    Assert.assertEquals(oldDecRetrieve.toString(), decRetrieve.toString());
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomRoundFloor() {
    Random r = new Random(1052);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestRandomRound(r, standardAlphabet, bigDecimalFlavor, HiveDecimal.ROUND_FLOOR);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestRandomRound(r, sparseAlphabet, bigDecimalFlavor, HiveDecimal.ROUND_FLOOR);
      }
    }
  }

  private void doTestRandomRound(Random r, String digitAlphabet, BigDecimalFlavor bigDecimalFlavor, int roundingMode) {

    for (int i = 0; i < POUND_FACTOR; i++) {
      BigDecimal bigDecimal = randHiveBigDecimal(r, digitAlphabet, bigDecimalFlavor);

      doTestRound(r, bigDecimal, roundingMode);
    }
  }

  @Test
  public void testRoundFloorSpecial() {
    Random r = new Random(1050);
    for (BigDecimal bigDecimal : specialBigDecimals) {
      doTestRound(r, bigDecimal, HiveDecimal.ROUND_FLOOR);
    }
  }

  // Used by all flavors.
  private void doTestRound(Random r, BigDecimal bigDecimal, int roundingMode) {

    // Temporarily....
    bigDecimal = bigDecimal.abs();

    HiveDecimalV1 oldDec = HiveDecimalV1.create(bigDecimal);
    if (oldDec != null && isTenPowerBug(oldDec.toString())) {
      return;
    }
    HiveDecimal dec = HiveDecimal.create(bigDecimal);
    if (oldDec == null) {
      assertTrue(dec == null);
      return;
    }
    assertTrue(dec != null);
    dec.validate();

    HiveDecimalV1 oldScaledDec;
    HiveDecimal scaledDec;

    for (int newScale = -(2 * HiveDecimal.MAX_SCALE + 1);
         newScale <= 2 * HiveDecimal.MAX_SCALE + 1;
         newScale++) {

      oldScaledDec = oldDec.setScale(newScale, roundingMode);
      boolean isEqual;
      if (oldScaledDec == null) {
        scaledDec = dec.setScale(newScale, roundingMode);
        if (scaledDec != null) {
          Assert.fail();
        }
        return;
      } else {
        scaledDec = dec.setScale(newScale, roundingMode);
        if (scaledDec == null) {
          if (isTenPowerBug(oldScaledDec.toString())) {
            continue;
          }
          Assert.fail();
        }
        scaledDec.validate();
        isEqual = oldScaledDec.toString().equals(scaledDec.toString());
        if (!isEqual) {
          Assert.fail();
        }
      }
    }
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomRoundCeiling() {
    Random r = new Random(1053);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestRandomRound(r, standardAlphabet, bigDecimalFlavor, HiveDecimal.ROUND_CEILING);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestRandomRound(r, sparseAlphabet, bigDecimalFlavor, HiveDecimal.ROUND_CEILING);
      }
    }
  }

  @Test
  public void testRoundCeilingSpecial() {
    Random r = new Random(1050);
    for (BigDecimal bigDecimal : specialBigDecimals) {
      doTestRound(r, bigDecimal, HiveDecimal.ROUND_CEILING);
    }
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomRoundHalfUp() {
    Random r = new Random(1053);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestRandomRound(r, standardAlphabet, bigDecimalFlavor, HiveDecimal.ROUND_HALF_UP);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestRandomRound(r, sparseAlphabet, bigDecimalFlavor, HiveDecimal.ROUND_HALF_UP);
      }
    }
  }

  @Test
  public void testRoundHalfUpSpecial() {
    Random r = new Random(1050);
    for (BigDecimal bigDecimal : specialBigDecimals) {
      doTestRound(r, bigDecimal, HiveDecimal.ROUND_HALF_UP);
    }
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomRoundHalfEven() {
    Random r = new Random(1053);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestRandomRound(r, standardAlphabet, bigDecimalFlavor, HiveDecimal.ROUND_HALF_EVEN);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestRandomRound(r, sparseAlphabet, bigDecimalFlavor, HiveDecimal.ROUND_HALF_EVEN);
      }
    }
  }

  @Test
  public void testRoundHalfEvenSpecial() {
    Random r = new Random(1050);
    for (BigDecimal bigDecimal : specialBigDecimals) {
      doTestRound(r, bigDecimal, HiveDecimal.ROUND_HALF_EVEN);
    }
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomCompareTo() {
    Random r = new Random(1054);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestRandomCompareTo(r, standardAlphabet, bigDecimalFlavor);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestRandomCompareTo(r, sparseAlphabet, bigDecimalFlavor);
      }
    }
  }

  private void doTestRandomCompareTo(Random r, String digitAlphabet, BigDecimalFlavor bigDecimalFlavor) {

    for (BigDecimalPairFlavor bigDecimalPairFlavor : BigDecimalPairFlavor.values()) {
      for (int i = 0; i < POUND_FACTOR; i++) {
        BigDecimal[] pair = randHiveBigDecimalPair(r, digitAlphabet, bigDecimalFlavor, bigDecimalPairFlavor);

        doTestCompareTo(r, pair[0], pair[1]);
      }
    }
  }

  @Test
  public void testCompareToSpecial() {
    Random r = new Random(1050);
    for (BigDecimal bigDecimal : specialBigDecimals) {
      for (BigDecimal bigDecimal2 : specialBigDecimals) {
        doTestCompareTo(r, bigDecimal, bigDecimal2);
      }
    }
  }

  private void doTestCompareTo(Random r, BigDecimal bigDecimal, BigDecimal bigDecimal2) {

    HiveDecimalV1 oldDec = HiveDecimalV1.create(bigDecimal);
    if (oldDec != null && isTenPowerBug(oldDec.toString())) {
      return;
    }
    HiveDecimal dec = HiveDecimal.create(bigDecimal);
    if (oldDec == null) {
      assertTrue(dec == null);
      return;
    }
    assertTrue(dec != null);
    dec.validate();

    HiveDecimalV1 oldDec2 = HiveDecimalV1.create(bigDecimal2);
    if (oldDec2 != null && isTenPowerBug(oldDec2.toString())) {
      return;
    }
    HiveDecimal dec2 = HiveDecimal.create(bigDecimal2);
    if (oldDec2 == null) {
      assertTrue(dec2 == null);
      return;
    }
    assertTrue(dec2 != null);
    dec.validate();

    // Verify.
    Assert.assertEquals(oldDec.toString(), dec.toString());
    Assert.assertEquals(oldDec2.toString(), dec2.toString());

    int oldCompareTo;
    int compareTo;

    // Same object.
    oldCompareTo = oldDec.compareTo(oldDec);
    Assert.assertEquals(0, oldCompareTo);
    compareTo = dec.compareTo(dec);
    Assert.assertEquals(0, compareTo);

    // Two objects.
    oldCompareTo = oldDec.compareTo(oldDec2);
    compareTo = dec.compareTo(dec2);
    Assert.assertEquals(oldCompareTo, compareTo);

    int oldCompareToReverse = oldDec2.compareTo(oldDec);
    int compareToReverse = dec2.compareTo(dec);
    Assert.assertEquals(oldCompareToReverse, compareToReverse);
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomAdd() {
    Random r = new Random(1055);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestRandomAdd(r, standardAlphabet, bigDecimalFlavor);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestRandomAdd(r, sparseAlphabet, bigDecimalFlavor);
      }
    }
  }

  private void doTestRandomAdd(Random r, String digitAlphabet, BigDecimalFlavor bigDecimalFlavor) {

    for (BigDecimalPairFlavor bigDecimalPairFlavor : BigDecimalPairFlavor.values()) {
      for (int i = 0; i < POUND_FACTOR; i++) {
        BigDecimal[] pair = randHiveBigDecimalPair(r, digitAlphabet, bigDecimalFlavor, bigDecimalPairFlavor);
  
        doTestAdd(r, pair[0], pair[1]);
      }
    }
  }

  @Test
  public void testAddSpecial() {
    Random r = new Random(1050);
    for (BigDecimal bigDecimal : specialBigDecimals) {
      for (BigDecimal bigDecimal2 : specialBigDecimals) {
        doTestAdd(r, bigDecimal, bigDecimal2);
      }
    }
  }

  private void doTestAdd(Random r, BigDecimal bigDecimal, BigDecimal bigDecimal2) {

    HiveDecimalV1 oldDec = HiveDecimalV1.create(bigDecimal);
    if (oldDec != null && isTenPowerBug(oldDec.toString())) {
      return;
    }
    HiveDecimal dec = HiveDecimal.create(bigDecimal);
    if (oldDec == null) {
      assertTrue(dec == null);
      return;
    }
    assertTrue(dec != null);
    dec.validate();

    HiveDecimalV1 oldDec2 = HiveDecimalV1.create(bigDecimal2);
    if (oldDec2 != null && isTenPowerBug(oldDec2.toString())) {
      return;
    }
    HiveDecimal dec2 = HiveDecimal.create(bigDecimal2);
    if (oldDec2 == null) {
      assertTrue(dec2 == null);
      return;
    }
    assertTrue(dec2 != null);
    dec.validate();

    // Verify.
    Assert.assertEquals(oldDec.toString(), dec.toString());
    Assert.assertEquals(oldDec2.toString(), dec2.toString());

    // Add to self.
    HiveDecimalV1 oldAddDec;
    HiveDecimal addDec;

    oldAddDec = oldDec.add(oldDec);
    boolean isEqual;
    if (oldAddDec == null) {
      addDec = dec.add(dec);
      assertTrue(addDec == null);
      return;
    } else {
      addDec = dec.add(dec);
      if (addDec == null) {
        if (isTenPowerBug(oldAddDec.toString())) {
          return;
        }
        Assert.fail();
      }
      addDec.validate();
      isEqual = oldAddDec.toString().equals(addDec.toString());
      if (!isEqual) {
        Assert.fail();
      }
    }

    // Add two decimals.
    oldAddDec = oldDec.add(oldDec2);
    if (oldAddDec == null) {
      addDec = dec.add(dec2);
      assertTrue(addDec == null);
      return;
    } else {
      addDec = dec.add(dec2);
      if (addDec == null) {
        if (isTenPowerBug(oldAddDec.toString())) {
          return;
        }
        Assert.fail();
      }
      addDec.validate();
      isEqual = oldAddDec.toString().equals(addDec.toString());
      if (!isEqual) {
        Assert.fail();
      }
    }

    // Add negative self.

    oldAddDec = oldDec.add(oldDec.negate());
    if (oldAddDec == null) {
      addDec = dec.add(dec.negate());
      assertTrue(addDec == null);
      return;
    } else {
      addDec = dec.add(dec.negate());
      if (addDec == null) {
        if (isTenPowerBug(oldAddDec.toString())) {
          return;
        }
        Assert.fail();
      }
      addDec.validate();
      isEqual = oldAddDec.toString().equals(addDec.toString());
      if (!isEqual) {
        Assert.fail();
      }
    }
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomSubtract() {
    Random r = new Random(1055);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestRandomSubtract(r, standardAlphabet, bigDecimalFlavor);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestRandomSubtract(r, sparseAlphabet, bigDecimalFlavor);
      }
    }
  }

  private void doTestRandomSubtract(Random r, String digitAlphabet, BigDecimalFlavor bigDecimalFlavor) {

    for (BigDecimalPairFlavor bigDecimalPairFlavor : BigDecimalPairFlavor.values()) {
      for (int i = 0; i < POUND_FACTOR; i++) {
        BigDecimal[] pair = randHiveBigDecimalPair(r, digitAlphabet, bigDecimalFlavor, bigDecimalPairFlavor);

        doTestSubtract(r, pair[0], pair[1]);
      }
    }
  }

  @Test
  public void testSubtractSpecial() {
    Random r = new Random(1050);
    for (BigDecimal bigDecimal : specialBigDecimals) {
      for (BigDecimal bigDecimal2 : specialBigDecimals) {
        doTestSubtract(r, bigDecimal, bigDecimal2);
      }
    }
  }

  private void doTestSubtract(Random r, BigDecimal bigDecimal, BigDecimal bigDecimal2) {

    HiveDecimalV1 oldDec = HiveDecimalV1.create(bigDecimal);
    if (oldDec != null && isTenPowerBug(oldDec.toString())) {
      return;
    }
    HiveDecimal dec = HiveDecimal.create(bigDecimal);
    if (oldDec == null) {
      assertTrue(dec == null);
      return;
    }
    assertTrue(dec != null);
    dec.validate();

    HiveDecimalV1 oldDec2 = HiveDecimalV1.create(bigDecimal2);
    if (oldDec2 != null && isTenPowerBug(oldDec2.toString())) {
      return;
    }
    HiveDecimal dec2 = HiveDecimal.create(bigDecimal2);
    if (oldDec2 == null) {
      assertTrue(dec2 == null);
      return;
    }
    assertTrue(dec2 != null);
    dec.validate();

    // Verify.
    Assert.assertEquals(oldDec.toString(), dec.toString());
    Assert.assertEquals(oldDec2.toString(), dec2.toString());

    // Subtract from self.
    HiveDecimalV1 oldSubtractDec;
    HiveDecimal subtractDec;

    oldSubtractDec = oldDec.subtract(oldDec);
    Assert.assertEquals(0, oldSubtractDec.signum());
    subtractDec = dec.subtract(dec);
    Assert.assertEquals(0, subtractDec.signum());

    boolean isEqual;
    oldSubtractDec = oldDec.subtract(oldDec2);
    if (oldSubtractDec == null) {
      subtractDec = dec.subtract(dec2);
      assertTrue(subtractDec == null);
      return;
    } else {
      subtractDec = dec.subtract(dec2);
      if (subtractDec == null) {
        if (isTenPowerBug(oldSubtractDec.toString())) {
          return;
        }
        Assert.fail();
      }
      subtractDec.validate();
      isEqual = oldSubtractDec.toString().equals(subtractDec.toString());
      if (!isEqual) {
        Assert.fail();
      }
    }
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomMultiply() {
    Random r = new Random(1056);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestRandomMultiply(r, standardAlphabet, bigDecimalFlavor);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestRandomMultiply(r, sparseAlphabet, bigDecimalFlavor);
      }
    }
  }

  private void doTestRandomMultiply(Random r, String digitAlphabet, BigDecimalFlavor bigDecimalFlavor) {

    for (BigDecimalPairFlavor bigDecimalPairFlavor : BigDecimalPairFlavor.values()) {
      for (int i = 0; i < POUND_FACTOR; i++) {
        BigDecimal[] pair = randHiveBigDecimalPair(r, digitAlphabet, bigDecimalFlavor, bigDecimalPairFlavor);

        doTestMultiply(r, pair[0], pair[1]);
      }
    }
  }

  @Test
  public void testMultiplySpecial() {
    Random r = new Random(1050);
    for (BigDecimal bigDecimal : specialBigDecimals) {
      for (BigDecimal bigDecimal2 : specialBigDecimals) {
        doTestMultiply(r, bigDecimal, bigDecimal2);
      }
    }
  }

  private void doTestMultiply(Random r, BigDecimal bigDecimal, BigDecimal bigDecimal2) {

    HiveDecimalV1 oldDec = HiveDecimalV1.create(bigDecimal);
    if (oldDec != null && isTenPowerBug(oldDec.toString())) {
      return;
    }
    HiveDecimal dec = HiveDecimal.create(bigDecimal);
    if (oldDec == null) {
      assertTrue(dec == null);
      return;
    }
    assertTrue(dec != null);
    dec.validate();

    HiveDecimalV1 oldDec2 = HiveDecimalV1.create(bigDecimal2);
    if (oldDec2 != null && isTenPowerBug(oldDec2.toString())) {
      return;
    }
    HiveDecimal dec2 = HiveDecimal.create(bigDecimal2);
    if (oldDec2 == null) {
      assertTrue(dec2 == null);
      return;
    }
    assertTrue(dec2 != null);
    dec.validate();

    // Verify.
    Assert.assertEquals(oldDec.toString(), dec.toString());
    Assert.assertEquals(oldDec2.toString(), dec2.toString());

    // Multiply by self.
    BigDecimal bigDecimalMultiply = bigDecimal.multiply(bigDecimal);
    BigDecimal bigDecimalMultiplyAbs = bigDecimalMultiply.abs();
    String bigDecimalMultiplyAbsString = bigDecimalMultiplyAbs.toString();
    int digits = bigDecimalMultiplyAbsString.indexOf('.') != -1 ? bigDecimalMultiplyAbsString.length() - 1: bigDecimalMultiplyAbsString.length();
    HiveDecimalV1 oldMultiplyDec;
    HiveDecimal multiplyDec;

    oldMultiplyDec = oldDec.multiply(oldDec);
    boolean isEqual;
    if (oldMultiplyDec == null) {
      multiplyDec = dec.multiply(dec);
      if (multiplyDec != null) {
        Assert.fail();
      }
      return;
    } else {
      multiplyDec = dec.multiply(dec);
      if (multiplyDec == null) {
        if (isTenPowerBug(oldMultiplyDec.toString())) {
          return;
        }
        Assert.fail();
      }
      multiplyDec.validate();
      isEqual = oldMultiplyDec.toString().equals(multiplyDec.toString());
      if (!isEqual) {
        Assert.fail();
      }
    }

    bigDecimalMultiply = bigDecimal.multiply(bigDecimal2);
    bigDecimalMultiplyAbs = bigDecimalMultiply.abs();
    bigDecimalMultiplyAbsString = bigDecimalMultiplyAbs.toString();
    digits = bigDecimalMultiplyAbsString.indexOf('.') != -1 ? bigDecimalMultiplyAbsString.length() - 1: bigDecimalMultiplyAbsString.length();
    oldMultiplyDec = oldDec.multiply(oldDec2);
    if (oldMultiplyDec == null) {
      multiplyDec = dec.multiply(dec2);
      if (multiplyDec != null) {
        Assert.fail();
      }
      return;
    } else {
      multiplyDec = dec.multiply(dec2);
      if (multiplyDec == null) {
        if (isTenPowerBug(oldMultiplyDec.toString())) {
          return;
        }
        Assert.fail();
      }
      multiplyDec.validate();
      isEqual = oldMultiplyDec.toString().equals(multiplyDec.toString());
      if (!isEqual) {
        Assert.fail();
      }
    }
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomDecimal64() {
    Random r = new Random(2497);
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      doTestRandomDecimal64(r, standardAlphabet, bigDecimalFlavor);
    }
    for (BigDecimalFlavor bigDecimalFlavor : BigDecimalFlavor.values()) {
      for (String sparseAlphabet : sparseAlphabets) {
        doTestRandomDecimal64(r, sparseAlphabet, bigDecimalFlavor);
      }
    }
  }

  private void doTestRandomDecimal64(Random r, String digitAlphabet, BigDecimalFlavor bigDecimalFlavor) {

    for (int i = 0; i < POUND_FACTOR; i++) {
      BigDecimal bigDecimal = randHiveBigDecimal(r, digitAlphabet, bigDecimalFlavor);

      doTestDecimal64(r, bigDecimal);
    }
  }

  @Test
  public void testDecimal64Special() {
    Random r = new Random(198);
    for (BigDecimal bigDecimal : specialBigDecimals) {
      int precision = Math.min(bigDecimal.precision(), HiveDecimalWritable.DECIMAL64_DECIMAL_DIGITS);
      int scale = Math.min(bigDecimal.scale(), precision);
      doTestDecimal64(r, bigDecimal, precision, scale);
    }
  }

  private void doTestDecimal64(Random r, BigDecimal inputBigDecimal) {
    final int precision = 1 + r.nextInt(HiveDecimalWritable.DECIMAL64_DECIMAL_DIGITS);
    assertTrue(HiveDecimalWritable.isPrecisionDecimal64(precision));
    final int scale = r.nextInt(precision + 1);

    doTestDecimal64(r, inputBigDecimal, precision, scale);
  }

  private void doTestDecimal64(Random r, BigDecimal inputBigDecimal, int precision, int scale) {

    BigDecimal bigDecimal = inputBigDecimal;

    if (!bigDecimal.equals(BigDecimal.ZERO)) {
      while (true) {
        bigDecimal = bigDecimal.remainder(BigDecimal.valueOf(10).pow(precision - scale));
        bigDecimal = bigDecimal.setScale(scale, BigDecimal.ROUND_DOWN);
        if (!bigDecimal.unscaledValue().equals(BigInteger.ZERO)) {
          break;
        }
        bigDecimal = randHiveBigDecimalNormalRange(r, standardAlphabet);
      }
    }

    HiveDecimal dec = HiveDecimal.create(bigDecimal);
    assertTrue(dec != null);
    dec.validate();

    HiveDecimalWritable decWritable = new HiveDecimalWritable(dec);

    final long decimal64Long = decWritable.serialize64(scale);
    assertTrue(decimal64Long <= HiveDecimalWritable.getDecimal64AbsMax(precision));
    HiveDecimalWritable resultWritable = new HiveDecimalWritable(0);
    resultWritable.deserialize64(decimal64Long, scale);

    assertEquals(dec, resultWritable.getHiveDecimal());
  }

  //------------------------------------------------------------------------------------------------

  public static String displayBytes(byte[] bytes, int start, int length) {
    StringBuilder sb = new StringBuilder();
    for (int i = start; i < start + length; i++) {
      sb.append(String.format("\\%03d", (int) (bytes[i] & 0xff)));
    }
    return sb.toString();
  }
}
