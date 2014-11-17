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
package org.apache.hadoop.hive.common.type;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.google.code.tempusfugit.concurrency.annotations.*;
import com.google.code.tempusfugit.concurrency.*;
import org.junit.*;
import static org.junit.Assert.*;

public class TestHiveDecimal {

  @Rule public ConcurrentRule concurrentRule = new ConcurrentRule();
  @Rule public RepeatingRule repeatingRule = new RepeatingRule();

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testPrecisionScaleEnforcement() {
    String decStr = "1786135888657847525803324040144343378.09799306448796128931113691624";
    HiveDecimal dec = HiveDecimal.create(decStr);
    Assert.assertEquals("1786135888657847525803324040144343378.1", dec.toString());
    Assert.assertTrue("Decimal precision should not go above maximum",
        dec.precision() <= HiveDecimal.MAX_PRECISION);
    Assert.assertTrue("Decimal scale should not go above maximum", dec.scale() <= HiveDecimal.MAX_SCALE);

    decStr = "57847525803324040144343378.09799306448796128931113691624";
    BigDecimal bd = new BigDecimal(decStr);
    BigDecimal bd1 = HiveDecimal.enforcePrecisionScale(bd, 20, 5);
    Assert.assertNull(bd1);
    bd1 = HiveDecimal.enforcePrecisionScale(bd, 35, 5);
    Assert.assertEquals("57847525803324040144343378.09799", bd1.toString());
    bd1 = HiveDecimal.enforcePrecisionScale(bd, 45, 20);
    Assert.assertNull(bd1);

    dec = HiveDecimal.create(bd, false);
    Assert.assertNull(dec);

    dec = HiveDecimal.create("-1786135888657847525803324040144343378.09799306448796128931113691624");
    Assert.assertEquals("-1786135888657847525803324040144343378.1", dec.toString());

    dec = HiveDecimal.create("005.34000");
    Assert.assertEquals(dec.precision(), 3);
    Assert.assertEquals(dec.scale(), 2);

    dec = HiveDecimal.create("178613588865784752580332404014434337809799306448796128931113691624");
    Assert.assertNull(dec);

    // Rounding numbers that increase int digits
    Assert.assertEquals("10",
        HiveDecimal.enforcePrecisionScale(new BigDecimal("9.5"), 2, 0).toString());
    Assert.assertNull(HiveDecimal.enforcePrecisionScale(new BigDecimal("9.5"), 1, 0));
    Assert.assertEquals("9",
        HiveDecimal.enforcePrecisionScale(new BigDecimal("9.4"), 1, 0).toString());
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testTrailingZeroRemovalAfterEnforcement() {
    String decStr = "8.090000000000000000000000000000000000000123456";
    HiveDecimal dec = HiveDecimal.create(decStr);
    Assert.assertEquals("8.09", dec.toString());
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testMultiply() {
    HiveDecimal dec1 = HiveDecimal.create("0.00001786135888657847525803");
    HiveDecimal dec2 = HiveDecimal.create("3.0000123456789");
    Assert.assertNull(dec1.multiply(dec2));

    dec1 = HiveDecimal.create("178613588865784752580323232232323444.4");
    dec2 = HiveDecimal.create("178613588865784752580302323232.3");
    Assert.assertNull(dec1.multiply(dec2));

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
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testPow() {
    HiveDecimal dec = HiveDecimal.create("3.00001415926");
    Assert.assertEquals(dec.pow(2), dec.multiply(dec));

    HiveDecimal dec1 = HiveDecimal.create("0.000017861358882");
    dec1 = dec1.pow(3);
    Assert.assertNull(dec1);

    dec1 = HiveDecimal.create("3.140");
    Assert.assertEquals("9.8596", dec1.pow(2).toString());
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
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
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testPlus() {
    HiveDecimal dec1 = HiveDecimal.create("99999999999999999999999999999999999");
    HiveDecimal dec2 = HiveDecimal.create("1");
    Assert.assertNotNull(dec1.add(dec2));

    dec1 = HiveDecimal.create("3.140");
    dec2 = HiveDecimal.create("1.00");
    Assert.assertEquals("4.14", dec1.add(dec2).toString());
  }


  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testSubtract() {
      HiveDecimal dec1 = HiveDecimal.create("3.140");
      HiveDecimal dec2 = HiveDecimal.create("1.00");
      Assert.assertEquals("2.14", dec1.subtract(dec2).toString());
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testPosMod() {
    HiveDecimal hd1 = HiveDecimal.create("-100.91");
    HiveDecimal hd2 = HiveDecimal.create("9.8");
    HiveDecimal dec = hd1.remainder(hd2).add(hd2).remainder(hd2);
    Assert.assertEquals("6.89", dec.toString());
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testHashCode() {
      Assert.assertEquals(HiveDecimal.create("9").hashCode(), HiveDecimal.create("9.00").hashCode());
      Assert.assertEquals(HiveDecimal.create("0").hashCode(), HiveDecimal.create("0.00").hashCode());
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testException() {
    HiveDecimal dec = HiveDecimal.create("3.1415.926");
    Assert.assertNull(dec);
    dec = HiveDecimal.create("3abc43");
    Assert.assertNull(dec);
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testBinaryConversion() {
    testBinaryConversion("0.00");
    testBinaryConversion("-12.25");
    testBinaryConversion("234.79");
  }

  private void testBinaryConversion(String num) {
    HiveDecimal dec = HiveDecimal.create(num);
    int scale = 2;
    byte[] d = dec.setScale(2).unscaledValue().toByteArray();
    Assert.assertEquals(dec, HiveDecimal.create(new BigInteger(d), scale));
    int prec = 5;
    int len =  (int)
        Math.ceil((Math.log(Math.pow(10, prec) - 1) / Math.log(2) + 1) / 8);
    byte[] res = new byte[len];
    if ( dec.signum() == -1)
      for (int i = 0; i < len; i++)
        res[i] |= 0xFF;
    System.arraycopy(d, 0, res, len-d.length, d.length); // Padding leading zeros.
    Assert.assertEquals(dec, HiveDecimal.create(new BigInteger(res), scale));
  }

}
