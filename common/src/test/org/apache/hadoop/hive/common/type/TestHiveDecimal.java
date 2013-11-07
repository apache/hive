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

import org.junit.Assert;
import org.junit.Test;

public class TestHiveDecimal {

  @Test
  public void testPrecisionScaleEnforcement() {
    String decStr = "1786135888657847525803324040144343378.09799306448796128931113691624";
    HiveDecimal dec = HiveDecimal.create(decStr);
    Assert.assertEquals("1786135888657847525803324040144343378.0979930644879612893111369162", dec.toString());
    Assert.assertTrue("Decimal precision should not go above maximum",
        dec.precision() <= HiveDecimal.MAX_PRECISION);
    Assert.assertTrue("Decimal scale should not go above maximum", dec.scale() <= HiveDecimal.MAX_SCALE);

    BigDecimal bd = new BigDecimal(decStr);
    BigDecimal bd1 = HiveDecimal.enforcePrecisionScale(bd, 20, 5);
    Assert.assertNull(bd1);
    bd1 = HiveDecimal.enforcePrecisionScale(bd, 45, 5);
    Assert.assertEquals("1786135888657847525803324040144343378.09799", bd1.toString());
    bd1 = HiveDecimal.enforcePrecisionScale(bd, 45, 20);
    Assert.assertNull(bd1);

    dec = HiveDecimal.create(bd, false);
    Assert.assertNull(dec);

    dec = HiveDecimal.create("-1786135888657847525803324040144343378.09799306448796128931113691624");
    Assert.assertEquals("-1786135888657847525803324040144343378.0979930644879612893111369162", dec.toString());

    dec = HiveDecimal.create("005.34000");
    Assert.assertEquals(dec.precision(), 3);
    Assert.assertEquals(dec.scale(), 2);

    dec = HiveDecimal.create("178613588865784752580332404014434337809799306448796128931113691624");
    Assert.assertNull(dec);
  }
  
  @Test
  public void testTrailingZeroRemovalAfterEnforcement() {
    String decStr = "8.0900000000000000000000000000000123456";
    HiveDecimal dec = HiveDecimal.create(decStr);
    Assert.assertEquals("8.09", dec.toString());
  }
  
  @Test
  public void testMultiply() {
    HiveDecimal dec1 = HiveDecimal.create("0.1786135888657847525803");
    HiveDecimal dec2 = HiveDecimal.create("3.123456789");
    Assert.assertNull(dec1.multiply(dec2));

    dec1 = HiveDecimal.create("1786135888657847525803232322323234442321.4");
    dec2 = HiveDecimal.create("178613588865784752580302323232.3");
    Assert.assertNull(dec1.multiply(dec2));

    dec1 = HiveDecimal.create("47.324");
    dec2 = HiveDecimal.create("9232.309");
    Assert.assertEquals("436909.791116", dec1.multiply(dec2).toString());
  }

  @Test
  public void testPow() {
    HiveDecimal dec = HiveDecimal.create("3.1415926");
    Assert.assertEquals(dec.pow(2), dec.multiply(dec));

    HiveDecimal dec1 = HiveDecimal.create("0.17861358882");
    dec1 = dec1.pow(3);
    Assert.assertNull(dec1);
  }

  @Test
  public void testDivide() {
    HiveDecimal dec1 = HiveDecimal.create("3.14");
    HiveDecimal dec2 = HiveDecimal.create("3");
    Assert.assertNotNull(dec1.divide(dec2));
  }

  @Test
  public void testPlus() {
    HiveDecimal dec1 = HiveDecimal.create("99999999999999999999999999999999999");
    HiveDecimal dec2 = HiveDecimal.create("1");
    Assert.assertNotNull(dec1.add(dec2));
  }

  @Test
  public void testException() {
    HiveDecimal dec = HiveDecimal.create("3.1415.926");
    Assert.assertNull(dec);
    dec = HiveDecimal.create("3abc43");
    Assert.assertNull(dec);
  }

}
