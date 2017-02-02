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

import java.util.Random;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.hadoop.hive.common.type.RandomTypeUtil;

public class HiveDecimalTestBase {

  public static int POUND_FACTOR = 1000;

  public static enum BigDecimalFlavor {
    NORMAL_RANGE,
    FRACTIONS_ONLY,
    NEGATIVE_SCALE,
    LONG_TAIL
  }

  public static enum BigDecimalPairFlavor {
    RANDOM,
    NEAR,
    INVERSE
  }

  public BigDecimal randHiveBigDecimal(Random r, String digitAlphabet, BigDecimalFlavor bigDecimalFlavor) {
    switch (bigDecimalFlavor) {
    case NORMAL_RANGE:
      return randHiveBigDecimalNormalRange(r, digitAlphabet);
    case FRACTIONS_ONLY:
      return randHiveBigDecimalFractionsOnly(r, digitAlphabet);
    case NEGATIVE_SCALE:
      return randHiveBigDecimalNegativeScale(r, digitAlphabet);
    case LONG_TAIL:
      return randHiveBigDecimalLongTail(r, digitAlphabet);
    default:
      throw new RuntimeException("Unexpected big decimal flavor " + bigDecimalFlavor);
    }
  }

  public BigDecimal[] randHiveBigDecimalPair(Random r, String digitAlphabet,
      BigDecimalFlavor bigDecimalFlavor, BigDecimalPairFlavor bigDecimalPairFlavor) {
    BigDecimal[] pair = new BigDecimal[2];
    BigDecimal bigDecimal1 = randHiveBigDecimal(r, digitAlphabet, bigDecimalFlavor);
    pair[0] = bigDecimal1;

    BigDecimal bigDecimal2;
    switch (bigDecimalPairFlavor) {
    case RANDOM:
      bigDecimal2 = randHiveBigDecimal(r, digitAlphabet, bigDecimalFlavor);
      break;
    case NEAR:
      bigDecimal2 = randHiveBigDecimalNear(r, bigDecimal1);
      break;
    case INVERSE:
      bigDecimal2 = randHiveBigDecimalNear(r, bigDecimal1);
      break;
    default:
      throw new RuntimeException("Unexpected big decimal pair flavor " + bigDecimalPairFlavor);
    }
    pair[1] = bigDecimal2;
    return pair;
  }

  public BigDecimal randHiveBigDecimalNormalRange(Random r, String digitAlphabet) {
    String digits = RandomTypeUtil.getRandString(r, digitAlphabet, 1 + r.nextInt(38));
    BigInteger bigInteger = new BigInteger(digits);
    boolean negated = false;
    if (r.nextBoolean()) {
      bigInteger = bigInteger.negate();
      negated = true;
    }
    int scale = 0 + r.nextInt(38 + 1);
    return new BigDecimal(bigInteger, scale);
  }

  public BigDecimal randHiveBigDecimalNegativeScale(Random r, String digitAlphabet) {
    String digits = RandomTypeUtil.getRandString(r, digitAlphabet, 1 + r.nextInt(38));
    BigInteger bigInteger = new BigInteger(digits);
    boolean negated = false;
    if (r.nextBoolean()) {
      bigInteger = bigInteger.negate();
      negated = true;
    }
    int scale = 0 + (r.nextBoolean() ? 0 : r.nextInt(38 + 1));
    if (r.nextBoolean()) {
      scale = -scale;
    }
    return new BigDecimal(bigInteger, scale);
  }

  public BigDecimal randHiveBigDecimalLongTail(Random r, String digitAlphabet) {
    int scale = 0 + r.nextInt(38 + 20);
    final int maxDigits = 38 + (scale == 0 ? 0 : 20);
    String digits = RandomTypeUtil.getRandString(r, digitAlphabet, 1 + r.nextInt(maxDigits));
    BigInteger bigInteger = new BigInteger(digits);
    boolean negated = false;
    if (r.nextBoolean()) {
      bigInteger = bigInteger.negate();
      negated = true;
    }
    return new BigDecimal(bigInteger, scale);
  }

  public BigDecimal randHiveBigDecimalFractionsOnly(Random r, String digitAlphabet) {
    int scale = 1 + r.nextInt(38 + 1);
    String digits = RandomTypeUtil.getRandString(r, digitAlphabet, 1 + r.nextInt(scale));
    BigInteger bigInteger = new BigInteger(digits);
    boolean negated = false;
    if (r.nextBoolean()) {
      bigInteger = bigInteger.negate();
      negated = true;
    }
    return new BigDecimal(bigInteger, scale);
  }

  public BigDecimal randHiveBigDecimalNear(Random r, BigDecimal bigDecimal) {

    int scale = bigDecimal.scale();
    int delta = r.nextInt(10);
    if (r.nextBoolean()) {
      return bigDecimal.add(new BigDecimal(BigInteger.valueOf(delta), scale));
    } else {
      return bigDecimal.subtract(new BigDecimal(BigInteger.valueOf(delta), scale));
    }
  }

  public BigDecimal randHiveBigDecimalInverse(Random r, BigDecimal bigDecimal) {
    if (bigDecimal.signum() == 0) {
      return bigDecimal;
    }
    return BigDecimal.ONE.divide(bigDecimal);
  }

  public BigInteger randHiveBigInteger(Random r, String digitAlphabet) {
    String digits = RandomTypeUtil.getRandString(r, digitAlphabet, 1 + r.nextInt(38));
    BigInteger bigInteger = new BigInteger(digits);
    boolean negated = false;
    if (r.nextBoolean()) {
      bigInteger = bigInteger.negate();
      negated = true;
    }
    return bigInteger;
  }

  public boolean isTenPowerBug(String string) {
    // // System.out.println("TEST_IS_TEN_TO_38_STRING isTenPowerBug " + string);
    if (string.charAt(0) == '-') {
      string = string.substring(1);
    }
    int index = string.indexOf('.');
    if (index != -1) {
      if (index == 0) {
        string = string.substring(1);
      } else {
        string = string.substring(0, index) + string.substring(index + 1);
      }
    }
    // // System.out.println("TEST_IS_TEN_TO_38_STRING isTenPowerBug " + string);
    return string.equals("100000000000000000000000000000000000000");
  }

  //------------------------------------------------------------------------------------------------

  public static String[] specialDecimalStrings = new String[] {
    "0",
    "1",
    "-1",
    "10",
    "-10",
    "100",
    "-100",
    "127",                                          // Byte.MAX_VALUE
    "127.1",
    "127.0008",
    "127.49",
    "127.5",
    "127.9999999999999999999",
    "-127",
    "-127.1",
    "-127.0008",
    "-127.49",
    "-127.5",
    "-127.999999",
    "128",
    "128.1",
    "128.0008",
    "128.49",
    "128.5",
    "128.9999999999999999999",
    "-128",                                         // Byte.MIN_VALUE
    "-128.1",
    "-128.0008",
    "-128.49",
    "-128.5",
    "-128.999",
    "129",
    "129.1",
    "-129",
    "-129.1",
    "1000",
    "-1000",
    "10000",
    "-10000",
    "32767",                                        // Short.MAX_VALUE
    "32767.1",
    "32767.0008",
    "32767.49",
    "32767.5",
    "32767.99999999999",
    "-32767",
    "-32767.1",
    "-32767.0008",
    "-32767.49",
    "-32767.5",
    "-32767.9",
    "32768",
    "32768.1",
    "32768.0008",
    "32768.49",
    "32768.5",
    "32768.9999999999",
    "-32768",                                       // Short.MIN_VALUE
    "-32768.1",
    "-32768.0008",
    "-32768.49",
    "-32768.5",
    "-32768.9999999",
    "32769",
    "32769.1",
    "-32769",
    "-32769.1",
    "100000",
    "-100000",
    "1000000",
    "-1000000",
    "10000000",
    "-10000000",
    "100000000",
    "99999999",                                     // 10^8 - 1
    "-99999999",
    "-100000000",
    "1000000000",
    "-1000000000",
    "2147483647",                                  // Integer.MAX_VALUE
    "2147483647.1",
    "2147483647.0008",
    "2147483647.49",
    "2147483647.5",
    "2147483647.9999999999",
    "-2147483647",
    "-2147483647.1",
    "-2147483647.0008",
    "-2147483647.49",
    "-2147483647.5",
    "-2147483647.9999999999999999999",
    "2147483648",
    "2147483648.1",
    "2147483648.0008",
    "2147483648.49",
    "2147483648.5",
    "2147483648.9",
    "-2147483648",                                 // Integer.MIN_VALUE
    "-2147483648.1",
    "-2147483648.0008",
    "-2147483648.49",
    "-2147483648.5",
    "-2147483648.999",
    "2147483649",
    "2147483649.1",
    "-2147483649",
    "-2147483649.1",
    "10000000000",
    "-10000000000",
    "100000000000",
    "-100000000000",
    "1000000000000",
    "-1000000000000",
    "10000000000000",
    "-10000000000000",
    "100000000000000",
    "-100000000000000",
    "999999999999999",
    "-999999999999999",
    "1000000000000000",                            // 10^15
    "-1000000000000000",
    "9999999999999999",                            // 10^16 - 1
    "-9999999999999999",
    "10000000000000000",                           // 10^16
    "-10000000000000000",
    "100000000000000000",
    "-100000000000000000",
    "1000000000000000000",
    "-1000000000000000000",
    "9223372036854775807",                         // Long.MAX_VALUE
    "9223372036854775807.1",
    "9223372036854775807.0008",
    "9223372036854775807.49",
    "9223372036854775807.5",
    "9223372036854775807.9",
    "-9223372036854775807",
    "-9223372036854775807.1",
    "-9223372036854775807.0008",
    "-9223372036854775807.49",
    "-9223372036854775807.5",
    "-9223372036854775807.9999999999999999999",
    "-9223372036854775808",
    "-9223372036854775808.1",
    "9223372036854775808",
    "9223372036854775808.1",
    "9223372036854775808.0008",
    "9223372036854775808.49",
    "9223372036854775808.5",
    "9223372036854775808.9",
    "9223372036854775809",
    "9223372036854775809.1",
    "-9223372036854775808",                        // Long.MIN_VALUE
    "-9223372036854775808.1",
    "-9223372036854775808.0008",
    "-9223372036854775808.49",
    "-9223372036854775808.5",
    "-9223372036854775808.9999999",
    "9223372036854775809",
    "9223372036854775809.1",
    "-9223372036854775809",
    "-9223372036854775809.1",
    "10000000000000000000000000000000",            // 10^31
    "-10000000000000000000000000000000",
    "99999999999999999999999999999999",            // 10^32 - 1
    "-99999999999999999999999999999999", 
    "100000000000000000000000000000000",           // 10^32
    "-100000000000000000000000000000000",
    "10000000000000000000000000000000000000",      // 10^37
    "-10000000000000000000000000000000000000",
    "99999999999999999999999999999999999999",      // 10^38 - 1
    "-99999999999999999999999999999999999999",
    "100000000000000000000000000000000000000",     // 10^38
    "-100000000000000000000000000000000000000", 
    "1000000000000000000000000000000000000000",    // 10^39
    "-1000000000000000000000000000000000000000",

    "18446744073709551616",                        // Unsigned 64 max.
    "-18446744073709551616",
    "340282366920938463463374607431768211455",     // 2^128 - 1
    "-340282366920938463463374607431768211455",

    "0.999999999999999",
    "-0.999999999999999",
    "0.0000000000000001",                          // 10^-15
    "-0.0000000000000001",
    "0.9999999999999999",
    "-0.9999999999999999",
    "0.00000000000000001",                         // 10^-16
    "-0.00000000000000001",
    "0.00000000000000000000000000000001",          // 10^-31
    "-0.00000000000000000000000000000001",
    "0.99999999999999999999999999999999",          // 10^-32 + 1
    "-0.99999999999999999999999999999999",
    "0.000000000000000000000000000000001",         // 10^-32
    "-0.000000000000000000000000000000001",
    "0.00000000000000000000000000000000000001",    // 10^-37
    "-0.00000000000000000000000000000000000001",
    "0.99999999999999999999999999999999999999",    // 10^-38 + 1
    "-0.99999999999999999999999999999999999999",
    "0.000000000000000000000000000000000000001",   // 10^-38
    "-0.000000000000000000000000000000000000001",
    "0.0000000000000000000000000000000000000001",  // 10^-39
    "-0.0000000000000000000000000000000000000001",
    "0.0000000000000000000000000000000000000005",  // 10^-39  (rounds)
    "-0.0000000000000000000000000000000000000005",
    "0.340282366920938463463374607431768211455",   // (2^128 - 1) * 10^-39
    "-0.340282366920938463463374607431768211455",
    "0.000000000000000000000000000000000000001",   // 10^-38
    "-0.000000000000000000000000000000000000001",
    "0.000000000000000000000000000000000000005",   // 10^-38
    "-0.000000000000000000000000000000000000005",

    "234.79",
    "342348.343",
    "12.25",
    "-12.25",
    "72057594037927935",                           // 2^56 - 1
    "-72057594037927935",
    "72057594037927936",                           // 2^56
    "-72057594037927936",
    "5192296858534827628530496329220095",          // 2^56 * 2^56 - 1
    "-5192296858534827628530496329220095",
    "5192296858534827628530496329220096",          // 2^56 * 2^56
    "-5192296858534827628530496329220096",

    "54216721532321902598.70",
    "-906.62545207002374150309544832320",
    "-0.0709351061072",
    "1460849063411925.53",
    "8.809130E-33",
    "-4.0786300706013636202E-20",
    "-3.8823936518E-1",
    "-3.8823936518E-28",
    "-3.8823936518E-29",
    "598575157855521918987423259.94094",
    "299999448432.001342152474197",
    "1786135888657847525803324040144343378.09799306448796128931113691624",  // More than 38 digits.
    "-1786135888657847525803324040144343378.09799306448796128931113691624",
    "57847525803324040144343378.09799306448796128931113691624",
    "0.999999999999999999990000",
    "005.34000",
    "1E-90",

    "0.4",
    "-0.4",
    "0.5",
    "-0.5",
    "0.6",
    "-0.6",
    "1.4",
    "-1.4",
    "1.5",
    "-1.5",
    "1.6",
    "-1.6",
    "2.4",
    "-2.4",
    "2.49",
    "-2.49",
    "2.5",
    "-2.5",
    "2.51",
    "-2.51",
    "-2.5",
    "2.6",
    "-2.6",
    "3.00001415926",
    "0.00",
    "-12.25",
    "234.79"
  };

  public static BigDecimal[] specialBigDecimals = stringArrayToBigDecimals(specialDecimalStrings);

  // decimal_1_1.txt
  public static String[] decimal_1_1_txt = {
    "0.0",
    "0.0000",
    ".0",
    "0.1",
    "0.15",
    "0.9",
    "0.94",
    "0.99",
    "0.345",
    "1.0",
    "1",
    "0",
    "00",
    "22",
    "1E-9",
    "-0.0",
    "-0.0000",
    "-.0",
    "-0.1",
    "-0.15",
    "-0.9",
    "-0.94",
    "-0.99",
    "-0.345",
    "-1.0",
    "-1",
    "-0",
    "-00",
    "-22",
    "-1E-9"
  };

  // kv7.txt KEYS
  public static String[] kv7_txt_keys = {
    "-4400",
    "1E+99",
    "1E-99",
    "0",
    "100",
    "10",
    "1",
    "0.1",
    "0.01",
    "200",
    "20",
    "2",
    "0",
    "0.2",
    "0.02",
    "0.3",
    "0.33",
    "0.333",
    "-0.3",
    "-0.33",
    "-0.333",
    "1.0",
    "2",
    "3.14",
    "-1.12",
    "-1.12",
    "-1.122",
    "1.12",
    "1.122",
    "124.00",
    "125.2",
    "-1255.49",
    "3.14",
    "3.14",
    "3.140",
    "0.9999999999999999999999999",
    "-1234567890.1234567890",
    "1234567890.1234567800"
  };

  public static String standardAlphabet = "0123456789";

  public static String[] sparseAlphabets = new String[] {

    "0000000000000000000000000000000000000003",
    "0000000000000000000000000000000000000009",
    "0000000000000000000000000000000000000001",
    "0000000000000000000003",
    "0000000000000000000009",
    "0000000000000000000001",
    "0000000000091",
    "000000000005",
    "9",
    "5555555555999999999000000000000001111111",
    "24680",
    "1"
  };

  public static BigDecimal[] stringArrayToBigDecimals(String[] strings) {
    BigDecimal[] result = new BigDecimal[strings.length];
    for (int i = 0; i < strings.length; i++) {
      result[i] = new BigDecimal(strings[i]);
    }
    return result;
  }
}