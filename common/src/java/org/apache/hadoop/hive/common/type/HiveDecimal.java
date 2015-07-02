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
import java.math.RoundingMode;

/**
 *
 * HiveDecimal. Simple wrapper for BigDecimal. Adds fixed max precision and non scientific string
 * representation
 *
 */
public class HiveDecimal implements Comparable<HiveDecimal> {
  public static final int MAX_PRECISION = 38;
  public static final int MAX_SCALE = 38;

  /**
   * Default precision/scale when user doesn't specify in the column metadata, such as
   * decimal and decimal(8).
   */
  public static final int USER_DEFAULT_PRECISION = 10;
  public static final int USER_DEFAULT_SCALE = 0;

  /**
   *  Default precision/scale when system is not able to determine them, such as in case
   *  of a non-generic udf.
   */
  public static final int SYSTEM_DEFAULT_PRECISION = 38;
  public static final int SYSTEM_DEFAULT_SCALE = 18;

  public static final HiveDecimal ZERO = new HiveDecimal(BigDecimal.ZERO);
  public static final HiveDecimal ONE = new HiveDecimal(BigDecimal.ONE);

  public static final int ROUND_FLOOR = BigDecimal.ROUND_FLOOR;
  public static final int ROUND_CEILING = BigDecimal.ROUND_CEILING;
  public static final int ROUND_HALF_UP = BigDecimal.ROUND_HALF_UP;

  private BigDecimal bd = BigDecimal.ZERO;

  private HiveDecimal(BigDecimal bd) {
    this.bd = bd;
  }

  public static HiveDecimal create(BigDecimal b) {
    return create(b, true);
  }

  public static HiveDecimal create(BigDecimal b, boolean allowRounding) {
    BigDecimal bd = normalize(b, allowRounding);
    return bd == null ? null : new HiveDecimal(bd);
  }

  public static HiveDecimal create(BigInteger unscaled, int scale) {
    BigDecimal bd = normalize(new BigDecimal(unscaled, scale), true);
    return bd == null ? null : new HiveDecimal(bd);
  }

  public static HiveDecimal create(String dec) {
    BigDecimal bd;
    try {
      bd = new BigDecimal(dec);
    } catch (NumberFormatException ex) {
      return null;
    }

    bd = normalize(bd, true);
    return bd == null ? null : new HiveDecimal(bd);
  }

  public static HiveDecimal create(BigInteger bi) {
    BigDecimal bd = normalize(new BigDecimal(bi), true);
    return bd == null ? null : new HiveDecimal(bd);
  }

  public static HiveDecimal create(int i) {
    return new HiveDecimal(new BigDecimal(i));
  }

  public static HiveDecimal create(long l) {
    return new HiveDecimal(new BigDecimal(l));
  }

  @Override
  public String toString() {
     return bd.toPlainString();
  }

  public HiveDecimal setScale(int i) {
    return new HiveDecimal(bd.setScale(i, RoundingMode.HALF_UP));
  }

  @Override
  public int compareTo(HiveDecimal dec) {
    return bd.compareTo(dec.bd);
  }

  @Override
  public int hashCode() {
    return bd.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != getClass()) {
      return false;
    }
    return bd.equals(((HiveDecimal) obj).bd);
  }

  public int scale() {
    return bd.scale();
  }

  /**
   * Returns the number of digits (integer and fractional) in the number, which is equivalent
   * to SQL decimal precision. Note that this is different from BigDecimal.precision(),
   * which returns the precision of the unscaled value (BigDecimal.valueOf(0.01).precision() = 1,
   * whereas HiveDecimal.create("0.01").precision() = 2).
   * If you want the BigDecimal precision, use HiveDecimal.bigDecimalValue().precision()
   * @return
   */
  public int precision() {
    int bdPrecision = bd.precision();
    int bdScale = bd.scale();

    if (bdPrecision < bdScale) {
      // This can happen for numbers less than 0.1
      // For 0.001234: bdPrecision=4, bdScale=6
      // In this case, we'll set the type to have the same precision as the scale.
      return bdScale;
    }
    return bdPrecision;
  }

  public int intValue() {
    return bd.intValue();
  }

  public double doubleValue() {
    return bd.doubleValue();
  }

  public long longValue() {
    return bd.longValue();
  }

  public short shortValue() {
    return bd.shortValue();
  }

  public float floatValue() {
    return bd.floatValue();
  }

  public BigDecimal bigDecimalValue() {
    return bd;
  }

  public byte byteValue() {
    return bd.byteValue();
  }

  public HiveDecimal setScale(int adjustedScale, int rm) {
    return create(bd.setScale(adjustedScale, rm));
  }

  public HiveDecimal subtract(HiveDecimal dec) {
    return create(bd.subtract(dec.bd));
  }

  public HiveDecimal multiply(HiveDecimal dec) {
    return create(bd.multiply(dec.bd), false);
  }

  public BigInteger unscaledValue() {
    return bd.unscaledValue();
  }

  public HiveDecimal scaleByPowerOfTen(int n) {
    return create(bd.scaleByPowerOfTen(n));
  }

  public HiveDecimal abs() {
    return create(bd.abs());
  }

  public HiveDecimal negate() {
    return create(bd.negate());
  }

  public HiveDecimal add(HiveDecimal dec) {
    return create(bd.add(dec.bd));
  }

  public HiveDecimal pow(int n) {
    BigDecimal result = normalize(bd.pow(n), false);
    return result == null ? null : new HiveDecimal(result);
  }

  public HiveDecimal remainder(HiveDecimal dec) {
    return create(bd.remainder(dec.bd));
  }

  public HiveDecimal divide(HiveDecimal dec) {
    return create(bd.divide(dec.bd, MAX_SCALE, RoundingMode.HALF_UP), true);
  }

  /**
   * Get the sign of the underlying decimal.
   * @return 0 if the decimal is equal to 0, -1 if less than zero, and 1 if greater than 0
   */
  public int signum() {
    return bd.signum();
  }

  private static BigDecimal trim(BigDecimal d) {
    if (d.compareTo(BigDecimal.ZERO) == 0) {
      // Special case for 0, because java doesn't strip zeros correctly on that number.
      d = BigDecimal.ZERO;
    } else {
      d = d.stripTrailingZeros();
      if (d.scale() < 0) {
        // no negative scale decimals
        d = d.setScale(0);
      }
    }
    return d;
  }

  private static BigDecimal normalize(BigDecimal bd, boolean allowRounding) {
    if (bd == null) {
      return null;
    }

    bd = trim(bd);

    int intDigits = bd.precision() - bd.scale();

    if (intDigits > MAX_PRECISION) {
      return null;
    }

    int maxScale = Math.min(MAX_SCALE, Math.min(MAX_PRECISION - intDigits, bd.scale()));
    if (bd.scale() > maxScale ) {
      if (allowRounding) {
        bd = bd.setScale(maxScale, RoundingMode.HALF_UP);
        // Trimming is again necessary, because rounding may introduce new trailing 0's.
        bd = trim(bd);
      } else {
        bd = null;
      }
    }

    return bd;
  }

  public static BigDecimal enforcePrecisionScale(BigDecimal bd, int maxPrecision, int maxScale) {
    if (bd == null) {
      return null;
    }

    bd = trim(bd);

    if (bd.scale() > maxScale) {
      bd = bd.setScale(maxScale, RoundingMode.HALF_UP);
    }

    int maxIntDigits = maxPrecision - maxScale;
    int intDigits = bd.precision() - bd.scale();
    if (intDigits > maxIntDigits) {
      return null;
    }

    return bd;
  }

  public static HiveDecimal enforcePrecisionScale(HiveDecimal dec, int maxPrecision, int maxScale) {
    if (dec == null) {
      return null;
    }

    BigDecimal bd = enforcePrecisionScale(dec.bd, maxPrecision, maxScale);
    if (bd == null) {
      return null;
    }

    return HiveDecimal.create(bd);
  }
}
