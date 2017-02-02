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
public final class HiveDecimalV1 implements Comparable<HiveDecimalV1> {
  @HiveDecimalVersionV1
  public static final int MAX_PRECISION = 38;
  @HiveDecimalVersionV1
  public static final int MAX_SCALE = 38;

  /**
   * Default precision/scale when user doesn't specify in the column metadata, such as
   * decimal and decimal(8).
   */
  @HiveDecimalVersionV1
  public static final int USER_DEFAULT_PRECISION = 10;
  @HiveDecimalVersionV1
  public static final int USER_DEFAULT_SCALE = 0;

  /**
   *  Default precision/scale when system is not able to determine them, such as in case
   *  of a non-generic udf.
   */
  @HiveDecimalVersionV1
  public static final int SYSTEM_DEFAULT_PRECISION = 38;
  @HiveDecimalVersionV1
  public static final int SYSTEM_DEFAULT_SCALE = 18;

  @HiveDecimalVersionV1
  public static final HiveDecimalV1 ZERO = new HiveDecimalV1(BigDecimal.ZERO);
  @HiveDecimalVersionV1
  public static final HiveDecimalV1 ONE = new HiveDecimalV1(BigDecimal.ONE);

  @HiveDecimalVersionV1
  public static final int ROUND_FLOOR = BigDecimal.ROUND_FLOOR;
  @HiveDecimalVersionV1
  public static final int ROUND_CEILING = BigDecimal.ROUND_CEILING;
  @HiveDecimalVersionV1
  public static final int ROUND_HALF_UP = BigDecimal.ROUND_HALF_UP;
  @HiveDecimalVersionV1
  public static final int ROUND_HALF_EVEN = BigDecimal.ROUND_HALF_EVEN;

  private BigDecimal bd = BigDecimal.ZERO;

  private HiveDecimalV1(BigDecimal bd) {
    this.bd = bd;
  }

  @HiveDecimalVersionV1
  public static HiveDecimalV1 create(BigDecimal b) {
    return create(b, true);
  }

  @HiveDecimalVersionV1
  public static HiveDecimalV1 create(BigDecimal b, boolean allowRounding) {
    BigDecimal bd = normalize(b, allowRounding);
    return bd == null ? null : new HiveDecimalV1(bd);
  }

  @HiveDecimalVersionV1
  public static HiveDecimalV1 create(BigInteger unscaled, int scale) {
    BigDecimal bd = normalize(new BigDecimal(unscaled, scale), true);
    return bd == null ? null : new HiveDecimalV1(bd);
  }

  @HiveDecimalVersionV1
  public static HiveDecimalV1 create(String dec) {
    BigDecimal bd;
    try {
      bd = new BigDecimal(dec.trim());
    } catch (NumberFormatException ex) {
      return null;
    }
    bd = normalize(bd, true);
    return bd == null ? null : new HiveDecimalV1(bd);
  }

  @HiveDecimalVersionV1
  public static HiveDecimalV1 create(BigInteger bi) {
    BigDecimal bd = normalize(new BigDecimal(bi), true);
    return bd == null ? null : new HiveDecimalV1(bd);
  }

  @HiveDecimalVersionV1
  public static HiveDecimalV1 create(int i) {
    return new HiveDecimalV1(new BigDecimal(i));
  }

  @HiveDecimalVersionV1
  public static HiveDecimalV1 create(long l) {
    return new HiveDecimalV1(new BigDecimal(l));
  }

  @HiveDecimalVersionV1
  @Override
  public String toString() {
     return bd.toPlainString();
  }
  
  /**
   * Return a string representation of the number with the number of decimal digits as
   * the given scale. Please note that this is different from toString().
   * @param scale the number of digits after the decimal point
   * @return the string representation of exact number of decimal digits
   */
  @HiveDecimalVersionV1
  public String toFormatString(int scale) {
    return (bd.scale() == scale ? bd :
      bd.setScale(scale, RoundingMode.HALF_UP)).toPlainString();
  }

  @HiveDecimalVersionV1
  public HiveDecimalV1 setScale(int i) {
    return new HiveDecimalV1(bd.setScale(i, RoundingMode.HALF_UP));
  }

  @HiveDecimalVersionV1
  @Override
  public int compareTo(HiveDecimalV1 dec) {
    return bd.compareTo(dec.bd);
  }

  @HiveDecimalVersionV1
  @Override
  public int hashCode() {
    return bd.hashCode();
  }

  @HiveDecimalVersionV1
  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != getClass()) {
      return false;
    }
    return bd.equals(((HiveDecimalV1) obj).bd);
  }

  @HiveDecimalVersionV1
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
  @HiveDecimalVersionV1
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

  /** Note - this method will corrupt the value if it doesn't fit. */
  @HiveDecimalVersionV1
  public int intValue() {
    return bd.intValue();
  }

  @HiveDecimalVersionV1
  public double doubleValue() {
    return bd.doubleValue();
  }

  /** Note - this method will corrupt the value if it doesn't fit. */
  @HiveDecimalVersionV1
  public long longValue() {
    return bd.longValue();
  }

  /** Note - this method will corrupt the value if it doesn't fit. */
  @HiveDecimalVersionV1
  public short shortValue() {
    return bd.shortValue();
  }

  @HiveDecimalVersionV1
  public float floatValue() {
    return bd.floatValue();
  }

  @HiveDecimalVersionV1
  public BigDecimal bigDecimalValue() {
    return bd;
  }

  @HiveDecimalVersionV1
  public byte byteValue() {
    return bd.byteValue();
  }

  @HiveDecimalVersionV1
  public HiveDecimalV1 setScale(int adjustedScale, int rm) {
    return create(bd.setScale(adjustedScale, rm));
  }

  @HiveDecimalVersionV1
  public HiveDecimalV1 subtract(HiveDecimalV1 dec) {
    return create(bd.subtract(dec.bd));
  }

  @HiveDecimalVersionV1
  public HiveDecimalV1 multiply(HiveDecimalV1 dec) {
    return create(bd.multiply(dec.bd), false);
  }

  @HiveDecimalVersionV1
  public BigInteger unscaledValue() {
    return bd.unscaledValue();
  }

  @HiveDecimalVersionV1
  public HiveDecimalV1 scaleByPowerOfTen(int n) {
    return create(bd.scaleByPowerOfTen(n));
  }

  @HiveDecimalVersionV1
  public HiveDecimalV1 abs() {
    return create(bd.abs());
  }

  @HiveDecimalVersionV1
  public HiveDecimalV1 negate() {
    return create(bd.negate());
  }

  @HiveDecimalVersionV1
  public HiveDecimalV1 add(HiveDecimalV1 dec) {
    return create(bd.add(dec.bd));
  }

  @HiveDecimalVersionV1
  public HiveDecimalV1 pow(int n) {
    BigDecimal result = normalize(bd.pow(n), false);
    return result == null ? null : new HiveDecimalV1(result);
  }

  @HiveDecimalVersionV1
  public HiveDecimalV1 remainder(HiveDecimalV1 dec) {
    return create(bd.remainder(dec.bd));
  }

  @HiveDecimalVersionV1
  public HiveDecimalV1 divide(HiveDecimalV1 dec) {
    return create(bd.divide(dec.bd, MAX_SCALE, RoundingMode.HALF_UP), true);
  }

  /**
   * Get the sign of the underlying decimal.
   * @return 0 if the decimal is equal to 0, -1 if less than zero, and 1 if greater than 0
   */
  @HiveDecimalVersionV1
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

  private static BigDecimal enforcePrecisionScale(BigDecimal bd, int maxPrecision, int maxScale) {
    if (bd == null) {
      return null;
    }

    /**
     * Specially handling the case that bd=0, and we are converting it to a type where precision=scale,
     * such as decimal(1, 1).
     */
    if (bd.compareTo(BigDecimal.ZERO) == 0 && bd.scale() == 0 && maxPrecision == maxScale) {
      return bd.setScale(maxScale);
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

  @HiveDecimalVersionV1
  public static HiveDecimalV1 enforcePrecisionScale(HiveDecimalV1 dec, int maxPrecision, int maxScale) {
    if (dec == null) {
      return null;
    }

    // Minor optimization, avoiding creating new objects.
    if (dec.precision() - dec.scale() <= maxPrecision - maxScale &&
        dec.scale() <= maxScale) {
      return dec;
    }

    BigDecimal bd = enforcePrecisionScale(dec.bd, maxPrecision, maxScale);
    if (bd == null) {
      return null;
    }

    return HiveDecimalV1.create(bd);
  }

  @HiveDecimalVersionV1
  public long longValueExact() {
    return bd.longValueExact();
  }
}
