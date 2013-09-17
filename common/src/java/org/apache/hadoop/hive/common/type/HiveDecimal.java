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

  public static final HiveDecimal ZERO = new HiveDecimal(BigDecimal.ZERO);

  public static final int MAX_PRECISION = 38; // fits into 128 bits

  public static final HiveDecimal ONE = new HiveDecimal(BigDecimal.ONE);

  public static final int ROUND_FLOOR = BigDecimal.ROUND_FLOOR;

  public static final int ROUND_CEILING = BigDecimal.ROUND_CEILING;

  public static final int ROUND_HALF_UP = BigDecimal.ROUND_HALF_UP;

  private BigDecimal bd = BigDecimal.ZERO;

  public HiveDecimal(BigDecimal b) {
    this(b, false);
  }

  public HiveDecimal(BigDecimal b, boolean allowRounding) {
    bd = this.normalize(b, MAX_PRECISION, allowRounding);
    if (bd == null) {
      throw new NumberFormatException("Assignment would result in truncation");
    }
  }

  public HiveDecimal(BigInteger unscaled, int scale) {
    bd = this.normalize(new BigDecimal(unscaled, scale), MAX_PRECISION, false);
    if (bd == null) {
      throw new NumberFormatException("Assignment would result in truncation");
    }
  }

  public HiveDecimal(String dec) {
    bd = this.normalize(new BigDecimal(dec), MAX_PRECISION, false);
    if (bd == null) {
      throw new NumberFormatException("Assignment would result in truncation");
    }
  }

  public HiveDecimal(BigInteger bi) {
    bd = this.normalize(new BigDecimal(bi), MAX_PRECISION, false);
    if (bd == null) {
      throw new NumberFormatException("Assignment would result in truncation");
    }
  }

  public HiveDecimal(int i) {
    bd = new BigDecimal(i);
  }

  public HiveDecimal(long l) {
    bd = new BigDecimal(l);
  }

  @Override
  public String toString() {
     return bd.toPlainString();
  }

  public HiveDecimal setScale(int i) {
    return new HiveDecimal(bd.setScale(i));
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

  public int precision() {
    return bd.precision();
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
    return new HiveDecimal(bd.setScale(adjustedScale, rm));
  }

  public HiveDecimal subtract(HiveDecimal dec) {
    return new HiveDecimal(bd.subtract(dec.bd));
  }

  public HiveDecimal multiply(HiveDecimal dec) {
    return new HiveDecimal(bd.multiply(dec.bd));
  }

  public BigInteger unscaledValue() {
    return bd.unscaledValue();
  }

  public HiveDecimal scaleByPowerOfTen(int n) {
    return new HiveDecimal(bd.scaleByPowerOfTen(n));
  }

  public HiveDecimal abs() {
    return new HiveDecimal(bd.abs());
  }

  public HiveDecimal negate() {
    return new HiveDecimal(bd.negate());
  }

  public HiveDecimal add(HiveDecimal dec) {
    return new HiveDecimal(bd.add(dec.bd));
  }

  public HiveDecimal pow(int n) {
    return new HiveDecimal(bd.pow(n));
  }

  public HiveDecimal remainder(HiveDecimal dec) {
    return new HiveDecimal(bd.remainder(dec.bd));
  }

  public HiveDecimal divide(HiveDecimal dec) {
    return new HiveDecimal(bd.divide(dec.bd, MAX_PRECISION, RoundingMode.HALF_UP), true);
  }

  private BigDecimal trim(BigDecimal d) {
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

  private BigDecimal normalize(BigDecimal d, int precision, boolean allowRounding) {
    if (d == null) {
      return null;
    }

    d = trim(d);

    // compute the number of digits of the decimal
    int valuePrecision = d.precision()
        + Math.max(0, 1 + d.scale() - d.precision());

    if (valuePrecision > precision) {
      if (allowRounding) {
        // round "half up" until we hit the decimal point
        int adjustedScale = d.scale() - (valuePrecision-precision);
        if (adjustedScale >= 0) {
          d = d.setScale(adjustedScale, RoundingMode.HALF_UP);
          d = trim(d);
        } else {
          d = null;
        }
      } else {
        d = null;
      }
    }
    return d;
  }
}
