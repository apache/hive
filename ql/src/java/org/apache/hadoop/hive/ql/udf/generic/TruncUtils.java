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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.udf.generic;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.hadoop.hive.common.type.FastHiveDecimalImpl;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

/**
 * Utility class for generic trunc UDF and its vectorized implementations.
 * <p>
 * Truncation drops the digits after the given scale, rounding towards zero. A negative scale
 * zeroes digits left of the decimal point. The input is never scaled into a long, so the result
 * is exact over the whole range of the input type.
 */
public final class TruncUtils {

  // HiveDecimal rounding takes the legacy BigDecimal rounding mode constants.
  @SuppressWarnings("deprecation")
  private static final int DECIMAL_ROUND_DOWN = BigDecimal.ROUND_DOWN;

  // Every decimal is smaller than 10^38 in magnitude, so lower scales also truncate it to zero.
  private static final int MIN_DECIMAL_SCALE = -HiveDecimal.MAX_PRECISION;

  // Doubles of at least this magnitude have no fractional part.
  private static final double MIN_INTEGRAL_DOUBLE = 0x1p52;

  // Double.MAX_VALUE is about 1.8E308, so lower scales truncate every double to zero.
  private static final int MIN_DOUBLE_SCALE = -309;

  private TruncUtils() {
  }

  /**
   * Truncates the input decimal into the output writable. The input is not modified.
   */
  public static void trunc(HiveDecimalWritable input, int scale, HiveDecimalWritable output) {
    output.set(input);
    output.mutateSetScale(Math.max(scale, MIN_DECIMAL_SCALE), DECIMAL_ROUND_DOWN);
  }

  /**
   * Truncating a double is approximate, as the double value itself is approximate. Like
   * {@link RoundUtils#round(double, int)}, this truncates the shortest decimal representation
   * of the double, so trunc(0.3, 1) is 0.3 even though the double nearest to 0.3 is below it.
   *
   * @param input input value
   * @param scale decimal place
   * @return truncated value
   */
  public static double trunc(double input, int scale) {
    if (Double.isNaN(input) || Double.isInfinite(input)) {
      return input;
    }
    if (scale == 0) {
      // Common trunc(x) case, no BigDecimal needed: the cast truncates towards zero.
      return Math.abs(input) < MIN_INTEGRAL_DOUBLE ? (double) (long) input : input;
    }
    if (scale < MIN_DOUBLE_SCALE) {
      return 0.0;
    }
    BigDecimal value = BigDecimal.valueOf(input);
    // Nothing to drop. Checked first since setScale would pad the value up to the given scale.
    if (scale >= value.scale()) {
      return input;
    }
    return value.setScale(scale, RoundingMode.DOWN).doubleValue();
  }

  public static long trunc(long input, int scale) {
    if (scale >= 0) {
      return input;
    }
    if (scale < -FastHiveDecimalImpl.MAX_LONG_POWER_OF_TEN) {
      // Every long is smaller than 10^19 in magnitude.
      return 0;
    }
    // The remainder has the sign of the input, so this truncates towards zero.
    return input - input % FastHiveDecimalImpl.powerOfTen(-scale);
  }
}
