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

import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import java.math.BigDecimal;
import java.time.DateTimeException;

/**
 * Utilities for Timestamps and the relevant conversions.
 */
public class TimestampUtils {
  public static final BigDecimal BILLION_BIG_DECIMAL = BigDecimal.valueOf(1000000000);

  /**
   * Convert the timestamp to a double measured in seconds.
   * @return double representation of the timestamp, accurate to nanoseconds
   */
  public static double getDouble(Timestamp ts) {
    long seconds = ts.toEpochSecond();
    return seconds + ((double) ts.getNanos()) / 1000000000;
  }

  public static Timestamp doubleToTimestamp(double f) {
    try {
      long seconds = (long) f;

      // We must ensure the exactness of the double's fractional portion.
      // 0.6 as the fraction part will be converted to 0.59999... and
      // significantly reduce the savings from binary serialization
      BigDecimal bd = new BigDecimal(String.valueOf(f));

      bd = bd.subtract(new BigDecimal(seconds)).multiply(new BigDecimal(1000000000));
      int nanos = bd.intValue();

      // Convert to millis
      long millis = seconds * 1000;
      if (nanos < 0) {
        millis -= 1000;
        nanos += 1000000000;
      }

      return Timestamp.ofEpochMilli(millis, nanos);
    } catch (IllegalArgumentException | DateTimeException nfe) {
      return null;
    }
  }

  /**
   * Take a HiveDecimal and return the timestamp representation where the fraction part is the
   * nanoseconds and integer part is the number of seconds.
   * @param dec
   * @return
   */
  public static Timestamp decimalToTimestamp(HiveDecimal dec) {
    try {
      HiveDecimalWritable nanosWritable = new HiveDecimalWritable(dec);
      nanosWritable.mutateFractionPortion();               // Clip off seconds portion.
      nanosWritable.mutateScaleByPowerOfTen(9);            // Bring nanoseconds into integer portion.
      if (!nanosWritable.isSet() || !nanosWritable.isInt()) {
        return null;
      }
      int nanos = nanosWritable.intValue();
      if (nanos < 0) {
        nanos += 1000000000;
      }
      nanosWritable.setFromLong(nanos);

      HiveDecimalWritable nanoInstant = new HiveDecimalWritable(dec);
      nanoInstant.mutateScaleByPowerOfTen(9);

      nanoInstant.mutateSubtract(nanosWritable);
      nanoInstant.mutateScaleByPowerOfTen(-9);              // Back to seconds.
      if (!nanoInstant.isSet() || !nanoInstant.isLong()) {
        return null;
      }
      long millis = nanoInstant.longValue() * 1000;
      return Timestamp.ofEpochMilli(millis, nanos);
    } catch (IllegalArgumentException | DateTimeException nfe) {
      // E.g. DateTimeException: Invalid value for Year (valid values -999999999 - 999999999)
      return null;
    }
  }

  /**
   * Take a HiveDecimalWritable and return the timestamp representation where the fraction part
   * is the nanoseconds and integer part is the number of seconds.
   *
   * This is a HiveDecimalWritable variation with supplied scratch objects.
   * @param decWritable
   * @param scratchDecWritable1
   * @param scratchDecWritable2
   * @return
   */
  public static Timestamp decimalToTimestamp(
      HiveDecimalWritable decWritable,
      HiveDecimalWritable scratchDecWritable1, HiveDecimalWritable scratchDecWritable2) {

    HiveDecimalWritable nanosWritable = scratchDecWritable1;
    nanosWritable.set(decWritable);
    nanosWritable.mutateFractionPortion();               // Clip off seconds portion.
    nanosWritable.mutateScaleByPowerOfTen(9);            // Bring nanoseconds into integer portion.
    if (!nanosWritable.isSet() || !nanosWritable.isInt()) {
      return null;
    }
    int nanos = nanosWritable.intValue();
    if (nanos < 0) {
      nanos += 1000000000;
    }
    nanosWritable.setFromLong(nanos);

    HiveDecimalWritable nanoInstant = scratchDecWritable2;
    nanoInstant.set(decWritable);
    nanoInstant.mutateScaleByPowerOfTen(9);

    nanoInstant.mutateSubtract(nanosWritable);
    nanoInstant.mutateScaleByPowerOfTen(-9);              // Back to seconds.
    if (!nanoInstant.isSet() || !nanoInstant.isLong()) {
      return null;
    }
    long seconds = nanoInstant.longValue();

    return Timestamp.ofEpochSecond(seconds, nanos);
  }

  public static Timestamp decimalToTimestamp(HiveDecimalV1 dec) {
    try {
      BigDecimal nanoInstant = dec.bigDecimalValue().multiply(BILLION_BIG_DECIMAL);
      int nanos = nanoInstant.remainder(BILLION_BIG_DECIMAL).intValue();
      if (nanos < 0) {
        nanos += 1000000000;
      }
      long seconds =
          nanoInstant.subtract(new BigDecimal(nanos)).divide(BILLION_BIG_DECIMAL).longValue();

      return Timestamp.ofEpochSecond(seconds, nanos);
    } catch (IllegalArgumentException | DateTimeException nfe) {
      return null;
    }
  }

  /**
   * Rounds the number of milliseconds relative to the epoch down to the nearest whole number of
   * seconds. 500 would round to 0, -500 would round to -1.
   */
  public static long millisToSeconds(long millis) {
    if (millis >= 0) {
      return millis / 1000;
    } else {
      return (millis - 999) / 1000;
    }
  }

}
