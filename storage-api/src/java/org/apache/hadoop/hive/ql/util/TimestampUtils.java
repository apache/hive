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

package org.apache.hadoop.hive.ql.util;

import org.apache.hadoop.hive.common.type.HiveDecimal;

import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * Utitilities for Timestamps and the relevant conversions.
 */
public class TimestampUtils {
  public static final BigDecimal BILLION_BIG_DECIMAL = BigDecimal.valueOf(1000000000);

  /**
   * Convert the timestamp to a double measured in seconds.
   * @return double representation of the timestamp, accurate to nanoseconds
   */
  public static double getDouble(Timestamp ts) {
    long seconds = millisToSeconds(ts.getTime());
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
      Timestamp t = new Timestamp(millis);

      // Set remaining fractional portion to nanos
      t.setNanos(nanos);
      return t;
    } catch (NumberFormatException nfe) {
      return null;
    } catch (IllegalArgumentException iae) {
      return null;
    }
  }

  public static Timestamp decimalToTimestamp(HiveDecimal d) {
    try {
      BigDecimal nanoInstant = d.bigDecimalValue().multiply(BILLION_BIG_DECIMAL);
      int nanos = nanoInstant.remainder(BILLION_BIG_DECIMAL).intValue();
      if (nanos < 0) {
        nanos += 1000000000;
      }
      long seconds =
          nanoInstant.subtract(new BigDecimal(nanos)).divide(BILLION_BIG_DECIMAL).longValue();
      Timestamp t = new Timestamp(seconds * 1000);
      t.setNanos(nanos);

      return t;
    } catch (NumberFormatException nfe) {
      return null;
    } catch (IllegalArgumentException iae) {
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
