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
package org.apache.hadoop.hive.serde2;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class RandomTypeUtil {

  public static String getRandString(Random r) {
    return getRandString(r, null, r.nextInt(10));
  }

  public static String getRandString(Random r, String characters, int length) {
    if (characters == null) {
      characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      if (characters == null) {
        sb.append((char) (r.nextInt(128)));
      } else {
        sb.append(characters.charAt(r.nextInt(characters.length())));
      }
    }
    return sb.toString();
  }

  public static byte[] getRandBinary(Random r, int len){
    byte[] bytes = new byte[len];
    for (int j = 0; j < len; j++){
      bytes[j] = Byte.valueOf((byte) r.nextInt());
    }
    return bytes;
  }

  private static final String DECIMAL_CHARS = "0123456789";

  public static HiveDecimal getRandHiveDecimal(Random r) {
    int precision;
    int scale;
    while (true) {
      StringBuilder sb = new StringBuilder();
      precision = 1 + r.nextInt(18);
      scale = 0 + r.nextInt(precision + 1);

      int integerDigits = precision - scale;

      if (r.nextBoolean()) {
        sb.append("-");
      }

      if (integerDigits == 0) {
        sb.append("0");
      } else {
        sb.append(getRandString(r, DECIMAL_CHARS, integerDigits));
      }
      if (scale != 0) {
        sb.append(".");
        sb.append(getRandString(r, DECIMAL_CHARS, scale));
      }

      return HiveDecimal.create(sb.toString());
    }
  }

  public static Date getRandDate(Random r) {
    String dateStr = String.format("%d-%02d-%02d",
        Integer.valueOf(1800 + r.nextInt(500)),  // year
        Integer.valueOf(1 + r.nextInt(12)),      // month
        Integer.valueOf(1 + r.nextInt(28)));     // day
    Date dateVal = Date.valueOf(dateStr);
    return dateVal;
  }

  /**
   * TIMESTAMP.
   */

  public static final long NANOSECONDS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
  public static final long MILLISECONDS_PER_SECOND = TimeUnit.SECONDS.toMillis(1);
  public static final long NANOSECONDS_PER_MILLISSECOND = TimeUnit.MILLISECONDS.toNanos(1);

  private static final ThreadLocal<DateFormat> DATE_FORMAT =
      new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
          return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
      };

  // We've switched to Joda/Java Calendar which has a more limited time range....
  public static final int MIN_YEAR = 1900;
  public static final int MAX_YEAR = 3000;
  private static final long MIN_FOUR_DIGIT_YEAR_MILLIS = parseToMillis("1900-01-01 00:00:00");
  private static final long MAX_FOUR_DIGIT_YEAR_MILLIS = parseToMillis("3000-01-01 00:00:00");

  private static long parseToMillis(String s) {
    try {
      return DATE_FORMAT.get().parse(s).getTime();
    } catch (ParseException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static Timestamp getRandTimestamp(Random r) {
    return getRandTimestamp(r, MIN_YEAR, MAX_YEAR);
  }

  public static Timestamp getRandTimestamp(Random r, int minYear, int maxYear) {
    String optionalNanos = "";
    switch (r.nextInt(4)) {
    case 0:
      // No nanos.
      break;
    case 1:
      optionalNanos = String.format(".%09d",
          Integer.valueOf(r.nextInt((int) NANOSECONDS_PER_SECOND)));
      break;
    case 2:
      // Limit to milliseconds only...
      optionalNanos = String.format(".%09d",
          Integer.valueOf(r.nextInt((int) MILLISECONDS_PER_SECOND)) * NANOSECONDS_PER_MILLISSECOND);
      break;
    case 3:
      // Limit to below milliseconds only...
      optionalNanos = String.format(".%09d",
          Integer.valueOf(r.nextInt((int) NANOSECONDS_PER_MILLISSECOND)));
      break;
    }
    String timestampStr = String.format("%04d-%02d-%02d %02d:%02d:%02d%s",
        Integer.valueOf(minYear + r.nextInt(maxYear - minYear + 1)),  // year
        Integer.valueOf(1 + r.nextInt(12)),      // month
        Integer.valueOf(1 + r.nextInt(28)),      // day
        Integer.valueOf(0 + r.nextInt(24)),      // hour
        Integer.valueOf(0 + r.nextInt(60)),      // minute
        Integer.valueOf(0 + r.nextInt(60)),      // second
        optionalNanos);
    Timestamp timestampVal;
    try {
      timestampVal = Timestamp.valueOf(timestampStr);
    } catch (Exception e) {
      System.err.println("Timestamp string " + timestampStr + " did not parse");
      throw e;
    }
    return timestampVal;
  }

  public static long randomMillis(long minMillis, long maxMillis, Random rand) {
    return minMillis + (long) ((maxMillis - minMillis) * rand.nextDouble());
  }

  public static long randomMillis(Random rand) {
    return randomMillis(MIN_FOUR_DIGIT_YEAR_MILLIS, MAX_FOUR_DIGIT_YEAR_MILLIS, rand);
  }

  public static int randomNanos(Random rand, int decimalDigits) {
    // Only keep the most significant decimalDigits digits.
    int nanos = rand.nextInt((int) NANOSECONDS_PER_SECOND);
    return nanos - nanos % (int) Math.pow(10, 9 - decimalDigits);
  }

  public static int randomNanos(Random rand) {
    return randomNanos(rand, 9);
  }
}