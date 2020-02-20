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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Conversion utilities from the hybrid Julian/Gregorian calendar to/from the
 * proleptic Gregorian.
 *
 * The semantics here are to hold the string representation constant and change
 * the epoch offset rather than holding the instant in time constant and change
 * the string representation.
 *
 * These utilities will be fast for the common case (> 1582 AD), but slow for
 * old dates.
 */
public class CalendarUtils {

  public static final long SWITCHOVER_MILLIS;
  public static final long SWITCHOVER_DAYS;

  private static SimpleDateFormat createFormatter(String fmt, boolean proleptic) {
    SimpleDateFormat result = new SimpleDateFormat(fmt);
    GregorianCalendar calendar = new GregorianCalendar(UTC);
    if (proleptic) {
      calendar.setGregorianChange(new Date(Long.MIN_VALUE));
    }
    result.setCalendar(calendar);
    return result;
  }

  private static final String DATE = "yyyy-MM-dd";
  private static final String TIME = DATE + " HH:mm:ss.SSS";
  private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
  private static final ThreadLocal<SimpleDateFormat> HYBRID_DATE_FORMAT =
      ThreadLocal.withInitial(() -> createFormatter(DATE, false));
  private static final ThreadLocal<SimpleDateFormat> HYBRID_TIME_FORMAT =
      ThreadLocal.withInitial(() -> createFormatter(TIME, false));

  private static final ThreadLocal<SimpleDateFormat> PROLEPTIC_DATE_FORMAT =
      ThreadLocal.withInitial(() -> createFormatter(DATE, true));
  private static final ThreadLocal<SimpleDateFormat> PROLEPTIC_TIME_FORMAT =
      ThreadLocal.withInitial(() -> createFormatter(TIME, true));

  static {
    // Get the last day where the two calendars agree with each other.
    try {
      SWITCHOVER_MILLIS = HYBRID_DATE_FORMAT.get().parse("1582-10-15").getTime();
      SWITCHOVER_DAYS = TimeUnit.MILLISECONDS.toDays(SWITCHOVER_MILLIS);
    } catch (ParseException e) {
      throw new IllegalArgumentException("Can't parse switch over date", e);
    }
  }

  /**
   * Convert an epoch day from the hybrid Julian/Gregorian calendar to the
   * proleptic Gregorian.
   * @param hybrid day of epoch in the hybrid Julian/Gregorian
   * @return day of epoch in the proleptic Gregorian
   */
  public static int convertDateToProleptic(int hybrid) {
    int proleptic = hybrid;
    if (hybrid < SWITCHOVER_DAYS) {
      String dateStr = HYBRID_DATE_FORMAT.get().format(
          new Date(TimeUnit.DAYS.toMillis(hybrid)));
      try {
        proleptic = (int) TimeUnit.MILLISECONDS.toDays(
            PROLEPTIC_DATE_FORMAT.get().parse(dateStr).getTime());
      } catch (ParseException e) {
        throw new IllegalArgumentException("Can't parse " + dateStr, e);
      }
    }
    return proleptic;
  }

  /**
   * Convert an epoch day from the proleptic Gregorian calendar to the hybrid
   * Julian/Gregorian.
   * @param proleptic day of epoch in the proleptic Gregorian
   * @return day of epoch in the hybrid Julian/Gregorian
   */
  public static int convertDateToHybrid(int proleptic) {
    int hyrbid = proleptic;
    if (proleptic < SWITCHOVER_DAYS) {
      String dateStr = PROLEPTIC_DATE_FORMAT.get().format(
          new Date(TimeUnit.DAYS.toMillis(proleptic)));
      try {
        hyrbid = (int) TimeUnit.MILLISECONDS.toDays(
            HYBRID_DATE_FORMAT.get().parse(dateStr).getTime());
      } catch (ParseException e) {
        throw new IllegalArgumentException("Can't parse " + dateStr, e);
      }
    }
    return hyrbid;
  }

  public static int convertDate(int original,
                                boolean fromProleptic,
                                boolean toProleptic) {
    if (fromProleptic != toProleptic) {
      return toProleptic
          ? convertDateToProleptic(original)
          : convertDateToHybrid(original);
    } else {
      return original;
    }
  }

  public static long convertTime(long original,
                                 boolean fromProleptic,
                                 boolean toProleptic) {
    if (fromProleptic != toProleptic) {
      return toProleptic
          ? convertTimeToProleptic(original)
          : convertTimeToHybrid(original);
    } else {
      return original;
    }
  }
  /**
   * Convert epoch millis from the hybrid Julian/Gregorian calendar to the
   * proleptic Gregorian.
   * @param hybrid millis of epoch in the hybrid Julian/Gregorian
   * @return millis of epoch in the proleptic Gregorian
   */
  public static long convertTimeToProleptic(long hybrid) {
    long proleptic = hybrid;
    if (hybrid < SWITCHOVER_MILLIS) {
      String dateStr = HYBRID_TIME_FORMAT.get().format(new Date(hybrid));
      try {
        proleptic = PROLEPTIC_TIME_FORMAT.get().parse(dateStr).getTime();
      } catch (ParseException e) {
        throw new IllegalArgumentException("Can't parse " + dateStr, e);
      }
    }
    return proleptic;
  }

  /**
   * Convert epoch millis from the proleptic Gregorian calendar to the hybrid
   * Julian/Gregorian.
   * @param proleptic millis of epoch in the proleptic Gregorian
   * @return millis of epoch in the hybrid Julian/Gregorian
   */
  public static long convertTimeToHybrid(long proleptic) {
    long hybrid = proleptic;
    if (proleptic < SWITCHOVER_MILLIS) {
      String dateStr = PROLEPTIC_TIME_FORMAT.get().format(new Date(proleptic));
      try {
        hybrid = HYBRID_TIME_FORMAT.get().parse(dateStr).getTime();
      } catch (ParseException e) {
        throw new IllegalArgumentException("Can't parse " + dateStr, e);
      }
    }
    return hybrid;
  }

  /**
   *
   * Formats epoch day to date according to proleptic or hybrid calendar
   *
   * @param epochDay  epoch day
   * @param useProleptic if true - uses proleptic formatter, else uses hybrid formatter
   * @return formatted date
   */
  public static String formatDate(long epochDay, boolean useProleptic) {
    long millis = TimeUnit.DAYS.toMillis(epochDay);
    return useProleptic ? PROLEPTIC_DATE_FORMAT.get().format(millis)
        : HYBRID_DATE_FORMAT.get().format(millis);
  }

  private CalendarUtils() {
    throw new UnsupportedOperationException();
  }
}