/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.timestamp;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import jodd.time.JulianDate;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;

/**
 * Utilities for converting from java.sql.Timestamp to parquet timestamp.
 * This utilizes the Jodd library.
 */
public class NanoTimeUtils {
  static final long NANOS_PER_HOUR = TimeUnit.HOURS.toNanos(1);
  static final long NANOS_PER_MINUTE = TimeUnit.MINUTES.toNanos(1);
  static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
  static final long NANOS_PER_DAY = TimeUnit.DAYS.toNanos(1);

  private static final ThreadLocal<Calendar> parquetGMTCalendar = new ThreadLocal<Calendar>();

  private static Calendar getGMTCalendar() {
    //Calendar.getInstance calculates the current-time needlessly, so cache an instance.
    if (parquetGMTCalendar.get() == null) {
      GregorianCalendar calendar = new GregorianCalendar();
      calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
      calendar.setGregorianChange(new Date(Long.MIN_VALUE));
      parquetGMTCalendar.set(calendar);
    }
    parquetGMTCalendar.get().clear();
    return parquetGMTCalendar.get();
  }

  /**
   * Converts a timestamp from the specified timezone to UTC and returns its representation in NanoTime.
   */
  public static NanoTime getNanoTime(Timestamp ts, ZoneId sourceZone, boolean legacyConversion) {
    ts = TimestampTZUtil.convertTimestampToZone(ts, sourceZone, ZoneOffset.UTC, legacyConversion);

    Calendar calendar = getGMTCalendar();
    calendar.setTimeInMillis(ts.toEpochMilli());
    int year = calendar.get(Calendar.YEAR);
    if (calendar.get(Calendar.ERA) == GregorianCalendar.BC) {
      year = 1 - year;
    }
    JulianDate jDateTime;
    jDateTime = JulianDate.of(year,
        calendar.get(Calendar.MONTH) + 1,  //java calendar index starting at 1.
        calendar.get(Calendar.DAY_OF_MONTH), 0, 0, 0, 0);
    int days = jDateTime.getJulianDayNumber();

    long hour = calendar.get(Calendar.HOUR_OF_DAY);
    long minute = calendar.get(Calendar.MINUTE);
    long second = calendar.get(Calendar.SECOND);
    long nanos = ts.getNanos();
    long nanosOfDay = nanos + NANOS_PER_SECOND * second + NANOS_PER_MINUTE * minute +
        NANOS_PER_HOUR * hour;

    return new NanoTime(days, nanosOfDay);
  }

  public static Timestamp getTimestamp(NanoTime nt, ZoneId targetZone) {
    return getTimestamp(nt, targetZone, false);
  }

  /**
   * Converts a nanotime representation in UTC, to a timestamp in the specified timezone.
   *
   * @param legacyConversion when true the conversion to the target timezone is done with legacy (backwards compatible)
   * method.
   */
  public static Timestamp getTimestamp(NanoTime nt, ZoneId targetZone, boolean legacyConversion) {
    int julianDay = nt.getJulianDay();
    long nanosOfDay = nt.getTimeOfDayNanos();

    long remainder = nanosOfDay;
    julianDay += remainder / NANOS_PER_DAY;
    remainder %= NANOS_PER_DAY;
    if (remainder < 0) {
      remainder += NANOS_PER_DAY;
      julianDay--;
    }

    JulianDate jDateTime;
    jDateTime = JulianDate.of((double) julianDay);
    Calendar calendar = getGMTCalendar();
    calendar.set(Calendar.YEAR, jDateTime.toLocalDateTime().getYear());
    calendar.set(Calendar.MONTH, jDateTime.toLocalDateTime().getMonth().getValue() - 1); //java calendar index starting at 1.
    calendar.set(Calendar.DAY_OF_MONTH, jDateTime.toLocalDateTime().getDayOfMonth());

    int hour = (int) (remainder / (NANOS_PER_HOUR));
    remainder = remainder % (NANOS_PER_HOUR);
    int minutes = (int) (remainder / (NANOS_PER_MINUTE));
    remainder = remainder % (NANOS_PER_MINUTE);
    int seconds = (int) (remainder / (NANOS_PER_SECOND));
    long nanos = remainder % NANOS_PER_SECOND;

    calendar.set(Calendar.HOUR_OF_DAY, hour);
    calendar.set(Calendar.MINUTE, minutes);
    calendar.set(Calendar.SECOND, seconds);

    Timestamp ts = Timestamp.ofEpochMilli(calendar.getTimeInMillis(), (int) nanos);
    ts = TimestampTZUtil.convertTimestampToZone(ts, ZoneOffset.UTC, targetZone, legacyConversion);
    return ts;
  }
}
