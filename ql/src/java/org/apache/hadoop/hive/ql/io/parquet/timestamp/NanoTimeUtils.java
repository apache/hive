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

import org.apache.hadoop.hive.common.type.Timestamp;

import jodd.datetime.JDateTime;
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

  public static NanoTime getNanoTime(Timestamp ts, boolean skipConversion) {
    return getNanoTime(ts, skipConversion, null);
  }

  /**
   * Gets a NanoTime object, which represents timestamps as nanoseconds since epoch, from a
   * Timestamp object. Parquet will store this NanoTime object as int96.
   *
   * If skipConversion flag is on, the timestamp will be converted to NanoTime as-is, i.e.
   * timeZoneId argument will be ignored.
   * If skipConversion is off, timestamp can be converted from a given time zone (timeZoneId) to UTC
   * if timeZoneId is present, and if not present: from system time zone to UTC, before being
   * converted to NanoTime.
   * (See TimestampDataWriter#write for current Hive writing procedure.)
   */
  public static NanoTime getNanoTime(Timestamp ts, boolean skipConversion, ZoneId timeZoneId) {
    if (skipConversion) {
      timeZoneId = ZoneOffset.UTC;
    } else if (timeZoneId == null) {
      timeZoneId = TimeZone.getDefault().toZoneId();
    }
    ts = TimestampTZUtil.convertTimestampToZone(ts, timeZoneId, ZoneOffset.UTC);

    Calendar calendar = getGMTCalendar();
    calendar.setTimeInMillis(ts.toEpochMilli());
    int year = calendar.get(Calendar.YEAR);
    if (calendar.get(Calendar.ERA) == GregorianCalendar.BC) {
      year = 1 - year;
    }
    JDateTime jDateTime = new JDateTime(year,
        calendar.get(Calendar.MONTH) + 1,  //java calendar index starting at 1.
        calendar.get(Calendar.DAY_OF_MONTH));
    int days = jDateTime.getJulianDayNumber();

    long hour = calendar.get(Calendar.HOUR_OF_DAY);
    long minute = calendar.get(Calendar.MINUTE);
    long second = calendar.get(Calendar.SECOND);
    long nanos = ts.getNanos();
    long nanosOfDay = nanos + NANOS_PER_SECOND * second + NANOS_PER_MINUTE * minute +
        NANOS_PER_HOUR * hour;

    return new NanoTime(days, nanosOfDay);
  }

  public static Timestamp getTimestamp(NanoTime nt, boolean skipConversion) {
    return getTimestamp(nt, skipConversion, null);
  }

  /**
   * Gets a Timestamp object from a NanoTime object, which represents timestamps as nanoseconds
   * since epoch. Parquet stores these as int96.
   *
   * Before converting to NanoTime, we may convert the timestamp to a desired time zone
   * (timeZoneId). This will only happen if skipConversion flag is off.
   * If skipConversion is off and timeZoneId is not found, then convert the timestamp to system
   * time zone.
   *
   * For skipConversion to be true it must be set in conf AND the parquet file must NOT be written
   * by parquet's java library (parquet-mr). This is enforced in ParquetRecordReaderBase#getSplit.
   */
  public static Timestamp getTimestamp(NanoTime nt, boolean skipConversion, ZoneId timeZoneId) {
    if (skipConversion) {
      timeZoneId = ZoneOffset.UTC;
    } else if (timeZoneId == null) {
      timeZoneId = TimeZone.getDefault().toZoneId();
    }

    int julianDay = nt.getJulianDay();
    long nanosOfDay = nt.getTimeOfDayNanos();

    long remainder = nanosOfDay;
    julianDay += remainder / NANOS_PER_DAY;
    remainder %= NANOS_PER_DAY;
    if (remainder < 0) {
      remainder += NANOS_PER_DAY;
      julianDay--;
    }

    JDateTime jDateTime = new JDateTime((double) julianDay);
    Calendar calendar = getGMTCalendar();
    calendar.set(Calendar.YEAR, jDateTime.getYear());
    calendar.set(Calendar.MONTH, jDateTime.getMonth() - 1); //java calendar index starting at 1.
    calendar.set(Calendar.DAY_OF_MONTH, jDateTime.getDay());

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
    ts = TimestampTZUtil.convertTimestampToZone(ts, ZoneOffset.UTC, timeZoneId);
    return ts;
  }
}
