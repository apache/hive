/**
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

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import jodd.datetime.JDateTime;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetTableUtils;

/**
 * Utilities for converting from java.sql.Timestamp to parquet timestamp.
 * This utilizes the Jodd library.
 */
public class NanoTimeUtils {
  private static final long NANOS_PER_HOUR = TimeUnit.HOURS.toNanos(1);
  private static final long NANOS_PER_MINUTE = TimeUnit.MINUTES.toNanos(1);
  private static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
  private static final long NANOS_PER_DAY = TimeUnit.DAYS.toNanos(1);

  private static final ThreadLocal<Calendar> parquetUTCCalendar = new ThreadLocal<Calendar>();
  private static final ThreadLocal<Calendar> parquetLocalCalendar = new ThreadLocal<Calendar>();

  private static Calendar getUTCCalendar() {
    //Calendar.getInstance calculates the current-time needlessly, so cache an instance.
    if (parquetUTCCalendar.get() == null) {
      parquetUTCCalendar.set(Calendar.getInstance(TimeZone.getTimeZone("UTC")));
    }
    return parquetUTCCalendar.get();
  }

  private static Calendar getLocalCalendar() {
    if (parquetLocalCalendar.get() == null) {
      parquetLocalCalendar.set(Calendar.getInstance());
    }
    return parquetLocalCalendar.get();
  }

  public static Calendar getCalendar(boolean skipConversion) {
    Calendar calendar = skipConversion ? Calendar.getInstance(TimeZone.getTimeZone("UTC"))
        : Calendar.getInstance();
    calendar.clear(); // Reset all fields before reusing this instance
    return calendar;
  }

  @Deprecated
  public static NanoTime getNanoTime(Timestamp ts, boolean skipConversion) {
    return getNanoTime(ts, getCalendar(skipConversion));
  }

  /**
   * Constructs a julian date from the floating time Timestamp.
   * If the timezone of the calendar is different from the current local
   * timezone, then the timestamp value will be adjusted.
   * Possible adjustments:
   *   - UTC Ts -> Local Ts copied to TableTZ Calendar -> UTC Ts -> JD
   * @param ts floating time timestamp to store
   * @param calendar timezone used to adjust the timestamp for parquet
   * @return adjusted julian date
   */
  public static NanoTime getNanoTime(Timestamp ts, Calendar calendar) {

    Calendar localCalendar = getLocalCalendar();
    localCalendar.setTimeInMillis(ts.getTime());

    Calendar adjustedCalendar = copyToCalendarWithTZ(localCalendar, calendar);

    Calendar utcCalendar = getUTCCalendar();
    utcCalendar.setTimeInMillis(adjustedCalendar.getTimeInMillis());

    int year = utcCalendar.get(Calendar.YEAR);
    if (utcCalendar.get(Calendar.ERA) == GregorianCalendar.BC) {
      year = 1 - year;
    }
    JDateTime jDateTime = new JDateTime(year,
        utcCalendar.get(Calendar.MONTH) + 1,  //java calendar index starting at 1.
        utcCalendar.get(Calendar.DAY_OF_MONTH));
    int days = jDateTime.getJulianDayNumber();

    long hour = utcCalendar.get(Calendar.HOUR_OF_DAY);
    long minute = utcCalendar.get(Calendar.MINUTE);
    long second = utcCalendar.get(Calendar.SECOND);
    long nanos = ts.getNanos();
    long nanosOfDay = nanos + NANOS_PER_SECOND * second + NANOS_PER_MINUTE * minute +
        NANOS_PER_HOUR * hour;

    return new NanoTime(days, nanosOfDay);
  }

  @Deprecated
  public static Timestamp getTimestamp(NanoTime nt, boolean skipConversion) {
    return getTimestamp(nt, getCalendar(skipConversion));
  }

  /**
   * Constructs a floating time Timestamp from the julian date contained in NanoTime.
   * If the timezone of the calendar is different from the current local
   * timezone, then the timestamp value will be adjusted.
   * Possible adjustments:
   *   - JD -> UTC Ts -> TableTZ Calendar copied to LocalTZ Calendar -> UTC Ts
   * @param nt stored julian date
   * @param calendar timezone used to adjust the timestamp for parquet
   * @return floating time represented as a timestamp. Guaranteed to display
   * the same when formatted using the current local timezone as with the local
   * timezone at the time it was stored.
   */
  public static Timestamp getTimestamp(NanoTime nt, Calendar calendar) {
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

    Calendar utcCalendar = getUTCCalendar();
    utcCalendar.clear();
    utcCalendar.set(Calendar.YEAR, jDateTime.getYear());
    utcCalendar.set(Calendar.MONTH, jDateTime.getMonth() - 1); //java calendar index starting at 1.
    utcCalendar.set(Calendar.DAY_OF_MONTH, jDateTime.getDay());

    int hour = (int) (remainder / (NANOS_PER_HOUR));
    remainder = remainder % (NANOS_PER_HOUR);
    int minutes = (int) (remainder / (NANOS_PER_MINUTE));
    remainder = remainder % (NANOS_PER_MINUTE);
    int seconds = (int) (remainder / (NANOS_PER_SECOND));
    long nanos = remainder % NANOS_PER_SECOND;

    utcCalendar.set(Calendar.HOUR_OF_DAY, hour);
    utcCalendar.set(Calendar.MINUTE, minutes);
    utcCalendar.set(Calendar.SECOND, seconds);

    calendar.setTimeInMillis(utcCalendar.getTimeInMillis());

    Calendar adjusterCalendar = copyToCalendarWithTZ(calendar, getLocalCalendar());

    Timestamp ts = new Timestamp(adjusterCalendar.getTimeInMillis());
    ts.setNanos((int) nanos);
    return ts;
  }

  /**
   * Check if the string id is a valid java TimeZone id.
   * TimeZone#getTimeZone will return "GMT" if the id cannot be understood.
   * @param timeZoneID
   */
  public static void validateTimeZone(String timeZoneID) {
    if (TimeZone.getTimeZone(timeZoneID).getID().equals("GMT")
        && !"GMT".equals(timeZoneID)) {
      throw new IllegalStateException(
          "Unexpected timezone id found for parquet int96 conversion: " + timeZoneID);
    }
  }

  private static Calendar copyToCalendarWithTZ(Calendar from, Calendar to) {
    if(from.getTimeZone().getID().equals(to.getTimeZone().getID())) {
      return from;
    } else {
      to.set(Calendar.ERA, from.get(Calendar.ERA));
      to.set(Calendar.YEAR, from.get(Calendar.YEAR));
      to.set(Calendar.MONTH, from.get(Calendar.MONTH));
      to.set(Calendar.DAY_OF_MONTH, from.get(Calendar.DAY_OF_MONTH));
      to.set(Calendar.HOUR_OF_DAY, from.get(Calendar.HOUR_OF_DAY));
      to.set(Calendar.MINUTE, from.get(Calendar.MINUTE));
      to.set(Calendar.SECOND, from.get(Calendar.SECOND));
      return to;
    }
  }
}
