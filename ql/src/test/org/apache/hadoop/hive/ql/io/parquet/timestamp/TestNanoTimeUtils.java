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

import org.junit.Assert;
import org.junit.Test;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class TestNanoTimeUtils {

  // 3:34:10.101010101 PM on 1 January 2000:
  public static final int JAN_1_2000 = 2451545; // according to Wikipedia
  public static final long PM_3_34_10_101010101 =
      ((15L*60L+34L)*60L+10L)*1000000000L + 101010101L;
  public static final NanoTime KNOWN_TIME = new NanoTime(
      JAN_1_2000, PM_3_34_10_101010101);

  public static final long KNOWN_IN_MILLIS = 946740850101L; // currentmillis.com

  public static final TimeZone UTC = TimeZone.getTimeZone("UTC");
  public static final TimeZone PST = TimeZone.getTimeZone("PST");
  public static final TimeZone CST = TimeZone.getTimeZone("CST");
  public static final TimeZone PLUS_6 = TimeZone.getTimeZone("GMT+6");
  public static final TimeZone MINUS_6 = TimeZone.getTimeZone("GMT-6");

  // From Spark's NanoTime implementation
  public static final int JULIAN_DAY_OF_EPOCH = 2440588;
  public static final long SECONDS_PER_DAY = 60 * 60 * 24L;
  public static final long MICROS_PER_SECOND = 1000L * 1000L;

  /**
   * Returns the number of microseconds since epoch from Julian day
   * and nanoseconds in a day
   *
   * This is Spark's NanoTime implementation
   */
  public long fromJulianDay(int julianDay, long nanoseconds) {
    // use Long to avoid rounding errors
    long seconds = (((long) julianDay) - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY;
    return seconds * MICROS_PER_SECOND + nanoseconds / 1000L;
  }

  /**
   * Returns a Calendar from number of micros since epoch.
   *
   * This is a reliable conversion from micros since epoch to local time.
   */
  public Calendar toCalendar(long timestamp_us, TimeZone zone) {
    Calendar cal = Calendar.getInstance(zone);
    cal.setTimeInMillis(timestamp_us / 1000L);
    return cal;
  }

  @Test
  public void testFromJulianDay() {
    Assert.assertEquals(KNOWN_IN_MILLIS,
        fromJulianDay(JAN_1_2000, PM_3_34_10_101010101) / 1000L);
  }

  @Test
  public void testKnownTimestampWithFromJulianDay() {
    Calendar known = toCalendar(fromJulianDay(
        JAN_1_2000, PM_3_34_10_101010101), UTC);
    Assert.assertEquals(2000, known.get(Calendar.YEAR));
    Assert.assertEquals(Calendar.JANUARY, known.get(Calendar.MONTH));
    Assert.assertEquals(1, known.get(Calendar.DAY_OF_MONTH));
    Assert.assertEquals(15, known.get(Calendar.HOUR_OF_DAY));
    Assert.assertEquals(34, known.get(Calendar.MINUTE));
    Assert.assertEquals(10, known.get(Calendar.SECOND));

    // can't validate nanos because Calendar calculations are done in millis
  }

  @Test
  public void testKnownTimestampWithoutConversion() {
    // a UTC calendar will produce the same values as not converting
    Calendar calendar = toCalendar(fromJulianDay(
        JAN_1_2000, PM_3_34_10_101010101), UTC);

    Timestamp known = NanoTimeUtils.getTimestamp(
        KNOWN_TIME, true /* skip conversion from UTC to local */ );

    Assert.assertEquals(calendar.get(Calendar.YEAR) - 1900, known.getYear());
    Assert.assertEquals(calendar.get(Calendar.MONTH), known.getMonth());
    Assert.assertEquals(calendar.get(Calendar.DAY_OF_MONTH), known.getDate());
    Assert.assertEquals(calendar.get(Calendar.HOUR_OF_DAY), known.getHours());
    Assert.assertEquals(calendar.get(Calendar.MINUTE), known.getMinutes());
    Assert.assertEquals(calendar.get(Calendar.SECOND), known.getSeconds());
    Assert.assertEquals(101010101, known.getNanos());

    NanoTime actualJD = NanoTimeUtils.getNanoTime(known, true);

    Assert.assertEquals(actualJD.getJulianDay(), JAN_1_2000);
    Assert.assertEquals(actualJD.getTimeOfDayNanos(), PM_3_34_10_101010101);
  }

  @Test
  public void testKnownTimestampWithConversion() {
    // a PST calendar will produce the same values when converting to local
    Calendar calendar = toCalendar(fromJulianDay(
        JAN_1_2000, PM_3_34_10_101010101), PST); // CHANGE ME IF LOCAL IS NOT PST

    Timestamp known = NanoTimeUtils.getTimestamp(
        KNOWN_TIME, false /* do not skip conversion from UTC to local */ );

    Assert.assertEquals(calendar.get(Calendar.YEAR) - 1900, known.getYear());
    Assert.assertEquals(calendar.get(Calendar.MONTH), known.getMonth());
    Assert.assertEquals(calendar.get(Calendar.DAY_OF_MONTH), known.getDate());
    Assert.assertEquals(calendar.get(Calendar.HOUR_OF_DAY), known.getHours());
    Assert.assertEquals(calendar.get(Calendar.MINUTE), known.getMinutes());
    Assert.assertEquals(calendar.get(Calendar.SECOND), known.getSeconds());
    Assert.assertEquals(101010101, known.getNanos());

    NanoTime actualJD = NanoTimeUtils.getNanoTime(known, false);

    Assert.assertEquals(actualJD.getJulianDay(), JAN_1_2000);
    Assert.assertEquals(actualJD.getTimeOfDayNanos(), PM_3_34_10_101010101);
  }

  @Test
  public void testKnownWithZoneArgumentUTC() { // EXPECTED BEHAVIOR
    // the UTC calendar should match the alternative implementation with UTC
    Calendar calendar = toCalendar(fromJulianDay(
        JAN_1_2000, PM_3_34_10_101010101), UTC);

    Timestamp known = NanoTimeUtils.getTimestamp(
        KNOWN_TIME, Calendar.getInstance(UTC));

    Assert.assertEquals(calendar.get(Calendar.YEAR) - 1900, known.getYear());
    Assert.assertEquals(calendar.get(Calendar.MONTH), known.getMonth());
    Assert.assertEquals(calendar.get(Calendar.DAY_OF_MONTH), known.getDate());
    Assert.assertEquals(calendar.get(Calendar.HOUR_OF_DAY), known.getHours());
    Assert.assertEquals(calendar.get(Calendar.MINUTE), known.getMinutes());
    Assert.assertEquals(calendar.get(Calendar.SECOND), known.getSeconds());
    Assert.assertEquals(101010101, known.getNanos());

    NanoTime actualJD = NanoTimeUtils.getNanoTime(known, Calendar.getInstance(UTC));

    Assert.assertEquals(actualJD.getJulianDay(), JAN_1_2000);
    Assert.assertEquals(actualJD.getTimeOfDayNanos(), PM_3_34_10_101010101);
  }

  @Test
  public void testKnownWithZoneArgumentGMTP6() {
    Calendar calendar = toCalendar(fromJulianDay(
        JAN_1_2000, PM_3_34_10_101010101), PLUS_6);

    Timestamp known = NanoTimeUtils.getTimestamp(
        KNOWN_TIME, Calendar.getInstance(PLUS_6));

    Assert.assertEquals(calendar.get(Calendar.YEAR) - 1900, known.getYear());
    Assert.assertEquals(calendar.get(Calendar.MONTH), known.getMonth());
    Assert.assertEquals(calendar.get(Calendar.DAY_OF_MONTH), known.getDate());
    Assert.assertEquals(calendar.get(Calendar.HOUR_OF_DAY), known.getHours());
    Assert.assertEquals(calendar.get(Calendar.MINUTE), known.getMinutes());
    Assert.assertEquals(calendar.get(Calendar.SECOND), known.getSeconds());
    Assert.assertEquals(101010101, known.getNanos());

    NanoTime actualJD = NanoTimeUtils.getNanoTime(known, Calendar.getInstance(PLUS_6));

    Assert.assertEquals(actualJD.getJulianDay(), JAN_1_2000);
    Assert.assertEquals(actualJD.getTimeOfDayNanos(), PM_3_34_10_101010101);
  }

  @Test
  public void testKnownWithZoneArgumentGMTM6() {
    Calendar calendar = toCalendar(fromJulianDay(
        JAN_1_2000, PM_3_34_10_101010101), MINUS_6);

    Timestamp known = NanoTimeUtils.getTimestamp(
        KNOWN_TIME, Calendar.getInstance(MINUS_6));

    Assert.assertEquals(calendar.get(Calendar.YEAR) - 1900, known.getYear());
    Assert.assertEquals(calendar.get(Calendar.MONTH), known.getMonth());
    Assert.assertEquals(calendar.get(Calendar.DAY_OF_MONTH), known.getDate());
    Assert.assertEquals(calendar.get(Calendar.HOUR_OF_DAY), known.getHours());
    Assert.assertEquals(calendar.get(Calendar.MINUTE), known.getMinutes());
    Assert.assertEquals(calendar.get(Calendar.SECOND), known.getSeconds());
    Assert.assertEquals(101010101, known.getNanos());

    NanoTime actualJD = NanoTimeUtils.getNanoTime(known, Calendar.getInstance(MINUS_6));

    Assert.assertEquals(actualJD.getJulianDay(), JAN_1_2000);
    Assert.assertEquals(actualJD.getTimeOfDayNanos(), PM_3_34_10_101010101);
  }

  @Test
  public void testCompareDeprecatedTimeStampWithNewTimeStamp() {
    Timestamp newTsLocal = NanoTimeUtils.getTimestamp(KNOWN_TIME, Calendar.getInstance());
    Timestamp depTsLocal = NanoTimeUtils.getTimestamp(KNOWN_TIME, false);

    Assert.assertEquals(newTsLocal, depTsLocal);

    Timestamp newTsUTC = NanoTimeUtils.getTimestamp(KNOWN_TIME, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
    Timestamp depTsUTC = NanoTimeUtils.getTimestamp(KNOWN_TIME, true);

    Assert.assertEquals(newTsUTC, depTsUTC);
  }

  @Test
  public void testCompareDeprecatedNanoTimeWithNewNanoTime() throws ParseException {
    Date d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse("2001-01-01 15:34:01.101");
    Timestamp knownTimestamp = new Timestamp(d.getTime());

    NanoTime newNTLocal = NanoTimeUtils.getNanoTime(knownTimestamp, Calendar.getInstance());
    NanoTime depNTLocal = NanoTimeUtils.getNanoTime(knownTimestamp, false);

    Assert.assertEquals(newNTLocal.getJulianDay(), depNTLocal.getJulianDay());
    Assert.assertEquals(newNTLocal.getTimeOfDayNanos(), depNTLocal.getTimeOfDayNanos());

    NanoTime newNTUTC = NanoTimeUtils.getNanoTime(knownTimestamp, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
    NanoTime depNTUTC = NanoTimeUtils.getNanoTime(knownTimestamp, true);

    Assert.assertEquals(newNTUTC.getJulianDay(), depNTUTC.getJulianDay());
    Assert.assertEquals(newNTUTC.getTimeOfDayNanos(), depNTUTC.getTimeOfDayNanos());
  }

  @Test
  public void testTimeZoneValidationWithCorrectZoneId() {
    NanoTimeUtils.validateTimeZone("GMT");
    NanoTimeUtils.validateTimeZone("UTC");
    NanoTimeUtils.validateTimeZone("GMT+10");
    NanoTimeUtils.validateTimeZone("Europe/Budapest");
  }

  @Test(expected = IllegalStateException.class)
  public void testTimeZoneValidationWithIncorrectZoneId() {
    NanoTimeUtils.validateTimeZone("UCC");
  }
}