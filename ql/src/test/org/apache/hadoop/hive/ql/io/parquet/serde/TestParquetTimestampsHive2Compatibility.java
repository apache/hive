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
package org.apache.hadoop.hive.ql.io.parquet.serde;

import jodd.time.JulianDate;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTime;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import com.google.common.base.Strings;

import java.time.ZoneId;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

/**
 * Compatibility tests for timestamp serialization/deserialization in Parquet files.
 * <p>
 * The main goal of the suite is to test that writing parquet files in Hive2 and reading them in Hive4 produces the
 * expected results and vice-versa. 
 * </p>
 * <p>
 * It is difficult to come up with an end-to-end test between Hive2 and Hive4 in both directions so we are limiting
 * the test to few APIs that are responsible for the conversion assuming that the main code path still relies on these
 * APIs. In order to test compatibility with Hive2 some pieces of code were copied from branch-2.3 inside this
 * class with a few changes to account for library upgrades (e.g., jodd-util). 
 * </p>
 */
class TestParquetTimestampsHive2Compatibility {
  private static final long NANOS_PER_HOUR = TimeUnit.HOURS.toNanos(1);
  private static final long NANOS_PER_MINUTE = TimeUnit.MINUTES.toNanos(1);
  private static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
  private static final long NANOS_PER_DAY = TimeUnit.DAYS.toNanos(1);

  /**
   * Tests that timestamps written using Hive2 APIs are read correctly by Hive2 APIs.
   *
   * This is test is here just for sanity reasons in case somebody changes something in the code and breaks
   * the Hive2 APIs.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("generateTimestamps")
  void testWriteHive2ReadHive2(String timestampString) {
    NanoTime nt = writeHive2(timestampString);
    java.sql.Timestamp ts = readHive2(nt);
    assertEquals(timestampString, ts.toString());
  }

  /**
   * Tests that timestamps written using Hive2 APIs are read correctly by Hive4 APIs when legacy conversion is on.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("generateTimestamps")
  void testWriteHive2ReadHive4UsingLegacyConversion(String timestampString) {
    NanoTime nt = writeHive2(timestampString);
    Timestamp ts = readHive4(nt, TimeZone.getDefault().getID(), true);
    assertEquals(timestampString, ts.toString());
  }

  /**
   * Tests that timestamps written using Hive2 APIs are read correctly by Hive4 APIs when legacy conversion is on.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("generateTimestamps")
  void testWriteHive2ReadHive4UsingLegacyConversionWithZone(String timestampString) {
    TimeZone original = TimeZone.getDefault();
    try {
      String zoneId = "US/Pacific";
      TimeZone.setDefault(TimeZone.getTimeZone(zoneId));
      NanoTime nt = writeHive2(timestampString);
      Timestamp ts = readHive4(nt, zoneId, true);
      assertEquals(timestampString, ts.toString());
    } finally {
      TimeZone.setDefault(original);
    }
  }

  /**
   * Tests that timestamps written using Hive2 APIs on julian leap years are read correctly by Hive4 APIs when legacy
   * conversion is on.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("generateTimestampsAndZoneIds")
  void testWriteHive2ReadHive4UsingLegacyConversionWithJulianLeapYears(String timestampString, String zoneId) {
    TimeZone original = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(zoneId));
      NanoTime nt = writeHive2(timestampString);
      Timestamp ts = readHive4(nt, zoneId, true);
      assertEquals(timestampString, ts.toString());
    } finally {
      TimeZone.setDefault(original);
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("generateTimestampsAndZoneIds28thFeb")
  void testWriteHive2ReadHive4UsingLegacyConversionWithJulianLeapYearsFor28thFeb(String timestampString,
      String zoneId) {
    TimeZone original = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(zoneId));
      NanoTime nt = writeHive2(timestampString);
      Timestamp ts = readHive4(nt, zoneId, true);
      assertEquals(timestampString, ts.toString());
    } finally {
      TimeZone.setDefault(original);
    }
  }

  @ParameterizedTest(name = " - From: Zone {0}, timestamp: {2}, To: Zone:{1}, expected Timestamp {3}")
  @MethodSource("julianLeapYearEdgeCases")
  void testWriteHive2ReadHive4UsingLegacyConversionWithJulianLeapYearsEdgeCase(String fromZoneId, String toZoneId,
      String timestampString, String expected) {
    TimeZone original = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(fromZoneId));
      NanoTime nt = writeHive2(timestampString);
      Timestamp ts = readHive4(nt, toZoneId, true);
      assertEquals(expected, ts.toString());
    } finally {
      TimeZone.setDefault(original);
    }
  }

  private static Stream<Arguments> julianLeapYearEdgeCases() {
    return Stream.of(Arguments.of("GMT-12:00", "GMT+14:00", "0200-02-27 22:00:00.000000001",
            "0200-03-01 00:00:00.000000001"),
        Arguments.of("GMT+14:00", "GMT-12:00", "0200-03-01 00:00:00.000000001",
            "0200-02-27 22:00:00.000000001"),
        Arguments.of("GMT+14:00", "GMT-12:00", "0200-03-02 00:00:00.000000001",
            "0200-02-28 22:00:00.000000001"),
        Arguments.of("GMT-12:00", "GMT+14:00", "0200-03-02 00:00:00.000000001",
            "0200-03-03 02:00:00.000000001"),
        Arguments.of("GMT-12:00", "GMT+12:00", "0200-02-28 00:00:00.000000001", "0200-03-01 00:00:00.000000001"),
        Arguments.of("GMT+12:00", "GMT-12:00", "0200-03-01 00:00:00.000000001", "0200-02-28 00:00:00.000000001"),
        Arguments.of("Asia/Singapore", "Asia/Singapore", "0200-03-01 00:00:00.000000001",
            "0200-03-01 00:00:00.000000001"));
  }

  /**
   * Tests that timestamps written using Hive4 APIs are read correctly by Hive4 APIs when legacy conversion is on. 
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("generateTimestamps")
  void testWriteHive4ReadHive4UsingLegacyConversion(String timestampString) {
    String zoneId = "US/Pacific";
    NanoTime nt = writeHive4(timestampString, zoneId, true);
    Timestamp ts = readHive4(nt, zoneId, true);
    assertEquals(timestampString, ts.toString());
  }

  /**
   * Tests that timestamps written using Hive4 APIs are read correctly by Hive4 APIs when legacy conversion is off.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("generateTimestamps")
  void testWriteHive4ReadHive4UsingNewConversion(String timestampString) {
    String zoneId = "US/Pacific";
    NanoTime nt = writeHive4(timestampString, zoneId, false);
    Timestamp ts = readHive4(nt, zoneId, false);
    assertEquals(timestampString, ts.toString());
  }

  /**
   * Tests that timestamps written using Hive4 APIs are read correctly by Hive2 APIs when legacy conversion is on when
   * writing.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("generateTimestamps")
  void testWriteHive4UsingLegacyConversionReadHive2(String timestampString) {
    NanoTime nt = writeHive4(timestampString, TimeZone.getDefault().getID(), true);
    java.sql.Timestamp ts = readHive2(nt);
    assertEquals(timestampString, ts.toString());
  }

  private static Stream<String> generateTimestamps() {
    return Stream.concat(Stream.generate(new Supplier<String>() {
      int i = 0;

      @Override
      public String get() {
        StringBuilder sb = new StringBuilder(29);
        int year = (i % 9999) + 1;
        sb.append(zeros(4 - digits(year)));
        sb.append(year);
        sb.append('-');
        int month = (i % 12) + 1;
        sb.append(zeros(2 - digits(month)));
        sb.append(month);
        sb.append('-');
        int day = (i % 28) + 1;
        sb.append(zeros(2 - digits(day)));
        sb.append(day);
        sb.append(' ');
        int hour = i % 24;
        sb.append(zeros(2 - digits(hour)));
        sb.append(hour);
        sb.append(':');
        int minute = i % 60;
        sb.append(zeros(2 - digits(minute)));
        sb.append(minute);
        sb.append(':');
        int second = i % 60;
        sb.append(zeros(2 - digits(second)));
        sb.append(second);
        sb.append('.');
        // Bitwise OR with one to avoid times with trailing zeros
        int nano = (i % 1000000000) | 1;
        sb.append(zeros(9 - digits(nano)));
        sb.append(nano);
        i++;
        return sb.toString();
      }
    })
    // Exclude dates falling in the default Gregorian change date since legacy code does not handle that interval
    // gracefully. It is expected that these do not work well when legacy APIs are in use. 
    .filter(s -> !s.startsWith("1582-10"))
    .limit(3000), Stream.of("9999-12-31 23:59:59.999"));
  }

  /** Generates timestamps for different timezone. Here we are testing UTC+14 : Pacific/Kiritimati ,
   *  UTC-12 : Etc/GMT+12 along with few other zones
   */
  private static Stream<Arguments> generateTimestampsAndZoneIds() {
    return generateJulianLeapYearTimestamps().flatMap(
        timestampString -> Stream.of("Asia/Singapore", "Pacific/Kiritimati", "Etc/GMT+12", "Pacific/Niue")
            .map(zoneId -> Arguments.of(timestampString, zoneId)));
  }

  private static Stream<Arguments> generateTimestampsAndZoneIds28thFeb() {
    return generateJulianLeapYearTimestamps28thFeb().flatMap(
        timestampString -> Stream.of("Asia/Singapore", "Pacific/Kiritimati", "Etc/GMT+12", "Pacific/Niue")
            .map(zoneId -> Arguments.of(timestampString, zoneId)));
  }

  private static Stream<String> generateJulianLeapYearTimestamps() {
    return IntStream.range(1, 100)
    .mapToObj(value -> Strings.padStart(String.valueOf(value * 100), 4, '0'))
    .map(value -> value + "-03-01 00:00:00.000000001");
  }

  private static Stream<String> generateJulianLeapYearTimestamps28thFeb() {
    return IntStream.range(1, 100)
    .mapToObj(value -> Strings.padStart(String.valueOf(value * 100), 4, '0'))
    .map(value -> value + "-02-28 00:00:00.000000001");
  }

  private static int digits(int number) {
    int digits = 0;
    do {
      digits++;
      number = number / 10;
    } while (number != 0);
    return digits;
  }

  private static char[] zeros(int len) {
    char[] array = new char[len];
    for (int i = 0; i < len; i++) {
      array[i] = '0';
    }
    return array;
  }

  private static java.sql.Timestamp toTimestampHive2(String s) {
    java.sql.Timestamp result;
    s = s.trim();

    // Throw away extra if more than 9 decimal places
    int periodIdx = s.indexOf(".");
    if (periodIdx != -1) {
      if (s.length() - periodIdx > 9) {
        s = s.substring(0, periodIdx + 10);
      }
    }
    if (s.indexOf(' ') < 0) {
      s = s.concat(" 00:00:00");
    }
    try {
      result = java.sql.Timestamp.valueOf(s);
    } catch (IllegalArgumentException e) {
      result = null;
    }
    return result;
  }

  /**
   * Converts the specified timestamp to nano time using Hive2 legacy code.
   *
   * In Hive2, the input string is considered to be in the default system timezone and it is converted to GMT before
   * it is transformed to nano time.
   */
  private static NanoTime writeHive2(String str) {
    java.sql.Timestamp ts = toTimestampHive2(str);
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of("GMT")));
    calendar.setTime(ts);
    int year = calendar.get(Calendar.YEAR);
    if (calendar.get(Calendar.ERA) == GregorianCalendar.BC) {
      year = 1 - year;
    }
    JulianDate jDateTime = JulianDate.of(year, calendar.get(Calendar.MONTH) + 1,  //java calendar index starting at 1.
        calendar.get(Calendar.DAY_OF_MONTH), 0, 0, 0, 0);
    int days = jDateTime.getJulianDayNumber();

    long hour = calendar.get(Calendar.HOUR_OF_DAY);
    long minute = calendar.get(Calendar.MINUTE);
    long second = calendar.get(Calendar.SECOND);
    long nanos = ts.getNanos();
    long nanosOfDay = nanos + NANOS_PER_SECOND * second + NANOS_PER_MINUTE * minute + NANOS_PER_HOUR * hour;

    return new NanoTime(days, nanosOfDay);
  }

  /**
   * Converts the specified nano time to a java.sql.Timestamp using Hive2 legacy code.
   */
  private static java.sql.Timestamp readHive2(NanoTime nt) {
    //Current Hive parquet timestamp implementation stores it in UTC, but other components do not do that.
    //If this file written by current Hive implementation itself, we need to do the reverse conversion, else skip the conversion.
    int julianDay = nt.getJulianDay();
    long nanosOfDay = nt.getTimeOfDayNanos();

    long remainder = nanosOfDay;
    julianDay += remainder / NANOS_PER_DAY;
    remainder %= NANOS_PER_DAY;
    if (remainder < 0) {
      remainder += NANOS_PER_DAY;
      julianDay--;
    }

    JulianDate jDateTime = JulianDate.of((double) julianDay);
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of("GMT")));
    calendar.set(Calendar.YEAR, jDateTime.toLocalDateTime().getYear());
    calendar.set(Calendar.MONTH, jDateTime.toLocalDateTime().getMonthValue() - 1); //java calendar index starting at 1.
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
    java.sql.Timestamp ts = new java.sql.Timestamp(calendar.getTimeInMillis());
    ts.setNanos((int) nanos);
    return ts;
  }

  private static NanoTime writeHive4(String str, String sourceZone, boolean legacyConversion) {
    Timestamp newTs = PrimitiveObjectInspectorUtils.getTimestampFromString(str);
    return NanoTimeUtils.getNanoTime(newTs, ZoneId.of(sourceZone), legacyConversion);
  }

  private static Timestamp readHive4(NanoTime nt, String targetZone, boolean legacyConversion) {
    return NanoTimeUtils.getTimestamp(nt, ZoneId.of(targetZone), legacyConversion);
  }

}
