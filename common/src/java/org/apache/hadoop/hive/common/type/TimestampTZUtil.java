/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.common.type;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimestampTZUtil {

  private static final Logger LOG = LoggerFactory.getLogger(TimestampTZ.class);

  private static final LocalTime DEFAULT_LOCAL_TIME = LocalTime.of(0, 0);
  private static final Pattern SINGLE_DIGIT_PATTERN = Pattern.compile("[\\+-]\\d:\\d\\d");

  static final DateTimeFormatter FORMATTER;
  static {
    DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
    // Date part
    builder.append(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    // Time part
    builder.optionalStart().appendLiteral(" ").append(DateTimeFormatter.ofPattern("HH:mm:ss")).
        optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true).
        optionalEnd().optionalEnd();
    // Zone part
    builder.optionalStart().appendLiteral(" ").optionalEnd();
    builder.optionalStart().appendZoneText(TextStyle.NARROW).optionalEnd();

    FORMATTER = builder.toFormatter();
  }

  public static TimestampTZ parse(String s) {
    return parse(s, null);
  }

  public static TimestampTZ parse(String s, ZoneId defaultTimeZone) {
    // need to handle offset with single digital hour, see JDK-8066806
    s = handleSingleDigitHourOffset(s);
    ZonedDateTime zonedDateTime;
    try {
      zonedDateTime = ZonedDateTime.parse(s, FORMATTER);
    } catch (DateTimeParseException e) {
      // try to be more tolerant
      // if the input is invalid instead of incomplete, we'll hit exception here again
      TemporalAccessor accessor = FORMATTER.parse(s);
      // LocalDate must be present
      LocalDate localDate = LocalDate.from(accessor);
      LocalTime localTime;
      ZoneId zoneId;
      try {
        localTime = LocalTime.from(accessor);
      } catch (DateTimeException e1) {
        localTime = DEFAULT_LOCAL_TIME;
      }
      try {
        zoneId = ZoneId.from(accessor);
      } catch (DateTimeException e2) {
        if (defaultTimeZone == null) {
          throw new DateTimeException("Time Zone not available");
        }
        zoneId = defaultTimeZone;
      }
      zonedDateTime = ZonedDateTime.of(localDate, localTime, zoneId);
    }

    if (defaultTimeZone == null) {
      return new TimestampTZ(zonedDateTime);
    }
    return new TimestampTZ(zonedDateTime.withZoneSameInstant(defaultTimeZone));
  }

  private static String handleSingleDigitHourOffset(String s) {
    Matcher matcher = SINGLE_DIGIT_PATTERN.matcher(s);
    if (matcher.find()) {
      int index = matcher.start() + 1;
      s = s.substring(0, index) + "0" + s.substring(index, s.length());
    }
    return s;
  }


  public static TimestampTZ parseOrNull(String s, ZoneId defaultTimeZone) {
    try {
      return parse(s, defaultTimeZone);
    } catch (DateTimeParseException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Invalid string " + s + " for TIMESTAMP WITH TIME ZONE", e);
      }
      return null;
    }
  }

  // Converts Date to TimestampTZ.
  public static TimestampTZ convert(Date date, ZoneId defaultTimeZone) {
    return parse(date.toString(), defaultTimeZone);
  }

  // Converts Timestamp to TimestampTZ.
  public static TimestampTZ convert(Timestamp ts, ZoneId defaultTimeZone) {
    return parse(ts.toString(), defaultTimeZone);
  }

  public static ZoneId parseTimeZone(String timeZoneStr) {
    if (timeZoneStr == null || timeZoneStr.trim().isEmpty() ||
        timeZoneStr.trim().toLowerCase().equals("local")) {
      // default
      return ZoneId.systemDefault();
    }
    try {
      return ZoneId.of(timeZoneStr);
    } catch (DateTimeException e1) {
      // default
      throw new RuntimeException("Invalid time zone displacement value", e1);
    }
  }

  /**
   * Timestamps are technically time zone agnostic, and this method sort of cheats its logic.
   * Timestamps are supposed to represent nanos since [UTC epoch]. Here,
   * the input timestamp represents nanoseconds since [epoch at fromZone], and
   * we return a Timestamp representing nanoseconds since [epoch at toZone].
   */
  public static Timestamp convertTimestampToZone(Timestamp ts, ZoneId fromZone, ZoneId toZone) {
    // get nanos since [epoch at fromZone]
    Instant instant = convert(ts, fromZone).getZonedDateTime().toInstant();
    // get [local time at toZone]
    LocalDateTime localDateTimeAtToZone = LocalDateTime.ofInstant(instant, toZone);
    // get nanos between [epoch at toZone] and [local time at toZone]
    return Timestamp.ofEpochSecond(localDateTimeAtToZone.toEpochSecond(ZoneOffset.UTC),
        localDateTimeAtToZone.getNano());
  }
}
