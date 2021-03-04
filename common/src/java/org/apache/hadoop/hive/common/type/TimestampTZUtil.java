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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Objects;
import java.util.TimeZone;

import org.apache.hive.common.util.HiveDateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimestampTZUtil {

  private static final Logger LOG = LoggerFactory.getLogger(TimestampTZ.class);

  public static TimestampTZ parse(String s) {
    return parse(s, null);
  }

  public static TimestampTZ parse(String text, ZoneId defaultTimeZone) {
    Objects.requireNonNull(text);

    final String s = HiveDateTimeFormatter.handleSingleDigitHourOffset(text.trim());

    TemporalAccessor accessor =
        HiveDateTimeFormatter.HIVE_DATE_TIME_ZONED.parseBest(s, ZonedDateTime::from, LocalDateTime::from);

    final ZonedDateTime zonedDateTime;

    if (ZonedDateTime.class.isInstance(accessor)) {
      zonedDateTime = ZonedDateTime.class.cast(accessor);
    } else {
      if (defaultTimeZone == null) {
        throw new DateTimeException("Time Zone not available");
      }
      if (LocalDateTime.class.isInstance(accessor)) {
        zonedDateTime = ZonedDateTime.of(LocalDateTime.class.cast(accessor), defaultTimeZone);
      } else {
        // Should never happen as parseBest itself will error if there are no
        // matches
        throw new DateTimeException("Unknown TemporalAccessor type");
      }
    }

    if (defaultTimeZone == null) {
      return new TimestampTZ(zonedDateTime);
    }
    return new TimestampTZ(zonedDateTime.withZoneSameInstant(defaultTimeZone));
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
    return new TimestampTZ(ZonedDateTime.ofInstant(Instant.ofEpochMilli(date.toEpochMilli()), ZoneOffset.UTC)
        .withZoneSameLocal(defaultTimeZone));
  }

  // Converts Timestamp to TimestampTZ.
  public static TimestampTZ convert(Timestamp ts, ZoneId defaultTimeZone) {
    return new TimestampTZ(
        ZonedDateTime.ofInstant(Instant.ofEpochSecond(ts.toEpochSecond(), ts.getNanos()), ZoneOffset.UTC)
            .withZoneSameLocal(defaultTimeZone));
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

  private static final ThreadLocal<DateFormat> LEGACY_DATE_FORMATTER = new ThreadLocal<>();

  private static DateFormat getLegacyDateFormatter() {
    if (LEGACY_DATE_FORMATTER.get() == null) {
      LEGACY_DATE_FORMATTER.set(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
    }
    return LEGACY_DATE_FORMATTER.get();
  }

  public static Timestamp convertTimestampToZone(Timestamp ts, ZoneId fromZone, ZoneId toZone) {
    return convertTimestampToZone(ts, fromZone, toZone, false);
  }

  /**
   * Timestamps are technically time zone agnostic, and this method sort of cheats its logic.
   * Timestamps are supposed to represent nanos since [UTC epoch]. Here,
   * the input timestamp represents nanoseconds since [epoch at fromZone], and
   * we return a Timestamp representing nanoseconds since [epoch at toZone].
   */
  public static Timestamp convertTimestampToZone(Timestamp ts, ZoneId fromZone, ZoneId toZone,
      boolean legacyConversion) {
    if (legacyConversion) {
      try {
        DateFormat formatter = getLegacyDateFormatter();
        formatter.setTimeZone(TimeZone.getTimeZone(fromZone));
        java.util.Date date = formatter.parse(ts.toString());
        // Set the formatter to use a different timezone
        formatter.setTimeZone(TimeZone.getTimeZone(toZone));
        Timestamp result = Timestamp.valueOf(formatter.format(date));
        result.setNanos(ts.getNanos());
        return result;
      } catch (ParseException e) {
        throw new RuntimeException(e);
      }
    }

    // get nanos since [epoch at fromZone]
    Instant instant = convert(ts, fromZone).getZonedDateTime().toInstant();
    // get [local time at toZone]
    LocalDateTime localDateTimeAtToZone = LocalDateTime.ofInstant(instant, toZone);
    // get nanos between [epoch at toZone] and [local time at toZone]
    return Timestamp.ofEpochSecond(localDateTimeAtToZone.toEpochSecond(ZoneOffset.UTC),
        localDateTimeAtToZone.getNano());
  }
}
