/**
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
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

/**
 * This is the internal type for Timestamp with time zone.
 * A wrapper of ZonedDateTime which automatically convert the Zone to UTC.
 * The full qualified input format of Timestamp with time zone is
 * "yyyy-MM-dd HH:mm:ss[.SSS...] zoneid/zoneoffset", where the time and zone parts are optional.
 * If time part is absent, a default '00:00:00.0' will be used.
 * If zone part is absent, the system time zone will be used.
 * All timestamp with time zone will be converted and stored as UTC retaining the instant.
 * E.g. "2017-04-14 18:00:00 Asia/Shanghai" will be converted to
 * "2017-04-14 10:00:00.0 Z".
 */
public class TimestampTZ implements Comparable<TimestampTZ> {

  private static final DateTimeFormatter formatter;
  private static final ZoneId UTC = ZoneOffset.UTC;
  private static final ZonedDateTime EPOCH = ZonedDateTime.ofInstant(Instant.EPOCH, UTC);
  private static final LocalTime DEFAULT_LOCAL_TIME = LocalTime.of(0, 0);
  private static final Pattern SINGLE_DIGIT_PATTERN = Pattern.compile("[\\+-]\\d:\\d\\d");
  private static final Logger LOG = LoggerFactory.getLogger(TimestampTZ.class);

  private static final ThreadLocal<DateFormat> CONVERT_FORMATTER =
      ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

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

    formatter = builder.toFormatter();
  }

  private ZonedDateTime zonedDateTime;

  public TimestampTZ() {
    this(EPOCH);
  }

  public TimestampTZ(ZonedDateTime zonedDateTime) {
    setZonedDateTime(zonedDateTime);
  }

  public TimestampTZ(long seconds, int nanos) {
    set(seconds, nanos);
  }

  public void set(long seconds, int nanos) {
    Instant instant = Instant.ofEpochSecond(seconds, nanos);
    setZonedDateTime(ZonedDateTime.ofInstant(instant, UTC));
  }

  public ZonedDateTime getZonedDateTime() {
    return zonedDateTime;
  }

  public void setZonedDateTime(ZonedDateTime zonedDateTime) {
    this.zonedDateTime = zonedDateTime != null ? zonedDateTime.withZoneSameInstant(UTC) : EPOCH;
  }

  @Override
  public String toString() {
    return zonedDateTime.format(formatter);
  }

  @Override
  public int hashCode() {
    return zonedDateTime.toInstant().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof TimestampTZ) {
      return compareTo((TimestampTZ) other) == 0;
    }
    return false;
  }

  @Override
  public int compareTo(TimestampTZ o) {
    return zonedDateTime.toInstant().compareTo(o.zonedDateTime.toInstant());
  }

  public long getEpochSecond() {
    return zonedDateTime.toInstant().getEpochSecond();
  }

  public int getNanos() {
    return zonedDateTime.toInstant().getNano();
  }

  public static TimestampTZ parse(String s) {
    // need to handle offset with single digital hour, see JDK-8066806
    s = handleSingleDigitHourOffset(s);
    ZonedDateTime zonedDateTime;
    try {
      zonedDateTime = ZonedDateTime.parse(s, formatter);
    } catch (DateTimeParseException e) {
      // try to be more tolerant
      // if the input is invalid instead of incomplete, we'll hit exception here again
      TemporalAccessor accessor = formatter.parse(s);
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
        // TODO: in future this may come from user specified zone (via set time zone command)
        zoneId = ZoneId.systemDefault();
      }
      zonedDateTime = ZonedDateTime.of(localDate, localTime, zoneId);
    }

    return new TimestampTZ(zonedDateTime);
  }

  private static String handleSingleDigitHourOffset(String s) {
    Matcher matcher = SINGLE_DIGIT_PATTERN.matcher(s);
    if (matcher.find()) {
      int index = matcher.start() + 1;
      s = s.substring(0, index) + "0" + s.substring(index, s.length());
    }
    return s;
  }

  public static TimestampTZ parseOrNull(String s) {
    try {
      return parse(s);
    } catch (DateTimeParseException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Invalid string " + s + " for TIMESTAMP WITH TIME ZONE", e);
      }
      return null;
    }
  }

  // Converts Date to TimestampTZ. The conversion is done text-wise since
  // Date/Timestamp should be treated as description of date/time.
  public static TimestampTZ convert(java.util.Date date) {
    String s = date instanceof Timestamp ? date.toString() : CONVERT_FORMATTER.get().format(date);
    // TODO: in future this may come from user specified zone (via set time zone command)
    return parse(s + " " + ZoneId.systemDefault().getId());
  }
}
