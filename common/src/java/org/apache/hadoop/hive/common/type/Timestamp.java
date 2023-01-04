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

import org.apache.hive.common.util.SuppressFBWarnings;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

/**
 * This is the internal type for Timestamp. The full qualified input format of
 * Timestamp is "uuuu-MM-dd HH:mm:ss[.SSS...]", where the time part is optional.
 * If time part is absent, a default '00:00:00.0' will be used.
 *
 * <table border="2" summary="">
 * <tr>
 * <th>Field</th>
 * <th>Format</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>Year</td>
 * <td>uuuu</td>
 * <td>The proleptic year, such as 2012. This represents the concept of the
 * year, counting sequentially and using negative numbers.</td>
 * </tr>
 * <tr>
 * <td>Month of Year</td>
 * <td>MM</td>
 * <td>The month-of-year, such as March. This represents the concept of the
 * month within the year. In the default ISO calendar system, this has values
 * from January (1) to December (12).</td>
 * </tr>
 * <tr>
 * <td>Day of Month</td>
 * <td>dd</td>
 * <td>This represents the concept of the day within the month. In the default
 * ISO calendar system, this has values from 1 to 31 in most months.</td>
 * </tr>
 * </table>
 * <p>
 * The {@link ChronoField#YEAR} and "uuuu" format string indicate the year. This
 * is not to be confused with the more common "yyyy" which standard for
 * "year-of-era" in Java. One important difference is that "year" includes
 * negative numbers whereas the "year-of-era" value should typically always be
 * positive.
 * </p>
 *
 * @see java.time.temporal.ChronoField#YEAR
 * @see java.time.temporal.ChronoField#YEAR_OF_ERA
 */
public class Timestamp implements Comparable<Timestamp> {
  
  private static final LocalDateTime EPOCH = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
  private static final DateTimeFormatter PARSE_FORMATTER = new DateTimeFormatterBuilder()
      // Date
      .appendValue(YEAR, 1, 10, SignStyle.NORMAL).appendLiteral('-').appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL)
      .appendLiteral('-').appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NORMAL)
      // Time (Optional)
      .optionalStart().appendLiteral(" ").appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NORMAL).appendLiteral(':')
      .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NORMAL).appendLiteral(':')
      .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NORMAL).optionalStart()
      .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true).optionalEnd().optionalEnd().toFormatter()
      .withResolverStyle(ResolverStyle.STRICT);

  private static final DateTimeFormatter PRINT_FORMATTER = new DateTimeFormatterBuilder()
      // Date and Time Parts
      .append(DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss"))
      // Fractional Part (Optional)
      .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd().toFormatter();

  private LocalDateTime localDateTime;

  public Timestamp(LocalDateTime localDateTime) {
    this.localDateTime = localDateTime != null ? localDateTime : EPOCH;
  }

  public Timestamp() {
    this(EPOCH);
  }

  public Timestamp(Timestamp t) {
    this(t.localDateTime);
  }

  public void set(Timestamp t) {
    this.localDateTime = t != null ? t.localDateTime : EPOCH;
  }

  public String format(DateTimeFormatter formatter) {
    return localDateTime.format(formatter);
  }

  @Override
  public String toString() {
    return localDateTime.format(PRINT_FORMATTER);
  }

  public int hashCode() {
    return localDateTime.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof Timestamp) {
      return compareTo((Timestamp) other) == 0;
    }
    return false;
  }

  @Override
  public int compareTo(Timestamp o) {
    return localDateTime.compareTo(o.localDateTime);
  }

  public long toEpochSecond() {
    return localDateTime.toEpochSecond(ZoneOffset.UTC);
  }

  public void setTimeInSeconds(long epochSecond) {
    setTimeInSeconds(epochSecond, 0);
  }

  public void setTimeInSeconds(long epochSecond, int nanos) {
    localDateTime = LocalDateTime.ofEpochSecond(
        epochSecond, nanos, ZoneOffset.UTC);
  }

  public long toEpochMilli() {
    return localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  public long toEpochMilli(ZoneId id) {
    return localDateTime.atZone(id).toInstant().toEpochMilli();
  }

  public void setTimeInMillis(long epochMilli) {
    localDateTime = LocalDateTime.ofInstant(
        Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC);
  }

  public void setTimeInMillis(long epochMilli, int nanos) {
    localDateTime = LocalDateTime
        .ofInstant(Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC)
        .withNano(nanos);
  }

  public int getNanos() {
    return localDateTime.getNano();
  }

  public static Timestamp valueOf(String s) {
    s = s.trim();
    LocalDateTime localDateTime;
    try {
      localDateTime = LocalDateTime.parse(s, PARSE_FORMATTER);
    } catch (DateTimeException e) {
      // Try ISO-8601 format
      try {
        localDateTime = LocalDateTime.parse(s);
      } catch (DateTimeException e2) {
        throw new IllegalArgumentException("Cannot create timestamp, parsing error " + s);
      }
    }
    return new Timestamp(localDateTime);
  }

  public static Timestamp getTimestampFromTime(String s) {
    return new Timestamp(LocalDateTime.of(LocalDate.now(),
        LocalTime.parse(s, DateTimeFormatter.ISO_LOCAL_TIME)));
  }

  public static Timestamp ofEpochSecond(long epochSecond) {
    return ofEpochSecond(epochSecond, 0);
  }

  public static Timestamp ofEpochSecond(long epochSecond, int nanos) {
    return new Timestamp(
        LocalDateTime.ofEpochSecond(epochSecond, nanos, ZoneOffset.UTC));
  }

  public static Timestamp ofEpochSecond(long epochSecond, long nanos, ZoneId zone) {
    return new Timestamp(LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSecond, nanos), zone));
  }

  public static Timestamp ofEpochMilli(long epochMilli) {
    return new Timestamp(LocalDateTime
        .ofInstant(Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC));
  }

  public static Timestamp ofEpochMilli(long epochMilli, ZoneId id) {
    return new Timestamp(LocalDateTime
        .ofInstant(Instant.ofEpochMilli(epochMilli), id));
  }

  public static Timestamp ofEpochMilli(long epochMilli, int nanos) {
    return new Timestamp(LocalDateTime
        .ofInstant(Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC)
        .withNano(nanos));
  }

  public void setNanos(int nanos) {
    localDateTime = localDateTime.withNano(nanos);
  }

  public int getYear() {
    return localDateTime.getYear();
  }

  public int getMonth() {
    return localDateTime.getMonthValue();
  }

  public int getDay() {
    return localDateTime.getDayOfMonth();
  }

  public int getHours() {
    return localDateTime.getHour();
  }

  public int getMinutes() {
    return localDateTime.getMinute();
  }

  public int getSeconds() {
    return localDateTime.getSecond();
  }

  public int getDayOfWeek() {
    return localDateTime.getDayOfWeek().plus(1).getValue();
  }

  /**
   * Return a copy of this object.
   */
  @Override
  @SuppressFBWarnings(value = "CN_IMPLEMENTS_CLONE_BUT_NOT_CLONEABLE", justification = "Intended")
  public Object clone() {
    // LocalDateTime is immutable.
    return new Timestamp(this.localDateTime);
  }

  public java.sql.Timestamp toSqlTimestamp() {
    java.sql.Timestamp ts = new java.sql.Timestamp(toEpochMilli());
    ts.setNanos(getNanos());
    return ts;
  }

}
