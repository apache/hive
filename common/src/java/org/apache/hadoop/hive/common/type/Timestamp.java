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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
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
 * This is the internal type for Timestamp.
 * The full qualified input format of Timestamp is
 * "yyyy-MM-dd HH:mm:ss[.SSS...]", where the time part is optional.
 * If time part is absent, a default '00:00:00.0' will be used.
 */
public class Timestamp implements Comparable<Timestamp> {
  
  private static final LocalDateTime EPOCH = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
  private static final DateTimeFormatter PARSE_FORMATTER;
  private static final DateTimeFormatter PRINT_FORMATTER;

  static {
    DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
    // Date part
    builder.appendValue(YEAR, 1, 10, SignStyle.NORMAL)
        .appendLiteral('-')
        .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL)
        .appendLiteral('-')
        .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NORMAL);
    // Time part
    builder
        .optionalStart().appendLiteral(" ")
        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NORMAL)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NORMAL)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NORMAL)
        .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true).optionalEnd()
        .optionalEnd();
    PARSE_FORMATTER = builder.toFormatter().withResolverStyle(ResolverStyle.LENIENT);
    builder = new DateTimeFormatterBuilder();
    // Date and time parts
    builder.append(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    // Fractional part
    builder.optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd();
    PRINT_FORMATTER = builder.toFormatter();
  }

  private LocalDateTime localDateTime;

  /* Private constructor */
  private Timestamp(LocalDateTime localDateTime) {
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
    } catch (DateTimeParseException e) {
      // Try ISO-8601 format
      try {
        localDateTime = LocalDateTime.parse(s);
      } catch (DateTimeParseException e2) {
        throw new IllegalArgumentException("Cannot create timestamp, parsing error");
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

  public static Timestamp ofEpochMilli(long epochMilli) {
    return new Timestamp(LocalDateTime
        .ofInstant(Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC));
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
