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
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;

/**
 * This is the internal type for Date.
 * The full qualified input format of Date is "yyyy-MM-dd".
 */
public class Date implements Comparable<Date> {

  private static final LocalDate EPOCH = LocalDate.of(1970, 1, 1);
  private static final DateTimeFormatter PARSE_FORMATTER;
  private static final DateTimeFormatter PRINT_FORMATTER;
  static {
    DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
    builder.appendValue(YEAR, 1, 10, SignStyle.NORMAL)
        .appendLiteral('-')
        .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL)
        .appendLiteral('-')
        .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NORMAL);
    PARSE_FORMATTER = builder.toFormatter().withResolverStyle(ResolverStyle.LENIENT);
    builder = new DateTimeFormatterBuilder();
    builder.append(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    PRINT_FORMATTER = builder.toFormatter();
  }

  private LocalDate localDate;

  private Date(LocalDate localDate) {
    this.localDate = localDate != null ? localDate : EPOCH;
  }

  public Date() {
    this(EPOCH);
  }

  public Date(Date d) {
    this(d.localDate);
  }

  @Override
  public String toString() {
    return localDate.format(PRINT_FORMATTER);
  }

  public int hashCode() {
    return localDate.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof Date) {
      return compareTo((Date) other) == 0;
    }
    return false;
  }

  @Override
  public int compareTo(Date o) {
    return localDate.compareTo(o.localDate);
  }

  public int toEpochDay() {
    return (int) localDate.toEpochDay();
  }

  public long toEpochSecond() {
    return localDate.atStartOfDay().toEpochSecond(ZoneOffset.UTC);
  }

  public long toEpochMilli() {
    return localDate.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  public void setYear(int year) {
    localDate = localDate.withYear(year);
  }

  public void setMonth(int month) {
    localDate = localDate.withMonth(month);
  }

  public void setDayOfMonth(int dayOfMonth) {
    localDate = localDate.withDayOfMonth(dayOfMonth);
  }

  public void setTimeInDays(int epochDay) {
    localDate = LocalDate.ofEpochDay(epochDay);
  }

  public void setTimeInMillis(long epochMilli) {
    localDate = LocalDateTime.ofInstant(
        Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC).toLocalDate();
  }

  public static Date valueOf(String s) {
    s = s.trim();
    int idx = s.indexOf(" ");
    if (idx != -1) {
      s = s.substring(0, idx);
    } else {
      idx = s.indexOf('T');
      if (idx != -1) {
        s = s.substring(0, idx);
      }
    }
    LocalDate localDate;
    try {
      localDate = LocalDate.parse(s, PARSE_FORMATTER);
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException("Cannot create date, parsing error");
    }
    return new Date(localDate);
  }

  public static Date ofEpochDay(int epochDay) {
    return new Date(LocalDate.ofEpochDay(epochDay));
  }

  public static Date ofEpochMilli(long epochMilli) {
    return new Date(LocalDateTime.ofInstant(
        Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC).toLocalDate());
  }

  public static Date of(int year, int month, int dayOfMonth) {
    return new Date(LocalDate.of(year, month, dayOfMonth));
  }

  public int getYear() {
    return localDate.getYear();
  }

  public int getMonth() {
    return localDate.getMonthValue();
  }

  public int getDay() {
    return localDate.getDayOfMonth();
  }

  public int lengthOfMonth() {
    return localDate.lengthOfMonth();
  }

  public int getDayOfWeek() {
    return localDate.getDayOfWeek().plus(1).getValue();
  }

  /**
   * Return a copy of this object.
   */
  public Object clone() {
    // LocalDateTime is immutable.
    return new Date(this.localDate);
  }

}
