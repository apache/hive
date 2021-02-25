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

package org.apache.hive.common.util;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class HiveDateTimeFormatter {

  public static final DateTimeFormatter HIVE_LOCAL_DATE =
      new DateTimeFormatterBuilder()
          .appendValue(YEAR, 1, 10, SignStyle.NORMAL)
          .appendLiteral('-')
          .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NOT_NEGATIVE)
          .appendLiteral('-')
          .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NOT_NEGATIVE)
          .toFormatter()
          .withChronology(IsoChronology.INSTANCE)
          .withResolverStyle(ResolverStyle.LENIENT);

  // Minute/Second are optional
  public static final DateTimeFormatter HIVE_LOCAL_TIME =
      new DateTimeFormatterBuilder()
          .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
          .optionalStart()
          .appendLiteral(':')
          .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
          .optionalStart()
          .appendLiteral(':')
          .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
          .optionalStart()
          .appendFraction(NANO_OF_SECOND, 0, 9, true)
          .toFormatter()
          .withResolverStyle(ResolverStyle.STRICT);

  // T or ' '
  public static final DateTimeFormatter HIVE_LOCAL_DATE_TIME = 
      new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .append(HIVE_LOCAL_DATE)
          .optionalStart()
          .optionalStart()
          .appendLiteral(' ')
          .optionalEnd()
          .optionalStart()
          .appendLiteral('T')
          .optionalEnd()
          .optionalStart()
          .appendLiteral(' ')
          .optionalEnd()
          .appendOptional(HIVE_LOCAL_TIME)
          .toFormatter()
          .withResolverStyle(ResolverStyle.LENIENT)
          .withChronology(IsoChronology.INSTANCE);


  public static final DateTimeFormatter HIVE_DATE_TIME =
      new DateTimeFormatterBuilder()
          .append(HIVE_LOCAL_DATE_TIME)
          .optionalStart()
          .optionalStart()
          .appendOffsetId()
          .optionalEnd()
          .optionalStart()
          .optionalStart()
          .appendLiteral(' ')
          .optionalEnd()
          .optionalStart()
          .appendLiteral('[')
          .optionalEnd()
          .parseCaseInsensitive()
          .appendZoneRegionId()
          .optionalStart()
          .appendLiteral(']')
          .optionalEnd()
          .optionalEnd()
          .optionalEnd()
          .toFormatter()
          .withResolverStyle(ResolverStyle.LENIENT)
          .withChronology(IsoChronology.INSTANCE);

  public static final DateTimeFormatter HIVE_DATE_DEFAULT_TIME =
      new DateTimeFormatterBuilder()
          .append(HIVE_DATE_TIME)
          .parseDefaulting(HOUR_OF_DAY, 0)
          .parseDefaulting(MINUTE_OF_HOUR, 0)
          .parseDefaulting(SECOND_OF_MINUTE, 0)
          .toFormatter()
          .withResolverStyle(ResolverStyle.LENIENT)
          .withChronology(IsoChronology.INSTANCE);

  private static final Pattern SINGLE_DIGIT_PATTERN = Pattern.compile("[\\+-]\\d:\\d\\d");

  /**
   * Need to handle offset with single digital hour, see JDK-8066806.
   * @param text The text to check
   * @return The updated text
   */
  public static String handleSingleDigitHourOffset(final String text) {
    Matcher matcher = SINGLE_DIGIT_PATTERN.matcher(text);
    if (matcher.find()) {
      int index = matcher.start() + 1;
      return text.substring(0, index) + "0" + text.substring(index, text.length());
    }
    return text;
  }
}
