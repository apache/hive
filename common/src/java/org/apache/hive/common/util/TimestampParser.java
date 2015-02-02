/**
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

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.joda.time.MutableDateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimeParserBucket;

/**
 * Timestamp parser using Joda DateTimeFormatter. Parser accepts 0 or more date time format
 * patterns. If no format patterns are provided it will default to the normal Timestamp parsing.
 * Datetime formats are compatible with Java SimpleDateFormat. Also added special case pattern
 * "millis" to parse the string as milliseconds since Unix epoch.
 * Since this uses Joda DateTimeFormatter, this parser should be thread safe.
 */
public class TimestampParser {

  protected final static String[] stringArray = new String[] {};
  protected final static String millisFormatString = "millis";
  protected final static DateTime startingDateValue = new DateTime(1970, 1, 1, 0, 0, 0, 0);

  protected String[] formatStrings = null;
  protected DateTimeFormatter fmt = null;

  public TimestampParser() {
  }

  public TimestampParser(TimestampParser tsParser) {
    this(tsParser.formatStrings == null ?
        null : Arrays.copyOf(tsParser.formatStrings, tsParser.formatStrings.length));
  }

  public TimestampParser(List<String> formatStrings) {
    this(formatStrings == null ? null : formatStrings.toArray(stringArray));
  }

  public TimestampParser(String[] formatStrings) {
    this.formatStrings = formatStrings;

    // create formatter that includes all of the input patterns
    if (formatStrings != null && formatStrings.length > 0) {
      DateTimeParser[] parsers = new DateTimeParser[formatStrings.length];
      for (int idx = 0; idx < formatStrings.length; ++idx) {
        String formatString = formatStrings[idx];
        if (formatString.equalsIgnoreCase(millisFormatString)) {
          // Use milliseconds parser if pattern matches our special-case millis pattern string
          parsers[idx] = new MillisDateFormatParser();
        } else {
          parsers[idx] = DateTimeFormat.forPattern(formatString).getParser();
        }
      }
      fmt = new DateTimeFormatterBuilder().append(null, parsers).toFormatter();
    }
  }

  /**
   * Parse the input string and return a timestamp value
   * @param strValue
   * @return
   * @throws IllegalArgumentException if input string cannot be parsed into timestamp
   */
  public Timestamp parseTimestamp(String strValue) throws IllegalArgumentException {
    if (fmt != null) {
      // reset value in case any date fields are missing from the date pattern
      MutableDateTime mdt = new MutableDateTime(startingDateValue);

      // Using parseInto() avoids throwing exception when parsing,
      // allowing fallback to default timestamp parsing if custom patterns fail.
      int ret = fmt.parseInto(mdt, strValue, 0);
      // Only accept parse results if we parsed the entire string
      if (ret == strValue.length()) {
        return new Timestamp(mdt.getMillis());
      }
    }

    // Otherwise try default timestamp parsing
    return Timestamp.valueOf(strValue);
  }

  /**
   * DateTimeParser to parse the date string as the millis since Unix epoch
   */
  public static class MillisDateFormatParser implements DateTimeParser {
    private static final ThreadLocal<Matcher> numericMatcher = new ThreadLocal<Matcher>() {
      @Override
      protected Matcher initialValue() {
        return Pattern.compile("(-?\\d+)(\\.\\d+)?$").matcher("");
      }
    };

    private final static DateTimeFieldType[] dateTimeFields = {
      DateTimeFieldType.year(),
      DateTimeFieldType.monthOfYear(),
      DateTimeFieldType.dayOfMonth(),
      DateTimeFieldType.hourOfDay(),
      DateTimeFieldType.minuteOfHour(),
      DateTimeFieldType.secondOfMinute(),
      DateTimeFieldType.millisOfSecond()
    };

    public int estimateParsedLength() {
      return 13; // Shouldn't hit 14 digits until year 2286
    }

    public int parseInto(DateTimeParserBucket bucket, String text, int position) {
      String substr = text.substring(position);
      Matcher matcher = numericMatcher.get();
      matcher.reset(substr);
      if (!matcher.matches()) {
        return -1;
      }

      // Joda DateTime only has precision to millis, cut off any fractional portion
      long millis = Long.parseLong(matcher.group(1));
      DateTime dt = new DateTime(millis);
      for (DateTimeFieldType field : dateTimeFields) {
        bucket.saveField(field, dt.get(field));
      }
      return substr.length();
    }
  }
}
