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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Objects;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Timestamp parser using JDK DateTimeFormatter. Parser accepts 0 or more date
 * time format patterns. If no format patterns are provided it will default to
 * the normal Timestamp parsing. Datetime formats are compatible with Java
 * SimpleDateFormat.
 *
 * In addition to accepting format patterns, this parser provides support for
 * three pre-defined formats:
 *
 * <table border="1" summary="">
 * <thead>
 * <tr>
 * <th>Formatter</th>
 * <th>Description</th>
 * <th>Example</th>
 * <th>Example Output</th>
 * </tr>
 * </thead>
 * <tr>
 * <td><b>millis</b></td>
 * <td>Milliseconds since EPOCH</td>
 * <td>1420509274123</td>
 * <td>2015-01-06T01:54:34Z</td>
 * </tr>
 * <tr>
 * <td><b>iso8601</b></td>
 * <td>Parses ISO-8601 timestamp format</td>
 * <td>'2011-12-03T10:15:30Z'</td>
 * <td>2011-12-03T10:15:30Z</td>
 * </tr>
 * <tr>
 * <td><b>rfc1123</b></td>
 * <td>Parses RFC 1123 timestamp format</td>
 * <td>'Tue, 3 Jun 2008 11:05:30 GMT'</td>
 * <td>2008-06-03T11:05:30Z</td>
 * </tr>
 * </table>
 *
 * @see java.text.SimpleDateFormat
 * @see DateTimeFormatter
 */
public class TimestampParser {

  private static final Logger LOG =
      LoggerFactory.getLogger(TimestampParser.class);

  public static final String MILLIS_FORMAT_STR = "millis";
  public static final String ISO_8601_FORMAT_STR = "iso8601";
  public static final String RFC_1123_FORMAT_STR = "rfc1123";

  private final Collection<DateTimeFormatter> dtFormatters;
  private final boolean supportMillisEpoch;

  /**
   * Create a default Timestamp parser with no formats defined.
   *
   * @see Timestamp#valueOf(String)
   */
  public TimestampParser() {
    this(Collections.emptyList());
  }

  /**
   * Create a Timestamp parser based on an existing one.
   *
   * @param tsParser The source TimestampParser
   */
  public TimestampParser(final TimestampParser tsParser) {
    this.dtFormatters = tsParser.dtFormatters;
    this.supportMillisEpoch = tsParser.supportMillisEpoch;
  }

  /**
   * Create a Timestamp parser which parses zero or more time stamp formats.
   *
   * @param formatStrings The format strings
   */
  public TimestampParser(final String[] formatStrings) {
    this(formatStrings == null ? Collections.emptyList()
        : Arrays.asList(formatStrings));
  }

  /**
   * Create a timestamp parser with one ore more date patterns. When parsing,
   * the first pattern in the list is selected for parsing first, so if one
   * format is more common than others, include it first in the list. If it
   * fails, the next is chosen, and so on. If none of these patterns succeeds, a
   * default formatting is expected.
   *
   * @see DateTimeFormatter
   * @see Timestamp#valueOf(String)
   * @param patterns a collection of timestamp formats
   */
  public TimestampParser(final Collection<String> patterns) {
    final Collection<String> patternSet = new HashSet<>(patterns);
    this.supportMillisEpoch = patternSet.remove(MILLIS_FORMAT_STR);

    if (patternSet.isEmpty()) {
      this.dtFormatters = Collections.emptyList();
      return;
    }

    this.dtFormatters = new ArrayList<>();

    for (final String patternText : patternSet) {
      final DateTimeFormatter formatter;
      switch (patternText) {
      case ISO_8601_FORMAT_STR:
        formatter = DateTimeFormatter.ISO_INSTANT;
        break;
      case RFC_1123_FORMAT_STR:
        formatter = DateTimeFormatter.RFC_1123_DATE_TIME;
        break;
      default:
        formatter = DateTimeFormatter.ofPattern(patternText);
        break;
      }

      this.dtFormatters.add(formatter);
    }
  }

  /**
   * Parse the input string and return a timestamp value.
   *
   * @param text The timestamp text
   * @return A timestamp based on the text provided
   * @throws IllegalArgumentException if input text cannot be parsed into
   *           timestamp
   */
  public Timestamp parseTimestamp(final String text) {
    if (supportMillisEpoch) {
      try {
        // support for milliseconds that include nanoseconds as well
        // example: "1420509274123.456789"
        final long millis = new BigDecimal(text).setScale(0, RoundingMode.DOWN)
            .longValueExact();
        return Timestamp.ofEpochMilli(millis);
      } catch (NumberFormatException e) {
        LOG.debug("Could not format millis: {}", text);
      }
    }
    for (DateTimeFormatter formatter : this.dtFormatters) {
      try {
        final TemporalAccessor parsed = formatter.parse(text);
        final Instant inst = Instant.from(wrap(parsed));
        return Timestamp.ofEpochMilli(inst.toEpochMilli());
      } catch (DateTimeParseException dtpe) {
        LOG.debug("Could not parse timestamp text: {}", text);
      }
    }

    try {
      return Timestamp.valueOf(text);
    } catch (IllegalArgumentException e) {
      return null;
    }

  }

  public TimestampTZ parseTimestamp(String text, ZoneId defaultTimeZone) {
    Objects.requireNonNull(text);
    for (DateTimeFormatter f : dtFormatters) {
      try {
        return TimestampTZUtil.parse(text, defaultTimeZone, f);
      } catch (DateTimeException e) {
        // Ignore and try next formatter
      }
    }
    return TimestampTZUtil.parse(text, defaultTimeZone);
  }


  /**
   * The goal of this class is to return a timestamp. A timestamp represents a
   * single moment (instant) on the time line. However, some strings will not
   * contain enough information to assign it to one instant in time. For
   * example, if no time zone information is supplied, or a date is supplied,
   * but no time. In those cases, they need to be populated manually. This
   * method accepts all the data parsed from the supplied String and assigns it
   * reasonable defaults if fields are missing.
   *
   * @param in The fields populated by parsing the supplied string
   * @return The fields populated with default values if required
   */
  private TemporalAccessor wrap(final TemporalAccessor in) {
    if (in.isSupported(ChronoField.INSTANT_SECONDS)
        && in.isSupported(ChronoField.NANO_OF_SECOND)) {
      return in;
    }
    return new DefaultingTemporalAccessor(in);
  }

  /**
   * Class to wrap a TemporalAccessor and add fields with reasonable defaults.
   */
  private static class DefaultingTemporalAccessor implements TemporalAccessor {
    private static final EnumSet<ChronoField> FIELDS =
        EnumSet.of(ChronoField.YEAR, ChronoField.MONTH_OF_YEAR,
            ChronoField.DAY_OF_MONTH, ChronoField.HOUR_OF_DAY,
            ChronoField.MINUTE_OF_HOUR, ChronoField.SECOND_OF_MINUTE,
            ChronoField.MILLI_OF_SECOND, ChronoField.NANO_OF_SECOND);

    private final TemporalAccessor wrapped;

    DefaultingTemporalAccessor(TemporalAccessor in) {
      ZonedDateTime dateTime =
          ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);
      for (ChronoField field : FIELDS) {
        if (in.isSupported(field)) {
          dateTime = dateTime.with(field, in.getLong(field));
        }
      }
      this.wrapped = dateTime.toInstant();
    }

    @Override
    public long getLong(TemporalField field) {
      return wrapped.getLong(field);
    }

    @Override
    public boolean isSupported(TemporalField field) {
      return wrapped.isSupported(field);
    }
  }
}
