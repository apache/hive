/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.udf.generic;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.Objects;
import java.util.TimeZone;

final class InstantSimpleDateFormatter extends InstantFormatterCache<SimpleDateFormat> {
  private static final String DEFAULT = "yyyy-MM-dd HH:mm:ss";

  InstantSimpleDateFormatter(final ZoneId zoneId) {
    super(zoneId, s -> {
      SimpleDateFormat f = new SimpleDateFormat(s);
      f.setTimeZone(TimeZone.getTimeZone(zoneId));
      return f;
    });
  }

  @Override
  public Instant parse(final String value) throws RuntimeException {
    return parse(value, DEFAULT);
  }

  @Override
  public Instant parse(String text, String pattern) {
    Objects.requireNonNull(text);
    Objects.requireNonNull(pattern);
    final SimpleDateFormat formatter = getFormatter(pattern);
    ParsePosition pos = new ParsePosition(0);
    Date d = formatter.parse(text, pos);
    if (d == null) {
      throw new DateTimeParseException(text + " cannot be parsed to date. Error at index " + pos.getErrorIndex(), text,
          pos.getErrorIndex());
    }
    return Instant.ofEpochMilli(d.getTime());
  }

  @Override
  public String format(final Instant instant) {
    return format(instant, DEFAULT);
  }

  @Override
  public String format(final Instant instant, final String pattern) {
    SimpleDateFormat formatter = getFormatter(pattern);
    Date date = new Date(instant.toEpochMilli());
    return formatter.format(date);
  }
}
