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

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.util.Objects;

final class InstantDateTimeFormatter extends InstantFormatterCache<DateTimeFormatter> {

  InstantDateTimeFormatter(final ZoneId zoneId, ResolverStyle resolverStyle) {
    super(zoneId,
        s -> new DateTimeFormatterBuilder().parseCaseInsensitive()
            .appendPattern(s).toFormatter().withResolverStyle(resolverStyle).withZone(zoneId));
  }

  @Override
  public Instant parse(String text) throws RuntimeException {
    Objects.requireNonNull(text);
    Timestamp timestamp = Timestamp.valueOf(text);
    TimestampTZ timestampTZ = TimestampTZUtil.convert(timestamp, zoneId);
    return Instant.ofEpochSecond(timestampTZ.getEpochSecond(), timestampTZ.getNanos());
  }

  @Override
  public Instant parse(String text, String pattern) {
    Objects.requireNonNull(text);
    Objects.requireNonNull(pattern);
    Timestamp timestamp;
    DateTimeFormatter formatter = getFormatter(pattern);
    try {
      ZonedDateTime zonedDateTime = ZonedDateTime.parse(text, formatter).withZoneSameInstant(zoneId);
      timestamp = new Timestamp(zonedDateTime.toLocalDateTime());
    } catch (DateTimeException e1) {
      LocalDate localDate = LocalDate.parse(text, formatter);
      timestamp = new Timestamp(localDate.atStartOfDay());
    }
    return TimestampTZUtil.convert(timestamp, zoneId).toInstant();
  }

  @Override
  public String format(final Instant instant) {
    return format(instant, "uuuu-MM-dd HH:mm:ss");
  }

  @Override
  public String format(final Instant instant, final String pattern) {
    DateTimeFormatter formatter = getFormatter(pattern);
    ZonedDateTime zonedDT = ZonedDateTime.ofInstant(instant, zoneId);
    return zonedDT.format(formatter);
  }
}
