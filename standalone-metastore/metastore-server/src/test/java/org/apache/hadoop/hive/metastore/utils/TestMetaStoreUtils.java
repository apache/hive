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
package org.apache.hadoop.hive.metastore.utils;

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Category(MetastoreUnitTest.class)
public class TestMetaStoreUtils {
  private static final TimeZone DEFAULT = TimeZone.getDefault();
  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");
  private final TimeZone timezone;
  private final String date;
  private final String timestampString;
  private final Timestamp timestamp;

  public TestMetaStoreUtils(String zoneId, LocalDateTime timestamp) {
    this.timezone = TimeZone.getTimeZone(zoneId);
    this.timestampString = timestamp.format(FORMATTER);
    this.date = timestamp.toLocalDate().format(DateTimeFormatter.ISO_LOCAL_DATE);

    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    this.timestamp = Timestamp.valueOf(timestamp);
    TimeZone.setDefault(DEFAULT);
  }

  @Parameterized.Parameters(name = "zoneId={0}, timestamp={1}")
  public static Collection<Object[]> generateZoneTimestampPairs() {
    List<Object[]> params = new ArrayList<>();
    long minDate = LocalDate.of(1, 1, 1).atStartOfDay().toEpochSecond(ZoneOffset.UTC);
    long maxDate = LocalDate.of(9999, 12, 31).atStartOfDay().toEpochSecond(ZoneOffset.UTC);
    new Random(23).longs(500, minDate, maxDate).forEach(i -> {
      LocalDateTime datetime = LocalDateTime.ofEpochSecond(i, 0, ZoneOffset.UTC);
      for (String zone : ZoneId.SHORT_IDS.values()) {
        params.add(new Object[] { zone, datetime });
      }
    });
    generateDaylightSavingTimestampPairs(params);
    return params;
  }

  public static void generateDaylightSavingTimestampPairs(List<Object[]> params) {
    // For U.S timezones, the timestamp 2024-03-10 02:01:00 falls between the transition from 02:00 AM to 03:00 AM for daylight savings
    params.add(new Object[] {"America/Anchorage", LocalDateTime.ofEpochSecond(1710036060L, 0, ZoneOffset.UTC)});
    params.add(new Object[] {"America/St_Johns", LocalDateTime.ofEpochSecond(1710036060L, 0, ZoneOffset.UTC)});
    params.add(new Object[] {"America/Chicago", LocalDateTime.ofEpochSecond(1710036060L, 0, ZoneOffset.UTC)});
    params.add(new Object[] {"America/Indiana/Indianapolis", LocalDateTime.ofEpochSecond(1710036060L, 0, ZoneOffset.UTC)});
    params.add(new Object[] {"America/Los_Angeles", LocalDateTime.ofEpochSecond(1710036060L, 0, ZoneOffset.UTC)});

    // For Paris timezone, the timestamp 2024-03-31 02:02:02 falls between the transition from 02:00 AM to 03:00 AM for daylight savings
    params.add(new Object[] {"Europe/Paris", LocalDateTime.ofEpochSecond(1711850522L, 0, ZoneOffset.UTC)});

    // For New Zealand timezone, the timestamp 2024-09-29 02:03:04 falls between the transition from 02:00 AM to 03:00 AM for daylight savings
    params.add(new Object[] {"Pacific/Auckland", LocalDateTime.ofEpochSecond(1727575384L, 0, ZoneOffset.UTC)});

    // For Sydney timezone, the timestamp 2024-10-06 02:04:06 falls between the transition from 02:00 AM to 03:00 AM for daylight savings
    params.add(new Object[] {"Australia/Sydney", LocalDateTime.ofEpochSecond(1728180246L, 0, ZoneOffset.UTC)});
  }

  @Before
  public void setup() {
    TimeZone.setDefault(timezone);
  }

  @Test
  public void testDateToString() {
    assertEquals(date, MetaStoreUtils.convertDateToString(Date.valueOf(date)));
  }

  @Test
  public void testTimestampToString() {
    assertEquals(timestampString, MetaStoreUtils.convertTimestampToString(timestamp));
  }

  @Test
  public void testStringToDate() {
    assertEquals(Date.valueOf(date), MetaStoreUtils.convertStringToDate(date));
  }

  @Test
  public void testStringToTimestamp() {
    assertEquals(timestamp, MetaStoreUtils.convertStringToTimestamp(timestampString));
  }

  @AfterClass
  public static void tearDown() {
    TimeZone.setDefault(DEFAULT);
  }
}
