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
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category(MetastoreUnitTest.class)
public class TestMetaStoreUtils {

  private TimeZone defaultTZ;

  @Before
  public void setup() {
    defaultTZ = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Hong_Kong")));
  }

  @Test
  public void testConvertDateToString() {
    String date = MetaStoreUtils.convertDateToString(Date.valueOf("2023-01-01"));
    assertEquals("2023-01-01", date);
  }

  @Test
  public void testcConvertTimestampToString() {
    String timestamp = MetaStoreUtils.convertTimestampToString(Timestamp.valueOf("2023-01-01 10:20:30"));
    assertEquals("2023-01-01 10:20:30", timestamp);
  }

  @Test
  public void testConvertStringToDate() {
    Date date = MetaStoreUtils.convertStringToDate("2023-01-01");
    assertEquals(Date.valueOf("2023-01-01"), date);
  }

  @Test
  public void testConvertStringToTimestamp() {
    Timestamp timestamp = MetaStoreUtils.convertStringToTimestamp("2023-01-01 10:20:30");
    assertEquals(Timestamp.valueOf("2023-01-01 10:20:30"), timestamp);
  }

  @Test
  public void testConvertStringToDateToString() {
    String inputDate = "2023-01-01";
    Date date = MetaStoreUtils.convertStringToDate(inputDate);
    String convertedDate = MetaStoreUtils.convertDateToString(date);
    assertEquals(inputDate, convertedDate);
  }

  @Test
  public void testConvertStringToTimestampToString() {
    String inputTimestamp = "2023-01-01 10:20:30";
    Timestamp timestamp = MetaStoreUtils.convertStringToTimestamp(inputTimestamp);
    String outputTimestamp = MetaStoreUtils.convertTimestampToString(timestamp);
    assertEquals(inputTimestamp, outputTimestamp);
  }

  @Test
  public void testConvertStringToDateForUSTimezone() {
    TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("America/Los_Angeles")));
    Date date = MetaStoreUtils.convertStringToDate("2023-10-20");
    assertEquals(Date.valueOf("2023-10-20"), date);
  }

  @Test
  public void testConvertStringToTimestampForUSTimezone() {
    TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("America/Los_Angeles")));
    Timestamp timestamp = MetaStoreUtils.convertStringToTimestamp("2023-10-20 10:20:30");
    assertEquals(Timestamp.valueOf("2023-10-20 10:20:30"), timestamp);
  }

  @Test
  public void testShouldThrowExceptionWhileConvertStringToDateIfInputIsInWrongFormat() {
    String inputDate = "01-01-2023";
    try {
      MetaStoreUtils.convertStringToDate(inputDate);
      fail("Should throw exception for the invalid input!!!");
    } catch (DateTimeParseException dpe) {
      assertEquals("Text '01-01-2023' could not be parsed at index 0", dpe.getMessage());
    }
  }

  @Test
  public void testShouldThrowExceptionWhileConvertStringToTimestampIfInputIsInWrongFormat() {
    String inputTimestamp = "2023-01-01 10:20:30.0";
    try {
      MetaStoreUtils.convertStringToTimestamp(inputTimestamp);
      fail("Should throw exception for the wrong input!!!");
    } catch (DateTimeParseException dpe) {
      assertEquals("Text '2023-01-01 10:20:30.0' could not be parsed, unparsed text found at index 19",
          dpe.getMessage());
    }
  }

  @Before
  public void tearDown() {
    TimeZone.setDefault(defaultTZ);
  }
}
