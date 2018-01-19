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
package org.apache.hadoop.hive.ql.util;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.TimeZone;

import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.ql.util.DateTimeMath;
import org.junit.*;

import static org.junit.Assert.*;

public class TestDateTimeMath {

  @Test
  public void testTimestampIntervalYearMonthArithmetic() throws Exception {
    char plus = '+';
    char minus = '-';

    checkTimestampIntervalYearMonthArithmetic("2001-01-01 01:02:03", plus, "0-0",
        "2001-01-01 01:02:03");
    checkTimestampIntervalYearMonthArithmetic("2001-01-01 01:02:03.456", plus, "1-1",
        "2002-02-01 01:02:03.456");
    checkTimestampIntervalYearMonthArithmetic("2001-01-01 01:02:03.456", plus, "10-0",
        "2011-01-01 01:02:03.456");
    checkTimestampIntervalYearMonthArithmetic("2001-01-01 01:02:03.456", plus, "0-11",
        "2001-12-01 01:02:03.456");
    checkTimestampIntervalYearMonthArithmetic("2001-03-01 01:02:03.500", plus, "1-11",
        "2003-02-01 01:02:03.500");
    checkTimestampIntervalYearMonthArithmetic("2001-01-01 01:02:03.500", plus, "-1-1",
        "1999-12-01 01:02:03.500");
    checkTimestampIntervalYearMonthArithmetic("2001-01-01 01:02:03.500", plus, "-0-0",
        "2001-01-01 01:02:03.500");
    checkTimestampIntervalYearMonthArithmetic("2001-01-01 01:02:03.123456789", plus, "-0-0",
        "2001-01-01 01:02:03.123456789");

    checkTimestampIntervalYearMonthArithmetic("2001-01-01 01:02:03", minus, "0-0",
        "2001-01-01 01:02:03");
    checkTimestampIntervalYearMonthArithmetic("2001-01-01 01:02:03", minus, "10-0",
        "1991-01-01 01:02:03");
    checkTimestampIntervalYearMonthArithmetic("2001-01-01 01:02:03", minus, "-10-0",
        "2011-01-01 01:02:03");
    checkTimestampIntervalYearMonthArithmetic("2001-01-01 01:02:03", minus, "8-2",
        "1992-11-01 01:02:03");
    checkTimestampIntervalYearMonthArithmetic("2001-01-01 01:02:03", minus, "-8-2",
        "2009-03-01 01:02:03");
    checkTimestampIntervalYearMonthArithmetic("2001-01-01 01:02:03.123456789", minus, "8-2",
        "1992-11-01 01:02:03.123456789");

    checkTimestampIntervalYearMonthArithmetic(null, plus, "1-1",
        null);
    checkTimestampIntervalYearMonthArithmetic("2001-01-01 01:02:03", plus, null,
        null);
    checkTimestampIntervalYearMonthArithmetic(null, minus, "1-1",
        null);
    checkTimestampIntervalYearMonthArithmetic("2001-01-01 01:02:03", minus, null,
        null);

    // End of the month behavior
    checkTimestampIntervalYearMonthArithmetic("2001-01-28 01:02:03", plus, "0-1",
        "2001-02-28 01:02:03");
    checkTimestampIntervalYearMonthArithmetic("2001-01-29 01:02:03", plus, "0-1",
        "2001-02-28 01:02:03");
    checkTimestampIntervalYearMonthArithmetic("2001-01-30 01:02:03", plus, "0-1",
        "2001-02-28 01:02:03");
    checkTimestampIntervalYearMonthArithmetic("2001-01-31 01:02:03", plus, "0-1",
        "2001-02-28 01:02:03");
    checkTimestampIntervalYearMonthArithmetic("2001-02-28 01:02:03", plus, "0-1",
        "2001-03-28 01:02:03");

    // Test that timestamp arithmetic is done in UTC and then converted back to local timezone,
    // matching Oracle behavior.
    TimeZone originalTz = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
      checkTimestampIntervalYearMonthArithmetic("2001-01-01 01:02:03", plus, "0-6",
          "2001-07-01 02:02:03");
        checkTimestampIntervalYearMonthArithmetic("2001-07-01 01:02:03", plus, "0-6",
          "2002-01-01 00:02:03");

      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      checkTimestampIntervalYearMonthArithmetic("2001-01-01 01:02:03", plus, "0-6",
          "2001-07-01 01:02:03");
        checkTimestampIntervalYearMonthArithmetic("2001-07-01 01:02:03", plus, "0-6",
          "2002-01-01 01:02:03");
    } finally {
      TimeZone.setDefault(originalTz);
    }
  }

  @Test
  public void testDateIntervalYearMonthArithmetic() throws Exception {
    char plus = '+';
    char minus = '-';

    checkDateIntervalDayTimeArithmetic("2001-01-01", plus, "0-0", "2001-01-01");
    checkDateIntervalDayTimeArithmetic("2001-01-01", plus, "0-1", "2001-02-01");
    checkDateIntervalDayTimeArithmetic("2001-01-01", plus, "0-6", "2001-07-01");
    checkDateIntervalDayTimeArithmetic("2001-01-01", plus, "1-0", "2002-01-01");
    checkDateIntervalDayTimeArithmetic("2001-01-01", plus, "1-1", "2002-02-01");
    checkDateIntervalDayTimeArithmetic("2001-10-10", plus, "1-6", "2003-04-10");
    checkDateIntervalDayTimeArithmetic("2003-04-10", plus, "-1-6", "2001-10-10");

    checkDateIntervalDayTimeArithmetic("2001-01-01", minus, "0-0", "2001-01-01");
    checkDateIntervalDayTimeArithmetic("2001-01-01", minus, "0-1", "2000-12-01");
    checkDateIntervalDayTimeArithmetic("2001-01-01", minus, "1-0", "2000-01-01");
    checkDateIntervalDayTimeArithmetic("2001-01-01", minus, "1-1", "1999-12-01");
    checkDateIntervalDayTimeArithmetic("2001-10-10", minus, "1-6", "2000-04-10");
    checkDateIntervalDayTimeArithmetic("2003-04-10", minus, "-1-6", "2004-10-10");

    // end of month behavior
    checkDateIntervalDayTimeArithmetic("2001-01-28", plus, "0-1", "2001-02-28");
    checkDateIntervalDayTimeArithmetic("2001-01-29", plus, "0-1", "2001-02-28");
    checkDateIntervalDayTimeArithmetic("2001-01-30", plus, "0-1", "2001-02-28");
    checkDateIntervalDayTimeArithmetic("2001-01-31", plus, "0-1", "2001-02-28");
    checkDateIntervalDayTimeArithmetic("2001-01-31", plus, "0-2", "2001-03-31");
    checkDateIntervalDayTimeArithmetic("2001-02-28", plus, "0-1", "2001-03-28");
    // leap year
    checkDateIntervalDayTimeArithmetic("2004-01-28", plus, "0-1", "2004-02-28");
    checkDateIntervalDayTimeArithmetic("2004-01-29", plus, "0-1", "2004-02-29");
    checkDateIntervalDayTimeArithmetic("2004-01-30", plus, "0-1", "2004-02-29");
    checkDateIntervalDayTimeArithmetic("2004-01-31", plus, "0-1", "2004-02-29");
  }

  @Test
  public void testIntervalYearMonthArithmetic() throws Exception {
    char plus = '+';
    char minus = '-';

    checkIntervalYearMonthArithmetic("0-0", plus, "0-0", "0-0");
    checkIntervalYearMonthArithmetic("0-0", plus, "4-5", "4-5");
    checkIntervalYearMonthArithmetic("4-5", plus, "0-0", "4-5");
    checkIntervalYearMonthArithmetic("0-0", plus, "1-1", "1-1");
    checkIntervalYearMonthArithmetic("1-1", plus, "0-0", "1-1");

    checkIntervalYearMonthArithmetic("0-0", minus, "0-0", "0-0");
    checkIntervalYearMonthArithmetic("0-0", minus, "1-0", "-1-0");
    checkIntervalYearMonthArithmetic("1-2", minus, "1-1", "0-1");
    checkIntervalYearMonthArithmetic("0-0", minus, "1-1", "-1-1");
    checkIntervalYearMonthArithmetic("-1-1", minus, "1-1", "-2-2");
    checkIntervalYearMonthArithmetic("-1-1", minus, "-1-1", "0-0");

    checkIntervalYearMonthArithmetic(null, plus, "1-1", null);
    checkIntervalYearMonthArithmetic("1-1", plus, null, null);
    checkIntervalYearMonthArithmetic(null, minus, "1-1", null);
    checkIntervalYearMonthArithmetic("1-1", minus, null, null);
  }

  @Test
  public void testTimestampIntervalDayTimeArithmetic() throws Exception {
    char plus = '+';
    char minus = '-';

    checkTsIntervalDayTimeArithmetic("2001-01-01 01:02:03", plus, "1 1:1:1",
        "2001-01-02 02:03:04");
    checkTsIntervalDayTimeArithmetic("2001-01-01 01:02:03.456", plus, "1 1:1:1",
        "2001-01-02 02:03:04.456");
    checkTsIntervalDayTimeArithmetic("2001-01-01 01:02:03.456", plus, "1 1:1:1.555",
        "2001-01-02 02:03:05.011");
    checkTsIntervalDayTimeArithmetic("2001-01-01 01:02:03", plus, "1 1:1:1.555555555",
        "2001-01-02 02:03:04.555555555");
    checkTsIntervalDayTimeArithmetic("2001-01-01 01:02:03.456", plus, "1 1:1:1.555555555",
        "2001-01-02 02:03:05.011555555");
    checkTsIntervalDayTimeArithmetic("2001-01-01 01:02:03.500", plus, "1 1:1:1.499",
        "2001-01-02 02:03:04.999");
    checkTsIntervalDayTimeArithmetic("2001-01-01 01:02:03.500", plus, "1 1:1:1.500",
        "2001-01-02 02:03:05.0");
    checkTsIntervalDayTimeArithmetic("2001-01-01 01:02:03.500", plus, "1 1:1:1.501",
        "2001-01-02 02:03:05.001");
    checkTsIntervalDayTimeArithmetic("2001-01-01 01:02:03.500000000", plus, "1 1:1:1.4999999999",
        "2001-01-02 02:03:04.999999999");
    checkTsIntervalDayTimeArithmetic("2001-01-01 01:02:03.500000000", plus, "1 1:1:1.500",
        "2001-01-02 02:03:05.0");
    checkTsIntervalDayTimeArithmetic("2001-01-01 01:02:03.500000000", plus, "1 1:1:1.500000001",
        "2001-01-02 02:03:05.000000001");

    checkTsIntervalDayTimeArithmetic("2001-01-01 01:02:03", minus, "0 01:02:03",
        "2001-01-01 00:00:00");
    checkTsIntervalDayTimeArithmetic("2001-01-01 01:02:03", minus, "0 0:0:0",
        "2001-01-01 01:02:03");

    checkTsIntervalDayTimeArithmetic(null, plus, "1 1:1:1.555555555",
        null);
    checkTsIntervalDayTimeArithmetic("2001-01-01 01:02:03", plus, null,
        null);
    checkTsIntervalDayTimeArithmetic(null, minus, "1 1:1:1.555555555",
        null);
    checkTsIntervalDayTimeArithmetic("2001-01-01 01:02:03", minus, null,
        null);

    // Try some time zone boundaries
    TimeZone originalTz = TimeZone.getDefault();
    try {
      // America/Los_Angeles DST dates - 2015-03-08 02:00:00/2015-11-01 02:00:00
      TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));

      checkTsIntervalDayTimeArithmetic("2015-03-08 01:59:58", plus, "0 0:0:01",
          "2015-03-08 01:59:59");
      checkTsIntervalDayTimeArithmetic("2015-03-08 01:59:59", plus, "0 0:0:01",
          "2015-03-08 03:00:00");
      checkTsIntervalDayTimeArithmetic("2015-03-08 03:00:00", minus, "0 0:0:01",
          "2015-03-08 01:59:59");
      checkTsIntervalDayTimeArithmetic("2015-03-08 01:59:59.995", plus, "0 0:0:0.005",
          "2015-03-08 03:00:00");
      checkTsIntervalDayTimeArithmetic("2015-03-08 01:59:59.995", plus, "0 0:0:0.0051",
          "2015-03-08 03:00:00.0001");
      checkTsIntervalDayTimeArithmetic("2015-03-08 03:00:00", minus, "0 0:0:0.005",
          "2015-03-08 01:59:59.995");
      checkTsIntervalDayTimeArithmetic("2015-11-01 01:59:58", plus, "0 0:0:01",
          "2015-11-01 01:59:59");
      checkTsIntervalDayTimeArithmetic("2015-11-01 01:59:59", plus, "0 0:0:01",
          "2015-11-01 02:00:00");

      // UTC has no such adjustment
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      checkTsIntervalDayTimeArithmetic("2015-03-08 01:59:58", plus, "0 0:0:01",
          "2015-03-08 01:59:59");
      checkTsIntervalDayTimeArithmetic("2015-03-08 01:59:59", plus, "0 0:0:01",
          "2015-03-08 02:00:00");
    } finally {
      TimeZone.setDefault(originalTz);
    }
  }

  @Test
  public void testIntervalDayTimeArithmetic() throws Exception {
    char plus = '+';
    char minus = '-';

    checkIntervalDayTimeArithmetic("0 0:0:0", plus, "0 0:0:0", "0 0:0:0");
    checkIntervalDayTimeArithmetic("0 01:02:03", plus, "6 0:0:0.0001", "6 01:02:03.0001");
    checkIntervalDayTimeArithmetic("6 0:0:0.0001", plus, "0 01:02:03", "6 01:02:03.0001");
    checkIntervalDayTimeArithmetic("0 01:02:03", plus, "1 10:10:10.0001", "1 11:12:13.0001");
    checkIntervalDayTimeArithmetic("1 10:10:10.0001", plus, "0 01:02:03", "1 11:12:13.0001");
    checkIntervalDayTimeArithmetic("0 0:0:0.900000000", plus, "0 0:0:0.099999999", "0 0:0:0.999999999");
    checkIntervalDayTimeArithmetic("0 0:0:0.900000001", plus, "0 0:0:0.099999999", "0 0:0:1");
    checkIntervalDayTimeArithmetic("0 0:0:0.900000002", plus, "0 0:0:0.099999999", "0 0:0:1.000000001");

    checkIntervalDayTimeArithmetic("0 0:0:0", minus, "0 0:0:0", "0 0:0:0");
    checkIntervalDayTimeArithmetic("0 0:0:0", minus, "0 0:0:0.123", "-0 0:0:0.123");
    checkIntervalDayTimeArithmetic("3 4:5:6.789", minus, "1 1:1:1.111", "2 3:4:5.678");
    checkIntervalDayTimeArithmetic("0 0:0:0.0", minus, "1 1:1:1.111", "-1 1:1:1.111");
    checkIntervalDayTimeArithmetic("-1 1:1:1.222", minus, "1 1:1:1.111", "-2 2:2:2.333");
    checkIntervalDayTimeArithmetic("-1 1:1:1.111", minus, "-1 1:1:1.111", "0 0:0:0");

    checkIntervalDayTimeArithmetic(null, plus, "1 1:1:1.111", null);
    checkIntervalDayTimeArithmetic("1 1:1:1.111", plus, null, null);
    checkIntervalDayTimeArithmetic(null, minus, "1 1:1:1.111", null);
    checkIntervalDayTimeArithmetic("1 1:1:1.111", minus, null, null);
  }

  @Test
  public void testTimestampSubtraction() throws Exception {
    checkTsArithmetic("2001-01-01 00:00:00", "2001-01-01 00:00:00", "0 0:0:0");
    checkTsArithmetic("2002-02-02 01:01:01", "2001-01-01 00:00:00", "397 1:1:1");
    checkTsArithmetic("2001-01-01 00:00:00", "2002-02-02 01:01:01", "-397 1:1:1");
    checkTsArithmetic("2015-01-01 00:00:00", "2014-12-31 00:00:00", "1 0:0:0");
    checkTsArithmetic("2014-12-31 00:00:00", "2015-01-01 00:00:00", "-1 0:0:0");
    checkTsArithmetic("2015-01-01 00:00:00", "2014-12-31 23:59:59", "0 0:0:01");
    checkTsArithmetic("2014-12-31 23:59:59", "2015-01-01 00:00:00", "-0 0:0:01");
    checkTsArithmetic("2015-01-01 00:00:00", "2014-12-31 23:59:59.9999", "0 0:0:00.0001");
    checkTsArithmetic("2014-12-31 23:59:59.9999", "2015-01-01 00:00:00", "-0 0:0:00.0001");
    checkTsArithmetic("2015-01-01 00:00:00", "2014-12-31 11:12:13.000000001", "0 12:47:46.999999999");
    checkTsArithmetic("2014-12-31 11:12:13.000000001", "2015-01-01 00:00:00", "-0 12:47:46.999999999");

    // Test that timestamp arithmetic is done in UTC and then converted back to local timezone,
    // matching Oracle behavior.
    TimeZone originalTz = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
      checkTsArithmetic("1999-12-15 00:00:00", "1999-09-15 00:00:00", "91 1:0:0");
      checkTsArithmetic("1999-09-15 00:00:00", "1999-12-15 00:00:00", "-91 1:0:0");
      checkTsArithmetic("1999-12-15 00:00:00", "1995-09-15 00:00:00", "1552 1:0:0");
      checkTsArithmetic("1995-09-15 00:00:00", "1999-12-15 00:00:00", "-1552 1:0:0");

      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      checkTsArithmetic("1999-12-15 00:00:00", "1999-09-15 00:00:00", "91 0:0:0");
      checkTsArithmetic("1999-09-15 00:00:00", "1999-12-15 00:00:00", "-91 0:0:0");
      checkTsArithmetic("1999-12-15 00:00:00", "1995-09-15 00:00:00", "1552 0:0:0");
      checkTsArithmetic("1995-09-15 00:00:00", "1999-12-15 00:00:00", "-1552 0:0:0");
    } finally {
      TimeZone.setDefault(originalTz);
    }
  }

  private static void checkTimestampIntervalYearMonthArithmetic(
      String left, char operationType, String right, String expected) throws Exception {
    Timestamp leftTs = null;
    if (left != null) {
      leftTs = Timestamp.valueOf(left);
    }
    HiveIntervalYearMonth rightInterval = null;
    if (right != null) {
      rightInterval = HiveIntervalYearMonth.valueOf(right);
    }
    Timestamp expectedResult = null;
    if (expected != null) {
      expectedResult = Timestamp.valueOf(expected);
    }
    Timestamp testResult = null;

    DateTimeMath dtm = new DateTimeMath();
    switch (operationType) {
      case '-':
        testResult = dtm.subtract(leftTs, rightInterval);
        break;
      case '+':
        testResult = dtm.add(leftTs, rightInterval);
        break;
      default:
        throw new IllegalArgumentException("Invalid operation " + operationType);
    }

    assertEquals(String.format("%s %s %s", leftTs, operationType, rightInterval),
        expectedResult, testResult);
  }

  private static void checkDateIntervalDayTimeArithmetic(
      String left, char operationType, String right, String expected) throws Exception {
    Date leftDt = null;
    if (left != null) {
      leftDt = Date.valueOf(left);
    }
    HiveIntervalYearMonth rightInterval = null;
    if (right != null) {
      rightInterval = HiveIntervalYearMonth.valueOf(right);
    }
    Date expectedResult = null;
    if (expected != null) {
      expectedResult = Date.valueOf(expected);
    }
    Date testResult = null;

    DateTimeMath dtm = new DateTimeMath();
    switch (operationType) {
      case '-':
        testResult = dtm.subtract(leftDt, rightInterval);
        break;
      case '+':
        testResult = dtm.add(leftDt, rightInterval);
        break;
      default:
        throw new IllegalArgumentException("Invalid operation " + operationType);
    }

    assertEquals(String.format("%s %s %s", leftDt, operationType, rightInterval),
        expectedResult, testResult);
  }

  private static void checkIntervalYearMonthArithmetic(
      String left, char operationType, String right, String expected) throws Exception {
    HiveIntervalYearMonth leftInterval = left == null ? null: HiveIntervalYearMonth.valueOf(left);
    HiveIntervalYearMonth rightInterval = right == null ? null : HiveIntervalYearMonth.valueOf(right);
    HiveIntervalYearMonth expectedResult = expected == null ? null : HiveIntervalYearMonth.valueOf(expected);
    HiveIntervalYearMonth testResult = null;

    DateTimeMath dtm = new DateTimeMath();
    switch (operationType) {
      case '-':
        testResult = dtm.subtract(leftInterval, rightInterval);
        break;
      case '+':
        testResult = dtm.add(leftInterval,  rightInterval);
        break;
      default:
        throw new IllegalArgumentException("Invalid operation " + operationType);
    }

    assertEquals(String.format("%s %s %s", leftInterval, operationType, rightInterval),
        expectedResult, testResult);
  }

  private static void checkTsIntervalDayTimeArithmetic(
      String left, char operationType, String right, String expected) throws Exception {
    Timestamp leftTs = null;
    if (left != null) {
      leftTs = Timestamp.valueOf(left);
    }
    HiveIntervalDayTime rightInterval = right == null ? null : HiveIntervalDayTime.valueOf(right);
    Timestamp expectedResult = null;
    if (expected != null) {
      expectedResult = Timestamp.valueOf(expected);
    }
    Timestamp testResult = null;

    DateTimeMath dtm = new DateTimeMath();
    switch (operationType) {
      case '-':
        testResult = dtm.subtract(leftTs, rightInterval);
        break;
      case '+':
        testResult = dtm.add(leftTs, rightInterval);
        break;
      default:
        throw new IllegalArgumentException("Invalid operation " + operationType);
    }

    assertEquals(String.format("%s %s %s", leftTs, operationType, rightInterval),
        expectedResult, testResult);
  }

  private static void checkIntervalDayTimeArithmetic(
      String left, char operationType, String right, String expected) throws Exception {
    HiveIntervalDayTime leftInterval = left == null ? null : HiveIntervalDayTime.valueOf(left);
    HiveIntervalDayTime rightInterval = right == null ? null : HiveIntervalDayTime.valueOf(right);
    HiveIntervalDayTime expectedResult = expected == null ? null : HiveIntervalDayTime.valueOf(expected);
    HiveIntervalDayTime testResult = null;

    DateTimeMath dtm = new DateTimeMath();
    switch (operationType) {
      case '-':
        testResult = dtm.subtract(leftInterval, rightInterval);
        break;
      case '+':
        testResult = dtm.add(leftInterval,  rightInterval);
        break;
      default:
        throw new IllegalArgumentException("Invalid operation " + operationType);
    }

    assertEquals(String.format("%s %s %s", leftInterval, operationType, rightInterval),
        expectedResult, testResult);
  }

  private static void checkTsArithmetic(
      String left, String right, String expected) throws Exception {
    Timestamp leftTs = null;
    if (left != null) {
      leftTs = Timestamp.valueOf(left);
    }
    Timestamp rightTs = null;
    if (left != null) {
      rightTs = Timestamp.valueOf(right);
    }
    HiveIntervalDayTime expectedResult = null;
    if (expected != null) {
      expectedResult = HiveIntervalDayTime.valueOf(expected);
    }
    DateTimeMath dtm = new DateTimeMath();
    HiveIntervalDayTime testResult =
        dtm.subtract(leftTs, rightTs);

    assertEquals(String.format("%s - %s", leftTs, rightTs),
        expectedResult, testResult);
  }
}
