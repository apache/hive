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

package org.apache.hadoop.hive.common.format.datetime;

import com.sun.tools.javac.util.List;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalField;
import java.util.ArrayList;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests HiveSqlDateTimeFormatter.
 */

public class TestHiveSqlDateTimeFormatter {

  private HiveSqlDateTimeFormatter formatter;

  @Test
  public void testSetPattern() {
    verifyPatternParsing(" ---yyyy-\'-:-  -,.;/MM-dd--", new ArrayList<>(List.of(
        null, // represents separator, which has no temporal field
        ChronoField.YEAR,
        null,
        ChronoField.MONTH_OF_YEAR,
        null,
        ChronoField.DAY_OF_MONTH,
        null
        )));

    verifyPatternParsing("ymmdddhh24::mi:ss A.M. pm", 25, "ymmdddhh24::mi:ss A.M. pm",
        new ArrayList<>(List.of(
        ChronoField.YEAR,
        ChronoField.MONTH_OF_YEAR,
        ChronoField.DAY_OF_YEAR,
        ChronoField.HOUR_OF_DAY,
        null, ChronoField.MINUTE_OF_HOUR,
        null, ChronoField.SECOND_OF_MINUTE,
        null, ChronoField.AMPM_OF_DAY,
        null, ChronoField.AMPM_OF_DAY
    )));
  }

  @Test
  public void testSetPatternWithBadPatterns() {
    verifyBadPattern("", true);
    verifyBadPattern("eyyyy-ddd", true);
    verifyBadPattern("1yyyy-mm-dd", true);

    //duplicates
    verifyBadPattern("yyyy Y", true);
    verifyBadPattern("yyyy R", true);

    //missing year or (month + dayofmonth or dayofyear)
    verifyBadPattern("yyyy", true);
    verifyBadPattern("yyyy-mm", true);
    verifyBadPattern("yyyy-dd", true);
    verifyBadPattern("mm-dd", true);
    verifyBadPattern("ddd", true);

    verifyBadPattern("yyyy-MM-DDD", true);
    verifyBadPattern("yyyy-mm-DD DDD", true);
    verifyBadPattern("yyyy-mm-dd HH24 HH12", true);
    verifyBadPattern("yyyy-mm-dd HH24 AM", true);
    verifyBadPattern("yyyy-mm-dd HH24 SSSSS", true);
    verifyBadPattern("yyyy-mm-dd HH12 SSSSS", true);
    verifyBadPattern("yyyy-mm-dd SSSSS AM", true);
    verifyBadPattern("yyyy-mm-dd MI SSSSS", true);
    verifyBadPattern("yyyy-mm-dd SS SSSSS", true);
    verifyBadPattern("yyyy mm-MON dd", true);
    verifyBadPattern("yyyy mm-MONTH dd", true);
    verifyBadPattern("yyyy MON, month dd", true);

    verifyBadPattern("iyyy-mm-dd", true); // can't mix iso and Gregorian
    verifyBadPattern("iyyy-id", true); // missing iyyy, iw, or id
    verifyBadPattern("iyyy-iw", true);
    verifyBadPattern("iw-id", true);

    verifyBadPattern("tzm", false);
    verifyBadPattern("tzh", false);

    //illegal for parsing
    verifyBadPattern("yyyy-mm-dd q", true);
    verifyBadPattern("yyyy-mm-dd d", true);
    verifyBadPattern("yyyy-mm-dd dy", true);
    verifyBadPattern("yyyy-mm-dd day", true);
    verifyBadPattern("yyyy-mm-dd w", true);
    verifyBadPattern("yyyy-mm-dd ww", true);
  }

  @Test
  public void testFormatTimestamp() {
    checkFormatTs("rr rrrr ddd", "2018-01-03 00:00:00", "18 2018 003");
    checkFormatTs("yyyy-mm-ddtsssss.ff4z", "2018-02-03 00:00:10.777777777", "2018-02-03T00010.7777Z");
    checkFormatTs("hh24:mi:ss.ff1", "2018-02-03 01:02:03.999999999", "01:02:03.9");
    checkFormatTs("y yyy hh:mi:ss.ffz", "2018-02-03 01:02:03.0070070", "8 018 01:02:03.007007Z");
    checkFormatTs("am a.m. pm p.m. AM A.M. PM P.M.", "2018-02-03 01:02:03.0070070", "am a.m. am a.m. AM A.M. AM A.M.");
    checkFormatTs("HH12 P.M.", "2019-01-01 00:15:10", "12 A.M.");
    checkFormatTs("HH12 AM", "2019-01-01 12:15:10", "12 PM");
    checkFormatTs("YYYY-MM-DD HH12PM", "2017-05-05 00:00:00", "2017-05-05 12AM");

    checkFormatTs("YYYY-MONTH-DD", "2019-01-01 00:00:00", "2019-JANUARY  -01"); //fill to length 9
    checkFormatTs("YYYY-MOnth-DD", "2019-01-01 00:00:00", "2019-JANUARY  -01");
    checkFormatTs("YYYY-Month-DD", "2019-01-01 00:00:00", "2019-January  -01");
    checkFormatTs("YYYY-MoNTH-DD", "2019-01-01 00:00:00", "2019-January  -01");
    checkFormatTs("YYYY-month-DD", "2019-01-01 00:00:00", "2019-january  -01");
    checkFormatTs("YYYY-mONTH-DD", "2019-01-01 00:00:00", "2019-january  -01");
    checkFormatTs("YYYY-MON-DD", "2019-01-01 00:00:00", "2019-JAN-01");
    checkFormatTs("YYYY-MOn-DD", "2019-01-01 00:00:00", "2019-JAN-01");
    checkFormatTs("YYYY-Mon-DD", "2019-01-01 00:00:00", "2019-Jan-01");
    checkFormatTs("YYYY-MoN-DD", "2019-01-01 00:00:00", "2019-Jan-01");
    checkFormatTs("YYYY-mon-DD", "2019-01-01 00:00:00", "2019-jan-01");
    checkFormatTs("YYYY-mON-DD", "2019-01-01 00:00:00", "2019-jan-01");

    checkFormatTs("D: DAY", "2019-01-01 00:00:00", "3: TUESDAY  "); //fill to length 9
    checkFormatTs("D: DAy", "2019-01-01 00:00:00", "3: TUESDAY  ");
    checkFormatTs("D: Day", "2019-01-02 00:00:00", "4: Wednesday");
    checkFormatTs("D: DaY", "2019-01-02 00:00:00", "4: Wednesday");
    checkFormatTs("D: day", "2019-01-03 00:00:00", "5: thursday ");
    checkFormatTs("D: dAY", "2019-01-03 00:00:00", "5: thursday ");
    checkFormatTs("D: DY", "2019-01-04 00:00:00", "6: FRI");
    checkFormatTs("D: Dy", "2019-01-05 00:00:00", "7: Sat");
    checkFormatTs("D: dy", "2019-01-06 00:00:00", "1: sun");
    checkFormatTs("D: DAY", "2019-01-07 00:00:00", "2: MONDAY   ");

    checkFormatTs("YYYY-mm-dd: Q WW W", "2019-01-01 00:00:00", "2019-01-01: 1 01 1");
    checkFormatTs("YYYY-mm-dd: Q WW W", "2019-01-07 00:00:00", "2019-01-07: 1 01 1");
    checkFormatTs("YYYY-mm-dd: Q WW W", "2019-01-08 00:00:00", "2019-01-08: 1 02 2");
    checkFormatTs("YYYY-mm-dd: Q WW W", "2019-03-31 00:00:00", "2019-03-31: 1 13 5");
    checkFormatTs("YYYY-mm-dd: Q WW W", "2019-04-01 00:00:00", "2019-04-01: 2 13 1");
    checkFormatTs("YYYY-mm-dd: Q WW W", "2019-12-31 00:00:00", "2019-12-31: 4 53 5");

    //ISO 8601
    checkFormatTs("YYYY-MM-DD : IYYY-IW-ID", "2018-12-31 00:00:00", "2018-12-31 : 2019-01-01");
    checkFormatTs("YYYY-MM-DD : IYYY-IW-ID", "2019-01-06 00:00:00", "2019-01-06 : 2019-01-07");
    checkFormatTs("YYYY-MM-DD : IYYY-IW-ID", "2019-01-07 00:00:00", "2019-01-07 : 2019-02-01");
    checkFormatTs("YYYY-MM-DD : IYYY-IW-ID", "2019-12-29 00:00:00", "2019-12-29 : 2019-52-07");
    checkFormatTs("YYYY-MM-DD : IYYY-IW-ID", "2019-12-30 00:00:00", "2019-12-30 : 2020-01-01");
    checkFormatTs("YYYY-MM-DD : IYY-IW-ID", "2019-12-30 00:00:00", "2019-12-30 : 020-01-01");
    checkFormatTs("YYYY-MM-DD : IY-IW-ID", "2019-12-30 00:00:00", "2019-12-30 : 20-01-01");
    checkFormatTs("YYYY-MM-DD : I-IW-ID", "2019-12-30 00:00:00", "2019-12-30 : 0-01-01");
    checkFormatTs("id: Day", "2018-12-31 00:00:00", "01: Monday   ");
    checkFormatTs("id: Day", "2019-01-01 00:00:00", "02: Tuesday  ");
    checkFormatTs("id: Day", "2019-01-02 00:00:00", "03: Wednesday");
    checkFormatTs("id: Day", "2019-01-03 00:00:00", "04: Thursday ");
    checkFormatTs("id: Day", "2019-01-04 00:00:00", "05: Friday   ");
    checkFormatTs("id: Day", "2019-01-05 00:00:00", "06: Saturday ");
    checkFormatTs("id: Day", "2019-01-06 00:00:00", "07: Sunday   ");
  }

  private void checkFormatTs(String pattern, String input, String expectedOutput) {
    formatter = new HiveSqlDateTimeFormatter(pattern, false);
    assertEquals("Format timestamp to string failed with pattern: " + pattern,
        expectedOutput, formatter.format(Timestamp.valueOf(input)));
  }

  @Test
  public void testFormatDate() {
    checkFormatDate("rr rrrr ddd", "2018-01-03", "18 2018 003");
    checkFormatDate("yyyy-mm-ddtsssss.ff4z", "2018-02-03", "2018-02-03T00000.0000Z");
    checkFormatDate("hh24:mi:ss.ff1", "2018-02-03", "00:00:00.0");
    checkFormatDate("y yyy T hh:mi:ss.ff am z", "2018-02-03", "8 018 T 12:00:00.0 am Z");
    checkFormatDate("am a.m. pm p.m. AM A.M. PM P.M.", "2018-02-03", "am a.m. am a.m. AM A.M. AM A.M.");
    checkFormatDate("DDD", "2019-12-31", "365");
    checkFormatDate("DDD", "2020-12-31", "366");
  }

  private void checkFormatDate(String pattern, String input, String expectedOutput) {
    formatter = new HiveSqlDateTimeFormatter(pattern, false);
    assertEquals("Format date to string failed with pattern: " + pattern,
        expectedOutput, formatter.format(Date.valueOf(input)));
  }

  @Test
  public void testParseTimestamp() {
    String thisYearString = String.valueOf(LocalDateTime.now().getYear());
    int firstTwoDigits = getFirstTwoDigits();

    //y
    checkParseTimestamp("y-mm-dd", "0-02-03", thisYearString.substring(0, 3) + "0-02-03 00:00:00");
    checkParseTimestamp("yy-mm-dd", "00-02-03", thisYearString.substring(0, 2) + "00-02-03 00:00:00");
    checkParseTimestamp("yyy-mm-dd", "000-02-03", thisYearString.substring(0, 1) + "000-02-03 00:00:00");
    checkParseTimestamp("yyyy-mm-dd", "000-02-03", thisYearString.substring(0, 1) + "000-02-03 00:00:00");
    checkParseTimestamp("rr-mm-dd", "0-02-03", thisYearString.substring(0, 3) + "0-02-03 00:00:00");
    checkParseTimestamp("rrrr-mm-dd", "000-02-03", thisYearString.substring(0, 1) + "000-02-03 00:00:00");

    //rr, rrrr
    checkParseTimestamp("rr-mm-dd", "00-02-03", firstTwoDigits + 1 + "00-02-03 00:00:00");
    checkParseTimestamp("rr-mm-dd", "49-02-03", firstTwoDigits + 1 + "49-02-03 00:00:00");
    checkParseTimestamp("rr-mm-dd", "50-02-03", firstTwoDigits + "50-02-03 00:00:00");
    checkParseTimestamp("rr-mm-dd", "99-02-03", firstTwoDigits + "99-02-03 00:00:00");
    checkParseTimestamp("rrrr-mm-dd", "00-02-03", firstTwoDigits + 1 + "00-02-03 00:00:00");
    checkParseTimestamp("rrrr-mm-dd", "49-02-03", firstTwoDigits + 1 + "49-02-03 00:00:00");
    checkParseTimestamp("rrrr-mm-dd", "50-02-03", firstTwoDigits + "50-02-03 00:00:00");
    checkParseTimestamp("rrrr-mm-dd", "99-02-03", firstTwoDigits + "99-02-03 00:00:00");

    //everything else
    checkParseTimestamp("yyyy-mm-ddThh24:mi:ss.ff8z", "2018-02-03T04:05:06.5665Z", "2018-02-03 04:05:06.5665");
    checkParseTimestamp("yyyy-mm-dd hh24:mi:ss.ff", "2018-02-03 04:05:06.555555555", "2018-02-03 04:05:06.555555555");
    checkParseTimestamp("yyyy-mm-dd hh12:mi:ss", "2099-2-03 04:05:06", "2099-02-03 04:05:06");
    checkParseTimestamp("yyyyddd", "2018284", "2018-10-11 00:00:00");
    checkParseTimestamp("yyyyddd", "20184", "2018-01-04 00:00:00");
    checkParseTimestamp("yyyy-mm-ddThh24:mi:ss.ffz", "2018-02-03t04:05:06.444Z", "2018-02-03 04:05:06.444");
    checkParseTimestamp("yyyy-mm-dd hh:mi:ss A.M.", "2018-02-03 04:05:06 P.M.", "2018-02-03 16:05:06");
    checkParseTimestamp("YYYY-MM-DD HH24:MI TZH:TZM", "2019-1-1 14:00--1:-30", "2019-01-01 14:00:00");
    checkParseTimestamp("YYYY-MM-DD HH24:MI TZH:TZM", "2019-1-1 14:00-1:30", "2019-01-01 14:00:00");
    checkParseTimestamp("yyyy-mm-dd TZM:TZH", "2019-01-01 1 -3", "2019-01-01 00:00:00");
    checkParseTimestamp("yyyy-mm-dd TZH:TZM", "2019-01-01 -0:30", "2019-01-01 00:00:00");
    checkParseTimestamp("TZM/YYY-MM-TZH/DD", "0/333-01-11/02", "2333-01-02 00:00:00");
    checkParseTimestamp("YYYY-MM-DD HH12:MI AM", "2019-01-01 11:00 p.m.", "2019-01-01 23:00:00");
    checkParseTimestamp("YYYY-MM-DD HH12:MI A.M..", "2019-01-01 11:00 pm.", "2019-01-01 23:00:00");
    checkParseTimestamp("MI DD-TZM-YYYY-MM TZHPM SS:HH12.FF9",
        "59 03-30-2017-05 01PM 01:08.123456789", "2017-05-03 20:59:01.123456789");
    checkParseTimestamp("YYYYDDMMHH12MISSFFAMTZHTZM",
        "20170501123159123456789AM-0130", "2017-01-05 00:31:59.123456789");
    checkParseTimestamp("YYYY-MM-DD AMHH12", "2017-05-06 P.M.12", "2017-05-06 12:00:00");
    checkParseTimestamp("YYYY-MM-DD HH12PM", "2017-05-05 12AM", "2017-05-05 00:00:00");
    checkParseTimestamp("YYYY-MM-DD HH12:MI:SS.FF9PM TZH:TZM",
        "2017-05-03 08:59:01.123456789PM 01:30", "2017-05-03 20:59:01.123456789");
    checkParseTimestamp("YYYYDDMMHH12MISSFFAMTZHTZM",
        "20170501120159123456789AM-0130", "2017-01-05 00:01:59.123456789");

    //Test "day in year" token in a leap year scenario
    checkParseTimestamp("YYYY DDD", "2000 60", "2000-02-29 00:00:00");
    checkParseTimestamp("YYYY DDD", "2000 61", "2000-03-01 00:00:00");
    checkParseTimestamp("YYYY DDD", "2000 366", "2000-12-31 00:00:00");
    //Test timezone offset parsing without separators
    checkParseTimestamp("YYYYMMDDHH12MIA.M.TZHTZM", "201812310800AM+0515", "2018-12-31 08:00:00");
    checkParseTimestamp("YYYYMMDDHH12MIA.M.TZHTZM", "201812310800AM0515", "2018-12-31 08:00:00");
    checkParseTimestamp("YYYYMMDDHH12MIA.M.TZHTZM", "201812310800AM-0515", "2018-12-31 08:00:00");

    //MONTH, MON : case really doesn't matter
    checkParseTimestamp("yyyy-MONTH-dd", "2018-FEBRUARY-28", "2018-02-28 00:00:00");
    checkParseTimestamp("yyyy-Month-dd", "2018-february-28", "2018-02-28 00:00:00");
    checkParseTimestamp("yyyy-month-dd", "2018-FEBRUARY-28", "2018-02-28 00:00:00");
    checkParseTimestamp("yyyy-montH-dd", "2018-febRuary-28", "2018-02-28 00:00:00");
    checkParseTimestamp("yyyy-MON-dd", "2018-FEB-28", "2018-02-28 00:00:00");
    checkParseTimestamp("yyyy-moN-dd", "2018-FeB-28", "2018-02-28 00:00:00");
    checkParseTimestamp("yyyy-mon-dd", "2018-FEB-28", "2018-02-28 00:00:00");
    verifyBadParseString("yyyy-MON-dd", "2018-FEBRUARY-28");
    verifyBadParseString("yyyy-MON-dd", "2018-FEBR-28");
    verifyBadParseString("yyyy-MONTH-dd", "2018-FEB-28");
    //letters and numbers are delimiters to each other, respectively
    checkParseDate("yyyy-ddMONTH", "2018-4March", "2018-03-04");
    checkParseDate("yyyy-MONTHdd", "2018-March4", "2018-03-04");
    //ISO 8601
    checkParseTimestamp("IYYY-IW-ID", "2019-01-01", "2018-12-31 00:00:00");
    checkParseTimestamp("IYYY-IW-ID", "2019-01-07", "2019-01-06 00:00:00");
    checkParseTimestamp("IYYY-IW-ID", "2019-02-01", "2019-01-07 00:00:00");
    checkParseTimestamp("IYYY-IW-ID", "2019-52-07", "2019-12-29 00:00:00");
    checkParseTimestamp("IYYY-IW-ID", "2020-01-01", "2019-12-30 00:00:00");
    checkParseTimestamp("IYYY-IW-ID", "020-01-04", thisYearString.substring(0, 1) + "020-01-02 00:00:00");
    checkParseTimestamp("IYY-IW-ID", "020-01-04", thisYearString.substring(0, 1) + "020-01-02 00:00:00");
    checkParseTimestamp("IYY-IW-ID", "20-01-04", thisYearString.substring(0, 2) + "20-01-02 00:00:00");
    checkParseTimestamp("IY-IW-ID", "20-01-04", thisYearString.substring(0, 2) + "20-01-02 00:00:00");
    checkParseTimestamp("IYYY-IW-DAY", "2019-01-monday", "2018-12-31 00:00:00");
    checkParseTimestamp("IYYY-IW-Day", "2019-01-Sunday", "2019-01-06 00:00:00");
    checkParseTimestamp("IYYY-IW-Dy", "2019-02-MON", "2019-01-07 00:00:00");
    checkParseTimestamp("IYYY-IW-DY", "2019-52-sun", "2019-12-29 00:00:00");
    checkParseTimestamp("IYYY-IW-dy", "2020-01-Mon", "2019-12-30 00:00:00");
    //Tests for these patterns would need changing every decade if done in the above way.
    //Thursday of the first week in an ISO year always matches the Gregorian year.
    checkParseTimestampIso("IY-IW-ID", "0-01-04", "iw, yyyy", "01, " + thisYearString.substring(0, 3) + "0");
    checkParseTimestampIso("I-IW-ID", "0-01-04", "iw, yyyy", "01, " + thisYearString.substring(0, 3) + "0");
    //time patterns are allowed; date patterns are not
    checkParseTimestamp("IYYY-IW-ID hh24:mi:ss", "2019-01-01 01:02:03", "2018-12-31 01:02:03");
  }

  private int getFirstTwoDigits() {
    int thisYear = LocalDateTime.now().getYear();
    int firstTwoDigits = thisYear / 100;
    if (thisYear % 100 < 50) {
      firstTwoDigits -= 1;
    }
    return firstTwoDigits;
  }

  private void checkParseTimestamp(String pattern, String input, String expectedOutput) {
    formatter = new HiveSqlDateTimeFormatter(pattern, true);
    assertEquals("Parse string to timestamp failed. Pattern: " + pattern,
        Timestamp.valueOf(expectedOutput), formatter.parseTimestamp(input));
  }

  private void checkParseTimestampIso(String parsePattern, String input, String formatPattern,
      String expectedOutput) {
    formatter = new HiveSqlDateTimeFormatter(parsePattern, true);
    Timestamp ts = formatter.parseTimestamp(input);
    formatter = new HiveSqlDateTimeFormatter(formatPattern, false);
    assertEquals(expectedOutput, formatter.format(ts));
  }

  @Test
  public void testParseDate() {

    String thisYearString = String.valueOf(LocalDateTime.now().getYear());
    int firstTwoDigits = getFirstTwoDigits();
    //y
    checkParseDate("y-mm-dd", "0-02-03", thisYearString.substring(0, 3) + "0-02-03");
    checkParseDate("yy-mm-dd", "00-02-03", thisYearString.substring(0, 2) + "00-02-03");
    checkParseDate("yyy-mm-dd", "000-02-03", thisYearString.substring(0, 1) + "000-02-03");
    checkParseDate("yyyy-mm-dd", "000-02-03", thisYearString.substring(0, 1) + "000-02-03");
    checkParseDate("rr-mm-dd", "0-02-03", thisYearString.substring(0, 3) + "0-02-03");
    checkParseDate("rrrr-mm-dd", "000-02-03", thisYearString.substring(0, 1) + "000-02-03");

    //rr, rrrr
    checkParseDate("rr-mm-dd", "00-02-03", firstTwoDigits + 1 + "00-02-03");
    checkParseDate("rr-mm-dd", "49-02-03", firstTwoDigits + 1 + "49-02-03");
    checkParseDate("rr-mm-dd", "50-02-03", firstTwoDigits + "50-02-03");
    checkParseDate("rr-mm-dd", "99-02-03", firstTwoDigits + "99-02-03");
    checkParseDate("rrrr-mm-dd", "00-02-03", firstTwoDigits + 1 + "00-02-03");
    checkParseDate("rrrr-mm-dd", "49-02-03", firstTwoDigits + 1 + "49-02-03");
    checkParseDate("rrrr-mm-dd", "50-02-03", firstTwoDigits + "50-02-03");
    checkParseDate("rrrr-mm-dd", "99-02-03", firstTwoDigits + "99-02-03");

    checkParseDate("yyyy-mm-dd hh mi ss.ff7", "2018/01/01 2.2.2.55", "2018-01-01");

    checkParseDate("dd/MonthT/yyyy", "31/AugustT/2020", "2020-08-31");
    checkParseDate("dd/MonthT/yyyy", "31/MarchT/2020", "2020-03-31");

    //ISO 8601
    checkParseDate("IYYY-IW-ID", "2019-01-01", "2018-12-31");
    checkParseDate("IW-ID-IYYY", "01-02-2019", "2019-01-01");
    checkParseDate("ID-IW-IYYY", "02-01-2019", "2019-01-01");
    checkParseDate("IYYY-IW-ID", "2019-01-07", "2019-01-06");
    checkParseDate("IYYY-IW-ID", "2019-02-01", "2019-01-07");
    checkParseDate("IYYY-IW-ID", "2019-52-07", "2019-12-29");
    checkParseDate("IYYY-IW-ID", "2020-01-01", "2019-12-30");
  }

  private void checkParseDate(String pattern, String input, String expectedOutput) {
    formatter = new HiveSqlDateTimeFormatter(pattern, true);
    assertEquals("Parse string to date failed. Pattern: " + pattern,
        Date.valueOf(expectedOutput), formatter.parseDate(input));
  }

  @Test
  public void testParseTimestampError() {
    verifyBadParseString("yyyy-mm-dd  ", "2019-02-03"); //separator missing
    verifyBadParseString("yyyy-mm-dd", "2019-02-03..."); //extra separators
    verifyBadParseString("yyyy-mm-dd hh12:mi:ss", "2019-02-03 14:00:00"); //hh12 out of range
    verifyBadParseString("yyyy-dddsssss", "2019-912345"); //ddd out of range
    verifyBadParseString("yyyy-mm-dd", "2019-13-23"); //mm out of range
    verifyBadParseString("yyyy-mm-dd tzh:tzm", "2019-01-01 +16:00"); //tzh out of range
    verifyBadParseString("yyyy-mm-dd tzh:tzm", "2019-01-01 +14:60"); //tzm out of range
    verifyBadParseString("YYYY DDD", "2000 367"); //ddd out of range
    verifyBadParseString("yyyy-month-dd", "2019-merch-23"); //invalid month of year
    verifyBadParseString("yyyy-mon-dd", "2019-mer-23"); //invalid month of year
    verifyBadParseString("yyyy-MON-dd", "2018-FEBRUARY-28"); // can't mix and match mon and month
    verifyBadParseString("yyyy-MON-dd", "2018-FEBR-28");
    verifyBadParseString("yyyy-MONTH-dd", "2018-FEB-28");
    verifyBadParseString("iyyy-iw-id", "2019-00-01"); //ISO 8601 week number out of range for year
    verifyBadParseString("iyyy-iw-id", "2019-53-01"); //ISO 8601 week number out of range for year
    verifyBadParseString("iw-iyyy-id", "53-2019-01"); //ISO 8601 week number out of range for year
    verifyBadParseString("iw-iyyy-id", "54-2019-01"); //ISO 8601 week number out of range
    verifyBadParseString("iyyy-iw-id", "2019-52-00"); //ISO 8601 day of week out of range
    verifyBadParseString("iyyy-iw-id", "2019-52-08"); //ISO 8601 day of week out of range
  }

  private void verifyBadPattern(String string, boolean forParsing) {
    try {
      formatter = new HiveSqlDateTimeFormatter(string, forParsing);
      fail("Bad pattern " + string + " should have thrown IllegalArgumentException but didn't");
    } catch (Exception e) {
      assertEquals("Expected IllegalArgumentException, got another exception.",
          e.getClass().getName(), IllegalArgumentException.class.getName());
    }
  }

  @Test
  public void testFm() {
    //year (019) becomes 19 even if pattern is yyy
    checkFormatTs("FMyyy-FMmm-dd FMHH12:MI:FMSS", "2019-01-01 01:01:01", "19-1-01 1:01:1");
    checkFormatTs("FMiyy-FMiw-id FMHH12:MI:FMSS", "2018-12-31 01:01:01", "19-1-01 1:01:1");
    //ff[1-9] shouldn't be affected, because leading zeroes hold information
    checkFormatTs("FF5/FMFF5", "2019-01-01 01:01:01.0333", "03330/03330");
    checkFormatTs("FF/FMFF", "2019-01-01 01:01:01.0333", "0333/0333");
    //omit trailing spaces from character temporal elements
    checkFormatTs("YYYY-fmMonth-DD", "2019-01-01 00:00:00", "2019-January-01");
    checkFormatTs("D: fmDAY", "2019-01-01 00:00:00", "3: TUESDAY");
    checkFormatTs("D: fmDay", "2019-01-02 00:00:00", "4: Wednesday");

    //only affects temporals that immediately follow
    verifyBadPattern("yyy-mm-dd FM,HH12", false);
    verifyBadPattern("yyy-mm-dd FM,HH12", true);
    verifyBadPattern("yyy-mm-dd HH12 tzh:fmtzm", true);
    verifyBadPattern("FMFMyyy-mm-dd", true);
    verifyBadPattern("FMFXDD-MM-YYYY ff2", true);
  }

  @Test
  public void testFx() {
    checkParseDate("FXDD-MM-YYYY", "01-01-1998", "1998-01-01");
    checkParseTimestamp("FXDD-MM-YYYY hh12:mi:ss.ff", "15-01-1998 11:12:13.0", "1998-01-15 11:12:13");
    //ff[1-9] are exempt
    checkParseTimestamp("FXDD-MM-YYYY hh12:mi:ss.ff6", "01-01-1998 00:00:00.4440", "1998-01-01 00:00:00.444");
    //fx can be anywhere in the pattern string
    checkParseTimestamp("DD-MM-YYYYFX", "01-01-1998", "1998-01-01 00:00:00");
    verifyBadParseString("DD-MM-YYYYFX", "1-01-1998");
    //same separators required
    verifyBadParseString("FXDD-MM-YYYY", "15/01/1998");
    //no filling in zeroes or year digits
    verifyBadParseString("FXDD-MM-YYYY", "1-01-1998");
    verifyBadParseString("FXDD-MM-YYYY", "01-01-98");
    //no leading or trailing whitespace
    verifyBadParseString("FXDD-MM-YYYY", "   01-01-1998   ");
    //enforce correct amount of leading zeroes
    verifyBadParseString("FXyyyy-mm-dd hh24:miss", "2018-01-01 17:005");
    verifyBadParseString("FXyyyy-mm-dd sssss", "2019-01-01 003");
    verifyBadParseString("FXiyyy-iw-id hh24:mi:ss", "019-01-02 17:00:05");
    //text case does not matter
    checkParseTimestamp("\"the DATE is\" yyyy-mm-dd", "the date is 2018-01-01", "2018-01-01 00:00:00");
    //AM/PM length has to match, but case doesn't
    checkParseTimestamp("FXDD-MM-YYYY hh12 am", "01-01-1998 12 PM", "1998-01-01 12:00:00");
    checkParseTimestamp("FXDD-MM-YYYY hh12 A.M.", "01-01-1998 12 p.m.", "1998-01-01 12:00:00");
    verifyBadParseString("FXDD-MM-YYYY hh12 am", "01-01-1998 12 p.m.");
    verifyBadParseString("FXDD-MM-YYYY hh12 a.m.", "01-01-1998 12 pm");
    //character temporals shouldn't have trailing spaces
    checkParseTimestamp("FXDD-month-YYYY", "15-March-1998", "1998-03-15 00:00:00");
  }

  @Test
  public void testFmFx() {
    checkParseTimestamp("FXDD-FMMM-YYYY hh12 am", "01-1-1998 12 PM", "1998-01-01 12:00:00");
    checkParseTimestamp("FXFMDD-MM-YYYY hh12 am", "1-01-1998 12 PM", "1998-01-01 12:00:00");
    checkParseTimestamp("FXFMiyyy-iw-id hh24:mi:ss", "019-01-02 17:00:05", "2019-01-01 17:00:05");
    verifyBadParseString("FXFMiyyy-iw-id hh24:mi:ss", "019-01-02 17:0:05");
    //ff[1-9] unaffected
    checkParseTimestamp("FXFMDD-MM-YYYY FMff2", "1-01-1998 4", "1998-01-01 00:00:00.4");
    checkParseTimestamp("FXFMDD-MM-YYYY ff2", "1-01-1998 4", "1998-01-01 00:00:00.4");
  }

  @Test
  public void testText() {
    // keep exact text upon format
    checkFormatTs("hh24:mi \" Is \" hh12 PM\".\"", "2008-01-01 17:00:00", "17:00  Is  05 PM.");
    checkFormatDate("\" `the _year_ is` \" yyyy\".\"", "2008-01-01", " `the _year_ is`  2008.");
    // empty text strings work
    checkParseTimestamp("\"\"yyyy\"\"-mm-dd\"\"", "2019-01-01", "2019-01-01 00:00:00");
    checkParseDate("\"\"yyyy\"\"-mm-dd\"\"", "2019-01-01", "2019-01-01");
    // Case doesn't matter upon parsing
    checkParseTimestamp("\"Year \"YYYY \"month\" MM \"day\" DD.\"!\"",
        "YEaR 3000 mOnTh 3 DaY 1...!", "3000-03-01 00:00:00");
    checkParseDate("\"Year \"YYYY \"month\" MM \"day\" DD.\"!\"",
        "YEaR 3000 mOnTh 3 DaY 1...!", "3000-03-01");
    // Characters matter upon parsing
    verifyBadParseString("\"Year! \"YYYY \"m\" MM \"d\" DD.\"!\"", "Year 3000 m 3 d 1,!");
    // non-numeric characters in text counts as a delimiter
    checkParseDate("yyyy\"m\"mm\"d\"dd", "19m1d1", LocalDate.now().getYear() / 100 + "19-01-01");
    checkParseDate("yyyy\"[\"mm\"]\"dd", "19[1]1", LocalDate.now().getYear() / 100 + "19-01-01");
    // parse character temporals correctly
    checkParseDate("dd/Month\"arch\"/yyyy", "31/Marcharch/2020", "2020-03-31");
    checkParseDate("dd/Month\"ember\"/yyyy", "31/Decemberember/2020", "2020-12-31");

    // single quotes are separators and not text delimiters
    checkParseTimestamp("\"Y\'ear \"YYYY \' \"month\" MM \"day\" DD.\"!\"",
        "Y'EaR 3000 ' mOnTh 3 DaY 1...!", "3000-03-01 00:00:00");
    checkParseDate("\"Y\'ear \"YYYY \' \"month\" MM \"day\" DD.\"!\"",
        "Y'EaR 3000 ' mOnTh 3 DaY 1...!", "3000-03-01");
    // literal double quotes are escaped
    checkFormatTs("\"the \\\"DATE\\\" is\" yyyy-mm-dd",
        "2018-01-01 00:00:00", "the \"DATE\" is 2018-01-01");
    checkFormatTs("\"\\\"\\\"\\\"\"", "2018-01-01 00:00:00", "\"\"\"");
    checkParseTimestamp("\"the \\\"DATE\\\" is\" yyyy-mm-dd",
        "the \"date\" is 2018-01-01", "2018-01-01 00:00:00");
    // Check variations of apostrophes, literal and non-literal double quotes
    checkParseTimestamp("yyyy'\"\"mm-dd", "2019\'01-01", "2019-01-01 00:00:00");
    checkParseTimestamp("yyyy\'\"\"mm-dd", "2019\'01-01", "2019-01-01 00:00:00");
    checkParseTimestamp("yyyy'\"\"mm-dd", "2019'01-01", "2019-01-01 00:00:00");
    checkParseTimestamp("yyyy\'\"\"mm-dd", "2019'01-01", "2019-01-01 00:00:00");
    checkParseTimestamp("yyyy\'\"\\\"\"mm-dd", "2019'\"01-01", "2019-01-01 00:00:00");
    checkParseTimestamp("yyyy\'\"\\\"\"mm-dd", "2019\'\"01-01", "2019-01-01 00:00:00");
  }

  /**
   * Verify pattern is parsed correctly.
   * Check:
   * -token.temporalField for each token
   * -sum of token.lengths
   * -concatenation of token.strings
   */
  private void verifyPatternParsing(String pattern, ArrayList<TemporalField> temporalFields) {
    verifyPatternParsing(pattern, pattern.length(), pattern.toLowerCase(), temporalFields);
  }

  private void verifyPatternParsing(String pattern, int expectedPatternLength,
      String expectedPattern, ArrayList<TemporalField> temporalFields) {
    formatter = new HiveSqlDateTimeFormatter(pattern, false);
    assertEquals(temporalFields.size(), formatter.getTokens().size());
    StringBuilder sb = new StringBuilder();
    int actualPatternLength = 0;
    for (int i = 0; i < temporalFields.size(); i++) {
      assertEquals("Generated list of tokens not correct", temporalFields.get(i),
          formatter.getTokens().get(i).temporalField);
      sb.append(formatter.getTokens().get(i).string);
      actualPatternLength += formatter.getTokens().get(i).length;
    }
    assertEquals("Token strings concatenated don't match original pattern string",
        expectedPattern, sb.toString());
    assertEquals(expectedPatternLength, actualPatternLength);
  }

  private void verifyBadParseString(String pattern, String string) {
    formatter = new HiveSqlDateTimeFormatter(pattern, true);
    try {
      Timestamp output = formatter.parseTimestamp(string);
      fail("Parse string to timestamp should have failed.\nString: " + string + "\nPattern: "
          + pattern + ", output = " + output);
    } catch (Exception e) {
      assertEquals("Expected IllegalArgumentException, got another exception.",
          e.getClass().getName(), IllegalArgumentException.class.getName());
    }
  }
}
