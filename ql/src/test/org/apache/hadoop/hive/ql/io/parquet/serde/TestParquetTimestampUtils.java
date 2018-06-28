/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.serde;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTime;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils;

import junit.framework.Assert;
import junit.framework.TestCase;



/**
 * Tests util-libraries used for parquet-timestamp.
 */
public class TestParquetTimestampUtils extends TestCase {

  public void testJulianDay() {
    //check if May 23, 1968 is Julian Day 2440000
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.YEAR,  1968);
    cal.set(Calendar.MONTH, Calendar.MAY);
    cal.set(Calendar.DAY_OF_MONTH, 23);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.setTimeZone(TimeZone.getTimeZone("GMT"));

    Timestamp ts = Timestamp.ofEpochMilli(cal.getTimeInMillis());
    NanoTime nt = NanoTimeUtils.getNanoTime(ts, false);
    Assert.assertEquals(nt.getJulianDay(), 2440000);

    Timestamp tsFetched = NanoTimeUtils.getTimestamp(nt, false);
    Assert.assertEquals(tsFetched, ts);

    //check if 30 Julian Days between Jan 1, 2005 and Jan 31, 2005.
    Calendar cal1 = Calendar.getInstance();
    cal1.set(Calendar.YEAR,  2005);
    cal1.set(Calendar.MONTH, Calendar.JANUARY);
    cal1.set(Calendar.DAY_OF_MONTH, 1);
    cal1.set(Calendar.HOUR_OF_DAY, 0);
    cal1.setTimeZone(TimeZone.getTimeZone("GMT"));

    Timestamp ts1 = Timestamp.ofEpochMilli(cal1.getTimeInMillis());
    NanoTime nt1 = NanoTimeUtils.getNanoTime(ts1, false);

    Timestamp ts1Fetched = NanoTimeUtils.getTimestamp(nt1, false);
    Assert.assertEquals(ts1Fetched, ts1);

    Calendar cal2 = Calendar.getInstance();
    cal2.set(Calendar.YEAR,  2005);
    cal2.set(Calendar.MONTH, Calendar.JANUARY);
    cal2.set(Calendar.DAY_OF_MONTH, 31);
    cal2.set(Calendar.HOUR_OF_DAY, 0);
    cal2.setTimeZone(TimeZone.getTimeZone("UTC"));

    Timestamp ts2 = Timestamp.ofEpochMilli(cal2.getTimeInMillis());
    NanoTime nt2 = NanoTimeUtils.getNanoTime(ts2, false);

    Timestamp ts2Fetched = NanoTimeUtils.getTimestamp(nt2, false);
    Assert.assertEquals(ts2Fetched, ts2);
    Assert.assertEquals(nt2.getJulianDay() - nt1.getJulianDay(), 30);

    //check if 1464305 Julian Days between Jan 1, 2005 BC and Jan 31, 2005.
    cal1 = Calendar.getInstance();
    cal1.set(Calendar.ERA,  GregorianCalendar.BC);
    cal1.set(Calendar.YEAR,  2005);
    cal1.set(Calendar.MONTH, Calendar.JANUARY);
    cal1.set(Calendar.DAY_OF_MONTH, 1);
    cal1.set(Calendar.HOUR_OF_DAY, 0);
    cal1.setTimeZone(TimeZone.getTimeZone("GMT"));

    ts1 = Timestamp.ofEpochMilli(cal1.getTimeInMillis());
    nt1 = NanoTimeUtils.getNanoTime(ts1, false);

    ts1Fetched = NanoTimeUtils.getTimestamp(nt1, false);
    Assert.assertEquals(ts1Fetched, ts1);

    cal2 = Calendar.getInstance();
    cal2.set(Calendar.YEAR,  2005);
    cal2.set(Calendar.MONTH, Calendar.JANUARY);
    cal2.set(Calendar.DAY_OF_MONTH, 31);
    cal2.set(Calendar.HOUR_OF_DAY, 0);
    cal2.setTimeZone(TimeZone.getTimeZone("UTC"));

    ts2 = Timestamp.ofEpochMilli(cal2.getTimeInMillis());
    nt2 = NanoTimeUtils.getNanoTime(ts2, false);

    ts2Fetched = NanoTimeUtils.getTimestamp(nt2, false);
    Assert.assertEquals(ts2Fetched, ts2);
    Assert.assertEquals(nt2.getJulianDay() - nt1.getJulianDay(), 1464305);
}

  public void testNanos() {
    //case 1: 01:01:01.0000000001
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.YEAR,  1968);
    cal.set(Calendar.MONTH, Calendar.MAY);
    cal.set(Calendar.DAY_OF_MONTH, 23);
    cal.set(Calendar.HOUR_OF_DAY, 1);
    cal.set(Calendar.MINUTE, 1);
    cal.set(Calendar.SECOND, 1);
    cal.setTimeZone(TimeZone.getTimeZone("GMT"));
    Timestamp ts = Timestamp.ofEpochMilli(cal.getTimeInMillis(), 1);

    //(1*60*60 + 1*60 + 1) * 10e9 + 1
    NanoTime nt = NanoTimeUtils.getNanoTime(ts, false);
    Assert.assertEquals(nt.getTimeOfDayNanos(), 3661000000001L);

    //case 2: 23:59:59.999999999
    cal = Calendar.getInstance();
    cal.set(Calendar.YEAR,  1968);
    cal.set(Calendar.MONTH, Calendar.MAY);
    cal.set(Calendar.DAY_OF_MONTH, 23);
    cal.set(Calendar.HOUR_OF_DAY, 23);
    cal.set(Calendar.MINUTE, 59);
    cal.set(Calendar.SECOND, 59);
    cal.setTimeZone(TimeZone.getTimeZone("GMT"));
    ts = Timestamp.ofEpochMilli(cal.getTimeInMillis(), 999999999);

    //(23*60*60 + 59*60 + 59)*10e9 + 999999999
    nt = NanoTimeUtils.getNanoTime(ts, false);
    Assert.assertEquals(nt.getTimeOfDayNanos(), 86399999999999L);

    //case 3: verify the difference.
    Calendar cal2 = Calendar.getInstance();
    cal2.set(Calendar.YEAR,  1968);
    cal2.set(Calendar.MONTH, Calendar.MAY);
    cal2.set(Calendar.DAY_OF_MONTH, 23);
    cal2.set(Calendar.HOUR_OF_DAY, 0);
    cal2.set(Calendar.MINUTE, 10);
    cal2.set(Calendar.SECOND, 0);
    cal2.setTimeZone(TimeZone.getTimeZone("GMT"));
    Timestamp ts2 = Timestamp.ofEpochMilli(cal2.getTimeInMillis(), 10);

    Calendar cal1 = Calendar.getInstance();
    cal1.set(Calendar.YEAR,  1968);
    cal1.set(Calendar.MONTH, Calendar.MAY);
    cal1.set(Calendar.DAY_OF_MONTH, 23);
    cal1.set(Calendar.HOUR_OF_DAY, 0);
    cal1.set(Calendar.MINUTE, 0);
    cal1.set(Calendar.SECOND, 0);
    cal1.setTimeZone(TimeZone.getTimeZone("GMT"));
    Timestamp ts1 = Timestamp.ofEpochMilli(cal1.getTimeInMillis(), 1);

    NanoTime n2 = NanoTimeUtils.getNanoTime(ts2, false);
    NanoTime n1 = NanoTimeUtils.getNanoTime(ts1, false);

    Assert.assertEquals(n2.getTimeOfDayNanos() - n1.getTimeOfDayNanos(), 600000000009L);

    NanoTime n3 = new NanoTime(n1.getJulianDay() - 1, n1.getTimeOfDayNanos() + TimeUnit.DAYS.toNanos(1));
    Assert.assertEquals(ts1, NanoTimeUtils.getTimestamp(n3, false));
    n3 = new NanoTime(n1.getJulianDay() + 3, n1.getTimeOfDayNanos() - TimeUnit.DAYS.toNanos(3));
    Assert.assertEquals(ts1, NanoTimeUtils.getTimestamp(n3, false));
  }

  public void testTimezone() {
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.YEAR,  1968);
    cal.set(Calendar.MONTH, Calendar.MAY);
    cal.set(Calendar.DAY_OF_MONTH, 23);
    cal.set(Calendar.HOUR_OF_DAY, 17);
    cal.set(Calendar.MINUTE, 1);
    cal.set(Calendar.SECOND, 1);
    cal.setTimeZone(TimeZone.getTimeZone("US/Pacific"));
    Timestamp ts = Timestamp.ofEpochMilli(cal.getTimeInMillis(), 1);

    /**
     * 17:00 PDT = 00:00 GMT (daylight-savings)
     * (0*60*60 + 1*60 + 1)*10e9 + 1 = 61000000001, or
     *
     * 17:00 PST = 01:00 GMT (if not daylight savings)
     * (1*60*60 + 1*60 + 1)*10e9 + 1 = 3661000000001
     */
    NanoTime nt = NanoTimeUtils.getNanoTime(ts, false);
    long timeOfDayNanos = nt.getTimeOfDayNanos();
    Assert.assertTrue(timeOfDayNanos == 61000000001L || timeOfDayNanos == 3661000000001L);

    //in both cases, this will be the next day in GMT
    Assert.assertEquals(nt.getJulianDay(), 2440001);
  }

  public void testTimezoneValues() {
    valueTest(false);
  }

  public void testTimezonelessValues() {
    valueTest(true);
  }

  public void testTimezoneless() {
    Timestamp ts1 = Timestamp.valueOf("2011-01-01 00:30:30.111111111");
    NanoTime nt1 = NanoTimeUtils.getNanoTime(ts1, true);
    Assert.assertEquals(nt1.getJulianDay(), 2455562);
    Assert.assertEquals(nt1.getTimeOfDayNanos(), 59430111111111L);
    Timestamp ts1Fetched = NanoTimeUtils.getTimestamp(nt1, true);
    Assert.assertEquals(ts1Fetched.toString(), ts1.toString());

    Timestamp ts2 = Timestamp.valueOf("2011-02-02 08:30:30.222222222");
    NanoTime nt2 = NanoTimeUtils.getNanoTime(ts2, true);
    Assert.assertEquals(nt2.getJulianDay(), 2455595);
    Assert.assertEquals(nt2.getTimeOfDayNanos(), 1830222222222L);
    Timestamp ts2Fetched = NanoTimeUtils.getTimestamp(nt2, true);
    Assert.assertEquals(ts2Fetched.toString(), ts2.toString());
  }

  private void valueTest(boolean local) {
    //exercise a broad range of timestamps close to the present.
    verifyTsString("2011-01-01 01:01:01.111111111", local);
    verifyTsString("2012-02-02 02:02:02.222222222", local);
    verifyTsString("2013-03-03 03:03:03.333333333", local);
    verifyTsString("2014-04-04 04:04:04.444444444", local);
    verifyTsString("2015-05-05 05:05:05.555555555", local);
    verifyTsString("2016-06-06 06:06:06.666666666", local);
    verifyTsString("2017-07-07 07:07:07.777777777", local);
    verifyTsString("2018-08-08 08:08:08.888888888", local);
    verifyTsString("2019-09-09 09:09:09.999999999", local);
    verifyTsString("2020-10-10 10:10:10.101010101", local);
    verifyTsString("2021-11-11 11:11:11.111111111", local);
    verifyTsString("2022-12-12 12:12:12.121212121", local);
    verifyTsString("2023-01-02 13:13:13.131313131", local);
    verifyTsString("2024-02-02 14:14:14.141414141", local);
    verifyTsString("2025-03-03 15:15:15.151515151", local);
    verifyTsString("2026-04-04 16:16:16.161616161", local);
    verifyTsString("2027-05-05 17:17:17.171717171", local);
    verifyTsString("2028-06-06 18:18:18.181818181", local);
    verifyTsString("2029-07-07 19:19:19.191919191", local);
    verifyTsString("2030-08-08 20:20:20.202020202", local);
    verifyTsString("2031-09-09 21:21:21.212121212", local);

    //test some extreme cases.
    verifyTsString("9999-09-09 09:09:09.999999999", local);
    verifyTsString("0001-01-01 00:00:00", local);
  }

  private void verifyTsString(String tsString, boolean local) {
    Timestamp ts = Timestamp.valueOf(tsString);
    NanoTime nt = NanoTimeUtils.getNanoTime(ts, local);
    Timestamp tsFetched = NanoTimeUtils.getTimestamp(nt, local);
    Assert.assertEquals(tsString, tsFetched.toString());
  }
}
