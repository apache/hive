/**
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

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTime;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils;



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

    Timestamp ts = new Timestamp(cal.getTimeInMillis());
    NanoTime nt = NanoTimeUtils.getNanoTime(ts);
    Assert.assertEquals(nt.getJulianDay(), 2440000);

    Timestamp tsFetched = NanoTimeUtils.getTimestamp(nt);
    Assert.assertEquals(tsFetched, ts);

    //check if 30 Julian Days between Jan 1, 2005 and Jan 31, 2005.
    Calendar cal1 = Calendar.getInstance();
    cal1.set(Calendar.YEAR,  2005);
    cal1.set(Calendar.MONTH, Calendar.JANUARY);
    cal1.set(Calendar.DAY_OF_MONTH, 1);
    cal1.set(Calendar.HOUR_OF_DAY, 0);
    cal1.setTimeZone(TimeZone.getTimeZone("GMT"));

    Timestamp ts1 = new Timestamp(cal1.getTimeInMillis());
    NanoTime nt1 = NanoTimeUtils.getNanoTime(ts1);

    Timestamp ts1Fetched = NanoTimeUtils.getTimestamp(nt1);
    Assert.assertEquals(ts1Fetched, ts1);

    Calendar cal2 = Calendar.getInstance();
    cal2.set(Calendar.YEAR,  2005);
    cal2.set(Calendar.MONTH, Calendar.JANUARY);
    cal2.set(Calendar.DAY_OF_MONTH, 31);
    cal2.set(Calendar.HOUR_OF_DAY, 0);
    cal2.setTimeZone(TimeZone.getTimeZone("UTC"));

    Timestamp ts2 = new Timestamp(cal2.getTimeInMillis());
    NanoTime nt2 = NanoTimeUtils.getNanoTime(ts2);

    Timestamp ts2Fetched = NanoTimeUtils.getTimestamp(nt2);
    Assert.assertEquals(ts2Fetched, ts2);
    Assert.assertEquals(nt2.getJulianDay() - nt1.getJulianDay(), 30);
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
    Timestamp ts = new Timestamp(cal.getTimeInMillis());
    ts.setNanos(1);

    //(1*60*60 + 1*60 + 1) * 10e9 + 1
    NanoTime nt = NanoTimeUtils.getNanoTime(ts);
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
    ts = new Timestamp(cal.getTimeInMillis());
    ts.setNanos(999999999);

    //(23*60*60 + 59*60 + 59)*10e9 + 999999999
    nt = NanoTimeUtils.getNanoTime(ts);
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
    Timestamp ts2 = new Timestamp(cal2.getTimeInMillis());
    ts2.setNanos(10);

    Calendar cal1 = Calendar.getInstance();
    cal1.set(Calendar.YEAR,  1968);
    cal1.set(Calendar.MONTH, Calendar.MAY);
    cal1.set(Calendar.DAY_OF_MONTH, 23);
    cal1.set(Calendar.HOUR_OF_DAY, 0);
    cal1.set(Calendar.MINUTE, 0);
    cal1.set(Calendar.SECOND, 0);
    cal1.setTimeZone(TimeZone.getTimeZone("GMT"));
    Timestamp ts1 = new Timestamp(cal1.getTimeInMillis());
    ts1.setNanos(1);

    NanoTime n2 = NanoTimeUtils.getNanoTime(ts2);
    NanoTime n1 = NanoTimeUtils.getNanoTime(ts1);

    Assert.assertEquals(n2.getTimeOfDayNanos() - n1.getTimeOfDayNanos(), 600000000009L);
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
    Timestamp ts = new Timestamp(cal.getTimeInMillis());
    ts.setNanos(1);

    /**
     * 17:00 PDT = 00:00 GMT (daylight-savings)
     * (0*60*60 + 1*60 + 1)*10e9 + 1 = 61000000001, or
     *
     * 17:00 PST = 01:00 GMT (if not daylight savings)
     * (1*60*60 + 1*60 + 1)*10e9 + 1 = 3661000000001
     */
    NanoTime nt = NanoTimeUtils.getNanoTime(ts);
    long timeOfDayNanos = nt.getTimeOfDayNanos();
    Assert.assertTrue(timeOfDayNanos == 61000000001L || timeOfDayNanos == 3661000000001L);

    //in both cases, this will be the next day in GMT
    Assert.assertEquals(nt.getJulianDay(), 2440001);
  }

  public void testValues() {
    //exercise a broad range of timestamps close to the present.
    verifyTsString("2011-01-01 01:01:01.111111111");
    verifyTsString("2012-02-02 02:02:02.222222222");
    verifyTsString("2013-03-03 03:03:03.333333333");
    verifyTsString("2014-04-04 04:04:04.444444444");
    verifyTsString("2015-05-05 05:05:05.555555555");
    verifyTsString("2016-06-06 06:06:06.666666666");
    verifyTsString("2017-07-07 07:07:07.777777777");
    verifyTsString("2018-08-08 08:08:08.888888888");
    verifyTsString("2019-09-09 09:09:09.999999999");
    verifyTsString("2020-10-10 10:10:10.101010101");
    verifyTsString("2021-11-11 11:11:11.111111111");
    verifyTsString("2022-12-12 12:12:12.121212121");
    verifyTsString("2023-01-02 13:13:13.131313131");
    verifyTsString("2024-02-02 14:14:14.141414141");
    verifyTsString("2025-03-03 15:15:15.151515151");
    verifyTsString("2026-04-04 16:16:16.161616161");
    verifyTsString("2027-05-05 17:17:17.171717171");
    verifyTsString("2028-06-06 18:18:18.181818181");
    verifyTsString("2029-07-07 19:19:19.191919191");
    verifyTsString("2030-08-08 20:20:20.202020202");
    verifyTsString("2031-09-09 21:21:21.212121212");

    //test some extreme cases.
    verifyTsString("9999-09-09 09:09:09.999999999");
    verifyTsString("0001-01-01 00:00:00.0");
  }

  private void verifyTsString(String tsString) {
    Timestamp ts = Timestamp.valueOf(tsString);
    NanoTime nt = NanoTimeUtils.getNanoTime(ts);
    Timestamp tsFetched = NanoTimeUtils.getTimestamp(nt);
    Assert.assertEquals(tsString, tsFetched.toString());
  }
}
