package org.apache.hadoop.hive.ql.cube.parse;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import junit.framework.Assert;

import org.apache.commons.lang.time.DateUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for cube DateUtil class
 * TestDateUtil.
 *
 */
public class TestDateUtil {
  public static final String[] testpairs = {
    "2013-Jan-01", "2013-Jan-31",
    "2013-Jan-01", "2013-May-31",
    "2013-Jan-01", "2013-Dec-31",
    "2013-Feb-01", "2013-Apr-25",
    "2012-Feb-01", "2013-Feb-01",
    "2011-Feb-01", "2013-Feb-01",
    "2013-Jan-02", "2013-Feb-02",
    "2013-Jan-02", "2013-Mar-02",
  };

  public static final SimpleDateFormat DATE_FMT = new SimpleDateFormat("yyyy-MMM-dd");

  private Date pairs[];

  @Before
  public void setUp() {
    pairs = new Date[testpairs.length];
    for (int i = 0; i < testpairs.length; i++) {
      try {
        pairs[i] = DATE_FMT.parse(testpairs[i]);
      } catch (ParseException e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  public void testMonthsBetween() throws Exception {
    int i = 0;
    Assert.assertEquals("2013-Jan-01 to 2013-Jan-31",
        1, DateUtil.getMonthsBetween(pairs[i],
        DateUtils.round(pairs[i+1], Calendar.MONTH)));

    i+=2;
    Assert.assertEquals("2013-Jan-01 to 2013-May-31",
        5, DateUtil.getMonthsBetween(pairs[i],
        DateUtils.round(pairs[i+1], Calendar.MONTH)));

    i+=2;
    Assert.assertEquals("2013-Jan-01 to 2013-Dec-31",
        12,
        DateUtil.getMonthsBetween(pairs[i], DateUtils.round(pairs[i+1], Calendar.MONTH)));

    i+=2;
    Assert.assertEquals("2013-Feb-01 to 2013-Apr-25",
        2, DateUtil.getMonthsBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals("2012-Feb-01 to 2013-Feb-01",
        12, DateUtil.getMonthsBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals("2011-Feb-01 to 2013-Feb-01",
        24, DateUtil.getMonthsBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals("2013-Jan-02 to 2013-Feb-02",
        0,
        DateUtil.getMonthsBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals( "2013-Jan-02 to 2013-Mar-02",
        1, DateUtil.getMonthsBetween(pairs[i], pairs[i+1]));
  }

  @Test
  public void testQuartersBetween() throws Exception {
    int i = 0;
    Assert.assertEquals("2013-Jan-01 to 2013-Jan-31",
        0, DateUtil.getQuartersBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals("2013-Jan-01 to 2013-May-31",
        1, DateUtil.getQuartersBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals("2013-Jan-01 to 2013-Dec-31",
        4, DateUtil.getQuartersBetween(pairs[i], DateUtils.round(pairs[i+1], Calendar.MONTH)));

    i+=2;
    Assert.assertEquals("2013-Feb-01 to 2013-Apr-25",
        0, DateUtil.getQuartersBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals("2012-Feb-01 to 2013-Feb-01",
        3, DateUtil.getQuartersBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals("2011-Feb-01 to 2013-Feb-01",
        7, DateUtil.getQuartersBetween(pairs[i], pairs[i+1]));
  }


  @Test
  public void testYearsBetween() throws Exception {
    int i = 0;
    Assert.assertEquals( "" + pairs[i] + "->" + pairs[i+1],
        0, DateUtil.getYearsBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals( "" + pairs[i] + "->" + pairs[i+1],
        0, DateUtil.getYearsBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals( "" + pairs[i] + "->" + pairs[i+1],
        1, DateUtil.getYearsBetween(pairs[i],
        DateUtils.round(pairs[i+1], Calendar.MONTH)));

    i+=2;
    Assert.assertEquals( "" + pairs[i] + "->" + pairs[i+1],
        0, DateUtil.getYearsBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals( "" + pairs[i] + "->" + pairs[i+1],
        0, DateUtil.getYearsBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals( "" + pairs[i] + "->" + pairs[i+1],
        1, DateUtil.getYearsBetween(pairs[i], pairs[i+1]));
  }

  @Test
  public void testWeeksBetween() throws Exception {
    int weeks = DateUtil.getWeeksBetween(DATE_FMT.parse("2013-May-26"),
        DATE_FMT.parse("2013-Jun-2"));
    Assert.assertEquals("2013-May-26 to 2013-Jun-2", 1, weeks);

    weeks = DateUtil.getWeeksBetween(DATE_FMT.parse("2013-May-27"),
        DATE_FMT.parse("2013-Jun-1"));
    Assert.assertEquals("2013-May-27 to 2013-Jun-1", 0, weeks);

    weeks = DateUtil.getWeeksBetween(DATE_FMT.parse("2013-May-25"),
        DATE_FMT.parse("2013-Jun-2"));
    Assert.assertEquals("2013-May-25 to 2013-Jun-1", 1, weeks);

    weeks = DateUtil.getWeeksBetween(DATE_FMT.parse("2013-May-26"),
        DATE_FMT.parse("2013-Jun-9"));
    Assert.assertEquals("2013-May-26 to 2013-Jun-8", 2, weeks);


    weeks = DateUtil.getWeeksBetween(DATE_FMT.parse("2013-May-26"),
        DATE_FMT.parse("2013-Jun-10"));
    Assert.assertEquals("2013-May-26 to 2013-Jun-10", 2, weeks);
  }

}
