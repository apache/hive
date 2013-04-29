package org.apache.hadoop.hive.ql.cube.parse;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import junit.framework.Assert;

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
    "2011-Feb-01", "2013-Feb-01"
  };

  public static final SimpleDateFormat DATE_FMT = new SimpleDateFormat("yyyy-MMM-dd");

  private final Date pairs[];

  public TestDateUtil() {
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
    Assert.assertEquals(0, DateUtil.getMonthsBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals(3, DateUtil.getMonthsBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals(10, DateUtil.getMonthsBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals(1, DateUtil.getMonthsBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals(11, DateUtil.getMonthsBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals(23, DateUtil.getMonthsBetween(pairs[i], pairs[i+1]));
  }

  @Test
  public void testQuartersBetween() throws Exception {
    int i = 0;
    Assert.assertEquals(0, DateUtil.getQuartersBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals(0, DateUtil.getQuartersBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals(2, DateUtil.getQuartersBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals(0, DateUtil.getQuartersBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals(3, DateUtil.getQuartersBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals(7, DateUtil.getQuartersBetween(pairs[i], pairs[i+1]));
  }


  @Test
  public void testYearsBetween() throws Exception {
    int i = 0;
    Assert.assertEquals(0, DateUtil.getYearsBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals(0, DateUtil.getYearsBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals(0, DateUtil.getYearsBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals(0, DateUtil.getYearsBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals(0, DateUtil.getYearsBetween(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals(1, DateUtil.getYearsBetween(pairs[i], pairs[i+1]));
  }

}
