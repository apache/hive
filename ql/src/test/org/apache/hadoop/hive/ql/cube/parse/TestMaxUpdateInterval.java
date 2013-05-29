package org.apache.hadoop.hive.ql.cube.parse;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.junit.Assert;
import org.junit.Test;

/*
 * Unit test for maxUpdateIntervalIn method in CubeFactTable
 */
public class TestMaxUpdateInterval {
  public static final String[] testpairs = {
    "2013-Jan-01", "2013-Jan-31",
    "2013-Jan-01", "2013-May-31",
    "2013-Jan-01", "2013-Dec-31",
    "2013-Feb-01", "2013-Apr-25",
    "2012-Feb-01", "2013-Feb-01",
    "2011-Feb-01", "2013-Feb-01",
    "2013-Feb-01", "2013-Feb-21",
    "2013-Feb-01", "2013-Feb-4"
  };

  public static final SimpleDateFormat DATE_FMT = new SimpleDateFormat(
      "yyyy-MMM-dd");

  private final Date pairs[];

  public TestMaxUpdateInterval () {
    pairs = new Date[testpairs.length];
    for (int i = 0; i < testpairs.length; i++) {
      try {
        pairs[i] = DATE_FMT.parse(testpairs[i]);
        System.out.println(pairs[i].toString());
      } catch (ParseException e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  public void testMaxUpdatePeriodInInterval() throws Exception {
    Set<UpdatePeriod> allPeriods = new HashSet<UpdatePeriod>();
    allPeriods.addAll(Arrays.asList(UpdatePeriod.values()));

    int i = 0;
    Assert.assertEquals("2013-Jan-01 to 2013-Jan-31",
        UpdatePeriod.WEEKLY, CubeFactTable.maxIntervalInRange(
        pairs[i], pairs[i+1], allPeriods));

    i+=2;
    Assert.assertEquals("2013-Jan-01 to 2013-May-31",
        UpdatePeriod.QUARTERLY, CubeFactTable.maxIntervalInRange(
        pairs[i], pairs[i+1], allPeriods));

    i+=2;
    Assert.assertEquals("2013-Jan-01 to 2013-Dec-31",
        UpdatePeriod.QUARTERLY, CubeFactTable.
        maxIntervalInRange(pairs[i], pairs[i+1], allPeriods));

    i+=2;
    Assert.assertEquals("2013-Feb-01 to 2013-Apr-25",
        UpdatePeriod.MONTHLY, CubeFactTable.maxIntervalInRange(
        pairs[i], pairs[i+1], allPeriods));

    i+=2;
    Assert.assertEquals("2012-Feb-01 to 2013-Feb-01",
        UpdatePeriod.QUARTERLY, CubeFactTable.
        maxIntervalInRange(pairs[i], pairs[i+1], allPeriods));

    i+=2;
    Assert.assertEquals("2011-Feb-01 to 2013-Feb-01",
        UpdatePeriod.YEARLY, CubeFactTable.maxIntervalInRange(
        pairs[i], pairs[i+1], allPeriods));

    i+=2;
    Assert.assertEquals("2013-Feb-01 to 2013-Feb-21",
        UpdatePeriod.WEEKLY, CubeFactTable.maxIntervalInRange(
        pairs[i], pairs[i+1], allPeriods));

    i+=2;
    Assert.assertEquals("2013-Feb-01 to 2013-Feb-4",
        UpdatePeriod.DAILY, CubeFactTable.maxIntervalInRange(
        pairs[i], pairs[i+1], allPeriods));
  }

}
