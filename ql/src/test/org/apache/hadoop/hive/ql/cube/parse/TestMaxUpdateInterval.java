package org.apache.hadoop.hive.ql.cube.parse;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.junit.Assert;
import org.junit.Test;

/*
 * Unit test for maxUpdateIntervalIn method in CubeFactTable
 */
public class TestMaxUpdateInterval<periods> {
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
    CubeTestSetup setup = new CubeTestSetup();
    setup.createSources();

    CubeMetastoreClient client =  CubeMetastoreClient.getInstance(
        new HiveConf(this.getClass()));

    CubeFactTable fact = client.getFactTable("testFact");
    List<UpdatePeriod> allPeriods = new ArrayList<UpdatePeriod>();
    for (List<UpdatePeriod >periods : fact.getUpdatePeriods().values()) {
      allPeriods.addAll(periods);
    }

    int i = 0;
    Assert.assertEquals(UpdatePeriod.DAILY, fact.maxIntervalInRange(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals(UpdatePeriod.MONTHLY, fact.maxIntervalInRange(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals(UpdatePeriod.QUARTERLY, fact.maxIntervalInRange(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals(UpdatePeriod.MONTHLY, fact.maxIntervalInRange(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals(UpdatePeriod.QUARTERLY, fact.maxIntervalInRange(pairs[i], pairs[i+1]));

    i+=2;
    Assert.assertEquals(UpdatePeriod.YEARLY, fact.maxIntervalInRange(pairs[i], pairs[i+1]));
  }

}
