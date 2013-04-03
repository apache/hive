package org.apache.hadoop.hive.ql.cube.processors;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCubeDriver {

  private static CubeDriver driver;

  @BeforeClass
  public static void setup() throws Exception {
    CubeTestSetup setup = new CubeTestSetup();
    setup.createSources();
    driver = new CubeDriver(new HiveConf());
  }

  public static String HOUR_FMT = "yyyy-MM-dd HH";
  public static final SimpleDateFormat HOUR_PARSER = new SimpleDateFormat(HOUR_FMT);

  public static String getDateUptoHours(Date dt) {
    return HOUR_PARSER.format(dt);
  }

  @Test
  public void testSimpleQuery1() throws Exception {
    Throwable th = null;
    try {
      String hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
    		" where time_range_in('NOW - 2DAYS', 'NOW')");
    } catch (SemanticException e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertNotNull(th);
  }

  @Test
  public void testSimpleQuery2() throws Exception {
    Calendar cal = Calendar.getInstance();
    Date now = cal.getTime();
    System.out.println("Test now:" + now);
    cal.add(Calendar.DAY_OF_MONTH, -2);
    Date twodaysBack = cal.getTime();
    System.out.println("Test twodaysBack:" + twodaysBack);
    System.out.println("Test from:" + getDateUptoHours(twodaysBack) + " to:" + getDateUptoHours(now));
    String hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
        " where time_range_in('" + getDateUptoHours(twodaysBack)
        + "','" + getDateUptoHours(now) + "')");
    System.out.println("cube hql:" + hqlQuery);
    //Assert.assertEquals(queries[1], cubeql.toHQL());
  }

}
