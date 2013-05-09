package org.apache.hadoop.hive.ql.cube.processors;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCubeDriver {

  private Configuration conf;
  private CubeDriver driver;

  static Date now;
  static Date twodaysBack;
  static Date twoMonthsBack;

  @BeforeClass
  public static void setup() throws Exception {
    CubeTestSetup setup = new CubeTestSetup();
    setup.createSources();
    Calendar cal = Calendar.getInstance();
    now = cal.getTime();
    System.out.println("Test now:" + now);
    cal.add(Calendar.DAY_OF_MONTH, -2);
    twodaysBack = cal.getTime();
    System.out.println("Test twodaysBack:" + twodaysBack);
    cal = Calendar.getInstance();
    cal.add(Calendar.MONTH, -2);
    twoMonthsBack = cal.getTime();
    System.out.println("Test twoMonthsBack:" + twoMonthsBack);

  }

  @Before
  public void setupDriver() throws Exception {
    conf = new Configuration();
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
  }

  public static String HOUR_FMT = "yyyy-MM-dd HH";
  public static final SimpleDateFormat HOUR_PARSER = new SimpleDateFormat(
      HOUR_FMT);

  public static String MONTH_FMT = "yyyy-MM";
  public static final SimpleDateFormat MONTH_PARSER = new SimpleDateFormat(
      MONTH_FMT);

  public static String getDateUptoHours(Date dt) {
    return HOUR_PARSER.format(dt);
  }

  public static String getDateUptoMonth(Date dt) {
    return MONTH_PARSER.format(dt);
  }

  @Test
  public void testQueryWithNow() throws Exception {
    Throwable th = null;
    try {
      String hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube"
          + " where time_range_in('NOW - 2DAYS', 'NOW')");
    } catch (SemanticException e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertNotNull(th);
  }

  @Test
  public void testCandidateTables() throws Exception {
    Throwable th = null;
    try {
      String hqlQuery = driver.compileCubeQuery("select dim1, SUM(msr2)" +
          " from testCube" +
          " where time_range_in('" + getDateUptoHours(twodaysBack)
          + "','" + getDateUptoHours(now) + "')");
    } catch (SemanticException e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertNotNull(th);
  }

  @Test
  public void testCubeWhereQuery() throws Exception {
    System.out.println("Test from:" + getDateUptoHours(twodaysBack) + " to:" +
        getDateUptoHours(now));
    //String expected = " sum( testcube.msr2 ) FROM  C1_testfact_HOURLY
    //testcube  WHERE " + whereClause(HOURLY) + " UNION " +
    // SELECT sum( testcube.msr2 ) FROM  C1_testfact_DAILY testcube
    //WHERE + whereClause(DAILY)

    String hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
        " where time_range_in('" + getDateUptoHours(twodaysBack)
        + "','" + getDateUptoHours(now) + "')");
    System.out.println("cube hql:" + hqlQuery);
    //Assert.assertEquals(queries[1], cubeql.toHQL());
  }

  @Test
  public void testCubeJoinQuery() throws Exception {
    //String expected = "select SUM(testCube.msr2) from "
    String hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube"
        + " join citytable on testCube.cityid = citytable.id"
        + " where time_range_in('" + getDateUptoHours(twodaysBack)
        + "','" + getDateUptoHours(now) + "')");
    System.out.println("cube hql:" + hqlQuery);

    hqlQuery = driver.compileCubeQuery("select statetable.name, SUM(msr2) from"
        + " testCube"
        + " join citytable on testCube.cityid = citytable.id"
        + " left outer join statetable on statetable.id = citytable.stateid"
        + " right outer join ziptable on citytable.zipcode = ziptable.code"
        + " where time_range_in('" + getDateUptoHours(twodaysBack)
        + "','" + getDateUptoHours(now) + "')");
    System.out.println("cube hql:" + hqlQuery);

    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube"
        + " join countrytable on testCube.countryid = countrytable.id"
        + " where time_range_in('" + getDateUptoMonth(twoMonthsBack)
        + "','" + getDateUptoMonth(now) + "')");
    System.out.println("cube hql:" + hqlQuery);

  }

  @Test
  public void testCubeGroupbyQuery() throws Exception {
    Calendar cal = Calendar.getInstance();
    Date now = cal.getTime();
    System.out.println("Test now:" + now);
    cal.add(Calendar.DAY_OF_MONTH, -2);
    Date twodaysBack = cal.getTime();
    System.out.println("Test twodaysBack:" + twodaysBack);
    System.out.println("Test from:" + getDateUptoHours(twodaysBack) + " to:" +
        getDateUptoHours(now));
    //String expected = "select SUM(testCube.msr2) from "
    String hqlQuery = driver.compileCubeQuery("select name, SUM(msr2) from" +
        " testCube"
        + " join citytable on testCube.cityid = citytable.id"
        + " where time_range_in('" + getDateUptoHours(twodaysBack)
        + "','" + getDateUptoHours(now) + "')");
    System.out.println("cube hql:" + hqlQuery);

    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube"
        + " join citytable on testCube.cityid = citytable.id"
        + " where time_range_in('" + getDateUptoHours(twodaysBack)
        + "','" + getDateUptoHours(now) + "')"
        + " group by name");
    System.out.println("cube hql:" + hqlQuery);

    hqlQuery = driver.compileCubeQuery("select cityid, SUM(msr2) from testCube"
        + " where time_range_in('" + getDateUptoHours(twodaysBack)
        + "','" + getDateUptoHours(now) + "')");
    System.out.println("cube hql:" + hqlQuery);

    hqlQuery = driver.compileCubeQuery("select round(cityid), SUM(msr2) from" +
        " testCube"
        + " where time_range_in('" + getDateUptoHours(twodaysBack)
        + "','" + getDateUptoHours(now) + "')");
    System.out.println("cube hql:" + hqlQuery);

    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube"
        + " where time_range_in('" + getDateUptoHours(twodaysBack)
        + "','" + getDateUptoHours(now) + "')"
        + " group by round(zipcode)");

    hqlQuery = driver.compileCubeQuery("select round(cityid), SUM(msr2) from" +
        " testCube"
        + " where time_range_in('" + getDateUptoHours(twodaysBack)
        + "','" + getDateUptoHours(now) + "')"
        + " group by zipcode");
    System.out.println("cube hql:" + hqlQuery);

    hqlQuery = driver.compileCubeQuery("select cityid, SUM(msr2) from testCube"
        + " where time_range_in('" + getDateUptoHours(twodaysBack)
        + "','" + getDateUptoHours(now) + "')"
        + " group by round(zipcode)");
    System.out.println("cube hql:" + hqlQuery);

    // TODO to be tested after aggregate resolver
    /*hqlQuery = driver.compileCubeQuery("select cityid, msr2 from testCube"
        + " where time_range_in('" + getDateUptoHours(twodaysBack)
        + "','" + getDateUptoHours(now) + "')"
        + " group by round(zipcode)");
    System.out.println("cube hql:" + hqlQuery);
     */
  }

  @Test
  public void testCubeQueryWithAilas() throws Exception {
    String hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
        " where time_range_in('" + getDateUptoHours(twodaysBack)
        + "','" + getDateUptoHours(now) + "')");
    System.out.println("cube hql:" + hqlQuery);
    hqlQuery = driver.compileCubeQuery("select SUM(msr2) m2 from testCube" +
        " where time_range_in('" + getDateUptoHours(twodaysBack)
        + "','" + getDateUptoHours(now) + "')");
    System.out.println("cube hql:" + hqlQuery);
    try {
      hqlQuery = driver.compileCubeQuery("select name, SUM(msr2) from testCube"
          + " join citytable" +
          " where time_range_in('" + getDateUptoHours(twodaysBack)
          + "','" + getDateUptoHours(now) + "')" +
          " group by name");
      System.out.println("cube hql:" + hqlQuery);
    } catch (SemanticException e) {
      e.printStackTrace();
    }
    hqlQuery = driver.compileCubeQuery("select SUM(mycube.msr2) from" +
        " testCube mycube" +
        " where time_range_in('" + getDateUptoHours(twodaysBack)
        + "','" + getDateUptoHours(now) + "')");
    System.out.println("cube hql:" + hqlQuery);
    //Assert.assertEquals(queries[1], cubeql.toHQL());

    hqlQuery = driver.compileCubeQuery("select SUM(testCube.msr2) from" +
        " testCube" +
        " where time_range_in('" + getDateUptoHours(twodaysBack)
        + "','" + getDateUptoHours(now) + "')");
    System.out.println("cube hql:" + hqlQuery);

    hqlQuery = driver.compileCubeQuery("select mycube.msr2 m2 from testCube" +
        " mycube" +
        " where time_range_in('" + getDateUptoHours(twodaysBack)
        + "','" + getDateUptoHours(now) + "')");
    System.out.println("cube hql:" + hqlQuery);

    hqlQuery = driver.compileCubeQuery("select testCube.msr2 m2 from testCube" +
        " where time_range_in('" + getDateUptoHours(twodaysBack)
        + "','" + getDateUptoHours(now) + "')");
    System.out.println("cube hql:" + hqlQuery);
  }

  @Test
  public void testCubeWhereQueryForMonth() throws Exception {
    System.out.println("Test from:" + getDateUptoHours(twoMonthsBack) + " to:" +
        getDateUptoHours(now));
    String hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
        " where time_range_in('" + getDateUptoHours(twoMonthsBack)
        + "','" + getDateUptoHours(now) + "')");
    System.out.println("cube hql:" + hqlQuery);
    //Assert.assertEquals(queries[1], cubeql.toHQL());

    // TODO this should consider only two month partitions. Month weight needs
    // to be fixed.
    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
        " where time_range_in('" + getDateUptoMonth(twoMonthsBack)
        + "','" + getDateUptoMonth(now) + "')");
    System.out.println("cube hql:" + hqlQuery);
  }

  @Test
  public void testDimensionQueryWithMultipleStorages() throws Exception {
    String hqlQuery = driver.compileCubeQuery("select name, stateid from" +
        " citytable");
    System.out.println("cube hql:" + hqlQuery);

    hqlQuery = driver.compileCubeQuery("select name, c.stateid from citytable c");
    System.out.println("cube hql:" + hqlQuery);

    conf.set(HiveConf.ConfVars.HIVE_DRIVER_SUPPORTED_STORAGES.toString(), "C2");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select name, stateid from citytable");
    System.out.println("cube hql:" + hqlQuery);
    //Assert.assertEquals(queries[1], cubeql.toHQL());

    conf.set(HiveConf.ConfVars.HIVE_DRIVER_SUPPORTED_STORAGES.toString(), "C1");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select name, stateid from citytable");
    System.out.println("cube hql:" + hqlQuery);

    conf.set(HiveConf.ConfVars.HIVE_DRIVER_SUPPORTED_STORAGES.toString(), "");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select name, stateid from citytable");
    System.out.println("cube hql:" + hqlQuery);
  }

  @Test
  public void testLimitQueryOnDimension() throws Exception {
    String hqlQuery = driver.compileCubeQuery("select name, stateid from" +
        " citytable limit 100");
    System.out.println("cube hql:" + hqlQuery);
    //Assert.assertEquals(queries[1], cubeql.toHQL());
    conf.set(HiveConf.ConfVars.HIVE_DRIVER_SUPPORTED_STORAGES.toString(), "C2");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select name, stateid from citytable " +
        "limit 100");
    System.out.println("cube hql:" + hqlQuery);
    conf.set(HiveConf.ConfVars.HIVE_DRIVER_SUPPORTED_STORAGES.toString(), "C1");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select name, stateid from citytable" +
        " limit 100");
    System.out.println("cube hql:" + hqlQuery);
  }

  @Test
  public void testAggregateResolver() throws Exception {
    String timeRange = " where  time_range_in('2013-05-01', '2013-05-03')";
    System.out.println("#$AGGREGATE_RESOLVER_ TIME_RANGE:" + timeRange);
    String q1 = "SELECT cityid, testCube.msr2 from testCube " + timeRange;
    String q2 = "SELECT cityid, testCube.msr2 * testCube.msr2 from testCube "
        + timeRange;
    String q3 = "SELECT cityid, sum(testCube.msr2) from testCube " + timeRange;
    String q4 = "SELECT cityid, sum(testCube.msr2) from testCube "  + timeRange
        + " having testCube.msr2 > 100";
    String q5 = "SELECT cityid, testCube.msr2 from testCube " + timeRange
        + " having testCube.msr2 + testCube.msr2 > 100";
    String q6 = "SELECT cityid, testCube.msr2 from testCube " + timeRange
        + " having testCube.msr2 > 100 AND testCube.msr2 < 100";
    String q7 = "SELECT cityid, sum(testCube.msr2) from testCube " + timeRange
        + " having (testCube.msr2 > 100) OR (testcube.msr2 < 100 AND " +
        "SUM(testcube.msr3) > 1000)";

    String tests[] = {q1, q2, q3, q4, q5, q6, q7};

    try {
      for (int i = 0; i < tests.length; i++) {
        String hql = driver.compileCubeQuery(tests[i]);
        System.out.println("cube hql:" + hql);
      }
    } catch (SemanticException e) {
      e.printStackTrace();
    }

    String q8 = "SELECT cityid, testCube.noAggrMsr FROM testCube " + timeRange;
    try {
      // Should throw exception in aggregate resolver because noAggrMsr does
      //not have a default aggregate defined.s
      String hql = driver.compileCubeQuery(q8);
      Assert.assertTrue("Should not reach here", false);
    } catch (SemanticException exc) {
      Assert.assertNotNull(exc);
      exc.printStackTrace();
    }
  }

  @Test
  public void testColumnAmbiguity() throws Exception {
    String timeRange = " where  time_range_in('2013-05-01', '2013-05-03')";
    String query = "SELECT ambigdim1, testCube.msr1 FROM testCube " + timeRange;

    try {
      String hql = driver.compileCubeQuery(query);
      Assert.assertTrue("Should not reach here", false);
    } catch (SemanticException exc) {
      Assert.assertNotNull(exc);
      exc.printStackTrace();
    }

    String q2 = "SELECT ambigdim2, testCube.msr1 FROM testCube " + timeRange;
    try {
      String hql = driver.compileCubeQuery(q2);
      Assert.assertTrue("Should not reach here", false);
    } catch (SemanticException exc) {
      Assert.assertNotNull(exc);
      exc.printStackTrace();
    }


  }

  @Test
  public void testMissingColumnValidation() throws Exception {
    String timeRange = " where  time_range_in('2013-05-01', '2013-05-03')";

    try {
      // this query should go through
      String q1Hql =
          driver.compileCubeQuery("SELECT cityid, msr2 from testCube " +
              timeRange);
    } catch (SemanticException exc) {
      exc.printStackTrace();
      Assert.assertTrue("Exception not expected here", false);
    }

    try {
      // this query should through exception because invalidMsr is invalid
      String q2Hql =
          driver.compileCubeQuery("SELECT cityid, invalidMsr from testCube " +
              timeRange);
      Assert.assertTrue("Should not reach here", false);
    } catch (SemanticException exc) {
      exc.printStackTrace();
      Assert.assertNotNull(exc);
    }
  }

  @Test
  public void testTimeRangeValidation() throws Exception {
    String timeRange1 = " where  time_range_in('2013-05-01', '2013-05-03')";

    try {
      String hql = driver.compileCubeQuery("SELECT cityid, testCube.msr2 from" +
          " testCube " + timeRange1);
    } catch (SemanticException exc) {
      exc.printStackTrace(System.out);
      Assert.assertTrue("Exception not expected here", false);
    }

    // swap to and from -
    String timeRange2 = " where  time_range_in('2013-05-03', '2013-05-01')";
    try {
      // this should throw exception because from date is after to date
      String hql = driver.compileCubeQuery("SELECT cityid, testCube.msr2 from" +
          " testCube " + timeRange2);
      Assert.assertTrue("Should not reach here", false);
    } catch (SemanticException exc) {
      exc.printStackTrace(System.out);
      Assert.assertNotNull(exc);
    }
  }

}
