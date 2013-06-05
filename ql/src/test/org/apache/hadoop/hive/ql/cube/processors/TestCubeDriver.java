package org.apache.hadoop.hive.ql.cube.processors;

import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.now;
import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.twoMonthsBack;
import static org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup.twodaysBack;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.metadata.Storage;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryConstants;
import org.apache.hadoop.hive.ql.cube.parse.CubeTestSetup;
import org.apache.hadoop.hive.ql.cube.parse.DateUtil;
import org.apache.hadoop.hive.ql.cube.parse.StorageTableResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCubeDriver {

  private Configuration conf;
  private CubeDriver driver;
  private final String cubeName = "testcube";
  private final String twoDaysRange = "time_range_in('" + getDateUptoHours(
      twodaysBack) + "','" + getDateUptoHours(now) + "')";
  private final String twoMonthsRangeUptoHours = "time_range_in('" +
      getDateUptoHours(twoMonthsBack) + "','" + getDateUptoHours(now) + "')";
  private final String twoMonthsRangeUptoMonth = "time_range_in('" +
      getDateUptoMonth(twoMonthsBack) + "','" + getDateUptoMonth(now) + "')";

  @BeforeClass
  public static void setup() throws Exception {
    CubeTestSetup setup = new CubeTestSetup();
    setup.createSources();
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
      driver.compileCubeQuery("select SUM(msr2) from testCube where" +
        " time_range_in('NOW - 2DAYS', 'NOW')");
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
      driver.compileCubeQuery("select dim12, SUM(msr2) from testCube" +
        " where " + twoDaysRange);
    } catch (SemanticException e) {
      th = e;
      e.printStackTrace();
    }
    Assert.assertNotNull(th);
    try {
      // this query should through exception because invalidMsr is invalid
      driver.compileCubeQuery("SELECT cityid, invalidMsr from testCube " +
        " where " + twoDaysRange);
      Assert.assertTrue("Should not reach here", false);
    } catch (SemanticException exc) {
      exc.printStackTrace();
      Assert.assertNotNull(exc);
    }

  }

  public String getExpectedQuery(String cubeName, String selExpr,
      String whereExpr, String postWhereExpr,
      Map<String, String> storageTableToWhereClause) {
    StringBuilder expected = new StringBuilder();
    int numTabs = storageTableToWhereClause.size();
    int i = 0;
    for (Map.Entry<String, String> entry : storageTableToWhereClause.entrySet())
    {
      String storageTable = entry.getKey();
      expected.append(selExpr);
      expected.append(storageTable);
      expected.append(" ");
      expected.append(cubeName);
      expected.append(" WHERE ");
      expected.append("(");
      if (whereExpr != null) {
        expected.append(whereExpr);
        expected.append(" AND ");
      }
      expected.append(entry.getValue());
      expected.append(")");
      if (postWhereExpr != null) {
        expected.append(postWhereExpr);
      }
      if (i < numTabs -1) {
        expected.append(" UNION ALL ");
      }
      i++;
    }
    return expected.toString();
  }

  public String getExpectedQuery(String cubeName, String selExpr,
      String joinExpr, String whereExpr, String postWhereExpr,
      List<String> joinWhereConds,
      Map<String, String> storageTableToWhereClause) {
    StringBuilder expected = new StringBuilder();
    int numTabs = storageTableToWhereClause.size();
    int i = 0;
    for (Map.Entry<String, String> entry : storageTableToWhereClause.entrySet())
    {
      String storageTable = entry.getKey();
      expected.append(selExpr);
      expected.append(storageTable);
      expected.append(" ");
      expected.append(cubeName);
      expected.append(joinExpr);
      expected.append(" WHERE ");
      expected.append("(");
      if (whereExpr != null) {
        expected.append(whereExpr);
        expected.append(" AND ");
      }
      expected.append(entry.getValue());
      if (joinWhereConds != null) {
        for (String joinEntry : joinWhereConds) {
          expected.append(" AND ");
          expected.append(joinEntry);
        }
      }
      expected.append(")");
      if (postWhereExpr != null) {
        expected.append(postWhereExpr);
      }
      if (i < numTabs -1) {
        expected.append(" UNION ALL ");
      }
      i++;
    }
    return expected.toString();
  }

  private Map<String, String> getWhereForDailyAndHourly2days(String cubeName,
      String hourlyTable, String dailyTable) {
    Map<String, String> storageTableToWhereClause =
        new LinkedHashMap<String, String>();
    if (!CubeTestSetup.isZerothHour()) {
      List<String> parts = new ArrayList<String>();
      addParts(parts, UpdatePeriod.HOURLY, twodaysBack,
          DateUtil.getCeilDate(twodaysBack, UpdatePeriod.DAILY));
      addParts(parts, UpdatePeriod.HOURLY,
          DateUtil.getFloorDate(now, UpdatePeriod.DAILY),
          DateUtil.getFloorDate(now, UpdatePeriod.HOURLY));
      storageTableToWhereClause.put(hourlyTable,
          StorageTableResolver.getWherePartClause(cubeName, parts));
    }
    List<String> parts = new ArrayList<String>();
    addParts(parts, UpdatePeriod.DAILY, DateUtil.getCeilDate(
        twodaysBack, UpdatePeriod.DAILY),
        DateUtil.getFloorDate(now, UpdatePeriod.DAILY));
    storageTableToWhereClause.put(dailyTable,
        StorageTableResolver.getWherePartClause(cubeName, parts));
    return storageTableToWhereClause;
  }

  private Map<String, String> getWhereForMonthlyDailyAndHourly2months(
      String hourlyTable, String dailyTable, String monthlyTable) {
    Map<String, String> storageTableToWhereClause =
        new LinkedHashMap<String, String>();
    if (!CubeTestSetup.isZerothHour()) {
      List<String> parts = new ArrayList<String>();
      addParts(parts, UpdatePeriod.HOURLY, twoMonthsBack,
          DateUtil.getCeilDate(twoMonthsBack, UpdatePeriod.DAILY));
      addParts(parts, UpdatePeriod.HOURLY,
          DateUtil.getFloorDate(now, UpdatePeriod.DAILY),
          DateUtil.getFloorDate(now, UpdatePeriod.HOURLY));
      storageTableToWhereClause.put(hourlyTable,
          StorageTableResolver.getWherePartClause(cubeName, parts));
    }
    if (!CubeTestSetup.isFirstDayOfMonth()) {
      List<String> parts = new ArrayList<String>();
      addParts(parts, UpdatePeriod.DAILY, DateUtil.getCeilDate(
          twoMonthsBack, UpdatePeriod.DAILY),
          DateUtil.getCeilDate(twoMonthsBack, UpdatePeriod.MONTHLY));
      addParts(parts, UpdatePeriod.DAILY,
          DateUtil.getFloorDate(now, UpdatePeriod.MONTHLY),
          DateUtil.getFloorDate(now, UpdatePeriod.DAILY));
      storageTableToWhereClause.put(dailyTable,
          StorageTableResolver.getWherePartClause(cubeName, parts));
    }
    List<String> parts = new ArrayList<String>();
    addParts(parts, UpdatePeriod.MONTHLY, DateUtil.getCeilDate(
        twoMonthsBack, UpdatePeriod.MONTHLY),
        DateUtil.getFloorDate(now, UpdatePeriod.MONTHLY));
    storageTableToWhereClause.put(monthlyTable,
        StorageTableResolver.getWherePartClause(cubeName, parts));
    return storageTableToWhereClause;
  }

  private Map<String, String> getWhereForMonthly2months(String monthlyTable) {
    Map<String, String> storageTableToWhereClause =
        new LinkedHashMap<String, String>();
    List<String> parts = new ArrayList<String>();
    addParts(parts, UpdatePeriod.MONTHLY,
        twoMonthsBack,
        DateUtil.getFloorDate(now, UpdatePeriod.MONTHLY));
    storageTableToWhereClause.put(monthlyTable,
        StorageTableResolver.getWherePartClause(cubeName, parts));
    return storageTableToWhereClause;
  }

  private Map<String, String> getWhereForHourly2days(String hourlyTable) {
    Map<String, String> storageTableToWhereClause =
        new LinkedHashMap<String, String>();
    List<String> parts = new ArrayList<String>();
    addParts(parts, UpdatePeriod.HOURLY, twodaysBack,
        DateUtil.getFloorDate(now, UpdatePeriod.HOURLY));
    storageTableToWhereClause.put(hourlyTable,
        StorageTableResolver.getWherePartClause(cubeName, parts));
    return storageTableToWhereClause;
  }

  private void addParts(List<String> partitions, UpdatePeriod updatePeriod,
      Date from, Date to) {
    String fmt = updatePeriod.format();
    Calendar cal = Calendar.getInstance();
    cal.setTime(from);
    Date dt = cal.getTime();
    while (dt.before(to)) {
      String part = new SimpleDateFormat(fmt).format(dt);
      cal.add(updatePeriod.calendarField(), 1);
      partitions.add(part);
      dt = cal.getTime();
    }
  }

  @Test
  public void testCubeExplain() throws Exception {
    String hqlQuery = driver.compileCubeQuery("explain select SUM(msr2) from " +
      "testCube where " + twoDaysRange);
    String expected = "EXPLAIN " + getExpectedQuery(cubeName,
      "select sum(testcube.msr2) FROM ", null, null,
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact_HOURLY",
        "C1_testfact_DAILY"));
    compareQueries(expected, hqlQuery);
  }

  private void compareQueries(String expected, String actual) {
    expected = expected.replaceAll("\\W", "");
    actual = actual.replaceAll("\\W", "");
    Assert.assertTrue(expected.equalsIgnoreCase(actual));
  }
  @Test
  public void testCubeWhereQuery() throws Exception {
    String hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
      " where " + twoDaysRange);
    String expected = getExpectedQuery(cubeName,
      "select sum(testcube.msr2) FROM ", null, null,
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact_HOURLY",
        "C1_testfact_DAILY"));
    compareQueries(expected, hqlQuery);

    // Test with partition existence
    conf.setBoolean(CubeQueryConstants.FAIL_QUERY_ON_PARTIAL_DATA, true);
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      null, null, getWhereForHourly2days("c1_testfact2_hourly"));
    compareQueries(expected, hqlQuery);
    conf.setBoolean(CubeQueryConstants.FAIL_QUERY_ON_PARTIAL_DATA, false);

    // Tests for valid tables
    conf.set(CubeQueryConstants.VALID_FACT_TABLES, "testFact");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      null, null, getWhereForDailyAndHourly2days(cubeName, "C1_testfact_HOURLY",
        "C1_testfact_DAILY"));
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConstants.DRIVER_SUPPORTED_STORAGES, "C2");
    conf.set(CubeQueryConstants.VALID_FACT_TABLES, "testFact");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      null, null, getWhereForDailyAndHourly2days(cubeName, "C2_testfact_HOURLY",
        "C2_testfact_DAILY"));
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConstants.DRIVER_SUPPORTED_STORAGES, "C1");
    conf.set(CubeQueryConstants.VALID_FACT_TABLES, "testFact2");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      null, null, getWhereForHourly2days("c1_testfact2_hourly"));
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConstants.VALID_FACT_TABLES, "");
    conf.set(CubeQueryConstants.DRIVER_SUPPORTED_STORAGES, "C1");
    conf.set(CubeQueryConstants.VALID_STORAGE_FACT_TABLES,
        "C1_testFact2_HOURLY");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      null, null, getWhereForHourly2days("c1_testfact2_hourly"));
    compareQueries(expected, hqlQuery);


    conf.set(CubeQueryConstants.VALID_STORAGE_FACT_TABLES, "C1_testFact_HOURLY");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      null, null, getWhereForHourly2days("c1_testfact_hourly"));
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConstants.DRIVER_SUPPORTED_STORAGES, "C2");
    conf.set(CubeQueryConstants.VALID_FACT_TABLES, "");
    conf.set(CubeQueryConstants.VALID_STORAGE_FACT_TABLES, "C2_testFact_HOURLY");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      null, null, getWhereForHourly2days("c2_testfact_hourly"));
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testCubeJoinQuery() throws Exception {
    String hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube"
      + " join citytable on testCube.cityid = citytable.id"
      + " where " + twoDaysRange);
    List<String> joinWhereConds = new ArrayList<String>();
    joinWhereConds.add(StorageTableResolver.getWherePartClause("citytable",
        Storage.getPartitionsForLatest()));
    String expected = getExpectedQuery(cubeName, "select sum(testcube.msr2)" +
      " FROM ", "INNER JOIN c1_citytable citytable ON" +
      " testCube.cityid = citytable.id", null, null, joinWhereConds,
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact_HOURLY",
          "C1_testfact_DAILY"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select statetable.name, SUM(msr2) from"
      + " testCube"
      + " join citytable on testCube.cityid = citytable.id"
      + " left outer join statetable on statetable.id = citytable.stateid"
      + " right outer join ziptable on citytable.zipcode = ziptable.code"
      + " where " + twoDaysRange);
    joinWhereConds = new ArrayList<String>();
    joinWhereConds.add(StorageTableResolver.getWherePartClause("statetable",
        Storage.getPartitionsForLatest()));
    joinWhereConds.add(StorageTableResolver.getWherePartClause("citytable",
        Storage.getPartitionsForLatest()));
    joinWhereConds.add(StorageTableResolver.getWherePartClause("ziptable",
        Storage.getPartitionsForLatest()));
    expected = getExpectedQuery(cubeName, "select statetable.name," +
      " sum(testcube.msr2) FROM ", "INNER JOIN c1_citytable citytable ON" +
      " testCube.cityid = citytable.id LEFT OUTER JOIN c1_statetable statetable"
      + " ON statetable.id = citytable.stateid RIGHT OUTER JOIN c1_ziptable" +
      " ziptable ON citytable.zipcode = ziptable.code", null, " group by" +
      " statetable.name ", joinWhereConds,
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact_HOURLY",
            "C1_testfact_DAILY"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube"
      + " join countrytable on testCube.countryid = countrytable.id"
      + " where " + twoMonthsRangeUptoMonth);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      " INNER JOIN c1_countrytable countrytable ON testCube.countryid = " +
      " countrytable.id", null, null, null,
        getWhereForMonthly2months("c2_testfactmonthly_monthly"));
    compareQueries(expected, hqlQuery);

    try {
      hqlQuery = driver.compileCubeQuery("select name, SUM(msr2) from testCube"
          + " join citytable" + " where " + twoDaysRange + " group by name");
      Assert.assertTrue(false);
    } catch (SemanticException e) {
      e.printStackTrace();
    }

  }

  @Test
  public void testCubeGroupbyQuery() throws Exception {
    String hqlQuery = driver.compileCubeQuery("select name, SUM(msr2) from" +
      " testCube join citytable on testCube.cityid = citytable.id where " +
      twoDaysRange);
    List<String> joinWhereConds = new ArrayList<String>();
    joinWhereConds.add(StorageTableResolver.getWherePartClause("citytable",
        Storage.getPartitionsForLatest()));
    String expected = getExpectedQuery(cubeName, "select citytable.name," +
      " sum(testcube.msr2) FROM ", "INNER JOIN c1_citytable citytable ON" +
      " testCube.cityid = citytable.id", null, " group by citytable.name ",
      joinWhereConds, getWhereForDailyAndHourly2days(cubeName,
         "C1_testfact_HOURLY", "C1_testfact_DAILY"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube"
      + " join citytable on testCube.cityid = citytable.id"
      + " where " + twoDaysRange + " group by name");
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select cityid, SUM(msr2) from testCube"
      + " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select testcube.cityid," +
      " sum(testcube.msr2) FROM ", null, " group by testcube.cityid ",
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact_HOURLY",
        "C1_testfact_DAILY"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select round(cityid), SUM(msr2) from" +
      " testCube where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select round(testcube.cityid)," +
      " sum(testcube.msr2) FROM ", null, " group by round(testcube.cityid) ",
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact_HOURLY",
        "C1_testfact_DAILY"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube"
      + "  where " + twoDaysRange + "group by round(zipcode)");
    expected = getExpectedQuery(cubeName, "select round(testcube.zipcode)," +
      " sum(testcube.msr2) FROM ", null, " group by round(testcube.zipcode) ",
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact_HOURLY",
        "C1_testfact_DAILY"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select round(cityid), SUM(msr2) from" +
      " testCube where " + twoDaysRange + " group by zipcode");
    expected = getExpectedQuery(cubeName, "select testcube.zipcode," +
      " round(testcube.cityid), sum(testcube.msr2) FROM ", null,
      " group by testcube.zipcode, round(testcube.cityid)",
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact_HOURLY",
        "C1_testfact_DAILY"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select cityid, SUM(msr2) from testCube"
      + " where " + twoDaysRange + " group by round(zipcode)");
    expected = getExpectedQuery(cubeName, "select round(testcube.zipcode)," +
      " testcube.cityid, sum(testcube.msr2) FROM ", null,
      " group by round(testcube.zipcode), testcube.cityid",
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact_HOURLY",
        "C1_testfact_DAILY"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select cityid, msr2 from testCube"
      + " where " + twoDaysRange + " group by round(zipcode)");
    expected = getExpectedQuery(cubeName, "select round(testcube.zipcode)," +
      " testcube.cityid, sum(testcube.msr2) FROM ", null,
      " group by round(testcube.zipcode), testcube.cityid",
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact_HOURLY",
        "C1_testfact_DAILY"));
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testCubeQueryWithAilas() throws Exception {
    String hqlQuery = driver.compileCubeQuery("select SUM(msr2) m2 from" +
      " testCube where " + twoDaysRange);
    String expected = getExpectedQuery(cubeName, "select sum(testcube.msr2)" +
      " m2 FROM ", null, null, getWhereForDailyAndHourly2days(cubeName,
        "C1_testfact_HOURLY", "C1_testfact_DAILY"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube mycube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery("mycube", "select sum(mycube.msr2) FROM ", null,
      null, getWhereForDailyAndHourly2days("mycube", "C1_testfact_HOURLY",
        "C1_testfact_DAILY"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select SUM(testCube.msr2) from testCube"
      + " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) FROM ",
      null, null, getWhereForDailyAndHourly2days(cubeName, "C1_testfact_HOURLY",
        "C1_testfact_DAILY"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select mycube.msr2 m2 from testCube" +
      " mycube where " + twoDaysRange);
    expected = getExpectedQuery("mycube", "select sum(mycube.msr2) m2 FROM ",
      null, null, getWhereForDailyAndHourly2days("mycube", "C1_testfact_HOURLY",
        "C1_testfact_DAILY"));
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select testCube.msr2 m2 from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName, "select sum(testcube.msr2) m2 FROM ",
      null, null, getWhereForDailyAndHourly2days(cubeName, "C1_testfact_HOURLY",
        "C1_testfact_DAILY"));
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testCubeWhereQueryForMonth() throws Exception {
    String hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
      " where " + twoMonthsRangeUptoHours);
    String expected = getExpectedQuery(cubeName,
      "select sum(testcube.msr2) FROM ", null, null,
      getWhereForMonthlyDailyAndHourly2months("C1_testfact_HOURLY",
        "C1_testfact_DAILY", "c1_testfact_monthly"));
    compareQueries(expected, hqlQuery);

    conf.setBoolean(CubeQueryConstants.FAIL_QUERY_ON_PARTIAL_DATA, true);
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    try {
      hqlQuery = driver.compileCubeQuery("select SUM(msr2) from testCube" +
        " where " + twoMonthsRangeUptoHours);
      Assert.assertTrue(false);
    } catch (SemanticException e) {
      e.printStackTrace();
    }
    conf.setBoolean(CubeQueryConstants.FAIL_QUERY_ON_PARTIAL_DATA, false);
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));

    // this should consider only two month partitions.
    hqlQuery = driver.compileCubeQuery("select cityid, SUM(msr2) from testCube"
      + " where " + twoMonthsRangeUptoMonth);
    expected = getExpectedQuery(cubeName, "select testcube,cityid," +
      " sum(testcube.msr2) FROM ", null, "group by testcube.cityid",
      getWhereForMonthly2months("c1_testfact_monthly"));
    compareQueries(expected, hqlQuery);
  }

  String getExpectedQuery(String dimName, String selExpr, String postWhereExpr,
      String storageTable, boolean hasPart) {
    StringBuilder expected = new StringBuilder();
    expected.append(selExpr);
    expected.append(storageTable);
    expected.append(" ");
    expected.append(dimName);
    if (hasPart) {
      expected.append(" WHERE ");
      expected.append(StorageTableResolver.getWherePartClause(dimName,
          Storage.getPartitionsForLatest()));
    }
    if (postWhereExpr != null) {
      expected.append(postWhereExpr);
    }
    return expected.toString();
  }

  @Test
  public void testDimensionQueryWithMultipleStorages() throws Exception {
    String hqlQuery = driver.compileCubeQuery("select name, stateid from" +
      " citytable");
    String expected = getExpectedQuery("citytable", "select citytable.name," +
      " citytable.stateid from ", null, "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    hqlQuery = driver.compileCubeQuery("select name, c.stateid from citytable" +
      " c");
    expected = getExpectedQuery("c", "select c.name, c.stateid from ", null,
      "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConstants.DRIVER_SUPPORTED_STORAGES, "C2");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select name, stateid from citytable");
    expected = getExpectedQuery("citytable", "select citytable.name," +
      " citytable.stateid from ", null, "c2_citytable", false);
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConstants.DRIVER_SUPPORTED_STORAGES, "C1");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select name, stateid from citytable");
    expected = getExpectedQuery("citytable", "select citytable.name," +
      " citytable.stateid from ", null, "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConstants.DRIVER_SUPPORTED_STORAGES, "");
    conf.set(CubeQueryConstants.VALID_STORAGE_DIM_TABLES, "C1_citytable");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select name, stateid from citytable");
    expected = getExpectedQuery("citytable", "select citytable.name," +
      " citytable.stateid from ", null, "c1_citytable", true);
    compareQueries(expected, hqlQuery);

    conf.set(CubeQueryConstants.DRIVER_SUPPORTED_STORAGES, "");
    conf.set(CubeQueryConstants.VALID_STORAGE_DIM_TABLES, "C2_citytable");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select name, stateid from citytable");
    expected = getExpectedQuery("citytable", "select citytable.name," +
      " citytable.stateid from ", null, "c2_citytable", false);
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testLimitQueryOnDimension() throws Exception {
    String hqlQuery = driver.compileCubeQuery("select name, stateid from" +
      " citytable limit 100");
    String expected = getExpectedQuery("citytable", "select citytable.name," +
      " citytable.stateid from ", " limit 100", "c1_citytable", true);
    compareQueries(expected, hqlQuery);
    conf.set(CubeQueryConstants.DRIVER_SUPPORTED_STORAGES, "C2");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select name, stateid from citytable " +
      "limit 100");
    expected = getExpectedQuery("citytable", "select citytable.name," +
      "citytable.stateid from ", " limit 100", "c2_citytable", false);
    compareQueries(expected, hqlQuery);
    conf.set(CubeQueryConstants.DRIVER_SUPPORTED_STORAGES, "C1");
    driver = new CubeDriver(new HiveConf(conf, HiveConf.class));
    hqlQuery = driver.compileCubeQuery("select name, stateid from citytable" +
      " limit 100");
    expected = getExpectedQuery("citytable", "select citytable.name," +
      " citytable.stateid from ", " limit 100", "c1_citytable", true);
    compareQueries(expected, hqlQuery);
  }

  @Test
  public void testAggregateResolver() throws Exception {
    String q1 = "SELECT cityid, testCube.msr2 from testCube where "
      + twoDaysRange;
    String q2 = "SELECT cityid, testCube.msr2 * testCube.msr2 from testCube where "
      + twoDaysRange;
    String q3 = "SELECT cityid, sum(testCube.msr2) from testCube where "
      + twoDaysRange;
    String q4 = "SELECT cityid, sum(testCube.msr2) from testCube where "
      + twoDaysRange + " having testCube.msr2 > 100";
    String q5 = "SELECT cityid, testCube.msr2 from testCube where "
      + twoDaysRange + " having testCube.msr2 + testCube.msr2 > 100";
    String q6 = "SELECT cityid, testCube.msr2 from testCube where "
      + twoDaysRange + " having testCube.msr2 > 100 AND testCube.msr2 < 1000";
    String q7 = "SELECT cityid, sum(testCube.msr2) from testCube where "
      + twoDaysRange + " having (testCube.msr2 > 100) OR (testcube.msr2 < 100" +
      " AND SUM(testcube.msr3) > 1000)";

    String expectedq1 = getExpectedQuery(cubeName, "SELECT testcube.cityid," +
      " sum(testCube.msr2) from ", null, "group by testcube.cityid",
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact_HOURLY",
        "C1_testfact_DAILY"));
    String expectedq2 = null;
    String expectedq3 = getExpectedQuery(cubeName, "SELECT testcube.cityid," +
     " sum(testCube.msr2) from ", null, "group by testcube.cityid",
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact_HOURLY",
        "C1_testfact_DAILY"));
    String expectedq4 = getExpectedQuery(cubeName, "SELECT testcube.cityid," +
      " sum(testCube.msr2) from ", null, "group by testcube.cityid having" +
      " sum(testCube.msr2) > 100",
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact_HOURLY",
        "C1_testfact_DAILY"));
    String expectedq5 = null;
    String expectedq6 = getExpectedQuery(cubeName, "SELECT testcube.cityid," +
      " sum(testCube.msr2) from ", null, "group by testcube.cityid having" +
      " sum(testCube.msr2) > 100 and sum(testCube.msr2) < 1000",
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact_HOURLY",
        "C1_testfact_DAILY"));
    String expectedq7 = getExpectedQuery(cubeName, "SELECT testcube.cityid," +
      " sum(testCube.msr2) from ", null, "group by testcube.cityid having" +
      " sum(testCube.msr2) > 100) OR (sum(testCube.msr2) < 100 AND" +
      " SUM(testcube.msr3) > 1000)",
      getWhereForDailyAndHourly2days(cubeName, "C1_testfact_HOURLY",
        "C1_testfact_DAILY"));

    String tests[] = {q1, q2, q3, q4, q5, q6, q7};
    String expected[] = {expectedq1, expectedq2, expectedq3, expectedq4,
        expectedq5, expectedq6, expectedq7};

    for (int i = 0; i < tests.length; i++) {
      try {
        String hql = driver.compileCubeQuery(tests[i]);
        if (expected[i] != null) {
          compareQueries(expected[i], hql);
        } else {
          Assert.assertTrue("Should not reach here", false);
        }
      } catch (SemanticException e) {
        e.printStackTrace();
      }
    }
    String q8 = "SELECT cityid, testCube.noAggrMsr FROM testCube where "
                + twoDaysRange;
    try {
      // Should throw exception in aggregate resolver because noAggrMsr does
      //not have a default aggregate defined.
      String hql = driver.compileCubeQuery(q8);
      Assert.assertTrue("Should not reach here: " + hql, false);
    } catch (SemanticException exc) {
      Assert.assertNotNull(exc);
      exc.printStackTrace();
    }
  }

  @Test
  public void testColumnAmbiguity() throws Exception {
    String query = "SELECT ambigdim1, sum(testCube.msr1) FROM testCube join" +
      " citytable on testcube.cityid = citytable.id where " + twoDaysRange;

    try {
      String hql = driver.compileCubeQuery(query);
      Assert.assertTrue("Should not reach here:" + hql, false);
    } catch (SemanticException exc) {
      Assert.assertNotNull(exc);
      exc.printStackTrace();
    }

    String q2 = "SELECT ambigdim2 from citytable join" +
      " statetable on citytable.stateid = statetable.id join countrytable on" +
      " statetable.countryid = countrytable.id";
    try {
      String hql = driver.compileCubeQuery(q2);
      Assert.assertTrue("Should not reach here: " + hql, false);
    } catch (SemanticException exc) {
      Assert.assertNotNull(exc);
      exc.printStackTrace();
    }
  }

  @Test
  public void testTimeRangeValidation() throws Exception {
    String timeRange2 = " time_range_in('" + getDateUptoHours(now)
        + "','" + getDateUptoHours(twodaysBack) + "')";
    try {
      // this should throw exception because from date is after to date
      driver.compileCubeQuery("SELECT cityid, testCube.msr2 from" +
          " testCube where " + timeRange2);
      Assert.assertTrue("Should not reach here", false);
    } catch (SemanticException exc) {
      exc.printStackTrace();
      Assert.assertNotNull(exc);
    }
  }

  @Test
  public void testAliasReplacer() throws Exception {
    String queries[] = {
      "SELECT cityid, t.msr2 FROM testCube t where " + twoDaysRange,
      "SELECT cityid, msr2 FROM testCube where msr2 > 100 and " + twoDaysRange +
      " HAVING msr2 < 1000",
      "SELECT cityid, testCube.msr2 FROM testCube where msr2 > 100 and "
        + twoDaysRange + " HAVING msr2 < 1000 ORDER BY cityid"
    };

    String expectedQueries[] = {
      getExpectedQuery("t", "SELECT t.cityid, sum(t.msr2) FROM ", null,
        " group by t.cityid", getWhereForDailyAndHourly2days("t",
          "C1_testfact_HOURLY", "C1_testfact_DAILY")),
      getExpectedQuery(cubeName, "SELECT testCube.cityid, sum(testCube.msr2)" +
        " FROM ", " testcube.msr2 > 100 ", " group by testcube.cityid having" +
        " sum(testCube.msr2 < 1000)", getWhereForDailyAndHourly2days(
          cubeName, "C1_testfact_HOURLY", "C1_testfact_DAILY")),
      getExpectedQuery(cubeName, "SELECT testCube.cityid, sum(testCube.msr2)" +
        " FROM ", " testcube.msr2 > 100 ", " group by testcube.cityid having" +
        " sum(testCube.msr2 < 1000) orderby testCube.cityid",
        getWhereForDailyAndHourly2days(cubeName, "C1_testfact_HOURLY",
          "C1_testfact_DAILY")),
    };

    for (int i = 0; i < queries.length; i++) {
      String hql = driver.compileCubeQuery(queries[i]);
      compareQueries(expectedQueries[i], hql);
    }
  }

  @Test
  public void testFactsWithInvalidColumns() throws Exception {
    String hqlQuery = driver.compileCubeQuery("select dim1, AVG(msr1)," +
      " msr2 from testCube" +
      " where " + twoDaysRange);
    String expected = getExpectedQuery(cubeName,
      "select testcube.dim1, avg(testcube.msr1), sum(testcube.msr2) FROM ",
      null, " group by testcube.dim1",
      getWhereForDailyAndHourly2days(cubeName, "C1_summary1_HOURLY",
        "C1_summary1_DAILY"));
    compareQueries(expected, hqlQuery);
    hqlQuery = driver.compileCubeQuery("select dim1, dim2, COUNT(msr1)," +
      " SUM(msr2), msr3 from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName,
      "select testcube.dim1, testcube,dim2, count(testcube.msr1)," +
      " sum(testcube.msr2), max(testcube.msr3) FROM ", null,
      " group by testcube.dim1, testcube.dim2",
      getWhereForDailyAndHourly2days(cubeName, "C1_summary2_HOURLY",
        "C1_summary2_DAILY"));
    compareQueries(expected, hqlQuery);
    hqlQuery = driver.compileCubeQuery("select dim1, dim2, cityid, SUM(msr1)," +
      " SUM(msr2), msr3 from testCube" +
      " where " + twoDaysRange);
    expected = getExpectedQuery(cubeName,
      "select testcube.dim1, testcube,dim2, testcube.cityid," +
      " sum(testcube.msr1), sum(testcube.msr2), max(testcube.msr3) FROM ",
      null, " group by testcube.dim1, testcube.dim2, testcube.cityid",
      getWhereForDailyAndHourly2days(cubeName, "C1_summary3_HOURLY",
        "C1_summary3_DAILY"));
    compareQueries(expected, hqlQuery);
  }
}
