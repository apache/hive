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

package org.apache.hive.streaming;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestStreamingDynamicPartitioning {
  private static final Logger LOG = LoggerFactory.getLogger(TestStreamingDynamicPartitioning.class);

  public static class RawFileSystem extends RawLocalFileSystem {
    private static final URI NAME;

    static {
      try {
        NAME = new URI("raw:///");
      } catch (URISyntaxException se) {
        throw new IllegalArgumentException("bad uri", se);
      }
    }

    @Override
    public URI getUri() {
      return NAME;
    }

    @Override
    public String getScheme() {
      return "raw";
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
      File file = pathToFile(path);
      if (!file.exists()) {
        throw new FileNotFoundException("Cannot find " + path);
      }
      // get close enough
      short mod = 0;
      if (file.canRead()) {
        mod |= 0444;
      }
      if (file.canWrite()) {
        mod |= 0200;
      }
      if (file.canExecute()) {
        mod |= 0111;
      }
      return new FileStatus(file.length(), file.isDirectory(), 1, 1024,
        file.lastModified(), file.lastModified(),
        FsPermission.createImmutable(mod), "owen", "users", path);
    }
  }

  private final HiveConf conf;
  private IDriver driver;
  private final IMetaStoreClient msClient;

  private static final String COL1 = "id";
  private static final String COL2 = "msg";
  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  // partitioned table
  private final static String dbName = "testing";
  private final static String tblName = "alerts";
  private final static String[] fieldNames = new String[]{COL1, COL2};
  private final static String[] colTypes = new String[]{serdeConstants.INT_TYPE_NAME, serdeConstants.STRING_TYPE_NAME};
  private final static String[] partNames = new String[]{"Continent", "Country"};
  private final static String[] bucketCols = new String[]{COL1};
  private final String loc1;

  // unpartitioned table
  private final static String dbName2 = "testing2";

  public TestStreamingDynamicPartitioning() throws Exception {
    conf = new HiveConf(this.getClass());
    conf.set("fs.raw.impl", RawFileSystem.class.getName());
    conf
      .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    TestTxnDbUtil.setConfValues(conf);
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
    dbFolder.create();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.WAREHOUSE, "raw://" + dbFolder.newFolder("warehouse"));
    loc1 = dbFolder.newFolder(dbName + ".db").toString();

    //1) Start from a clean slate (metastore)
    TestTxnDbUtil.cleanDb(conf);
    TestTxnDbUtil.prepDb(conf);

    //2) obtain metastore clients
    msClient = new HiveMetaStoreClient(conf);
  }

  @Before
  public void setup() throws Exception {
    SessionState.start(new CliSessionState(conf));
    driver = DriverFactory.newDriver(conf);
    driver.setMaxRows(200002);//make sure Driver returns all results
    // drop and recreate the necessary databases and tables
    dropDB(msClient, dbName);

    createDbAndTable(driver, dbName, tblName, null, fieldNames, colTypes, bucketCols, partNames, loc1, 1);

    dropDB(msClient, dbName2);
    String loc2 = dbFolder.newFolder(dbName2 + ".db").toString();
    String loc3 = dbFolder.newFolder("testing5.db").toString();
    createStoreSales("testing5", loc3);

    runDDL(driver, "drop table testBucketing3.streamedtable");
    runDDL(driver, "drop table testBucketing3.finaltable");
    runDDL(driver, "drop table testBucketing3.nobucket");
  }

  @After
  public void cleanup() throws Exception {
    msClient.close();
    driver.close();
  }

  private void createStoreSales(String dbName, String loc) throws Exception {
    String dbUri = "raw://" + new Path(loc).toUri().toString();
    String tableLoc = dbUri + Path.SEPARATOR + "store_sales";

    boolean success = runDDL(driver, "create database IF NOT EXISTS " + dbName + " location '" + dbUri + "'");
    Assert.assertTrue(success);
    success = runDDL(driver, "use " + dbName);
    Assert.assertTrue(success);

    success = runDDL(driver, "drop table if exists store_sales");
    Assert.assertTrue(success);
    success = runDDL(driver, "create table store_sales\n" +
      "(\n" +
      "    ss_sold_date_sk           int,\n" +
      "    ss_sold_time_sk           int,\n" +
      "    ss_item_sk                int,\n" +
      "    ss_customer_sk            int,\n" +
      "    ss_cdemo_sk               int,\n" +
      "    ss_hdemo_sk               int,\n" +
      "    ss_addr_sk                int,\n" +
      "    ss_store_sk               int,\n" +
      "    ss_promo_sk               int,\n" +
      "    ss_ticket_number          int,\n" +
      "    ss_quantity               int,\n" +
      "    ss_wholesale_cost         decimal(7,2),\n" +
      "    ss_list_price             decimal(7,2),\n" +
      "    ss_sales_price            decimal(7,2),\n" +
      "    ss_ext_discount_amt       decimal(7,2),\n" +
      "    ss_ext_sales_price        decimal(7,2),\n" +
      "    ss_ext_wholesale_cost     decimal(7,2),\n" +
      "    ss_ext_list_price         decimal(7,2),\n" +
      "    ss_ext_tax                decimal(7,2),\n" +
      "    ss_coupon_amt             decimal(7,2),\n" +
      "    ss_net_paid               decimal(7,2),\n" +
      "    ss_net_paid_inc_tax       decimal(7,2),\n" +
      "    ss_net_profit             decimal(7,2)\n" +
      ")\n" +
      " partitioned by (dt string)\n" +
      "clustered by (ss_store_sk, ss_promo_sk)\n" +
      "INTO 4 BUCKETS stored as orc " + " location '" + tableLoc + "'" +
      "  TBLPROPERTIES ('orc.compress'='NONE', 'transactional'='true')");
    Assert.assertTrue(success);

    success = runDDL(driver, "alter table store_sales add partition(dt='2015')");
    Assert.assertTrue(success);
  }

  @Test
  public void testDynamicPartitioning() throws Exception {
    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
      .withFieldDelimiter(',')
      .build();
    HiveStreamingConnection connection = HiveStreamingConnection.newBuilder()
      .withDatabase("testing5")
      .withTable("store_sales")
      .withAgentInfo("UT_" + Thread.currentThread().getName())
      .withRecordWriter(writer)
      .withHiveConf(conf)
      .connect();
    connection.beginTransaction();

    for (int i = 0; i < 10; i++) {
      StringBuilder row = new StringBuilder();
      for (int ints = 0; ints < 11; ints++) {
        row.append(ints).append(',');
      }
      for (int decs = 0; decs < 12; decs++) {
        row.append(i + 0.1).append(',');
      }
      row.append("2018-04-").append(i);
      connection.write(row.toString().getBytes());
    }
    connection.commitTransaction();
    connection.close();

    List<String> partitions = queryTable(driver, "show partitions testing5.store_sales");
    // 1 static partition created during setup + 10 dynamic partitions
    assertEquals(11, partitions.size());
    // ignore the first static partition
    for (int i = 1; i < partitions.size(); i++) {
      assertEquals("dt=2018-04-" + (i - 1), partitions.get(i));
    }

    ArrayList<String> res = queryTable(driver, "select * from testing5.store_sales");
    for (String re : res) {
      System.out.println(re);
      assertEquals(true, re.contains("2018-04-"));
    }
  }

  // stream data into streaming table with N buckets, then copy the data into another bucketed table
  // check if bucketing in both was done in the same way
  @Test
  public void testDPStreamBucketingMatchesRegularBucketing() throws Exception {
    int bucketCount = 100;

    String dbUri = "raw://" + new Path(dbFolder.newFolder().toString()).toUri().toString();
    String tableLoc = "'" + dbUri + Path.SEPARATOR + "streamedtable" + "'";
    String tableLoc2 = "'" + dbUri + Path.SEPARATOR + "finaltable" + "'";
    String tableLoc3 = "'" + dbUri + Path.SEPARATOR + "nobucket" + "'";

    // disabling vectorization as this test yields incorrect results with vectorization
    conf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, false);
    try (IDriver driver = DriverFactory.newDriver(conf)) {
      runDDL(driver, "create database testBucketing3");
      runDDL(driver, "use testBucketing3");
      runDDL(driver, "create table streamedtable ( key1 string,key2 int,data string ) partitioned by (year " +
        "int) clustered by " + "( " + "key1,key2 ) into "
        + bucketCount + " buckets  stored as orc  location " + tableLoc + " TBLPROPERTIES ('transactional'='true')");
      //  In 'nobucket' table we capture bucketid from streamedtable to workaround a hive bug that prevents joins two identically bucketed tables
      runDDL(driver, "create table nobucket ( bucketid int, key1 string,key2 int,data string ) partitioned by " +
        "(year int) location " + tableLoc3);
      runDDL(driver, "create table finaltable ( bucketid int, key1 string,key2 int,data string ) partitioned " +
        "by (year int) clustered by ( key1,key2 ) into "
        + bucketCount + " buckets  stored as orc location " + tableLoc2 + " TBLPROPERTIES ('transactional'='true')");


      String[] records = new String[]{
        "PSFAHYLZVC,29,EPNMA,2017",
        "PPPRKWAYAU,96,VUTEE,2017",
        "MIAOFERCHI,3,WBDSI,2017",
        "CEGQAZOWVN,0,WCUZL,2017",
        "XWAKMNSVQF,28,YJVHU,2017",
        "XBWTSAJWME,2,KDQFO,2017",
        "FUVLQTAXAY,5,LDSDG,2017",
        "QTQMDJMGJH,6,QBOMA,2018",
        "EFLOTLWJWN,71,GHWPS,2018",
        "PEQNAOJHCM,82,CAAFI,2018",
        "MOEKQLGZCP,41,RUACR,2018",
        "QZXMCOPTID,37,LFLWE,2018",
        "EYALVWICRD,13,JEZLC,2018",
        "VYWLZAYTXX,16,DMVZX,2018",
        "OSALYSQIXR,47,HNZVE,2018",
        "JGKVHKCEGQ,25,KSCJB,2018",
        "WQFMMYDHET,12,DTRWA,2018",
        "AJOVAYZKZQ,15,YBKFO,2018",
        "YAQONWCUAU,31,QJNHZ,2018",
        "DJBXUEUOEB,35,IYCBL,2018"
      };


      StrictDelimitedInputWriter wr = StrictDelimitedInputWriter.newBuilder()
        .withFieldDelimiter(',')
        .build();

      HiveStreamingConnection connection = HiveStreamingConnection.newBuilder()
        .withDatabase("testBucketing3")
        .withTable("streamedtable")
        .withAgentInfo("UT_" + Thread.currentThread().getName())
        .withRecordWriter(wr)
        .withHiveConf(conf)
        .connect();

      connection.beginTransaction();

      for (String record : records) {
        connection.write(record.getBytes());
      }

      connection.commitTransaction();
      connection.close();

      ArrayList<String> res1 = queryTable(driver, "select row__id.bucketid, * from streamedtable order by key2");
      for (String re : res1) {
        System.out.println(re);
        assertTrue(re.endsWith("2017") || re.endsWith("2018"));
      }

      driver.run("insert into nobucket partition(year) select row__id.bucketid,* from streamedtable");
      ArrayList<String> res = queryTable(driver, "select * from nobucket");
      assertEquals(records.length, res.size());
      runDDL(driver, " insert into finaltable partition(year) select * from nobucket");
      res = queryTable(driver, "select * from finaltable");
      assertEquals(records.length, res.size());
      ArrayList<String> res2 = queryTable(driver,
        "select row__id.bucketid,* from finaltable where row__id.bucketid<>bucketid");
      for (String s : res2) {
        LOG.error(s);
      }
      Assert.assertTrue(res2.isEmpty());

      res2 = queryTable(driver, "select * from finaltable where year=2018");
      assertEquals(13, res2.size());
      for (String s : res2) {
        assertTrue(s.endsWith("2018"));
      }

      res2 = queryTable(driver, "show partitions finaltable");
      assertEquals(2, res2.size());
      assertEquals("year=2017", res2.get(0));
      assertEquals("year=2018", res2.get(1));
    } finally {
      conf.unset(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED.varname);
    }
  }

  @Test
  public void testDPTwoLevel() throws Exception {
    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
      .withFieldDelimiter(',')
      .build();
    HiveStreamingConnection connection = HiveStreamingConnection.newBuilder()
      .withDatabase(dbName)
      .withTable(tblName)
      .withAgentInfo("UT_" + Thread.currentThread().getName())
      .withRecordWriter(writer)
      .withHiveConf(conf)
      .connect();
    connection.beginTransaction();
    connection.write("1,foo,Asia,India".getBytes());
    connection.write("2,bar,Europe,Germany".getBytes());
    connection.commitTransaction();
    connection.beginTransaction();
    connection.write("3,foo,Asia,India".getBytes());
    connection.write("4,bar,Europe,Germany".getBytes());
    connection.commitTransaction();
    connection.beginTransaction();
    connection.write("5,foo,Asia,China".getBytes());
    connection.write("6,bar,Europe,France".getBytes());
    connection.commitTransaction();
    connection.beginTransaction();
    connection.write("7,foo,Asia,China".getBytes());
    connection.write("8,bar,Europe,France".getBytes());
    connection.commitTransaction();
    connection.close();

    List<String> res = queryTable(driver, "select * from " + (dbName + "." + tblName) + " order by id");
    assertEquals(8, res.size());
    assertEquals("1\tfoo\tAsia\tIndia", res.get(0));
    assertEquals("2\tbar\tEurope\tGermany", res.get(1));
    assertEquals("3\tfoo\tAsia\tIndia", res.get(2));
    assertEquals("4\tbar\tEurope\tGermany", res.get(3));
    assertEquals("5\tfoo\tAsia\tChina", res.get(4));
    assertEquals("6\tbar\tEurope\tFrance", res.get(5));
    assertEquals("7\tfoo\tAsia\tChina", res.get(6));
    assertEquals("8\tbar\tEurope\tFrance", res.get(7));

    res = queryTable(driver, "show partitions " + (dbName + "." + tblName));
    assertEquals(4, res.size());
    assertTrue(res.contains("continent=Asia/country=India"));
    assertTrue(res.contains("continent=Asia/country=China"));
    assertTrue(res.contains("continent=Europe/country=Germany"));
    assertTrue(res.contains("continent=Europe/country=France"));
  }

  @Test
  public void testDPTwoLevelMissingPartitionValues() throws Exception {
    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
      .withFieldDelimiter(',')
      .build();
    HiveStreamingConnection connection = HiveStreamingConnection.newBuilder()
      .withDatabase(dbName)
      .withTable(tblName)
      .withAgentInfo("UT_" + Thread.currentThread().getName())
      .withRecordWriter(writer)
      .withHiveConf(conf)
      .connect();
    connection.beginTransaction();
    connection.write("1,foo,Asia,India".getBytes());
    connection.write("2,bar,Europe,Germany".getBytes());
    connection.commitTransaction();
    connection.beginTransaction();
    connection.write("3,foo,Asia,India".getBytes());
    connection.write("4,bar,Europe,Germany".getBytes());
    connection.commitTransaction();
    connection.beginTransaction();
    connection.write("5,foo,Asia,China".getBytes());
    connection.write("6,bar,Europe,France".getBytes());
    connection.commitTransaction();
    connection.beginTransaction();
    connection.write("7,foo,Asia,China".getBytes());
    connection.write("8,bar,Europe,France".getBytes());
    connection.commitTransaction();
    connection.close();

    List<String> res = queryTable(driver, "select * from " + (dbName + "." + tblName) + " order by id");
    assertEquals(8, res.size());
    assertEquals("1\tfoo\tAsia\tIndia", res.get(0));
    assertEquals("2\tbar\tEurope\tGermany", res.get(1));
    assertEquals("3\tfoo\tAsia\tIndia", res.get(2));
    assertEquals("4\tbar\tEurope\tGermany", res.get(3));
    assertEquals("5\tfoo\tAsia\tChina", res.get(4));
    assertEquals("6\tbar\tEurope\tFrance", res.get(5));
    assertEquals("7\tfoo\tAsia\tChina", res.get(6));
    assertEquals("8\tbar\tEurope\tFrance", res.get(7));

    res = queryTable(driver, "show partitions " + (dbName + "." + tblName));
    assertEquals(4, res.size());
    assertTrue(res.contains("continent=Asia/country=India"));
    assertTrue(res.contains("continent=Asia/country=China"));
    assertTrue(res.contains("continent=Europe/country=Germany"));
    assertTrue(res.contains("continent=Europe/country=France"));
  }

  @Test
  public void testDPTwoLevelNonStringPartitionColumns() throws Exception {
    String tblName = "alerts2";
    String[] partNames = new String[] {"year", "month"};
    createDbAndTable(driver, dbName, tblName, null, fieldNames, colTypes, bucketCols, partNames, loc1, 2,
      "partitioned by (year int, month int)");
    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
      .withFieldDelimiter(',')
      .build();
    HiveStreamingConnection connection = HiveStreamingConnection.newBuilder()
      .withDatabase(dbName)
      .withTable(tblName)
      .withAgentInfo("UT_" + Thread.currentThread().getName())
      .withRecordWriter(writer)
      .withHiveConf(conf)
      .connect();
    connection.beginTransaction();
    connection.write("1,foo,2018,2".getBytes());
    connection.write("2,bar,2019".getBytes());
    connection.commitTransaction();
    connection.beginTransaction();
    connection.write("3,foo,2018".getBytes());
    connection.write("4,bar,2019".getBytes());
    connection.commitTransaction();
    connection.beginTransaction();
    connection.write("5,foo,2018".getBytes());
    connection.write("6,bar,2019".getBytes());
    connection.commitTransaction();
    connection.beginTransaction();
    connection.write("7,foo,,".getBytes());
    connection.write("8,bar,,12".getBytes());
    connection.commitTransaction();
    connection.close();

    // when partition column type is not string, the values from __HIVE_DEFAULT_PARTITION__ will be NULL
    String defaultPartitionName = "NULL";
    List<String> res = queryTable(driver, "select * from " + (dbName + "." + tblName) + " order by id");
    assertEquals(8, res.size());
    assertEquals("1\tfoo\t2018\t2", res.get(0));
    assertEquals("2\tbar\t2019\t" + defaultPartitionName, res.get(1));
    assertEquals("3\tfoo\t2018\t" + defaultPartitionName, res.get(2));
    assertEquals("4\tbar\t2019\t" + defaultPartitionName, res.get(3));
    assertEquals("5\tfoo\t2018\t" + defaultPartitionName, res.get(4));
    assertEquals("6\tbar\t2019\t" + defaultPartitionName, res.get(5));
    assertEquals("7\tfoo\t" + defaultPartitionName + "\t" + defaultPartitionName, res.get(6));
    assertEquals("8\tbar\t" + defaultPartitionName + "\t12", res.get(7));

    defaultPartitionName = conf.getVar(HiveConf.ConfVars.DEFAULT_PARTITION_NAME);
    res = queryTable(driver, "show partitions " + (dbName + "." + tblName));
    assertEquals(5, res.size());
    assertTrue(res.contains("year=2018/month=2"));
    assertTrue(res.contains("year=2018/month=" + defaultPartitionName));
    assertTrue(res.contains("year=2019/month=" + defaultPartitionName));
    assertTrue(res.contains("year=" + defaultPartitionName + "/month=" + defaultPartitionName));
    assertTrue(res.contains("year=" + defaultPartitionName + "/month=12"));
  }

  @Test
  public void testWriteBeforeBegin() throws Exception {
    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
      .withFieldDelimiter(',')
      .build();
    HiveStreamingConnection connection = HiveStreamingConnection.newBuilder()
      .withDatabase(dbName)
      .withTable(tblName)
      .withAgentInfo("UT_" + Thread.currentThread().getName())
      .withRecordWriter(writer)
      .withHiveConf(conf)
      .connect();

    // begin + write + commit
    connection.beginTransaction();
    connection.write("1,foo,Asia".getBytes());
    connection.write("2,bar,Europe".getBytes());
    connection.commitTransaction();

    // no begin + write
    Exception exception = null;
    try {
      connection.write("3,SHOULD FAIL!".getBytes());
    } catch (Exception e) {
      exception = e;
    }
    assertNotNull(exception);
    assertTrue(exception.getMessage().equals("Transaction state is not OPEN. Missing beginTransaction?"));

    // no begin + commit
    exception = null;
    try {
      connection.commitTransaction();
    } catch (Exception e) {
      exception = e;
    }
    assertNotNull(exception);
    assertTrue(exception.getMessage().equals("Transaction state is not OPEN. Missing beginTransaction?"));

    // no begin + abort
    exception = null;
    try {
      connection.abortTransaction();
    } catch (Exception e) {
      exception = e;
    }
    assertNotNull(exception);
    assertTrue(exception.getMessage().equals("Transaction state is not OPEN. Missing beginTransaction?"));

    connection.close();
    String defaultPartitionName = conf.getVar(HiveConf.ConfVars.DEFAULT_PARTITION_NAME);
    List<String> res = queryTable(driver, "select * from " + (dbName + "." + tblName) + " order by id");
    assertEquals(2, res.size());
    assertEquals("1\tfoo\tAsia\t" + defaultPartitionName, res.get(0));
    assertEquals("2\tbar\tEurope\t" + defaultPartitionName, res.get(1));
  }

  @Test
  public void testRegexInputStreamDP() throws Exception {
    String regex = "([^,]*),(.*),(.*),(.*)";
    StrictRegexWriter writer = StrictRegexWriter.newBuilder()
      // if unspecified, default one or [\r\n] will be used for line break
      .withRegex(regex)
      .build();
    StreamingConnection connection = HiveStreamingConnection.newBuilder()
      .withDatabase(dbName)
      .withTable(tblName)
      .withAgentInfo("UT_" + Thread.currentThread().getName())
      .withHiveConf(conf)
      .withRecordWriter(writer)
      .connect();

    String rows = "1,foo,Asia,India\r2,bar,Europe,Germany\r3,baz,Asia,China\r4,cat,Australia,";
    ByteArrayInputStream bais = new ByteArrayInputStream(rows.getBytes());
    connection.beginTransaction();
    connection.write(bais);
    connection.commitTransaction();
    bais.close();
    connection.close();

    List<String> rs = queryTable(driver, "select * from " + dbName + "." + tblName + " order by id");
    Assert.assertEquals(4, rs.size());
    Assert.assertEquals("1\tfoo\tAsia\tIndia", rs.get(0));
    Assert.assertEquals("2\tbar\tEurope\tGermany", rs.get(1));
    Assert.assertEquals("3\tbaz\tAsia\tChina", rs.get(2));
    Assert.assertEquals("4\tcat\tAustralia\t__HIVE_DEFAULT_PARTITION__", rs.get(3));
    rs = queryTable(driver, "show partitions " + dbName + "." + tblName);
    Assert.assertEquals(4, rs.size());
    Assert.assertTrue(rs.contains("continent=Asia/country=India"));
    Assert.assertTrue(rs.contains("continent=Asia/country=China"));
    Assert.assertTrue(rs.contains("continent=Europe/country=Germany"));
    Assert.assertTrue(rs.contains("continent=Australia/country=__HIVE_DEFAULT_PARTITION__"));
  }

  @Test
  public void testJsonInputStreamDP() throws Exception {
    StrictJsonWriter writer = StrictJsonWriter.newBuilder()
      .withLineDelimiterPattern("\\|")
      .build();
    HiveStreamingConnection connection = HiveStreamingConnection.newBuilder()
      .withDatabase(dbName)
      .withTable(tblName)
      .withAgentInfo("UT_" + Thread.currentThread().getName())
      .withRecordWriter(writer)
      .withHiveConf(conf)
      .connect();

    // 1st Txn
    connection.beginTransaction();
    Assert.assertEquals(HiveStreamingConnection.TxnState.OPEN, connection.getCurrentTransactionState());
    String records = "{\"id\" : 1, \"msg\": \"Hello streaming\", \"continent\": \"Asia\", \"Country\": \"India\"}|" +
      "{\"id\" : 2, \"msg\": \"Hello world\", \"continent\": \"Europe\", \"Country\": \"Germany\"}|" +
      "{\"id\" : 3, \"msg\": \"Hello world!!\", \"continent\": \"Asia\", \"Country\": \"China\"}|" +
      "{\"id\" : 4, \"msg\": \"Hmm..\", \"continent\": \"Australia\", \"Unknown-field\": \"whatever\"}|";
    ByteArrayInputStream bais = new ByteArrayInputStream(records.getBytes());
    connection.write(bais);
    connection.commitTransaction();
    bais.close();
    connection.close();
    List<String> rs = queryTable(driver, "select * from " + dbName + "." + tblName + " order by id");
    Assert.assertEquals(4, rs.size());
    Assert.assertEquals("1\tHello streaming\tAsia\tIndia", rs.get(0));
    Assert.assertEquals("2\tHello world\tEurope\tGermany", rs.get(1));
    Assert.assertEquals("3\tHello world!!\tAsia\tChina", rs.get(2));
    Assert.assertEquals("4\tHmm..\tAustralia\t__HIVE_DEFAULT_PARTITION__", rs.get(3));
    rs = queryTable(driver, "show partitions " + dbName + "." + tblName);
    Assert.assertEquals(4, rs.size());
    Assert.assertTrue(rs.contains("continent=Asia/country=India"));
    Assert.assertTrue(rs.contains("continent=Asia/country=China"));
    Assert.assertTrue(rs.contains("continent=Europe/country=Germany"));
    Assert.assertTrue(rs.contains("continent=Australia/country=__HIVE_DEFAULT_PARTITION__"));
  }

  @Test
  public void testWriteAfterClose() throws Exception {
    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
      .withFieldDelimiter(',')
      .build();
    HiveStreamingConnection connection = HiveStreamingConnection.newBuilder()
      .withDatabase(dbName)
      .withTable(tblName)
      .withAgentInfo("UT_" + Thread.currentThread().getName())
      .withRecordWriter(writer)
      .withHiveConf(conf)
      .connect();

    // begin + write + commit
    connection.beginTransaction();
    connection.write("1,foo,Asia".getBytes());
    connection.write("2,bar,Europe".getBytes());
    connection.commitTransaction();

    // close + write
    connection.close();

    Exception exception = null;
    try {
      connection.write("3,SHOULD FAIL!".getBytes());
    } catch (Exception e) {
      exception = e;
    }
    assertNotNull(exception);
    assertTrue(exception.getMessage().endsWith("Streaming connection is closed already."));

    // close + commit
    exception = null;
    try {
      connection.commitTransaction();
    } catch (Exception e) {
      exception = e;
    }
    assertNotNull(exception);
    assertTrue(exception.getMessage().endsWith("Streaming connection is closed already."));

    // close + abort
    exception = null;
    try {
      connection.abortTransaction();
    } catch (Exception e) {
      exception = e;
    }
    assertNotNull(exception);
    assertTrue(exception.getMessage().endsWith("Streaming connection is closed already."));

    String defaultPartitionName = conf.getVar(HiveConf.ConfVars.DEFAULT_PARTITION_NAME);
    List<String> res = queryTable(driver, "select * from " + (dbName + "." + tblName) + " order by id");
    assertEquals(2, res.size());
    assertEquals("1\tfoo\tAsia\t" + defaultPartitionName, res.get(0));
    assertEquals("2\tbar\tEurope\t" + defaultPartitionName, res.get(1));
  }

  @Test
  public void testWriteAfterAbort() throws Exception {
    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
      .withFieldDelimiter(',')
      .build();
    HiveStreamingConnection connection = HiveStreamingConnection.newBuilder()
      .withDatabase(dbName)
      .withTable(tblName)
      .withAgentInfo("UT_" + Thread.currentThread().getName())
      .withRecordWriter(writer)
      .withHiveConf(conf)
      .connect();

    // begin + write + commit
    connection.beginTransaction();
    connection.write("1,foo,Asia".getBytes());
    connection.write("2,bar,Europe".getBytes());
    connection.commitTransaction();

    // begin + write + abort
    connection.beginTransaction();
    connection.write("3,oops!".getBytes());
    connection.abortTransaction();

    // begin + write + abort
    connection.beginTransaction();
    connection.write("4,I did it again!".getBytes());
    connection.abortTransaction();

    // begin + write + commit
    connection.beginTransaction();
    connection.write("5,Not now!,Europe".getBytes());
    connection.commitTransaction();

    // close + write
    connection.close();
    Exception exception = null;
    try {
      connection.write("6,SHOULD FAIL!".getBytes());
    } catch (Exception e) {
      exception = e;
    }
    assertNotNull(exception);
    assertTrue(exception.getMessage().equals("Streaming connection is closed already."));
    String defaultPartitionName = conf.getVar(HiveConf.ConfVars.DEFAULT_PARTITION_NAME);
    List<String> res = queryTable(driver, "select * from " + (dbName + "." + tblName) + " order by id");
    assertEquals(3, res.size());
    assertEquals("1\tfoo\tAsia\t" + defaultPartitionName, res.get(0));
    assertEquals("2\tbar\tEurope\t" + defaultPartitionName, res.get(1));
    assertEquals("5\tNot now!\tEurope\t" + defaultPartitionName, res.get(2));
  }

  @Test
  public void testTableValidation() throws Exception {
    int bucketCount = 100;

    String dbUri = "raw://" + new Path(dbFolder.newFolder().toString()).toUri().toString();
    String tbl1 = "validation1";
    String tbl2 = "validation2";

    String tableLoc = "'" + dbUri + Path.SEPARATOR + tbl1 + "'";
    String tableLoc2 = "'" + dbUri + Path.SEPARATOR + tbl2 + "'";

    runDDL(driver, "create database testBucketing3");
    runDDL(driver, "use testBucketing3");

    runDDL(driver, "create table " + tbl1 + " ( key1 string, data string ) clustered by ( key1 ) into "
      + bucketCount + " buckets  stored as orc  location " + tableLoc + " TBLPROPERTIES ('transactional'='false')");

    runDDL(driver, "create table " + tbl2 + " ( key1 string, data string ) clustered by ( key1 ) into "
      + bucketCount + " buckets  stored as orc  location " + tableLoc2 + " TBLPROPERTIES ('transactional'='false')");

    StrictDelimitedInputWriter wr = StrictDelimitedInputWriter.newBuilder()
      .withFieldDelimiter(',')
      .build();
    HiveStreamingConnection connection = null;
    try {
      connection = HiveStreamingConnection.newBuilder()
        .withDatabase("testBucketing3")
        .withTable("validation2")
        .withAgentInfo("UT_" + Thread.currentThread().getName())
        .withRecordWriter(wr)
        .withHiveConf(conf)
        .connect();
      Assert.assertTrue("InvalidTable exception was not thrown", false);
    } catch (InvalidTable e) {
      // expecting this exception
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
    try {
      connection = HiveStreamingConnection.newBuilder()
        .withDatabase("testBucketing3")
        .withTable("validation2")
        .withAgentInfo("UT_" + Thread.currentThread().getName())
        .withRecordWriter(wr)
        .withHiveConf(conf)
        .connect();
      Assert.assertTrue("InvalidTable exception was not thrown", false);
    } catch (InvalidTable e) {
      // expecting this exception
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  private static boolean runDDL(IDriver driver, String sql) {
    LOG.debug(sql);
    System.out.println(sql);
    try {
      driver.run(sql);
      return true;
    } catch (CommandProcessorException e) {
      LOG.error("Statement: " + sql + " failed: " + e);
      return false;
    }
  }


  private static ArrayList<String> queryTable(IDriver driver, String query) throws IOException {
    try {
      driver.run(query);
    } catch (CommandProcessorException e) {
      throw new RuntimeException(query + " failed: " + e);
    }
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    return res;
  }


  // delete db and all tables in it
  public static void dropDB(IMetaStoreClient client, String databaseName) {
    try {
      for (String table : client.listTableNamesByFilter(databaseName, "", (short) -1)) {
        client.dropTable(databaseName, table, true, true);
      }
      client.dropDatabase(databaseName);
    } catch (TException e) {
    }

  }


  ///////// -------- UTILS ------- /////////
  // returns Path of the partition created (if any) else Path of table
  private static Path createDbAndTable(IDriver driver, String databaseName,
    String tableName, List<String> partVals,
    String[] colNames, String[] colTypes,
    String[] bucketCols,
    String[] partNames, String dbLocation, int bucketCount)
    throws Exception {

    String dbUri = "raw://" + new Path(dbLocation).toUri().toString();
    String tableLoc = dbUri + Path.SEPARATOR + tableName;

    runDDL(driver, "create database IF NOT EXISTS " + databaseName + " location '" + dbUri + "'");
    runDDL(driver, "use " + databaseName);
    String crtTbl = "create table " + tableName +
      " ( " + getTableColumnsStr(colNames, colTypes) + " )" +
      getPartitionStmtStr(partNames) +
      " clustered by ( " + join(bucketCols, ",") + " )" +
      " into " + bucketCount + " buckets " +
      " stored as orc " +
      " location '" + tableLoc + "'" +
      " TBLPROPERTIES ('transactional'='true') ";
    runDDL(driver, crtTbl);
    if (partNames != null && partNames.length != 0 && partVals != null) {
      return addPartition(driver, tableName, partVals, partNames);
    }
    return new Path(tableLoc);
  }

  private static Path createDbAndTable(IDriver driver, String databaseName,
    String tableName, List<String> partVals,
    String[] colNames, String[] colTypes,
    String[] bucketCols,
    String[] partNames, String dbLocation, int bucketCount, String partLine)
    throws Exception {

    String dbUri = "raw://" + new Path(dbLocation).toUri().toString();
    String tableLoc = dbUri + Path.SEPARATOR + tableName;

    runDDL(driver, "create database IF NOT EXISTS " + databaseName + " location '" + dbUri + "'");
    runDDL(driver, "use " + databaseName);
    String crtTbl = "create table " + tableName +
      " ( " + getTableColumnsStr(colNames, colTypes) + " )" +
      partLine +
      " clustered by ( " + join(bucketCols, ",") + " )" +
      " into " + bucketCount + " buckets " +
      " stored as orc " +
      " location '" + tableLoc + "'" +
      " TBLPROPERTIES ('transactional'='true') ";
    runDDL(driver, crtTbl);
    if (partNames != null && partNames.length != 0 && partVals != null) {
      return addPartition(driver, tableName, partVals, partNames);
    }
    return new Path(tableLoc);
  }

  private static Path addPartition(IDriver driver, String tableName, List<String> partVals, String[] partNames)
    throws Exception {
    String partSpec = getPartsSpec(partNames, partVals);
    String addPart = "alter table " + tableName + " add partition ( " + partSpec + " )";
    runDDL(driver, addPart);
    return getPartitionPath(driver, tableName, partSpec);
  }

  private static Path getPartitionPath(IDriver driver, String tableName, String partSpec) throws Exception {
    ArrayList<String> res = queryTable(driver, "describe extended " + tableName + " PARTITION (" + partSpec + ")");
    String partInfo = res.get(res.size() - 1);
    int start = partInfo.indexOf("location:") + "location:".length();
    int end = partInfo.indexOf(",", start);
    return new Path(partInfo.substring(start, end));
  }

  private static String getTableColumnsStr(String[] colNames, String[] colTypes) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < colNames.length; ++i) {
      sb.append(colNames[i]).append(" ").append(colTypes[i]);
      if (i < colNames.length - 1) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  // converts partNames into "partName1 string, partName2 string"
  private static String getTablePartsStr(String[] partNames) {
    if (partNames == null || partNames.length == 0) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < partNames.length; ++i) {
      sb.append(partNames[i]).append(" string");
      if (i < partNames.length - 1) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  // converts partNames,partVals into "partName1=val1, partName2=val2"
  private static String getPartsSpec(String[] partNames, List<String> partVals) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < partVals.size(); ++i) {
      sb.append(partNames[i]).append(" = '").append(partVals.get(i)).append("'");
      if (i < partVals.size() - 1) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  private static String join(String[] values, String delimiter) {
    if (values == null) {
      return null;
    }
    StringBuilder strbuf = new StringBuilder();

    boolean first = true;

    for (Object value : values) {
      if (!first) {
        strbuf.append(delimiter);
      } else {
        first = false;
      }
      strbuf.append(value.toString());
    }

    return strbuf.toString();
  }

  private static String getPartitionStmtStr(String[] partNames) {
    if (partNames == null || partNames.length == 0) {
      return "";
    }
    return " partitioned by (" + getTablePartsStr(partNames) + " )";
  }
}
