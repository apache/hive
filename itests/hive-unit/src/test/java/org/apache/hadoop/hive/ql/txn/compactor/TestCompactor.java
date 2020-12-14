/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.txn.compactor;

import static org.apache.hadoop.hive.ql.TestTxnCommands2.runCleaner;
import static org.apache.hadoop.hive.ql.TestTxnCommands2.runInitiator;
import static org.apache.hadoop.hive.ql.TestTxnCommands2.runWorker;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.common.util.Retry;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.streaming.HiveStreamingConnection;
import org.apache.hive.streaming.StreamingConnection;
import org.apache.hive.streaming.StreamingException;
import org.apache.hive.streaming.StrictDelimitedInputWriter;
import org.apache.orc.OrcConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Compaction related unit tests.
 */
@SuppressWarnings("deprecation")
public class TestCompactor {
  private static final AtomicInteger salt = new AtomicInteger(new Random().nextInt());
  private static final Logger LOG = LoggerFactory.getLogger(TestCompactor.class);
  private final String TEST_DATA_DIR = HCatUtil.makePathASafeFileName(System.getProperty("java.io.tmpdir") +
    File.separator + TestCompactor.class.getCanonicalName() + "-" + System.currentTimeMillis() + "_" +
    salt.getAndIncrement());
  private final String BASIC_FILE_NAME = TEST_DATA_DIR + "/basic.input.data";
  private final String TEST_WAREHOUSE_DIR = TEST_DATA_DIR + "/warehouse";

  @Rule
  public TemporaryFolder stagingFolder = new TemporaryFolder();

  @Rule
  public Retry retry = new Retry(2);

  private HiveConf conf;
  IMetaStoreClient msClient;
  private IDriver driver;

  @Before
  public void setup() throws Exception {

    File f = new File(TEST_WAREHOUSE_DIR);
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    if (!(new File(TEST_WAREHOUSE_DIR).mkdirs())) {
      throw new RuntimeException("Could not create " + TEST_WAREHOUSE_DIR);
    }

    HiveConf hiveConf = new HiveConf(this.getClass());
    hiveConf.setVar(HiveConf.ConfVars.PREEXECHOOKS, "");
    hiveConf.setVar(HiveConf.ConfVars.POSTEXECHOOKS, "");
    hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, TEST_WAREHOUSE_DIR);
    hiveConf.setVar(HiveConf.ConfVars.HIVEINPUTFORMAT, HiveInputFormat.class.getName());
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER, false);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVEOPTIMIZEMETADATAQUERIES, false);

    TxnDbUtil.setConfValues(hiveConf);
    TxnDbUtil.cleanDb(hiveConf);
    TxnDbUtil.prepDb(hiveConf);

    conf = hiveConf;
    HiveConf.setBoolVar(conf, ConfVars.HIVE_MM_ALLOW_ORIGINALS, true);
    HiveConf.setTimeVar(conf, ConfVars.HIVE_COMPACTOR_ABORTEDTXN_TIME_THRESHOLD, 0, TimeUnit.MILLISECONDS);
    msClient = new HiveMetaStoreClient(conf);
    driver = DriverFactory.newDriver(hiveConf);
    SessionState.start(new CliSessionState(hiveConf));


    int LOOP_SIZE = 3;
    String[] input = new String[LOOP_SIZE * LOOP_SIZE];
    int k = 0;
    for (int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for (int j = 1; j <= LOOP_SIZE; j++) {
        String sj = "S" + j + "S";
        input[k] = si + "\t" + sj;
        k++;
      }
    }
    createTestDataFile(BASIC_FILE_NAME, input);
  }

  @After
  public void tearDown() {
    conf = null;
    if (msClient != null) {
      msClient.close();
    }
    if (driver != null) {
      driver.close();
    }
  }

  /**
   * Simple schema evolution add columns with partitioning.
   *
   * @throws Exception
   */
  @Test
  public void schemaEvolutionAddColDynamicPartitioningInsert() throws Exception {
    String tblName = "dpct";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " PARTITIONED BY(ds string)" +
      " CLUSTERED BY(a) INTO 2 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC TBLPROPERTIES ('transactional'='true')", driver);

    // First INSERT round.
    executeStatementOnDriver("insert into " + tblName + " partition (ds) values (1, 'fred', " +
      "'today'), (2, 'wilma', 'yesterday')", driver);

    // ALTER TABLE ... ADD COLUMNS
    executeStatementOnDriver("ALTER TABLE " + tblName + " ADD COLUMNS(c int)", driver);

    // Validate there is an added NULL for column c.
    executeStatementOnDriver("SELECT * FROM " + tblName + " ORDER BY a", driver);
    ArrayList<String> valuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(valuesReadFromHiveDriver);
    Assert.assertEquals(2, valuesReadFromHiveDriver.size());
    Assert.assertEquals("1\tfred\tNULL\ttoday", valuesReadFromHiveDriver.get(0));
    Assert.assertEquals("2\twilma\tNULL\tyesterday", valuesReadFromHiveDriver.get(1));

    // Second INSERT round with new inserts into previously existing partition 'yesterday'.
    executeStatementOnDriver("insert into " + tblName + " partition (ds) values " +
        "(3, 'mark', 1900, 'soon'), (4, 'douglas', 1901, 'last_century'), " +
        "(5, 'doc', 1902, 'yesterday')",
      driver);

    // Validate there the new insertions for column c.
    executeStatementOnDriver("SELECT * FROM " + tblName + " ORDER BY a", driver);
    valuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(valuesReadFromHiveDriver);
    Assert.assertEquals(5, valuesReadFromHiveDriver.size());
    Assert.assertEquals("1\tfred\tNULL\ttoday", valuesReadFromHiveDriver.get(0));
    Assert.assertEquals("2\twilma\tNULL\tyesterday", valuesReadFromHiveDriver.get(1));
    Assert.assertEquals("3\tmark\t1900\tsoon", valuesReadFromHiveDriver.get(2));
    Assert.assertEquals("4\tdouglas\t1901\tlast_century", valuesReadFromHiveDriver.get(3));
    Assert.assertEquals("5\tdoc\t1902\tyesterday", valuesReadFromHiveDriver.get(4));
    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 0);
    runInitiator(conf);

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(4, compacts.size());
    SortedSet<String> partNames = new TreeSet<String>();
    verifyCompactions(compacts, partNames, tblName);
    List<String> names = new ArrayList<String>(partNames);
    Assert.assertEquals("ds=last_century", names.get(0));
    Assert.assertEquals("ds=soon", names.get(1));
    Assert.assertEquals("ds=today", names.get(2));
    Assert.assertEquals("ds=yesterday", names.get(3));

    // Validate after compaction.
    executeStatementOnDriver("SELECT * FROM " + tblName + " ORDER BY a", driver);
    valuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(valuesReadFromHiveDriver);
    Assert.assertEquals(5, valuesReadFromHiveDriver.size());
    Assert.assertEquals("1\tfred\tNULL\ttoday", valuesReadFromHiveDriver.get(0));
    Assert.assertEquals("2\twilma\tNULL\tyesterday", valuesReadFromHiveDriver.get(1));
    Assert.assertEquals("3\tmark\t1900\tsoon", valuesReadFromHiveDriver.get(2));
    Assert.assertEquals("4\tdouglas\t1901\tlast_century", valuesReadFromHiveDriver.get(3));
    Assert.assertEquals("5\tdoc\t1902\tyesterday", valuesReadFromHiveDriver.get(4));

  }

  @Test
  public void schemaEvolutionAddColDynamicPartitioningUpdate() throws Exception {
    String tblName = "udpct";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " PARTITIONED BY(ds string)" +
      " CLUSTERED BY(a) INTO 2 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("insert into " + tblName + " partition (ds) values (1, 'fred', " +
      "'today'), (2, 'wilma', 'yesterday')", driver);

    executeStatementOnDriver("update " + tblName + " set b = 'barney'", driver);

    // Validate the update.
    executeStatementOnDriver("SELECT * FROM " + tblName + " ORDER BY a", driver);
    ArrayList<String> valuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(valuesReadFromHiveDriver);
    Assert.assertEquals(2, valuesReadFromHiveDriver.size());
    Assert.assertEquals("1\tbarney\ttoday", valuesReadFromHiveDriver.get(0));
    Assert.assertEquals("2\tbarney\tyesterday", valuesReadFromHiveDriver.get(1));

    // ALTER TABLE ... ADD COLUMNS
    executeStatementOnDriver("ALTER TABLE " + tblName + " ADD COLUMNS(c int)", driver);

    // Validate there is an added NULL for column c.
    executeStatementOnDriver("SELECT * FROM " + tblName + " ORDER BY a", driver);
    valuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(valuesReadFromHiveDriver);
    Assert.assertEquals(2, valuesReadFromHiveDriver.size());
    Assert.assertEquals("1\tbarney\tNULL\ttoday", valuesReadFromHiveDriver.get(0));
    Assert.assertEquals("2\tbarney\tNULL\tyesterday", valuesReadFromHiveDriver.get(1));

    // Second INSERT round with new inserts into previously existing partition 'yesterday'.
    executeStatementOnDriver("insert into " + tblName + " partition (ds) values " +
        "(3, 'mark', 1900, 'soon'), (4, 'douglas', 1901, 'last_century'), " +
        "(5, 'doc', 1902, 'yesterday')",
      driver);

    // Validate there the new insertions for column c.
    executeStatementOnDriver("SELECT * FROM " + tblName + " ORDER BY a", driver);
    valuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(valuesReadFromHiveDriver);
    Assert.assertEquals(5, valuesReadFromHiveDriver.size());
    Assert.assertEquals("1\tbarney\tNULL\ttoday", valuesReadFromHiveDriver.get(0));
    Assert.assertEquals("2\tbarney\tNULL\tyesterday", valuesReadFromHiveDriver.get(1));
    Assert.assertEquals("3\tmark\t1900\tsoon", valuesReadFromHiveDriver.get(2));
    Assert.assertEquals("4\tdouglas\t1901\tlast_century", valuesReadFromHiveDriver.get(3));
    Assert.assertEquals("5\tdoc\t1902\tyesterday", valuesReadFromHiveDriver.get(4));

    executeStatementOnDriver("update " + tblName + " set c = 2000", driver);

    // Validate the update of new column c, even in old rows.
    executeStatementOnDriver("SELECT * FROM " + tblName + " ORDER BY a", driver);
    valuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(valuesReadFromHiveDriver);
    Assert.assertEquals(5, valuesReadFromHiveDriver.size());
    Assert.assertEquals("1\tbarney\t2000\ttoday", valuesReadFromHiveDriver.get(0));
    Assert.assertEquals("2\tbarney\t2000\tyesterday", valuesReadFromHiveDriver.get(1));
    Assert.assertEquals("3\tmark\t2000\tsoon", valuesReadFromHiveDriver.get(2));
    Assert.assertEquals("4\tdouglas\t2000\tlast_century", valuesReadFromHiveDriver.get(3));
    Assert.assertEquals("5\tdoc\t2000\tyesterday", valuesReadFromHiveDriver.get(4));

    // Set to 1 so insert doesn't set it off but update does
    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 1);
    runInitiator(conf);

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(4, compacts.size());
    SortedSet<String> partNames = new TreeSet<String>();
    verifyCompactions(compacts, partNames, tblName);
    List<String> names = new ArrayList<String>(partNames);
    Assert.assertEquals("ds=last_century", names.get(0));
    Assert.assertEquals("ds=soon", names.get(1));
    Assert.assertEquals("ds=today", names.get(2));
    Assert.assertEquals("ds=yesterday", names.get(3));

    // Validate after compaction.
    executeStatementOnDriver("SELECT * FROM " + tblName + " ORDER BY a", driver);
    valuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(valuesReadFromHiveDriver);
    Assert.assertEquals(5, valuesReadFromHiveDriver.size());
    Assert.assertEquals("1\tbarney\t2000\ttoday", valuesReadFromHiveDriver.get(0));
    Assert.assertEquals("2\tbarney\t2000\tyesterday", valuesReadFromHiveDriver.get(1));
    Assert.assertEquals("3\tmark\t2000\tsoon", valuesReadFromHiveDriver.get(2));
    Assert.assertEquals("4\tdouglas\t2000\tlast_century", valuesReadFromHiveDriver.get(3));
    Assert.assertEquals("5\tdoc\t2000\tyesterday", valuesReadFromHiveDriver.get(4));
  }

  /**
   * After each major compaction, stats need to be updated on each column of the
   * table/partition which previously had stats.
   * 1. create a bucketed ORC backed table (Orc is currently required by ACID)
   * 2. populate 2 partitions with data
   * 3. compute stats
   * 4. insert some data into the table using StreamingAPI
   * 5. Trigger major compaction (which should update stats)
   * 6. check that stats have been updated
   *
   * @throws Exception todo:
   *                   2. add non-partitioned test
   *                   4. add a test with sorted table?
   */
  @Test
  public void testStatsAfterCompactionPartTbl() throws Exception {
    //as of (8/27/2014) Hive 0.14, ACID/Orc requires HiveInputFormat
    String tblName = "compaction_test";
    String tblNameStg = tblName + "_stg";
    List<String> colNames = Arrays.asList("a", "b");
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("drop table if exists " + tblNameStg, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " PARTITIONED BY(bkt INT)" +
      " CLUSTERED BY(a) INTO 4 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("CREATE EXTERNAL TABLE " + tblNameStg + "(a INT, b STRING)" +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'" +
      " STORED AS TEXTFILE" +
      " LOCATION '" + stagingFolder.newFolder().toURI().getPath() + "'", driver);

    executeStatementOnDriver("load data local inpath '" + BASIC_FILE_NAME +
      "' overwrite into table " + tblNameStg, driver);
    execSelectAndDumpData("select * from " + tblNameStg, driver, "Dumping data for " +
      tblNameStg + " after load:");
    executeStatementOnDriver("FROM " + tblNameStg +
      " INSERT INTO TABLE " + tblName + " PARTITION(bkt=0) " +
      "SELECT a, b where a < 2", driver);
    executeStatementOnDriver("FROM " + tblNameStg +
      " INSERT INTO TABLE " + tblName + " PARTITION(bkt=1) " +
      "SELECT a, b where a >= 2", driver);
    execSelectAndDumpData("select * from " + tblName, driver, "Dumping data for " +
      tblName + " after load:");

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    CompactionInfo ci = new CompactionInfo("default", tblName, "bkt=0", CompactionType.MAJOR);
    LOG.debug("List of stats columns before analyze Part1: " + txnHandler.findColumnsWithStats(ci));
    Worker.StatsUpdater su = Worker.StatsUpdater.init(ci, colNames, conf,
      System.getProperty("user.name"));
    su.gatherStats();//compute stats before compaction
    LOG.debug("List of stats columns after analyze Part1: " + txnHandler.findColumnsWithStats(ci));

    CompactionInfo ciPart2 = new CompactionInfo("default", tblName, "bkt=1", CompactionType.MAJOR);
    LOG.debug("List of stats columns before analyze Part2: " + txnHandler.findColumnsWithStats(ci));
    su = Worker.StatsUpdater.init(ciPart2, colNames, conf, System.getProperty("user.name"));
    su.gatherStats();//compute stats before compaction
    LOG.debug("List of stats columns after analyze Part2: " + txnHandler.findColumnsWithStats(ci));

    //now make sure we get the stats we expect for partition we are going to add data to later
    Map<String, List<ColumnStatisticsObj>> stats = msClient.getPartitionColumnStatistics(ci.dbname,
      ci.tableName, Arrays.asList(ci.partName), colNames, Constants.HIVE_ENGINE);
    List<ColumnStatisticsObj> colStats = stats.get(ci.partName);
    assertNotNull("No stats found for partition " + ci.partName, colStats);
    Assert.assertEquals("Expected column 'a' at index 0", "a", colStats.get(0).getColName());
    Assert.assertEquals("Expected column 'b' at index 1", "b", colStats.get(1).getColName());
    LongColumnStatsData colAStats = colStats.get(0).getStatsData().getLongStats();
    Assert.assertEquals("lowValue a", 1, colAStats.getLowValue());
    Assert.assertEquals("highValue a", 1, colAStats.getHighValue());
    Assert.assertEquals("numNulls a", 0, colAStats.getNumNulls());
    Assert.assertEquals("numNdv a", 1, colAStats.getNumDVs());
    StringColumnStatsData colBStats = colStats.get(1).getStatsData().getStringStats();
    Assert.assertEquals("maxColLen b", 3, colBStats.getMaxColLen());
    Assert.assertEquals("avgColLen b", 3.0, colBStats.getAvgColLen(), 0.01);
    Assert.assertEquals("numNulls b", 0, colBStats.getNumNulls());
    Assert.assertEquals("nunDVs", 3, colBStats.getNumDVs());

    //now save stats for partition we won't modify
    stats = msClient.getPartitionColumnStatistics(ciPart2.dbname,
      ciPart2.tableName, Arrays.asList(ciPart2.partName), colNames, Constants.HIVE_ENGINE);
    colStats = stats.get(ciPart2.partName);
    LongColumnStatsData colAStatsPart2 = colStats.get(0).getStatsData().getLongStats();
    StringColumnStatsData colBStatsPart2 = colStats.get(1).getStatsData().getStringStats();

    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
      .withFieldDelimiter(',')
      .build();
    HiveStreamingConnection connection = HiveStreamingConnection.newBuilder()
      .withDatabase(ci.dbname)
      .withTable(ci.tableName)
      .withStaticPartitionValues(Arrays.asList("0"))
      .withAgentInfo("UT_" + Thread.currentThread().getName())
      .withHiveConf(conf)
      .withRecordWriter(writer)
      .connect();
    connection.beginTransaction();
    connection.write("50,Kiev".getBytes());
    connection.write("51,St. Petersburg".getBytes());
    connection.write("44,Boston".getBytes());
    connection.commitTransaction();

    connection.beginTransaction();
    connection.write("52,Tel Aviv".getBytes());
    connection.write("53,Atlantis".getBytes());
    connection.write("53,Boston".getBytes());
    connection.commitTransaction();
    connection.close();
    execSelectAndDumpData("select * from " + ci.getFullTableName(), driver, ci.getFullTableName());

    //so now we have written some new data to bkt=0 and it shows up
    CompactionRequest rqst = new CompactionRequest(ci.dbname, ci.tableName, CompactionType.MAJOR);
    rqst.setPartitionname(ci.partName);
    txnHandler.compact(rqst);
    runWorker(conf);
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    if (1 != compacts.size()) {
      Assert.fail("Expecting 1 file and found " + compacts.size() + " files " + compacts.toString());
    }
    Assert.assertEquals("ready for cleaning", compacts.get(0).getState());

    stats = msClient.getPartitionColumnStatistics(ci.dbname, ci.tableName,
      Arrays.asList(ci.partName), colNames, Constants.HIVE_ENGINE);
    colStats = stats.get(ci.partName);
    assertNotNull("No stats found for partition " + ci.partName, colStats);
    Assert.assertEquals("Expected column 'a' at index 0", "a", colStats.get(0).getColName());
    Assert.assertEquals("Expected column 'b' at index 1", "b", colStats.get(1).getColName());
    colAStats = colStats.get(0).getStatsData().getLongStats();
    Assert.assertEquals("lowValue a", 1, colAStats.getLowValue());
    Assert.assertEquals("highValue a", 53, colAStats.getHighValue());
    Assert.assertEquals("numNulls a", 0, colAStats.getNumNulls());
    Assert.assertEquals("numNdv a", 6, colAStats.getNumDVs());
    colBStats = colStats.get(1).getStatsData().getStringStats();
    Assert.assertEquals("maxColLen b", 14, colBStats.getMaxColLen());
    //cast it to long to get rid of periodic decimal
    Assert.assertEquals("avgColLen b", (long) 6.1111111111, (long) colBStats.getAvgColLen());
    Assert.assertEquals("numNulls b", 0, colBStats.getNumNulls());
    Assert.assertEquals("nunDVs", 8, colBStats.getNumDVs());

    //now check that stats for partition we didn't modify did not change
    stats = msClient.getPartitionColumnStatistics(ciPart2.dbname, ciPart2.tableName,
      Arrays.asList(ciPart2.partName), colNames, Constants.HIVE_ENGINE);
    colStats = stats.get(ciPart2.partName);
    Assert.assertEquals("Expected stats for " + ciPart2.partName + " to stay the same",
      colAStatsPart2, colStats.get(0).getStatsData().getLongStats());
    Assert.assertEquals("Expected stats for " + ciPart2.partName + " to stay the same",
      colBStatsPart2, colStats.get(1).getStatsData().getStringStats());
  }

  @Test
  public void dynamicPartitioningInsert() throws Exception {
    String tblName = "dpct";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " PARTITIONED BY(ds string)" +
      " CLUSTERED BY(a) INTO 2 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("insert into " + tblName + " partition (ds) values (1, 'fred', " +
      "'today'), (2, 'wilma', 'yesterday')", driver);

    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 0);
    runInitiator(conf);

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(2, compacts.size());
    SortedSet<String> partNames = new TreeSet<String>();
    verifyCompactions(compacts, partNames, tblName);
    List<String> names = new ArrayList<String>(partNames);
    Assert.assertEquals("ds=today", names.get(0));
    Assert.assertEquals("ds=yesterday", names.get(1));
  }

  @Test
  public void dynamicPartitioningUpdate() throws Exception {
    String tblName = "udpct";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " PARTITIONED BY(ds string)" +
      " CLUSTERED BY(a) INTO 2 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("insert into " + tblName + " partition (ds) values (1, 'fred', " +
      "'today'), (2, 'wilma', 'yesterday')", driver);

    executeStatementOnDriver("update " + tblName + " set b = 'barney'", driver);

    // Set to 1 so insert doesn't set it off but update does
    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 1);
    runInitiator(conf);

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(2, compacts.size());
    SortedSet<String> partNames = new TreeSet<String>();
    verifyCompactions(compacts, partNames, tblName);
    List<String> names = new ArrayList<String>(partNames);
    Assert.assertEquals("ds=today", names.get(0));
    Assert.assertEquals("ds=yesterday", names.get(1));
  }

  @Test
  public void dynamicPartitioningDelete() throws Exception {
    String tblName = "ddpct";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " PARTITIONED BY(ds string)" +
      " CLUSTERED BY(a) INTO 2 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("insert into " + tblName + " partition (ds) values (1, 'fred', " +
      "'today'), (2, 'wilma', 'yesterday')", driver);

    executeStatementOnDriver("update " + tblName + " set b = 'fred' where a = 1", driver);

    executeStatementOnDriver("delete from " + tblName + " where b = 'fred'", driver);

    // Set to 2 so insert and update don't set it off but delete does
    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 2);
    runInitiator(conf);

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    SortedSet<String> partNames = new TreeSet<String>();
    verifyCompactions(compacts, partNames, tblName);
    List<String> names = new ArrayList<String>(partNames);
    Assert.assertEquals("ds=today", names.get(0));
  }

  @Test
  public void minorCompactWhileStreaming() throws Exception {
    String dbName = "default";
    String tblName = "cws";
    String columnNamesProperty = "a,b";
    String columnTypesProperty = "int:string";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);

    StreamingConnection connection = null;
    try {
      // Write a couple of batches
      for (int i = 0; i < 2; i++) {
        CompactorTestUtil.writeBatch(conf, dbName, tblName, false, false);
      }

      // Start a third batch, but don't close it.
      connection = CompactorTestUtil.writeBatch(conf, dbName, tblName, false, true);

      // Now, compact
      TxnStore txnHandler = TxnUtils.getTxnStore(conf);
      txnHandler.compact(new CompactionRequest(dbName, tblName, CompactionType.MINOR));
      runWorker(conf);

      // Find the location of the table
      IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
      Table table = msClient.getTable(dbName, tblName);
      FileSystem fs = FileSystem.get(conf);
      FileStatus[] stat =
        fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.deltaFileFilter);
      String[] names = new String[stat.length];
      Path resultFile = null;
      for (int i = 0; i < names.length; i++) {
        names[i] = stat[i].getPath().getName();
        if (names[i].equals("delta_0000001_0000004_v0000009")) {
          resultFile = stat[i].getPath();
        }
      }
      Arrays.sort(names);
      String[] expected = new String[]{"delta_0000001_0000002",
        "delta_0000001_0000004_v0000009", "delta_0000003_0000004", "delta_0000005_0000006"};
      if (!Arrays.deepEquals(expected, names)) {
        Assert.fail("Expected: " + Arrays.toString(expected) + ", found: " + Arrays.toString(names) + ",stat="
            + CompactorTestUtil.printFileStatus(stat));
      }
      CompactorTestUtil
          .checkExpectedTxnsPresent(null, new Path[] {resultFile}, columnNamesProperty, columnTypesProperty, 0, 1L,
              4L, null, 1);

    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test
  public void majorCompactWhileStreaming() throws Exception {
    String dbName = "default";
    String tblName = "cws";
    String columnNamesProperty = "a,b";
    String columnTypesProperty = "int:string";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ('transactional'='true') ", driver);

    StreamingConnection connection = null;
    try {
      // Write a couple of batches
      for (int i = 0; i < 2; i++) {
        CompactorTestUtil.writeBatch(conf, dbName, tblName, false, false);
      }

      // Start a third batch, but don't close it.  this delta will be ignored by compaction since
      // it has an open txn in it
      connection = CompactorTestUtil.writeBatch(conf, dbName, tblName, false, true);

      runMajorCompaction(dbName, tblName);

      // Find the location of the table
      IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
      Table table = msClient.getTable(dbName, tblName);
      FileSystem fs = FileSystem.get(conf);
      FileStatus[] stat =
        fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.baseFileFilter);
      if (1 != stat.length) {
        Assert.fail("Expecting 1 file \"base_0000004\" and found " + stat.length + " files " + Arrays.toString(stat));
      }
      String name = stat[0].getPath().getName();
      Assert.assertEquals("base_0000004_v0000009", name);
      CompactorTestUtil
          .checkExpectedTxnsPresent(stat[0].getPath(), null, columnNamesProperty, columnTypesProperty, 0, 1L, 4L, null,
              1);
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test
  public void autoCompactOnStreamingIngestWithDynamicPartition() throws Exception {
    String dbName = "default";
    String tblName = "cws";
    String columnNamesProperty = "a,b";
    String columnTypesProperty = "string:int";
    String agentInfo = "UT_" + Thread.currentThread().getName();

    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a STRING) " +
        " PARTITIONED BY (b INT)" + //currently ACID requires table to be bucketed
        " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);

    StrictDelimitedInputWriter writer1 = StrictDelimitedInputWriter.newBuilder()
        .withFieldDelimiter(',')
        .build();
    StrictDelimitedInputWriter writer2 = StrictDelimitedInputWriter.newBuilder()
        .withFieldDelimiter(',')
        .build();

    StreamingConnection connection1 = HiveStreamingConnection.newBuilder()
        .withDatabase(dbName)
        .withTable(tblName)
        .withAgentInfo(agentInfo)
        .withHiveConf(conf)
        .withRecordWriter(writer1)
        .withStreamingOptimizations(true)
        // Transaction size has to be one or exception should happen.
        .withTransactionBatchSize(1)
        .connect();

    StreamingConnection connection2 = HiveStreamingConnection.newBuilder()
        .withDatabase(dbName)
        .withTable(tblName)
        .withAgentInfo(agentInfo)
        .withHiveConf(conf)
        .withRecordWriter(writer2)
        .withStreamingOptimizations(true)
        // Transaction size has to be one or exception should happen.
        .withTransactionBatchSize(1)
        .connect();

    try {
      connection1.beginTransaction();
      connection1.write("1,1".getBytes());
      connection1.commitTransaction();

      connection1.beginTransaction();
      connection1.write("1,1".getBytes());
      connection1.commitTransaction();
      connection1.close();

      conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 1);
      runInitiator(conf);

      TxnStore txnHandler = TxnUtils.getTxnStore(conf);
      ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());

      List<ShowCompactResponseElement> compacts1 = rsp.getCompacts();
      Assert.assertEquals(1, compacts1.size());
      SortedSet<String> partNames1 = new TreeSet<String>();
      verifyCompactions(compacts1, partNames1, tblName);
      List<String> names1 = new ArrayList<String>(partNames1);
      Assert.assertEquals("b=1", names1.get(0));

      runWorker(conf);
      runCleaner(conf);

      connection2.beginTransaction();
      connection2.write("1,1".getBytes());
      connection2.commitTransaction();

      connection2.beginTransaction();
      connection2.write("1,1".getBytes());
      connection2.commitTransaction();
      connection2.close();

      runInitiator(conf);

      List<ShowCompactResponseElement> compacts2 = rsp.getCompacts();
      Assert.assertEquals(1, compacts2.size());
      SortedSet<String> partNames2 = new TreeSet<String>();
      verifyCompactions(compacts2, partNames2, tblName);
      List<String> names2 = new ArrayList<String>(partNames2);
      Assert.assertEquals("b=1", names2.get(0));

      runWorker(conf);
      runCleaner(conf);

      // Find the location of the table
      IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
      Table table = msClient.getTable(dbName, tblName);
      String tablePath = table.getSd().getLocation();
      String partName = "b=1";
      Path partPath = new Path(tablePath, partName);
      FileSystem fs = FileSystem.get(conf);
      FileStatus[] stat = fs.listStatus(partPath, AcidUtils.baseFileFilter);
      if (1 != stat.length) {
        Assert.fail("Expecting 1 file \"base_0000004\" and found " + stat.length + " files " + Arrays.toString(stat));
      }
      String name = stat[0].getPath().getName();
      Assert.assertEquals("base_0000004_v0000008", name);
      CompactorTestUtil
          .checkExpectedTxnsPresent(stat[0].getPath(), null, columnNamesProperty, columnTypesProperty, 0, 1L, 4L, null,
              1);
    } finally {
      if (connection1 != null) {
        connection1.close();
      }
      if (connection2 != null) {
        connection2.close();
      }
    }
  }

  @Test
  public void minorCompactAfterAbort() throws Exception {
    String dbName = "default";
    String tblName = "cws";
    String columnNamesProperty = "a,b";
    String columnTypesProperty = "int:string";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);

    processStreamingAPI(dbName, tblName);

    // Now, compact
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    txnHandler.compact(new CompactionRequest(dbName, tblName, CompactionType.MINOR));
    runWorker(conf);

    // Find the location of the table
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tblName);
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat =
      fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.deltaFileFilter);
    String[] names = new String[stat.length];
    Path resultDelta = null;
    for (int i = 0; i < names.length; i++) {
      names[i] = stat[i].getPath().getName();
      if (names[i].equals("delta_0000001_0000004_v0000009")) {
        resultDelta = stat[i].getPath();
      }
    }
    Arrays.sort(names);
    String[] expected = new String[]{"delta_0000001_0000002",
      "delta_0000001_0000004_v0000009", "delta_0000003_0000004"};
    if (!Arrays.deepEquals(expected, names)) {
      Assert.fail("Expected: " + Arrays.toString(expected) + ", found: " + Arrays.toString(names));
    }
    CompactorTestUtil
        .checkExpectedTxnsPresent(null, new Path[] {resultDelta}, columnNamesProperty, columnTypesProperty, 0, 1L, 4L,
            Lists.newArrayList(5, 6), 1);
  }

  @Test
  public void majorCompactAfterAbort() throws Exception {
    String dbName = "default";
    String tblName = "cws";
    String columnNamesProperty = "a,b";
    String columnTypesProperty = "int:string";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);

    processStreamingAPI(dbName, tblName);
    runMajorCompaction(dbName, tblName);

    // Find the location of the table
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tblName);
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat =
      fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.baseFileFilter);
    if (1 != stat.length) {
      Assert.fail("Expecting 1 file \"base_0000004\" and found " + stat.length + " files " + Arrays.toString(stat));
    }
    String name = stat[0].getPath().getName();
    if (!name.equals("base_0000004_v0000009")) {
      Assert.fail("majorCompactAfterAbort name " + name + " not equals to base_0000004");
    }
    CompactorTestUtil
        .checkExpectedTxnsPresent(stat[0].getPath(), null, columnNamesProperty, columnTypesProperty, 0, 1L, 4L,
            Lists.newArrayList(5, 6), 1);
  }

  @Test
  public void testCleanAbortCompactAfter2ndCommitAbort() throws Exception {
    String dbName = "default";
    String tblName = "cws";

    HiveStreamingConnection connection = prepareTableAndConnection(dbName, tblName, 2);

    connection.beginTransaction();
    connection.write("1,1".getBytes());
    connection.write("2,2".getBytes());
    connection.commitTransaction();

    connection.beginTransaction();
    connection.write("3,2".getBytes());
    connection.write("3,3".getBytes());
    connection.abortTransaction();

    assertAndCompactCleanAbort(dbName, tblName, true, true);
    connection.close();
  }

  @Test
  public void testCleanAbortCompactAfter1stCommitAbort() throws Exception {
    String dbName = "default";
    String tblName = "cws";

    HiveStreamingConnection connection = prepareTableAndConnection(dbName, tblName, 2);

    connection.beginTransaction();
    connection.write("1,1".getBytes());
    connection.write("2,2".getBytes());
    connection.abortTransaction();

    connection.beginTransaction();
    connection.write("3,2".getBytes());
    connection.write("3,3".getBytes());
    connection.commitTransaction();

    assertAndCompactCleanAbort(dbName, tblName, true, true);
    connection.close();
  }

  @Test
  public void testCleanAbortCompactAfterAbortTwoPartitions() throws Exception {
    String dbName = "default";
    String tblName = "cws";

    HiveStreamingConnection connection1 = prepareTableTwoPartitionsAndConnection(dbName, tblName, 1);
    HiveStreamingConnection connection2 = prepareTableTwoPartitionsAndConnection(dbName, tblName, 1);

    // to skip optimization introduced in HIVE-22122
    executeStatementOnDriver("truncate " + tblName, driver);

    connection1.beginTransaction();
    connection1.write("1,1".getBytes());
    connection1.write("2,2".getBytes());
    connection1.abortTransaction();

    connection2.beginTransaction();
    connection2.write("1,3".getBytes());
    connection2.write("2,3".getBytes());
    connection2.write("3,3".getBytes());
    connection2.abortTransaction();

    assertAndCompactCleanAbort(dbName, tblName, false, false);

    connection1.close();
    connection2.close();
  }

  @Test
  public void testCleanAbortCompactAfterAbort() throws Exception {
    String dbName = "default";
    String tblName = "cws";

    // Create three folders with two different transactions
    HiveStreamingConnection connection1 = prepareTableAndConnection(dbName, tblName, 1);
    HiveStreamingConnection connection2 = prepareTableAndConnection(dbName, tblName, 1);

    // to skip optimization introduced in HIVE-22122
    executeStatementOnDriver("truncate " + tblName, driver);

    connection1.beginTransaction();
    connection1.write("1,1".getBytes());
    connection1.write("2,2".getBytes());
    connection1.abortTransaction();

    connection2.beginTransaction();
    connection2.write("1,3".getBytes());
    connection2.write("2,3".getBytes());
    connection2.write("3,3".getBytes());
    connection2.abortTransaction();

    assertAndCompactCleanAbort(dbName, tblName, false, false);

    connection1.close();
    connection2.close();
  }

  private void assertAndCompactCleanAbort(String dbName, String tblName, boolean partialAbort, boolean singleSession) throws Exception {
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    Table table = msClient.getTable(dbName, tblName);
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat =
        fs.listStatus(new Path(table.getSd().getLocation()));
    if (3 != stat.length) {
      Assert.fail("Expecting three directories corresponding to three partitions, FileStatus[] stat " + Arrays.toString(stat));
    }

    int count = TxnDbUtil.countQueryAgent(conf, "select count(*) from TXN_COMPONENTS where TC_OPERATION_TYPE='i'");
    // We should have two rows corresponding to the two aborted transactions
    Assert.assertEquals(TxnDbUtil.queryToString(conf, "select * from TXN_COMPONENTS"), partialAbort ? 1 : 2, count);

    runInitiator(conf);
    count = TxnDbUtil.countQueryAgent(conf, "select count(*) from COMPACTION_QUEUE");
    // Only one job is added to the queue per table. This job corresponds to all the entries for a particular table
    // with rows in TXN_COMPONENTS
    Assert.assertEquals(TxnDbUtil.queryToString(conf, "select * from COMPACTION_QUEUE"), 1, count);
    runWorker(conf);

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompacts().size());
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, rsp.getCompacts().get(0).getState());
    Assert.assertEquals("cws", rsp.getCompacts().get(0).getTablename());
    Assert.assertEquals(CompactionType.MINOR, rsp.getCompacts().get(0).getType());

    runCleaner(conf);

    // After the cleaner runs TXN_COMPONENTS and COMPACTION_QUEUE should have zero rows, also the folders should have been deleted.
    count = TxnDbUtil.countQueryAgent(conf, "select count(*) from TXN_COMPONENTS");
    Assert.assertEquals(TxnDbUtil.queryToString(conf, "select * from TXN_COMPONENTS"), (singleSession && partialAbort) ? 1 : 0, count);

    count = TxnDbUtil.countQueryAgent(conf, "select count(*) from COMPACTION_QUEUE");
    Assert.assertEquals(TxnDbUtil.queryToString(conf, "select * from COMPACTION_QUEUE"), 0, count);

    RemoteIterator it =
        fs.listFiles(new Path(table.getSd().getLocation()), true);
    if (it.hasNext() && !partialAbort) {
      Assert.fail("Expected cleaner to drop aborted delta & base directories, FileStatus[] stat " + Arrays.toString(stat));
    }

    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompacts().size());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());
    Assert.assertEquals("cws", rsp.getCompacts().get(0).getTablename());
    Assert.assertEquals(CompactionType.MINOR, rsp.getCompacts().get(0).getType());
  }

  @Test
  public void testCleanAbortCompactSeveralTables() throws Exception {
    String dbName = "default";
    String tblName1 = "cws1";
    String tblName2 = "cws2";

    HiveStreamingConnection connection1 = prepareTableAndConnection(dbName, tblName1, 1);
    HiveStreamingConnection connection2 = prepareTableAndConnection(dbName, tblName2, 1);

    connection1.beginTransaction();
    connection1.write("1,1".getBytes());
    connection1.write("2,2".getBytes());
    connection1.abortTransaction();

    connection2.beginTransaction();
    connection2.write("1,1".getBytes());
    connection2.write("2,2".getBytes());
    connection2.abortTransaction();

    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    FileSystem fs = FileSystem.get(conf);
    Table table1 = msClient.getTable(dbName, tblName1);
    FileStatus[] stat =
        fs.listStatus(new Path(table1.getSd().getLocation()));
    if (2 != stat.length) {
      Assert.fail("Expecting two directories corresponding to two partitions, FileStatus[] stat " + Arrays.toString(stat));
    }
    Table table2 = msClient.getTable(dbName, tblName2);
    stat = fs.listStatus(new Path(table2.getSd().getLocation()));
    if (2 != stat.length) {
      Assert.fail("Expecting two directories corresponding to two partitions, FileStatus[] stat " + Arrays.toString(stat));
    }

    int count = TxnDbUtil.countQueryAgent(conf, "select count(*) from TXN_COMPONENTS where TC_OPERATION_TYPE='i'");
    // We should have two rows corresponding to the two aborted transactions
    Assert.assertEquals(TxnDbUtil.queryToString(conf, "select * from TXN_COMPONENTS"), 2, count);

    runInitiator(conf);
    count = TxnDbUtil.countQueryAgent(conf, "select count(*) from COMPACTION_QUEUE");
    // Only one job is added to the queue per table. This job corresponds to all the entries for a particular table
    // with rows in TXN_COMPONENTS
    Assert.assertEquals(TxnDbUtil.queryToString(conf, "select * from COMPACTION_QUEUE"), 2, count);

    runWorker(conf);
    runWorker(conf);

    runCleaner(conf);
    // After the cleaner runs TXN_COMPONENTS and COMPACTION_QUEUE should have zero rows, also the folders should have been deleted.
    count = TxnDbUtil.countQueryAgent(conf, "select count(*) from TXN_COMPONENTS");
    Assert.assertEquals(TxnDbUtil.queryToString(conf, "select * from TXN_COMPONENTS"), 0, count);

    count = TxnDbUtil.countQueryAgent(conf, "select count(*) from COMPACTION_QUEUE");
    Assert.assertEquals(TxnDbUtil.queryToString(conf, "select * from COMPACTION_QUEUE"), 0, count);

    RemoteIterator it =
        fs.listFiles(new Path(table1.getSd().getLocation()), true);
    if (it.hasNext()) {
      Assert.fail("Expected cleaner to drop aborted delta & base directories, FileStatus[] stat " + Arrays.toString(stat));
    }

    connection1.close();
    connection2.close();
  }

  @Test
  public void testCleanAbortCorrectlyCleaned() throws Exception {
    // Test that at commit the tables are cleaned properly
    String dbName = "default";
    String tblName = "cws";
    HiveStreamingConnection connection = prepareTableAndConnection(dbName, tblName, 1);
    connection.beginTransaction();
    connection.write("1,1".getBytes());
    connection.write("2,2".getBytes());

    int count = TxnDbUtil.countQueryAgent(conf, "select count(*) from TXN_COMPONENTS where TC_OPERATION_TYPE='i'");
    // We should have 1 row corresponding to the aborted transaction
    Assert.assertEquals(TxnDbUtil.queryToString(conf, "select * from TXN_COMPONENTS"), 1, count);

    connection.commitTransaction();

    // After commit the row should have been deleted
    count = TxnDbUtil.countQueryAgent(conf, "select count(*) from TXN_COMPONENTS where TC_OPERATION_TYPE='i'");
    Assert.assertEquals(TxnDbUtil.queryToString(conf, "select * from TXN_COMPONENTS"), 0, count);
  }

  @Test
  public void testCleanAbortAndMinorCompact() throws Exception {
    String dbName = "default";
    String tblName = "cws";

    HiveStreamingConnection connection = prepareTableAndConnection(dbName, tblName, 1);

    connection.beginTransaction();
    connection.write("1,1".getBytes());
    connection.write("2,2".getBytes());
    connection.abortTransaction();

    executeStatementOnDriver("insert into " + tblName + " partition (a) values (1, '1')", driver);
    executeStatementOnDriver("delete from " + tblName + " where b=1", driver);

    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 0);
    runInitiator(conf);

    int count = TxnDbUtil.countQueryAgent(conf, "select count(*) from COMPACTION_QUEUE");
    Assert.assertEquals(TxnDbUtil.queryToString(conf, "select * from COMPACTION_QUEUE"), 2, count);

    runWorker(conf);
    runWorker(conf);

    // Cleaning should happen in threads concurrently for the minor compaction and the clean abort one.
    runCleaner(conf);

    count = TxnDbUtil.countQueryAgent(conf, "select count(*) from COMPACTION_QUEUE");
    Assert.assertEquals(TxnDbUtil.queryToString(conf, "select * from COMPACTION_QUEUE"), 0, count);

    count = TxnDbUtil.countQueryAgent(conf, "select count(*) from TXN_COMPONENTS");
    Assert.assertEquals(TxnDbUtil.queryToString(conf, "select * from TXN_COMPONENTS"), 0, count);

  }

  private HiveStreamingConnection prepareTableAndConnection(String dbName, String tblName, int batchSize) throws Exception {
    String agentInfo = "UT_" + Thread.currentThread().getName();

    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(b STRING) " +
        " PARTITIONED BY (a INT)" + //currently ACID requires table to be bucketed
        " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);

    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
        .withFieldDelimiter(',')
        .build();

    // Create three folders with two different transactions
    return HiveStreamingConnection.newBuilder()
        .withDatabase(dbName)
        .withTable(tblName)
        .withAgentInfo(agentInfo)
        .withHiveConf(conf)
        .withRecordWriter(writer)
        .withStreamingOptimizations(true)
        // Transaction size has to be one or exception should happen.
        .withTransactionBatchSize(batchSize)
        .connect();
  }

  private HiveStreamingConnection prepareTableTwoPartitionsAndConnection(String dbName, String tblName, int batchSize) throws Exception {
    String agentInfo = "UT_" + Thread.currentThread().getName();

    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(c STRING) " +
        " PARTITIONED BY (a INT, b INT)" + //currently ACID requires table to be bucketed
        " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);

    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
        .withFieldDelimiter(',')
        .build();

    // Create three folders with two different transactions
    return HiveStreamingConnection.newBuilder()
        .withDatabase(dbName)
        .withTable(tblName)
        .withAgentInfo(agentInfo)
        .withHiveConf(conf)
        .withRecordWriter(writer)
        .withStreamingOptimizations(true)
        // Transaction size has to be one or exception should happen.
        .withTransactionBatchSize(batchSize)
        .connect();
  }

  /**
   * There is a special case handled in Compaction Worker that will skip compaction
   * if there is only one valid delta. But this compaction will be still cleaned up, if there are aborted directories.
   * @see Worker.isEnoughToCompact
   * However if no compaction was done, deltas containing mixed aborted / committed writes from streaming can not be cleaned
   * and the metadata belonging to those aborted transactions can not be removed.
   * @throws Exception ex
   */
  @Test
  public void testSkippedCompactionCleanerKeepsAborted() throws Exception {
    String dbName = "default";
    String tblName = "cws";

    String agentInfo = "UT_" + Thread.currentThread().getName();
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);

    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(b STRING) " +
        " PARTITIONED BY (a INT) STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("alter table " + tblName + " add partition(a=1)", driver);

    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
        .withFieldDelimiter(',')
        .build();

    // Create initial aborted txn
    HiveStreamingConnection connection = HiveStreamingConnection.newBuilder()
        .withDatabase(dbName)
        .withTable(tblName)
        .withStaticPartitionValues(Collections.singletonList("1"))
        .withAgentInfo(agentInfo)
        .withHiveConf(conf)
        .withRecordWriter(writer)
        .withStreamingOptimizations(true)
        .withTransactionBatchSize(1)
        .connect();

    connection.beginTransaction();
    connection.write("3,1".getBytes());
    connection.write("4,1".getBytes());
    connection.abortTransaction();

    connection.close();

    // Create a sequence of commit, abort, commit to the same delta folder
    connection = HiveStreamingConnection.newBuilder()
        .withDatabase(dbName)
        .withTable(tblName)
        .withStaticPartitionValues(Collections.singletonList("1"))
        .withAgentInfo(agentInfo)
        .withHiveConf(conf)
        .withRecordWriter(writer)
        .withStreamingOptimizations(true)
        .withTransactionBatchSize(3)
        .connect();

    connection.beginTransaction();
    connection.write("1,1".getBytes());
    connection.write("2,1".getBytes());
    connection.commitTransaction();

    connection.beginTransaction();
    connection.write("3,1".getBytes());
    connection.write("4,1".getBytes());
    connection.abortTransaction();

    connection.beginTransaction();
    connection.write("5,1".getBytes());
    connection.write("6,1".getBytes());
    connection.commitTransaction();

    connection.close();

    // Check that aborted are not read back
    driver.run("select * from cws");
    List res = new ArrayList();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals(4, res.size());

    int count = TxnDbUtil.countQueryAgent(conf, "select count(*) from TXN_COMPONENTS");
    Assert.assertEquals("There should be 2 record for two aborted transaction", 2, count);

    // Start a compaction, that will be skipped, because only one valid delta is there
    driver.run("alter table cws partition(a='1') compact 'minor'");
    runWorker(conf);
    // Cleaner should not delete info about aborted txn 2
    runCleaner(conf);
    txnHandler.cleanEmptyAbortedAndCommittedTxns();
    count = TxnDbUtil.countQueryAgent(conf, "select count(*) from TXN_COMPONENTS");
    Assert.assertEquals("There should be 1 record for the second aborted transaction", 1, count);

    driver.run("select * from cws");
    res.clear();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals(4, res.size());

  }

  @Test
  public void mmTable() throws Exception {
    String dbName = "default";
    String tblName = "mm_nonpart";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) STORED AS ORC" +
        " TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')",
        driver);
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tblName);
    msClient.close();

    executeStatementOnDriver("INSERT INTO " + tblName +"(a,b) VALUES(1, 'foo')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName +"(a,b) VALUES(2, 'bar')", driver);

    verifyFooBarResult(tblName, 1);

    // Check that we have two deltas.
    FileSystem fs = FileSystem.get(conf);
    verifyDeltaCount(table.getSd(), fs, 2);

    runMajorCompaction(dbName, tblName);
    verifyFooBarResult(tblName, 1);
    verifyHasBase(table.getSd(), fs, "base_0000002_v0000006");

    // Make sure we don't compact if we don't need to compact.
    runMajorCompaction(dbName, tblName);
    verifyFooBarResult(tblName, 1);
    verifyHasBase(table.getSd(), fs, "base_0000002_v0000006");
  }

  @Test public void mmTableOriginalsMajorOrc() throws Exception {
    mmTableOriginalsMajor("orc", true);
  }

  @Test public void mmTableOriginalsMajorText() throws Exception {
    mmTableOriginalsMajor("textfile", false);
  }

  /**
   * Major compact an mm table that contains original files.
   */
  private void mmTableOriginalsMajor(String format, boolean allowOriginals) throws Exception {
    driver.getConf().setBoolVar(ConfVars.HIVE_MM_ALLOW_ORIGINALS, allowOriginals);
    String dbName = "default";
    String tblName = "mm_nonpart";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) STORED AS " + format
        + " TBLPROPERTIES ('transactional'='false')", driver);
    Table table = msClient.getTable(dbName, tblName);

    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) VALUES(1, 'foo')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + " (a,b) VALUES(2, 'bar')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) SELECT a,b FROM " + tblName
        + " UNION ALL SELECT a,b FROM " + tblName, driver);

    verifyFooBarResult(tblName, 3);

    FileSystem fs = FileSystem.get(conf);
    executeStatementOnDriver("ALTER TABLE " + tblName + " SET TBLPROPERTIES "
        + "('transactional'='true', 'transactional_properties'='insert_only')", driver);

    if (allowOriginals) {
      verifyDeltaCount(table.getSd(), fs, 0);
      verifyFooBarResult(tblName, 3);

      runMajorCompaction(dbName, tblName);
      verifyFooBarResult(tblName, 3);
      verifyHasBase(table.getSd(), fs, "base_0000001_v0000009");
    } else {
      verifyDeltaCount(table.getSd(), fs, 1);
      // 1 delta dir won't be compacted. Skip testing major compaction.
    }
  }

  @Test public void mmMajorOriginalsDeltasOrc() throws Exception {
    mmMajorOriginalsDeltas("orc", true);
  }

  @Test public void mmMajorOriginalsDeltasText() throws Exception {
    mmMajorOriginalsDeltas("textfile", false);
  }

  /**
   * Major compact an mm table with both original and delta files.
   */
  private void mmMajorOriginalsDeltas(String format, boolean allowOriginals) throws Exception {
    driver.getConf().setBoolVar(ConfVars.HIVE_MM_ALLOW_ORIGINALS, allowOriginals);
    String dbName = "default";
    String tblName = "mm_nonpart";
    FileSystem fs = FileSystem.get(conf);
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) STORED AS " + format
        + " TBLPROPERTIES ('transactional'='false')", driver);
    Table table = msClient.getTable(dbName, tblName);

    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) VALUES(1, 'foo')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + " (a,b) VALUES(2, 'bar')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) SELECT a,b FROM " + tblName
        + " UNION ALL SELECT a,b FROM " + tblName, driver);
    verifyFooBarResult(tblName, 3);

    executeStatementOnDriver("ALTER TABLE " + tblName + " SET TBLPROPERTIES "
        + "('transactional'='true', 'transactional_properties'='insert_only')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) SELECT a,b FROM " + tblName
        + " UNION ALL SELECT a,b FROM " + tblName, driver);

    verifyFooBarResult(tblName, 9);

    runMajorCompaction(dbName, tblName);
    verifyFooBarResult(tblName, 9);
    verifyHasBase(table.getSd(), fs, "base_0000002_v0000010");
  }

  @Test public void mmMajorOriginalsBaseOrc() throws Exception {
    mmMajorOriginalsBase("orc", true);
  }

  @Test public void mmMajorOriginalsBaseText() throws Exception {
    mmMajorOriginalsBase("textfile", false);
  }

  /**
   * Insert overwrite and major compact an mm table with only original files.
   *
   * @param format file format for table
   * @throws Exception
   */
  private void mmMajorOriginalsBase(String format, boolean allowOriginals) throws Exception {
    driver.getConf().setBoolVar(ConfVars.HIVE_MM_ALLOW_ORIGINALS, allowOriginals);
    // Try with an extra base.
    String dbName = "default";
    String tblName = "mm_nonpart";
    FileSystem fs = FileSystem.get(conf);
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) STORED AS " + format
        + " TBLPROPERTIES ('transactional'='false')", driver);
    Table table = msClient.getTable(dbName, tblName);

    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) VALUES(1, 'foo')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + " (a,b) VALUES(2, 'bar')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) SELECT a,b FROM " + tblName
        + " UNION ALL SELECT a,b FROM " + tblName, driver);
    verifyFooBarResult(tblName, 3);

    executeStatementOnDriver("ALTER TABLE " + tblName + " SET TBLPROPERTIES "
        + "('transactional'='true', 'transactional_properties'='insert_only')", driver);
    executeStatementOnDriver("INSERT OVERWRITE TABLE " + tblName + " SELECT a,b FROM " + tblName
        + " UNION ALL SELECT a,b FROM " + tblName, driver);
    verifyFooBarResult(tblName, 6);

    runMajorCompaction(dbName, tblName);
    verifyFooBarResult(tblName, 6);
    verifyHasBase(table.getSd(), fs, "base_0000002");
  }

  @Test
  public void mmTableBucketed() throws Exception {
    String dbName = "default";
    String tblName = "mm_nonpart";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) CLUSTERED BY (a) " +
        "INTO 64 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true', " +
        "'transactional_properties'='insert_only')", driver);
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tblName);
    msClient.close();

    executeStatementOnDriver("INSERT INTO " + tblName +"(a,b) VALUES(1, 'foo')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName +"(a,b) VALUES(2, 'bar')", driver);

    verifyFooBarResult(tblName, 1);

    // Check that we have two deltas.
    FileSystem fs = FileSystem.get(conf);
    verifyDeltaCount(table.getSd(), fs, 2);

    runMajorCompaction(dbName, tblName);
    verifyFooBarResult(tblName, 1);
    String baseDir = "base_0000002_v0000006";
    verifyHasBase(table.getSd(), fs, baseDir);

    FileStatus[] files = fs.listStatus(new Path(table.getSd().getLocation(), baseDir),
        AcidUtils.hiddenFileFilter);
    Assert.assertEquals(Lists.newArrayList(files).toString(), 64, files.length);
  }

  @Test
  public void mmTableOpenWriteId() throws Exception {
    String dbName = "default";
    String tblName = "mm_nonpart";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) STORED AS TEXTFILE" +
        " TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')",
        driver);
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tblName);
    msClient.close();

    executeStatementOnDriver("INSERT INTO " + tblName +"(a,b) VALUES(1, 'foo')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName +"(a,b) VALUES(2, 'bar')", driver);

    verifyFooBarResult(tblName, 1);

    long openTxnId = msClient.openTxn("test");
    long openWriteId = msClient.allocateTableWriteId(openTxnId, dbName, tblName);
    Assert.assertEquals(3, openWriteId); // Just check to make sure base_5 below is not new.

    executeStatementOnDriver("INSERT INTO " + tblName +"(a,b) VALUES(1, 'foo')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName +"(a,b) VALUES(2, 'bar')", driver);

    verifyFooBarResult(tblName, 2);

    runMajorCompaction(dbName, tblName); // Don't compact 4 and 5; 3 is opened.
    FileSystem fs = FileSystem.get(conf);
    verifyHasBase(table.getSd(), fs, "base_0000002_v0000010");
    verifyDirCount(table.getSd(), fs, 1, AcidUtils.baseFileFilter);
    verifyFooBarResult(tblName, 2);

    runCleaner(conf);
    verifyHasDir(table.getSd(), fs, "delta_0000004_0000004_0000", AcidUtils.deltaFileFilter);
    verifyHasDir(table.getSd(), fs, "delta_0000005_0000005_0000", AcidUtils.deltaFileFilter);
    verifyFooBarResult(tblName, 2);

    msClient.abortTxns(Lists.newArrayList(openTxnId)); // Now abort 3.
    runMajorCompaction(dbName, tblName); // Compact 4 and 5.
    verifyFooBarResult(tblName, 2);
    verifyHasBase(table.getSd(), fs, "base_0000005_v0000016");
    runCleaner(conf);
    verifyDeltaCount(table.getSd(), fs, 0);
  }

  private void verifyHasBase(
      StorageDescriptor sd, FileSystem fs, String baseName) throws Exception {
    verifyHasDir(sd, fs, baseName, AcidUtils.baseFileFilter);
  }

  private void verifyHasDir(
      StorageDescriptor sd, FileSystem fs, String name, PathFilter filter) throws Exception {
    FileStatus[] stat = fs.listStatus(new Path(sd.getLocation()), filter);
    for (FileStatus file : stat) {
      if (name.equals(file.getPath().getName())) return;
    }
    Assert.fail("Cannot find " + name + ": " + Arrays.toString(stat));
  }

  private void verifyDeltaCount(
      StorageDescriptor sd, FileSystem fs, int count) throws Exception {
    verifyDirCount(sd, fs, count, AcidUtils.deltaFileFilter);
  }

  private void verifyDirCount(
      StorageDescriptor sd, FileSystem fs, int count, PathFilter filter) throws Exception {
    FileStatus[] stat = fs.listStatus(new Path(sd.getLocation()), filter);
    Assert.assertEquals(Arrays.toString(stat), count, stat.length);
  }

  @Test
  public void mmTablePartitioned() throws Exception {
    String dbName = "default";
    String tblName = "mm_part";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
        " PARTITIONED BY(ds int) STORED AS TEXTFILE" +
        " TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')",
        driver);

    executeStatementOnDriver("INSERT INTO " + tblName + " partition (ds) VALUES(1, 'foo', 1)", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + " partition (ds) VALUES(1, 'foo', 1)", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + " partition (ds) VALUES(2, 'bar', 1)", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + " partition (ds) VALUES(1, 'foo', 2)", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + " partition (ds) VALUES(2, 'bar', 2)", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + " partition (ds) VALUES(2, 'bar', 3)", driver);

    verifyFooBarResult(tblName, 3);

    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Partition p1 = msClient.getPartition(dbName, tblName, "ds=1"),
        p2 = msClient.getPartition(dbName, tblName, "ds=2"),
        p3 = msClient.getPartition(dbName, tblName, "ds=3");
    msClient.close();

    FileSystem fs = FileSystem.get(conf);
    verifyDeltaCount(p1.getSd(), fs, 3);
    verifyDeltaCount(p2.getSd(), fs, 2);
    verifyDeltaCount(p3.getSd(), fs, 1);

    runMajorCompaction(dbName, tblName, "ds=1", "ds=2", "ds=3");

    verifyFooBarResult(tblName, 3);
    verifyDeltaCount(p3.getSd(), fs, 1);
    verifyHasBase(p1.getSd(), fs, "base_0000006_v0000010");
    verifyHasBase(p2.getSd(), fs, "base_0000006_v0000014");

    executeStatementOnDriver("INSERT INTO " + tblName + " partition (ds) VALUES(1, 'foo', 2)", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + " partition (ds) VALUES(2, 'bar', 2)", driver);

    runMajorCompaction(dbName, tblName, "ds=1", "ds=2", "ds=3");

    // Make sure we don't compact if we don't need to compact; but do if we do.
    verifyFooBarResult(tblName, 4);
    verifyDeltaCount(p3.getSd(), fs, 1);
    verifyHasBase(p1.getSd(), fs, "base_0000006_v0000010");
    verifyHasBase(p2.getSd(), fs, "base_0000008_v0000023");

  }

  private void verifyFooBarResult(String tblName, int count) throws Exception {
    List<String> valuesReadFromHiveDriver = new ArrayList<>();
    executeStatementOnDriver("SELECT a,b FROM " + tblName, driver);
    driver.getResults(valuesReadFromHiveDriver);
    Assert.assertEquals(2 * count, valuesReadFromHiveDriver.size());
    int fooCount = 0, barCount = 0;
    for (String s : valuesReadFromHiveDriver) {
      if ("1\tfoo".equals(s)) {
        ++fooCount;
      } else if ("2\tbar".equals(s)) {
        ++barCount;
      } else {
        Assert.fail("Unexpected " + s);
      }
    }
    Assert.assertEquals(fooCount, count);
    Assert.assertEquals(barCount, count);
  }

  private void runMajorCompaction(
      String dbName, String tblName, String... partNames) throws Exception {
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setConf(conf);
    t.init(new AtomicBoolean(true));
    if (partNames.length == 0) {
      txnHandler.compact(new CompactionRequest(dbName, tblName, CompactionType.MAJOR));
      t.run();
    } else {
      for (String partName : partNames) {
        CompactionRequest cr = new CompactionRequest(dbName, tblName, CompactionType.MAJOR);
        cr.setPartitionname(partName);
        txnHandler.compact(cr);
        t.run();
      }
    }
  }

  @Test
  public void majorCompactWhileStreamingForSplitUpdate() throws Exception {
    String dbName = "default";
    String tblName = "cws";
    String columnNamesProperty = "a,b";
    String columnTypesProperty = "int:string";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 2 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ('transactional'='true', "
      + "'transactional_properties'='default') ", driver); // this turns on split-update U=D+I

    // Write a couple of batches
    for (int i = 0; i < 2; i++) {
      CompactorTestUtil.writeBatch(conf, dbName, tblName, false, false);
    }

    // Start a third batch, but don't close it.
    StreamingConnection connection1 = CompactorTestUtil.writeBatch(conf, dbName, tblName, false, true);

    runMajorCompaction(dbName, tblName);

    // Find the location of the table
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tblName);
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat =
      fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.baseFileFilter);
    if (1 != stat.length) {
      Assert.fail("Expecting 1 file \"base_0000004\" and found " + stat.length + " files " + Arrays.toString(stat));
    }
    String name = stat[0].getPath().getName();
    Assert.assertEquals("base_0000004_v0000009", name);
    CompactorTestUtil
        .checkExpectedTxnsPresent(stat[0].getPath(), null, columnNamesProperty, columnTypesProperty, 1, 1L, 4L, null,
            2);
    if (connection1 != null) {
      connection1.close();
    }
  }

  @Test
  public void testMinorCompactionForSplitUpdateWithInsertsAndDeletes() throws Exception {
    String dbName = "default";
    String tblName = "cws";
    String columnNamesProperty = "a,b";
    String columnTypesProperty = "int:string";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ('transactional'='true',"
      + "'transactional_properties'='default')", driver);

    // Insert some data -> this will generate only insert deltas and no delete deltas: delta_3_3
    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) VALUES(1, 'foo')", driver);

    // Insert some data -> this will again generate only insert deltas and no delete deltas: delta_4_4
    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) VALUES(2, 'bar')", driver);

    // Delete some data -> this will generate only delete deltas and no insert deltas: delete_delta_5_5
    executeStatementOnDriver("DELETE FROM " + tblName + " WHERE a = 2", driver);

    // Now, compact -> Compaction produces a single range for both delta and delete delta
    // That is, both delta and delete_deltas would be compacted into delta_3_5 and delete_delta_3_5
    // even though there are only two delta_3_3, delta_4_4 and one delete_delta_5_5.
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    txnHandler.compact(new CompactionRequest(dbName, tblName, CompactionType.MINOR));
    runWorker(conf);

    // Find the location of the table
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tblName);
    FileSystem fs = FileSystem.get(conf);

    // Verify that we have got correct set of deltas.
    FileStatus[] stat =
      fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.deltaFileFilter);
    String[] deltas = new String[stat.length];
    Path minorCompactedDelta = null;
    for (int i = 0; i < deltas.length; i++) {
      deltas[i] = stat[i].getPath().getName();
      if (deltas[i].equals("delta_0000001_0000003_v0000006")) {
        minorCompactedDelta = stat[i].getPath();
      }
    }
    Arrays.sort(deltas);
    String[] expectedDeltas = new String[]{"delta_0000001_0000001_0000", "delta_0000001_0000003_v0000006",
      "delta_0000002_0000002_0000"};
    if (!Arrays.deepEquals(expectedDeltas, deltas)) {
      Assert.fail("Expected: " + Arrays.toString(expectedDeltas) + ", found: " + Arrays.toString(deltas));
    }
    CompactorTestUtil
        .checkExpectedTxnsPresent(null, new Path[] {minorCompactedDelta}, columnNamesProperty, columnTypesProperty, 0,
            1L, 2L, null, 1);

    // Verify that we have got correct set of delete_deltas.
    FileStatus[] deleteDeltaStat =
      fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.deleteEventDeltaDirFilter);
    String[] deleteDeltas = new String[deleteDeltaStat.length];
    Path minorCompactedDeleteDelta = null;
    for (int i = 0; i < deleteDeltas.length; i++) {
      deleteDeltas[i] = deleteDeltaStat[i].getPath().getName();
      if (deleteDeltas[i].equals("delete_delta_0000001_0000003_v0000006")) {
        minorCompactedDeleteDelta = deleteDeltaStat[i].getPath();
      }
    }
    Arrays.sort(deleteDeltas);
    String[] expectedDeleteDeltas = new String[]{"delete_delta_0000001_0000003_v0000006", "delete_delta_0000003_0000003_0000"};
    if (!Arrays.deepEquals(expectedDeleteDeltas, deleteDeltas)) {
      Assert.fail("Expected: " + Arrays.toString(expectedDeleteDeltas) + ", found: " + Arrays.toString(deleteDeltas));
    }
    CompactorTestUtil.checkExpectedTxnsPresent(null, new Path[] {minorCompactedDeleteDelta}, columnNamesProperty,
        columnTypesProperty, 0, 2L, 2L, null, 1);
  }

  @Test
  public void testMinorCompactionForSplitUpdateWithOnlyInserts() throws Exception {
    String dbName = "default";
    String tblName = "cws";
    String columnNamesProperty = "a,b";
    String columnTypesProperty = "int:string";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ('transactional'='true',"
      + "'transactional_properties'='default')", driver);

    // Insert some data -> this will generate only insert deltas and no delete deltas: delta_1_1
    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) VALUES(1, 'foo')", driver);

    // Insert some data -> this will again generate only insert deltas and no delete deltas: delta_2_2
    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) VALUES(2, 'bar')", driver);

    // Now, compact
    // One important thing to note in this test is that minor compaction always produces
    // delta_x_y and a counterpart delete_delta_x_y, even when there are no delete_delta events.
    // Such a choice has been made to simplify processing of AcidUtils.getAcidState().

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    txnHandler.compact(new CompactionRequest(dbName, tblName, CompactionType.MINOR));
    runWorker(conf);

    // Find the location of the table
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tblName);
    FileSystem fs = FileSystem.get(conf);

    // Verify that we have got correct set of deltas.
    FileStatus[] stat =
      fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.deltaFileFilter);
    String[] deltas = new String[stat.length];
    Path minorCompactedDelta = null;
    for (int i = 0; i < deltas.length; i++) {
      deltas[i] = stat[i].getPath().getName();
      if (deltas[i].equals("delta_0000001_0000002_v0000005")) {
        minorCompactedDelta = stat[i].getPath();
      }
    }
    Arrays.sort(deltas);
    String[] expectedDeltas = new String[]{"delta_0000001_0000001_0000", "delta_0000001_0000002_v0000005",
      "delta_0000002_0000002_0000"};
    if (!Arrays.deepEquals(expectedDeltas, deltas)) {
      Assert.fail("Expected: " + Arrays.toString(expectedDeltas) + ", found: " + Arrays.toString(deltas));
    }
    CompactorTestUtil
        .checkExpectedTxnsPresent(null, new Path[] {minorCompactedDelta}, columnNamesProperty, columnTypesProperty, 0,
            1L, 2L, null, 1);

    //Assert that we have no delete deltas if there are no input delete events.
    FileStatus[] deleteDeltaStat =
      fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.deleteEventDeltaDirFilter);
    assertEquals(0, deleteDeltaStat.length);
  }

  @Test
  public void testCompactionForFileInSratchDir() throws Exception {
    String dbName = "default";
    String tblName = "cfs";
    String columnNamesProperty = "a,b";
    String columnTypesProperty = "int:string";
    String createQuery = "CREATE TABLE " + tblName + "(a INT, b STRING) " + "STORED AS ORC  TBLPROPERTIES ('transactional'='true',"
            + "'transactional_properties'='default')";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver(createQuery, driver);



    // Insert some data -> this will generate only insert deltas
    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) VALUES(1, 'foo')", driver);

    // Insert some data -> this will again generate only insert deltas
    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) VALUES(2, 'bar')", driver);

    // Find the location of the table
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tblName);
    FileSystem fs = FileSystem.get(conf);

    Map<String, String> tblProperties = new HashMap<>();
    tblProperties.put("compactor.hive.compactor.input.tmp.dir",table.getSd().getLocation() + "/" + "_tmp");

    //Create empty file in ScratchDir under table location
    String scratchDirPath = table.getSd().getLocation() + "/" + "_tmp";
    Path dir = new Path(scratchDirPath + "/base_0000002_v0000005");
    fs.mkdirs(dir);
    Path emptyFile = AcidUtils.createBucketFile(dir, 0);
    fs.create(emptyFile);

    //Run MajorCompaction
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setConf(conf);
    t.init(new AtomicBoolean(true));
    CompactionRequest Cr = new CompactionRequest(dbName, tblName, CompactionType.MAJOR);
    Cr.setProperties(tblProperties);
    txnHandler.compact(Cr);
    t.run();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompacts().size());
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, rsp.getCompacts().get(0).getState());

  }

  @Test
  public void minorCompactWhileStreamingWithSplitUpdate() throws Exception {
    String dbName = "default";
    String tblName = "cws";
    String columnNamesProperty = "a,b";
    String columnTypesProperty = "int:string";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ('transactional'='true',"
      + "'transactional_properties'='default')", driver);

    // Write a couple of batches
    for (int i = 0; i < 2; i++) {
      CompactorTestUtil.writeBatch(conf, dbName, tblName, false, false);
    }

    // Start a third batch, but don't close it.
    StreamingConnection connection1 = CompactorTestUtil.writeBatch(conf, dbName, tblName, false, true);
    // Now, compact
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    txnHandler.compact(new CompactionRequest(dbName, tblName, CompactionType.MINOR));
    runWorker(conf);

    // Find the location of the table
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tblName);
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat =
      fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.deltaFileFilter);
    String[] names = new String[stat.length];
    Path resultFile = null;
    for (int i = 0; i < names.length; i++) {
      names[i] = stat[i].getPath().getName();
      if (names[i].equals("delta_0000001_0000004_v0000009")) {
        resultFile = stat[i].getPath();
      }
    }
    Arrays.sort(names);
    String[] expected = new String[]{"delta_0000001_0000002",
      "delta_0000001_0000004_v0000009", "delta_0000003_0000004", "delta_0000005_0000006"};
    if (!Arrays.deepEquals(expected, names)) {
      Assert.fail("Expected: " + Arrays.toString(expected) + ", found: " + Arrays.toString(names));
    }
    CompactorTestUtil
        .checkExpectedTxnsPresent(null, new Path[] {resultFile}, columnNamesProperty, columnTypesProperty, 0, 1L, 4L,
            null, 1);

    //Assert that we have no delete deltas if there are no input delete events.
    FileStatus[] deleteDeltaStat =
      fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.deleteEventDeltaDirFilter);
    assertEquals(0, deleteDeltaStat.length);

    if (connection1 != null) {
      connection1.close();
    }
  }

  @Test
  public void testCompactorGatherStats() throws Exception {
    String dbName = "default";
    String tableName = "stats_comp_test";
    List<String> colNames = Arrays.asList("a");
    executeStatementOnDriver("drop table if exists " + dbName + "." + tableName, driver);
    executeStatementOnDriver("create table " + dbName + "." + tableName +
        " (a INT) STORED AS ORC TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("insert into " + dbName + "." + tableName + " values(1)", driver);
    executeStatementOnDriver("insert into " + dbName + "." + tableName + " values(1)", driver);

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    txnHandler.compact(new CompactionRequest(dbName, tableName, CompactionType.MAJOR));
    runWorker(conf);

    // Make sure we do not have statistics for this table yet
    // Compaction generates stats only if there is any
    List<ColumnStatisticsObj> colStats = msClient.getTableColumnStatistics(dbName,
        tableName, colNames, Constants.HIVE_ENGINE);
    assertEquals("No stats should be there for the table", 0, colStats.size());

    executeStatementOnDriver("analyze table " + dbName + "." + tableName + " compute statistics for columns", driver);
    executeStatementOnDriver("insert into " + dbName + "." + tableName + " values(2)", driver);

    // Make sure we have old statistics for the table
    colStats = msClient.getTableColumnStatistics(dbName, tableName, colNames, Constants.HIVE_ENGINE);
    assertEquals("Stats should be there", 1, colStats.size());
    assertEquals("Value should contain old data", 1, colStats.get(0).getStatsData().getLongStats().getHighValue());
    assertEquals("Value should contain old data", 1, colStats.get(0).getStatsData().getLongStats().getLowValue());

    txnHandler.compact(new CompactionRequest(dbName, tableName, CompactionType.MAJOR));
    runWorker(conf);
    // Make sure the statistics is updated for the table
    colStats = msClient.getTableColumnStatistics(dbName, tableName, colNames, Constants.HIVE_ENGINE);
    assertEquals("Stats should be there", 1, colStats.size());
    assertEquals("Value should contain new data", 2, colStats.get(0).getStatsData().getLongStats().getHighValue());
    assertEquals("Value should contain new data", 1, colStats.get(0).getStatsData().getLongStats().getLowValue());

    executeStatementOnDriver("insert into " + dbName + "." + tableName + " values(3)", driver);
    HiveConf workerConf = new HiveConf(conf);
    workerConf.setBoolVar(ConfVars.HIVE_MR_COMPACTOR_GATHER_STATS, false);

    txnHandler.compact(new CompactionRequest(dbName, tableName, CompactionType.MAJOR));
    runWorker(workerConf);
    // Make sure the statistics is NOT updated for the table
    colStats = msClient.getTableColumnStatistics(dbName, tableName, colNames, Constants.HIVE_ENGINE);
    assertEquals("Stats should be there", 1, colStats.size());
    assertEquals("Value should contain new data", 2, colStats.get(0).getStatsData().getLongStats().getHighValue());
    assertEquals("Value should contain new data", 1, colStats.get(0).getStatsData().getLongStats().getLowValue());
  }

  /**
   * Users have the choice of specifying compaction related tblproperties either in CREATE TABLE
   * statement or in ALTER TABLE .. COMPACT statement. This tests both cases.
   */
  @Test
  public void testTableProperties() throws Exception {
    conf.setVar(HiveConf.ConfVars.COMPACTOR_JOB_QUEUE, "root.user1");
    String tblName1 = "ttp1"; // plain acid table
    String tblName2 = "ttp2"; // acid table with customized tblproperties
    executeStatementOnDriver("drop table if exists " + tblName1, driver);
    executeStatementOnDriver("drop table if exists " + tblName2, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName1 + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 2 BUCKETS STORED AS ORC" +
      " TBLPROPERTIES ('transactional'='true', 'orc.compress.size'='2700')", driver);
    executeStatementOnDriver("CREATE TABLE " + tblName2 + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES (" +
      "'transactional'='true'," +
      "'compactor.mapreduce.map.memory.mb'='2048'," + // 2048 MB memory for compaction map job
      "'compactorthreshold.hive.compactor.delta.num.threshold'='4'," +  // minor compaction if more than 4 delta dirs
      "'compactorthreshold.hive.compactor.delta.pct.threshold'='0.47'," + // major compaction if more than 47%
      "'compactor.hive.compactor.job.queue'='root.user2'" + // Override the system wide compactor queue for this table
      ")", driver);

    // Insert 5 rows to both tables
    executeStatementOnDriver("insert into " + tblName1 + " values (1, 'a')", driver);
    executeStatementOnDriver("insert into " + tblName1 + " values (2, 'b')", driver);
    executeStatementOnDriver("insert into " + tblName1 + " values (3, 'c')", driver);
    executeStatementOnDriver("insert into " + tblName1 + " values (4, 'd')", driver);
    executeStatementOnDriver("insert into " + tblName1 + " values (5, 'e')", driver);

    executeStatementOnDriver("insert into " + tblName2 + " values (1, 'a')", driver);
    executeStatementOnDriver("insert into " + tblName2 + " values (2, 'b')", driver);
    executeStatementOnDriver("insert into " + tblName2 + " values (3, 'c')", driver);
    executeStatementOnDriver("insert into " + tblName2 + " values (4, 'd')", driver);
    executeStatementOnDriver("insert into " + tblName2 + " values (5, 'e')", driver);

    runInitiator(conf);

    // Compactor should only schedule compaction for ttp2 (delta.num.threshold=4), not ttp1
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompacts().size());
    Assert.assertEquals(TxnStore.INITIATED_RESPONSE, rsp.getCompacts().get(0).getState());
    Assert.assertEquals("ttp2", rsp.getCompacts().get(0).getTablename());
    Assert.assertEquals(CompactionType.MAJOR,
      rsp.getCompacts().get(0).getType()); // type is MAJOR since there's no base yet

    // Finish the scheduled compaction for ttp2, and manually compact ttp1, to make them comparable again
    executeStatementOnDriver("alter table " + tblName1 + " compact 'major'", driver);
    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(2, rsp.getCompacts().size());
    Assert.assertEquals("ttp2", rsp.getCompacts().get(0).getTablename());
    Assert.assertEquals(TxnStore.INITIATED_RESPONSE, rsp.getCompacts().get(0).getState());
    Assert.assertEquals("ttp1", rsp.getCompacts().get(1).getTablename());
    Assert.assertEquals(TxnStore.INITIATED_RESPONSE, rsp.getCompacts().get(1).getState());
    // compact ttp2, by running the Worker explicitly, in order to get the reference to the compactor MR job
    runWorker(conf);
    // Compact ttp1
    runWorker(conf);
    // Clean up
    runCleaner(conf);
    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(2, rsp.getCompacts().size());
    Assert.assertEquals("ttp2", rsp.getCompacts().get(0).getTablename());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());
    Assert.assertEquals("ttp1", rsp.getCompacts().get(1).getTablename());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(1).getState());

    /**
     * we just did a major compaction on ttp1.  Open any file produced by it and check buffer size.
     * It should be the default.
     */
    List<String> rs = execSelectAndDumpData("select distinct INPUT__FILE__NAME from "
      + tblName1, driver, "Find Orc File bufer default");
    Assert.assertTrue("empty rs?", rs != null && rs.size() > 0);
    Path p = new Path(rs.get(0));
    Reader orcReader = OrcFile.createReader(p.getFileSystem(conf), p);
    Assert.assertEquals("Expected default compression size",
      2700, orcReader.getCompressionSize());
    //make sure 2700 is not the default so that we are testing if tblproperties indeed propagate
    Assert.assertNotEquals("Unexpected default compression size", 2700,
      OrcConf.BUFFER_SIZE.getDefaultValue());

    // Insert one more row - this should trigger hive.compactor.delta.pct.threshold to be reached for ttp2
    executeStatementOnDriver("insert into " + tblName1 + " values (6, 'f')", driver);
    executeStatementOnDriver("insert into " + tblName2 + " values (6, 'f')", driver);

    // Intentionally set this high so that it will not trigger major compaction for ttp1.
    // Only trigger major compaction for ttp2 (delta.pct.threshold=0.5) because of the newly inserted row (actual pct: 0.66)
    conf.setFloatVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_PCT_THRESHOLD, 0.8f);
    runInitiator(conf);
    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(3, rsp.getCompacts().size());
    Assert.assertEquals("ttp2", rsp.getCompacts().get(0).getTablename());
    Assert.assertEquals(TxnStore.INITIATED_RESPONSE, rsp.getCompacts().get(0).getState());

    // Finish the scheduled compaction for ttp2
    runWorker(conf);
    runCleaner(conf);
    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(3, rsp.getCompacts().size());
    Assert.assertEquals("ttp2", rsp.getCompacts().get(0).getTablename());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());

    // Now test tblproperties specified on ALTER TABLE .. COMPACT .. statement
    executeStatementOnDriver("insert into " + tblName2 + " values (7, 'g')", driver);
    executeStatementOnDriver("alter table " + tblName2 + " compact 'major'" +
      " with overwrite tblproperties (" +
      "'compactor.mapreduce.map.memory.mb'='3072'," +
      "'tblprops.orc.compress.size'='3141'," +
      "'compactor.hive.compactor.job.queue'='root.user2')", driver);

    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(4, rsp.getCompacts().size());
    Assert.assertEquals("ttp2", rsp.getCompacts().get(0).getTablename());
    Assert.assertEquals(TxnStore.INITIATED_RESPONSE, rsp.getCompacts().get(0).getState());
    //make sure we are checking the right (latest) compaction entry
    Assert.assertEquals(4, rsp.getCompacts().get(0).getId());

    // Run the Worker explicitly, in order to get the reference to the compactor MR job
    runWorker(conf);
    /*createReader(FileSystem fs, Path path) throws IOException {
     */
    //we just ran Major compaction so we should have a base_x in tblName2 that has the new files
    // Get the name of a file and look at its properties to see if orc.compress.size was respected.
    rs = execSelectAndDumpData("select distinct INPUT__FILE__NAME from " + tblName2,
      driver, "Find Compacted Orc File");
    Assert.assertTrue("empty rs?", rs != null && rs.size() > 0);
    p = new Path(rs.get(0));
    orcReader = OrcFile.createReader(p.getFileSystem(conf), p);
    Assert.assertEquals("File written with wrong buffer size",
      3141, orcReader.getCompressionSize());
  }

  @Test
  public void testCompactionInfoEquals() {
    CompactionInfo compactionInfo = new CompactionInfo("dbName", "tableName", "partName", CompactionType.MINOR);
    CompactionInfo compactionInfo1 = new CompactionInfo("dbName", "tableName", "partName", CompactionType.MINOR);
    Assert.assertTrue("The object must be equal", compactionInfo.equals(compactionInfo));

    Assert.assertFalse("The object must be not equal", compactionInfo.equals(new Object()));
    Assert.assertTrue("The object must be equal", compactionInfo.equals(compactionInfo1));
  }

  @Test
  public void testCompactionInfoHashCode() {
    CompactionInfo compactionInfo = new CompactionInfo("dbName", "tableName", "partName", CompactionType.MINOR);
    CompactionInfo compactionInfo1 = new CompactionInfo("dbName", "tableName", "partName", CompactionType.MINOR);

    Assert.assertEquals("The hash codes must be equal", compactionInfo.hashCode(), compactionInfo1.hashCode());
  }

  @Test
  public void testDisableCompactionDuringReplLoad() throws Exception {
    String tblName = "discomp";
    String database = "discomp_db";
    executeStatementOnDriver("drop database if exists " + database + " cascade", driver);
    executeStatementOnDriver("create database " + database, driver);
    executeStatementOnDriver("CREATE TABLE " + database + "." + tblName + "(a INT, b STRING) " +
            " PARTITIONED BY(ds string)" +
            " CLUSTERED BY(a) INTO 2 BUCKETS" + //currently ACID requires table to be bucketed
            " STORED AS ORC TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("insert into " + database + "." + tblName + " partition (ds) values (1, 'fred', " +
            "'today'), (2, 'wilma', 'yesterday')", driver);

    executeStatementOnDriver("ALTER TABLE " + database + "." + tblName +
            " SET TBLPROPERTIES ( 'hive.repl.first.inc.pending' = 'true')", driver);
    List<ShowCompactResponseElement> compacts = getCompactionList();
    Assert.assertEquals(0, compacts.size());

    executeStatementOnDriver("alter database " + database +
            " set dbproperties ('hive.repl.first.inc.pending' = 'true')", driver);
    executeStatementOnDriver("ALTER TABLE " + database + "." + tblName +
            " SET TBLPROPERTIES ( 'hive.repl.first.inc.pending' = 'false')", driver);
    compacts = getCompactionList();
    Assert.assertEquals(0, compacts.size());

    executeStatementOnDriver("alter database " + database +
            " set dbproperties ('hive.repl.first.inc.pending' = 'false')", driver);
    executeStatementOnDriver("ALTER TABLE " + database + "." + tblName +
            " SET TBLPROPERTIES ( 'hive.repl.first.inc.pending' = 'false')", driver);
    compacts = getCompactionList();
    Assert.assertEquals(2, compacts.size());
    List<String> partNames = new ArrayList<String>();
    for (int i = 0; i < compacts.size(); i++) {
      Assert.assertEquals(database, compacts.get(i).getDbname());
      Assert.assertEquals(tblName, compacts.get(i).getTablename());
      Assert.assertEquals("initiated", compacts.get(i).getState());
      partNames.add(compacts.get(i).getPartitionname());
    }
    Collections.sort(partNames);
    Assert.assertEquals("ds=today", partNames.get(0));
    Assert.assertEquals("ds=yesterday", partNames.get(1));
    executeStatementOnDriver("drop database if exists " + database + " cascade", driver);

    // Finish the scheduled compaction for ttp2
    runWorker(conf);
    runCleaner(conf);
  }

  /**
   * Tests compaction of tables that were populated by LOAD DATA INPATH statements.
   *
   * In this scenario original ORC files are a structured in the following way:
   * comp3
   * |--delta_0000001_0000001_0000
   *    |--000000_0
   * |--delta_0000002_0000002_0000
   *    |--000000_0
   *    |--000001_0
   *
   * ..where comp3 table is not bucketed.
   *
   * @throws Exception
   */
  @Test
  public void testCompactionOnDataLoadedInPath() throws Exception {
    // Setup of LOAD INPATH scenario.
    executeStatementOnDriver("drop table if exists comp0", driver);
    executeStatementOnDriver("drop table if exists comp1", driver);
    executeStatementOnDriver("drop table if exists comp3", driver);

    executeStatementOnDriver("create external table comp0 (a string)", driver);
    executeStatementOnDriver("insert into comp0 values ('1111111111111')", driver);
    executeStatementOnDriver("insert into comp0 values ('2222222222222')", driver);
    executeStatementOnDriver("insert into comp0 values ('3333333333333')", driver);
    executeStatementOnDriver("create external table comp1 stored as orc as select * from comp0", driver);

    executeStatementOnDriver("create table comp3 (a string) stored as orc " +
        "TBLPROPERTIES ('transactional'='true')", driver);

    IMetaStoreClient hmsClient = new HiveMetaStoreClient(conf);
    Table table = hmsClient.getTable("default", "comp1");
    FileSystem fs = FileSystem.get(conf);
    Path path000 = fs.listStatus(new Path(table.getSd().getLocation()))[0].getPath();
    Path path001 = new Path(path000.toString().replace("000000", "000001"));
    Path path002 = new Path(path000.toString().replace("000000", "000002"));
    fs.copyFromLocalFile(path000, path001);
    fs.copyFromLocalFile(path000, path002);

    executeStatementOnDriver("load data inpath '" + path002.toString() + "' into table comp3", driver);
    executeStatementOnDriver("load data inpath '" + path002.getParent().toString() + "' into table comp3", driver);

    // Run compaction.
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    CompactionRequest rqst = new CompactionRequest("default", "comp3", CompactionType.MAJOR);
    txnHandler.compact(rqst);
    runWorker(conf);
    ShowCompactRequest scRqst = new ShowCompactRequest();
    List<ShowCompactResponseElement> compacts = txnHandler.showCompact(scRqst).getCompacts();
    assertEquals(1, compacts.size());
    assertEquals(TxnStore.CLEANING_RESPONSE, compacts.get(0).getState());

    runCleaner(conf);
    compacts = txnHandler.showCompact(scRqst).getCompacts();
    assertEquals(1, compacts.size());
    assertEquals(TxnStore.SUCCEEDED_RESPONSE, compacts.get(0).getState());

    // Check compacted content and file structure.
    table = hmsClient.getTable("default", "comp3");
    List<String> rs = execSelectAndDumpData("select * from comp3", driver, "select");
    assertEquals(9, rs.size());
    assertEquals(3, rs.stream().filter(p -> "1111111111111".equals(p)).count());
    assertEquals(3, rs.stream().filter(p -> "2222222222222".equals(p)).count());
    assertEquals(3, rs.stream().filter(p -> "3333333333333".equals(p)).count());

    FileStatus[] files = fs.listStatus(new Path(table.getSd().getLocation()));
    // base dir
    assertEquals(1, files.length);
    assertEquals("base_0000002_v0000012", files[0].getPath().getName());
    files = fs.listStatus(files[0].getPath(), AcidUtils.bucketFileFilter);
    // files
    assertEquals(2, files.length);
    Arrays.stream(files).filter(p->"bucket_00000".equals(p.getPath().getName())).count();
    Arrays.stream(files).filter(p->"bucket_00001".equals(p.getPath().getName())).count();

    // Another insert into the newly compacted table.
    executeStatementOnDriver("insert into comp3 values ('4444444444444')", driver);

    // Compact with extra row too.
    txnHandler.compact(rqst);
    runWorker(conf);
    compacts = txnHandler.showCompact(scRqst).getCompacts();
    assertEquals(2, compacts.size());
    assertEquals(TxnStore.CLEANING_RESPONSE, compacts.get(0).getState());

    runCleaner(conf);
    compacts = txnHandler.showCompact(scRqst).getCompacts();
    assertEquals(2, compacts.size());
    assertEquals(TxnStore.SUCCEEDED_RESPONSE, compacts.get(0).getState());

    // Check compacted content and file structure.
    rs = execSelectAndDumpData("select * from comp3", driver, "select");
    assertEquals(10, rs.size());
    assertEquals(3, rs.stream().filter(p -> "1111111111111".equals(p)).count());
    assertEquals(3, rs.stream().filter(p -> "2222222222222".equals(p)).count());
    assertEquals(3, rs.stream().filter(p -> "3333333333333".equals(p)).count());
    assertEquals(1, rs.stream().filter(p -> "4444444444444".equals(p)).count());

    files = fs.listStatus(new Path(table.getSd().getLocation()));
    // base dir
    assertEquals(1, files.length);
    assertEquals("base_0000003_v0000015", files[0].getPath().getName());
    files = fs.listStatus(files[0].getPath(), AcidUtils.bucketFileFilter);
    // files
    assertEquals(2, files.length);
    Arrays.stream(files).filter(p->"bucket_00000".equals(p.getPath().getName())).count();
    Arrays.stream(files).filter(p->"bucket_00001".equals(p.getPath().getName())).count();

  }

  @Test
  public void testCompactionDataLoadedWithInsertOverwrite() throws Exception {
    String externalTableName = "test_comp_txt";
    String tableName = "test_comp";
    executeStatementOnDriver("DROP TABLE IF EXISTS " + externalTableName, driver);
    executeStatementOnDriver("DROP TABLE IF EXISTS " + tableName, driver);
    executeStatementOnDriver("CREATE EXTERNAL TABLE " + externalTableName + "(a int, b int, c int) STORED AS TEXTFILE",
        driver);
    executeStatementOnDriver(
        "CREATE TABLE " + tableName + "(a int, b int, c int) STORED AS ORC TBLPROPERTIES('transactional'='true')",
        driver);

    executeStatementOnDriver("INSERT INTO " + externalTableName + " values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4)",
        driver);
    executeStatementOnDriver("INSERT OVERWRITE TABLE " + tableName + " SELECT * FROM test_comp_txt", driver);

    executeStatementOnDriver("UPDATE " + tableName + " SET b=55, c=66 WHERE a=2", driver);
    executeStatementOnDriver("DELETE FROM " + tableName + " WHERE a=4", driver);
    executeStatementOnDriver("UPDATE " + tableName + " SET b=77 WHERE a=1", driver);

    executeStatementOnDriver("SELECT * FROM " + tableName + " ORDER BY a", driver);
    ArrayList<String> valuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(valuesReadFromHiveDriver);
    Assert.assertEquals(3, valuesReadFromHiveDriver.size());
    Assert.assertEquals("1\t77\t1", valuesReadFromHiveDriver.get(0));
    Assert.assertEquals("2\t55\t66", valuesReadFromHiveDriver.get(1));
    Assert.assertEquals("3\t3\t3", valuesReadFromHiveDriver.get(2));

    runMajorCompaction("default", tableName);

    // Validate after compaction.
    executeStatementOnDriver("SELECT * FROM " + tableName + " ORDER BY a", driver);
    valuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(valuesReadFromHiveDriver);
    Assert.assertEquals(3, valuesReadFromHiveDriver.size());
    Assert.assertEquals("1\t77\t1", valuesReadFromHiveDriver.get(0));
    Assert.assertEquals("2\t55\t66", valuesReadFromHiveDriver.get(1));
  }

  private List<ShowCompactResponseElement> getCompactionList() throws Exception {
    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 0);
    runInitiator(conf);

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    return rsp.getCompacts();
  }

  /**
   * convenience method to execute a select stmt and dump results to log file
   */
  private static List<String> execSelectAndDumpData(String selectStmt, IDriver driver, String msg)
    throws Exception {
    executeStatementOnDriver(selectStmt, driver);
    ArrayList<String> valuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(valuesReadFromHiveDriver);
    int rowIdx = 0;
    LOG.debug(msg);
    for (String row : valuesReadFromHiveDriver) {
      LOG.debug(" rowIdx=" + rowIdx++ + ":" + row);
    }
    return valuesReadFromHiveDriver;
  }

  /**
   * Execute Hive CLI statement
   *
   * @param cmd arbitrary statement to execute
   */
  static void executeStatementOnDriver(String cmd, IDriver driver) throws Exception {
    LOG.debug("Executing: " + cmd);
    driver.run(cmd);
  }

  static void createTestDataFile(String filename, String[] lines) throws IOException {
    FileWriter writer = null;
    try {
      File file = new File(filename);
      file.deleteOnExit();
      writer = new FileWriter(file);
      for (String line : lines) {
        writer.write(line + "\n");
      }
    } finally {
      if (writer != null) {
        writer.close();
      }
    }

  }

  private void verifyCompactions(List<ShowCompactResponseElement> compacts, SortedSet<String> partNames, String tblName) {
    for (ShowCompactResponseElement compact : compacts) {
      Assert.assertEquals("default", compact.getDbname());
      Assert.assertEquals(tblName, compact.getTablename());
      Assert.assertEquals("initiated", compact.getState());
      partNames.add(compact.getPartitionname());
    }
  }

  private void processStreamingAPI(String dbName, String tblName)
      throws StreamingException, ClassNotFoundException,
      InterruptedException {
      List<CompactorTestUtil.StreamingConnectionOption> options = Lists
          .newArrayList(new CompactorTestUtil.StreamingConnectionOption(false, false),
              new CompactorTestUtil.StreamingConnectionOption(false, false),
              new CompactorTestUtil.StreamingConnectionOption(true, false));
      CompactorTestUtil.runStreamingAPI(conf, dbName, tblName, options);
  }
}
