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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.common.util.Retry;
import org.apache.hive.common.util.RetryTestRunner;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.streaming.DelimitedInputWriter;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.TransactionBatch;
import org.apache.hive.streaming.HiveStreamingConnection;
import org.apache.hive.streaming.StreamingConnection;
import org.apache.hive.streaming.StreamingException;
import org.apache.hive.streaming.StrictDelimitedInputWriter;
import org.apache.orc.OrcConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class TestCompactor {
  private static final AtomicInteger salt = new AtomicInteger(new Random().nextInt());
  private static final Logger LOG = LoggerFactory.getLogger(TestCompactor.class);
  private final String TEST_DATA_DIR = HCatUtil.makePathASafeFileName(System.getProperty("java.io.tmpdir") +
    File.separator + TestCompactor.class.getCanonicalName() + "-" + System.currentTimeMillis() + "_" +
    salt.getAndIncrement());
  private final String BASIC_FILE_NAME = TEST_DATA_DIR + "/basic.input.data";
  private final String TEST_WAREHOUSE_DIR = TEST_DATA_DIR + "/warehouse";

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{{true}, {false}});
  }

  private boolean newStreamingAPI;

  public TestCompactor(boolean newStreamingAPI) {
    this.newStreamingAPI = newStreamingAPI;
  }

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
    hiveConf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");

    TxnDbUtil.setConfValues(hiveConf);
    TxnDbUtil.cleanDb(hiveConf);
    TxnDbUtil.prepDb(hiveConf);

    conf = hiveConf;
    HiveConf.setBoolVar(conf, ConfVars.HIVE_MM_ALLOW_ORIGINALS, true);
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

    Initiator initiator = new Initiator();
    initiator.setThreadId((int) initiator.getId());
    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 0);
    initiator.setConf(conf);
    AtomicBoolean stop = new AtomicBoolean();
    stop.set(true);
    initiator.init(stop, new AtomicBoolean());
    initiator.run();

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(4, compacts.size());
    SortedSet<String> partNames = new TreeSet<String>();
    for (int i = 0; i < compacts.size(); i++) {
      Assert.assertEquals("default", compacts.get(i).getDbname());
      Assert.assertEquals(tblName, compacts.get(i).getTablename());
      Assert.assertEquals("initiated", compacts.get(i).getState());
      partNames.add(compacts.get(i).getPartitionname());
    }
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

    Initiator initiator = new Initiator();
    initiator.setThreadId((int) initiator.getId());
    // Set to 1 so insert doesn't set it off but update does
    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 1);
    initiator.setConf(conf);
    AtomicBoolean stop = new AtomicBoolean();
    stop.set(true);
    initiator.init(stop, new AtomicBoolean());
    initiator.run();

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(4, compacts.size());
    SortedSet<String> partNames = new TreeSet<String>();
    for (int i = 0; i < compacts.size(); i++) {
      Assert.assertEquals("default", compacts.get(i).getDbname());
      Assert.assertEquals(tblName, compacts.get(i).getTablename());
      Assert.assertEquals("initiated", compacts.get(i).getState());
      partNames.add(compacts.get(i).getPartitionname());
    }
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
      ci.tableName, Arrays.asList(ci.partName), colNames);
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
      ciPart2.tableName, Arrays.asList(ciPart2.partName), colNames);
    colStats = stats.get(ciPart2.partName);
    LongColumnStatsData colAStatsPart2 = colStats.get(0).getStatsData().getLongStats();
    StringColumnStatsData colBStatsPart2 = colStats.get(1).getStatsData().getStringStats();

    if (newStreamingAPI) {
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
    } else {
      HiveEndPoint endPt = new HiveEndPoint(null, ci.dbname, ci.tableName, Arrays.asList("0"));
      DelimitedInputWriter writer = new DelimitedInputWriter(new String[]{"a", "b"}, ",", endPt);
    /*next call will eventually end up in HiveEndPoint.createPartitionIfNotExists() which
    makes an operation on Driver
    * and starts it's own CliSessionState and then closes it, which removes it from ThreadLoacal;
    * thus the session
    * created in this class is gone after this; I fixed it in HiveEndPoint*/
      org.apache.hive.hcatalog.streaming.StreamingConnection connection = endPt
        .newConnection(true, "UT_" + Thread.currentThread().getName());

      TransactionBatch txnBatch = connection.fetchTransactionBatch(2, writer);
      txnBatch.beginNextTransaction();
      Assert.assertEquals(TransactionBatch.TxnState.OPEN, txnBatch.getCurrentTransactionState());
      txnBatch.write("50,Kiev".getBytes());
      txnBatch.write("51,St. Petersburg".getBytes());
      txnBatch.write("44,Boston".getBytes());
      txnBatch.commit();

      txnBatch.beginNextTransaction();
      txnBatch.write("52,Tel Aviv".getBytes());
      txnBatch.write("53,Atlantis".getBytes());
      txnBatch.write("53,Boston".getBytes());
      txnBatch.commit();

      txnBatch.close();
      connection.close();
    }
    execSelectAndDumpData("select * from " + ci.getFullTableName(), driver, ci.getFullTableName());

    //so now we have written some new data to bkt=0 and it shows up
    CompactionRequest rqst = new CompactionRequest(ci.dbname, ci.tableName, CompactionType.MAJOR);
    rqst.setPartitionname(ci.partName);
    txnHandler.compact(rqst);
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setConf(conf);
    AtomicBoolean stop = new AtomicBoolean();
    AtomicBoolean looped = new AtomicBoolean();
    stop.set(true);
    t.init(stop, looped);
    t.run();
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    if (1 != compacts.size()) {
      Assert.fail("Expecting 1 file and found " + compacts.size() + " files " + compacts.toString());
    }
    Assert.assertEquals("ready for cleaning", compacts.get(0).getState());

    stats = msClient.getPartitionColumnStatistics(ci.dbname, ci.tableName,
      Arrays.asList(ci.partName), colNames);
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
      Arrays.asList(ciPart2.partName), colNames);
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

    Initiator initiator = new Initiator();
    initiator.setThreadId((int) initiator.getId());
    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 0);
    initiator.setConf(conf);
    AtomicBoolean stop = new AtomicBoolean();
    stop.set(true);
    initiator.init(stop, new AtomicBoolean());
    initiator.run();

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(2, compacts.size());
    SortedSet<String> partNames = new TreeSet<String>();
    for (int i = 0; i < compacts.size(); i++) {
      Assert.assertEquals("default", compacts.get(i).getDbname());
      Assert.assertEquals(tblName, compacts.get(i).getTablename());
      Assert.assertEquals("initiated", compacts.get(i).getState());
      partNames.add(compacts.get(i).getPartitionname());
    }
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

    Initiator initiator = new Initiator();
    initiator.setThreadId((int) initiator.getId());
    // Set to 1 so insert doesn't set it off but update does
    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 1);
    initiator.setConf(conf);
    AtomicBoolean stop = new AtomicBoolean();
    stop.set(true);
    initiator.init(stop, new AtomicBoolean());
    initiator.run();

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(2, compacts.size());
    SortedSet<String> partNames = new TreeSet<String>();
    for (int i = 0; i < compacts.size(); i++) {
      Assert.assertEquals("default", compacts.get(i).getDbname());
      Assert.assertEquals(tblName, compacts.get(i).getTablename());
      Assert.assertEquals("initiated", compacts.get(i).getState());
      partNames.add(compacts.get(i).getPartitionname());
    }
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

    Initiator initiator = new Initiator();
    initiator.setThreadId((int) initiator.getId());
    // Set to 2 so insert and update don't set it off but delete does
    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 2);
    initiator.setConf(conf);
    AtomicBoolean stop = new AtomicBoolean();
    stop.set(true);
    initiator.init(stop, new AtomicBoolean());
    initiator.run();

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    SortedSet<String> partNames = new TreeSet<String>();
    for (int i = 0; i < compacts.size(); i++) {
      Assert.assertEquals("default", compacts.get(i).getDbname());
      Assert.assertEquals(tblName, compacts.get(i).getTablename());
      Assert.assertEquals("initiated", compacts.get(i).getState());
      partNames.add(compacts.get(i).getPartitionname());
    }
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
        writeBatch(dbName, tblName, false);
      }

      // Start a third batch, but don't close it.
      connection = writeBatch(dbName, tblName, true);

      // Now, compact
      TxnStore txnHandler = TxnUtils.getTxnStore(conf);
      txnHandler.compact(new CompactionRequest(dbName, tblName, CompactionType.MINOR));
      Worker t = new Worker();
      t.setThreadId((int) t.getId());
      t.setConf(conf);
      AtomicBoolean stop = new AtomicBoolean(true);
      AtomicBoolean looped = new AtomicBoolean();
      t.init(stop, looped);
      t.run();

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
        if (names[i].equals("delta_0000001_0000004")) {
          resultFile = stat[i].getPath();
        }
      }
      Arrays.sort(names);
      String[] expected = new String[]{"delta_0000001_0000002",
        "delta_0000001_0000004", "delta_0000003_0000004", "delta_0000005_0000006"};
      if (!Arrays.deepEquals(expected, names)) {
        Assert.fail("Expected: " + Arrays.toString(expected) + ", found: " + Arrays.toString(names));
      }
      checkExpectedTxnsPresent(null, new Path[]{resultFile}, columnNamesProperty, columnTypesProperty,
        0, 1L, 4L, 1);

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
        writeBatch(dbName, tblName, false);
      }

      // Start a third batch, but don't close it.  this delta will be ignored by compaction since
      // it has an open txn in it
      connection = writeBatch(dbName, tblName, true);

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
      Assert.assertEquals(name, "base_0000004");
      checkExpectedTxnsPresent(stat[0].getPath(), null, columnNamesProperty, columnTypesProperty, 0, 1L, 4L, 1);
    } finally {
      if (connection != null) {
        connection.close();
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

    if (newStreamingAPI) {
      StreamingConnection connection = null;
      try {
        // Write a couple of batches
        for (int i = 0; i < 2; i++) {
          connection = writeBatch(dbName, tblName, false);
          assertNull(connection);
        }

        StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
          .withFieldDelimiter(',')
          .build();
        StreamingConnection connection2 = HiveStreamingConnection.newBuilder()
          .withDatabase(dbName)
          .withTable(tblName)
          .withAgentInfo("UT_" + Thread.currentThread().getName())
          .withHiveConf(conf)
          .withRecordWriter(writer)
          .withTransactionBatchSize(2)
          .connect();
        // Start a third batch, abortTransaction everything, don't properly close it
        connection2.beginTransaction();
        connection2.abortTransaction();
        connection2.close();
      } finally {
        if (connection != null) {
          connection.close();
        }
      }
    } else {
      HiveEndPoint endPt = new HiveEndPoint(null, dbName, tblName, null);
      DelimitedInputWriter writer = new DelimitedInputWriter(new String[]{"a", "b"}, ",", endPt);
      org.apache.hive.hcatalog.streaming.StreamingConnection connection = endPt
        .newConnection(false, "UT_" + Thread.currentThread().getName());
      try {
        // Write a couple of batches
        for (int i = 0; i < 2; i++) {
          writeBatch(connection, writer, false);
        }

        // Start a third batch, abort everything, don't properly close it
        TransactionBatch txnBatch = connection.fetchTransactionBatch(2, writer);
        txnBatch.beginNextTransaction();
        txnBatch.abort();
        txnBatch.beginNextTransaction();
        txnBatch.abort();
      } finally {
        connection.close();
      }
    }
    // Now, compact
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    txnHandler.compact(new CompactionRequest(dbName, tblName, CompactionType.MINOR));
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setConf(conf);
    AtomicBoolean stop = new AtomicBoolean(true);
    AtomicBoolean looped = new AtomicBoolean();
    t.init(stop, looped);
    t.run();

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
      if (names[i].equals("delta_0000001_0000004")) {
        resultDelta = stat[i].getPath();
      }
    }
    Arrays.sort(names);
    String[] expected = new String[]{"delta_0000001_0000002",
      "delta_0000001_0000004", "delta_0000003_0000004"};
    if (!Arrays.deepEquals(expected, names)) {
      Assert.fail("Expected: " + Arrays.toString(expected) + ", found: " + Arrays.toString(names));
    }
    checkExpectedTxnsPresent(null, new Path[]{resultDelta}, columnNamesProperty, columnTypesProperty, 0, 1L, 4L, 1);
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

    if (newStreamingAPI) {
      StreamingConnection connection = null;
      try {
        // Write a couple of batches
        for (int i = 0; i < 2; i++) {
          connection = writeBatch(dbName, tblName, false);
          assertNull(connection);
        }

        StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
          .withFieldDelimiter(',')
          .build();
        StreamingConnection connection2 = HiveStreamingConnection.newBuilder()
          .withDatabase(dbName)
          .withTable(tblName)
          .withAgentInfo("UT_" + Thread.currentThread().getName())
          .withHiveConf(conf)
          .withRecordWriter(writer)
          .withTransactionBatchSize(2)
          .connect();
        // Start a third batch, but don't close it.
        connection2.beginTransaction();
        connection2.abortTransaction();
        connection2.close();
      } finally {
        if (connection != null) {
          connection.close();
        }
      }
    } else {
      HiveEndPoint endPt = new HiveEndPoint(null, dbName, tblName, null);
      DelimitedInputWriter writer = new DelimitedInputWriter(new String[]{"a", "b"}, ",", endPt);
      org.apache.hive.hcatalog.streaming.StreamingConnection connection = endPt
        .newConnection(false, "UT_" + Thread.currentThread().getName());
      try {
        // Write a couple of batches
        for (int i = 0; i < 2; i++) {
          writeBatch(connection, writer, false);
        }

        // Start a third batch, abort everything, don't properly close it
        TransactionBatch txnBatch = connection.fetchTransactionBatch(2, writer);
        txnBatch.beginNextTransaction();
        txnBatch.abort();
        txnBatch.beginNextTransaction();
        txnBatch.abort();
      } finally {
        connection.close();
      }
    }
    runMajorCompaction(dbName, tblName);

    // Find the location of the table
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tblName);
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat =
      fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.baseFileFilter);
    if (1 != stat.length) {
      Assert.fail("majorCompactAfterAbort FileStatus[] stat " + Arrays.toString(stat));
    }
    if (1 != stat.length) {
      Assert.fail("Expecting 1 file \"base_0000004\" and found " + stat.length + " files " + Arrays.toString(stat));
    }
    String name = stat[0].getPath().getName();
    if (!name.equals("base_0000004")) {
      Assert.fail("majorCompactAfterAbort name " + name + " not equals to base_0000004");
    }
    checkExpectedTxnsPresent(stat[0].getPath(), null, columnNamesProperty, columnTypesProperty, 0, 1L, 4L, 1);
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
    verifyHasBase(table.getSd(), fs, "base_0000002");

    // Make sure we don't compact if we don't need to compact.
    runMajorCompaction(dbName, tblName);
    verifyFooBarResult(tblName, 1);
    verifyHasBase(table.getSd(), fs, "base_0000002");
  }

  @Test
  public void mmTableOriginalsOrc() throws Exception {
    mmTableOriginals("ORC");
  }

  @Test
  public void mmTableOriginalsText() throws Exception {
    mmTableOriginals("TEXTFILE");
  }

  private void mmTableOriginals(String format) throws Exception {
    // Originals split won't work due to MAPREDUCE-7086 issue in FileInputFormat.
    boolean isBrokenUntilMapreduce7086 = "TEXTFILE".equals(format);
    String dbName = "default";
    String tblName = "mm_nonpart";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) STORED AS " +
        format + " TBLPROPERTIES ('transactional'='false')", driver);
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tblName);

    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) VALUES(1, 'foo')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName +" (a,b) VALUES(2, 'bar')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) SELECT a,b FROM "
        + tblName + " UNION ALL SELECT a,b FROM " + tblName, driver);

    verifyFooBarResult(tblName, 3);

    FileSystem fs = FileSystem.get(conf);
    executeStatementOnDriver("ALTER TABLE " + tblName + " SET TBLPROPERTIES "
       + "('transactional'='true', 'transactional_properties'='insert_only')", driver);

    verifyFooBarResult(tblName, 3);

    runMajorCompaction(dbName, tblName);
    verifyFooBarResult(tblName, 3);
    verifyHasBase(table.getSd(), fs, "base_0000001");

    // Try with an extra delta.
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) STORED AS " +
        format + " TBLPROPERTIES ('transactional'='false')", driver);
    table = msClient.getTable(dbName, tblName);

    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) VALUES(1, 'foo')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName +" (a,b) VALUES(2, 'bar')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) SELECT a,b FROM "
        + tblName + " UNION ALL SELECT a,b FROM " + tblName, driver);
    verifyFooBarResult(tblName, 3);

    executeStatementOnDriver("ALTER TABLE " + tblName + " SET TBLPROPERTIES "
       + "('transactional'='true', 'transactional_properties'='insert_only')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) SELECT a,b FROM "
        + tblName + " UNION ALL SELECT a,b FROM " + tblName, driver);

    // Neither select nor compaction (which is a select) wil work after this.
    if (isBrokenUntilMapreduce7086) return;

    verifyFooBarResult(tblName, 9);

    runMajorCompaction(dbName, tblName);
    verifyFooBarResult(tblName, 9);
    verifyHasBase(table.getSd(), fs, "base_0000002");

    // Try with an extra base.
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) STORED AS " +
        format + " TBLPROPERTIES ('transactional'='false')", driver);
    table = msClient.getTable(dbName, tblName);

    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) VALUES(1, 'foo')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName +" (a,b) VALUES(2, 'bar')", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + "(a,b) SELECT a,b FROM "
        + tblName + " UNION ALL SELECT a,b FROM " + tblName, driver);
    verifyFooBarResult(tblName, 3);

    executeStatementOnDriver("ALTER TABLE " + tblName + " SET TBLPROPERTIES "
       + "('transactional'='true', 'transactional_properties'='insert_only')", driver);
    executeStatementOnDriver("INSERT OVERWRITE TABLE " + tblName + " SELECT a,b FROM "
        + tblName + " UNION ALL SELECT a,b FROM " + tblName, driver);
    verifyFooBarResult(tblName, 6);

    runMajorCompaction(dbName, tblName);
    verifyFooBarResult(tblName, 6);
    verifyHasBase(table.getSd(), fs, "base_0000002");

    msClient.close();
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
    String baseDir = "base_0000002";
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
    verifyHasBase(table.getSd(), fs, "base_0000002");
    verifyDirCount(table.getSd(), fs, 1, AcidUtils.baseFileFilter);
    verifyFooBarResult(tblName, 2);

    runCleaner(conf);
    verifyHasDir(table.getSd(), fs, "delta_0000004_0000004_0000", AcidUtils.deltaFileFilter);
    verifyHasDir(table.getSd(), fs, "delta_0000005_0000005_0000", AcidUtils.deltaFileFilter);
    verifyFooBarResult(tblName, 2);

    msClient.abortTxns(Lists.newArrayList(openTxnId)); // Now abort 3.
    runMajorCompaction(dbName, tblName); // Compact 4 and 5.
    verifyFooBarResult(tblName, 2);
    verifyHasBase(table.getSd(), fs, "base_0000005");
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
    verifyHasBase(p1.getSd(), fs, "base_0000006");
    verifyHasBase(p2.getSd(), fs, "base_0000006");

    executeStatementOnDriver("INSERT INTO " + tblName + " partition (ds) VALUES(1, 'foo', 2)", driver);
    executeStatementOnDriver("INSERT INTO " + tblName + " partition (ds) VALUES(2, 'bar', 2)", driver);

    runMajorCompaction(dbName, tblName, "ds=1", "ds=2", "ds=3");

    // Make sure we don't compact if we don't need to compact; but do if we do.
    verifyFooBarResult(tblName, 4);
    verifyDeltaCount(p3.getSd(), fs, 1);
    verifyHasBase(p1.getSd(), fs, "base_0000006");
    verifyHasBase(p2.getSd(), fs, "base_0000008");

  }

  private void verifyFooBarResult(String tblName, int count) throws Exception, IOException {
    List<String> valuesReadFromHiveDriver = new ArrayList<String>();
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
      String dbName, String tblName, String... partNames) throws MetaException {
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setConf(conf);
    t.init(new AtomicBoolean(true), new AtomicBoolean());
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

    StreamingConnection connection1 = null;
    org.apache.hive.hcatalog.streaming.StreamingConnection connection2 = null;
    if (newStreamingAPI) {
      // Write a couple of batches
      for (int i = 0; i < 2; i++) {
        writeBatch(dbName, tblName, false);
      }

      // Start a third batch, but don't close it.
      connection1 = writeBatch(dbName, tblName, true);
    } else {
      HiveEndPoint endPt = new HiveEndPoint(null, dbName, tblName, null);
      DelimitedInputWriter writer = new DelimitedInputWriter(new String[]{"a", "b"}, ",", endPt);
      connection2 = endPt
        .newConnection(false, "UT_" + Thread.currentThread().getName());
      // Write a couple of batches
      for (int i = 0; i < 2; i++) {
        writeBatch(connection2, writer, false);
      }

      // Start a third batch, but don't close it.
      writeBatch(connection2, writer, true);
    }
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
    Assert.assertEquals(name, "base_0000004");
    checkExpectedTxnsPresent(stat[0].getPath(), null, columnNamesProperty, columnTypesProperty, 1, 1L, 4L, 2);
    if (connection1 != null) {
      connection1.close();
    }
    if (connection2 != null) {
      connection2.close();
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
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setConf(conf);
    AtomicBoolean stop = new AtomicBoolean(true);
    AtomicBoolean looped = new AtomicBoolean();
    t.init(stop, looped);
    t.run();

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
      if (deltas[i].equals("delta_0000001_0000003")) {
        minorCompactedDelta = stat[i].getPath();
      }
    }
    Arrays.sort(deltas);
    String[] expectedDeltas = new String[]{"delta_0000001_0000001_0000", "delta_0000001_0000003",
      "delta_0000002_0000002_0000"};
    if (!Arrays.deepEquals(expectedDeltas, deltas)) {
      Assert.fail("Expected: " + Arrays.toString(expectedDeltas) + ", found: " + Arrays.toString(deltas));
    }
    checkExpectedTxnsPresent(null, new Path[]{minorCompactedDelta}, columnNamesProperty, columnTypesProperty,
      0, 1L, 2L, 1);

    // Verify that we have got correct set of delete_deltas.
    FileStatus[] deleteDeltaStat =
      fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.deleteEventDeltaDirFilter);
    String[] deleteDeltas = new String[deleteDeltaStat.length];
    Path minorCompactedDeleteDelta = null;
    for (int i = 0; i < deleteDeltas.length; i++) {
      deleteDeltas[i] = deleteDeltaStat[i].getPath().getName();
      if (deleteDeltas[i].equals("delete_delta_0000001_0000003")) {
        minorCompactedDeleteDelta = deleteDeltaStat[i].getPath();
      }
    }
    Arrays.sort(deleteDeltas);
    String[] expectedDeleteDeltas = new String[]{"delete_delta_0000001_0000003", "delete_delta_0000003_0000003_0000"};
    if (!Arrays.deepEquals(expectedDeleteDeltas, deleteDeltas)) {
      Assert.fail("Expected: " + Arrays.toString(expectedDeleteDeltas) + ", found: " + Arrays.toString(deleteDeltas));
    }
    checkExpectedTxnsPresent(null, new Path[]{minorCompactedDeleteDelta}, columnNamesProperty, columnTypesProperty,
      0, 2L, 2L, 1);
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
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setConf(conf);
    AtomicBoolean stop = new AtomicBoolean(true);
    AtomicBoolean looped = new AtomicBoolean();
    t.init(stop, looped);
    t.run();

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
      if (deltas[i].equals("delta_0000001_0000002")) {
        minorCompactedDelta = stat[i].getPath();
      }
    }
    Arrays.sort(deltas);
    String[] expectedDeltas = new String[]{"delta_0000001_0000001_0000", "delta_0000001_0000002",
      "delta_0000002_0000002_0000"};
    if (!Arrays.deepEquals(expectedDeltas, deltas)) {
      Assert.fail("Expected: " + Arrays.toString(expectedDeltas) + ", found: " + Arrays.toString(deltas));
    }
    checkExpectedTxnsPresent(null, new Path[]{minorCompactedDelta}, columnNamesProperty, columnTypesProperty,
      0, 1L, 2L, 1);

    // Verify that we have got correct set of delete_deltas.
    FileStatus[] deleteDeltaStat =
      fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.deleteEventDeltaDirFilter);
    String[] deleteDeltas = new String[deleteDeltaStat.length];
    Path minorCompactedDeleteDelta = null;
    for (int i = 0; i < deleteDeltas.length; i++) {
      deleteDeltas[i] = deleteDeltaStat[i].getPath().getName();
      if (deleteDeltas[i].equals("delete_delta_0000001_0000002")) {
        minorCompactedDeleteDelta = deleteDeltaStat[i].getPath();
      }
    }
    Arrays.sort(deleteDeltas);
    String[] expectedDeleteDeltas = new String[]{"delete_delta_0000001_0000002"};
    if (!Arrays.deepEquals(expectedDeleteDeltas, deleteDeltas)) {
      Assert.fail("Expected: " + Arrays.toString(expectedDeleteDeltas) + ", found: " + Arrays.toString(deleteDeltas));
    }
    // There should be no rows in the delete_delta because there have been no delete events.
    checkExpectedTxnsPresent(null, new Path[]{minorCompactedDeleteDelta}, columnNamesProperty, columnTypesProperty, 0,
      0L, 0L, 1);
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

    StreamingConnection connection1 = null;
    org.apache.hive.hcatalog.streaming.StreamingConnection connection2 = null;
    if (newStreamingAPI) {

      // Write a couple of batches
      for (int i = 0; i < 2; i++) {
        writeBatch(dbName, tblName, false);
      }

      // Start a third batch, but don't close it.
      connection1 = writeBatch(dbName, tblName, true);
    } else {
      HiveEndPoint endPt = new HiveEndPoint(null, dbName, tblName, null);
      DelimitedInputWriter writer = new DelimitedInputWriter(new String[]{"a", "b"}, ",", endPt);
      connection2 = endPt
        .newConnection(false, "UT_" + Thread.currentThread().getName());
      // Write a couple of batches
      for (int i = 0; i < 2; i++) {
        writeBatch(connection2, writer, false);
      }

      // Start a third batch, but don't close it.
      writeBatch(connection2, writer, true);
    }
    // Now, compact
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    txnHandler.compact(new CompactionRequest(dbName, tblName, CompactionType.MINOR));
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setConf(conf);
    AtomicBoolean stop = new AtomicBoolean(true);
    AtomicBoolean looped = new AtomicBoolean();
    t.init(stop, looped);
    t.run();

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
      if (names[i].equals("delta_0000001_0000004")) {
        resultFile = stat[i].getPath();
      }
    }
    Arrays.sort(names);
    String[] expected = new String[]{"delta_0000001_0000002",
      "delta_0000001_0000004", "delta_0000003_0000004", "delta_0000005_0000006"};
    if (!Arrays.deepEquals(expected, names)) {
      Assert.fail("Expected: " + Arrays.toString(expected) + ", found: " + Arrays.toString(names));
    }
    checkExpectedTxnsPresent(null, new Path[]{resultFile}, columnNamesProperty, columnTypesProperty,
      0, 1L, 4L, 1);

    // Verify that we have got correct set of delete_deltas also
    FileStatus[] deleteDeltaStat =
      fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.deleteEventDeltaDirFilter);
    String[] deleteDeltas = new String[deleteDeltaStat.length];
    Path minorCompactedDeleteDelta = null;
    for (int i = 0; i < deleteDeltas.length; i++) {
      deleteDeltas[i] = deleteDeltaStat[i].getPath().getName();
      if (deleteDeltas[i].equals("delete_delta_0000001_0000004")) {
        minorCompactedDeleteDelta = deleteDeltaStat[i].getPath();
      }
    }
    Arrays.sort(deleteDeltas);
    String[] expectedDeleteDeltas = new String[]{"delete_delta_0000001_0000004"};
    if (!Arrays.deepEquals(expectedDeleteDeltas, deleteDeltas)) {
      Assert.fail("Expected: " + Arrays.toString(expectedDeleteDeltas) + ", found: " + Arrays.toString(deleteDeltas));
    }
    // There should be no rows in the delete_delta because there have been no delete events.
    checkExpectedTxnsPresent(null, new Path[]{minorCompactedDeleteDelta}, columnNamesProperty, columnTypesProperty, 0,
      0L, 0L, 1);

    if (connection1 != null) {
      connection1.close();
    }
    if (connection2 != null) {
      connection2.close();
    }
  }

  /**
   * Users have the choice of specifying compaction related tblproperties either in CREATE TABLE
   * statement or in ALTER TABLE .. COMPACT statement. This tests both cases.
   */
  @Test
  public void testTableProperties() throws Exception {
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
      "'compactorthreshold.hive.compactor.delta.pct.threshold'='0.47'" + // major compaction if more than 47%
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
    AtomicBoolean stop = new AtomicBoolean(true);
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setConf(conf);
    AtomicBoolean looped = new AtomicBoolean();
    t.init(stop, looped);
    t.run();
    JobConf job = t.getMrJob();
    Assert.assertEquals(2048, job.getMemoryForMapTask());  // 2048 comes from tblproperties
    // Compact ttp1
    stop = new AtomicBoolean(true);
    t = new Worker();
    t.setThreadId((int) t.getId());
    t.setConf(conf);
    looped = new AtomicBoolean();
    t.init(stop, looped);
    t.run();
    job = t.getMrJob();
    Assert.assertEquals(1024, job.getMemoryForMapTask());  // 1024 is the default value
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
      "'tblprops.orc.compress.size'='3141')", driver);

    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(4, rsp.getCompacts().size());
    Assert.assertEquals("ttp2", rsp.getCompacts().get(0).getTablename());
    Assert.assertEquals(TxnStore.INITIATED_RESPONSE, rsp.getCompacts().get(0).getState());
    //make sure we are checking the right (latest) compaction entry
    Assert.assertEquals(4, rsp.getCompacts().get(0).getId());

    // Run the Worker explicitly, in order to get the reference to the compactor MR job
    stop = new AtomicBoolean(true);
    t = new Worker();
    t.setThreadId((int) t.getId());
    t.setConf(conf);
    looped = new AtomicBoolean();
    t.init(stop, looped);
    t.run();
    job = t.getMrJob();
    Assert.assertEquals(3072, job.getMemoryForMapTask());
    Assert.assertTrue(job.get("hive.compactor.table.props").contains("orc.compress.size4:3141"));
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

  private void writeBatch(org.apache.hive.hcatalog.streaming.StreamingConnection connection,
    DelimitedInputWriter writer,
    boolean closeEarly) throws InterruptedException, org.apache.hive.hcatalog.streaming.StreamingException {
    TransactionBatch txnBatch = connection.fetchTransactionBatch(2, writer);
    txnBatch.beginNextTransaction();
    txnBatch.write("50,Kiev".getBytes());
    txnBatch.write("51,St. Petersburg".getBytes());
    txnBatch.write("44,Boston".getBytes());
    txnBatch.commit();
    if (!closeEarly) {
      txnBatch.beginNextTransaction();
      txnBatch.write("52,Tel Aviv".getBytes());
      txnBatch.write("53,Atlantis".getBytes());
      txnBatch.write("53,Boston".getBytes());
      txnBatch.commit();
      txnBatch.close();
    }
  }

  private StreamingConnection writeBatch(String dbName, String tblName, boolean closeEarly) throws StreamingException {
    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
      .withFieldDelimiter(',')
      .build();
    StreamingConnection connection = HiveStreamingConnection.newBuilder()
      .withDatabase(dbName)
      .withTable(tblName)
      .withAgentInfo("UT_" + Thread.currentThread().getName())
      .withHiveConf(conf)
      .withRecordWriter(writer)
      .withTransactionBatchSize(2)
      .connect();
    connection.beginTransaction();
    connection.write("50,Kiev".getBytes());
    connection.write("51,St. Petersburg".getBytes());
    connection.write("44,Boston".getBytes());
    connection.commitTransaction();

    if (!closeEarly) {
      connection.beginTransaction();
      connection.write("52,Tel Aviv".getBytes());
      connection.write("53,Atlantis".getBytes());
      connection.write("53,Boston".getBytes());
      connection.commitTransaction();
      connection.close();
      return null;
    }
    return connection;
  }

  private void checkExpectedTxnsPresent(Path base, Path[] deltas, String columnNamesProperty,
    String columnTypesProperty, int bucket, long min, long max, int numBuckets)
    throws IOException {
    ValidWriteIdList writeIdList = new ValidWriteIdList() {
      @Override
      public String getTableName() {
        return "AcidTable";
      }

      @Override
      public boolean isWriteIdValid(long writeid) {
        return true;
      }

      @Override
      public RangeResponse isWriteIdRangeValid(long minWriteId, long maxWriteId) {
        return RangeResponse.ALL;
      }

      @Override
      public String writeToString() {
        return "";
      }

      @Override
      public void readFromString(String src) {

      }

      @Override
      public Long getMinOpenWriteId() {
        return null;
      }

      @Override
      public long getHighWatermark() {
        return Long.MAX_VALUE;
      }

      @Override
      public long[] getInvalidWriteIds() {
        return new long[0];
      }

      @Override
      public boolean isValidBase(long writeid) {
        return true;
      }

      @Override
      public boolean isWriteIdAborted(long writeid) {
        return true;
      }

      @Override
      public RangeResponse isWriteIdRangeAborted(long minWriteId, long maxWriteId) {
        return RangeResponse.ALL;
      }
    };

    OrcInputFormat aif = new OrcInputFormat();

    Configuration conf = new Configuration();
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS, columnNamesProperty);
    conf.set(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES, columnTypesProperty);
    conf.set(hive_metastoreConstants.BUCKET_COUNT, Integer.toString(numBuckets));
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_TRANSACTIONAL_TABLE_SCAN, true);
    AcidInputFormat.RawReader<OrcStruct> reader =
      aif.getRawReader(conf, true, bucket, writeIdList, base, deltas);
    RecordIdentifier identifier = reader.createKey();
    OrcStruct value = reader.createValue();
    long currentTxn = min;
    boolean seenCurrentTxn = false;
    while (reader.next(identifier, value)) {
      if (!seenCurrentTxn) {
        Assert.assertEquals(currentTxn, identifier.getWriteId());
        seenCurrentTxn = true;
      }
      if (currentTxn != identifier.getWriteId()) {
        Assert.assertEquals(currentTxn + 1, identifier.getWriteId());
        currentTxn++;
      }
    }
    Assert.assertEquals(max, currentTxn);
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
    CommandProcessorResponse cpr = driver.run(cmd);
    if (cpr.getResponseCode() != 0) {
      throw new IOException("Failed to execute \"" + cmd + "\". Driver returned: " + cpr);
    }
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

  static void runInitiator(HiveConf hiveConf) throws MetaException {
    AtomicBoolean stop = new AtomicBoolean(true);
    Initiator t = new Initiator();
    t.setThreadId((int) t.getId());
    t.setConf(hiveConf);
    AtomicBoolean looped = new AtomicBoolean();
    t.init(stop, looped);
    t.run();
  }

  static void runWorker(HiveConf hiveConf) throws MetaException {
    AtomicBoolean stop = new AtomicBoolean(true);
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setConf(hiveConf);
    AtomicBoolean looped = new AtomicBoolean();
    t.init(stop, looped);
    t.run();
  }

  static void runCleaner(HiveConf hiveConf) throws MetaException {
    AtomicBoolean stop = new AtomicBoolean(true);
    Cleaner t = new Cleaner();
    t.setThreadId((int) t.getId());
    t.setConf(hiveConf);
    AtomicBoolean looped = new AtomicBoolean();
    t.init(stop, looped);
    t.run();
  }
}
