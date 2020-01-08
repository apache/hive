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
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.common.ValidWriteIdList;
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
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.common.util.Retry;
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
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

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
    testStatsAfterCompactionPartTbl(false);
  }
  @Test
  public void testStatsAfterCompactionPartTblNew() throws Exception {
    testStatsAfterCompactionPartTbl(true);
  }
  private void testStatsAfterCompactionPartTbl(boolean newStreamingAPI) throws Exception {
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
        writeBatch(dbName, tblName, false);
      }

      // Start a third batch, but don't close it.
      connection = writeBatch(dbName, tblName, true);

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
        Assert.fail("Expected: " + Arrays.toString(expected) + ", found: " + Arrays.toString(names) + ",stat=" + toString(stat));
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
      Assert.assertEquals("base_0000004_v0000009", name);
      checkExpectedTxnsPresent(stat[0].getPath(), null, columnNamesProperty, columnTypesProperty, 0, 1L, 4L, 1);
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test
  public void minorCompactAfterAbort() throws Exception {
    minorCompactAfterAbort(false);
  }
  @Test
  public void minorCompactAfterAbortNew() throws Exception {
    minorCompactAfterAbort(true);
  }
  private void minorCompactAfterAbort(boolean newStreamingAPI) throws Exception {
    String dbName = "default";
    String tblName = "cws";
    String columnNamesProperty = "a,b";
    String columnTypesProperty = "int:string";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);

    processStreamingAPI(dbName, tblName, newStreamingAPI);
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
    checkExpectedTxnsPresent(null, new Path[]{resultDelta}, columnNamesProperty, columnTypesProperty, 0, 1L, 4L, 1);
  }

  @Test
  public void majorCompactAfterAbort() throws Exception {
    majorCompactAfterAbort(false);
  }
  @Test
  public void majorCompactAfterAbortNew() throws Exception {
    majorCompactAfterAbort(true);
  }
  private void majorCompactAfterAbort(boolean newStreamingAPI) throws Exception {
    String dbName = "default";
    String tblName = "cws";
    String columnNamesProperty = "a,b";
    String columnTypesProperty = "int:string";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);

    processStreamingAPI(dbName, tblName, newStreamingAPI);
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
    if (!name.equals("base_0000004_v0000009")) {
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
    verifyHasBase(table.getSd(), fs, "base_0000002_v0000006");

    // Make sure we don't compact if we don't need to compact.
    runMajorCompaction(dbName, tblName);
    verifyFooBarResult(tblName, 1);
    verifyHasBase(table.getSd(), fs, "base_0000002_v0000006");
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
    verifyHasBase(table.getSd(), fs, "base_0000001_v0000009");

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
    verifyHasBase(table.getSd(), fs, "base_0000002_v0000023");

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
      String dbName, String tblName, String... partNames) throws Exception {
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
    majorCompactWhileStreamingForSplitUpdate(false);
  }
  @Test
  public void majorCompactWhileStreamingForSplitUpdateNew() throws Exception {
    majorCompactWhileStreamingForSplitUpdate(true);
  }
  private void majorCompactWhileStreamingForSplitUpdate(boolean newStreamingAPI) throws Exception {
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
    Assert.assertEquals("base_0000004_v0000009", name);
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
    checkExpectedTxnsPresent(null, new Path[]{minorCompactedDelta}, columnNamesProperty, columnTypesProperty,
      0, 1L, 2L, 1);

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
    checkExpectedTxnsPresent(null, new Path[]{minorCompactedDelta}, columnNamesProperty, columnTypesProperty,
      0, 1L, 2L, 1);

    //Assert that we have no delete deltas if there are no input delete events.
    FileStatus[] deleteDeltaStat =
      fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.deleteEventDeltaDirFilter);
    assertEquals(0, deleteDeltaStat.length);
  }

  @Test
  public void minorCompactWhileStreamingWithSplitUpdate() throws Exception {
    minorCompactWhileStreamingWithSplitUpdate(true);
  }
  @Test
  public void minorCompactWhileStreamingWithSplitUpdateNew() throws Exception {
    minorCompactWhileStreamingWithSplitUpdate(true);
  }
  private void minorCompactWhileStreamingWithSplitUpdate(boolean newStreamingAPI) throws Exception {
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
    checkExpectedTxnsPresent(null, new Path[]{resultFile}, columnNamesProperty, columnTypesProperty,
      0, 1L, 4L, 1);

    //Assert that we have no delete deltas if there are no input delete events.
    FileStatus[] deleteDeltaStat =
      fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.deleteEventDeltaDirFilter);
    assertEquals(0, deleteDeltaStat.length);

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

  private List<ShowCompactResponseElement> getCompactionList() throws Exception {
    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 0);
    runInitiator(conf);

    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    return rsp.getCompacts();
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

  private static String toString(FileStatus[] stat) {
    StringBuilder sb = new StringBuilder("stat{");
    if(stat == null) {
      return sb.toString();
    }
    for(FileStatus f : stat) {
      sb.append(f.getPath()).append(",");
    }
    sb.setCharAt(sb.length() - 1, '}');
    return sb.toString();
  }

  private void verifyCompactions(List<ShowCompactResponseElement> compacts, SortedSet<String> partNames, String tblName) {
    for (ShowCompactResponseElement compact : compacts) {
      Assert.assertEquals("default", compact.getDbname());
      Assert.assertEquals(tblName, compact.getTablename());
      Assert.assertEquals("initiated", compact.getState());
      partNames.add(compact.getPartitionname());
    }
  }


  private void processStreamingAPI(String dbName, String tblName, boolean newStreamingAPI)
      throws StreamingException, ClassNotFoundException,
      org.apache.hive.hcatalog.streaming.StreamingException, InterruptedException {
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
  }
}
