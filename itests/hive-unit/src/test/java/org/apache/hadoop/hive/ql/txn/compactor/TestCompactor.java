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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.CompactionTxnHandler;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.streaming.DelimitedInputWriter;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.StreamingConnection;
import org.apache.hive.hcatalog.streaming.StreamingException;
import org.apache.hive.hcatalog.streaming.TransactionBatch;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class TestCompactor {
  private static final AtomicInteger salt = new AtomicInteger(new Random().nextInt());
  private static final Logger LOG = LoggerFactory.getLogger(TestCompactor.class);
  private final String TEST_DATA_DIR = HCatUtil.makePathASafeFileName(System.getProperty("java.io.tmpdir") +
    File.separator + TestCompactor.class.getCanonicalName() + "-" + System.currentTimeMillis() + "_" + salt.getAndIncrement());
  private final String BASIC_FILE_NAME = TEST_DATA_DIR + "/basic.input.data";
  private final String TEST_WAREHOUSE_DIR = TEST_DATA_DIR + "/warehouse";

  @Rule
  public TemporaryFolder stagingFolder = new TemporaryFolder();
  private HiveConf conf;
  IMetaStoreClient msClient;
  private Driver driver;

  @Before
  public void setup() throws Exception {

    File f = new File(TEST_WAREHOUSE_DIR);
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    if(!(new File(TEST_WAREHOUSE_DIR).mkdirs())) {
      throw new RuntimeException("Could not create " + TEST_WAREHOUSE_DIR);
    }

    HiveConf hiveConf = new HiveConf(this.getClass());
    hiveConf.setVar(HiveConf.ConfVars.PREEXECHOOKS, "");
    hiveConf.setVar(HiveConf.ConfVars.POSTEXECHOOKS, "");
    hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, TEST_WAREHOUSE_DIR);
    hiveConf.setVar(HiveConf.ConfVars.HIVEINPUTFORMAT, HiveInputFormat.class.getName());
    hiveConf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
    hiveConf.setVar(HiveConf.ConfVars.HIVEMAPREDMODE, "nonstrict");
    //"org.apache.hadoop.hive.ql.io.HiveInputFormat"

    TxnDbUtil.setConfValues(hiveConf);
    TxnDbUtil.cleanDb();
    TxnDbUtil.prepDb();

    conf = hiveConf;
    msClient = new HiveMetaStoreClient(conf);
    driver = new Driver(hiveConf);
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
    if(msClient != null) {
      msClient.close();
    }
    if(driver != null) {
      driver.close();
    }
  }

  /**
   * Simple schema evolution add columns with partitioning.
   * @throws Exception
   */
  @Test
  public void schemaEvolutionAddColDynamicPartitioningInsert() throws Exception {
    String tblName = "dpct";
    List<String> colNames = Arrays.asList("a", "b");
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
    initiator.setThreadId((int)initiator.getId());
    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 0);
    initiator.setHiveConf(conf);
    AtomicBoolean stop = new AtomicBoolean();
    stop.set(true);
    initiator.init(stop, new AtomicBoolean());
    initiator.run();

    CompactionTxnHandler txnHandler = new CompactionTxnHandler(conf);
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
    List<String> colNames = Arrays.asList("a", "b");
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
    initiator.setThreadId((int)initiator.getId());
    // Set to 1 so insert doesn't set it off but update does
    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 1);
    initiator.setHiveConf(conf);
    AtomicBoolean stop = new AtomicBoolean();
    stop.set(true);
    initiator.init(stop, new AtomicBoolean());
    initiator.run();

    CompactionTxnHandler txnHandler = new CompactionTxnHandler(conf);
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
   * @throws Exception
   * todo:
   * 2. add non-partitioned test
   * 4. add a test with sorted table?
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
      " LOCATION '" + stagingFolder.newFolder().toURI().getPath() + "'" +
      " TBLPROPERTIES ('transactional'='true')", driver);

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

    CompactionTxnHandler txnHandler = new CompactionTxnHandler(conf);
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
    Assert.assertNotNull("No stats found for partition " + ci.partName, colStats);
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
    Assert.assertEquals("nunDVs", 2, colBStats.getNumDVs());

    //now save stats for partition we won't modify
    stats = msClient.getPartitionColumnStatistics(ciPart2.dbname,
      ciPart2.tableName, Arrays.asList(ciPart2.partName), colNames);
    colStats = stats.get(ciPart2.partName);
    LongColumnStatsData colAStatsPart2 = colStats.get(0).getStatsData().getLongStats();
    StringColumnStatsData colBStatsPart2 = colStats.get(1).getStatsData().getStringStats();


    HiveEndPoint endPt = new HiveEndPoint(null, ci.dbname, ci.tableName, Arrays.asList("0"));
    DelimitedInputWriter writer = new DelimitedInputWriter(new String[] {"a","b"},",", endPt);
    /*next call will eventually end up in HiveEndPoint.createPartitionIfNotExists() which
    makes an operation on Driver
    * and starts it's own CliSessionState and then closes it, which removes it from ThreadLoacal;
    * thus the session
    * created in this class is gone after this; I fixed it in HiveEndPoint*/
    StreamingConnection connection = endPt.newConnection(true);

    TransactionBatch txnBatch =  connection.fetchTransactionBatch(2, writer);
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
    execSelectAndDumpData("select * from " + ci.getFullTableName(), driver, ci.getFullTableName());

    //so now we have written some new data to bkt=0 and it shows up
    CompactionRequest rqst = new CompactionRequest(ci.dbname, ci.tableName, CompactionType.MAJOR);
    rqst.setPartitionname(ci.partName);
    txnHandler.compact(rqst);
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setHiveConf(conf);
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
    Assert.assertNotNull("No stats found for partition " + ci.partName, colStats);
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
    Assert.assertEquals("avgColLen b", (long)6.1111111111, (long)colBStats.getAvgColLen());
    Assert.assertEquals("numNulls b", 0, colBStats.getNumNulls());
    Assert.assertEquals("nunDVs", 10, colBStats.getNumDVs());

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
    List<String> colNames = Arrays.asList("a", "b");
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " PARTITIONED BY(ds string)" +
      " CLUSTERED BY(a) INTO 2 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("insert into " + tblName + " partition (ds) values (1, 'fred', " +
        "'today'), (2, 'wilma', 'yesterday')", driver);

    Initiator initiator = new Initiator();
    initiator.setThreadId((int)initiator.getId());
    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 0);
    initiator.setHiveConf(conf);
    AtomicBoolean stop = new AtomicBoolean();
    stop.set(true);
    initiator.init(stop, new AtomicBoolean());
    initiator.run();

    CompactionTxnHandler txnHandler = new CompactionTxnHandler(conf);
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
    List<String> colNames = Arrays.asList("a", "b");
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " PARTITIONED BY(ds string)" +
      " CLUSTERED BY(a) INTO 2 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("insert into " + tblName + " partition (ds) values (1, 'fred', " +
        "'today'), (2, 'wilma', 'yesterday')", driver);

    executeStatementOnDriver("update " + tblName + " set b = 'barney'", driver);

    Initiator initiator = new Initiator();
    initiator.setThreadId((int)initiator.getId());
    // Set to 1 so insert doesn't set it off but update does
    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 1);
    initiator.setHiveConf(conf);
    AtomicBoolean stop = new AtomicBoolean();
    stop.set(true);
    initiator.init(stop, new AtomicBoolean());
    initiator.run();

    CompactionTxnHandler txnHandler = new CompactionTxnHandler(conf);
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
    List<String> colNames = Arrays.asList("a", "b");
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
    initiator.setThreadId((int)initiator.getId());
    // Set to 2 so insert and update don't set it off but delete does
    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 2);
    initiator.setHiveConf(conf);
    AtomicBoolean stop = new AtomicBoolean();
    stop.set(true);
    initiator.init(stop, new AtomicBoolean());
    initiator.run();

    CompactionTxnHandler txnHandler = new CompactionTxnHandler(conf);
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
    List<String> colNames = Arrays.asList("a", "b");
    String columnNamesProperty = "a,b";
    String columnTypesProperty = "int:string";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
        " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
        " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);

    HiveEndPoint endPt = new HiveEndPoint(null, dbName, tblName, null);
    DelimitedInputWriter writer = new DelimitedInputWriter(new String[] {"a","b"},",", endPt);
    StreamingConnection connection = endPt.newConnection(false);
    try {
      // Write a couple of batches
      for (int i = 0; i < 2; i++) {
        writeBatch(connection, writer, false);
      }

      // Start a third batch, but don't close it.
      writeBatch(connection, writer, true);

      // Now, compact
      CompactionTxnHandler txnHandler = new CompactionTxnHandler(conf);
      txnHandler.compact(new CompactionRequest(dbName, tblName, CompactionType.MINOR));
      Worker t = new Worker();
      t.setThreadId((int) t.getId());
      t.setHiveConf(conf);
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
      checkExpectedTxnsPresent(null, new Path[]{resultFile},columnNamesProperty, columnTypesProperty,  0, 1L, 4L);

    } finally {
      connection.close();
    }
  }

  @Test
  public void majorCompactWhileStreaming() throws Exception {
    String dbName = "default";
    String tblName = "cws";
    List<String> colNames = Arrays.asList("a", "b");
    String columnNamesProperty = "a,b";
    String columnTypesProperty = "int:string";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
        " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
        " STORED AS ORC  TBLPROPERTIES ('transactional'='true') ", driver);

    HiveEndPoint endPt = new HiveEndPoint(null, dbName, tblName, null);
    DelimitedInputWriter writer = new DelimitedInputWriter(new String[] {"a","b"},",", endPt);
    StreamingConnection connection = endPt.newConnection(false);
    try {
      // Write a couple of batches
      for (int i = 0; i < 2; i++) {
        writeBatch(connection, writer, false);
      }

      // Start a third batch, but don't close it.
      writeBatch(connection, writer, true);

      // Now, compact
      CompactionTxnHandler txnHandler = new CompactionTxnHandler(conf);
      txnHandler.compact(new CompactionRequest(dbName, tblName, CompactionType.MAJOR));
      Worker t = new Worker();
      t.setThreadId((int) t.getId());
      t.setHiveConf(conf);
      AtomicBoolean stop = new AtomicBoolean(true);
      AtomicBoolean looped = new AtomicBoolean();
      t.init(stop, looped);
      t.run();

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
      checkExpectedTxnsPresent(stat[0].getPath(), null, columnNamesProperty, columnTypesProperty, 0, 1L, 4L);
    } finally {
      connection.close();
    }
  }

  @Test
  public void minorCompactAfterAbort() throws Exception {
    String dbName = "default";
    String tblName = "cws";
    List<String> colNames = Arrays.asList("a", "b");
    String columnNamesProperty = "a,b";
    String columnTypesProperty = "int:string";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
        " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
        " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);

    HiveEndPoint endPt = new HiveEndPoint(null, dbName, tblName, null);
    DelimitedInputWriter writer = new DelimitedInputWriter(new String[] {"a","b"},",", endPt);
    StreamingConnection connection = endPt.newConnection(false);
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

      // Now, compact
      CompactionTxnHandler txnHandler = new CompactionTxnHandler(conf);
      txnHandler.compact(new CompactionRequest(dbName, tblName, CompactionType.MINOR));
      Worker t = new Worker();
      t.setThreadId((int) t.getId());
      t.setHiveConf(conf);
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
        if (names[i].equals("delta_0000001_0000006")) {
          resultDelta = stat[i].getPath();
        }
      }
      Arrays.sort(names);
      String[] expected = new String[]{"delta_0000001_0000002",
          "delta_0000001_0000006", "delta_0000003_0000004", "delta_0000005_0000006"};
      if (!Arrays.deepEquals(expected, names)) {
        Assert.fail("Expected: " + Arrays.toString(expected) + ", found: " + Arrays.toString(names));
      }
      checkExpectedTxnsPresent(null, new Path[]{resultDelta}, columnNamesProperty, columnTypesProperty, 0, 1L, 4L);
    } finally {
      connection.close();
    }
  }

  /**
   * HIVE-12352 has details
   * @throws Exception
   */
  @Test
  public void writeBetweenWorkerAndCleaner() throws Exception {
    String tblName = "HIVE12352";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
      " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
      " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);

    //create some data
    executeStatementOnDriver("insert into " + tblName + " values(1, 'foo'),(2, 'bar'),(3, 'baz')", driver);
    executeStatementOnDriver("update " + tblName + " set b = 'blah' where a = 3", driver);

    //run Worker to execute compaction
    CompactionTxnHandler txnHandler = new CompactionTxnHandler(conf);
    txnHandler.compact(new CompactionRequest("default", tblName, CompactionType.MINOR));
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setHiveConf(conf);
    AtomicBoolean stop = new AtomicBoolean(true);
    AtomicBoolean looped = new AtomicBoolean();
    t.init(stop, looped);
    t.run();

    //delete something, but make sure txn is rolled back
    conf.setBoolVar(HiveConf.ConfVars.HIVETESTMODEROLLBACKTXN, true);
    executeStatementOnDriver("delete from " + tblName + " where a = 1", driver);
    conf.setBoolVar(HiveConf.ConfVars.HIVETESTMODEROLLBACKTXN, false);

    List<String> expected = new ArrayList<>();
    expected.add("1\tfoo");
    expected.add("2\tbar");
    expected.add("3\tblah");
    Assert.assertEquals("", expected,
      execSelectAndDumpData("select a,b from " + tblName + " order by a", driver, "writeBetweenWorkerAndCleaner()"));

    //run Cleaner
    Cleaner c = new Cleaner();
    c.setThreadId((int)c.getId());
    c.setHiveConf(conf);
    c.init(stop, new AtomicBoolean());
    c.run();

    //this seems odd, but we wan to make sure that to run CompactionTxnHandler.cleanEmptyAbortedTxns()
    Initiator i = new Initiator();
    i.setThreadId((int)i.getId());
    i.setHiveConf(conf);
    i.init(stop, new AtomicBoolean());
    i.run();

    //check that aborted operation didn't become committed
    Assert.assertEquals("", expected,
      execSelectAndDumpData("select a,b from " + tblName + " order by a", driver, "writeBetweenWorkerAndCleaner()"));
  }
  @Test
  public void majorCompactAfterAbort() throws Exception {
    String dbName = "default";
    String tblName = "cws";
    List<String> colNames = Arrays.asList("a", "b");
    String columnNamesProperty = "a,b";
    String columnTypesProperty = "int:string";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("CREATE TABLE " + tblName + "(a INT, b STRING) " +
        " CLUSTERED BY(a) INTO 1 BUCKETS" + //currently ACID requires table to be bucketed
        " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);

    HiveEndPoint endPt = new HiveEndPoint(null, dbName, tblName, null);
    DelimitedInputWriter writer = new DelimitedInputWriter(new String[] {"a","b"},",", endPt);
    StreamingConnection connection = endPt.newConnection(false);
    try {
      // Write a couple of batches
      for (int i = 0; i < 2; i++) {
        writeBatch(connection, writer, false);
      }

      // Start a third batch, but don't close it.
      TransactionBatch txnBatch = connection.fetchTransactionBatch(2, writer);
      txnBatch.beginNextTransaction();
      txnBatch.abort();
      txnBatch.beginNextTransaction();
      txnBatch.abort();


      // Now, compact
      CompactionTxnHandler txnHandler = new CompactionTxnHandler(conf);
      txnHandler.compact(new CompactionRequest(dbName, tblName, CompactionType.MAJOR));
      Worker t = new Worker();
      t.setThreadId((int) t.getId());
      t.setHiveConf(conf);
      AtomicBoolean stop = new AtomicBoolean(true);
      AtomicBoolean looped = new AtomicBoolean();
      t.init(stop, looped);
      t.run();

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
        Assert.fail("Expecting 1 file \"base_0000006\" and found " + stat.length + " files " + Arrays.toString(stat));
      }
      String name = stat[0].getPath().getName();
      if (!name.equals("base_0000006")) {
        Assert.fail("majorCompactAfterAbort name " + name + " not equals to base_0000006");
      }
      checkExpectedTxnsPresent(stat[0].getPath(), null, columnNamesProperty, columnTypesProperty, 0, 1L, 4L);
    } finally {
      connection.close();
    }
  }
  private void writeBatch(StreamingConnection connection, DelimitedInputWriter writer,
                          boolean closeEarly)
      throws InterruptedException, StreamingException {
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

  private void checkExpectedTxnsPresent(Path base, Path[] deltas, String columnNamesProperty,
      String columnTypesProperty, int bucket, long min, long max)
      throws IOException {
    ValidTxnList txnList = new ValidTxnList() {
      @Override
      public boolean isTxnValid(long txnid) {
        return true;
      }

      @Override
      public RangeResponse isTxnRangeValid(long minTxnId, long maxTxnId) {
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
      public long getHighWatermark() {
        return  Long.MAX_VALUE;
      }

      @Override
      public long[] getInvalidTransactions() {
        return new long[0];
      }
    };

    OrcInputFormat aif = new OrcInputFormat();

    Configuration conf = new Configuration();
    conf.set("columns", columnNamesProperty);
    conf.set("columns.types", columnTypesProperty);
    AcidInputFormat.RawReader<OrcStruct> reader =
        aif.getRawReader(conf, false, bucket, txnList, base, deltas);
    RecordIdentifier identifier = reader.createKey();
    OrcStruct value = reader.createValue();
    long currentTxn = min;
    boolean seenCurrentTxn = false;
    while (reader.next(identifier, value)) {
      if (!seenCurrentTxn) {
        Assert.assertEquals(currentTxn, identifier.getTransactionId());
        seenCurrentTxn = true;
      }
      if (currentTxn != identifier.getTransactionId()) {
        Assert.assertEquals(currentTxn + 1, identifier.getTransactionId());
        currentTxn++;
      }
    }
    Assert.assertEquals(max, currentTxn);
  }

  /**
   * convenience method to execute a select stmt and dump results to log file
   */
  private static List<String> execSelectAndDumpData(String selectStmt, Driver driver, String msg)
    throws  Exception {
    executeStatementOnDriver(selectStmt, driver);
    ArrayList<String> valuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(valuesReadFromHiveDriver);
    int rowIdx = 0;
    LOG.debug(msg);
    for(String row : valuesReadFromHiveDriver) {
      LOG.debug(" rowIdx=" + rowIdx++ + ":" + row);
    }
    return valuesReadFromHiveDriver;
  }
  /**
   * Execute Hive CLI statement
   * @param cmd arbitrary statement to execute
   */
  static void executeStatementOnDriver(String cmd, Driver driver) throws IOException, CommandNeedRetryException {
    LOG.debug("Executing: " + cmd);
    CommandProcessorResponse cpr = driver.run(cmd);
    if(cpr.getResponseCode() != 0) {
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
}
