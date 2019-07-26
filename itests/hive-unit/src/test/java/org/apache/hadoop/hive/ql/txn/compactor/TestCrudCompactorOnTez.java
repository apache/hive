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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.orc.OrcConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("deprecation")
// TODO: Add tests for bucketing_version=1 when HIVE-21167 is fixed
public class TestCrudCompactorOnTez {
  private static final AtomicInteger salt = new AtomicInteger(new Random().nextInt());
  private static final Logger LOG = LoggerFactory.getLogger(TestCrudCompactorOnTez.class);
  private static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") + File.separator
      + TestCrudCompactorOnTez.class.getCanonicalName() + "-" + System.currentTimeMillis() + "_" + salt
          .getAndIncrement()).getPath().replaceAll("\\\\", "/");
  private static final String TEST_WAREHOUSE_DIR = TEST_DATA_DIR + "/warehouse";
  private HiveConf conf;
  IMetaStoreClient msClient;
  private IDriver driver;

  @Before
  // Note: we create a new conf and driver object before every test
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
    hiveConf.setVar(HiveConf.ConfVars.HIVEFETCHTASKCONVERSION, "none");
    TxnDbUtil.setConfValues(hiveConf);
    TxnDbUtil.cleanDb(hiveConf);
    TxnDbUtil.prepDb(hiveConf);
    conf = hiveConf;
    // Use tez as execution engine for this test class
    setupTez(conf);
    msClient = new HiveMetaStoreClient(conf);
    driver = DriverFactory.newDriver(conf);
    SessionState.start(new CliSessionState(conf));
  }

  private void setupTez(HiveConf conf) {
    conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "tez");
    conf.setVar(HiveConf.ConfVars.HIVE_USER_INSTALL_DIR, TEST_DATA_DIR);
    conf.set("tez.am.resource.memory.mb", "128");
    conf.set("tez.am.dag.scheduler.class", "org.apache.tez.dag.app.dag.impl.DAGSchedulerNaturalOrderControlled");
    conf.setBoolean("tez.local.mode", true);
    conf.set("fs.defaultFS", "file:///");
    conf.setBoolean("tez.runtime.optimize.local.fetch", true);
    conf.set("tez.staging-dir", TEST_DATA_DIR);
    conf.setBoolean("tez.ignore.lib.uris", true);
    conf.set("hive.tez.container.size", "128");
    conf.setBoolean("hive.merge.tezfiles", false); 
    conf.setBoolean("hive.in.tez.test", true);
  }

  @After
  public void tearDown() {
    if (msClient != null) {
      msClient.close();
    }
    if (driver != null) {
      driver.close();
    }
    conf = null;
  }

  @Test
  public void testMajorCompaction() throws Exception {
    String dbName = "default";
    String tblName = "testMajorCompaction";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("create transactional table " + tblName + " (a int, b int) clustered by (a) into 2 buckets"
        + " stored as ORC TBLPROPERTIES('bucketing_version'='2', 'transactional'='true',"
        + " 'transactional_properties'='default')", driver);
    executeStatementOnDriver("insert into " + tblName + " values(1,2),(1,3),(1,4),(2,2),(2,3),(2,4)", driver);
    executeStatementOnDriver("insert into " + tblName + " values(3,2),(3,3),(3,4),(4,2),(4,3),(4,4)", driver);
    executeStatementOnDriver("delete from " + tblName + " where b = 2", driver);
    // Find the location of the table
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tblName);
    FileSystem fs = FileSystem.get(conf);
    // Verify deltas (delta_0000001_0000001_0000, delta_0000002_0000002_0000) are present
    FileStatus[] filestatus = fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.deltaFileFilter);
    String[] deltas = new String[filestatus.length];
    for (int i = 0; i < deltas.length; i++) {
      deltas[i] = filestatus[i].getPath().getName();
    }
    Arrays.sort(deltas);
    String[] expectedDeltas = new String[] { "delta_0000001_0000001_0000", "delta_0000002_0000002_0000" };
    if (!Arrays.deepEquals(expectedDeltas, deltas)) {
      Assert.fail("Expected: " + Arrays.toString(expectedDeltas) + ", found: " + Arrays.toString(deltas));
    }
    // Verify that delete delta (delete_delta_0000003_0000003_0000) is present
    FileStatus[] deleteDeltaStat = fs.listStatus(new Path(table.getSd().getLocation()),
        AcidUtils.deleteEventDeltaDirFilter);
    String[] deleteDeltas = new String[deleteDeltaStat.length];
    for (int i = 0; i < deleteDeltas.length; i++) {
      deleteDeltas[i] = deleteDeltaStat[i].getPath().getName();
    }
    Arrays.sort(deleteDeltas);
    String[] expectedDeleteDeltas = new String[] { "delete_delta_0000003_0000003_0000" };
    if (!Arrays.deepEquals(expectedDeleteDeltas, deleteDeltas)) {
      Assert.fail("Expected: " + Arrays.toString(expectedDeleteDeltas) + ", found: " + Arrays.toString(deleteDeltas));
    }
    List<String> expectedRsBucket0 = new ArrayList<>();
    expectedRsBucket0.add("{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t2\t3");
    expectedRsBucket0.add("{\"writeid\":1,\"bucketid\":536870912,\"rowid\":2}\t2\t4");
    expectedRsBucket0.add("{\"writeid\":2,\"bucketid\":536870912,\"rowid\":1}\t3\t3");
    expectedRsBucket0.add("{\"writeid\":2,\"bucketid\":536870912,\"rowid\":2}\t3\t4");
    List<String> expectedRsBucket1 = new ArrayList<>();
    expectedRsBucket1.add("{\"writeid\":1,\"bucketid\":536936448,\"rowid\":1}\t1\t3");
    expectedRsBucket1.add("{\"writeid\":1,\"bucketid\":536936448,\"rowid\":2}\t1\t4");
    expectedRsBucket1.add("{\"writeid\":2,\"bucketid\":536936448,\"rowid\":1}\t4\t3");
    expectedRsBucket1.add("{\"writeid\":2,\"bucketid\":536936448,\"rowid\":2}\t4\t4");
    // Bucket 0
    List<String> rsBucket0 = executeStatementOnDriverAndReturnResults("select ROW__ID, * from " + tblName
        + " where ROW__ID.bucketid = 536870912 order by ROW__ID", driver);
    Assert.assertEquals("normal read", expectedRsBucket0, rsBucket0);
    // Bucket 1
    List<String> rsBucket1 = executeStatementOnDriverAndReturnResults("select ROW__ID, * from " + tblName
        + " where ROW__ID.bucketid = 536936448 order by ROW__ID", driver);
    Assert.assertEquals("normal read", expectedRsBucket1, rsBucket1);
    // Run major compaction and cleaner
    runCompaction(dbName, tblName, CompactionType.MAJOR);
    runCleaner(conf);
    // Should contain only one base directory now
    filestatus = fs.listStatus(new Path(table.getSd().getLocation()));
    String[] bases = new String[filestatus.length];
    for (int i = 0; i < bases.length; i++) {
      bases[i] = filestatus[i].getPath().getName();
    }
    Arrays.sort(bases);
    String[] expectedBases = new String[] { "base_0000003_v0000008" };
    if (!Arrays.deepEquals(expectedBases, bases)) {
      Assert.fail("Expected: " + Arrays.toString(expectedBases) + ", found: " + Arrays.toString(bases));
    }
    // Bucket 0
    List<String> rsCompactBucket0 = executeStatementOnDriverAndReturnResults("select ROW__ID, * from  " + tblName
        + " where ROW__ID.bucketid = 536870912", driver);
    Assert.assertEquals("compacted read", rsBucket0, rsCompactBucket0);
    // Bucket 1
    List<String> rsCompactBucket1 = executeStatementOnDriverAndReturnResults("select ROW__ID, * from  " + tblName
        + " where ROW__ID.bucketid = 536936448", driver);
    Assert.assertEquals("compacted read", rsBucket1, rsCompactBucket1);
    // Clean up
    executeStatementOnDriver("drop table " + tblName, driver);
  }

  @Test
  public void testMinorCompactionDisabled() throws Exception {
    String dbName = "default";
    String tblName = "testMinorCompactionDisabled";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("create transactional table " + tblName + " (a int, b int) clustered by (a) into 2 buckets"
        + " stored as ORC TBLPROPERTIES('bucketing_version'='2', 'transactional'='true',"
        + " 'transactional_properties'='default')", driver);
    executeStatementOnDriver("insert into " + tblName + " values(1,2),(1,3),(1,4),(2,2),(2,3),(2,4)", driver);
    executeStatementOnDriver("insert into " + tblName + " values(3,2),(3,3),(3,4),(4,2),(4,3),(4,4)", driver);
    executeStatementOnDriver("delete from " + tblName + " where b = 2", driver);
    // Find the location of the table
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tblName);
    FileSystem fs = FileSystem.get(conf);
    // Verify deltas (delta_0000001_0000001_0000, delta_0000002_0000002_0000) are present
    FileStatus[] filestatus = fs.listStatus(new Path(table.getSd().getLocation()), AcidUtils.deltaFileFilter);
    String[] deltas = new String[filestatus.length];
    for (int i = 0; i < deltas.length; i++) {
      deltas[i] = filestatus[i].getPath().getName();
    }
    Arrays.sort(deltas);
    String[] expectedDeltas = new String[] { "delta_0000001_0000001_0000", "delta_0000002_0000002_0000" };
    if (!Arrays.deepEquals(expectedDeltas, deltas)) {
      Assert.fail("Expected: " + Arrays.toString(expectedDeltas) + ", found: " + Arrays.toString(deltas));
    }
    // Verify that delete delta (delete_delta_0000003_0000003_0000) is present
    FileStatus[] deleteDeltaStat = fs.listStatus(new Path(table.getSd().getLocation()),
        AcidUtils.deleteEventDeltaDirFilter);
    String[] deleteDeltas = new String[deleteDeltaStat.length];
    for (int i = 0; i < deleteDeltas.length; i++) {
      deleteDeltas[i] = deleteDeltaStat[i].getPath().getName();
    }
    Arrays.sort(deleteDeltas);
    String[] expectedDeleteDeltas = new String[] { "delete_delta_0000003_0000003_0000" };
    if (!Arrays.deepEquals(expectedDeleteDeltas, deleteDeltas)) {
      Assert.fail("Expected: " + Arrays.toString(expectedDeleteDeltas) + ", found: " + Arrays.toString(deleteDeltas));
    }
    // Initiate a compaction, make sure it's not queued
    runInitiator(conf);
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(0, compacts.size());
    // Clean up
    executeStatementOnDriver("drop table " + tblName, driver);
  }

  @Test
  public void testCompactionWithSchemaEvolutionAndBuckets() throws Exception {
    String dbName = "default";
    String tblName = "testCompactionWithSchemaEvolutionAndBuckets";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("create transactional table " + tblName
        + " (a int, b int) partitioned by(ds string) clustered by (a) into 2 buckets"
        + " stored as ORC TBLPROPERTIES('bucketing_version'='2', 'transactional'='true',"
        + " 'transactional_properties'='default')", driver);
    // Insert some data
    executeStatementOnDriver("insert into " + tblName
        + " partition (ds) values(1,2,'today'),(1,3,'today'),(1,4,'yesterday'),(2,2,'yesterday'),(2,3,'today'),(2,4,'today')",
        driver);
    // Add a new column
    executeStatementOnDriver("alter table " + tblName + " add columns(c int)", driver);
    // Insert more data
    executeStatementOnDriver("insert into " + tblName
        + " partition (ds) values(3,2,1000,'yesterday'),(3,3,1001,'today'),(3,4,1002,'yesterday'),(4,2,1003,'today'),"
        + "(4,3,1004,'yesterday'),(4,4,1005,'today')", driver);
    executeStatementOnDriver("delete from " + tblName + " where b = 2", driver);
    //  Run major compaction and cleaner
    runCompaction(dbName, tblName, CompactionType.MAJOR, "ds=yesterday", "ds=today");
    runCleaner(conf);
    List<String> expectedRsBucket0PtnToday = new ArrayList<>();
    expectedRsBucket0PtnToday.add("{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t2\t3\tNULL\ttoday");
    expectedRsBucket0PtnToday.add("{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t2\t4\tNULL\ttoday");
    expectedRsBucket0PtnToday.add("{\"writeid\":3,\"bucketid\":536870912,\"rowid\":0}\t3\t3\t1001\ttoday");
    List<String> expectedRsBucket1PtnToday = new ArrayList<>();
    expectedRsBucket1PtnToday.add("{\"writeid\":1,\"bucketid\":536936448,\"rowid\":1}\t1\t3\tNULL\ttoday");
    expectedRsBucket1PtnToday.add("{\"writeid\":3,\"bucketid\":536936448,\"rowid\":1}\t4\t4\t1005\ttoday");
    // Bucket 0, partition 'today'
    List<String> rsCompactBucket0PtnToday = executeStatementOnDriverAndReturnResults("select ROW__ID, * from  "
        + tblName + " where ROW__ID.bucketid = 536870912 and ds='today'", driver);
    Assert.assertEquals("compacted read", expectedRsBucket0PtnToday, rsCompactBucket0PtnToday);
    // Bucket 1, partition 'today'
    List<String> rsCompactBucket1PtnToday = executeStatementOnDriverAndReturnResults("select ROW__ID, * from  "
        + tblName + " where ROW__ID.bucketid = 536936448 and ds='today'", driver);
    Assert.assertEquals("compacted read", expectedRsBucket1PtnToday, rsCompactBucket1PtnToday);
    // Clean up
    executeStatementOnDriver("drop table " + tblName, driver);
  }

  @Test
  public void testCompactionWithSchemaEvolutionNoBucketsMultipleReducers() throws Exception {
    HiveConf hiveConf = new HiveConf(conf);
    hiveConf.setIntVar(HiveConf.ConfVars.MAXREDUCERS, 2);
    hiveConf.setIntVar(HiveConf.ConfVars.HADOOPNUMREDUCERS, 2);
    driver = DriverFactory.newDriver(hiveConf);
    String dbName = "default";
    String tblName = "testCompactionWithSchemaEvolutionNoBucketsMultipleReducers";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("create transactional table " + tblName + " (a int, b int) partitioned by(ds string)"
        + " stored as ORC TBLPROPERTIES('transactional'='true'," + " 'transactional_properties'='default')", driver);
    // Insert some data
    executeStatementOnDriver("insert into " + tblName
        + " partition (ds) values(1,2,'today'),(1,3,'today'),(1,4,'yesterday'),(2,2,'yesterday'),(2,3,'today'),(2,4,'today')",
        driver);
    // Add a new column
    executeStatementOnDriver("alter table " + tblName + " add columns(c int)", driver);
    // Insert more data
    executeStatementOnDriver("insert into " + tblName
        + " partition (ds) values(3,2,1000,'yesterday'),(3,3,1001,'today'),(3,4,1002,'yesterday'),(4,2,1003,'today'),"
        + "(4,3,1004,'yesterday'),(4,4,1005,'today')", driver);
    executeStatementOnDriver("delete from " + tblName + " where b = 2", driver);
    //  Run major compaction and cleaner
    runCompaction(dbName, tblName, CompactionType.MAJOR, "ds=yesterday", "ds=today");
    runCleaner(hiveConf);
    List<String> expectedRsPtnToday = new ArrayList<>();
    expectedRsPtnToday.add("{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t1\t3\tNULL\ttoday");
    expectedRsPtnToday.add("{\"writeid\":1,\"bucketid\":536870912,\"rowid\":2}\t2\t3\tNULL\ttoday");
    expectedRsPtnToday.add("{\"writeid\":1,\"bucketid\":536870912,\"rowid\":3}\t2\t4\tNULL\ttoday");
    expectedRsPtnToday.add("{\"writeid\":3,\"bucketid\":536870912,\"rowid\":0}\t3\t3\t1001\ttoday");
    expectedRsPtnToday.add("{\"writeid\":3,\"bucketid\":536870912,\"rowid\":2}\t4\t4\t1005\ttoday");
    List<String> expectedRsPtnYesterday = new ArrayList<>();
    expectedRsPtnYesterday.add("{\"writeid\":1,\"bucketid\":536936448,\"rowid\":0}\t1\t4\tNULL\tyesterday");
    expectedRsPtnYesterday.add("{\"writeid\":3,\"bucketid\":536936448,\"rowid\":1}\t3\t4\t1002\tyesterday");
    expectedRsPtnYesterday.add("{\"writeid\":3,\"bucketid\":536936448,\"rowid\":2}\t4\t3\t1004\tyesterday");
    // Partition 'today'
    List<String> rsCompactPtnToday = executeStatementOnDriverAndReturnResults("select ROW__ID, * from  " + tblName
        + " where ds='today'", driver);
    Assert.assertEquals("compacted read", expectedRsPtnToday, rsCompactPtnToday);
    // Partition 'yesterday'
    List<String> rsCompactPtnYesterday = executeStatementOnDriverAndReturnResults("select ROW__ID, * from  " + tblName
        + " where ds='yesterday'", driver);
    Assert.assertEquals("compacted read", expectedRsPtnYesterday, rsCompactPtnYesterday);
    // Clean up
    executeStatementOnDriver("drop table " + tblName, driver);
  }

  private void runCompaction(String dbName, String tblName, CompactionType compactionType, String... partNames)
      throws Exception {
    HiveConf hiveConf = new HiveConf(conf);
    hiveConf.setBoolVar(HiveConf.ConfVars.COMPACTOR_CRUD_QUERY_BASED, true);
    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setConf(hiveConf);
    t.init(new AtomicBoolean(true), new AtomicBoolean());
    if (partNames.length == 0) {
      txnHandler.compact(new CompactionRequest(dbName, tblName, compactionType));
      t.run();
    } else {
      for (String partName : partNames) {
        CompactionRequest cr = new CompactionRequest(dbName, tblName, compactionType);
        cr.setPartitionname(partName);
        txnHandler.compact(cr);
        t.run();
      }
    }
  }

  static void runInitiator(HiveConf hConf) throws Exception {
    HiveConf hiveConf = new HiveConf(hConf);
    hiveConf.setBoolVar(HiveConf.ConfVars.COMPACTOR_CRUD_QUERY_BASED, true);
    AtomicBoolean stop = new AtomicBoolean(true);
    Initiator t = new Initiator();
    t.setThreadId((int) t.getId());
    t.setConf(hiveConf);
    AtomicBoolean looped = new AtomicBoolean();
    t.init(stop, looped);
    t.run();
  }

  static void runWorker(HiveConf hConf) throws Exception {
    HiveConf hiveConf = new HiveConf(hConf);
    hiveConf.setBoolVar(HiveConf.ConfVars.COMPACTOR_CRUD_QUERY_BASED, true);
    AtomicBoolean stop = new AtomicBoolean(true);
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setConf(hiveConf);
    AtomicBoolean looped = new AtomicBoolean();
    t.init(stop, looped);
    t.run();
  }

  static void runCleaner(HiveConf hConf) throws Exception {
    HiveConf hiveConf = new HiveConf(hConf);
    AtomicBoolean stop = new AtomicBoolean(true);
    Cleaner t = new Cleaner();
    t.setThreadId((int) t.getId());
    t.setConf(hiveConf);
    AtomicBoolean looped = new AtomicBoolean();
    t.init(stop, looped);
    t.run();
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

  static List<String> executeStatementOnDriverAndReturnResults(String cmd, IDriver driver) throws Exception {
    LOG.debug("Executing: " + cmd);
    CommandProcessorResponse cpr = driver.run(cmd);
    if (cpr.getResponseCode() != 0) {
      throw new IOException("Failed to execute \"" + cmd + "\". Driver returned: " + cpr);
    }
    List<String> rs = new ArrayList<String>();
    driver.getResults(rs);
    return rs;
  }
}
