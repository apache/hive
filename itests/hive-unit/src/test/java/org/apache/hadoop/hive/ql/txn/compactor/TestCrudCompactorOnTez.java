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
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.streaming.HiveStreamingConnection;
import org.apache.hive.streaming.StreamingConnection;
import org.apache.hive.streaming.StrictDelimitedInputWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactor.executeStatementOnDriver;
import static org.apache.hadoop.hive.ql.txn.compactor.CompactorTestUtil.executeStatementOnDriverAndReturnResults;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

@SuppressWarnings("deprecation")
public class TestCrudCompactorOnTez {
  private static final AtomicInteger salt = new AtomicInteger(new Random().nextInt());
  private static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") + File.separator
      + TestCrudCompactorOnTez.class.getCanonicalName() + "-" + System.currentTimeMillis() + "_" + salt
          .getAndIncrement()).getPath().replaceAll("\\\\", "/");
  private static final String TEST_WAREHOUSE_DIR = TEST_DATA_DIR + "/warehouse";
  private HiveConf conf;
  private IMetaStoreClient msClient;
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
    CompactorTestUtil.runCompaction(conf, dbName, tblName, CompactionType.MAJOR, true);
    CompactorTestUtil.runCleaner(conf);
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
  public void testMinorCompactionNotPartitionedWithoutBuckets() throws Exception {
    String dbName = "default";
    String tableName = "testMinorCompaction";
    // Create test table
    TestDataProvider dataProvider = new TestDataProvider();
    dataProvider.createTable(tableName, false, false);
    // Find the location of the table
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tableName);
    FileSystem fs = FileSystem.get(conf);
    // Insert test data into test table
    dataProvider.insertTestData(tableName);
    // Get all data before compaction is run
    List<String> expectedData = dataProvider.getAllData(tableName);
    // Verify deltas
    Assert.assertEquals("Delta directories does not match",
        Arrays.asList("delta_0000001_0000001_0000", "delta_0000002_0000002_0000", "delta_0000004_0000004_0000"),
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, null));
    // Verify delete delta
    Assert.assertEquals("Delete directories does not match",
        Arrays.asList("delete_delta_0000003_0000003_0000", "delete_delta_0000005_0000005_0000"),
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deleteEventDeltaDirFilter, table, null));
    // Run a compaction
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MINOR, true);
    // Clean up resources
    CompactorTestUtil.runCleaner(conf);
    // Only 1 compaction should be in the response queue with succeeded state
    List<ShowCompactResponseElement> compacts =
        TxnUtils.getTxnStore(conf).showCompact(new ShowCompactRequest()).getCompacts();
    Assert.assertEquals("Completed compaction queue must contain one element", 1, compacts.size());
    Assert.assertEquals("Compaction state is not succeeded", "succeeded", compacts.get(0).getState());
    // Verify delta directories after compaction
    List<String> actualDeltasAfterComp =
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, null);
    Assert.assertEquals("Delta directories does not match after compaction",
        Collections.singletonList("delta_0000001_0000005_v0000009"), actualDeltasAfterComp);
    List<String> actualDeleteDeltasAfterComp =
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deleteEventDeltaDirFilter, table, null);
    Assert.assertEquals("Delete delta directories does not match after compaction",
        Collections.singletonList("delete_delta_0000001_0000005_v0000009"), actualDeleteDeltasAfterComp);
    // Verify bucket files in delta dirs
    List<String> expectedBucketFiles = Collections.singletonList("bucket_00000");
    Assert.assertEquals("Bucket names are not matching after compaction", expectedBucketFiles,
        CompactorTestUtil.getBucketFileNames(fs, table, null, actualDeltasAfterComp.get(0)));
    Assert.assertEquals("Bucket names are not matching after compaction", expectedBucketFiles,
        CompactorTestUtil.getBucketFileNames(fs, table, null, actualDeleteDeltasAfterComp.get(0)));
    // Verify contents of bucket files.
    // Bucket 0
    List<String> expectedRsBucket0 = Arrays.asList("{\"writeid\":1,\"bucketid\":536870912,\"rowid\":4}\t2\t3",
        "{\"writeid\":1,\"bucketid\":536870912,\"rowid\":5}\t2\t4",
        "{\"writeid\":2,\"bucketid\":536870912,\"rowid\":1}\t3\t3",
        "{\"writeid\":2,\"bucketid\":536870912,\"rowid\":2}\t3\t4",
        "{\"writeid\":2,\"bucketid\":536870912,\"rowid\":4}\t4\t3",
        "{\"writeid\":2,\"bucketid\":536870912,\"rowid\":5}\t4\t4",
        "{\"writeid\":4,\"bucketid\":536870912,\"rowid\":0}\t5\t2",
        "{\"writeid\":4,\"bucketid\":536870912,\"rowid\":1}\t5\t3",
        "{\"writeid\":4,\"bucketid\":536870912,\"rowid\":2}\t5\t4",
        "{\"writeid\":4,\"bucketid\":536870912,\"rowid\":3}\t6\t2",
        "{\"writeid\":4,\"bucketid\":536870912,\"rowid\":4}\t6\t3",
        "{\"writeid\":4,\"bucketid\":536870912,\"rowid\":5}\t6\t4");
    List<String> rsBucket0 = dataProvider.getBucketData(tableName, "536870912");
    Assert.assertEquals(expectedRsBucket0, rsBucket0);
    // Verify all contents
    List<String> actualData = dataProvider.getAllData(tableName);
    Assert.assertEquals(expectedData, actualData);
    // Clean up
    dataProvider.dropTable(tableName);
  }

  @Test
  public void testMinorCompactionNotPartitionedWithBuckets() throws Exception {
    String dbName = "default";
    String tableName = "testMinorCompaction";
    // Create test table
    TestDataProvider dataProvider = new TestDataProvider();
    dataProvider.createTable(tableName, false, true);
    // Find the location of the table
    IMetaStoreClient metaStoreClient = new HiveMetaStoreClient(conf);
    Table table = metaStoreClient.getTable(dbName, tableName);
    FileSystem fs = FileSystem.get(conf);
    // Insert test data into test table
    dataProvider.insertTestData(tableName);
    // Get all data before compaction is run
    List<String> expectedData = dataProvider.getAllData(tableName);
    // Verify deltas
    Assert.assertEquals("Delta directories does not match",
        Arrays.asList("delta_0000001_0000001_0000", "delta_0000002_0000002_0000", "delta_0000004_0000004_0000"),
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, null));
    // Verify delete delta
    Assert.assertEquals("Delete directories does not match",
        Arrays.asList("delete_delta_0000003_0000003_0000", "delete_delta_0000005_0000005_0000"),
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deleteEventDeltaDirFilter, table, null));
    // Run a compaction
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MINOR, true);
    // Clean up resources
    CompactorTestUtil.runCleaner(conf);
    // Only 1 compaction should be in the response queue with succeeded state
    List<ShowCompactResponseElement> compacts =
        TxnUtils.getTxnStore(conf).showCompact(new ShowCompactRequest()).getCompacts();
    Assert.assertEquals("Completed compaction queue must contain one element", 1, compacts.size());
    Assert.assertEquals("Compaction state is not succeeded", "succeeded", compacts.get(0).getState());
    // Verify delta directories after compaction
    List<String> actualDeltasAfterComp =
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, null);
    Assert.assertEquals("Delta directories does not match after compaction",
        Collections.singletonList("delta_0000001_0000005_v0000009"), actualDeltasAfterComp);
    List<String> actualDeleteDeltasAfterComp =
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deleteEventDeltaDirFilter, table, null);
    Assert.assertEquals("Delete delta directories does not match after compaction",
        Collections.singletonList("delete_delta_0000001_0000005_v0000009"), actualDeleteDeltasAfterComp);
    // Verify bucket files in delta dirs
    List<String> expectedBucketFiles = Arrays.asList("bucket_00000", "bucket_00001");
    Assert.assertEquals("Bucket names are not matching after compaction", expectedBucketFiles,
        CompactorTestUtil.getBucketFileNames(fs, table, null, actualDeltasAfterComp.get(0)));
    Assert.assertEquals("Bucket names are not matching after compaction", expectedBucketFiles,
        CompactorTestUtil.getBucketFileNames(fs, table, null, actualDeleteDeltasAfterComp.get(0)));
    // Verify contents of bucket files.
    // Bucket 0
    List<String> expectedRsBucket0 = Arrays.asList("{\"writeid\":2,\"bucketid\":536870912,\"rowid\":1}\t3\t3",
        "{\"writeid\":2,\"bucketid\":536870912,\"rowid\":2}\t3\t4");
    List<String> rsBucket0 = dataProvider.getBucketData(tableName, "536870912");
    Assert.assertEquals(expectedRsBucket0, rsBucket0);
    // Bucket 1
    List<String> expectedRs1Bucket = Arrays.asList("{\"writeid\":1,\"bucketid\":536936448,\"rowid\":1}\t2\t3",
        "{\"writeid\":1,\"bucketid\":536936448,\"rowid\":2}\t2\t4",
        "{\"writeid\":2,\"bucketid\":536936448,\"rowid\":1}\t4\t3",
        "{\"writeid\":2,\"bucketid\":536936448,\"rowid\":2}\t4\t4",
        "{\"writeid\":4,\"bucketid\":536936448,\"rowid\":0}\t5\t2",
        "{\"writeid\":4,\"bucketid\":536936448,\"rowid\":1}\t5\t3",
        "{\"writeid\":4,\"bucketid\":536936448,\"rowid\":2}\t5\t4",
        "{\"writeid\":4,\"bucketid\":536936448,\"rowid\":3}\t6\t2",
        "{\"writeid\":4,\"bucketid\":536936448,\"rowid\":4}\t6\t3",
        "{\"writeid\":4,\"bucketid\":536936448,\"rowid\":5}\t6\t4");
    List<String> rsBucket1 = dataProvider.getBucketData(tableName, "536936448");
    Assert.assertEquals(expectedRs1Bucket, rsBucket1);
    // Verify all contents
    List<String> actualData = dataProvider.getAllData(tableName);
    Assert.assertEquals(expectedData, actualData);
    // Clean up
    dataProvider.dropTable(tableName);
  }

  @Test
  public void testMinorCompactionPartitionedWithoutBuckets() throws Exception {
    String dbName = "default";
    String tableName = "testMinorCompaction";
    // Create test table
    TestDataProvider dataProvider = new TestDataProvider();
    dataProvider.createTable(tableName, true, false);
    // Find the location of the table
    IMetaStoreClient metaStoreClient = new HiveMetaStoreClient(conf);
    Table table = metaStoreClient.getTable(dbName, tableName);
    FileSystem fs = FileSystem.get(conf);
    // Insert test data into test table
    dataProvider.insertTestDataPartitioned(tableName);
    // Get all data before compaction is run
    List<String> expectedData = dataProvider.getAllData(tableName);
    // Verify deltas
    String partitionToday = "ds=today";
    String partitionTomorrow = "ds=tomorrow";
    String partitionYesterday = "ds=yesterday";
    Assert.assertEquals("Delta directories does not match",
        Arrays.asList("delta_0000001_0000001_0000", "delta_0000002_0000002_0000", "delta_0000004_0000004_0000"),
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, partitionToday));
    // Verify delete delta
    Assert.assertEquals("Delete directories does not match",
        Arrays.asList("delete_delta_0000003_0000003_0000", "delete_delta_0000005_0000005_0000"),
        CompactorTestUtil
            .getBaseOrDeltaNames(fs, AcidUtils.deleteEventDeltaDirFilter, table, partitionToday));
    // Run a compaction
    CompactorTestUtil
        .runCompaction(conf, dbName, tableName, CompactionType.MINOR, true, partitionToday, partitionTomorrow,
            partitionYesterday);
    // Clean up resources
    CompactorTestUtil.runCleaner(conf);
    // 3 compaction should be in the response queue with succeeded state
    List<ShowCompactResponseElement> compacts =
        TxnUtils.getTxnStore(conf).showCompact(new ShowCompactRequest()).getCompacts();
    Assert.assertEquals("Completed compaction queue must contain 3 element", 3, compacts.size());
    compacts.forEach(c -> Assert.assertEquals("Compaction state is not succeeded", "succeeded", c.getState()));
    // Verify delta directories after compaction in each partition
    List<String> actualDeltasAfterCompPartToday =
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, partitionToday);
    Assert.assertEquals("Delta directories does not match after compaction",
        Collections.singletonList("delta_0000001_0000005_v0000009"), actualDeltasAfterCompPartToday);
    List<String> actualDeleteDeltasAfterCompPartToday =
        CompactorTestUtil
            .getBaseOrDeltaNames(fs, AcidUtils.deleteEventDeltaDirFilter, table, partitionToday);
    Assert.assertEquals("Delete delta directories does not match after compaction",
        Collections.singletonList("delete_delta_0000001_0000005_v0000009"), actualDeleteDeltasAfterCompPartToday);
    // Verify bucket files in delta dirs
    List<String> expectedBucketFiles = Collections.singletonList("bucket_00000");
    Assert.assertEquals("Bucket names are not matching after compaction", expectedBucketFiles,
        CompactorTestUtil
            .getBucketFileNames(fs, table, partitionToday, actualDeltasAfterCompPartToday.get(0)));
    Assert.assertEquals("Bucket names are not matching after compaction", expectedBucketFiles,
        CompactorTestUtil
            .getBucketFileNames(fs, table, partitionToday, actualDeleteDeltasAfterCompPartToday.get(0)));
    // Verify contents of bucket files.
    // Bucket 0
    List<String> expectedRsBucket0 = Arrays
        .asList("{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t2\t3\tyesterday",
            "{\"writeid\":1,\"bucketid\":536870912,\"rowid\":2}\t2\t4\ttoday",
            "{\"writeid\":2,\"bucketid\":536870912,\"rowid\":0}\t3\t3\ttoday",
            "{\"writeid\":2,\"bucketid\":536870912,\"rowid\":0}\t3\t4\tyesterday",
            "{\"writeid\":2,\"bucketid\":536870912,\"rowid\":1}\t4\t3\ttomorrow",
            "{\"writeid\":2,\"bucketid\":536870912,\"rowid\":2}\t4\t4\ttoday",
            "{\"writeid\":4,\"bucketid\":536870912,\"rowid\":0}\t5\t4\ttoday",
            "{\"writeid\":4,\"bucketid\":536870912,\"rowid\":0}\t5\t2\tyesterday",
            "{\"writeid\":4,\"bucketid\":536870912,\"rowid\":1}\t6\t2\ttoday",
            "{\"writeid\":4,\"bucketid\":536870912,\"rowid\":1}\t5\t3\tyesterday",
            "{\"writeid\":4,\"bucketid\":536870912,\"rowid\":2}\t6\t3\ttoday",
            "{\"writeid\":4,\"bucketid\":536870912,\"rowid\":3}\t6\t4\ttoday");
    List<String> rsBucket0 = dataProvider.getBucketData(tableName, "536870912");
    Assert.assertEquals(expectedRsBucket0, rsBucket0);
    // Verify all contents
    List<String> actualData = dataProvider.getAllData(tableName);
    Assert.assertEquals(expectedData, actualData);
    // Clean up
    dataProvider.dropTable(tableName);
  }

  @Test
  public void testMinorCompactionPartitionedWithBuckets() throws Exception {
    String dbName = "default";
    String tableName = "testMinorCompaction";
    // Create test table
    TestDataProvider dataProvider = new TestDataProvider();
    dataProvider.createTable(tableName, true, true);
    // Find the location of the table
    IMetaStoreClient metaStoreClient = new HiveMetaStoreClient(conf);
    Table table = metaStoreClient.getTable(dbName, tableName);
    FileSystem fs = FileSystem.get(conf);
    // Insert test data into test table
    dataProvider.insertTestDataPartitioned(tableName);
    // Get all data before compaction is run
    List<String> expectedData = dataProvider.getAllData(tableName);
    // Verify deltas
    String partitionToday = "ds=today";
    String partitionTomorrow = "ds=tomorrow";
    String partitionYesterday = "ds=yesterday";
    Assert.assertEquals("Delta directories does not match",
        Arrays.asList("delta_0000001_0000001_0000", "delta_0000002_0000002_0000", "delta_0000004_0000004_0000"),
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, partitionToday));
    // Verify delete delta
    Assert.assertEquals("Delete directories does not match",
        Arrays.asList("delete_delta_0000003_0000003_0000", "delete_delta_0000005_0000005_0000"),
        CompactorTestUtil
            .getBaseOrDeltaNames(fs, AcidUtils.deleteEventDeltaDirFilter, table, partitionToday));
    // Run a compaction
    CompactorTestUtil
        .runCompaction(conf, dbName, tableName, CompactionType.MINOR, true, partitionToday, partitionTomorrow,
            partitionYesterday);
    // Clean up resources
    CompactorTestUtil.runCleaner(conf);
    // Only 1 compaction should be in the response queue with succeeded state
    List<ShowCompactResponseElement> compacts =
        TxnUtils.getTxnStore(conf).showCompact(new ShowCompactRequest()).getCompacts();
    Assert.assertEquals("Completed compaction queue must contain 3 element", 3, compacts.size());
    compacts.forEach(c -> Assert.assertEquals("Compaction state is not succeeded", "succeeded", c.getState()));
    // Verify delta directories after compaction in each partition
    List<String> actualDeltasAfterCompPartToday =
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, partitionToday);
    Assert.assertEquals("Delta directories does not match after compaction",
        Collections.singletonList("delta_0000001_0000005_v0000009"), actualDeltasAfterCompPartToday);
    List<String> actualDeleteDeltasAfterCompPartToday =
        CompactorTestUtil
            .getBaseOrDeltaNames(fs, AcidUtils.deleteEventDeltaDirFilter, table, partitionToday);
    Assert.assertEquals("Delete delta directories does not match after compaction",
        Collections.singletonList("delete_delta_0000001_0000005_v0000009"), actualDeleteDeltasAfterCompPartToday);
    // Verify bucket files in delta dirs
    List<String> expectedBucketFiles = Arrays.asList("bucket_00000", "bucket_00001");
    Assert.assertEquals("Bucket names are not matching after compaction", expectedBucketFiles,
        CompactorTestUtil
            .getBucketFileNames(fs, table, partitionToday, actualDeltasAfterCompPartToday.get(0)));
    Assert.assertEquals("Bucket names are not matching after compaction", expectedBucketFiles,
        CompactorTestUtil
            .getBucketFileNames(fs, table, partitionToday, actualDeleteDeltasAfterCompPartToday.get(0)));
    // Verify contents of bucket files.
    // Bucket 0
    List<String> expectedRsBucket0 = Arrays.asList("{\"writeid\":2,\"bucketid\":536870912,\"rowid\":0}\t3\t3\ttoday",
        "{\"writeid\":2,\"bucketid\":536870912,\"rowid\":0}\t3\t4\tyesterday");
    List<String> rsBucket0 = dataProvider.getBucketData(tableName, "536870912");
    Assert.assertEquals(expectedRsBucket0, rsBucket0);
    // Bucket 1
    List<String> expectedRsBucket1 = Arrays.asList("{\"writeid\":1,\"bucketid\":536936448,\"rowid\":0}\t2\t4\ttoday",
        "{\"writeid\":1,\"bucketid\":536936448,\"rowid\":0}\t2\t3\tyesterday",
        "{\"writeid\":2,\"bucketid\":536936448,\"rowid\":0}\t4\t3\ttomorrow",
        "{\"writeid\":2,\"bucketid\":536936448,\"rowid\":1}\t4\t4\ttoday",
        "{\"writeid\":4,\"bucketid\":536936448,\"rowid\":0}\t5\t4\ttoday",
        "{\"writeid\":4,\"bucketid\":536936448,\"rowid\":0}\t5\t2\tyesterday",
        "{\"writeid\":4,\"bucketid\":536936448,\"rowid\":1}\t6\t2\ttoday",
        "{\"writeid\":4,\"bucketid\":536936448,\"rowid\":1}\t5\t3\tyesterday",
        "{\"writeid\":4,\"bucketid\":536936448,\"rowid\":2}\t6\t3\ttoday",
        "{\"writeid\":4,\"bucketid\":536936448,\"rowid\":3}\t6\t4\ttoday");
    List<String> rsBucket1 = dataProvider.getBucketData(tableName, "536936448");
    Assert.assertEquals(expectedRsBucket1, rsBucket1);
    // Verify all contents
    List<String> actualData = dataProvider.getAllData(tableName);
    Assert.assertEquals(expectedData, actualData);
    // Clean up
    dataProvider.dropTable(tableName);
  }

  @Test
  public void testMinorCompaction10DeltaDirs() throws Exception {
    String dbName = "default";
    String tableName = "testMinorCompaction";
    // Create test table
    TestDataProvider dataProvider = new TestDataProvider();
    dataProvider.createTable(tableName, false, false);
    // Find the location of the table
    IMetaStoreClient metaStoreClient = new HiveMetaStoreClient(conf);
    Table table = metaStoreClient.getTable(dbName, tableName);
    FileSystem fs = FileSystem.get(conf);
    // Insert test data into test table
    dataProvider.insertTestData(tableName, 10);
    // Get all data before compaction is run
    List<String> expectedData = dataProvider.getAllData(tableName);
    Collections.sort(expectedData);
    // Verify deltas
    List<String> deltaNames = CompactorTestUtil
        .getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, null);
    Assert.assertEquals(10, deltaNames.size());
    List<String> deleteDeltaName =
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deleteEventDeltaDirFilter, table, null);
    Assert.assertEquals(5, deleteDeltaName.size());
    // Run a compaction
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MINOR, true);
    // Clean up resources
    CompactorTestUtil.runCleaner(conf);
    List<ShowCompactResponseElement> compacts =
        TxnUtils.getTxnStore(conf).showCompact(new ShowCompactRequest()).getCompacts();
    Assert.assertEquals("Completed compaction queue must contain 3 element", 1, compacts.size());
    compacts.forEach(c -> Assert.assertEquals("Compaction state is not succeeded", "succeeded", c.getState()));
    // Verify delta directories after compaction
    List<String> actualDeltasAfterComp =
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, null);
    Assert.assertEquals(Collections.singletonList("delta_0000001_0000015_v0000019"), actualDeltasAfterComp);
    List<String> actualDeleteDeltasAfterComp =
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deleteEventDeltaDirFilter, table, null);
    Assert
        .assertEquals(Collections.singletonList("delete_delta_0000001_0000015_v0000019"), actualDeleteDeltasAfterComp);
    // Verify bucket file in delta dir
    List<String> expectedBucketFile = Collections.singletonList("bucket_00000");
    Assert.assertEquals("Bucket names are not matching after compaction", expectedBucketFile,
        CompactorTestUtil.getBucketFileNames(fs, table, null, actualDeltasAfterComp.get(0)));
    Assert.assertEquals("Bucket names are not matching after compaction", expectedBucketFile,
        CompactorTestUtil.getBucketFileNames(fs, table, null, actualDeleteDeltasAfterComp.get(0)));
    // Verify contents of bucket file
    List<String> rsBucket0 = dataProvider.getBucketData(tableName, "536870912");
    Assert.assertEquals(5, rsBucket0.size());
    // Verify all contents
    List<String> actualData = dataProvider.getAllData(tableName);
    Assert.assertEquals(expectedData, actualData);
    // Clean up
    dataProvider.dropTable(tableName);
  }

  @Test
  public void testMultipleMinorCompactions() throws Exception {
    String dbName = "default";
    String tableName = "testMinorCompaction";
    // Create test table
    TestDataProvider dataProvider = new TestDataProvider();
    dataProvider.createTable(tableName, false, true);
    // Find the location of the table
    IMetaStoreClient metaStoreClient = new HiveMetaStoreClient(conf);
    Table table = metaStoreClient.getTable(dbName, tableName);
    FileSystem fs = FileSystem.get(conf);
    // Insert test data into test table
    dataProvider.insertTestData(tableName);
    // Run a compaction
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MINOR, true);
    // Clean up resources
    CompactorTestUtil.runCleaner(conf);
    // Only 1 compaction should be in the response queue with succeeded state
    List<ShowCompactResponseElement> compacts =
        TxnUtils.getTxnStore(conf).showCompact(new ShowCompactRequest()).getCompacts();
    Assert.assertEquals("Completed compaction queue must contain one element", 1, compacts.size());
    Assert.assertEquals("Compaction state is not succeeded", "succeeded", compacts.get(0).getState());
    // Insert test data into test table
    dataProvider.insertTestData(tableName);
    // Run a compaction
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MINOR, true);
    // Clean up resources
    CompactorTestUtil.runCleaner(conf);
    // 2 compaction should be in the response queue with succeeded state
    compacts = TxnUtils.getTxnStore(conf).showCompact(new ShowCompactRequest()).getCompacts();
    Assert.assertEquals("Completed compaction queue must contain one element", 2, compacts.size());
    Assert.assertEquals("Compaction state is not succeeded", "succeeded", compacts.get(1).getState());
    // Insert test data into test table
    dataProvider.insertTestData(tableName);
    // Run a compaction
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MINOR, true);
    // Clean up resources
    CompactorTestUtil.runCleaner(conf);
    // 3 compaction should be in the response queue with succeeded state
    compacts = TxnUtils.getTxnStore(conf).showCompact(new ShowCompactRequest()).getCompacts();
    Assert.assertEquals("Completed compaction queue must contain one element", 3, compacts.size());
    Assert.assertEquals("Compaction state is not succeeded", "succeeded", compacts.get(2).getState());
    // Verify delta directories after compaction
    List<String> actualDeltasAfterComp =
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, null);
    Assert.assertEquals("Delta directories does not match after compaction",
        Collections.singletonList("delta_0000001_0000015_v0000044"), actualDeltasAfterComp);
    List<String> actualDeleteDeltasAfterComp =
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deleteEventDeltaDirFilter, table, null);
    Assert.assertEquals("Delete delta directories does not match after compaction",
        Collections.singletonList("delete_delta_0000001_0000015_v0000044"), actualDeleteDeltasAfterComp);

  }

  @Test
  public void testMinorCompactionWhileStreaming() throws Exception {
    String dbName = "default";
    String tableName = "testMinorCompaction";
    executeStatementOnDriver("drop table if exists " + tableName, driver);
    executeStatementOnDriver("CREATE TABLE " + tableName + "(a INT, b STRING) " + " CLUSTERED BY(a) INTO 1 BUCKETS"
        + " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);
    StreamingConnection connection = null;
    try {
      // Write a couple of batches
      for (int i = 0; i < 2; i++) {
        CompactorTestUtil.writeBatch(conf, dbName, tableName, false, false);
      }

      // Start a third batch, but don't close it.
      connection = CompactorTestUtil.writeBatch(conf, dbName, tableName, false, true);

      // Now, compact
      CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MINOR, true);

      // Find the location of the table
      IMetaStoreClient metaStoreClient = new HiveMetaStoreClient(conf);
      Table table = metaStoreClient.getTable(dbName, tableName);
      FileSystem fs = FileSystem.get(conf);
      Assert.assertEquals("Delta names does not match", Arrays
          .asList("delta_0000001_0000002", "delta_0000001_0000005_v0000009", "delta_0000003_0000004",
              "delta_0000005_0000006"), CompactorTestUtil.getBaseOrDeltaNames(fs, null, table, null));
      CompactorTestUtil.checkExpectedTxnsPresent(null,
          new Path[] {new Path(table.getSd().getLocation(), "delta_0000001_0000005_v0000009")}, "a,b", "int:string",
          0, 1L, 4L, null, 1);

    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test
  public void testMinorCompactionWhileStreamingAfterAbort() throws Exception {
    String dbName = "default";
    String tableName = "testMinorCompaction";
    executeStatementOnDriver("drop table if exists " + tableName, driver);
    executeStatementOnDriver("CREATE TABLE " + tableName + "(a INT, b STRING) " + " CLUSTERED BY(a) INTO 1 BUCKETS"
        + " STORED AS ORC  TBLPROPERTIES ('transactional'='true')", driver);
    CompactorTestUtil.runStreamingAPI(conf, dbName, tableName, Lists
        .newArrayList(new CompactorTestUtil.StreamingConnectionOption(false, false),
            new CompactorTestUtil.StreamingConnectionOption(false, false),
            new CompactorTestUtil.StreamingConnectionOption(true, false)));
    // Now, compact
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MINOR, true);
    // Find the location of the table
    IMetaStoreClient metaStoreClient = new HiveMetaStoreClient(conf);
    Table table = metaStoreClient.getTable(dbName, tableName);
    FileSystem fs = FileSystem.get(conf);
    Assert.assertEquals("Delta names does not match",
        Arrays.asList("delta_0000001_0000002", "delta_0000001_0000006_v0000009", "delta_0000003_0000004"),
        CompactorTestUtil.getBaseOrDeltaNames(fs, null, table, null));
    CompactorTestUtil.checkExpectedTxnsPresent(null,
        new Path[] {new Path(table.getSd().getLocation(), "delta_0000001_0000006_v0000009")}, "a,b", "int:string", 0,
        1L, 4L, Lists.newArrayList(5, 6), 1);
  }

  @Test
  public void testMinorCompactionWhileStreamingWithAbort() throws Exception {
    String dbName = "default";
    String tableName = "testMinorCompaction";
    executeStatementOnDriver("drop table if exists " + tableName, driver);
    executeStatementOnDriver(
        "CREATE TABLE " + tableName + "(a INT, b STRING) " + " STORED AS ORC  TBLPROPERTIES ('transactional'='true')",
        driver);
    CompactorTestUtil.runStreamingAPI(conf, dbName, tableName, Lists
        .newArrayList(new CompactorTestUtil.StreamingConnectionOption(false, false),
            new CompactorTestUtil.StreamingConnectionOption(true, false),
            new CompactorTestUtil.StreamingConnectionOption(false, false)));
    // Now, copact
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MINOR, true);
    // Find the location of the table
    IMetaStoreClient metaStoreClient = new HiveMetaStoreClient(conf);
    Table table = metaStoreClient.getTable(dbName, tableName);
    FileSystem fs = FileSystem.get(conf);
    Assert.assertEquals("Delta names does not match",
        Arrays.asList("delta_0000001_0000002", "delta_0000001_0000006_v0000009", "delta_0000005_0000006"),
        CompactorTestUtil.getBaseOrDeltaNames(fs, null, table, null));
    CompactorTestUtil.checkExpectedTxnsPresent(null,
        new Path[] {new Path(table.getSd().getLocation(), "delta_0000001_0000006_v0000009")}, "a,b", "int:string", 0,
        1L, 6L, Lists.newArrayList(3, 4), 1);
  }

  @Test
  public void testMinorCompactionWhileStreamingWithAbortInMiddle() throws Exception {
    String dbName = "default";
    String tableName = "testMinorCompaction";
    executeStatementOnDriver("drop table if exists " + tableName, driver);
    executeStatementOnDriver(
        "CREATE TABLE " + tableName + "(a INT, b STRING) " + " STORED AS ORC  TBLPROPERTIES ('transactional'='true')",
        driver);
    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder().withFieldDelimiter(',').build();
    StreamingConnection connection = HiveStreamingConnection.newBuilder().withDatabase(dbName).withTable(tableName)
        .withAgentInfo("UT_" + Thread.currentThread().getName()).withHiveConf(conf).withRecordWriter(writer).connect();
    connection.beginTransaction();
    connection.write("50,Kiev".getBytes());
    connection.write("51,St. Petersburg".getBytes());
    connection.write("52,Boston".getBytes());
    connection.commitTransaction();
    connection.beginTransaction();
    connection.write("60,Budapest".getBytes());
    connection.abortTransaction();
    connection.beginTransaction();
    connection.write("71,Szeged".getBytes());
    connection.write("72,Debrecen".getBytes());
    connection.commitTransaction();
    connection.close();
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MINOR, true);
    CompactorTestUtil.runCleaner(conf);
    // Find the location of the table
    IMetaStoreClient metaStoreClient = new HiveMetaStoreClient(conf);
    Table table = metaStoreClient.getTable(dbName, tableName);
    FileSystem fs = FileSystem.get(conf);
    Assert.assertEquals("Delta names does not match", Collections.singletonList("delta_0000001_0000003_v0000006"),
        CompactorTestUtil.getBaseOrDeltaNames(fs, null, table, null));
    CompactorTestUtil.checkExpectedTxnsPresent(null,
        new Path[] {new Path(table.getSd().getLocation(), "delta_0000001_0000003_v0000006")}, "a,b", "int:string", 0,
        1L, 3L, Lists.newArrayList(2), 1);
  }

  @Test
  public void testMajorCompactionAfterMinor() throws Exception {
    String dbName = "default";
    String tableName = "testMinorCompaction";
    // Create test table
    TestDataProvider dataProvider = new TestDataProvider();
    dataProvider.createTable(tableName, false, false);
    // Find the location of the table
    IMetaStoreClient metaStoreClient = new HiveMetaStoreClient(conf);
    Table table = metaStoreClient.getTable(dbName, tableName);
    FileSystem fs = FileSystem.get(conf);
    // Insert test data into test table
    dataProvider.insertTestData(tableName);
    // Get all data before compaction is run
    List<String> expectedData = dataProvider.getAllData(tableName);
    Collections.sort(expectedData);
    // Run a compaction
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MINOR, true);
    // Clean up resources
    CompactorTestUtil.runCleaner(conf);
    // Only 1 compaction should be in the response queue with succeeded state
    List<ShowCompactResponseElement> compacts =
        TxnUtils.getTxnStore(conf).showCompact(new ShowCompactRequest()).getCompacts();
    Assert.assertEquals("Completed compaction queue must contain one element", 1, compacts.size());
    Assert.assertEquals("Compaction state is not succeeded", "succeeded", compacts.get(0).getState());
    // Verify delta directories after compaction
    Assert.assertEquals("Delta directories does not match after minor compaction",
        Collections.singletonList("delta_0000001_0000005_v0000009"),
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, null));
    Assert.assertEquals("Delete delta directories does not match after minor compaction",
        Collections.singletonList("delete_delta_0000001_0000005_v0000009"),
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deleteEventDeltaDirFilter, table, null));
    // Verify all contents
    List<String> actualData = dataProvider.getAllData(tableName);
    Assert.assertEquals(expectedData, actualData);
    // Run a compaction
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MAJOR, true);
    // Clean up resources
    CompactorTestUtil.runCleaner(conf);
    // 2 compaction should be in the response queue with succeeded state
    compacts = TxnUtils.getTxnStore(conf).showCompact(new ShowCompactRequest()).getCompacts();
    Assert.assertEquals("Completed compaction queue must contain one element", 2, compacts.size());
    Assert.assertEquals("Compaction state is not succeeded", "succeeded", compacts.get(1).getState());
    // Verify base directory after compaction
    Assert.assertEquals("Base directory does not match after major compaction",
        Collections.singletonList("base_0000005_v0000023"),
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.baseFileFilter, table, null));
    // Verify all contents
    actualData = dataProvider.getAllData(tableName);
    Assert.assertEquals(expectedData, actualData);
  }

  @Test
  public void testMinorCompactionWhileStreamingWithSplitUpdate() throws Exception {
    String dbName = "default";
    String tableName = "testMinorCompaction";
    executeStatementOnDriver("drop table if exists " + tableName, driver);
    executeStatementOnDriver("CREATE TABLE " + tableName + "(a INT, b STRING) " + " CLUSTERED BY(a) INTO 1 BUCKETS"
        + " STORED AS ORC  TBLPROPERTIES ('transactional'='true'," + "'transactional_properties'='default')", driver);
    StreamingConnection connection = null;
    // Write a couple of batches
    try {
      for (int i = 0; i < 2; i++) {
        CompactorTestUtil.writeBatch(conf, dbName, tableName, false, false);
      }
      // Start a third batch, but don't close it.
      connection = CompactorTestUtil.writeBatch(conf, dbName, tableName, false, true);
      // Now, compact
      CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MINOR, true);
      // Find the location of the table
      IMetaStoreClient metaStoreClient = new HiveMetaStoreClient(conf);
      Table table = metaStoreClient.getTable(dbName, tableName);
      FileSystem fs = FileSystem.get(conf);
      Assert.assertEquals("Delta names does not match", Arrays
          .asList("delta_0000001_0000002", "delta_0000001_0000005_v0000009", "delta_0000003_0000004",
              "delta_0000005_0000006"), CompactorTestUtil.getBaseOrDeltaNames(fs, null, table, null));
      CompactorTestUtil.checkExpectedTxnsPresent(null,
          new Path[] {new Path(table.getSd().getLocation(), "delta_0000001_0000005_v0000009")}, "a,b", "int:string",
          0, 1L, 4L, null, 1);
      //Assert that we have no delete deltas if there are no input delete events.
      Assert.assertEquals(0,
          CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deleteEventDeltaDirFilter, table, null).size());
    } finally {
      if (connection != null) {
        connection.close();
      }
    }

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
    CompactorTestUtil
        .runCompaction(conf, dbName, tblName, CompactionType.MAJOR, true, "ds=yesterday", "ds=today");
    CompactorTestUtil.runCleaner(conf);
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
    CompactorTestUtil
        .runCompaction(conf, dbName, tblName, CompactionType.MAJOR, true, "ds=yesterday", "ds=today");
    CompactorTestUtil.runCleaner(hiveConf);
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

  /**
   * Tests whether hive.llap.io.etl.skip.format config is handled properly whenever QueryCompactor#runCompactionQueries
   * is invoked.
   * @throws Exception
   */
  @Test
  public void testLlapCacheOffDuringCompaction() throws Exception {
    // Setup
    QueryCompactor qc = new QueryCompactor() {
      @Override
      void runCompaction(HiveConf hiveConf, Table table, Partition partition, StorageDescriptor storageDescriptor,
                         ValidWriteIdList writeIds, CompactionInfo compactionInfo) throws IOException {
      }

      @Override
      protected void commitCompaction(String dest, String tmpTableName, HiveConf conf, ValidWriteIdList actualWriteIds,
                                      long compactorTxnId) throws IOException, HiveException {
      }
    };
    StorageDescriptor sdMock = mock(StorageDescriptor.class);
    doAnswer(invocationOnMock -> {
      return null;
    }).when(sdMock).getLocation();
    List<String> emptyQueries = new ArrayList<>();
    HiveConf hiveConf = new HiveConf();
    hiveConf.set(ValidTxnList.VALID_TXNS_KEY, "8:9223372036854775807::");

    // Check for default case.
    qc.runCompactionQueries(hiveConf, null, sdMock, null, null, emptyQueries, emptyQueries, emptyQueries);
    Assert.assertEquals("all", hiveConf.getVar(HiveConf.ConfVars.LLAP_IO_ETL_SKIP_FORMAT));

    // Check for case where  hive.llap.io.etl.skip.format is explicitly set to none - as to always use cache.
    hiveConf.setVar(HiveConf.ConfVars.LLAP_IO_ETL_SKIP_FORMAT, "none");
    qc.runCompactionQueries(hiveConf, null, sdMock, null, null, emptyQueries, emptyQueries, emptyQueries);
    Assert.assertEquals("none", hiveConf.getVar(HiveConf.ConfVars.LLAP_IO_ETL_SKIP_FORMAT));

  }

  private class TestDataProvider {

    private void createTable(String tblName, boolean isPartitioned, boolean isBucketed) throws Exception {
      executeStatementOnDriver("drop table if exists " + tblName, driver);
      StringBuilder query = new StringBuilder();
      query.append("create table ").append(tblName).append(" (a string, b int)");
      if (isPartitioned) {
        query.append(" partitioned by (ds string)");
      }
      if (isBucketed) {
        query.append(" clustered by (a) into 2 buckets");
      }
      query.append(" stored as ORC TBLPROPERTIES('transactional'='true'," + " 'transactional_properties'='default')");
      executeStatementOnDriver(query.toString(), driver);
    }

    private void insertTestDataPartitioned(String tblName) throws Exception {
      executeStatementOnDriver("insert into " + tblName
          + " values('1',2, 'today'),('1',3, 'today'),('1',4, 'yesterday'),('2',2, 'tomorrow'),"
          + "('2',3, 'yesterday'),('2',4, 'today')", driver);
      executeStatementOnDriver("insert into " + tblName
          + " values('3',2, 'tomorrow'),('3',3, 'today'),('3',4, 'yesterday'),('4',2, 'today'),"
          + "('4',3, 'tomorrow'),('4',4, 'today')", driver);
      executeStatementOnDriver("delete from " + tblName + " where b = 2", driver);
      executeStatementOnDriver("insert into " + tblName + " values('5',2, 'yesterday'),('5',3, 'yesterday'),"
          + "('5',4, 'today'),('6',2, 'today'),('6',3, 'today'),('6',4, 'today')", driver);
      executeStatementOnDriver("delete from " + tblName + " where a = '1'", driver);
    }

    private void insertTestData(String tblName) throws Exception {
      executeStatementOnDriver("insert into " + tblName + " values('1',2),('1',3),('1',4),('2',2),('2',3),('2',4)",
          driver);
      executeStatementOnDriver("insert into " + tblName + " values('3',2),('3',3),('3',4),('4',2),('4',3),('4',4)",
          driver);
      executeStatementOnDriver("delete from " + tblName + " where b = 2", driver);
      executeStatementOnDriver("insert into " + tblName + " values('5',2),('5',3),('5',4),('6',2),('6',3),('6',4)",
          driver);
      executeStatementOnDriver("delete from " + tblName + " where a = '1'", driver);
    }

    private void insertTestData(String tblName, int iterations) throws Exception {
      for (int i = 0; i < iterations; i++) {
        executeStatementOnDriver("insert into " + tblName + " values('" + i + "'," + i + ")", driver);
      }
      for (int i = 0; i < iterations; i += 2) {
        executeStatementOnDriver("delete from " + tblName + " where b = " + i, driver);
      }
    }

    private List<String> getAllData(String tblName) throws Exception {
      List<String> result = executeStatementOnDriverAndReturnResults("select * from " + tblName, driver);
      Collections.sort(result);
      return result;
    }

    private List<String> getBucketData(String tblName, String bucketId) throws Exception {
      return executeStatementOnDriverAndReturnResults(
          "select ROW__ID, * from " + tblName + " where ROW__ID.bucketid = " + bucketId + " order by ROW__ID", driver);
    }

    private void dropTable(String tblName) throws Exception {
      executeStatementOnDriver("drop table " + tblName, driver);
    }
  }
}
