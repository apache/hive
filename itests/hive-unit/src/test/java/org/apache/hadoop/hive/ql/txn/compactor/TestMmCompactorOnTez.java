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
package org.apache.hadoop.hive.ql.txn.compactor;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.hooks.HiveProtoLoggingHook;
import org.apache.hadoop.hive.ql.hooks.HiveProtoLoggingHook.ExecutionMode;
import org.apache.hadoop.hive.ql.hooks.TestHiveProtoLoggingHook;
import org.apache.hadoop.hive.ql.hooks.proto.HiveHookEvents;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.tez.dag.history.logging.proto.ProtoMessageReader;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactor.executeStatementOnDriver;

/**
 * Test functionality of MmMinorQueryCompactor.
 */
public class TestMmCompactorOnTez extends CompactorOnTezTest {

  public TestMmCompactorOnTez() {
    mmCompaction = true;
  }

  @Test public void testMmMinorCompactionNotPartitionedWithoutBuckets() throws Exception {
    conf.setVar(HiveConf.ConfVars.COMPACTOR_JOB_QUEUE, CUSTOM_COMPACTION_QUEUE);
    conf.setVar(HiveConf.ConfVars.HIVE_PROTO_EVENTS_BASE_PATH, tmpFolder);

    String dbName = "default";
    String tableName = "testMmMinorCompaction";
    String dbTableName = dbName + "." + tableName;
    // Create test table
    TestDataProvider testDataProvider = new TestCrudCompactorOnTez.TestDataProvider();
    testDataProvider.createMmTable(tableName, false, false);
    // Find the location of the table
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    Table table = msClient.getTable(dbName, tableName);
    FileSystem fs = FileSystem.get(conf);
    // Insert test data into test table
    testDataProvider.insertOnlyTestData(tableName);
    // Get all data before compaction is run
    List<String> expectedData = testDataProvider.getAllData(tableName);
    // Verify deltas
    Assert.assertEquals("Delta directories does not match", Arrays
            .asList("delta_0000001_0000001_0000", "delta_0000002_0000002_0000",
                "delta_0000003_0000003_0000"),
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, null));

    if (isTez(conf)) {
      conf.setVar(HiveConf.ConfVars.PRE_EXEC_HOOKS, HiveProtoLoggingHook.class.getName());
    }
    // Run a compaction
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MINOR, true);
    conf.setVar(HiveConf.ConfVars.PRE_EXEC_HOOKS, StringUtils.EMPTY);

    CompactorTestUtil.runCleaner(conf);
    verifySuccessulTxn(1);
    // Verify delta directories after compaction
    List<String> actualDeltasAfterComp =
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, null);
    Assert.assertEquals("Delta directories does not match after compaction",
        Collections.singletonList("delta_0000001_0000003_v0000007"), actualDeltasAfterComp);
    // Verify bucket files in delta dirs
    List<String> expectedBucketFiles = Collections.singletonList("000000_0");
    Assert.assertEquals("Bucket names are not matching after compaction", expectedBucketFiles,
        CompactorTestUtil.getBucketFileNamesForMMTables(fs, table, null, actualDeltasAfterComp.get(0)));
    verifyAllContents(tableName, testDataProvider, expectedData);
    // Clean up
    testDataProvider.dropTable(tableName);

    if (isTez(conf)) {
      List<ProtoMessageReader<HiveHookEvents.HiveHookEventProto>> readers = TestHiveProtoLoggingHook.getTestReader(conf, tmpFolder);
      Assert.assertEquals(1, readers.size());
      ProtoMessageReader<HiveHookEvents.HiveHookEventProto> reader = readers.get(0);
      HiveHookEvents.HiveHookEventProto event = reader.readEvent();
      while (ExecutionMode.TEZ != ExecutionMode.valueOf(event.getExecutionMode())) {
        event = reader.readEvent();
      }
      Assert.assertNotNull(event);
      Assert.assertEquals(event.getQueue(), CUSTOM_COMPACTION_QUEUE);
    }
  }

  @Test public void testMmMinorCompactionNotPartitionedWithBuckets() throws Exception {
    String dbName = "default";
    String tableName = "testMmMinorCompaction";
    // expected name of the delta dir that will be created with minor compaction
    String newDeltaName = "delta_0000001_0000003_v0000007";
    // Create test table
    TestDataProvider testDataProvider = new TestDataProvider();
    testDataProvider.createMmTable(tableName, false, true);
    // Find the location of the table
    IMetaStoreClient metaStoreClient = new HiveMetaStoreClient(conf);
    Table table = metaStoreClient.getTable(dbName, tableName);
    FileSystem fs = FileSystem.get(conf);
    // Insert test data into test table
    testDataProvider.insertOnlyTestData(tableName);
    // Get all data before compaction is run
    List<String> expectedData = testDataProvider.getAllData(tableName);
    // Verify deltas
    Assert.assertEquals("Delta directories does not match", Arrays
            .asList("delta_0000001_0000001_0000", "delta_0000002_0000002_0000",
                "delta_0000003_0000003_0000"),
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, null));
    // Run a compaction
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MINOR, true);
    CompactorTestUtil.runCleaner(conf);
    verifySuccessulTxn(1);
    // Verify delta directories after compaction
    List<String> actualDeltasAfterComp =
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, null);
    Assert.assertEquals("Delta directories does not match after compaction",
        Collections.singletonList(newDeltaName), actualDeltasAfterComp);
    // Verify number of files in directory
    FileStatus[] files = fs.listStatus(new Path(table.getSd().getLocation(), newDeltaName),
       FileUtils.HIDDEN_FILES_PATH_FILTER);
    Assert.assertEquals("Incorrect number of bucket files", 2, files.length);
    // Verify bucket files in delta dirs
    List<String> expectedBucketFiles = Arrays.asList("000000_0", "000001_0");
    Assert.assertEquals("Bucket names are not matching after compaction", expectedBucketFiles,
        CompactorTestUtil.getBucketFileNamesForMMTables(fs, table, null, actualDeltasAfterComp.get(0)));

    verifyAllContents(tableName, testDataProvider, expectedData);
    // Clean up
    testDataProvider.dropTable(tableName);
  }

  @Test public void testMmMinorCompactionPartitionedWithoutBuckets() throws Exception {
    String dbName = "default";
    String tableName = "testMmMinorCompaction";
    // Create test table
    TestDataProvider dataProvider = new TestDataProvider();
    dataProvider.createMmTable(tableName, true, false);
    // Find the location of the table
    IMetaStoreClient metaStoreClient = new HiveMetaStoreClient(conf);
    Table table = metaStoreClient.getTable(dbName, tableName);
    FileSystem fs = FileSystem.get(conf);
    // Insert test data into test table
    dataProvider.insertOnlyTestDataPartitioned(tableName);
    // Get all data before compaction is run
    List<String> expectedData = dataProvider.getAllData(tableName);
    // Verify deltas
    String partitionToday = "ds=today";
    String partitionTomorrow = "ds=tomorrow";
    String partitionYesterday = "ds=yesterday";
    Assert.assertEquals("Delta directories does not match", Arrays
        .asList("delta_0000001_0000001_0000", "delta_0000002_0000002_0000",
            "delta_0000003_0000003_0000"), CompactorTestUtil
        .getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, partitionToday));
    // Run a compaction
    CompactorTestUtil
        .runCompaction(conf, dbName, tableName, CompactionType.MINOR, true, partitionToday,
            partitionTomorrow, partitionYesterday);
    CompactorTestUtil.runCleaner(conf);
    verifySuccessulTxn(3);
    // Verify delta directories after compaction in each partition
    List<String> actualDeltasAfterCompPartToday =
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, partitionToday);
    Assert.assertEquals("Delta directories does not match after compaction",
        Collections.singletonList("delta_0000001_0000003_v0000007"),
        actualDeltasAfterCompPartToday);
    // Verify bucket files in delta dirs
    List<String> expectedBucketFiles = Collections.singletonList("000000_0");
    Assert.assertEquals("Bucket names are not matching after compaction", expectedBucketFiles,
        CompactorTestUtil
            .getBucketFileNamesForMMTables(fs, table, partitionToday, actualDeltasAfterCompPartToday.get(0)));
    verifyAllContents(tableName, dataProvider, expectedData);
    // Clean up
    dataProvider.dropTable(tableName);
  }

  @Test public void testMmMinorCompactionPartitionedWithBucketsOrc() throws Exception {
    testMmMinorCompactionPartitionedWithBuckets("orc");
  }

  @Test public void testMmMinorCompactionPartitionedWithBucketsParquet() throws Exception {
    testMmMinorCompactionPartitionedWithBuckets("parquet");
  }

  @Test public void testMmMinorCompactionPartitionedWithBucketsAvro() throws Exception {
    testMmMinorCompactionPartitionedWithBuckets("avro");
  }

  @Test public void testMmMinorCompactionPartitionedWithBucketsTextFile() throws Exception {
    testMmMinorCompactionPartitionedWithBuckets("textfile");
  }

  @Test public void testMmMinorCompactionPartitionedWithBucketsSequenceFile() throws Exception {
    testMmMinorCompactionPartitionedWithBuckets("sequencefile");
  }

  @Test public void testMmMinorCompactionPartitionedWithBucketsRcFile() throws Exception {
    testMmMinorCompactionPartitionedWithBuckets("RcFile");
  }

  @Test public void testMmMinorCompactionPartitionedWithBucketsJsonFile() throws Exception {
    testMmMinorCompactionPartitionedWithBuckets("JsonFile");
  }

  private void testMmMinorCompactionPartitionedWithBuckets(String fileFormat) throws Exception {
    String dbName = "default";
    String tableName = "testMmMinorCompaction";
    // Create test table
    TestDataProvider dataProvider = new TestDataProvider();
    dataProvider.createMmTable(tableName, true, true, fileFormat);
    // Find the location of the table
    IMetaStoreClient metaStoreClient = new HiveMetaStoreClient(conf);
    Table table = metaStoreClient.getTable(dbName, tableName);
    FileSystem fs = FileSystem.get(conf);
    // Insert test data into test table
    dataProvider.insertOnlyTestDataPartitioned(tableName);
    // Get all data before compaction is run
    List<String> expectedData = dataProvider.getAllData(tableName);
    // Verify deltas
    String partitionToday = "ds=today";
    String partitionTomorrow = "ds=tomorrow";
    String partitionYesterday = "ds=yesterday";
    Assert.assertEquals("Delta directories does not match", Arrays
        .asList("delta_0000001_0000001_0000", "delta_0000002_0000002_0000",
            "delta_0000003_0000003_0000"), CompactorTestUtil
        .getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, partitionToday));
    // Run a compaction
    CompactorTestUtil
        .runCompaction(conf, dbName, tableName, CompactionType.MINOR, true, partitionToday,
            partitionTomorrow, partitionYesterday);
    CompactorTestUtil.runCleaner(conf);
    verifySuccessulTxn(3);
    // Verify delta directories after compaction in each partition
    List<String> actualDeltasAfterCompPartToday =
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, partitionToday);
    Assert.assertEquals("Delta directories does not match after compaction",
        Collections.singletonList("delta_0000001_0000003_v0000007"),
        actualDeltasAfterCompPartToday);
    // Verify bucket files in delta dirs
    List<String> expectedBucketFiles = Arrays.asList("000000_0", "000001_0");
    Assert.assertEquals("Bucket names are not matching after compaction", expectedBucketFiles,
        CompactorTestUtil
            .getBucketFileNamesForMMTables(fs, table, partitionToday, actualDeltasAfterCompPartToday.get(0)));
    verifyAllContents(tableName, dataProvider, expectedData);
    // Clean up
    dataProvider.dropTable(tableName);
  }

  @Test public void testMmMinorCompaction10DeltaDirs() throws Exception {
    String dbName = "default";
    String tableName = "testMmMinorCompaction";
    // Create test table
    TestDataProvider dataProvider = new TestDataProvider();
    dataProvider.createMmTable(tableName, false, false);
    // Find the location of the table
    IMetaStoreClient metaStoreClient = new HiveMetaStoreClient(conf);
    Table table = metaStoreClient.getTable(dbName, tableName);
    FileSystem fs = FileSystem.get(conf);
    // Insert test data into test table
    dataProvider.insertOnlyTestData(tableName, 10);
    // Get all data before compaction is run
    List<String> expectedData = dataProvider.getAllData(tableName);
    Collections.sort(expectedData);
    // Verify deltas
    List<String> deltaNames =
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, null);
    Assert.assertEquals(10, deltaNames.size());
    // Run a compaction
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MINOR, true);
    CompactorTestUtil.runCleaner(conf);
    List<ShowCompactResponseElement> compacts =
        TxnUtils.getTxnStore(conf).showCompact(new ShowCompactRequest()).getCompacts();
    Assert.assertEquals("Completed compaction queue must contain 3 element", 1, compacts.size());
    compacts.forEach(
        c -> Assert.assertEquals("Compaction state is not succeeded", "succeeded", c.getState()));
    // Verify delta directories after compaction
    List<String> actualDeltasAfterComp =
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, null);
    Assert.assertEquals(Collections.singletonList("delta_0000001_0000010_v0000014"),
        actualDeltasAfterComp);
    // Verify bucket file in delta dir
    List<String> expectedBucketFile = Collections.singletonList("000000_0");
    Assert.assertEquals("Bucket names are not matching after compaction", expectedBucketFile,
        CompactorTestUtil.getBucketFileNamesForMMTables(fs, table, null, actualDeltasAfterComp.get(0)));
    verifyAllContents(tableName, dataProvider, expectedData);
    // Clean up
    dataProvider.dropTable(tableName);
  }

  @Test public void testMultipleMmMinorCompactions() throws Exception {
    String dbName = "default";
    String tableName = "testMmMinorCompaction";
    // Create test table
    TestDataProvider dataProvider = new TestDataProvider();
    dataProvider.createMmTable(tableName, false, true);
    // Find the location of the table
    IMetaStoreClient metaStoreClient = new HiveMetaStoreClient(conf);
    Table table = metaStoreClient.getTable(dbName, tableName);
    FileSystem fs = FileSystem.get(conf);
    // Insert test data into test table
    dataProvider.insertOnlyTestData(tableName);
    // Run a compaction
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MINOR, true);
    CompactorTestUtil.runCleaner(conf);
    verifySuccessulTxn(1);
    // Insert test data into test table
    dataProvider.insertOnlyTestData(tableName);
    // Run a compaction
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MINOR, true);
    CompactorTestUtil.runCleaner(conf);
    verifySuccessulTxn(2);
    // Insert test data into test table
    dataProvider.insertOnlyTestData(tableName);
    // Run a compaction
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MINOR, true);
    CompactorTestUtil.runCleaner(conf);
    verifySuccessulTxn(3);
    // Verify delta directories after compaction
    List<String> actualDeltasAfterComp =
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, null);
    Assert.assertEquals("Delta directories does not match after compaction",
        Collections.singletonList("delta_0000001_0000009_v0000028"), actualDeltasAfterComp);

  }

  @Test public void testMmMajorCompactionAfterMinor() throws Exception {
    String dbName = "default";
    String tableName = "testMmMinorCompaction";
    // Create test table
    TestDataProvider dataProvider = new TestDataProvider();
    dataProvider.createMmTable(tableName, false, false);
    // Find the location of the table
    IMetaStoreClient metaStoreClient = new HiveMetaStoreClient(conf);
    Table table = metaStoreClient.getTable(dbName, tableName);
    FileSystem fs = FileSystem.get(conf);
    // Insert test data into test table
    dataProvider.insertOnlyTestData(tableName);
    // Get all data before compaction is run
    List<String> expectedData = dataProvider.getAllData(tableName);
    Collections.sort(expectedData);
    // Run a compaction
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MINOR, true);
    CompactorTestUtil.runCleaner(conf);
    verifySuccessulTxn(1);
    // Verify delta directories after compaction
    Assert.assertEquals("Delta directories does not match after minor compaction",
        Collections.singletonList("delta_0000001_0000003_v0000007"),
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, null));
    verifyAllContents(tableName, dataProvider, expectedData);
    List<String> actualData;
    // Insert a second round of test data into test table; update expectedData
    dataProvider.insertOnlyTestData(tableName);
    expectedData = dataProvider.getAllData(tableName);
    // Run a compaction
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MAJOR, true);
    CompactorTestUtil.runCleaner(conf);
    verifySuccessulTxn(2);
    // Verify base directory after compaction
    Assert.assertEquals("Base directory does not match after major compaction",
        Collections.singletonList("base_0000006_v0000020"),
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.baseFileFilter, table, null));
    actualData = dataProvider.getAllData(tableName);
    Assert.assertEquals(expectedData, actualData);
  }

  @Test public void testMmMinorCompactionAfterMajor() throws Exception {
    String dbName = "default";
    String tableName = "testMmMinorCompaction";
    // Create test table
    TestDataProvider dataProvider = new TestDataProvider();
    dataProvider.createMmTable(tableName, false, false);
    // Find the location of the table
    IMetaStoreClient metaStoreClient = new HiveMetaStoreClient(conf);
    Table table = metaStoreClient.getTable(dbName, tableName);
    FileSystem fs = FileSystem.get(conf);
    // Insert test data into test table
    dataProvider.insertOnlyTestData(tableName);
    // Get all data before compaction is run
    List<String> expectedData = dataProvider.getAllData(tableName);
    Collections.sort(expectedData);
    // Run a major compaction
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MAJOR, true);
    CompactorTestUtil.runCleaner(conf);
    verifySuccessulTxn(1);
    // Verify base directory after compaction
    Assert.assertEquals("Base directory does not match after major compaction",
        Collections.singletonList("base_0000003_v0000007"),
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.baseFileFilter, table, null));
    verifyAllContents(tableName, dataProvider, expectedData);
    // Insert test data into test table
    dataProvider.insertOnlyTestData(tableName);
    expectedData = dataProvider.getAllData(tableName);
    Collections.sort(expectedData);
    // Run a compaction
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MINOR, true);
    CompactorTestUtil.runCleaner(conf);
    verifySuccessulTxn(2);
    // Verify base/delta directories after compaction
    Assert.assertEquals("Base directory does not match after major compaction",
        Collections.singletonList("base_0000003_v0000007"),
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.baseFileFilter, table, null));
    Assert.assertEquals("Delta directories does not match after minor compaction",
        Collections.singletonList("delta_0000004_0000006_v0000017"),
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, null));
    verifyAllContents(tableName, dataProvider, expectedData);
  }

  @Test public void testMmMinorCompactionWithSchemaEvolutionAndBuckets() throws Exception {
    String dbName = "default";
    String tblName = "testMmMinorCompactionWithSchemaEvolutionAndBuckets";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver("create transactional table " + tblName
        + " (a int, b int) partitioned by(ds string) clustered by (a) into 2 buckets"
        + " stored as ORC TBLPROPERTIES('bucketing_version'='2', 'transactional'='true',"
        + " 'transactional_properties'='insert_only')", driver);
    // Insert some data
    executeStatementOnDriver("insert into " + tblName
            + " partition (ds) values"
            + "(1,2,'today'),(1,3,'today'),(1,4,'yesterday'),"
            + "(2,2,'yesterday'),(2,3,'today'),(2,4,'today')",
        driver);
    // Add a new column
    executeStatementOnDriver("alter table " + tblName + " add columns(c int)", driver);
    // TODO uncomment this line after HIVE-22826 fixed:
    // executeStatementOnDriver("alter table " + tblName + " change column a aa int", driver);
    // Insert more data
    executeStatementOnDriver("insert into " + tblName
        + " partition (ds) values"
        + "(3,2,1000,'yesterday'),(3,3,1001,'today'),"
        + "(3,4,1002,'yesterday'),(4,2,1003,'today'),"
        + "(4,3,1004,'yesterday'),(4,4,1005,'today')", driver);
    // Get all data before compaction is run
    TestDataProvider dataProvider = new TestDataProvider();
    List<String> expectedData = dataProvider.getAllData(tblName);
    Collections.sort(expectedData);
    //  Run minor compaction and cleaner
    CompactorTestUtil
        .runCompaction(conf, dbName, tblName, CompactionType.MINOR, true, "ds=yesterday",
            "ds=today");
    CompactorTestUtil.runCleaner(conf);
    verifySuccessulTxn(2);
    verifyAllContents(tblName, dataProvider, expectedData);
    // Clean up
    executeStatementOnDriver("drop table " + tblName, driver);
  }

  @Test public void testMmMinorCompactionWithSchemaEvolutionNoBucketsMultipleReducers()
      throws Exception {
    HiveConf hiveConf = new HiveConf(conf);
    hiveConf.setIntVar(HiveConf.ConfVars.MAX_REDUCERS, 2);
    hiveConf.setIntVar(HiveConf.ConfVars.HADOOP_NUM_REDUCERS, 2);
    driver = DriverFactory.newDriver(hiveConf);
    String dbName = "default";
    String tblName = "testMmMinorCompactionWithSchemaEvolutionNoBucketsMultipleReducers";
    executeStatementOnDriver("drop table if exists " + tblName, driver);
    executeStatementOnDriver(
        "create transactional table " + tblName + " (a int, b int) partitioned by(ds string)"
            + " stored as ORC TBLPROPERTIES('transactional'='true',"
            + " 'transactional_properties'='insert_only')", driver);
    // Insert some data
    executeStatementOnDriver("insert into " + tblName + " partition (ds) values"
            + "(1,2,'today'),(1,3,'today'),"
            + "(1,4,'yesterday'),(2,2,'yesterday'),"
            + "(2,3,'today'),(2,4,'today')",
        driver);
    // Add a new column
    executeStatementOnDriver("alter table " + tblName + " add columns(c int)", driver);
    // Insert more data
    executeStatementOnDriver("insert into " + tblName
        + " partition (ds) values(3,2,1000,'yesterday'),(3,3,1001,'today'),(3,4,1002,'yesterday'),(4,2,1003,'today'),"
        + "(4,3,1004,'yesterday'),(4,4,1005,'today')", driver);
    // Get all data before compaction is run
    TestDataProvider dataProvider = new TestDataProvider();
    List<String> expectedData = dataProvider.getAllData(tblName);
    Collections.sort(expectedData);
    //  Run minor compaction and cleaner
    CompactorTestUtil
        .runCompaction(conf, dbName, tblName, CompactionType.MINOR, true, "ds=yesterday",
            "ds=today");
    CompactorTestUtil.runCleaner(hiveConf);
    verifySuccessulTxn(2);

    verifyAllContents(tblName, dataProvider, expectedData);
    // Clean up
    executeStatementOnDriver("drop table " + tblName, driver);
  }

  @Test public void testMinorMmCompactionRemovesAbortedDirs()
      throws Exception { // see mmTableOpenWriteId
    String dbName = "default";
    String tableName = "testMmMinorCompaction";
    // Create test table
    TestDataProvider testDataProvider = new TestDataProvider();
    testDataProvider.createMmTable(tableName, false, false);
    // Find the location of the table
    IMetaStoreClient metaStoreClient = new HiveMetaStoreClient(conf);
    Table table = metaStoreClient.getTable(dbName, tableName);
    FileSystem fs = FileSystem.get(conf);
    // Insert test data into test table
    testDataProvider.insertOnlyTestData(tableName);
    // Get all data before compaction is run. Expected data is 2 x MmTestData insertion
    List<String> expectedData = new ArrayList<>();
    List<String> oneMmTestDataInsertion = testDataProvider.getAllData(tableName);
    expectedData.addAll(oneMmTestDataInsertion);
    expectedData.addAll(oneMmTestDataInsertion);
    Collections.sort(expectedData);
    // Insert an aborted directory (txns 4-6)
    rollbackAllTxns(true, driver);
    testDataProvider.insertOnlyTestData(tableName);
    rollbackAllTxns(false, driver);
    // Check that delta dirs 4-6 exist
    List<String> actualDeltasAfterComp =
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, null);
    Assert.assertEquals(Lists
        .newArrayList("delta_0000001_0000001_0000", "delta_0000002_0000002_0000",
            "delta_0000003_0000003_0000", "delta_0000004_0000004_0000",
            "delta_0000005_0000005_0000", "delta_0000006_0000006_0000"), actualDeltasAfterComp);
    // Insert another round of test data (txns 7-9)
    testDataProvider.insertOnlyTestData(tableName);
    verifyAllContents(tableName, testDataProvider, expectedData);
    // Run a minor compaction
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MINOR, true);
    CompactorTestUtil.runCleaner(conf);
    verifySuccessulTxn(1);
    // Verify delta directories after compaction
    Assert.assertEquals("Delta directories does not match after minor compaction",
        Collections.singletonList("delta_0000001_0000009_v0000014"),
        CompactorTestUtil.getBaseOrDeltaNames(fs, AcidUtils.deltaFileFilter, table, null));
    verifyAllContents(tableName, testDataProvider, expectedData);
  }

  @Test public void testMmMajorCompactionDb() throws Exception {
    testMmCompactionDb(CompactionType.MAJOR, "base_0000003_v0000009");
  }

  @Test public void testMmMinorCompactionDb() throws Exception {
    testMmCompactionDb(CompactionType.MINOR, "delta_0000001_0000003_v0000009");
  }

  /**
   * Make sure db is specified in compaction queries.
   */
  private void testMmCompactionDb(CompactionType compactionType, String resultDirName) throws Exception {
    String dbName = "myDb";
    String tableName = "testMmCompactionDb";
    // Create test table
    TestDataProvider dataProvider = new TestDataProvider();
    dataProvider.createDb(dbName);
    dataProvider.createMmTable(dbName, tableName, false, false, "orc");
    // Find the location of the table
    IMetaStoreClient metaStoreClient = new HiveMetaStoreClient(conf);
    Table table = metaStoreClient.getTable(dbName, tableName);
    FileSystem fs = FileSystem.get(conf);
    // Insert test data into test table
    dataProvider.insertOnlyTestData(dbName, tableName);
    // Get all data before compaction is run
    List<String> expectedData = dataProvider.getAllData(dbName, tableName, false);
    Collections.sort(expectedData);
    // Run a compaction
    CompactorTestUtil.runCompaction(conf, dbName, tableName, compactionType, true);
    CompactorTestUtil.runCleaner(conf);
    verifySuccessulTxn(1);
    // Verify directories after compaction
    PathFilter pathFilter = compactionType == CompactionType.MAJOR ? AcidUtils.baseFileFilter :
        AcidUtils.deltaFileFilter;
    Assert.assertEquals("Result directories does not match after " + compactionType.name()
            + " compaction", Collections.singletonList(resultDirName),
        CompactorTestUtil.getBaseOrDeltaNames(fs, pathFilter, table, null));
    List<String> actualData = dataProvider.getAllData(dbName, tableName, false);
    Collections.sort(actualData);
    Assert.assertEquals(expectedData, actualData);
  }

  @Test public void testVectorizationOff() throws Exception {
    conf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, false);
    testMmMinorCompactionAfterMajor();
  }

  /**
   * Verify that the expected number of transactions have run, and their state is "succeeded".
   *
   * @param expectedCompleteCompacts number of compactions already run
   * @throws MetaException
   */
  private void verifySuccessulTxn(int expectedCompleteCompacts) throws MetaException {
    List<ShowCompactResponseElement> compacts =
        TxnUtils.getTxnStore(conf).showCompact(new ShowCompactRequest()).getCompacts();
    Assert.assertEquals("Completed compaction queue must contain one element",
        expectedCompleteCompacts, compacts.size());
    compacts.forEach(
        c -> Assert.assertEquals("Compaction state is not succeeded", "succeeded", c.getState()));
  }

  /**
   * Results of a select on the table results in the same data as expectedData.
   */
  private void verifyAllContents(String tblName, TestDataProvider dataProvider,
      List<String> expectedData) throws Exception {
    List<String> actualData = dataProvider.getAllData(tblName);
    Collections.sort(actualData);
    Assert.assertEquals(expectedData, actualData);
  }

  /**
   * Set to true to cause all transactions to be rolled back, until set back to false.
   */
  private static void rollbackAllTxns(boolean val, IDriver driver) {
    driver.getConf().setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, val);
  }

  private boolean isTez(HiveConf conf){
    return HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equalsIgnoreCase(
        ExecutionMode.TEZ.name());
  }
}
