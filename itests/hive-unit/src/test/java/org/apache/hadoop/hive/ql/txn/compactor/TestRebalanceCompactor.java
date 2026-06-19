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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hive.ql.ErrorMsg.TXN_ABORTED;
import static org.apache.hadoop.hive.ql.TxnCommandsBaseForTests.runWorker;
import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactorBase.dropTables;
import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactorBase.execSelectAndDumpData;
import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactorBase.executeStatementOnDriver;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

public class TestRebalanceCompactor extends CompactorOnTezTest {

  @Test
  public void testRebalanceCompactionWithParallelDeleteAsSecondOptimisticLock() throws Exception {
    testRebalanceCompactionWithParallelDeleteAsSecond(true);
  }

  @Test
  public void testRebalanceCompactionWithParallelDeleteAsSecondPessimisticLock() throws Exception {
    testRebalanceCompactionWithParallelDeleteAsSecond(false);
  }

  @Test
  public void testRebalanceCompactionOfNotPartitionedImplicitlyBucketedTableWithOrder() throws Exception {
    prepareHiveConfForRebalanceCompaction();

    conf.set("tez.grouping.min-size", "400");
    conf.set("tez.grouping.max-size", "5000");
    driver = new Driver(conf);

    final String tableName = "rebalance_test";
    TestDataProvider testDataProvider = prepareRebalanceTestData();

    //Try to do a rebalancing compaction
    executeStatementOnDriver("ALTER TABLE " + tableName + " COMPACT 'rebalance' ORDER BY b DESC", driver);
    runWorker(conf);

    driver.close();
    driver = new Driver(conf);

    //Check if the compaction succeed
    verifyCompaction(1, TxnStore.CLEANING_RESPONSE);

    // Populate expected data
    List<RowData> expectedData = new ArrayList<>();

    expectedData.addAll(List.of(
        new RowData("17", "17"),
        new RowData("16", "16"),
        new RowData("15", "15"),
        new RowData("14", "14"),
        new RowData("13", "13"),
        new RowData("12", "12")
    ));

    // Adding the '4' group
    expectedData.addAll(List.of(
        new RowData("6", "4"),
        new RowData("3", "4"),
        new RowData("4", "4"),
        new RowData("2", "4"),
        new RowData("5", "4")
    ));

    // Adding the '3' group
    expectedData.addAll(List.of(
        new RowData("2", "3"),
        new RowData("3", "3"),
        new RowData("6", "3"),
        new RowData("4", "3"),
        new RowData("5", "3")
    ));

    // Adding the '2' group
    expectedData.addAll(List.of(
        new RowData("6", "2"),
        new RowData("5", "2")
    ));

    verifyDataAfterCompaction(expectedData, testDataProvider);
  }

  private void prepareHiveConfForRebalanceCompaction() {
    conf.setBoolVar(HiveConf.ConfVars.COMPACTOR_CRUD_QUERY_BASED, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_COMPACTOR_GATHER_STATS, false);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER, false);
  }

  @Test
  public void testRebalanceCompactionOfNotPartitionedImplicitlyBucketedTable() throws Exception {
    prepareHiveConfForRebalanceCompaction();

    // set grouping size to have 3 buckets, and re-create driver with the new config
    conf.set("tez.grouping.min-size", "400");
    conf.set("tez.grouping.max-size", "5000");
    driver = new Driver(conf);

    final String tableName = "rebalance_test";
    TestDataProvider testDataProvider = prepareRebalanceTestData();

    // Run rebalance compaction
    executeStatementOnDriver("ALTER TABLE " + tableName + " COMPACT 'rebalance'", driver);
    runWorker(conf);

    // Check if the compaction succeed
    verifyCompaction(1, TxnStore.CLEANING_RESPONSE);

    // Populate expected data
    List<RowData> expectedData = new ArrayList<>();

    expectedData.addAll(List.of(
        new RowData("5", "4"),
        new RowData("6", "2"),
        new RowData("6", "3"),
        new RowData("6", "4"),
        new RowData("5", "2")
    ));

    expectedData.addAll(List.of(
        new RowData("5", "3"),
        new RowData("2", "4"),
        new RowData("3", "3"),
        new RowData("4", "4"),
        new RowData("4", "3")
    ));

    expectedData.addAll(List.of(
        new RowData("2", "3"),
        new RowData("3", "4"),
        new RowData("12", "12"),
        new RowData("13", "13"),
        new RowData("14", "14")
    ));

    expectedData.addAll(List.of(
        new RowData("15", "15"),
        new RowData("16", "16"),
        new RowData("17", "17")
    ));

    verifyDataAfterCompaction(expectedData, testDataProvider, null, false);
  }

  @Test
  public void testRebalanceCompactionOfPartitionedImplicitlyBucketedTable() throws Exception {
    prepareHiveConfForRebalanceCompaction();

    //set grouping size to have 3 buckets, and re-create driver with the new config
    conf.set("tez.grouping.min-size", "1");
    driver = new Driver(conf);

    final String stageTableName = "stage_rebalance_test";
    final String tableName = "rebalance_test";

    TestDataProvider testDataProvider = new TestDataProvider();
    testDataProvider.createFullAcidTable(stageTableName, true, false);
    executeStatementOnDriver("insert into " + stageTableName +" values " +
            "('1',1,'yesterday'), ('1',2,'yesterday'), ('1',3, 'yesterday'), ('1',4, 'yesterday'), " +
            "('2',1,'today'), ('2',2,'today'), ('2',3,'today'), ('2',4, 'today'), " +
            "('3',1,'tomorrow'), ('3',2,'tomorrow'), ('3',3,'tomorrow'), ('3',4,'tomorrow')",
        driver);

    dropTables(driver, tableName);
    executeStatementOnDriver("CREATE TABLE " + tableName + "(a string, b int) " +
        "PARTITIONED BY (ds string) STORED AS ORC TBLPROPERTIES('transactional'='true')", driver);
    executeStatementOnDriver(
        "INSERT OVERWRITE TABLE " + tableName + " partition (ds='tomorrow') select a, b from " + stageTableName, driver
    );

    //do some single inserts to have more data in the first bucket.
    executeStatementOnDriver("INSERT INTO TABLE " + tableName + " values ('12',12,'tomorrow')", driver);
    executeStatementOnDriver("INSERT INTO TABLE " + tableName + " values ('13',13,'tomorrow')", driver);
    executeStatementOnDriver("INSERT INTO TABLE " + tableName + " values ('14',14,'tomorrow')", driver);
    executeStatementOnDriver("INSERT INTO TABLE " + tableName + " values ('15',15,'tomorrow')", driver);
    executeStatementOnDriver("INSERT INTO TABLE " + tableName + " values ('16',16,'tomorrow')", driver);
    executeStatementOnDriver("INSERT INTO TABLE " + tableName + " values ('17',17,'tomorrow')", driver);

    // Verify buckets and their content before rebalance in partition ds=tomorrow
    // Make sure we have all the records persisted
    List<String> allRecords = execSelectAndDumpData(
        "SELECT * FROM " + tableName, driver, "Dumping data from test table, " + tableName);
    Assert.assertEquals(18, allRecords.size());

    Assert.assertFalse(isBalanced(testDataProvider, "ds=tomorrow"));

    //Try to do a rebalancing compaction
    executeStatementOnDriver("ALTER TABLE " + tableName + " PARTITION (ds='tomorrow') COMPACT 'rebalance'", driver);
    runWorker(conf);

    //Check if the compaction succeed
    verifyCompaction(1, TxnStore.CLEANING_RESPONSE);

    List<RowData> expectedData = new ArrayList<>();

    expectedData.addAll(List.of(
      new RowData("2", "1", "tomorrow"),
      new RowData("2", "2", "tomorrow"),
      new RowData("2", "3", "tomorrow"),
      new RowData("2", "4", "tomorrow"),
      new RowData("3", "1", "tomorrow"),
      new RowData("3", "2", "tomorrow")
    ));

    expectedData.addAll(List.of(
        new RowData("3", "3", "tomorrow"),
        new RowData("3", "4", "tomorrow"),
        new RowData("1", "1", "tomorrow"),
        new RowData("1", "2", "tomorrow"),
        new RowData("1", "3", "tomorrow"),
        new RowData("1", "4", "tomorrow")
    ));

    expectedData.addAll(List.of(
        new RowData("12", "12", "tomorrow"),
        new RowData("13", "13", "tomorrow"),
        new RowData("14", "14", "tomorrow"),
        new RowData("15", "15", "tomorrow"),
        new RowData("16", "16", "tomorrow"),
        new RowData("17", "17", "tomorrow")
    ));

    verifyDataAfterCompaction(expectedData, testDataProvider, "ds=tomorrow", false);
  }

  @Test
  public void testRebalanceCompactionOfNotPartitionedExplicitlyBucketedTable() throws Exception {
    prepareHiveConfForRebalanceCompaction();

    final String tableName = "rebalance_test";
    dropTables(driver, tableName);
    executeStatementOnDriver("CREATE TABLE " + tableName + "(a string, b int) " +
        "CLUSTERED BY(a) INTO 4 BUCKETS STORED AS ORC TBLPROPERTIES('transactional'='true')", driver);
    executeStatementOnDriver(
        "INSERT INTO TABLE " + tableName + " values ('11',11),('22',22),('33',33),('44',44)", driver
    );

    //Try to do a rebalancing compaction
    executeStatementOnDriver("ALTER TABLE " + tableName + " COMPACT 'rebalance'", driver);
    runWorker(conf);

    //Check if the compaction is refused
    List<ShowCompactResponseElement> compacts = verifyCompaction(1, TxnStore.REFUSED_RESPONSE);
    assertEquals(
        "Expecting error message 'Cannot execute rebalancing compaction on bucketed tables.' and found:" +
              compacts.getFirst().getState(),
        "Cannot execute rebalancing compaction on bucketed tables.", compacts.getFirst().getErrorMessage());
  }

  @Test
  public void testRebalanceCompactionNotPartitionedExplicitBucketNumbers() throws Exception {
    prepareHiveConfForRebalanceCompaction();

    //set grouping size to have 3 buckets, and re-create driver with the new config
    conf.set("tez.grouping.min-size", "400");
    conf.set("tez.grouping.max-size", "5000");
    driver = new Driver(conf);

    final String tableName = "rebalance_test";
    TestDataProvider testDataProvider = prepareRebalanceTestData();

    //Try to do a rebalancing compaction
    executeStatementOnDriver("ALTER TABLE " + tableName + " COMPACT 'rebalance' CLUSTERED INTO 4 BUCKETS", driver);
    runWorker(conf);

    verifyCompaction(1,  TxnStore.CLEANING_RESPONSE);

    List<RowData> expectedData = new ArrayList<>();
    expectedData.addAll(List.of(
        new RowData("5", "4"),
        new RowData("6", "2"),
        new RowData("6", "3"),
        new RowData("6", "4"),
        new RowData("5", "2")
    ));

    expectedData.addAll(List.of(
        new RowData("5", "3"),
        new RowData("2", "4"),
        new RowData("3", "3"),
        new RowData("4", "4"),
        new RowData("4", "3")
    ));

    expectedData.addAll(List.of(
        new RowData("2", "3"),
        new RowData("3", "4"),
        new RowData("12", "12"),
        new RowData("13", "13"),
        new RowData("14", "14")
    ));

    expectedData.addAll(List.of(
        new RowData("15", "15"),
        new RowData("16", "16"),
        new RowData("17", "17")
    ));

    verifyDataAfterCompaction(expectedData, testDataProvider, null, false);
  }

  @SuppressWarnings("java:S2925")
  private void testRebalanceCompactionWithParallelDeleteAsSecond(boolean optimisticLock) throws Exception {
    prepareHiveConfForRebalanceCompaction();
    conf.setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, optimisticLock);

    //set grouping size to have 3 buckets, and re-create driver with the new config
    conf.set("tez.grouping.min-size", "400");
    conf.set("tez.grouping.max-size", "5000");
    driver = new Driver(conf);

    final String tableName = "rebalance_test";
    TestDataProvider testDataProvider = prepareRebalanceTestData();

    //Try to do a rebalancing compaction
    executeStatementOnDriver("ALTER TABLE " + tableName + " COMPACT 'rebalance' ORDER BY b DESC", driver);

    CountDownLatch startDelete = new CountDownLatch(1);
    CountDownLatch endDelete = new CountDownLatch(1);
    CompactorFactory factory = Mockito.spy(CompactorFactory.getInstance());
    doAnswer(invocation -> {
      Object result = invocation.callRealMethod();
      startDelete.countDown();
      Thread.sleep(1000);
      return result;
    }).when(factory).getCompactorPipeline(any(), any(), any(), any());

    Worker worker = new Worker(factory);
    worker.setConf(conf);
    worker.init(new AtomicBoolean(true));
    worker.start();

    if (!startDelete.await(10, TimeUnit.SECONDS)) {
      throw new RuntimeException("Waiting for the compaction to start timed out!");
    }

    boolean aborted = false;
    try {
      executeStatementOnDriver("DELETE FROM " + tableName + " WHERE b = 12", driver);
    } catch (CommandProcessorException e) {
      if (optimisticLock) {
        Assert.fail("In case of TXN_WRITE_X_LOCK = true, the transaction must be retried instead of being aborted.");
      }
      aborted = true;
      Assert.assertEquals(12, e.getResponseCode());
      Assert.assertEquals(TXN_ABORTED.getErrorCode(), e.getErrorCode());

      // Delete the record, so the rest of the test can be the same in both cases
      executeStatementOnDriver("DELETE FROM " + tableName + " WHERE b = 12", driver);
    } finally {
      if(!optimisticLock && !aborted) {
        Assert.fail("In case of TXN_WRITE_X_LOCK = false, the transaction must be aborted instead of being retried.");
      }
    }
    endDelete.countDown();

    worker.join();

    driver.close();
    driver = new Driver(conf);

    List<String> result = execSelectAndDumpData("select * from " + tableName + " WHERE b = 12", driver,
        "Dumping data for " + tableName + " after load:");
    assertEquals(0, result.size());

    //Check if the compaction succeed
    verifyCompaction(1, TxnStore.CLEANING_RESPONSE);

    // Populate expected data
    List<RowData> expectedData = new ArrayList<>();

    expectedData.addAll(List.of(
        new RowData("17", "17"),
        new RowData("16", "16"),
        new RowData("15", "15"),
        new RowData("14", "14"),
        new RowData("13", "13")
    ));

    // Adding the '4' group
    expectedData.addAll(List.of(
        new RowData("6", "4"),
        new RowData("3", "4"),
        new RowData("4", "4"),
        new RowData("2", "4"),
        new RowData("5", "4")
    ));

    // Adding the '3' group
    expectedData.addAll(List.of(
        new RowData("2", "3"),
        new RowData("3", "3"),
        new RowData("6", "3"),
        new RowData("4", "3"),
        new RowData("5", "3")
    ));

    // Adding the '2' group
    expectedData.addAll(List.of(
        new RowData("6", "2"),
        new RowData("5", "2")
    ));

    verifyDataAfterCompaction(expectedData, testDataProvider);
  }

  record RowData(String... columns) {
    @Override
    public boolean equals(Object obj) {
      if (obj instanceof RowData(String[] otherColumns)) {
        return Arrays.equals(otherColumns, this.columns);
      }

      return false;
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(columns);
    }
  }

  /**
   * Validate the data after rebalance compaction.
   * - the table is balanced (or if not, only numberOfDeletedRows amount of rows are missing
   * - there is only one writeId
   * - buckets has unique bucketId and the bucketId doesn't change inside a bucket
   * - all the required value present
   * - rowId must be strictly monotonic
   *
   * @param expectedData     Expected row data
   * @param testDataProvider Test data provider
   * @throws Exception Any exception that occurs during the execution
   */
  private void verifyDataAfterCompaction(List<RowData> expectedData, TestDataProvider testDataProvider)
      throws Exception {
    verifyDataAfterCompaction(expectedData, testDataProvider, (String) null, true);
  }
  /**
   * Validate the data after rebalance compaction.
   * - the table is balanced (or if not, only numberOfDeletedRows amount of rows are missing
   * - writeId must be strictly monotonic
   * - buckets has unique bucketId and the bucketId doesn't change inside a bucket
   * - if we expect the output sorted, data is sorted by column b (so the order of column a is not predictable)
   * - all the required value present
   * - rowId must be strictly monotonic
   *
   * @param expectedData      Expected row data
   * @param testDataProvider  Test data provider
   * @param sorted            True if the data must be sorted
   * @throws Exception        Any exception that occurs during the execution
   */
  private void verifyDataAfterCompaction(
      List<RowData> expectedData, TestDataProvider testDataProvider, String partition, boolean sorted
  ) throws Exception {

    FileSystem fs = FileSystem.get(conf);
    GetTableRequest getTableRequest = new GetTableRequest("default", "rebalance_test");
    Table table = msClient.getTable(getTableRequest);
    List<String> bucketFilenames = CompactorTestUtil.getBucketFileNames(
        fs, table, partition, "base_0000001");

    int bucketCount = bucketFilenames.size();
    assertTrue(bucketCount > 0);

    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf);

    int upperBound = (expectedData.size() + bucketCount - 1) / bucketCount;

    long previousValueForColB = Long.MAX_VALUE;
    long previousRowId = Long.MIN_VALUE;

    for (int i = 0; i < bucketCount; i++) {
      List<TestDataProvider.RowInfo> bucket =
          testDataProvider.getStructuredBucketData(
              table.getTableName(), BucketCodec.V1.encode(options.bucket(i)) + ""
          );

      int bucketSize = bucket.size();
      assertTrue(bucketSize <= upperBound);

      long bucketId = -1L;

      long previousWriteId = -1L;

      for (TestDataProvider.RowInfo rowInfo : bucket) {

        // RowId must be strictly monotonic
        assertTrue(
            String.format(
                "RowId must be strictly monotonic rule failed. Previous RowId: %d, Bucket: %s,  ",
                previousRowId, rowInfo),
            rowInfo.rowId() > previousRowId);
        previousRowId = rowInfo.rowId();

        // Check if writeId is strictly monotonic
        if (previousWriteId == -1L) {
          // we are at the first element
          previousWriteId = rowInfo.writeId();
        } else {
          assertTrue(previousWriteId <= rowInfo.writeId());
          previousWriteId = rowInfo.writeId();
        }

        // Check if bucketId doesn't change inside the bucket
        if (bucketId == -1) {
          // we are at the first element of the bucket
          bucketId = rowInfo.bucketId();
        } else {
          assertEquals(bucketId, rowInfo.bucketId());
        }

        // Check if all the necessary data persist
        RowData rowData = rowInfo.rowData();
        assertTrue(expectedData.contains(rowData));
        expectedData.remove(rowData);

        // Check if the data is sorted by colB desc
        if (sorted) {
          long colB = Long.parseLong(rowData.columns()[1]);
          assertTrue(colB <= previousValueForColB);
          previousValueForColB = colB;
        }
      }
    }

    // check if we got all the expected values
    assertEquals(0, expectedData.size()); // we have found all the elements in a proper order
  }

  private TestDataProvider prepareRebalanceTestData() throws Exception {
    final String stageTableName = "stage_" + "rebalance_test";

    TestDataProvider testDataProvider = new TestDataProvider();
    testDataProvider.createFullAcidTable(stageTableName, true, false);
    testDataProvider.insertTestData(stageTableName, true);

    dropTables(driver, "rebalance_test");
    executeStatementOnDriver("CREATE TABLE " + "rebalance_test" + "(a string, b int) " +
        "STORED AS ORC TBLPROPERTIES('transactional'='true')", driver);
    executeStatementOnDriver(
        "INSERT OVERWRITE TABLE " + "rebalance_test" + " select a, b from " + stageTableName, driver);

    //do some single inserts to have more data in the first bucket.
    executeStatementOnDriver("INSERT INTO TABLE " + "rebalance_test" + " values ('12',12)", driver);
    executeStatementOnDriver("INSERT INTO TABLE " + "rebalance_test" + " values ('13',13)", driver);
    executeStatementOnDriver("INSERT INTO TABLE " + "rebalance_test" + " values ('14',14)", driver);
    executeStatementOnDriver("INSERT INTO TABLE " + "rebalance_test" + " values ('15',15)", driver);
    executeStatementOnDriver("INSERT INTO TABLE " + "rebalance_test" + " values ('16',16)", driver);
    executeStatementOnDriver("INSERT INTO TABLE " + "rebalance_test" + " values ('17',17)", driver);

    // Make sure we have all the records persisted
    List<String> allRecords = execSelectAndDumpData(
        "SELECT * FROM " + "rebalance_test", driver, "Dumping data from test table, " + "rebalance_test");
    Assert.assertEquals(18, allRecords.size());

    Assert.assertFalse(isBalanced(testDataProvider, null));

    // Please note, as the test tests rebalance compaction, not insert overwrite, it is not necessary to test if
    // we have the exact same data after preparing the test data as we had at the source table.
    return testDataProvider;
  }

  /**
   * checks if the test data is unbalanced
   * Balanced if all the buckets contain between n / bucket count and n / bucket count + bucket count rows,
   * where n is the number of rows in the table.
   * In our test case, we inserted 6 extra rows into the first bucket so, we can say it is properly unbalanced
   * if the first bucket has 6 more elements than the second one.
   **/
  private boolean isBalanced(TestDataProvider testDataProvider, String partition) throws Exception {
    FileSystem fs = FileSystem.get(conf);
    GetTableRequest getTableRequest = new GetTableRequest("default", "rebalance_test");
    Table table = msClient.getTable(getTableRequest);

    // Assert that we have multiple buckets
    List<String> bucketFilenames = CompactorTestUtil.getBucketFileNames(fs, table, partition, "base_0000001");
    assertTrue(bucketFilenames.size() > 1);

    int bucketCount = bucketFilenames.size();

    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf);
    List<String>[] bucketData = new ArrayList[bucketCount];
    for (int i = 0; i < bucketCount; i++) {
      bucketData[i] = testDataProvider.getBucketData(
          table.getTableName(), BucketCodec.V1.encode(options.bucket(i)) + "");
    }

    int allRecordCount = Arrays.stream(bucketData)
        .map(Collection::size)
        .reduce(0, Integer::sum);

    int lowerBound = allRecordCount / bucketCount;
    int upperBound = (allRecordCount + bucketCount - 1) / bucketCount;

    for (int i = 0; i < bucketCount; i++) {
      if (bucketData[i].size() > upperBound || bucketData[i].size() < lowerBound) {
        return false;
      }
    }

    return true;
  }
}
