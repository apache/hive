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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
    Set<RowData> expectedData = new HashSet<>();

    expectedData.addAll(List.of(
        new RowData("17", 17L),
        new RowData("16", 16L),
        new RowData("15", 15L),
        new RowData("14", 14L),
        new RowData("13", 13L),
        new RowData("12", 12L)
    ));

    // Adding the '4' group
    expectedData.addAll(List.of(
        new RowData("6", 4L),
        new RowData("3", 4L),
        new RowData("4", 4L),
        new RowData("2", 4L),
        new RowData("5", 4L)
    ));

    // Adding the '3' group
    expectedData.addAll(List.of(
        new RowData("2", 3L),
        new RowData("3", 3L),
        new RowData("6", 3L),
        new RowData("4", 3L),
        new RowData("5", 3L)
    ));

    // Adding the '2' group
    expectedData.addAll(List.of(
        new RowData("6", 2L),
        new RowData("5", 2L)
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

    String[][] expectedBuckets = new String[][] {
        {
            "{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t5\t4",
            "{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t6\t2",
            "{\"writeid\":1,\"bucketid\":536870912,\"rowid\":2}\t6\t3",
            "{\"writeid\":1,\"bucketid\":536870912,\"rowid\":3}\t6\t4",
            "{\"writeid\":1,\"bucketid\":536870912,\"rowid\":4}\t5\t2",
        },
        {

            "{\"writeid\":1,\"bucketid\":536936448,\"rowid\":5}\t5\t3",
            "{\"writeid\":1,\"bucketid\":536936448,\"rowid\":6}\t2\t4",
            "{\"writeid\":1,\"bucketid\":536936448,\"rowid\":7}\t3\t3",
            "{\"writeid\":1,\"bucketid\":536936448,\"rowid\":8}\t4\t4",
            "{\"writeid\":1,\"bucketid\":536936448,\"rowid\":9}\t4\t3",
        },
        {
            "{\"writeid\":1,\"bucketid\":537001984,\"rowid\":10}\t2\t3",
            "{\"writeid\":1,\"bucketid\":537001984,\"rowid\":11}\t3\t4",
            "{\"writeid\":2,\"bucketid\":537001984,\"rowid\":12}\t12\t12",
            "{\"writeid\":3,\"bucketid\":537001984,\"rowid\":13}\t13\t13",
            "{\"writeid\":4,\"bucketid\":537001984,\"rowid\":14}\t14\t14",
        },
        {
            "{\"writeid\":5,\"bucketid\":537067520,\"rowid\":15}\t15\t15",
            "{\"writeid\":6,\"bucketid\":537067520,\"rowid\":16}\t16\t16",
            "{\"writeid\":7,\"bucketid\":537067520,\"rowid\":17}\t17\t17",
        },
    };
    verifyRebalance(testDataProvider, null, expectedBuckets,
        new String[] {"bucket_00000", "bucket_00001", "bucket_00002","bucket_00003"});
  }

  @Test
  public void testRebalanceCompactionOfPartitionedImplicitlyBucketedTable() throws Exception {
    prepareHiveConfForRebalanceCompaction();

    //set grouping size to have 3 buckets, and re-create driver with the new config
    conf.set("tez.grouping.min-size", "1");
    driver = new Driver(conf);

    final String stageTableName = "stage_rebalance_test";
    final String tableName = "rebalance_test";
    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf);

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
    GetTableRequest getTableRequest = new GetTableRequest("default", tableName);
    Table table = msClient.getTable(getTableRequest);
    FileSystem fs = FileSystem.get(conf);
    assertEquals("Test setup does not match the expected: different buckets",
        Arrays.asList("bucket_00000_0", "bucket_00001_0", "bucket_00002_0"),
        CompactorTestUtil.getBucketFileNames(fs, table, "ds=tomorrow", "base_0000001"));
    String[][] expectedBuckets = new String[][] {
        {
            "{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t2\t1\ttomorrow",
            "{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t2\t2\ttomorrow",
            "{\"writeid\":1,\"bucketid\":536870912,\"rowid\":2}\t2\t3\ttomorrow",
            "{\"writeid\":1,\"bucketid\":536870912,\"rowid\":3}\t2\t4\ttomorrow",
            "{\"writeid\":2,\"bucketid\":536870912,\"rowid\":0}\t12\t12\ttomorrow",
            "{\"writeid\":3,\"bucketid\":536870912,\"rowid\":0}\t13\t13\ttomorrow",
            "{\"writeid\":4,\"bucketid\":536870912,\"rowid\":0}\t14\t14\ttomorrow",
            "{\"writeid\":5,\"bucketid\":536870912,\"rowid\":0}\t15\t15\ttomorrow",
            "{\"writeid\":6,\"bucketid\":536870912,\"rowid\":0}\t16\t16\ttomorrow",
            "{\"writeid\":7,\"bucketid\":536870912,\"rowid\":0}\t17\t17\ttomorrow",
        },
        {
            "{\"writeid\":1,\"bucketid\":536936448,\"rowid\":0}\t3\t1\ttomorrow",
            "{\"writeid\":1,\"bucketid\":536936448,\"rowid\":1}\t3\t2\ttomorrow",
            "{\"writeid\":1,\"bucketid\":536936448,\"rowid\":2}\t3\t3\ttomorrow",
            "{\"writeid\":1,\"bucketid\":536936448,\"rowid\":3}\t3\t4\ttomorrow",
        },
        {
            "{\"writeid\":1,\"bucketid\":537001984,\"rowid\":0}\t1\t1\ttomorrow",
            "{\"writeid\":1,\"bucketid\":537001984,\"rowid\":1}\t1\t2\ttomorrow",
            "{\"writeid\":1,\"bucketid\":537001984,\"rowid\":2}\t1\t3\ttomorrow",
            "{\"writeid\":1,\"bucketid\":537001984,\"rowid\":3}\t1\t4\ttomorrow",
        },
    };
    for(int i = 0; i < 3; i++) {
      assertEquals("rebalanced bucket " + i, Arrays.asList(expectedBuckets[i]),
          testDataProvider.getBucketData(tableName, BucketCodec.V1.encode(options.bucket(i)) + ""));
    }

    //Try to do a rebalancing compaction
    executeStatementOnDriver("ALTER TABLE " + tableName + " PARTITION (ds='tomorrow') COMPACT 'rebalance'", driver);
    runWorker(conf);

    //Check if the compaction succeed
    verifyCompaction(1, TxnStore.CLEANING_RESPONSE);

    expectedBuckets = new String[][] {
        {
            "{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t2\t1\ttomorrow",
            "{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t2\t2\ttomorrow",
            "{\"writeid\":1,\"bucketid\":536870912,\"rowid\":2}\t2\t3\ttomorrow",
            "{\"writeid\":1,\"bucketid\":536870912,\"rowid\":3}\t2\t4\ttomorrow",
            "{\"writeid\":1,\"bucketid\":536870912,\"rowid\":4}\t3\t1\ttomorrow",
            "{\"writeid\":1,\"bucketid\":536870912,\"rowid\":5}\t3\t2\ttomorrow",
        },
        {
            "{\"writeid\":1,\"bucketid\":536936448,\"rowid\":6}\t3\t3\ttomorrow",
            "{\"writeid\":1,\"bucketid\":536936448,\"rowid\":7}\t3\t4\ttomorrow",
            "{\"writeid\":1,\"bucketid\":536936448,\"rowid\":8}\t1\t1\ttomorrow",
            "{\"writeid\":1,\"bucketid\":536936448,\"rowid\":9}\t1\t2\ttomorrow",
            "{\"writeid\":1,\"bucketid\":536936448,\"rowid\":10}\t1\t3\ttomorrow",
            "{\"writeid\":1,\"bucketid\":536936448,\"rowid\":11}\t1\t4\ttomorrow",
        },
        {
            "{\"writeid\":2,\"bucketid\":537001984,\"rowid\":12}\t12\t12\ttomorrow",
            "{\"writeid\":3,\"bucketid\":537001984,\"rowid\":13}\t13\t13\ttomorrow",
            "{\"writeid\":4,\"bucketid\":537001984,\"rowid\":14}\t14\t14\ttomorrow",
            "{\"writeid\":5,\"bucketid\":537001984,\"rowid\":15}\t15\t15\ttomorrow",
            "{\"writeid\":6,\"bucketid\":537001984,\"rowid\":16}\t16\t16\ttomorrow",
            "{\"writeid\":7,\"bucketid\":537001984,\"rowid\":17}\t17\t17\ttomorrow",
        },
    };
    verifyRebalance(testDataProvider, "ds=tomorrow", expectedBuckets,
        new String[] {"bucket_00000", "bucket_00001", "bucket_00002"});
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

    String[][] expectedBuckets = new String[][] {
        {
            "{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t5\t4",
            "{\"writeid\":1,\"bucketid\":536870912,\"rowid\":1}\t6\t2",
            "{\"writeid\":1,\"bucketid\":536870912,\"rowid\":2}\t6\t3",
            "{\"writeid\":1,\"bucketid\":536870912,\"rowid\":3}\t6\t4",
            "{\"writeid\":1,\"bucketid\":536870912,\"rowid\":4}\t5\t2",
        },
        {
            "{\"writeid\":1,\"bucketid\":536936448,\"rowid\":5}\t5\t3",
            "{\"writeid\":1,\"bucketid\":536936448,\"rowid\":6}\t2\t4",
            "{\"writeid\":1,\"bucketid\":536936448,\"rowid\":7}\t3\t3",
            "{\"writeid\":1,\"bucketid\":536936448,\"rowid\":8}\t4\t4",
            "{\"writeid\":1,\"bucketid\":536936448,\"rowid\":9}\t4\t3",
        },
        {
            "{\"writeid\":1,\"bucketid\":537001984,\"rowid\":10}\t2\t3",
            "{\"writeid\":1,\"bucketid\":537001984,\"rowid\":11}\t3\t4",
            "{\"writeid\":2,\"bucketid\":537001984,\"rowid\":12}\t12\t12",
            "{\"writeid\":3,\"bucketid\":537001984,\"rowid\":13}\t13\t13",
            "{\"writeid\":4,\"bucketid\":537001984,\"rowid\":14}\t14\t14",
        },
        {
            "{\"writeid\":5,\"bucketid\":537067520,\"rowid\":15}\t15\t15",
            "{\"writeid\":6,\"bucketid\":537067520,\"rowid\":16}\t16\t16",
            "{\"writeid\":7,\"bucketid\":537067520,\"rowid\":17}\t17\t17",
        },
    };
    verifyRebalance(testDataProvider, null, expectedBuckets,
        new String[] {"bucket_00000", "bucket_00001", "bucket_00002", "bucket_00003"});
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
    Set<RowData> expectedData = new HashSet<>();

    expectedData.addAll(List.of(
        new RowData("17", 17L),
        new RowData("16", 16L),
        new RowData("15", 15L),
        new RowData("14", 14L),
        new RowData("13", 13L)
    ));

    // Adding the '4' group
    expectedData.addAll(List.of(
        new RowData("6", 4L),
        new RowData("3", 4L),
        new RowData("4", 4L),
        new RowData("2", 4L),
        new RowData("5", 4L)
    ));

    // Adding the '3' group
    expectedData.addAll(List.of(
        new RowData("2", 3L),
        new RowData("3", 3L),
        new RowData("6", 3L),
        new RowData("4", 3L),
        new RowData("5", 3L)
    ));

    // Adding the '2' group
    expectedData.addAll(List.of(
        new RowData("6", 2L),
        new RowData("5", 2L)
    ));

    verifyDataAfterCompaction(expectedData, testDataProvider);
  }

  record RowData(String colA, Long colB) {}

  /**
   * Validate the data after rebalance compaction
   * - the table is balanced (or if not, only numberOfDeletedRows amount of rows are missing
   * - there is only one writeId
   * - buckets has unique bucketId and the bucketId doesn't change inside a bucket
   * - data is sorted by column b (so the order of column a is not predictable)
   * - all the required value present
   * - rowId must be strictly monotonic
   *
   * @param expectedData      Expected row data
   * @param testDataProvider  Test data provider
   * @throws Exception        Any exception that occurs during the execution
   */
  private void verifyDataAfterCompaction(Set<RowData> expectedData, TestDataProvider testDataProvider)
      throws Exception {
    FileSystem fs = FileSystem.get(conf);
    GetTableRequest getTableRequest = new GetTableRequest("default", "rebalance_test");
    Table table = msClient.getTable(getTableRequest);
    List<String> bucketFilenames = CompactorTestUtil.getBucketFileNames(
        fs, table, null, "base_0000001");

    int bucketCount = bucketFilenames.size();
    assertTrue(bucketCount > 0);

    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf);

    int upperBound = (expectedData.size() + bucketCount - 1) / bucketCount;

    long previousValueForColB = Long.MAX_VALUE;
    long previousRowId = Long.MIN_VALUE;

    for (int i = 0; i < bucketCount; i++) {
      List<TestDataProvider.RowInfo> bucket = testDataProvider.getStructuredBucketData(table.getTableName(), BucketCodec.V1.encode(options.bucket(i)) + "");

      int bucketSize = bucket.size();
      assertTrue(bucketSize <= upperBound);

      long bucketId = -1L;
      long writeId = -1L;

      for (TestDataProvider.RowInfo rowInfo : bucket) {

        // RowId must be strictly monotonic
        assertTrue(
          String.format("RowId must be strictly monotonic rule failed. Previous RowId: %d, Bucket: %s,  ", previousRowId, rowInfo),
          rowInfo.rowId() > previousRowId);
        previousRowId = rowInfo.rowId();

        // Check if writeId doesn't change
        if (writeId == -1L) {
          // we are at the first element
          writeId = rowInfo.writeId();
        } else {
          assertEquals(writeId, rowInfo.writeId());
        }

        // Check if bucketId doesn't change inside the bucket
        if (bucketId == -1) {
          // we are at the first element of the bucket
          bucketId = rowInfo.bucketId();
        } else {
          assertEquals(bucketId, rowInfo.bucketId());
        }

        // Check if the data is sorted by colB desc
        RowData rowData = rowInfo.rowData();
        assertTrue(rowData.colB <= previousValueForColB);
        previousValueForColB = rowData.colB;

        // Check if all the necessary data persist
        assertTrue(expectedData.contains(rowData));
        expectedData.remove(rowData);
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
    executeStatementOnDriver("INSERT OVERWRITE TABLE " + "rebalance_test" + " select a, b from " + stageTableName, driver);

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

    /*
     check if the test data is unbalanced
     Balanced if all the buckets contain between n / bucket count and n / bucket count + bucket count rows,
     where n is the number of rows in the table.
     In our test case, we inserted 6 extra rows into the first bucket so, we can say it is properly unbalanced
     if the first bucket has 6 more elements than the second one.
    */
    Assert.assertFalse(isBalanced(testDataProvider));

    // Please note, as the test tests rebalance compaction, not insert overwrite, it is not necessary to test if
    // we have the exact same data after preparing the test data as we had at the source table.
    return testDataProvider;
  }

  private boolean isBalanced(TestDataProvider testDataProvider) throws Exception {
    FileSystem fs = FileSystem.get(conf);
    GetTableRequest getTableRequest = new GetTableRequest("default", "rebalance_test");
    Table table = msClient.getTable(getTableRequest);

    // Assert that we have multiple buckets
    List<String> bucketFilenames = CompactorTestUtil.getBucketFileNames(fs, table, null, "base_0000001");
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

  private void verifyRebalance(TestDataProvider testDataProvider, String partitionName,
      String[][] expectedBucketContent, String[] bucketNames) throws Exception {
    // Verify buckets and their content after rebalance
    GetTableRequest getTableRequest = new GetTableRequest("default", "rebalance_test");
    Table table = msClient.getTable(getTableRequest);
    FileSystem fs = FileSystem.get(conf);
    assertEquals("Buckets does not match after compaction", Arrays.asList(bucketNames),
        CompactorTestUtil.getBucketFileNames(fs, table, partitionName, findTheBaseFolder(table, partitionName, fs)));
    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf);
    for (int i = 0; i < expectedBucketContent.length; i++) {
      assertEquals("rebalanced bucket " + i, Arrays.asList(expectedBucketContent[i]),
          testDataProvider.getBucketData("rebalance_test", BucketCodec.V1.encode(options.bucket(i)) + ""));
    }
  }

  private String findTheBaseFolder(Table table, String partitionName, FileSystem fs) throws IOException {
    Path searchPath = partitionName == null ? new Path(table.getSd().getLocation(), "base_*_v*") : new Path(
        new Path(table.getSd().getLocation()), new Path(partitionName,  "base_*_v*"));

    return Arrays.stream(
        fs.globStatus(searchPath, AcidUtils.baseFileFilter))
        .map(FileStatus::getPath)
        .map(Path::getName)
        .sorted()
        .findFirst().orElse(null);
  }
}
