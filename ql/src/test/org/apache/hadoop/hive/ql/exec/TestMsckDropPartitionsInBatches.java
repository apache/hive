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
package org.apache.hadoop.hive.ql.exec;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.CheckResult.PartitionResult;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.common.util.RetryUtilities.RetryException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit test for function dropPartitionsInBatches in DDLTask.
 *
 **/
public class TestMsckDropPartitionsInBatches {
  private static HiveConf hiveConf;
  private static DDLTask ddlTask;
  private final String tableName = "test_msck_batch";
  private static Hive db;
  private List<String> repairOutput;
  private Table table;

  @BeforeClass
  public static void setupClass() throws HiveException {
    hiveConf = new HiveConf(TestMsckCreatePartitionsInBatches.class);
    hiveConf.setIntVar(ConfVars.HIVE_MSCK_REPAIR_BATCH_SIZE, 5);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    SessionState.start(hiveConf);
    db = Hive.get(hiveConf);
    ddlTask = new DDLTask();
  }

  @Before
  public void before() throws Exception {
    createPartitionedTable("default", tableName);
    table = db.getTable(tableName);
    repairOutput = new ArrayList<String>();
  }

  @After
  public void after() throws Exception {
    cleanUpTableQuietly("default", tableName);
  }

  private Table createPartitionedTable(String dbName, String tableName) throws Exception {
    try {
      db.dropTable(dbName, tableName);
      db.createTable(tableName, Arrays.asList("key", "value"), // Data columns.
          Arrays.asList("city"), // Partition columns.
          TextInputFormat.class, HiveIgnoreKeyTextOutputFormat.class);
      return db.getTable(dbName, tableName);
    } catch (Exception exception) {
      fail("Unable to drop and create table " + StatsUtils
          .getFullyQualifiedTableName(dbName, tableName) + " because " + StringUtils
          .stringifyException(exception));
      throw exception;
    }
  }

  private void cleanUpTableQuietly(String dbName, String tableName) {
    try {
      db.dropTable(dbName, tableName, true, true, true);
    } catch (Exception exception) {
      fail("Unexpected exception: " + StringUtils.stringifyException(exception));
    }
  }

  private Set<PartitionResult> dropPartsNotInFs(int numOfParts) {
    Set<PartitionResult> partsNotInFs = new HashSet<>();
    for (int i = 0; i < numOfParts; i++) {
      PartitionResult result = new PartitionResult();
      result.setTableName(tableName);
      result.setPartitionName("city=dummyCity_" + String.valueOf(i));
      partsNotInFs.add(result);
    }
    return partsNotInFs;
  }

  // Find most significant bit with starting index as 0
  private int findMSB(int n) {
    int msbIndex = 0;

    Assert.assertTrue("Only positive values expected", n > 0);

    while (n > 1) {
      n = (n >> 1);
      msbIndex++;
    }

    return msbIndex;
  }

  // runDropPartitions is the main function that gets called with different options
  // partCount: total number of partitions that will be deleted
  // batchSize: maximum number of partitions that can be deleted in a batch
  //    based on the above the test will check that the batch sizes are as expected
  // exceptionStatus can take 3 values
  //   noException: no exception is expected.
  //   oneException: first call throws exception.  Since dropPartitionInBatches will retry, this
  //                  will succeed after the first failure
  //   allException: failure case where everything fails.  Will test that the test fails after
  //                  retrying based on maxRetries when specified, or based on a decaying factor
  //                  of 2
  private final int noException = 1;
  private final int oneException = 2;
  private final int allException = 3;
  private void runDropPartitions(int partCount, int batchSize, int maxRetries, int exceptionStatus)
      throws Exception {
    Hive spyDb = Mockito.spy(db);

    // create partCount dummy partitions
    Set<PartitionResult> partsNotInFs = dropPartsNotInFs(partCount);

    // Expected number of dropPartitions call
    int expectedCallCount = maxRetries;

    // Expected number of partitions dropped in each of those calls
    int[] expectedBatchSizes;

    // Last batch can sometimes have less number of elements
    int lastBatchSize = batchSize;

    // Actual Batch size that will be used
    int actualBatchSize = batchSize;

    if (exceptionStatus == oneException) {
      // After one exception everything is expected to run
      actualBatchSize = batchSize/2;
    }

    if (exceptionStatus != allException) {
      expectedCallCount = partCount/actualBatchSize;

      if (expectedCallCount*actualBatchSize < partCount) {
        // partCount not equally divided into batches.  last batch size will be less than batch size
        lastBatchSize = partCount - (expectedCallCount * actualBatchSize);

        // Add 1 to counter default rounding
        expectedCallCount++;
      }

      if (exceptionStatus == oneException) {
        // first one will fail - count it in
        expectedCallCount++;

        // only first call throws exception
        Mockito.doThrow(HiveException.class).doCallRealMethod().doCallRealMethod().when(spyDb)
            .dropPartitions(Mockito.eq(table), Mockito.any(List.class), Mockito.eq(false),
                Mockito.eq(true));
      }

      expectedBatchSizes = new int[expectedCallCount];

      // first batch is always based on batch size
      expectedBatchSizes[0] = Integer.min(partCount, batchSize);

      // second batch to last but one batch will be actualBatchSize
      // actualBatchSize is same as batchSize when no exceptions are expected
      // actualBatchSize is half of batchSize when 1 exception is expected
      for (int i = 1; i < expectedCallCount-1; i++) {
        expectedBatchSizes[i] = Integer.min(partCount, actualBatchSize);
      }

      expectedBatchSizes[expectedCallCount-1] = lastBatchSize;

      // batch size from input and decaying factor of 2
      ddlTask.dropPartitionsInBatches(spyDb, repairOutput, partsNotInFs, table, batchSize, 2,
          maxRetries);
    } else {
      if (maxRetries == 0) {
        // Retries will be done till decaying factor reduces to 0.  Decaying Factor is 2.
        // So, log to base 2 of batchSize plus 1 or Most Significant Bit
        // of batchsize plus 1 will give the number of expected calls
        expectedCallCount = findMSB(batchSize) + 1;
      }
      expectedBatchSizes = new int[expectedCallCount];

      // decaying where the batchSize keeps reducing by half
      for (int i = 0; i < expectedCallCount; i++) {
        actualBatchSize = batchSize / (int) Math.pow(2, i);
        expectedBatchSizes[i] = Integer.min(partCount, actualBatchSize);
      }
      // all calls fail
      Mockito.doThrow(HiveException.class).when(spyDb)
          .dropPartitions(Mockito.eq(table), Mockito.any(List.class), Mockito.eq(false),
              Mockito.eq(true));

      Exception ex = null;
      try {
        ddlTask.dropPartitionsInBatches(spyDb, repairOutput, partsNotInFs, table, batchSize, 2,
            maxRetries);
      } catch (Exception retryEx) {
        ex = retryEx;
      }
      Assert.assertFalse("Exception was expected but was not thrown", ex == null);
      Assert.assertTrue("Unexpected class of exception thrown", ex instanceof RetryException);
    }

    // there should be expectedCallCount calls to drop partitions with each batch size of
    // actualBatchSize
    ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);
    Mockito.verify(spyDb, Mockito.times(expectedCallCount))
        .dropPartitions(Mockito.eq(table), argument.capture(), Mockito.eq(false), Mockito.eq(true));

    // confirm the batch sizes were as expected
    List<List> droppedParts = argument.getAllValues();

    for (int i = 0; i < expectedCallCount; i++) {
      Assert.assertEquals(
          String.format("Unexpected batch size in attempt %d.  Expected: %d.  Found: %d", i + 1,
              expectedBatchSizes[i], droppedParts.get(i).size()),
          expectedBatchSizes[i], droppedParts.get(i).size());
    }
  }

  private void runDropPartitions(int partCount, int batchSize) throws Exception {
    runDropPartitions(partCount, batchSize, 0, noException);
  }

  /**
   * Tests the number of times Hive.dropPartitions calls are executed with total number of
   * partitions to be added are equally divisible by batch size.
   *
   * @throws Exception
   */
  @Test
  public void testNumberOfDropPartitionsCalls() throws Exception {
    runDropPartitions(10, 5);
  }

  /**
   * Tests the number of times Hive.dropPartitions calls are executed with total number of
   * partitions to be added are not exactly divisible by batch size.
   *
   * @throws Exception
   */
  @Test
  public void testUnevenNumberOfCreatePartitionCalls() throws Exception {
    runDropPartitions(14, 5);
  }

  /**
   * Tests the number of times Hive.dropPartitions calls are executed with total number of
   * partitions exactly equal to batch size.
   *
   * @throws Exception
   */
  @Test
  public void testEqualNumberOfPartitions() throws Exception {
    runDropPartitions(13, 13);
  }

  /**
   * Tests the number of times Hive.dropPartitions calls are executed with total number of
   * partitions to is less than batch size.
   *
   * @throws Exception
   */
  @Test
  public void testSmallNumberOfPartitions() throws Exception {
    runDropPartitions(10, 20);
  }

  /**
   * Tests the number of calls to dropPartitions and the respective batch sizes when first call to
   * dropPartitions throws HiveException. The batch size should be reduced once by the
   * decayingFactor 2, iow after batch size is halved.
   *
   * @throws Exception
   */
  @Test
  public void testBatchingWhenException() throws Exception {
    runDropPartitions(23, 30, 0, oneException);
  }

  /**
   * Tests the retries exhausted case when Hive.DropPartitions method call always keep throwing
   * HiveException. The batch sizes should exponentially decreased based on the decaying factor and
   * ultimately give up when it reaches 0.
   *
   * @throws Exception
   */
  @Test
  public void testRetriesExhaustedBatchSize() throws Exception {
    runDropPartitions(17, 30, 0, allException);
  }

  /**
   * Tests the maximum retry attempt is set to 2.
   * @throws Exception
   */
  @Test
  public void testMaxRetriesReached() throws Exception {
    runDropPartitions(17, 30, 2, allException);
  }

  /**
   * Tests when max number of retries is set to 1.
   * @throws Exception
   */
  @Test
  public void testOneMaxRetries() throws Exception {
    runDropPartitions(17, 30, 1, allException);
  }
}
