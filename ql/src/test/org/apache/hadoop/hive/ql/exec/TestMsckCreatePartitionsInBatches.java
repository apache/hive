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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.CheckResult.PartitionResult;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Msck;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.RetryUtilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestMsckCreatePartitionsInBatches {
  private static HiveConf hiveConf;
  private static Msck msck;
  private final String catName = "hive";
  private final String dbName = "default";
  private final String tableName = "test_msck_batch";
  private static IMetaStoreClient db;
  private List<String> repairOutput;
  private Table table;

  @BeforeClass
  public static void setupClass() throws HiveException, MetaException {
    hiveConf = new HiveConf(TestMsckCreatePartitionsInBatches.class);
    hiveConf.setIntVar(ConfVars.HIVE_MSCK_REPAIR_BATCH_SIZE, 5);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    SessionState.start(hiveConf);
    try {
      db = new HiveMetaStoreClient(hiveConf);
    } catch (MetaException e) {
      throw new HiveException(e);
    }
    msck = new Msck( false, false);
    msck.init(Msck.getMsckConf(hiveConf));
  }

  @Before
  public void before() throws Exception {
    PartitionUtil.createPartitionedTable(db, catName, dbName, tableName);
    table = db.getTable(catName, dbName, tableName);
    repairOutput = new ArrayList<String>();
  }

  @After
  public void after() throws Exception {
    PartitionUtil.cleanUpTableQuietly(db, catName, dbName, tableName);
  }

  /**
   * Tests the number of times Hive.createPartitions calls are executed with total number of
   * partitions to be added are equally divisible by batch size
   *
   * @throws Exception
   */
  @Test
  public void testNumberOfCreatePartitionCalls() throws Exception {
    // create 10 dummy partitions
    Set<PartitionResult> partsNotInMs = PartitionUtil.createPartsNotInMs(10);
    IMetaStoreClient spyDb = spy(db);
    // batch size of 5 and decaying factor of 2
    msck.createPartitionsInBatches(spyDb, repairOutput, partsNotInMs, table, 5, 2, 0);
    // there should be 2 calls to create partitions with each batch size of 5
    ArgumentCaptor<Boolean> ifNotExistsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<Boolean> needResultsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<List<Partition>> argParts = ArgumentCaptor.forClass(List.class);
    verify(spyDb, times(2)).add_partitions(argParts.capture(), ifNotExistsArg.capture(), needResultsArg.capture());
    // confirm the batch sizes were 5, 5 in the two calls to create partitions
    List<List<Partition>> apds = argParts.getAllValues();
    int retryAttempt = 1;
    Assert.assertEquals(String.format("Unexpected batch size in retry attempt %d ", retryAttempt++),
        5, apds.get(0).size());
    Assert.assertEquals(String.format("Unexpected batch size in retry attempt %d ", retryAttempt++),
        5, apds.get(1).size());
    assertTrue(ifNotExistsArg.getValue());
    assertFalse(needResultsArg.getValue());
  }

  /**
   * Tests the number of times Hive.createPartitions calls are executed with total number of
   * partitions to be added are not exactly divisible by batch size
   *
   * @throws Exception
   */
  @Test
  public void testUnevenNumberOfCreatePartitionCalls() throws Exception {
    // create 9 dummy partitions
    Set<PartitionResult> partsNotInMs = PartitionUtil.createPartsNotInMs(9);
    IMetaStoreClient spyDb = spy(db);
    // batch size of 5 and decaying factor of 2
    msck.createPartitionsInBatches(spyDb, repairOutput, partsNotInMs, table, 5, 2, 0);
    // there should be 2 calls to create partitions with batch sizes of 5, 4
    ArgumentCaptor<Boolean> ifNotExistsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<Boolean> needResultsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<List<Partition>> argParts = ArgumentCaptor.forClass(List.class);
    verify(spyDb, times(2)).add_partitions(argParts.capture(), ifNotExistsArg.capture(), needResultsArg.capture());
    // confirm the batch sizes were 5, 4 in the two calls to create partitions
    List<List<Partition>> apds = argParts.getAllValues();
    int retryAttempt = 1;
    Assert.assertEquals(String.format("Unexpected batch size in retry attempt %d ", retryAttempt++),
        5, apds.get(0).size());
    Assert.assertEquals(String.format("Unexpected batch size in retry attempt %d ", retryAttempt++),
        4, apds.get(1).size());
    assertTrue(ifNotExistsArg.getValue());
    assertFalse(needResultsArg.getValue());
  }

  /**
   * Tests the number of times Hive.createPartitions calls are executed with total number of
   * partitions exactly equal to batch size
   *
   * @throws Exception
   */
  @Test
  public void testEqualNumberOfPartitions() throws Exception {
    // create 13 dummy partitions
    Set<PartitionResult> partsNotInMs = PartitionUtil.createPartsNotInMs(13);
    IMetaStoreClient spyDb = spy(db);
    // batch size of 13 and decaying factor of 2
    msck.createPartitionsInBatches(spyDb, repairOutput, partsNotInMs, table, 13, 2, 0);
    // there should be 1 call to create partitions with batch sizes of 13
    ArgumentCaptor<Boolean> ifNotExistsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<Boolean> needResultsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<List<Partition>> argParts = ArgumentCaptor.forClass((Class) List.class);
    // there should be 1 call to create partitions with batch sizes of 13
    verify(spyDb, times(1)).add_partitions(argParts.capture(), ifNotExistsArg.capture(),
      needResultsArg.capture());
    Assert.assertEquals("Unexpected number of batch size", 13,
        argParts.getValue().size());
    assertTrue(ifNotExistsArg.getValue());
    assertFalse(needResultsArg.getValue());
  }

  /**
   * Tests the number of times Hive.createPartitions calls are executed with total number of
   * partitions to is less than batch size
   *
   * @throws Exception
   */
  @Test
  public void testSmallNumberOfPartitions() throws Exception {
    // create 10 dummy partitions
    Set<PartitionResult> partsNotInMs = PartitionUtil.createPartsNotInMs(10);
    IMetaStoreClient spyDb = spy(db);
    // batch size of 20 and decaying factor of 2
    msck.createPartitionsInBatches(spyDb, repairOutput, partsNotInMs, table, 20, 2, 0);
    // there should be 1 call to create partitions with batch sizes of 10
    verify(spyDb, times(1)).add_partitions(Mockito.any(), anyBoolean(), anyBoolean());
    ArgumentCaptor<Boolean> ifNotExistsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<Boolean> needResultsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<List<Partition>> argParts = ArgumentCaptor.forClass(List.class);
    // there should be 1 call to create partitions with batch sizes of 10
    verify(spyDb, times(1)).add_partitions(argParts.capture(), ifNotExistsArg.capture(),
      needResultsArg.capture());
    Assert.assertEquals("Unexpected number of batch size", 10,
        argParts.getValue().size());
    assertTrue(ifNotExistsArg.getValue());
    assertFalse(needResultsArg.getValue());
  }

  /**
   * Tests the number of calls to createPartitions and the respective batch sizes when first call to
   * createPartitions throws HiveException. The batch size should be reduced by the decayingFactor
   *
   * @throws Exception
   */
  @Test
  public void testBatchingWhenException() throws Exception {
    // create 13 dummy partitions
    Set<PartitionResult> partsNotInMs = PartitionUtil.createPartsNotInMs(23);
    IMetaStoreClient spyDb = spy(db);
    // first call to createPartitions should throw exception
    doThrow(MetaException.class).doCallRealMethod().doCallRealMethod().when(spyDb)
      .add_partitions(any(), anyBoolean(), anyBoolean());

    // test with a batch size of 30 and decaying factor of 2
    msck.createPartitionsInBatches(spyDb, repairOutput, partsNotInMs, table, 30, 2, 0);
    // confirm the batch sizes were 23, 15, 8 in the three calls to create partitions
    ArgumentCaptor<Boolean> ifNotExistsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<Boolean> needResultsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<List<Partition>> argParts = ArgumentCaptor.forClass(List.class);
    // there should be 3 calls to create partitions with batch sizes of 23, 15, 8
    verify(spyDb, times(3)).add_partitions(argParts.capture(), ifNotExistsArg.capture(),
      needResultsArg.capture());
    List<List<Partition>> apds = argParts.getAllValues();
    int retryAttempt = 1;
    Assert.assertEquals(
        String.format("Unexpected batch size in retry attempt %d ", retryAttempt++), 23,
        apds.get(0).size());
    Assert.assertEquals(
        String.format("Unexpected batch size in retry attempt %d ", retryAttempt++), 15,
        apds.get(1).size());
    Assert.assertEquals(
        String.format("Unexpected batch size in retry attempt %d ", retryAttempt++), 8,
        apds.get(2).size());
    assertTrue(ifNotExistsArg.getValue());
    assertFalse(needResultsArg.getValue());
  }

  /**
   * Tests the retries exhausted case when Hive.createPartitions method call always keep throwing
   * HiveException. The batch sizes should exponentially decreased based on the decaying factor and
   * ultimately give up when it reaches 0
   *
   * @throws Exception
   */
  @Test
  public void testRetriesExhaustedBatchSize() throws Exception {
    Set<PartitionResult> partsNotInMs = PartitionUtil.createPartsNotInMs(17);
    IMetaStoreClient spyDb = spy(db);
    doThrow(MetaException.class).when(spyDb)
      .add_partitions(any(), anyBoolean(), anyBoolean());
    // batch size of 5 and decaying factor of 2
    Exception ex = null;
    try {
      msck.createPartitionsInBatches(spyDb, repairOutput, partsNotInMs, table, 30, 2, 0);
    } catch (Exception retryEx) {
      ex = retryEx;
    }
    assertFalse("Exception was expected but was not thrown", ex == null);
    Assert.assertTrue("Unexpected class of exception thrown", ex instanceof RetryUtilities.RetryException);
    // there should be 5 calls to create partitions with batch sizes of 17, 15, 7, 3, 1
    ArgumentCaptor<Boolean> ifNotExistsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<Boolean> needResultsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<List<Partition>> argParts = ArgumentCaptor.forClass(List.class);
    // there should be 5 calls to create partitions with batch sizes of 17, 15, 7, 3, 1
    verify(spyDb, times(5)).add_partitions(argParts.capture(), ifNotExistsArg.capture(),
      needResultsArg.capture());
    List<List<Partition>> apds = argParts.getAllValues();
    int retryAttempt = 1;
    Assert.assertEquals(
        String.format("Unexpected batch size in retry attempt %d ", retryAttempt++), 17,
        apds.get(0).size());
    Assert.assertEquals(
        String.format("Unexpected batch size in retry attempt %d ", retryAttempt++), 15,
        apds.get(1).size());
    Assert.assertEquals(
        String.format("Unexpected batch size in retry attempt %d ", retryAttempt++), 7,
        apds.get(2).size());
    Assert.assertEquals(
        String.format("Unexpected batch size in retry attempt %d ", retryAttempt++), 3,
        apds.get(3).size());
    Assert.assertEquals(
        String.format("Unexpected batch size in retry attempt %d ", retryAttempt++), 1,
        apds.get(4).size());
    assertTrue(ifNotExistsArg.getValue());
    assertFalse(needResultsArg.getValue());
  }

  /**
   * Tests the maximum retry attempts provided by configuration
   * @throws Exception
   */
  @Test
  public void testMaxRetriesReached() throws Exception {
    Set<PartitionResult> partsNotInMs = PartitionUtil.createPartsNotInMs(17);
    IMetaStoreClient spyDb = spy(db);
    doThrow(MetaException.class).when(spyDb)
      .add_partitions(any(), anyBoolean(), anyBoolean());
    // batch size of 5 and decaying factor of 2
    Exception ex = null;
    try {
      msck.createPartitionsInBatches(spyDb, repairOutput, partsNotInMs, table, 30, 2, 2);
    } catch (Exception retryEx) {
      ex = retryEx;
    }
    assertFalse("Exception was expected but was not thrown", ex == null);
    Assert.assertTrue("Unexpected class of exception thrown", ex instanceof RetryUtilities.RetryException);
    ArgumentCaptor<Boolean> ifNotExistsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<Boolean> needResultsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<List<Partition>> argParts = ArgumentCaptor.forClass(List.class);
    verify(spyDb, times(2)).add_partitions(argParts.capture(), ifNotExistsArg.capture(), needResultsArg.capture());
    List<List<Partition>> apds = argParts.getAllValues();
    int retryAttempt = 1;
    Assert.assertEquals(
        String.format("Unexpected batch size in retry attempt %d ", retryAttempt++), 17,
        apds.get(0).size());
    Assert.assertEquals(
        String.format("Unexpected batch size in retry attempt %d ", retryAttempt++), 15,
        apds.get(1).size());
    assertTrue(ifNotExistsArg.getValue());
    assertFalse(needResultsArg.getValue());
  }

  /**
   * Tests when max number of retries is set to 1. In this case the number of retries should
   * be specified
   * @throws Exception
   */
  @Test
  public void testOneMaxRetries() throws Exception {
    Set<PartitionResult> partsNotInMs = PartitionUtil.createPartsNotInMs(17);
    IMetaStoreClient spyDb = spy(db);
    doThrow(MetaException.class).when(spyDb)
      .add_partitions(any(), anyBoolean(), anyBoolean());
    // batch size of 5 and decaying factor of 2
    Exception ex = null;
    try {
      msck.createPartitionsInBatches(spyDb, repairOutput, partsNotInMs, table, 30, 2, 1);
    } catch (Exception retryEx) {
      ex = retryEx;
    }
    assertFalse("Exception was expected but was not thrown", ex == null);
    Assert.assertTrue("Unexpected class of exception thrown", ex instanceof RetryUtilities.RetryException);
    // there should be 5 calls to create partitions with batch sizes of 17, 15, 7, 3, 1
    ArgumentCaptor<Boolean> ifNotExistsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<Boolean> needResultsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<List<Partition>> argParts = ArgumentCaptor.forClass(List.class);
    // there should be 5 calls to create partitions with batch sizes of 17, 15, 7, 3, 1
    verify(spyDb, times(1)).add_partitions(argParts.capture(), ifNotExistsArg.capture(),
      needResultsArg.capture());
    List<List<Partition>> apds = argParts.getAllValues();
    int retryAttempt = 1;
    Assert.assertEquals(
        String.format("Unexpected batch size in retry attempt %d ", retryAttempt++), 17,
        apds.get(0).size());
    assertTrue(ifNotExistsArg.getValue());
    assertFalse(needResultsArg.getValue());
  }
}
