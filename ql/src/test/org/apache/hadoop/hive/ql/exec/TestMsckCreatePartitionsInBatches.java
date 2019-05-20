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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.CheckResult.PartitionResult;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Msck;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.RetryUtilities;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

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
    msck.init(hiveConf);
  }

  @Before
  public void before() throws Exception {
    createPartitionedTable(catName, dbName, tableName);
    table = db.getTable(catName, dbName, tableName);
    repairOutput = new ArrayList<String>();
  }

  @After
  public void after() throws Exception {
    cleanUpTableQuietly(catName, dbName, tableName);
  }

  private Table createPartitionedTable(String catName, String dbName, String tableName) throws Exception {
    try {
      db.dropTable(catName, dbName, tableName);
      Table table = new Table();
      table.setCatName(catName);
      table.setDbName(dbName);
      table.setTableName(tableName);
      FieldSchema col1 = new FieldSchema("key", "string", "");
      FieldSchema col2 = new FieldSchema("value", "int", "");
      FieldSchema col3 = new FieldSchema("city", "string", "");
      StorageDescriptor sd = new StorageDescriptor();
      sd.setSerdeInfo(new SerDeInfo());
      sd.setInputFormat(TextInputFormat.class.getCanonicalName());
      sd.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
      sd.setCols(Arrays.asList(col1, col2));
      table.setPartitionKeys(Arrays.asList(col3));
      table.setSd(sd);
      db.createTable(table);
      return db.getTable(catName, dbName, tableName);
    } catch (Exception exception) {
      fail("Unable to drop and create table " + StatsUtils.getFullyQualifiedTableName(dbName, tableName) + " because "
          + StringUtils.stringifyException(exception));
      throw exception;
    }
  }

  private void cleanUpTableQuietly(String catName, String dbName, String tableName) {
    try {
      db.dropTable(catName, dbName, tableName);
    } catch (Exception exception) {
      fail("Unexpected exception: " + StringUtils.stringifyException(exception));
    }
  }

  private Set<PartitionResult> createPartsNotInMs(int numOfParts) {
    Set<PartitionResult> partsNotInMs = new HashSet<>();
    for (int i = 0; i < numOfParts; i++) {
      PartitionResult result = new PartitionResult();
      result.setPartitionName("city=dummyCity_" + String.valueOf(i));
      partsNotInMs.add(result);
    }
    return partsNotInMs;
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
    Set<PartitionResult> partsNotInMs = createPartsNotInMs(10);
    IMetaStoreClient spyDb = Mockito.spy(db);
    // batch size of 5 and decaying factor of 2
    msck.createPartitionsInBatches(spyDb, repairOutput, partsNotInMs, table, 5, 2, 0);
    // there should be 2 calls to create partitions with each batch size of 5
    ArgumentCaptor<Boolean> ifNotExistsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<Boolean> needResultsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<List<Partition>> argParts = ArgumentCaptor.forClass((Class) List.class);
    Mockito.verify(spyDb, Mockito.times(2)).add_partitions(argParts.capture(), ifNotExistsArg.capture(), needResultsArg.capture());
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
    Set<PartitionResult> partsNotInMs = createPartsNotInMs(9);
    IMetaStoreClient spyDb = Mockito.spy(db);
    // batch size of 5 and decaying factor of 2
    msck.createPartitionsInBatches(spyDb, repairOutput, partsNotInMs, table, 5, 2, 0);
    // there should be 2 calls to create partitions with batch sizes of 5, 4
    ArgumentCaptor<Boolean> ifNotExistsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<Boolean> needResultsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<List<Partition>> argParts = ArgumentCaptor.forClass((Class) List.class);
    Mockito.verify(spyDb, Mockito.times(2)).add_partitions(argParts.capture(), ifNotExistsArg.capture(), needResultsArg.capture());
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
    Set<PartitionResult> partsNotInMs = createPartsNotInMs(13);
    IMetaStoreClient spyDb = Mockito.spy(db);
    // batch size of 13 and decaying factor of 2
    msck.createPartitionsInBatches(spyDb, repairOutput, partsNotInMs, table, 13, 2, 0);
    // there should be 1 call to create partitions with batch sizes of 13
    ArgumentCaptor<Boolean> ifNotExistsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<Boolean> needResultsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<List<Partition>> argParts = ArgumentCaptor.forClass((Class) List.class);
    // there should be 1 call to create partitions with batch sizes of 13
    Mockito.verify(spyDb, Mockito.times(1)).add_partitions(argParts.capture(), ifNotExistsArg.capture(),
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
    Set<PartitionResult> partsNotInMs = createPartsNotInMs(10);
    IMetaStoreClient spyDb = Mockito.spy(db);
    // batch size of 20 and decaying factor of 2
    msck.createPartitionsInBatches(spyDb, repairOutput, partsNotInMs, table, 20, 2, 0);
    // there should be 1 call to create partitions with batch sizes of 10
    Mockito.verify(spyDb, Mockito.times(1)).add_partitions(Mockito.anyObject(), Mockito.anyBoolean(),
      Mockito.anyBoolean());
    ArgumentCaptor<Boolean> ifNotExistsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<Boolean> needResultsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<List<Partition>> argParts = ArgumentCaptor.forClass((Class) List.class);
    // there should be 1 call to create partitions with batch sizes of 10
    Mockito.verify(spyDb, Mockito.times(1)).add_partitions(argParts.capture(), ifNotExistsArg.capture(),
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
    Set<PartitionResult> partsNotInMs = createPartsNotInMs(23);
    IMetaStoreClient spyDb = Mockito.spy(db);
    // first call to createPartitions should throw exception
    Mockito.doThrow(HiveException.class).doCallRealMethod().doCallRealMethod().when(spyDb)
      .add_partitions(Mockito.anyObject(), Mockito.anyBoolean(),
        Mockito.anyBoolean());

    // test with a batch size of 30 and decaying factor of 2
    msck.createPartitionsInBatches(spyDb, repairOutput, partsNotInMs, table, 30, 2, 0);
    // confirm the batch sizes were 23, 15, 8 in the three calls to create partitions
    ArgumentCaptor<Boolean> ifNotExistsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<Boolean> needResultsArg = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<List<Partition>> argParts = ArgumentCaptor.forClass((Class) List.class);
    // there should be 3 calls to create partitions with batch sizes of 23, 15, 8
    Mockito.verify(spyDb, Mockito.times(3)).add_partitions(argParts.capture(), ifNotExistsArg.capture(),
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
    Set<PartitionResult> partsNotInMs = createPartsNotInMs(17);
    IMetaStoreClient spyDb = Mockito.spy(db);
    Mockito.doThrow(HiveException.class).when(spyDb)
      .add_partitions(Mockito.anyObject(), Mockito.anyBoolean(), Mockito.anyBoolean());
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
    ArgumentCaptor<List<Partition>> argParts = ArgumentCaptor.forClass((Class) List.class);
    // there should be 5 calls to create partitions with batch sizes of 17, 15, 7, 3, 1
    Mockito.verify(spyDb, Mockito.times(5)).add_partitions(argParts.capture(), ifNotExistsArg.capture(),
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
    Set<PartitionResult> partsNotInMs = createPartsNotInMs(17);
    IMetaStoreClient spyDb = Mockito.spy(db);
    Mockito.doThrow(HiveException.class).when(spyDb)
      .add_partitions(Mockito.anyObject(), Mockito.anyBoolean(), Mockito.anyBoolean());
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
    ArgumentCaptor<List<Partition>> argParts = ArgumentCaptor.forClass((Class) List.class);
    Mockito.verify(spyDb, Mockito.times(2)).add_partitions(argParts.capture(), ifNotExistsArg.capture(), needResultsArg.capture());
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
    Set<PartitionResult> partsNotInMs = createPartsNotInMs(17);
    IMetaStoreClient spyDb = Mockito.spy(db);
    Mockito.doThrow(HiveException.class).when(spyDb)
      .add_partitions(Mockito.anyObject(), Mockito.anyBoolean(), Mockito.anyBoolean());
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
    ArgumentCaptor<List<Partition>> argParts = ArgumentCaptor.forClass((Class) List.class);
    // there should be 5 calls to create partitions with batch sizes of 17, 15, 7, 3, 1
    Mockito.verify(spyDb, Mockito.times(1)).add_partitions(argParts.capture(), ifNotExistsArg.capture(),
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
