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
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for {@link org.apache.hadoop.hive.metastore.HiveMetaStoreClient}.  For now this just has
 * transaction and locking tests.  The goal here is not to test all
 * functionality possible through the interface, as all permutations of DB
 * operations should be tested in the appropriate DB handler classes.  The
 * goal is to test that we can properly pass the messages through the thrift
 * service.
 *
 * This is in the ql directory rather than the metastore directory because it
 * required the hive-exec jar, and hive-exec jar already depends on
 * hive-metastore jar, thus I can't make hive-metastore depend on hive-exec.
 */
@Category(MetastoreUnitTest.class)
public class TestHiveMetaStoreTxns {

  private final Configuration conf = MetastoreConf.newMetastoreConf();
  private IMetaStoreClient client;
  private Connection conn;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testTxns() throws Exception {
    List<Long> tids = client.openTxns("me", 3).getTxn_ids();
    Assert.assertEquals(1L, (long) tids.get(0));
    Assert.assertEquals(2L, (long) tids.get(1));
    Assert.assertEquals(3L, (long) tids.get(2));
    client.rollbackTxn(1);
    client.commitTxn(2);
    ValidTxnList validTxns = client.getValidTxns();
    Assert.assertFalse(validTxns.isTxnValid(1));
    Assert.assertTrue(validTxns.isTxnValid(2));
    Assert.assertFalse(validTxns.isTxnValid(3));
    Assert.assertFalse(validTxns.isTxnValid(4));
  }

  @Test
  public void testOpenTxnNotExcluded() throws Exception {
    List<Long> tids = client.openTxns("me", 3).getTxn_ids();
    Assert.assertEquals(1L, (long) tids.get(0));
    Assert.assertEquals(2L, (long) tids.get(1));
    Assert.assertEquals(3L, (long) tids.get(2));
    client.rollbackTxn(1);
    client.commitTxn(2);
    ValidTxnList validTxns = client.getValidTxns(3);
    Assert.assertFalse(validTxns.isTxnValid(1));
    Assert.assertTrue(validTxns.isTxnValid(2));
    Assert.assertTrue(validTxns.isTxnValid(3));
    Assert.assertFalse(validTxns.isTxnValid(4));
  }

  @Test
  public void testOpenReadOnlyTxnExcluded() throws Exception {
    client.openTxn("me", TxnType.READ_ONLY);
    client.openTxns("me", 3);
    client.rollbackTxn(2);
    client.commitTxn(3);
    ValidTxnList validTxns = client.getValidTxns(4);
    Assert.assertTrue(validTxns.isTxnValid(1));
    Assert.assertFalse(validTxns.isTxnValid(2));
    Assert.assertTrue(validTxns.isTxnValid(3));
    Assert.assertTrue(validTxns.isTxnValid(4));
  }

  @Test
  public void testLocks() throws Exception {
    LockRequestBuilder rqstBuilder = new LockRequestBuilder();
    rqstBuilder.addLockComponent(new LockComponentBuilder()
        .setDbName("mydb")
        .setTableName("mytable")
        .setPartitionName("mypartition")
        .setExclusive()
        .setOperationType(DataOperationType.NO_TXN)
        .build());
    rqstBuilder.addLockComponent(new LockComponentBuilder()
        .setDbName("mydb")
        .setTableName("yourtable")
        .setSemiShared()
        .setOperationType(DataOperationType.NO_TXN)
        .build());
    rqstBuilder.addLockComponent(new LockComponentBuilder()
        .setDbName("yourdb")
        .setOperationType(DataOperationType.NO_TXN)
        .setShared()
        .build());
    rqstBuilder.setUser("fred");

    LockResponse res = client.lock(rqstBuilder.build());
    Assert.assertEquals(1L, res.getLockid());
    Assert.assertEquals(LockState.ACQUIRED, res.getState());

    res = client.checkLock(1);
    Assert.assertEquals(1L, res.getLockid());
    Assert.assertEquals(LockState.ACQUIRED, res.getState());

    client.heartbeat(0, 1);

    client.unlock(1);
  }

  @Test
  public void testLocksWithTxn() throws Exception {
    long txnid = client.openTxn("me");

    LockRequestBuilder rqstBuilder = new LockRequestBuilder();
    rqstBuilder.setTransactionId(txnid)
      .addLockComponent(new LockComponentBuilder()
        .setDbName("mydb")
        .setTableName("mytable")
        .setPartitionName("mypartition")
        .setSemiShared()
        .setOperationType(DataOperationType.UPDATE)
        .build())
      .addLockComponent(new LockComponentBuilder()
        .setDbName("mydb")
        .setTableName("yourtable")
        .setSemiShared()
        .setOperationType(DataOperationType.UPDATE)
        .build())
      .addLockComponent(new LockComponentBuilder()
        .setDbName("yourdb")
        .setShared()
        .setOperationType(DataOperationType.SELECT)
        .build())
      .setUser("fred");

    LockResponse res = client.lock(rqstBuilder.build());
    Assert.assertEquals(1L, res.getLockid());
    Assert.assertEquals(LockState.ACQUIRED, res.getState());

    res = client.checkLock(1);
    Assert.assertEquals(1L, res.getLockid());
    Assert.assertEquals(LockState.ACQUIRED, res.getState());

    client.heartbeat(txnid, 1);

    client.commitTxn(txnid);
  }

  @Test
  public void stringifyValidTxns() throws Exception {
    // Test with just high water mark
    ValidTxnList validTxns = new ValidReadTxnList("1:" + Long.MAX_VALUE + "::");
    String asString = validTxns.toString();
    Assert.assertEquals("1:" + Long.MAX_VALUE + "::", asString);
    validTxns = new ValidReadTxnList(asString);
    Assert.assertEquals(1, validTxns.getHighWatermark());
    Assert.assertNotNull(validTxns.getInvalidTransactions());
    Assert.assertEquals(0, validTxns.getInvalidTransactions().length);
    asString = validTxns.toString();
    Assert.assertEquals("1:" + Long.MAX_VALUE + "::", asString);
    validTxns = new ValidReadTxnList(asString);
    Assert.assertEquals(1, validTxns.getHighWatermark());
    Assert.assertNotNull(validTxns.getInvalidTransactions());
    Assert.assertEquals(0, validTxns.getInvalidTransactions().length);

    // Test with open transactions
    validTxns = new ValidReadTxnList("10:3:5:3");
    asString = validTxns.toString();
    if (!asString.equals("10:3:3:5") && !asString.equals("10:3:5:3")) {
      Assert.fail("Unexpected string value " + asString);
    }
    validTxns = new ValidReadTxnList(asString);
    Assert.assertEquals(10, validTxns.getHighWatermark());
    Assert.assertNotNull(validTxns.getInvalidTransactions());
    Assert.assertEquals(2, validTxns.getInvalidTransactions().length);
    boolean sawThree = false, sawFive = false;
    for (long tid : validTxns.getInvalidTransactions()) {
      if (tid == 3)  sawThree = true;
      else if (tid == 5) sawFive = true;
      else  Assert.fail("Unexpected value " + tid);
    }
    Assert.assertTrue(sawThree);
    Assert.assertTrue(sawFive);
  }

  @Test
  public void testOpenTxnWithType() throws Exception {
    long txnId = client.openTxn("me", TxnType.DEFAULT);
    client.commitTxn(txnId);
    ValidTxnList validTxns = client.getValidTxns();
    Assert.assertTrue(validTxns.isTxnValid(txnId));
  }

  @Test
  public void testTxnTypePersisted() throws Exception {
    long txnId = client.openTxn("me", TxnType.READ_ONLY);
    Statement stm = conn.createStatement();
    ResultSet rs = stm.executeQuery("SELECT txn_type FROM txns WHERE txn_id = " + txnId);
    Assert.assertTrue(rs.next());
    Assert.assertEquals(TxnType.findByValue(rs.getInt(1)), TxnType.READ_ONLY);
  }

  @Test
  public void testAllocateTableWriteIdForReadOnlyTxn() throws Exception {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Write ID allocation failed on db.tbl as not all input txns in open state or read-only");

    long txnId = client.openTxn("me", TxnType.READ_ONLY);
    client.allocateTableWriteId(txnId, "db", "tbl");
  }

  @Test
  public void testGetValidWriteIds() throws TException {
    List<Long> tids = client.openTxns("me", 3).getTxn_ids();
    client.allocateTableWriteIdsBatch(tids, "db", "tbl");
    client.rollbackTxn(tids.get(0));

    ValidTxnList validTxnList = client.getValidTxns();
    String fullTableName = TxnUtils.getFullTableName("db", "tbl");

    List<TableValidWriteIds> tableValidWriteIds = client.getValidWriteIds(
        Collections.singletonList(fullTableName), validTxnList.writeToString());

    Assert.assertEquals(tableValidWriteIds.size(), 1);
    TableValidWriteIds writeIds = tableValidWriteIds.get(0);
    Assert.assertNotNull(writeIds);

    ValidReaderWriteIdList writeIdList = TxnUtils.createValidReaderWriteIdList(writeIds);
    Assert.assertNotNull(writeIdList);

    Assert.assertEquals(writeIdList.getInvalidWriteIds().length, 1);
    Assert.assertTrue(validTxnList.isTxnAborted(tids.get(0)));
    Assert.assertEquals(writeIdList.getHighWatermark(), 1);
    Assert.assertEquals(writeIdList.getMinOpenWriteId().longValue(), 2);

    client.commitTxn(tids.get(2));
    validTxnList = client.getValidTxns();

    tableValidWriteIds = client.getValidWriteIds(
      Collections.singletonList(fullTableName), validTxnList.writeToString());

    Assert.assertEquals(tableValidWriteIds.size(), 1);
    writeIds = tableValidWriteIds.get(0);
    Assert.assertNotNull(writeIds);

    writeIdList = TxnUtils.createValidReaderWriteIdList(writeIds);
    Assert.assertNotNull(writeIdList);

    Assert.assertEquals(writeIdList.getInvalidWriteIds().length, 2);
    Assert.assertTrue(validTxnList.isTxnAborted(tids.get(0)));
    Assert.assertFalse(validTxnList.isTxnValid(tids.get(1)));
    Assert.assertEquals(writeIdList.getHighWatermark(), 3);
    Assert.assertEquals(writeIdList.getMinOpenWriteId().longValue(), 2);
  }

  @Before
  public void setUp() throws Exception {
    conf.setBoolean(ConfVars.HIVE_IN_TEST.getVarname(), true);
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    TxnDbUtil.setConfValues(conf);
    TxnDbUtil.prepDb(conf);
    client = new HiveMetaStoreClient(conf);

    String connectionStr = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY);
    conn = DriverManager.getConnection(connectionStr);
  }

  @After
  public void tearDown() throws Exception {
    conn.close();
    TxnDbUtil.cleanDb(conf);
  }
}
