/**
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

import junit.framework.Assert;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
public class TestHiveMetaStoreTxns {

  private HiveConf conf = new HiveConf();
  private IMetaStoreClient client;

  public TestHiveMetaStoreTxns() throws Exception {
    TxnDbUtil.setConfValues(conf);
    LogManager.getRootLogger().setLevel(Level.DEBUG);
    tearDown();
  }

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
  public void testTxnRange() throws Exception {
    ValidTxnList validTxns = client.getValidTxns();
    Assert.assertEquals(ValidTxnList.RangeResponse.NONE,
        validTxns.isTxnRangeValid(1L, 3L));
    List<Long> tids = client.openTxns("me", 5).getTxn_ids();

    HeartbeatTxnRangeResponse rsp = client.heartbeatTxnRange(1, 5);
    Assert.assertEquals(0, rsp.getNosuch().size());
    Assert.assertEquals(0, rsp.getAborted().size());

    client.rollbackTxn(1L);
    client.commitTxn(2L);
    client.commitTxn(3L);
    client.commitTxn(4L);
    validTxns = client.getValidTxns();
    System.out.println("validTxns = " + validTxns);
    Assert.assertEquals(ValidTxnList.RangeResponse.ALL,
        validTxns.isTxnRangeValid(2L, 2L));
    Assert.assertEquals(ValidTxnList.RangeResponse.ALL,
        validTxns.isTxnRangeValid(2L, 3L));
    Assert.assertEquals(ValidTxnList.RangeResponse.ALL,
        validTxns.isTxnRangeValid(2L, 4L));
    Assert.assertEquals(ValidTxnList.RangeResponse.ALL,
        validTxns.isTxnRangeValid(3L, 4L));

    Assert.assertEquals(ValidTxnList.RangeResponse.SOME,
        validTxns.isTxnRangeValid(1L, 4L));
    Assert.assertEquals(ValidTxnList.RangeResponse.SOME,
        validTxns.isTxnRangeValid(2L, 5L));
    Assert.assertEquals(ValidTxnList.RangeResponse.SOME,
        validTxns.isTxnRangeValid(1L, 2L));
    Assert.assertEquals(ValidTxnList.RangeResponse.SOME,
        validTxns.isTxnRangeValid(4L, 5L));

    Assert.assertEquals(ValidTxnList.RangeResponse.NONE,
        validTxns.isTxnRangeValid(1L, 1L));
    Assert.assertEquals(ValidTxnList.RangeResponse.NONE,
        validTxns.isTxnRangeValid(5L, 10L));

    validTxns = new ValidReadTxnList("10:4:5:6");
    Assert.assertEquals(ValidTxnList.RangeResponse.NONE,
        validTxns.isTxnRangeValid(4,6));
    Assert.assertEquals(ValidTxnList.RangeResponse.ALL,
        validTxns.isTxnRangeValid(7, 10));
    Assert.assertEquals(ValidTxnList.RangeResponse.SOME,
        validTxns.isTxnRangeValid(7, 11));
    Assert.assertEquals(ValidTxnList.RangeResponse.SOME,
        validTxns.isTxnRangeValid(3, 6));
    Assert.assertEquals(ValidTxnList.RangeResponse.SOME,
        validTxns.isTxnRangeValid(4, 7));
    Assert.assertEquals(ValidTxnList.RangeResponse.SOME,
        validTxns.isTxnRangeValid(1, 12));
    Assert.assertEquals(ValidTxnList.RangeResponse.ALL,
        validTxns.isTxnRangeValid(1, 3));
  }

  @Test
  public void testLocks() throws Exception {
    LockRequestBuilder rqstBuilder = new LockRequestBuilder();
    rqstBuilder.addLockComponent(new LockComponentBuilder()
        .setDbName("mydb")
        .setTableName("mytable")
        .setPartitionName("mypartition")
        .setExclusive()
        .build());
    rqstBuilder.addLockComponent(new LockComponentBuilder()
        .setDbName("mydb")
        .setTableName("yourtable")
        .setSemiShared()
        .build());
    rqstBuilder.addLockComponent(new LockComponentBuilder()
        .setDbName("yourdb")
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
        .setExclusive()
        .build())
      .addLockComponent(new LockComponentBuilder()
        .setDbName("mydb")
        .setTableName("yourtable")
        .setSemiShared()
        .build())
      .addLockComponent(new LockComponentBuilder()
        .setDbName("yourdb")
        .setShared()
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
    ValidTxnList validTxns = new ValidReadTxnList("1:");
    String asString = validTxns.toString();
    Assert.assertEquals("1:", asString);
    validTxns = new ValidReadTxnList(asString);
    Assert.assertEquals(1, validTxns.getHighWatermark());
    Assert.assertNotNull(validTxns.getInvalidTransactions());
    Assert.assertEquals(0, validTxns.getInvalidTransactions().length);
    asString = validTxns.toString();
    Assert.assertEquals("1:", asString);
    validTxns = new ValidReadTxnList(asString);
    Assert.assertEquals(1, validTxns.getHighWatermark());
    Assert.assertNotNull(validTxns.getInvalidTransactions());
    Assert.assertEquals(0, validTxns.getInvalidTransactions().length);

    // Test with open transactions
    validTxns = new ValidReadTxnList("10:5:3");
    asString = validTxns.toString();
    if (!asString.equals("10:3:5") && !asString.equals("10:5:3")) {
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

  @Before
  public void setUp() throws Exception {
    TxnDbUtil.prepDb();
    client = new HiveMetaStoreClient(conf);
  }

  @After
  public void tearDown() throws Exception {
    TxnDbUtil.cleanDb();
  }
}
