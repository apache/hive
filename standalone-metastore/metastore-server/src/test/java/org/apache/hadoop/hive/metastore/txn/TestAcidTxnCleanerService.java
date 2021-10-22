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

package org.apache.hadoop.hive.metastore.txn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.LockComponentBuilder;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AbortTxnsRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


import static java.util.Collections.singletonList;

/**
 * Testing whether AcidTxnCleanerService removes the correct records
 * from the TXNS table (via TxnStore).
 */
public class TestAcidTxnCleanerService {

  private AcidTxnCleanerService underTest;
  private TxnStore txnHandler;
  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    underTest = new AcidTxnCleanerService();
    underTest.setConf(conf);
    txnHandler = TxnUtils.getTxnStore(conf);
    txnHandler.setOpenTxnTimeOutMillis(100);
    TestTxnDbUtil.prepDb(conf);
  }

  @After
  public void tearDown() throws Exception {
    TestTxnDbUtil.cleanDb(conf);
  }

  @Test
  public void cleansEmptyAbortedTxns() throws Exception {
    for (int i = 0; i < 5; ++i) {
      long txnid = openTxn();
      txnHandler.abortTxn(new AbortTxnRequest(txnid));
    }
    // +1 represents the initial TXNS record (txnid=0)
    Assert.assertEquals(5 + 1, getTxnCount());
    Thread.sleep(txnHandler.getOpenTxnTimeOutMillis() * 2);

    underTest.run();

    // always leaves the MAX(TXN_ID) in the TXNS table
    Assert.assertEquals(1, getTxnCount());
    // The max TxnId might be greater id the openTxns failes with slow commit and retries
    Assert.assertTrue("The max txnId should be at least 5", getMaxTxnId() >= 5);
  }

  @Test
  public void doesNotCleanAbortedTxnsThatAreNonEmpty() throws Exception {
    for (int i = 0; i < 5; ++i) {
      openNonEmptyThenAbort();
    }
    Assert.assertEquals(5 + 1, getTxnCount());
    Thread.sleep(txnHandler.getOpenTxnTimeOutMillis() * 2);

    underTest.run();

    // deletes only the initial (committed) TXNS record
    Assert.assertEquals(5, getTxnCount());
    Assert.assertTrue("The max txnId should be at least 5", getMaxTxnId() >= 5);
  }

  @Test
  public void cleansAllCommittedTxns() throws Exception {
    for (int i = 0; i < 5; ++i) {
      long txnid = openTxn();
      txnHandler.commitTxn(new CommitTxnRequest(txnid));
    }
    Assert.assertEquals(5 + 1, getTxnCount());
    Thread.sleep(txnHandler.getOpenTxnTimeOutMillis() * 2);

    underTest.run();

    // always leaves the MAX(TXN_ID) in the TXNS table
    Assert.assertEquals(1, getTxnCount());
    Assert.assertTrue("The max txnId should be at least 5", getMaxTxnId() >= 5);
  }

  @Test
  public void cleansCommittedAndEmptyAbortedOnly() throws Exception {
    for (int i = 0; i < 5; ++i) {
      // commit one
      long txnid = openTxn();
      txnHandler.commitTxn(new CommitTxnRequest(txnid));
      // abort one empty
      txnid = openTxn();
      txnHandler.abortTxn(new AbortTxnRequest(txnid));
      // abort one non-empty
      openNonEmptyThenAbort();
    }
    Assert.assertEquals(15 + 1, getTxnCount());
    Thread.sleep(txnHandler.getOpenTxnTimeOutMillis() * 2);

    underTest.run();

    // kept only the 5 non-empty aborted ones
    Assert.assertEquals(5, getTxnCount());
    Assert.assertTrue("The max txnId should be at least 15", getMaxTxnId() >= 15);
  }

  @Test
  public void cleansEmptyAbortedBatchTxns() throws Exception {
    // add one non-empty aborted txn
    openNonEmptyThenAbort();
    // add a batch of empty, aborted txns
    txnHandler.setOpenTxnTimeOutMillis(30000);
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.TXN_MAX_OPEN_BATCH,
        TxnStore.TIMED_OUT_TXN_ABORT_BATCH_SIZE + 50);
    OpenTxnsResponse resp = txnHandler.openTxns(new OpenTxnRequest(
        TxnStore.TIMED_OUT_TXN_ABORT_BATCH_SIZE + 50, "user", "hostname"));
    txnHandler.setOpenTxnTimeOutMillis(1);
    txnHandler.abortTxns(new AbortTxnsRequest(resp.getTxn_ids()));
    GetOpenTxnsResponse openTxns = txnHandler.getOpenTxns();
    Assert.assertEquals(TxnStore.TIMED_OUT_TXN_ABORT_BATCH_SIZE + 50 + 1, openTxns.getOpen_txnsSize());

    underTest.run();

    openTxns = txnHandler.getOpenTxns();
    Assert.assertEquals(2, openTxns.getOpen_txnsSize());
    Assert.assertTrue("The max txnId should be at least", getMaxTxnId() >= TxnStore.TIMED_OUT_TXN_ABORT_BATCH_SIZE + 50 + 1);
  }

  private void openNonEmptyThenAbort() throws MetaException, NoSuchTxnException, TxnAbortedException {
    long txnid = openTxn();
    LockRequest req = getLockRequest();
    req.setTxnid(txnid);
    txnHandler.lock(req);
    txnHandler.abortTxn(new AbortTxnRequest(txnid));
  }

  private long openTxn() throws MetaException {
    return txnHandler
        .openTxns(new OpenTxnRequest(1, "me", "localhost"))
        .getTxn_ids()
        .get(0);
  }

  private LockRequest getLockRequest() {
    LockComponent comp = new LockComponentBuilder()
        .setDbName("default")
        .setTableName("ceat")
        .setOperationType(DataOperationType.UPDATE)
        .setSharedWrite()
        .build();
    return new LockRequest(singletonList(comp), "me", "localhost");
  }

  private long getTxnCount() throws Exception {
    return TestTxnDbUtil.countQueryAgent(conf, "SELECT COUNT(*) FROM \"TXNS\"");
  }

  private long getMaxTxnId() throws Exception {
    return TestTxnDbUtil.countQueryAgent(conf, "SELECT MAX(\"TXN_ID\") FROM \"TXNS\"");
  }
}
