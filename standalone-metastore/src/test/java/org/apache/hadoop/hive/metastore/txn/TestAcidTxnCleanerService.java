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
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

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
    txnHandler = TxnUtils.getTxnStore(conf);
    TxnDbUtil.prepDb(conf);

    underTest = new AcidTxnCleanerService();
    underTest.setConf(conf);
  }

  @After
  public void tearDown() throws Exception {
    TxnDbUtil.cleanDb(conf);
  }

  @Test
  public void cleanEmptyAbortedTxns() throws Exception {
    // Test that we are cleaning aborted transactions with no components left in txn_components.
    // Put one aborted transaction with an entry in txn_components to make sure we don't
    // accidentally clean it too.

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
    comp.setTablename("ceat");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    txnHandler.lock(req);
    txnHandler.abortTxn(new AbortTxnRequest(txnid));

    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.TXN_MAX_OPEN_BATCH, TxnStore.TIMED_OUT_TXN_ABORT_BATCH_SIZE + 50);
    OpenTxnsResponse resp = txnHandler.openTxns(new OpenTxnRequest(
      TxnStore.TIMED_OUT_TXN_ABORT_BATCH_SIZE + 50, "user", "hostname"));
    txnHandler.abortTxns(new AbortTxnsRequest(resp.getTxn_ids()));
    GetOpenTxnsResponse openTxns = txnHandler.getOpenTxns();
    Assert.assertEquals(TxnStore.TIMED_OUT_TXN_ABORT_BATCH_SIZE + 50 + 1, openTxns.getOpen_txnsSize());

    underTest.run();

    openTxns = txnHandler.getOpenTxns();
    Assert.assertEquals(1, openTxns.getOpen_txnsSize());
  }

  private long openTxn() throws MetaException {
    return txnHandler
        .openTxns(new OpenTxnRequest(1, "me", "localhost"))
        .getTxn_ids()
        .get(0);
  }
}
