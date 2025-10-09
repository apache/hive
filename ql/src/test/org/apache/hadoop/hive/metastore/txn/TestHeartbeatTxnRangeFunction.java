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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

public class TestHeartbeatTxnRangeFunction {

  private static final HiveConf conf = new HiveConf();
  private static TxnStore txnHandler;

  @BeforeClass
  public static void setUp() throws Exception {
    TestTxnDbUtil.setConfValues(conf);
    TestTxnDbUtil.prepDb(conf);
    txnHandler = TxnUtils.getTxnStore(conf);
  }

  @After
  public void tearDown() throws Exception {
    TestTxnDbUtil.cleanDb(conf);
  }

  @Test
  public void testHeartbeatTxnRangeFunction_NoSuchTxn() throws MetaException {
    HeartbeatTxnRangeRequest request = new HeartbeatTxnRangeRequest(1L, 1L);

    HeartbeatTxnRangeResponse response = txnHandler.heartbeatTxnRange(request);

    assertEquals(1, response.getNosuchSize());
  }

  @Test
  public void testHeartbeatTxnRangeFunction_AbortedTxn() throws MetaException, TxnAbortedException, NoSuchTxnException {
    openTxn();
    openTxn();
    long txnId = openTxn();

    txnHandler.abortTxn(new AbortTxnRequest(txnId));
    HeartbeatTxnRangeRequest request = new HeartbeatTxnRangeRequest(1L, txnId);

    HeartbeatTxnRangeResponse response = txnHandler.heartbeatTxnRange(request);

    assertEquals(1, response.getAbortedSize());
    Long txn = response.getAbortedIterator().next();
    assertEquals(3L, (long)txn);
    assertEquals(0, response.getNosuch().size());
  }

  @Test
  public void testHeartbeatTxnRangeFunction_Success() throws Exception {
    openTxn();
    openTxn();
    long txnId = openTxn();

    String firstHeartbeat = TestTxnDbUtil.queryToString(conf, "select \"TXN_LAST_HEARTBEAT\" from \"TXNS\" where \"TXN_ID\" = " + txnId, false);
    HeartbeatTxnRangeRequest request = new HeartbeatTxnRangeRequest(1L, txnId);

    HeartbeatTxnRangeResponse response = txnHandler.heartbeatTxnRange(request);
    String updatedHeartbeat = TestTxnDbUtil.queryToString(conf, "select \"TXN_LAST_HEARTBEAT\" from \"TXNS\" where \"TXN_ID\" = " + txnId, false);

    assertEquals(0, response.getAbortedSize());
    assertEquals(0, response.getNosuchSize());
    assertNotNull(firstHeartbeat);
    assertNotNull(updatedHeartbeat);

    assertNotEquals(firstHeartbeat, updatedHeartbeat);
  }

  @Test
  public void testHeartbeatTxnRangeOneCommitted_Mixed() throws Exception {
    openTxn();
    txnHandler.commitTxn(new CommitTxnRequest(1));
    openTxn();
    long txnId = openTxn();

    HeartbeatTxnRangeResponse rsp =
            txnHandler.heartbeatTxnRange(new HeartbeatTxnRangeRequest(1, txnId));

    assertEquals(1, rsp.getNosuchSize());
    long noSuchTxnId = rsp.getNosuchIterator().next();
    assertEquals(1L, noSuchTxnId);

    assertEquals(0, rsp.getAbortedSize());
  }

  private long openTxn() throws MetaException {
    List<Long> txns = txnHandler.openTxns(new OpenTxnRequest(1, "me", "localhost")).getTxn_ids();
    return txns.get(0);
  }
}
