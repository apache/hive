/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.txn;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.TxnState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * This test checks that the transaction handler works when the connection pool is set to none.
 */
public class TestTxnHandlerNoConnectionPool {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestTxnHandlerNoConnectionPool.class.getName());

  private HiveConf conf = new HiveConf();
  private TxnStore txnHandler;

  @Before
  public void setUp() throws Exception {
    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE, "None");
    TxnDbUtil.setConfValues(conf);
    try {
      TxnDbUtil.prepDb(conf);
    } catch (SQLException e) {
      // Usually this means we've already created the tables, so clean them and then try again
      tearDown();
      TxnDbUtil.prepDb(conf);
    }
    txnHandler = TxnUtils.getTxnStore(conf);
  }

  @After
  public void tearDown() throws Exception {
    TxnDbUtil.cleanDb(conf);
  }

  @Test
  public void testOpenTxn() throws Exception {
    long first = openTxn();
    assertEquals(1L, first);
    long second = openTxn();
    assertEquals(2L, second);
    GetOpenTxnsInfoResponse txnsInfo = txnHandler.getOpenTxnsInfo();
    assertEquals(2L, txnsInfo.getTxn_high_water_mark());
    assertEquals(2, txnsInfo.getOpen_txns().size());
    assertEquals(1L, txnsInfo.getOpen_txns().get(0).getId());
    assertEquals(TxnState.OPEN, txnsInfo.getOpen_txns().get(0).getState());
    assertEquals(2L, txnsInfo.getOpen_txns().get(1).getId());
    assertEquals(TxnState.OPEN, txnsInfo.getOpen_txns().get(1).getState());
    assertEquals("me", txnsInfo.getOpen_txns().get(1).getUser());
    assertEquals("localhost", txnsInfo.getOpen_txns().get(1).getHostname());

    GetOpenTxnsResponse txns = txnHandler.getOpenTxns();
    assertEquals(2L, txns.getTxn_high_water_mark());
    assertEquals(2, txns.getOpen_txns().size());
    boolean[] saw = new boolean[3];
    for (int i = 0; i < saw.length; i++) saw[i] = false;
    for (Long tid : txns.getOpen_txns()) {
      saw[tid.intValue()] = true;
    }
    for (int i = 1; i < saw.length; i++) assertTrue(saw[i]);
  }

  private long openTxn() throws MetaException {
    List<Long> txns = txnHandler.openTxns(new OpenTxnRequest(1, "me", "localhost")).getTxn_ids();
    return txns.get(0);
  }

}
