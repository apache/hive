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
package org.apache.hadoop.hive.ql.materializedview;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.txn.TxnHandler;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

public class TestHeartbeatTxnRangeFunction {
  static final private String CLASS_NAME = TxnHandler.class.getName();

  private final HiveConf conf = new HiveConf();
  private TxnStore txnHandler;

  public TestHeartbeatTxnRangeFunction() throws Exception {
    TestTxnDbUtil.setConfValues(conf);
    TestTxnDbUtil.prepDb(conf);
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    Configuration conf = ctx.getConfiguration();
    conf.getLoggerConfig(CLASS_NAME).setLevel(Level.DEBUG);
    ctx.updateLoggers(conf);
    tearDown();
  }

  @Before
  public void setUp() throws Exception {
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
    long txnId = openTxn();
    txnHandler.abortTxn(new AbortTxnRequest(txnId));
    HeartbeatTxnRangeRequest request = new HeartbeatTxnRangeRequest(txnId, txnId);

    HeartbeatTxnRangeResponse response = txnHandler.heartbeatTxnRange(request);

    assertEquals(1, response.getAbortedSize());
  }

  @Test
  public void testHeartbeatTxnRangeFunction_Success() throws Exception {
    long txnId = openTxn();

    String firstHeartbeat = TestTxnDbUtil.queryToString(conf, "select \"TXN_LAST_HEARTBEAT\" from \"TXNS\" where \"TXN_ID\" = " + txnId, false);
    HeartbeatTxnRangeRequest request = new HeartbeatTxnRangeRequest(txnId, txnId);

    HeartbeatTxnRangeResponse response = txnHandler.heartbeatTxnRange(request);
    String updatedHeartbeat = TestTxnDbUtil.queryToString(conf, "select \"TXN_LAST_HEARTBEAT\" from \"TXNS\" where \"TXN_ID\" = " + txnId, false);

    assertEquals(0, response.getAbortedSize());
    assertEquals(0, response.getNosuchSize());
    assertNotNull(firstHeartbeat);
    assertNotNull(updatedHeartbeat);

    assertNotEquals(firstHeartbeat, updatedHeartbeat);
  }

  private long openTxn() throws MetaException {
    List<Long> txns = txnHandler.openTxns(new OpenTxnRequest(1, "me", "localhost")).getTxn_ids();
    return txns.get(0);
  }
}
