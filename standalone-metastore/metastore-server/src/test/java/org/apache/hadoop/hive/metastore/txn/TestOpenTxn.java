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
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.datasource.DataSourceProvider;
import org.apache.hadoop.hive.metastore.datasource.DataSourceProviderFactory;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Test openTxn and getOpenTxnList calls on TxnStore.
 */
public class TestOpenTxn {

  private Configuration conf = MetastoreConf.newMetastoreConf();
  private TxnStore txnHandler;

  @Before
  public void setUp() throws Exception {
    // This will init the metastore db
    txnHandler = TxnUtils.getTxnStore(conf);
    TestTxnDbUtil.prepDb(conf);
  }

  @After
  public void tearDown() throws Exception {
    TestTxnDbUtil.cleanDb(conf);
  }

  @Test
  public void testSingleOpen() throws MetaException {
    OpenTxnRequest openTxnRequest = new OpenTxnRequest(1, "me", "localhost");
    long txnId = txnHandler.openTxns(openTxnRequest).getTxn_ids().get(0);
    Assert.assertEquals(1, txnId);
  }

  @Test
  public void testGap() throws Exception {
    OpenTxnRequest openTxnRequest = new OpenTxnRequest(1, "me", "localhost");
    txnHandler.openTxns(openTxnRequest);
    long second = txnHandler.openTxns(openTxnRequest).getTxn_ids().get(0);
    deleteTransaction(second);
    txnHandler.openTxns(openTxnRequest);
    GetOpenTxnsResponse openTxns = txnHandler.getOpenTxns();
    Assert.assertEquals(3, openTxns.getOpen_txnsSize());

  }

  @Test
  public void testGapWithOldOpen() throws Exception {
    OpenTxnRequest openTxnRequest = new OpenTxnRequest(1, "me", "localhost");
    txnHandler.openTxns(openTxnRequest);
    Thread.sleep(1000);
    long second = txnHandler.openTxns(openTxnRequest).getTxn_ids().get(0);
    deleteTransaction(second);
    txnHandler.openTxns(openTxnRequest);
    GetOpenTxnsResponse openTxns = txnHandler.getOpenTxns();
    Assert.assertEquals(3, openTxns.getOpen_txnsSize());
  }

  @Test
  public void testGapWithOldCommit() throws Exception {
    OpenTxnRequest openTxnRequest = new OpenTxnRequest(1, "me", "localhost");
    long first = txnHandler.openTxns(openTxnRequest).getTxn_ids().get(0);
    txnHandler.commitTxn(new CommitTxnRequest(first));
    long second = txnHandler.openTxns(openTxnRequest).getTxn_ids().get(0);
    deleteTransaction(second);
    txnHandler.openTxns(openTxnRequest);
    GetOpenTxnsResponse openTxns = txnHandler.getOpenTxns();
    Assert.assertEquals(2, openTxns.getOpen_txnsSize());
  }

  @Test
  public void testMultiGapWithOldCommit() throws Exception {
    OpenTxnRequest openTxnRequest = new OpenTxnRequest(1, "me", "localhost");
    long first = txnHandler.openTxns(openTxnRequest).getTxn_ids().get(0);
    txnHandler.commitTxn(new CommitTxnRequest(first));
    long second = txnHandler.openTxns(new OpenTxnRequest(10, "me", "localhost")).getTxn_ids().get(0);
    deleteTransaction(second, second + 9);
    txnHandler.openTxns(openTxnRequest);
    GetOpenTxnsResponse openTxns = txnHandler.getOpenTxns();
    Assert.assertEquals(11, openTxns.getOpen_txnsSize());
  }

  private void deleteTransaction(long txnId) throws SQLException {
    deleteTransaction(txnId, txnId);
  }

  private void deleteTransaction(long minTxnId, long maxTxnId) throws SQLException {
    DataSourceProvider dsp = DataSourceProviderFactory.tryGetDataSourceProviderOrNull(conf);
    DataSource ds = dsp.create(conf);
    Connection dbConn = ds.getConnection();
    Statement stmt = dbConn.createStatement();
    stmt.executeUpdate("DELETE FROM TXNS WHERE TXN_ID >=" + minTxnId + " AND TXN_ID <=" + maxTxnId);
    dbConn.commit();
    stmt.close();
    dbConn.close();
  }

}
