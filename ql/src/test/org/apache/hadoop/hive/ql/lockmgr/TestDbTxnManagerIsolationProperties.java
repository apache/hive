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
package org.apache.hadoop.hive.ql.lockmgr;

import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.datasource.DataSourceProvider;
import org.apache.hadoop.hive.metastore.datasource.DataSourceProviderFactory;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests to check the ACID properties and isolation level requirements.
 */
public class TestDbTxnManagerIsolationProperties extends DbTxnManagerEndToEndTestBase {

  @Test
  public void basicOpenTxnsNoDirtyRead() throws Exception {
    driver.run(("drop table if exists gap"));
    driver.run("create table gap (a int, b int) " + "stored as orc TBLPROPERTIES ('transactional'='true')");
    // Create one TXN to read and do not run it
    driver.compileAndRespond("select * from gap");
    long first = txnMgr.getCurrentTxnId();

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver2.compileAndRespond("insert into gap values(1,2)");
    long second = txnMgr2.getCurrentTxnId();
    Assert.assertTrue("Sequence number goes onward", second > first);
    driver2.run();

    // Now we run our read query it should not see the write results of the insert
    swapTxnManager(txnMgr);
    driver.run();

    FetchTask fetchTask = driver.getFetchTask();
    List res = new ArrayList();
    fetchTask.fetch(res);
    Assert.assertEquals("No dirty read", 0, res.size());

  }
  @Test
  public void gapOpenTxnsNoDirtyRead() throws Exception {
    driver.run(("drop table if exists gap"));
    driver.run("create table gap (a int, b int) " + "stored as orc TBLPROPERTIES ('transactional'='true')");
    // Create one TXN to delete later
    driver.compileAndRespond("select * from gap");
    long first = txnMgr.getCurrentTxnId();
    driver.run();
    // The second one we use for Low water mark
    driver.run("select * from gap");
    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    // Make sure, that the time window is great enough to consider the gap open
    txnHandler.setOpenTxnTimeOutMillis(30000);
    // Create a gap
    deleteTransactionId(first);
    CommandProcessorResponse resp = driver2.compileAndRespond("select * from gap");
    long third = txnMgr2.getCurrentTxnId();
    Assert.assertTrue("Sequence number goes onward", third > first);
    ValidTxnList validTxns = txnMgr2.getValidTxns();
    Assert.assertEquals("Expect to see the gap as open", first, (long) validTxns.getMinOpenTxn());
    txnHandler.setOpenTxnTimeOutMillis(1000);

    // Now we cheat and create a transaction with the first sequenceId again imitating a very slow openTxns call
    setBackSequence(first);
    swapTxnManager(txnMgr);
    driver.compileAndRespond("insert into gap values(1,2)");
    long forth = txnMgr.getCurrentTxnId();
    Assert.assertEquals(first, forth);
    driver.run();

    // Now we run our read query it should not see the write results of the insert
    swapTxnManager(txnMgr2);
    driver2.run();

    FetchTask fetchTask = driver2.getFetchTask();
    List res = new ArrayList();
    fetchTask.fetch(res);
    Assert.assertEquals("No dirty read", 0, res.size());

  }



  @Test
  public void multipleGapOpenTxnsNoDirtyRead() throws Exception {
    driver.run(("drop table if exists gap"));
    driver.run("create table gap (a int, b int) " + "stored as orc TBLPROPERTIES ('transactional'='true')");
    // Create some TXN to delete later
    OpenTxnsResponse openTxns = txnHandler.openTxns(new OpenTxnRequest(10, "user", "local"));
    openTxns.getTxn_ids().stream().forEach(txnId -> {
      silentCommitTxn(new CommitTxnRequest(txnId));
    });

    long first = openTxns.getTxn_ids().get(0);
    long last = openTxns.getTxn_ids().get(9);
    // The next one we use for Low water mark
    driver.run("select * from gap");
    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    // Make sure, that the time window is great enough to consider the gap open
    txnHandler.setOpenTxnTimeOutMillis(30000);
    // Create a gap
    deleteTransactionId(first, last);
    CommandProcessorResponse resp = driver2.compileAndRespond("select * from gap");
    long next = txnMgr2.getCurrentTxnId();
    Assert.assertTrue("Sequence number goes onward", next > last);
    ValidTxnList validTxns = txnMgr2.getValidTxns();
    Assert.assertEquals("Expect to see the gap as open", first, (long) validTxns.getMinOpenTxn());
    txnHandler.setOpenTxnTimeOutMillis(1000);

    // Now we cheat and create a transaction with the first sequenceId again imitating a very slow openTxns call
    setBackSequence(first);
    swapTxnManager(txnMgr);
    driver.compileAndRespond("insert into gap values(1,2)");
    next = txnMgr.getCurrentTxnId();
    Assert.assertEquals(first, next);
    driver.run();

    // Now we run our read query it should not see the write results of the insert
    swapTxnManager(txnMgr2);
    driver2.run();

    FetchTask fetchTask = driver2.getFetchTask();
    List res = new ArrayList();
    fetchTask.fetch(res);
    Assert.assertEquals("No dirty read", 0, res.size());

  }

  @Test
  public void gapOpenTxnsDirtyRead() throws Exception {
    driver.run(("drop table if exists gap"));
    driver.run("create table gap (a int, b int) " + "stored as orc TBLPROPERTIES ('transactional'='true')");
    // Create one TXN to delete later
    driver.compileAndRespond("select * from gap");
    long first = txnMgr.getCurrentTxnId();
    driver.run();
    //The second one we use for Low water mark
    driver.run("select * from gap");
    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    // Now we wait for the time window to move forward
    Thread.sleep(txnHandler.getOpenTxnTimeOutMillis());
    // Create a gap
    deleteTransactionId(first);
    CommandProcessorResponse resp = driver2.compileAndRespond("select * from gap");
    long third = txnMgr2.getCurrentTxnId();
    Assert.assertTrue("Sequence number goes onward", third > first);
    ValidTxnList validTxns = txnMgr2.getValidTxns();
    Assert.assertNull("Expect to see no gap", validTxns.getMinOpenTxn());

    // Now we cheat and create a transaction with the first sequenceId again imitating a very slow openTxns call
    // This should never happen
    setBackSequence(first);
    swapTxnManager(txnMgr);
    driver.compileAndRespond("insert into gap values(1,2)");
    long forth = txnMgr.getCurrentTxnId();
    Assert.assertEquals(first, forth);
    driver.run();

    // Now we run our read query it should unfortunately see the results of the insert
    swapTxnManager(txnMgr2);
    driver2.run();

    FetchTask fetchTask = driver2.getFetchTask();
    List res = new ArrayList();
    fetchTask.fetch(res);
    Assert.assertEquals("Dirty read!", 1, res.size());

  }

  @Test
  public void testRebuildMVWhenOpenTxnPresents() throws Exception {
    driver.run(("drop table if exists t1"));
    driver.run("create table t1 (a int, b int) stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into t1 values(1,2),(2,2)");
    driver.run("create materialized view mat1 stored as orc " +
        "TBLPROPERTIES ('transactional'='true') as " +
        "select a,b from t1 where a > 1");

    driver.run("insert into t1 values(3,3)");

    // Simulate starting a transaction by another client
    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver2.compileAndRespond("delete from t1 where a = 2");

    // Switch back to client #1 and rebuild the MV, the transaction with the delete statement still open
    swapTxnManager(txnMgr);
    driver.run("alter materialized view mat1 rebuild");

    driver.run("select * from mat1 order by a");
    FetchTask fetchTask = driver.getFetchTask();
    List res = new ArrayList();
    fetchTask.fetch(res);
    Assert.assertEquals(2, res.size());
    Assert.assertEquals("2\t2", res.get(0));
    Assert.assertEquals("3\t3", res.get(1));

    // execute the delete statement and commit the transaction
    swapTxnManager(txnMgr2);
    driver2.run();

    // Rebuild the view again.
    swapTxnManager(txnMgr);
    driver.run("alter materialized view mat1 rebuild");

    driver.run("select * from mat1");
    fetchTask = driver.getFetchTask();
    res = new ArrayList();
    fetchTask.fetch(res);
    Assert.assertEquals(1, res.size());
    Assert.assertEquals("3\t3", res.get(0));
  }

  private void silentCommitTxn(CommitTxnRequest commitTxnRequest) {
    try {
      txnHandler.commitTxn(commitTxnRequest);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private void deleteTransactionId(long txnId) throws SQLException {
    deleteTransactionId(txnId, txnId);
  }

  private void deleteTransactionId(long minTxnId, long maxTxnId) throws SQLException {
    DataSourceProvider dsp = DataSourceProviderFactory.tryGetDataSourceProviderOrNull(conf);
    DataSource ds = dsp.create(conf);
    Connection dbConn = ds.getConnection();
    Statement stmt = dbConn.createStatement();
    stmt.executeUpdate("DELETE FROM TXNS WHERE TXN_ID >=" + minTxnId + " AND TXN_ID <=" + maxTxnId);
    dbConn.commit();
    stmt.close();
    dbConn.close();
  }

  private void setBackSequence(long txnId) throws SQLException {
    DataSourceProvider dsp = DataSourceProviderFactory.tryGetDataSourceProviderOrNull(conf);
    DataSource ds = dsp.create(conf);
    Connection dbConn = ds.getConnection();
    Statement stmt = dbConn.createStatement();
    stmt.executeUpdate("ALTER TABLE TXNS ALTER TXN_ID RESTART WITH " + txnId);
    dbConn.commit();
    stmt.close();
    dbConn.close();
  }

  public static HiveTxnManager swapTxnManager(HiveTxnManager txnMgr) {
    return SessionState.get().setTxnMgr(txnMgr);
  }
}
