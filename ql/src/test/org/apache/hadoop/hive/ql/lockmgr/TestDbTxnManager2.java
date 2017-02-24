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
package org.apache.hadoop.hive.ql.lockmgr;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.TestTxnCommands2;
import org.apache.hadoop.hive.ql.txn.AcidWriteSetService;
import org.junit.After;
import org.junit.Assert;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * See additional tests in {@link org.apache.hadoop.hive.ql.lockmgr.TestDbTxnManager}
 * Tests here are "end-to-end"ish and simulate concurrent queries.
 * 
 * The general approach is to use an instance of Driver to use Driver.run() to create tables
 * Use Driver.compile() to generate QueryPlan which can then be passed to HiveTxnManager.acquireLocks().
 * Same HiveTxnManager is used to openTxn()/commitTxn() etc.  This can exercise almost the entire
 * code path that CLI would but with the advantage that you can create a 2nd HiveTxnManager and then
 * simulate interleaved transactional/locking operations but all from within a single thread.
 * The later not only controls concurrency precisely but is the only way to run in UT env with DerbyDB.
 */
public class TestDbTxnManager2 {
  private static HiveConf conf = new HiveConf(Driver.class);
  private HiveTxnManager txnMgr;
  private Context ctx;
  private Driver driver;
  TxnStore txnHandler;

  public TestDbTxnManager2() throws Exception {
    conf
    .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    TxnDbUtil.setConfValues(conf);
    conf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
  }
  @Before
  public void setUp() throws Exception {
    SessionState.start(conf);
    ctx = new Context(conf);
    driver = new Driver(conf);
    driver.init();
    TxnDbUtil.cleanDb();
    TxnDbUtil.prepDb();
    SessionState ss = SessionState.get();
    ss.initTxnMgr(conf);
    txnMgr = ss.getTxnMgr();
    Assert.assertTrue(txnMgr instanceof DbTxnManager);
    txnHandler = TxnUtils.getTxnStore(conf);

  }
  @After
  public void tearDown() throws Exception {
    driver.close();
    if (txnMgr != null) txnMgr.closeTxnManager();
  }
  @Test
  public void testLocksInSubquery() throws Exception {
    dropTable(new String[] {"T","S", "R"});
    checkCmdOnDriver(driver.run("create table if not exists T (a int, b int)"));
    checkCmdOnDriver(driver.run("create table if not exists S (a int, b int) clustered by(b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')"));
    checkCmdOnDriver(driver.run("create table if not exists R (a int, b int) clustered by(b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')"));

    checkCmdOnDriver(driver.compileAndRespond("delete from S where a in (select a from T where b = 1)"));
    txnMgr.openTxn(ctx, "one");
    txnMgr.acquireLocks(driver.getPlan(), ctx, "one");
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T", null, locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "S", null, locks);
    txnMgr.rollbackTxn();

    checkCmdOnDriver(driver.compileAndRespond("update S set a = 7 where a in (select a from T where b = 1)"));
    txnMgr.openTxn(ctx, "one");
    txnMgr.acquireLocks(driver.getPlan(), ctx, "one");
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T", null, locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "S", null, locks);
    txnMgr.rollbackTxn();

    checkCmdOnDriver(driver.compileAndRespond("insert into R select * from S where a in (select a from T where b = 1)"));
    txnMgr.openTxn(ctx, "three");
    txnMgr.acquireLocks(driver.getPlan(), ctx, "three");
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 3, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T", null, locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "S", null, locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "R", null, locks);
  }
  @Test
  public void createTable() throws Exception {
    dropTable(new String[] {"T"});
    CommandProcessorResponse cpr = driver.compileAndRespond("create table if not exists T (a int, b int)");
    checkCmdOnDriver(cpr);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer");
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", null, null, locks);
    txnMgr.getLockManager().releaseLocks(ctx.getHiveLocks());
    Assert.assertEquals("Lock remained", 0, getLocks().size());
  }
  @Test
  public void insertOverwriteCreate() throws Exception {
    dropTable(new String[] {"T2", "T3"});
    CommandProcessorResponse cpr = driver.run("create table if not exists T2(a int)");
    checkCmdOnDriver(cpr);
    cpr = driver.run("create table if not exists T3(a int)");
    checkCmdOnDriver(cpr);
    cpr = driver.compileAndRespond("insert overwrite table T3 select a from T2");
    checkCmdOnDriver(cpr);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer");
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T2", null, locks);
    checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "T3", null, locks);
    txnMgr.getLockManager().releaseLocks(ctx.getHiveLocks());
    Assert.assertEquals("Lock remained", 0, getLocks().size());
    cpr = driver.run("drop table if exists T1");
    checkCmdOnDriver(cpr);
    cpr = driver.run("drop table if exists T2");
    checkCmdOnDriver(cpr);
  }
  @Test
  public void insertOverwritePartitionedCreate() throws Exception {
    dropTable(new String[] {"T4"});
    CommandProcessorResponse cpr = driver.run("create table if not exists T4 (name string, gpa double) partitioned by (age int)");
    checkCmdOnDriver(cpr);
    cpr = driver.run("create table if not exists T5(name string, age int, gpa double)");
    checkCmdOnDriver(cpr);
    cpr = driver.compileAndRespond("INSERT OVERWRITE TABLE T4 PARTITION (age) SELECT name, age, gpa FROM T5");
    checkCmdOnDriver(cpr);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer");
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T5", null, locks);
    checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "T4", null, locks);
    txnMgr.getLockManager().releaseLocks(ctx.getHiveLocks());
    Assert.assertEquals("Lock remained", 0, getLocks().size());
    cpr = driver.run("drop table if exists T5");
    checkCmdOnDriver(cpr);
    cpr = driver.run("drop table if exists T4");
    checkCmdOnDriver(cpr);
  }
  @Test
  public void basicBlocking() throws Exception {
    dropTable(new String[] {"T6"});
    CommandProcessorResponse cpr = driver.run("create table if not exists T6(a int)");
    checkCmdOnDriver(cpr);
    cpr = driver.compileAndRespond("select a from T6");
    checkCmdOnDriver(cpr);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer");//gets S lock on T6
    List<HiveLock> selectLocks = ctx.getHiveLocks();
    cpr = driver.compileAndRespond("drop table if exists T6");
    checkCmdOnDriver(cpr);
    //tries to get X lock on T1 and gets Waiting state
    LockState lockState = ((DbTxnManager) txnMgr).acquireLocks(driver.getPlan(), ctx, "Fiddler", false);
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T6", null, locks);
    checkLock(LockType.EXCLUSIVE, LockState.WAITING, "default", "T6", null, locks);
    txnMgr.getLockManager().releaseLocks(selectLocks);//release S on T6
    //attempt to X on T6 again - succeed
    lockState = ((DbLockManager)txnMgr.getLockManager()).checkLock(locks.get(1).getLockid());
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "T6", null, locks);
    List<HiveLock> xLock = new ArrayList<HiveLock>(0);
    xLock.add(new DbLockManager.DbHiveLock(locks.get(0).getLockid()));
    txnMgr.getLockManager().releaseLocks(xLock);
    cpr = driver.run("drop table if exists T6");
    locks = getLocks();
    Assert.assertEquals("Unexpected number of locks found", 0, locks.size());
    checkCmdOnDriver(cpr);
  }
  @Test
  public void lockConflictDbTable() throws Exception {
    dropTable(new String[] {"temp.T7"});
    CommandProcessorResponse cpr = driver.run("create database if not exists temp");
    checkCmdOnDriver(cpr);
    cpr = driver.run("create table if not exists temp.T7(a int, b int) clustered by(b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    cpr = driver.compileAndRespond("update temp.T7 set a = 5 where b = 6");
    checkCmdOnDriver(cpr);
    txnMgr.openTxn(ctx, "Fifer");
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer");
    checkCmdOnDriver(driver.compileAndRespond("drop database if exists temp"));
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    //txnMgr2.openTxn("Fiddler");
    ((DbTxnManager)txnMgr2).acquireLocks(driver.getPlan(), ctx, "Fiddler", false);//gets SS lock on T7
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "temp", "T7", null, locks);
    checkLock(LockType.EXCLUSIVE, LockState.WAITING, "temp", null, null, locks);
    txnMgr.commitTxn();
    ((DbLockManager)txnMgr.getLockManager()).checkLock(locks.get(1).getLockid());
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "temp", null, null, locks);
    List<HiveLock> xLock = new ArrayList<HiveLock>(0);
    xLock.add(new DbLockManager.DbHiveLock(locks.get(0).getLockid()));
    txnMgr2.getLockManager().releaseLocks(xLock);
  }
  @Test
  public void updateSelectUpdate() throws Exception {
    dropTable(new String[] {"T8"});
    CommandProcessorResponse cpr = driver.run("create table T8(a int, b int) clustered by(b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    cpr = driver.compileAndRespond("delete from T8 where b = 89");
    checkCmdOnDriver(cpr);
    txnMgr.openTxn(ctx, "Fifer");
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer");//gets SS lock on T8
    cpr = driver.compileAndRespond("select a from T8");//gets S lock on T8
    checkCmdOnDriver(cpr);
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnMgr2.openTxn(ctx, "Fiddler");
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "Fiddler");
    checkCmdOnDriver(driver.compileAndRespond("update T8 set a = 1 where b = 1"));
    ((DbTxnManager) txnMgr2).acquireLocks(driver.getPlan(), ctx, "Practical", false);//waits for SS lock on T8 from fifer
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 3, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T8", null, locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "T8", null, locks);
    checkLock(LockType.SHARED_WRITE, LockState.WAITING, "default", "T8", null, locks);
    txnMgr.rollbackTxn();
    ((DbLockManager)txnMgr2.getLockManager()).checkLock(locks.get(2).getLockid());
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T8", null, locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "T8", null, locks);
    txnMgr2.commitTxn();
    cpr = driver.run("drop table if exists T6");
    locks = getLocks();
    Assert.assertEquals("Unexpected number of locks found", 0, locks.size());
    checkCmdOnDriver(cpr);
  }

  @Test
  public void testLockRetryLimit() throws Exception {
    dropTable(new String[] {"T9"});
    conf.setIntVar(HiveConf.ConfVars.HIVE_LOCK_NUMRETRIES, 2);
    conf.setBoolVar(HiveConf.ConfVars.TXN_MGR_DUMP_LOCK_STATE_ON_ACQUIRE_TIMEOUT, true);
    HiveTxnManager otherTxnMgr = new DbTxnManager(); 
    ((DbTxnManager)otherTxnMgr).setHiveConf(conf);
    CommandProcessorResponse cpr = driver.run("create table T9(a int)");
    checkCmdOnDriver(cpr);
    cpr = driver.compileAndRespond("select * from T9");
    checkCmdOnDriver(cpr);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Vincent Vega");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T9", null, locks);
    
    cpr = driver.compileAndRespond("drop table T9");
    checkCmdOnDriver(cpr);
    try {
      otherTxnMgr.acquireLocks(driver.getPlan(), ctx, "Winston Winnfield");
    }
    catch(LockException ex) {
      Assert.assertEquals("Got wrong lock exception", ErrorMsg.LOCK_ACQUIRE_TIMEDOUT, ex.getCanonicalErrorMsg());
    }
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T9", null, locks);
    otherTxnMgr.closeTxnManager();
  }

  /**
   * check that locks in Waiting state show what they are waiting on
   * This test is somewhat abusive in that it make DbLockManager retain locks for 2
   * different queries (which are not part of the same transaction) which can never
   * happen in real use cases... but it makes testing convenient.
   * @throws Exception
   */
  @Test
  public void testLockBlockedBy() throws Exception {
    dropTable(new String[] {"TAB_BLOCKED"});
    CommandProcessorResponse cpr = driver.run("create table TAB_BLOCKED (a int, b int) clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    cpr = driver.compileAndRespond("select * from TAB_BLOCKED");
    checkCmdOnDriver(cpr);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "I AM SAM");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "TAB_BLOCKED", null, locks);
    cpr = driver.compileAndRespond("drop table TAB_BLOCKED");
    checkCmdOnDriver(cpr);
    ((DbTxnManager)txnMgr).acquireLocks(driver.getPlan(), ctx, "SAM I AM", false);//make non-blocking
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "TAB_BLOCKED", null, locks);
    checkLock(LockType.EXCLUSIVE, LockState.WAITING, "default", "TAB_BLOCKED", null, locks);
    Assert.assertEquals("BlockedByExtId doesn't match", locks.get(0).getLockid(), locks.get(1).getBlockedByExtId());
    Assert.assertEquals("BlockedByIntId doesn't match", locks.get(0).getLockIdInternal(), locks.get(1).getBlockedByIntId());
  }

  @Test
  public void testDummyTxnManagerOnAcidTable() throws Exception {
    dropTable(new String[] {"T10", "T11"});
    // Create an ACID table with DbTxnManager
    CommandProcessorResponse cpr = driver.run("create table T10 (a int, b int) clustered by(b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    cpr = driver.run("create table T11 (a int, b int) clustered by(b) into 2 buckets stored as orc");
    checkCmdOnDriver(cpr);

    // All DML should fail with DummyTxnManager on ACID table
    useDummyTxnManagerTemporarily(conf);
    cpr = driver.compileAndRespond("select * from T10");
    Assert.assertEquals(ErrorMsg.TXNMGR_NOT_ACID.getErrorCode(), cpr.getResponseCode());
    Assert.assertTrue(cpr.getErrorMessage().contains("This command is not allowed on an ACID table"));

    useDummyTxnManagerTemporarily(conf);
    cpr = driver.compileAndRespond("insert into table T10 values (1, 2)");
    Assert.assertEquals(ErrorMsg.TXNMGR_NOT_ACID.getErrorCode(), cpr.getResponseCode());
    Assert.assertTrue(cpr.getErrorMessage().contains("This command is not allowed on an ACID table"));

    useDummyTxnManagerTemporarily(conf);
    cpr = driver.compileAndRespond("insert overwrite table T10 select a, b from T11");
    Assert.assertEquals(ErrorMsg.NO_INSERT_OVERWRITE_WITH_ACID.getErrorCode(), cpr.getResponseCode());
    Assert.assertTrue(cpr.getErrorMessage().contains("INSERT OVERWRITE not allowed on table with OutputFormat" +
        " that implements AcidOutputFormat while transaction manager that supports ACID is in use"));

    useDummyTxnManagerTemporarily(conf);
    cpr = driver.compileAndRespond("update T10 set a=0 where b=1");
    Assert.assertEquals(ErrorMsg.ACID_OP_ON_NONACID_TXNMGR.getErrorCode(), cpr.getResponseCode());
    Assert.assertTrue(cpr.getErrorMessage().contains("Attempt to do update or delete using transaction manager that does not support these operations."));

    useDummyTxnManagerTemporarily(conf);
    cpr = driver.compileAndRespond("delete from T10");
    Assert.assertEquals(ErrorMsg.ACID_OP_ON_NONACID_TXNMGR.getErrorCode(), cpr.getResponseCode());
    Assert.assertTrue(cpr.getErrorMessage().contains("Attempt to do update or delete using transaction manager that does not support these operations."));

    conf.setVar(HiveConf.ConfVars.HIVE_TXN_MANAGER, "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
  }

  /**
   * Temporarily set DummyTxnManager as the txn manager for the session.
   * HIVE-10632: we have to do this for every new query, because this jira introduced an AcidEventListener
   * in HiveMetaStore, which will instantiate a txn handler, but due to HIVE-12902, we have to call
   * TxnHandler.setConf and TxnHandler.checkQFileTestHack and TxnDbUtil.setConfValues, which will
   * set txn manager back to DbTxnManager.
   */
  private void useDummyTxnManagerTemporarily(HiveConf hiveConf) throws Exception {
    hiveConf.setVar(HiveConf.ConfVars.HIVE_TXN_MANAGER, "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager");
    txnMgr = SessionState.get().initTxnMgr(hiveConf);
    Assert.assertTrue(txnMgr instanceof DummyTxnManager);
  }

  /**
   * Normally the compaction process will clean up records in TXN_COMPONENTS, COMPLETED_TXN_COMPONENTS,
   * COMPACTION_QUEUE and COMPLETED_COMPACTIONS. But if a table/partition has been dropped before
   * compaction and there are still relevant records in those metastore tables, the Initiator will
   * complain about not being able to find the table/partition. This method is to test and make sure
   * we clean up relevant records as soon as a table/partition is dropped.
   *
   * Note, here we don't need to worry about cleaning up TXNS table, since it's handled separately.
   * @throws Exception
   */
  @Test
  public void testMetastoreTablesCleanup() throws Exception {
    dropTable(new String[] {"temp.T10", "temp.T11", "temp.T12p", "temp.T13p"});

    CommandProcessorResponse cpr = driver.run("create database if not exists temp");
    checkCmdOnDriver(cpr);

    // Create some ACID tables: T10, T11 - unpartitioned table, T12p, T13p - partitioned table
    cpr = driver.run("create table temp.T10 (a int, b int) clustered by(b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    cpr = driver.run("create table temp.T11 (a int, b int) clustered by(b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    cpr = driver.run("create table temp.T12p (a int, b int) partitioned by (ds string, hour string) clustered by(b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    cpr = driver.run("create table temp.T13p (a int, b int) partitioned by (ds string, hour string) clustered by(b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);

    // Successfully insert some data into ACID tables, so that we have records in COMPLETED_TXN_COMPONENTS
    cpr = driver.run("insert into temp.T10 values (1, 1)");
    checkCmdOnDriver(cpr);
    cpr = driver.run("insert into temp.T10 values (2, 2)");
    checkCmdOnDriver(cpr);
    cpr = driver.run("insert into temp.T11 values (3, 3)");
    checkCmdOnDriver(cpr);
    cpr = driver.run("insert into temp.T11 values (4, 4)");
    checkCmdOnDriver(cpr);
    cpr = driver.run("insert into temp.T12p partition (ds='today', hour='1') values (5, 5)");
    checkCmdOnDriver(cpr);
    cpr = driver.run("insert into temp.T12p partition (ds='tomorrow', hour='2') values (6, 6)");
    checkCmdOnDriver(cpr);
    cpr = driver.run("insert into temp.T13p partition (ds='today', hour='1') values (7, 7)");
    checkCmdOnDriver(cpr);
    cpr = driver.run("insert into temp.T13p partition (ds='tomorrow', hour='2') values (8, 8)");
    checkCmdOnDriver(cpr);
    int count = TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS where CTC_DATABASE='temp' and CTC_TABLE in ('t10', 't11')");
    Assert.assertEquals(4, count);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS where CTC_DATABASE='temp' and CTC_TABLE in ('t12p', 't13p')");
    Assert.assertEquals(4, count);

    // Fail some inserts, so that we have records in TXN_COMPONENTS
    conf.setBoolVar(HiveConf.ConfVars.HIVETESTMODEROLLBACKTXN, true);
    cpr = driver.run("insert into temp.T10 values (9, 9)");
    checkCmdOnDriver(cpr);
    cpr = driver.run("insert into temp.T11 values (10, 10)");
    checkCmdOnDriver(cpr);
    cpr = driver.run("insert into temp.T12p partition (ds='today', hour='1') values (11, 11)");
    checkCmdOnDriver(cpr);
    cpr = driver.run("insert into temp.T13p partition (ds='today', hour='1') values (12, 12)");
    checkCmdOnDriver(cpr);
    count = TxnDbUtil.countQueryAgent("select count(*) from TXN_COMPONENTS where TC_DATABASE='temp' and TC_TABLE in ('t10', 't11', 't12p', 't13p')");
    Assert.assertEquals(4, count);
    conf.setBoolVar(HiveConf.ConfVars.HIVETESTMODEROLLBACKTXN, false);

    // Drop a table/partition; corresponding records in TXN_COMPONENTS and COMPLETED_TXN_COMPONENTS should disappear
    count = TxnDbUtil.countQueryAgent("select count(*) from TXN_COMPONENTS where TC_DATABASE='temp' and TC_TABLE='t10'");
    Assert.assertEquals(1, count);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS where CTC_DATABASE='temp' and CTC_TABLE='t10'");
    Assert.assertEquals(2, count);
    cpr = driver.run("drop table temp.T10");
    checkCmdOnDriver(cpr);
    count = TxnDbUtil.countQueryAgent("select count(*) from TXN_COMPONENTS where TC_DATABASE='temp' and TC_TABLE='t10'");
    Assert.assertEquals(0, count);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS where CTC_DATABASE='temp' and CTC_TABLE='t10'");
    Assert.assertEquals(0, count);

    count = TxnDbUtil.countQueryAgent("select count(*) from TXN_COMPONENTS where TC_DATABASE='temp' and TC_TABLE='t12p' and TC_PARTITION='ds=today/hour=1'");
    Assert.assertEquals(1, count);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS where CTC_DATABASE='temp' and CTC_TABLE='t12p' and CTC_PARTITION='ds=today/hour=1'");
    Assert.assertEquals(1, count);
    cpr = driver.run("alter table temp.T12p drop partition (ds='today', hour='1')");
    checkCmdOnDriver(cpr);
    count = TxnDbUtil.countQueryAgent("select count(*) from TXN_COMPONENTS where TC_DATABASE='temp' and TC_TABLE='t12p' and TC_PARTITION='ds=today/hour=1'");
    Assert.assertEquals(0, count);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS where CTC_DATABASE='temp' and CTC_TABLE='t12p' and CTC_PARTITION='ds=today/hour=1'");
    Assert.assertEquals(0, count);

    // Successfully perform compaction on a table/partition, so that we have successful records in COMPLETED_COMPACTIONS
    cpr = driver.run("alter table temp.T11 compact 'minor'");
    checkCmdOnDriver(cpr);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPACTION_QUEUE where CQ_DATABASE='temp' and CQ_TABLE='t11' and CQ_STATE='i' and CQ_TYPE='i'");
    Assert.assertEquals(1, count);
    org.apache.hadoop.hive.ql.TestTxnCommands2.runWorker(conf);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPACTION_QUEUE where CQ_DATABASE='temp' and CQ_TABLE='t11' and CQ_STATE='r' and CQ_TYPE='i'");
    Assert.assertEquals(1, count);
    org.apache.hadoop.hive.ql.TestTxnCommands2.runCleaner(conf);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPACTION_QUEUE where CQ_DATABASE='temp' and CQ_TABLE='t11'");
    Assert.assertEquals(0, count);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_COMPACTIONS where CC_DATABASE='temp' and CC_TABLE='t11' and CC_STATE='s' and CC_TYPE='i'");
    Assert.assertEquals(1, count);

    cpr = driver.run("alter table temp.T12p partition (ds='tomorrow', hour='2') compact 'minor'");
    checkCmdOnDriver(cpr);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPACTION_QUEUE where CQ_DATABASE='temp' and CQ_TABLE='t12p' and CQ_PARTITION='ds=tomorrow/hour=2' and CQ_STATE='i' and CQ_TYPE='i'");
    Assert.assertEquals(1, count);
    org.apache.hadoop.hive.ql.TestTxnCommands2.runWorker(conf);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPACTION_QUEUE where CQ_DATABASE='temp' and CQ_TABLE='t12p' and CQ_PARTITION='ds=tomorrow/hour=2' and CQ_STATE='r' and CQ_TYPE='i'");
    Assert.assertEquals(1, count);
    org.apache.hadoop.hive.ql.TestTxnCommands2.runCleaner(conf);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPACTION_QUEUE where CQ_DATABASE='temp' and CQ_TABLE='t12p'");
    Assert.assertEquals(0, count);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_COMPACTIONS where CC_DATABASE='temp' and CC_TABLE='t12p' and CC_STATE='s' and CC_TYPE='i'");
    Assert.assertEquals(1, count);

    // Fail compaction, so that we have failed records in COMPLETED_COMPACTIONS
    conf.setBoolVar(HiveConf.ConfVars.HIVETESTMODEFAILCOMPACTION, true);
    cpr = driver.run("alter table temp.T11 compact 'major'");
    checkCmdOnDriver(cpr);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPACTION_QUEUE where CQ_DATABASE='temp' and CQ_TABLE='t11' and CQ_STATE='i' and CQ_TYPE='a'");
    Assert.assertEquals(1, count);
    org.apache.hadoop.hive.ql.TestTxnCommands2.runWorker(conf); // will fail
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPACTION_QUEUE where CQ_DATABASE='temp' and CQ_TABLE='t11' and CQ_STATE='i' and CQ_TYPE='a'");
    Assert.assertEquals(0, count);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_COMPACTIONS where CC_DATABASE='temp' and CC_TABLE='t11' and CC_STATE='f' and CC_TYPE='a'");
    Assert.assertEquals(1, count);

    cpr = driver.run("alter table temp.T12p partition (ds='tomorrow', hour='2') compact 'major'");
    checkCmdOnDriver(cpr);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPACTION_QUEUE where CQ_DATABASE='temp' and CQ_TABLE='t12p' and CQ_PARTITION='ds=tomorrow/hour=2' and CQ_STATE='i' and CQ_TYPE='a'");
    Assert.assertEquals(1, count);
    org.apache.hadoop.hive.ql.TestTxnCommands2.runWorker(conf); // will fail
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPACTION_QUEUE where CQ_DATABASE='temp' and CQ_TABLE='t12p' and CQ_PARTITION='ds=tomorrow/hour=2' and CQ_STATE='i' and CQ_TYPE='a'");
    Assert.assertEquals(0, count);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_COMPACTIONS where CC_DATABASE='temp' and CC_TABLE='t12p' and CC_STATE='f' and CC_TYPE='a'");
    Assert.assertEquals(1, count);
    conf.setBoolVar(HiveConf.ConfVars.HIVETESTMODEFAILCOMPACTION, false);

    // Put 2 records into COMPACTION_QUEUE and do nothing
    cpr = driver.run("alter table temp.T11 compact 'major'");
    checkCmdOnDriver(cpr);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPACTION_QUEUE where CQ_DATABASE='temp' and CQ_TABLE='t11' and CQ_STATE='i' and CQ_TYPE='a'");
    Assert.assertEquals(1, count);
    cpr = driver.run("alter table temp.T12p partition (ds='tomorrow', hour='2') compact 'major'");
    checkCmdOnDriver(cpr);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPACTION_QUEUE where CQ_DATABASE='temp' and CQ_TABLE='t12p' and CQ_PARTITION='ds=tomorrow/hour=2' and CQ_STATE='i' and CQ_TYPE='a'");
    Assert.assertEquals(1, count);

    // Drop a table/partition, corresponding records in COMPACTION_QUEUE and COMPLETED_COMPACTIONS should disappear
    cpr = driver.run("drop table temp.T11");
    checkCmdOnDriver(cpr);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPACTION_QUEUE where CQ_DATABASE='temp' and CQ_TABLE='t11'");
    Assert.assertEquals(0, count);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_COMPACTIONS where CC_DATABASE='temp' and CC_TABLE='t11'");
    Assert.assertEquals(0, count);

    cpr = driver.run("alter table temp.T12p drop partition (ds='tomorrow', hour='2')");
    checkCmdOnDriver(cpr);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPACTION_QUEUE where CQ_DATABASE='temp' and CQ_TABLE='t12p'");
    Assert.assertEquals(0, count);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_COMPACTIONS where CC_DATABASE='temp' and CC_TABLE='t12p'");
    Assert.assertEquals(0, count);

    // Put 1 record into COMPACTION_QUEUE and do nothing
    cpr = driver.run("alter table temp.T13p partition (ds='today', hour='1') compact 'major'");
    checkCmdOnDriver(cpr);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPACTION_QUEUE where CQ_DATABASE='temp' and CQ_TABLE='t13p' and CQ_STATE='i' and CQ_TYPE='a'");
    Assert.assertEquals(1, count);

    // Drop database, everything in all 4 meta tables should disappear
    count = TxnDbUtil.countQueryAgent("select count(*) from TXN_COMPONENTS where TC_DATABASE='temp' and TC_TABLE in ('t10', 't11', 't12p', 't13p')");
    Assert.assertEquals(1, count);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS where CTC_DATABASE='temp' and CTC_TABLE in ('t10', 't11', 't12p', 't13p')");
    Assert.assertEquals(2, count);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPACTION_QUEUE where CQ_DATABASE='temp' and CQ_TABLE in ('t10', 't11', 't12p', 't13p')");
    Assert.assertEquals(1, count);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_COMPACTIONS where CC_DATABASE='temp' and CC_TABLE in ('t10', 't11', 't12p', 't13p')");
    Assert.assertEquals(0, count);
    cpr = driver.run("drop database if exists temp cascade");
    checkCmdOnDriver(cpr);
    count = TxnDbUtil.countQueryAgent("select count(*) from TXN_COMPONENTS where TC_DATABASE='temp' and TC_TABLE in ('t10', 't11', 't12p', 't13p')");
    Assert.assertEquals(0, count);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS where CTC_DATABASE='temp' and CTC_TABLE in ('t10', 't11', 't12p', 't13p')");
    Assert.assertEquals(0, count);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPACTION_QUEUE where CQ_DATABASE='temp' and CQ_TABLE in ('t10', 't11', 't12p', 't13p')");
    Assert.assertEquals(0, count);
    count = TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_COMPACTIONS where CC_DATABASE='temp' and CC_TABLE in ('t10', 't11', 't12p', 't13p')");
    Assert.assertEquals(0, count);
  }

  /**
   * collection of queries where we ensure that we get the locks that are expected
   * @throws Exception
   */
  @Test
  public void checkExpectedLocks() throws Exception {
    dropTable(new String[] {"acidPart", "nonAcidPart"});
    CommandProcessorResponse cpr = null;
    cpr = driver.run("create table acidPart(a int, b int) partitioned by (p string) clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    cpr = driver.run("create table nonAcidPart(a int, b int) partitioned by (p string) stored as orc");
    checkCmdOnDriver(cpr);

    cpr = driver.compileAndRespond("insert into nonAcidPart partition(p) values(1,2,3)");
    checkCmdOnDriver(cpr);
    LockState lockState = ((DbTxnManager) txnMgr).acquireLocks(driver.getPlan(), ctx, "Practical", false);
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "nonAcidPart", null, locks);
    List<HiveLock> relLocks = new ArrayList<HiveLock>(1);
    relLocks.add(new DbLockManager.DbHiveLock(locks.get(0).getLockid()));
    txnMgr.getLockManager().releaseLocks(relLocks);

    cpr = driver.compileAndRespond("insert into nonAcidPart partition(p=1) values(5,6)");
    checkCmdOnDriver(cpr);
    lockState = ((DbTxnManager) txnMgr).acquireLocks(driver.getPlan(), ctx, "Practical", false);
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "nonAcidPart", "p=1", locks);
    relLocks = new ArrayList<HiveLock>(1);
    relLocks.add(new DbLockManager.DbHiveLock(locks.get(0).getLockid()));
    txnMgr.getLockManager().releaseLocks(relLocks);

    cpr = driver.compileAndRespond("insert into acidPart partition(p) values(1,2,3)");
    checkCmdOnDriver(cpr);
    lockState = ((DbTxnManager) txnMgr).acquireLocks(driver.getPlan(), ctx, "Practical", false);
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "acidPart", null, locks);
    relLocks = new ArrayList<HiveLock>(1);
    relLocks.add(new DbLockManager.DbHiveLock(locks.get(0).getLockid()));
    txnMgr.getLockManager().releaseLocks(relLocks);

    cpr = driver.compileAndRespond("insert into acidPart partition(p=1) values(5,6)");
    checkCmdOnDriver(cpr);
    lockState = ((DbTxnManager) txnMgr).acquireLocks(driver.getPlan(), ctx, "Practical", false);
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "acidPart", "p=1", locks);
    relLocks = new ArrayList<HiveLock>(1);
    relLocks.add(new DbLockManager.DbHiveLock(locks.get(0).getLockid()));
    txnMgr.getLockManager().releaseLocks(relLocks);

    cpr = driver.compileAndRespond("update acidPart set b = 17 where a = 1");
    checkCmdOnDriver(cpr);
    lockState = ((DbTxnManager) txnMgr).acquireLocks(driver.getPlan(), ctx, "Practical", false);
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "acidPart", null, locks);
    relLocks = new ArrayList<HiveLock>(1);
    relLocks.add(new DbLockManager.DbHiveLock(locks.get(0).getLockid()));
    txnMgr.getLockManager().releaseLocks(relLocks);

    cpr = driver.compileAndRespond("update acidPart set b = 17 where p = 1");
    checkCmdOnDriver(cpr);
    lockState = ((DbTxnManager) txnMgr).acquireLocks(driver.getPlan(), ctx, "Practical", false);
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "acidPart", null, locks);//https://issues.apache.org/jira/browse/HIVE-13212
    relLocks = new ArrayList<HiveLock>(1);
    relLocks.add(new DbLockManager.DbHiveLock(locks.get(0).getLockid()));
    txnMgr.getLockManager().releaseLocks(relLocks);
  }
  /**
   * Check to make sure we acquire proper locks for queries involving acid and non-acid tables
   */
  @Test
  public void checkExpectedLocks2() throws Exception {
    dropTable(new String[] {"tab_acid", "tab_not_acid"});
    checkCmdOnDriver(driver.run("create table if not exists tab_acid (a int, b int) partitioned by (p string) " +
      "clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')"));
    checkCmdOnDriver(driver.run("create table if not exists tab_not_acid (na int, nb int) partitioned by (np string) " +
      "clustered by (na) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='false')"));
    checkCmdOnDriver(driver.run("insert into tab_acid partition(p) (a,b,p) values(1,2,'foo'),(3,4,'bar')"));
    checkCmdOnDriver(driver.run("insert into tab_not_acid partition(np) (na,nb,np) values(1,2,'blah'),(3,4,'doh')"));
    txnMgr.openTxn(ctx, "T1");
    checkCmdOnDriver(driver.compileAndRespond("select * from tab_acid inner join tab_not_acid on a = na"));
    txnMgr.acquireLocks(driver.getPlan(), ctx, "T1");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 6, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_acid", null, locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_acid", "p=bar", locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_acid", "p=foo", locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_not_acid", null, locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_not_acid", "np=blah", locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_not_acid", "np=doh", locks);

    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnMgr2.openTxn(ctx, "T2");
    checkCmdOnDriver(driver.compileAndRespond("insert into tab_not_acid partition(np='doh') values(5,6)"));
    LockState ls = ((DbTxnManager)txnMgr2).acquireLocks(driver.getPlan(), ctx, "T2", false);
    locks = getLocks(txnMgr2);
    Assert.assertEquals("Unexpected lock count", 7, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_acid", null, locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_acid", "p=bar", locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_acid", "p=foo", locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_not_acid", null, locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_not_acid", "np=blah", locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_not_acid", "np=doh", locks);
    checkLock(LockType.EXCLUSIVE, LockState.WAITING, "default", "tab_not_acid", "np=doh", locks);

    // Test strict locking mode, i.e. backward compatible locking mode for non-ACID resources.
    // With non-strict mode, INSERT got SHARED_READ lock, instead of EXCLUSIVE with ACID semantics
    conf.setBoolVar(HiveConf.ConfVars.HIVE_TXN_STRICT_LOCKING_MODE, false);
    HiveTxnManager txnMgr3 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnMgr3.openTxn(ctx, "T3");
    checkCmdOnDriver(driver.compileAndRespond("insert into tab_not_acid partition(np='blah') values(7,8)"));
    ((DbTxnManager)txnMgr3).acquireLocks(driver.getPlan(), ctx, "T3", false);
    locks = getLocks(txnMgr3);
    Assert.assertEquals("Unexpected lock count", 8, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_acid", null, locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_acid", "p=bar", locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_acid", "p=foo", locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_not_acid", null, locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_not_acid", "np=blah", locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_not_acid", "np=doh", locks);
    checkLock(LockType.EXCLUSIVE, LockState.WAITING, "default", "tab_not_acid", "np=doh", locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_not_acid", "np=blah", locks);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_TXN_STRICT_LOCKING_MODE, true);
  }

  /** The list is small, and the object is generated, so we don't use sets/equals/etc. */
  public static ShowLocksResponseElement checkLock(LockType expectedType, LockState expectedState, String expectedDb,
      String expectedTable, String expectedPartition, List<ShowLocksResponseElement> actuals) {
    for (ShowLocksResponseElement actual : actuals) {
      if (expectedType == actual.getType() && expectedState == actual.getState()
          && StringUtils.equals(normalizeCase(expectedDb), normalizeCase(actual.getDbname()))
          && StringUtils.equals(normalizeCase(expectedTable), normalizeCase(actual.getTablename()))
          && StringUtils.equals(
              normalizeCase(expectedPartition), normalizeCase(actual.getPartname()))) {
        return actual;
      }
    }
    Assert.fail("Could't find {" + expectedType + ", " + expectedState + ", " + expectedDb
       + ", " + expectedTable  + ", " + expectedPartition + "} in " + actuals);
    throw new IllegalStateException("How did it get here?!");
  }

  @Test
  public void testShowLocksFilterOptions() throws Exception {
    CommandProcessorResponse cpr = driver.run("drop table if exists db1.t14");
    checkCmdOnDriver(cpr);
    cpr = driver.run("drop table if exists db2.t14"); // Note that db1 and db2 have a table with common name
    checkCmdOnDriver(cpr);
    cpr = driver.run("drop table if exists db2.t15");
    checkCmdOnDriver(cpr);
    cpr = driver.run("drop table if exists db2.t16");
    checkCmdOnDriver(cpr);
    cpr = driver.run("drop database if exists db1");
    checkCmdOnDriver(cpr);
    cpr = driver.run("drop database if exists db2");
    checkCmdOnDriver(cpr);

    cpr = driver.run("create database if not exists db1");
    checkCmdOnDriver(cpr);
    cpr = driver.run("create database if not exists db2");
    checkCmdOnDriver(cpr);
    cpr = driver.run("create table if not exists db1.t14 (a int, b int) partitioned by (ds string) clustered by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    cpr = driver.run("create table if not exists db2.t14 (a int, b int) clustered by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    cpr = driver.run("create table if not exists db2.t15 (a int, b int) clustered by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    cpr = driver.run("create table if not exists db2.t16 (a int, b int) clustered by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);

    // Acquire different locks at different levels

    cpr = driver.compileAndRespond("insert into table db1.t14 partition (ds='today') values (1, 2)");
    checkCmdOnDriver(cpr);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Tom");

    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    cpr = driver.compileAndRespond("insert into table db1.t14 partition (ds='tomorrow') values (3, 4)");
    checkCmdOnDriver(cpr);
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "Jerry");

    HiveTxnManager txnMgr3 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    cpr = driver.compileAndRespond("select * from db2.t15");
    checkCmdOnDriver(cpr);
    txnMgr3.acquireLocks(driver.getPlan(), ctx, "Donald");

    HiveTxnManager txnMgr4 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    cpr = driver.compileAndRespond("select * from db2.t16");
    checkCmdOnDriver(cpr);
    txnMgr4.acquireLocks(driver.getPlan(), ctx, "Hillary");

    HiveTxnManager txnMgr5 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    cpr = driver.compileAndRespond("select * from db2.t14");
    checkCmdOnDriver(cpr);
    txnMgr5.acquireLocks(driver.getPlan(), ctx, "Obama");

    // Simulate SHOW LOCKS with different filter options

    // SHOW LOCKS (no filter)
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 5, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "db1", "t14", "ds=today", locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "db1", "t14", "ds=tomorrow", locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "db2", "t15", null, locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "db2", "t16", null, locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "db2", "t14", null, locks);

    // SHOW LOCKS db2
    locks = getLocksWithFilterOptions(txnMgr3, "db2", null, null);
    Assert.assertEquals("Unexpected lock count", 3, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "db2", "t15", null, locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "db2", "t16", null, locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "db2", "t14", null, locks);

    // SHOW LOCKS t14
    cpr = driver.run("use db1");
    checkCmdOnDriver(cpr);
    locks = getLocksWithFilterOptions(txnMgr, null, "t14", null);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "db1", "t14", "ds=today", locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "db1", "t14", "ds=tomorrow", locks);
    // Note that it shouldn't show t14 from db2

    // SHOW LOCKS t14 PARTITION ds='today'
    Map<String, String> partSpec = new HashMap<String, String>();
    partSpec.put("ds", "today");
    locks = getLocksWithFilterOptions(txnMgr, null, "t14", partSpec);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "db1", "t14", "ds=today", locks);

    // SHOW LOCKS t15
    cpr = driver.run("use db2");
    checkCmdOnDriver(cpr);
    locks = getLocksWithFilterOptions(txnMgr3, null, "t15", null);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "db2", "t15", null, locks);
  }

  private void checkCmdOnDriver(CommandProcessorResponse cpr) {
    Assert.assertTrue(cpr.toString(), cpr.getResponseCode() == 0);
  }
  private static String normalizeCase(String s) {
    return s == null ? null : s.toLowerCase();
  }

  private List<ShowLocksResponseElement> getLocks() throws Exception {
    return getLocks(txnMgr);
  }
  private List<ShowLocksResponseElement> getLocks(HiveTxnManager txnMgr) throws Exception {
    ShowLocksResponse rsp = ((DbLockManager)txnMgr.getLockManager()).getLocks();
    return rsp.getLocks();
  }
    /**
   * txns update same resource but do not overlap in time - no conflict
   */
  @Test
  public void testWriteSetTracking1() throws Exception {
    dropTable(new String[] {"TAB_PART"});
    CommandProcessorResponse cpr = driver.run("create table if not exists TAB_PART (a int, b int) " +
      "partitioned by (p string) clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);

    checkCmdOnDriver(driver.compileAndRespond("select * from TAB_PART"));
    txnMgr.openTxn(ctx, "Nicholas");
    checkCmdOnDriver(driver.compileAndRespond("update TAB_PART set b = 7 where p = 'blah'"));
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Nicholas");
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnMgr.commitTxn();
    txnMgr2.openTxn(ctx, "Alexandra");
    checkCmdOnDriver(driver.compileAndRespond("update TAB_PART set b = 7 where p = 'blah'"));
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "Nicholas");
    txnMgr2.commitTxn();
  }
  private void dropTable(String[] tabs) throws Exception {
    for(String tab : tabs) {
      driver.run("drop table if exists " + tab);
    }
  }
  /**
   * txns overlap in time but do not update same resource - no conflict
   */
  @Test
  public void testWriteSetTracking2() throws Exception {
    dropTable(new String[] {"TAB_PART", "TAB2"});
    CommandProcessorResponse cpr = driver.run("create table if not exists TAB_PART (a int, b int) " +
      "partitioned by (p string) clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    cpr = driver.run("create table if not exists TAB2 (a int, b int) partitioned by (p string) " +
      "clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);

    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnMgr.openTxn(ctx, "Peter");
    checkCmdOnDriver(driver.compileAndRespond("update TAB_PART set b = 7 where p = 'blah'"));
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Peter");
    txnMgr2.openTxn(ctx, "Catherine");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    //note that "update" uses dynamic partitioning thus lock is on the table not partition
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB_PART", null, locks);
    txnMgr.commitTxn();
    checkCmdOnDriver(driver.compileAndRespond("update TAB2 set b = 9 where p = 'doh'"));
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "Catherine");
    txnMgr2.commitTxn();
  }
  /**
   * txns overlap and update the same resource - can't commit 2nd txn
   */
  @Test
  public void testWriteSetTracking3() throws Exception {
    dropTable(new String[] {"TAB_PART"});
    CommandProcessorResponse cpr = driver.run("create table if not exists TAB_PART (a int, b int) " +
      "partitioned by (p string) clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    checkCmdOnDriver(driver.run("insert into TAB_PART partition(p='blah') values(1,2)"));
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);

    long txnId = txnMgr.openTxn(ctx, "Known");
    long txnId2 = txnMgr2.openTxn(ctx, "Unknown");
    checkCmdOnDriver(driver.compileAndRespond("update TAB_PART set b = 7 where p = 'blah'"));
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Known");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB_PART", "p=blah", locks);
    checkCmdOnDriver(driver.compileAndRespond("update TAB_PART set b = 7 where p = 'blah'"));
    ((DbTxnManager)txnMgr2).acquireLocks(driver.getPlan(), ctx, "Unknown", false);
    locks = getLocks(txnMgr2);//should not matter which txnMgr is used here
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB_PART", "p=blah", locks);
    checkLock(LockType.SHARED_WRITE, LockState.WAITING, "default", "TAB_PART", "p=blah", locks);
    AddDynamicPartitions adp = new AddDynamicPartitions(txnId, "default", "TAB_PART",
      Collections.singletonList("p=blah"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr.commitTxn();
    
    adp.setTxnid(txnId2);
    txnHandler.addDynamicPartitions(adp);
    LockException expectedException = null;
    try {
      //with HIVE-15032 this should use static parts and thus not need addDynamicPartitions
      txnMgr2.commitTxn();
    }
    catch (LockException e) {
      expectedException = e;
    }
    Assert.assertTrue("Didn't get exception", expectedException != null);
    Assert.assertEquals("Got wrong message code", ErrorMsg.TXN_ABORTED, expectedException.getCanonicalErrorMsg());
    Assert.assertEquals("Exception msg didn't match",
      "Aborting [txnid:3,3] due to a write conflict on default/TAB_PART/p=blah committed by [txnid:2,3] u/u",
      expectedException.getCause().getMessage());
  }
  /**
   * txns overlap, update same resource, simulate multi-stmt txn case
   * Also tests that we kill txn when it tries to acquire lock if we already know it will not be committed
   */
  @Test
  public void testWriteSetTracking4() throws Exception {
    dropTable(new String[] {"TAB_PART", "TAB2"});
    Assert.assertEquals(0, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET"));
    CommandProcessorResponse cpr = driver.run("create table if not exists TAB_PART (a int, b int) " +
      "partitioned by (p string) clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    cpr = driver.run("create table if not exists TAB2 (a int, b int) partitioned by (p string) " +
      "clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    
    txnMgr.openTxn(ctx, "Long Running");
    checkCmdOnDriver(driver.compileAndRespond("select a from  TAB_PART where p = 'blah'"));
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Long Running");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    //for some reason this just locks the table; if I alter table to add this partition, then 
    //we end up locking both table and partition with share_read.  (Plan has 2 ReadEntities)...?
    //same for other locks below
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "TAB_PART", null, locks);

    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnMgr2.openTxn(ctx, "Short Running");
    checkCmdOnDriver(driver.compileAndRespond("update TAB2 set b = 7 where p = 'blah'"));//no such partition
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "Short Running");
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "TAB_PART", null, locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB2", null, locks);
    //update stmt has p=blah, thus nothing is actually update and we generate empty dyn part list
    Assert.assertEquals(0, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET"));
    AddDynamicPartitions adp = new AddDynamicPartitions(txnMgr2.getCurrentTxnId(),
      "default", "tab2", Collections.EMPTY_LIST);
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr2.commitTxn();
    //Short Running updated nothing, so we expect 0 rows in WRITE_SET
    Assert.assertEquals( 0, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET"));

    txnMgr2.openTxn(ctx, "T3");
    checkCmdOnDriver(driver.compileAndRespond("update TAB2 set b = 7 where p = 'two'"));//pretend this partition exists
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "T3");
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "TAB_PART", null, locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB2", null, locks);//since TAB2 is empty
    //update stmt has p=blah, thus nothing is actually update and we generate empty dyn part list
    Assert.assertEquals(0, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET"));
    adp = new AddDynamicPartitions(txnMgr2.getCurrentTxnId(),
      "default", "tab2", Collections.singletonList("p=two"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);//simulate partition update
    txnMgr2.commitTxn();
    Assert.assertEquals("WRITE_SET mismatch: " + TxnDbUtil.queryToString("select * from WRITE_SET"),
      1, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET"));
    
    AcidWriteSetService houseKeeper = new AcidWriteSetService();
    TestTxnCommands2.runHouseKeeperService(houseKeeper, conf);
    //since T3 overlaps with Long Running (still open) GC does nothing
    Assert.assertEquals(1, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET"));
    checkCmdOnDriver(driver.compileAndRespond("update TAB2 set b = 17 where a = 1"));//no rows match
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Long Running");
    //so generate empty Dyn Part call
    adp = new AddDynamicPartitions(txnMgr.getCurrentTxnId(),
      "default", "tab2", Collections.EMPTY_LIST);
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);     
    txnMgr.commitTxn();

    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 0, locks.size());
    TestTxnCommands2.runHouseKeeperService(houseKeeper, conf);
    Assert.assertEquals(0, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET"));
  }
  /**
   * overlapping txns updating the same resource but 1st one rolls back; 2nd commits
   * @throws Exception
   */
  @Test
  public void testWriteSetTracking5() throws Exception {
    dropTable(new String[] {"TAB_PART"});
    Assert.assertEquals(0, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET"));
    CommandProcessorResponse cpr = driver.run("create table if not exists TAB_PART (a int, b int) " +
      "partitioned by (p string) clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    checkCmdOnDriver(driver.run("insert into TAB_PART partition(p='blah') values(1,2)"));
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);

    txnMgr.openTxn(ctx, "Known");
    long txnId = txnMgr2.openTxn(ctx, "Unknown");
    checkCmdOnDriver(driver.compileAndRespond("update TAB_PART set b = 7 where p = 'blah'"));
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Known");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB_PART", "p=blah", locks);
    checkCmdOnDriver(driver.compileAndRespond("update TAB_PART set b = 7 where p = 'blah'"));
    ((DbTxnManager)txnMgr2).acquireLocks(driver.getPlan(), ctx, "Unknown", false);
    locks = getLocks(txnMgr2);//should not matter which txnMgr is used here
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB_PART", "p=blah", locks);
    checkLock(LockType.SHARED_WRITE, LockState.WAITING, "default", "TAB_PART", "p=blah", locks);
    txnMgr.rollbackTxn();

    AddDynamicPartitions adp = new AddDynamicPartitions(txnId, "default", "TAB_PART",
      Arrays.asList("p=blah"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    Assert.assertEquals(0, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET"));
    txnMgr2.commitTxn();//since conflicting txn rolled back, commit succeeds
    Assert.assertEquals(1, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET"));
  }
  /**
   * check that read query concurrent with txn works ok
   */
  @Test
  public void testWriteSetTracking6() throws Exception {
    dropTable(new String[] {"TAB2"});
    Assert.assertEquals(0, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET"));
    CommandProcessorResponse cpr = driver.run("create table if not exists TAB2(a int, b int) clustered " +
      "by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    checkCmdOnDriver(driver.compileAndRespond("select * from TAB2 where a = 113"));
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Works");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "TAB2", null, locks);
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnMgr2.openTxn(ctx, "Horton");
    checkCmdOnDriver(driver.compileAndRespond("update TAB2 set b = 17 where a = 101"));
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "Horton");
    Assert.assertEquals(0, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET"));
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "TAB2", null, locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB2", null, locks);
    txnMgr2.commitTxn();//no conflict
    Assert.assertEquals(1, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET"));
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "TAB2", null, locks);
    TestTxnCommands2.runHouseKeeperService(new AcidWriteSetService(), conf);
    Assert.assertEquals(0, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET"));
  }

  /**
   * 2 concurrent txns update different partitions of the same table and succeed
   * @throws Exception
   */
  @Test
  public void testWriteSetTracking7() throws Exception {
    dropTable(new String[] {"tab2", "TAB2"});
    Assert.assertEquals(0, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET"));
    CommandProcessorResponse cpr = driver.run("create table if not exists tab2 (a int, b int) " +
      "partitioned by (p string) clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    checkCmdOnDriver(driver.run("insert into tab2 partition(p)(a,b,p) values(1,1,'one'),(2,2,'two')"));//txnid:1
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);

    //test with predicates such that partition pruning works
    txnMgr2.openTxn(ctx, "T2");
    checkCmdOnDriver(driver.compileAndRespond("update tab2 set b = 7 where p='two'"));
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "T2");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr2);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB2", "p=two", locks);

    //now start concurrent txn
    txnMgr.openTxn(ctx, "T3");
    checkCmdOnDriver(driver.compileAndRespond("update tab2 set b = 7 where p='one'"));
    ((DbTxnManager)txnMgr).acquireLocks(driver.getPlan(), ctx, "T3", false);
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB2", "p=two", locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB2", "p=one", locks);
    
    //this simulates the completion of txnid:2
    AddDynamicPartitions adp = new AddDynamicPartitions(txnMgr2.getCurrentTxnId(), "default", "tab2",
      Collections.singletonList("p=two"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr2.commitTxn();//txnid:2
    
    locks = getLocks(txnMgr2);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB2", "p=one", locks);
    //completion of txnid:3
    adp = new AddDynamicPartitions(txnMgr.getCurrentTxnId(), "default", "tab2",
      Collections.singletonList("p=one"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr.commitTxn();//txnid:3
    //now both txns concurrently updated TAB2 but different partitions.
    
    Assert.assertEquals("WRITE_SET mismatch: " + TxnDbUtil.queryToString("select * from WRITE_SET"),
      1, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET where ws_partition='p=one' and ws_operation_type='u'"));
    Assert.assertEquals("WRITE_SET mismatch: " + TxnDbUtil.queryToString("select * from WRITE_SET"),
      1, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET where ws_partition='p=two' and ws_operation_type='u'"));
    //2 from txnid:1, 1 from txnid:2, 1 from txnid:3
    Assert.assertEquals("COMPLETED_TXN_COMPONENTS mismatch: " + TxnDbUtil.queryToString("select * from COMPLETED_TXN_COMPONENTS"),
      4, TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS where ctc_table='tab2' and ctc_partition is not null"));
    
    //================
    //test with predicates such that partition pruning doesn't kick in
    cpr = driver.run("create table if not exists tab1 (a int, b int) partitioned by (p string) " +
      "clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    checkCmdOnDriver(driver.run("insert into tab1 partition(p)(a,b,p) values(1,1,'one'),(2,2,'two')"));//txnid:4
    txnMgr2.openTxn(ctx, "T5");
    checkCmdOnDriver(driver.compileAndRespond("update tab1 set b = 7 where b=1"));
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "T5");
    locks = getLocks(txnMgr2);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);

    //now start concurrent txn
    txnMgr.openTxn(ctx, "T6");
    checkCmdOnDriver(driver.compileAndRespond("update tab1 set b = 7 where b = 2"));
    ((DbTxnManager)txnMgr).acquireLocks(driver.getPlan(), ctx, "T6", false);
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 4, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);
    checkLock(LockType.SHARED_WRITE, LockState.WAITING, "default", "TAB1", "p=two", locks);
    checkLock(LockType.SHARED_WRITE, LockState.WAITING, "default", "TAB1", "p=one", locks);

    //this simulates the completion of txnid:5
    adp = new AddDynamicPartitions(txnMgr2.getCurrentTxnId(), "default", "tab1",
      Collections.singletonList("p=one"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr2.commitTxn();//txnid:5

    ((DbLockManager)txnMgr.getLockManager()).checkLock(locks.get(2).getLockid());//retest WAITING locks (both have same ext id)
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);
    //completion of txnid:6
    adp = new AddDynamicPartitions(txnMgr.getCurrentTxnId(), "default", "tab1",
      Collections.singletonList("p=two"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr.commitTxn();//txnid:6

    Assert.assertEquals("WRITE_SET mismatch: " + TxnDbUtil.queryToString("select * from WRITE_SET"),
      1, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET where ws_partition='p=one' and ws_operation_type='u' and ws_table='tab1'"));
    Assert.assertEquals("WRITE_SET mismatch: " + TxnDbUtil.queryToString("select * from WRITE_SET"),
      1, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET where ws_partition='p=two' and ws_operation_type='u' and ws_table='tab1'"));
    //2 from insert + 1 for each update stmt
    Assert.assertEquals("COMPLETED_TXN_COMPONENTS mismatch: " + TxnDbUtil.queryToString("select * from COMPLETED_TXN_COMPONENTS"),
      4, TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS where ctc_table='tab1' and ctc_partition is not null"));
  }
  /**
   * Concurrent updates with partition pruning predicate and w/o one
   */
  @Test
  public void testWriteSetTracking8() throws Exception {
    dropTable(new String[] {"tab1", "TAB1"});
    CommandProcessorResponse cpr = driver.run("create table if not exists tab1 (a int, b int) partitioned by (p string) " +
      "clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    checkCmdOnDriver(driver.run("insert into tab1 partition(p)(a,b,p) values(1,1,'one'),(2,2,'two')"));//txnid:1
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnMgr2.openTxn(ctx, "T2");
    checkCmdOnDriver(driver.compileAndRespond("update tab1 set b = 7 where b=1"));
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "T2");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr2);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);

    //now start concurrent txn
    txnMgr.openTxn(ctx, "T3");
    checkCmdOnDriver(driver.compileAndRespond("update tab1 set b = 7 where p='two'"));
    ((DbTxnManager)txnMgr).acquireLocks(driver.getPlan(), ctx, "T3", false);
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 3, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);
    checkLock(LockType.SHARED_WRITE, LockState.WAITING, "default", "TAB1", "p=two", locks);

    //this simulates the completion of txnid:2
    AddDynamicPartitions adp = new AddDynamicPartitions(txnMgr2.getCurrentTxnId(), "default", "tab1",
      Collections.singletonList("p=one"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr2.commitTxn();//txnid:2

    ((DbLockManager)txnMgr.getLockManager()).checkLock(locks.get(2).getLockid());//retest WAITING locks (both have same ext id)
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    //completion of txnid:3
    adp = new AddDynamicPartitions(txnMgr.getCurrentTxnId(), "default", "tab1",
      Collections.singletonList("p=two"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr.commitTxn();//txnid:3

    Assert.assertEquals("WRITE_SET mismatch: " + TxnDbUtil.queryToString("select * from WRITE_SET"),
      1, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET where ws_partition='p=one' and ws_operation_type='u' and ws_table='tab1'"));
    Assert.assertEquals("WRITE_SET mismatch: " + TxnDbUtil.queryToString("select * from WRITE_SET"),
      1, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET where ws_partition='p=two' and ws_operation_type='u' and ws_table='tab1'"));
    Assert.assertEquals("COMPLETED_TXN_COMPONENTS mismatch: " + TxnDbUtil.queryToString("select * from COMPLETED_TXN_COMPONENTS"),
      4, TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS where ctc_table='tab1' and ctc_partition is not null"));
  }
  /**
   * Concurrent update/delete of different partitions - should pass
   */
  @Test
  public void testWriteSetTracking9() throws Exception {
    dropTable(new String[] {"TAB1"});
    CommandProcessorResponse cpr = driver.run("create table if not exists tab1 (a int, b int) partitioned by (p string) " +
      "clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    checkCmdOnDriver(driver.run("insert into tab1 partition(p)(a,b,p) values(1,1,'one'),(2,2,'two')"));//txnid:1
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnMgr2.openTxn(ctx, "T2");
    checkCmdOnDriver(driver.compileAndRespond("update tab1 set b = 7 where b=1"));
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "T2");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr2);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);

    //now start concurrent txn
    txnMgr.openTxn(ctx, "T3");
    checkCmdOnDriver(driver.compileAndRespond("delete from tab1 where p='two' and b=2"));
    ((DbTxnManager)txnMgr).acquireLocks(driver.getPlan(), ctx, "T3", false);
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 3, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);
    checkLock(LockType.SHARED_WRITE, LockState.WAITING, "default", "TAB1", "p=two", locks);

    //this simulates the completion of txnid:2
    AddDynamicPartitions adp = new AddDynamicPartitions(txnMgr2.getCurrentTxnId(), "default", "tab1",
      Collections.singletonList("p=one"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr2.commitTxn();//txnid:2

    ((DbLockManager)txnMgr.getLockManager()).checkLock(locks.get(2).getLockid());//retest WAITING locks (both have same ext id)
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    //completion of txnid:3
    adp = new AddDynamicPartitions(txnMgr.getCurrentTxnId(), "default", "tab1",
      Collections.singletonList("p=two"));
    adp.setOperationType(DataOperationType.DELETE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr.commitTxn();//txnid:3

    Assert.assertEquals("WRITE_SET mismatch: " + TxnDbUtil.queryToString("select * from COMPLETED_TXN_COMPONENTS"),
      2, TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS where ctc_txnid=1  and ctc_table='tab1'"));
    Assert.assertEquals("WRITE_SET mismatch: " + TxnDbUtil.queryToString("select * from COMPLETED_TXN_COMPONENTS"),
      1, TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS where ctc_txnid=2  and ctc_table='tab1' and ctc_partition='p=one'"));
    Assert.assertEquals("WRITE_SET mismatch: " + TxnDbUtil.queryToString("select * from COMPLETED_TXN_COMPONENTS"),
      1, TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS where ctc_txnid=3  and ctc_table='tab1' and ctc_partition='p=two'"));
    Assert.assertEquals("WRITE_SET mismatch: " + TxnDbUtil.queryToString("select * from WRITE_SET"),
      1, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET where ws_partition='p=one' and ws_operation_type='u' and ws_table='tab1'"));
    Assert.assertEquals("WRITE_SET mismatch: " + TxnDbUtil.queryToString("select * from WRITE_SET"),
      1, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET where ws_partition='p=two' and ws_operation_type='d' and ws_table='tab1'"));
    Assert.assertEquals("COMPLETED_TXN_COMPONENTS mismatch: " + TxnDbUtil.queryToString("select * from COMPLETED_TXN_COMPONENTS"),
      4, TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS where ctc_table='tab1' and ctc_partition is not null"));
  }
  /**
   * Concurrent update/delete of same partition - should fail to commit
   */
  @Test
  public void testWriteSetTracking10() throws Exception {
    dropTable(new String[] {"TAB1"});
    CommandProcessorResponse cpr = driver.run("create table if not exists tab1 (a int, b int) partitioned by (p string) " +
      "clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    checkCmdOnDriver(driver.run("insert into tab1 partition(p)(a,b,p) values(1,1,'one'),(2,2,'two')"));//txnid:1
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnMgr2.openTxn(ctx, "T2");
    checkCmdOnDriver(driver.compileAndRespond("update tab1 set b = 7 where b=2"));
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "T2");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr2);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);

    //now start concurrent txn
    txnMgr.openTxn(ctx, "T3");
    checkCmdOnDriver(driver.compileAndRespond("delete from tab1 where p='two' and b=2"));
    ((DbTxnManager)txnMgr).acquireLocks(driver.getPlan(), ctx, "T3", false);
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 3, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);
    checkLock(LockType.SHARED_WRITE, LockState.WAITING, "default", "TAB1", "p=two", locks);

    //this simulates the completion of txnid:2
    AddDynamicPartitions adp = new AddDynamicPartitions(txnMgr2.getCurrentTxnId(), "default", "tab1",
      Collections.singletonList("p=two"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr2.commitTxn();//txnid:2

    ((DbLockManager)txnMgr.getLockManager()).checkLock(locks.get(2).getLockid());//retest WAITING locks (both have same ext id)
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    //completion of txnid:3
    adp = new AddDynamicPartitions(txnMgr.getCurrentTxnId(), "default", "tab1",
      Collections.singletonList("p=two"));
    adp.setOperationType(DataOperationType.DELETE);
    txnHandler.addDynamicPartitions(adp);
    LockException exception = null;
    try {
      txnMgr.commitTxn();//txnid:3
    }
    catch(LockException e) {
      exception = e;
    }
    Assert.assertNotEquals("Expected exception", null, exception);
    Assert.assertEquals("Exception msg doesn't match",
      "Aborting [txnid:3,3] due to a write conflict on default/tab1/p=two committed by [txnid:2,3] d/u",
      exception.getCause().getMessage());

    Assert.assertEquals("WRITE_SET mismatch: " + TxnDbUtil.queryToString("select * from WRITE_SET"),
      1, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET where ws_partition='p=two' and ws_operation_type='u' and ws_table='tab1'"));
    Assert.assertEquals("COMPLETED_TXN_COMPONENTS mismatch: " + TxnDbUtil.queryToString("select * from COMPLETED_TXN_COMPONENTS"),
      3, TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS where ctc_table='tab1' and ctc_partition is not null"));
  }
  /**
   * Concurrent delte/detele of same partition - should pass
   */
  @Test
  public void testWriteSetTracking11() throws Exception {
    dropTable(new String[] {"TAB1"});
    CommandProcessorResponse cpr = driver.run("create table if not exists tab1 (a int, b int) partitioned by (p string) " +
      "clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    checkCmdOnDriver(driver.run("insert into tab1 partition(p)(a,b,p) values(1,1,'one'),(2,2,'two')"));//txnid:1
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnMgr2.openTxn(ctx, "T2");
    checkCmdOnDriver(driver.compileAndRespond("delete from tab1 where b=2"));
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "T2");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr2);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);

    //now start concurrent txn
    txnMgr.openTxn(ctx, "T3");
    checkCmdOnDriver(driver.compileAndRespond("select * from tab1 where b=1 and p='one'"));
    ((DbTxnManager)txnMgr).acquireLocks(driver.getPlan(), ctx, "T3", false);
    checkCmdOnDriver(driver.compileAndRespond("delete from tab1 where p='two' and b=2"));
    ((DbTxnManager)txnMgr).acquireLocks(driver.getPlan(), ctx, "T3", false);
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 5, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "TAB1", null, locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);
    checkLock(LockType.SHARED_WRITE, LockState.WAITING, "default", "TAB1", "p=two", locks);

    //this simulates the completion of txnid:2
    AddDynamicPartitions adp = new AddDynamicPartitions(txnMgr2.getCurrentTxnId(), "default", "tab1",
      Collections.singletonList("p=two"));
    adp.setOperationType(DataOperationType.DELETE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr2.commitTxn();//txnid:2

    ((DbLockManager)txnMgr.getLockManager()).checkLock(locks.get(4).getLockid());//retest WAITING locks (both have same ext id)
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 3, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "TAB1", null, locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    //completion of txnid:3
    adp = new AddDynamicPartitions(txnMgr.getCurrentTxnId(), "default", "tab1",
      Collections.singletonList("p=two"));
    adp.setOperationType(DataOperationType.DELETE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr.commitTxn();//txnid:3

    Assert.assertEquals("WRITE_SET mismatch: " + TxnDbUtil.queryToString("select * from WRITE_SET"),
      1, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET where ws_partition='p=two' and ws_operation_type='d' and ws_table='tab1' and ws_txnid=2"));
    Assert.assertEquals("WRITE_SET mismatch: " + TxnDbUtil.queryToString("select * from WRITE_SET"),
      1, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET where ws_partition='p=two' and ws_operation_type='d' and ws_table='tab1' and ws_txnid=3"));
    Assert.assertEquals("WRITE_SET mismatch: " + TxnDbUtil.queryToString("select * from WRITE_SET"),
      1, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET where ws_partition='p=two' and ws_operation_type='d' and ws_table='tab1' and ws_txnid=2"));
    Assert.assertEquals("WRITE_SET mismatch: " + TxnDbUtil.queryToString("select * from WRITE_SET"),
      1, TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET where ws_partition='p=two' and ws_operation_type='d' and ws_table='tab1' and ws_txnid=3"));
    Assert.assertEquals("COMPLETED_TXN_COMPONENTS mismatch: " + TxnDbUtil.queryToString("select * from COMPLETED_TXN_COMPONENTS"),
      4, TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS where ctc_table='tab1' and ctc_partition is not null"));
  }
  @Test
  public void testCompletedTxnComponents() throws Exception {
    dropTable(new String[] {"TAB1", "tab_not_acid2"});
    CommandProcessorResponse cpr = driver.run("create table if not exists tab1 (a int, b int) partitioned by (p string) " +
      "clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    cpr = driver.run("create table if not exists tab_not_acid2 (a int, b int)");
    checkCmdOnDriver(cpr);
    checkCmdOnDriver(driver.run("insert into tab_not_acid2 values(1,1),(2,2)"));
    //writing both acid and non-acid resources in the same txn
    checkCmdOnDriver(driver.run("from tab_not_acid2 insert into tab1 partition(p='two')(a,b) select a,b insert into tab_not_acid2(a,b) select a,b "));//txnid:1
    Assert.assertEquals(TxnDbUtil.queryToString("select * from COMPLETED_TXN_COMPONENTS"),
      1, TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS"));
    //only expect transactional components to be in COMPLETED_TXN_COMPONENTS
    Assert.assertEquals(TxnDbUtil.queryToString("select * from COMPLETED_TXN_COMPONENTS"),
      1, TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS where ctc_txnid=1 and ctc_table='tab1'"));
  }

  /**
   * ToDo: multi-insert into txn table and non-tx table should be prevented
   */
  @Test
  public void testMultiInsert() throws Exception {
    dropTable(new String[] {"TAB1", "tab_not_acid"});
    checkCmdOnDriver(driver.run("drop table if exists tab1"));
    checkCmdOnDriver(driver.run("drop table if exists tab_not_acid"));
    CommandProcessorResponse cpr = driver.run("create table if not exists tab1 (a int, b int) partitioned by (p string) " +
      "clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    cpr = driver.run("create table if not exists tab_not_acid (a int, b int, p string)");
    checkCmdOnDriver(cpr);
    checkCmdOnDriver(driver.run("insert into tab_not_acid values(1,1,'one'),(2,2,'two')"));
    checkCmdOnDriver(driver.run("insert into tab1 partition(p) values(3,3,'one'),(4,4,'two')"));//txinid:1
    //writing both acid and non-acid resources in the same txn
    //tab1 write is a dynamic partition insert
    checkCmdOnDriver(driver.run("from tab_not_acid insert into tab1 partition(p)(a,b,p) select a,b,p insert into tab_not_acid(a,b) select a,b where p='two'"));//txnid:2
    Assert.assertEquals(TxnDbUtil.queryToString("select * from COMPLETED_TXN_COMPONENTS"),
      4, TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS"));
    //only expect transactional components to be in COMPLETED_TXN_COMPONENTS
    Assert.assertEquals(TxnDbUtil.queryToString("select * from COMPLETED_TXN_COMPONENTS"),
      2, TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS where ctc_txnid=2"));
    Assert.assertEquals(TxnDbUtil.queryToString("select * from COMPLETED_TXN_COMPONENTS"),
      2, TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS where ctc_txnid=2 and ctc_table='tab1'"));
  }
  //todo: Concurrent insert/update of same partition - should pass

  private List<ShowLocksResponseElement> getLocksWithFilterOptions(HiveTxnManager txnMgr,
      String dbName, String tblName, Map<String, String> partSpec) throws Exception {
    if (dbName == null && tblName != null) {
      dbName = SessionState.get().getCurrentDatabase();
    }
    ShowLocksRequest rqst = new ShowLocksRequest();
    rqst.setDbname(dbName);
    rqst.setTablename(tblName);
    if (partSpec != null) {
      List<String> keyList = new ArrayList<String>();
      List<String> valList = new ArrayList<String>();
      for (String partKey : partSpec.keySet()) {
        String partVal = partSpec.remove(partKey);
        keyList.add(partKey);
        valList.add(partVal);
      }
      String partName = FileUtils.makePartName(keyList, valList);
      rqst.setPartname(partName);
    }
    ShowLocksResponse rsp = ((DbLockManager)txnMgr.getLockManager()).getLocks(rqst);
    return rsp.getLocks();
  }
  
  @Test
  public void testShowLocksAgentInfo() throws Exception {
    CommandProcessorResponse cpr = driver.run("create table if not exists XYZ (a int, b int)");
    checkCmdOnDriver(cpr);
    checkCmdOnDriver(driver.compileAndRespond("select a from XYZ where b = 8"));
    txnMgr.acquireLocks(driver.getPlan(), ctx, "XYZ");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "XYZ", null, locks);
    Assert.assertEquals("Wrong AgentInfo", driver.getPlan().getQueryId(), locks.get(0).getAgentInfo());
  }
  @Test
  public void testMerge3Way01() throws Exception {
    testMerge3Way(false);
  }
  @Test
  public void testMerge3Way02() throws Exception {
    testMerge3Way(true);
  }

  /**
   * @param cc whether to cause a WW conflict or not
   * @throws Exception
   */
  private void testMerge3Way(boolean cc) throws Exception {
    dropTable(new String[] {"target","source", "source2"});
    checkCmdOnDriver(driver.run("create table target (a int, b int) " +
      "partitioned by (p int, q int) clustered by (a) into 2  buckets " +
      "stored as orc TBLPROPERTIES ('transactional'='true')"));
    //in practice we don't really care about the data in any of these tables (except as far as
    //it creates partitions, the SQL being test is not actually executed and results of the
    //wrt ACID metadata is supplied manually via addDynamicPartitions().  But having data makes
    //it easier to follow the intent
    checkCmdOnDriver(driver.run("insert into target partition(p,q) values (1,2,1,2), (3,4,1,2), (5,6,1,3), (7,8,2,2)"));
    checkCmdOnDriver(driver.run("create table source (a int, b int, p int, q int)"));
    checkCmdOnDriver(driver.run("insert into source values " +
      // I-(1/2)            D-(1/2)    I-(1/3)     U-(1/3)     D-(2/2)     I-(1/1) - new part
      "(9,10,1,2),        (3,4,1,2), (11,12,1,3), (5,13,1,3), (7,8,2,2), (14,15,1,1)"));
    checkCmdOnDriver(driver.run("create table source2 (a int, b int, p int, q int)"));
    checkCmdOnDriver(driver.run("insert into source2 values " +
  //cc ? -:U-(1/2)     D-(1/2)         cc ? U-(1/3):-             D-(2/2)       I-(1/1) - new part 2
      "(9,100,1,2),      (3,4,1,2),               (5,13,1,3),       (7,8,2,2), (14,15,2,1)"));
    

    long txnId1 = txnMgr.openTxn(ctx, "T1");
    checkCmdOnDriver(driver.compileAndRespond("merge into target t using source s on t.a=s.b " +
      "when matched and t.a=5 then update set b=s.b " + //updates p=1/q=3
      "when matched and t.a in (3,7) then delete " + //deletes from p=1/q=2, p=2/q=2
      "when not matched and t.a >= 8 then insert values(s.a, s.b, s.p, s.q)"));//insert p=1/q=2, p=1/q=3 and new part 1/1
    txnMgr.acquireLocks(driver.getPlan(), ctx, "T1");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 5, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "target", null, locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "source", null, locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "target", "p=1/q=2", locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "target", "p=1/q=3", locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "target", "p=2/q=2", locks);

    //start concurrent txn
    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    long txnId2 = txnMgr2.openTxn(ctx, "T2");
    checkCmdOnDriver(driver.compileAndRespond("merge into target t using source2 s on t.a=s.b " +
      "when matched and t.a=" + (cc ? 5 : 9) + " then update set b=s.b " + //if conflict updates p=1/q=3 else update p=1/q=2
      "when matched and t.a in (3,7) then delete " + //deletes from p=1/q=2, p=2/q=2
      "when not matched and t.a >= 8 then insert values(s.a, s.b, s.p, s.q)"));//insert p=1/q=2, p=1/q=3 and new part 1/1
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "T1", false);
    locks = getLocks(txnMgr2);
    Assert.assertEquals("Unexpected lock count", 10, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "target", null, locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "source", null, locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "target", "p=1/q=2", locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "target", "p=1/q=3", locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "target", "p=2/q=2", locks);

    long extLockId = checkLock(LockType.SHARED_READ, LockState.WAITING, "default", "target", null, locks).getLockid();
    checkLock(LockType.SHARED_READ, LockState.WAITING, "default", "source2", null, locks);
    checkLock(LockType.SHARED_WRITE, LockState.WAITING, "default", "target", "p=1/q=2", locks);
    checkLock(LockType.SHARED_WRITE, LockState.WAITING, "default", "target", "p=1/q=3", locks);
    checkLock(LockType.SHARED_WRITE, LockState.WAITING, "default", "target", "p=2/q=2", locks);

    Assert.assertEquals(
      "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId1) + "): " +
        TxnDbUtil.queryToString("select * from TXN_COMPONENTS"),
      0,
      TxnDbUtil.countQueryAgent("select count(*) from TXN_COMPONENTS where tc_txnid=" + txnId1));
    //complete 1st txn
    AddDynamicPartitions adp = new AddDynamicPartitions(txnId1, "default", "target",
      Collections.singletonList("p=1/q=3"));//update clause
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    adp = new AddDynamicPartitions(txnId1, "default", "target",
      Arrays.asList("p=1/q=2","p=2/q=2"));//delete clause
    adp.setOperationType(DataOperationType.DELETE);
    txnHandler.addDynamicPartitions(adp);
    adp = new AddDynamicPartitions(txnId1, "default", "target",
      Arrays.asList("p=1/q=2","p=1/q=3","p=1/q=1"));//insert clause
    adp.setOperationType(DataOperationType.INSERT);
    txnHandler.addDynamicPartitions(adp);
    Assert.assertEquals(
      "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId1) + "): " +
        TxnDbUtil.queryToString("select * from TXN_COMPONENTS"),
      1,
      TxnDbUtil.countQueryAgent("select count(*) from TXN_COMPONENTS where tc_txnid=" + txnId1 +
        " and tc_operation_type='u'"));
    Assert.assertEquals(
      "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId1) + "): " +
        TxnDbUtil.queryToString("select * from TXN_COMPONENTS"),
      2,
      TxnDbUtil.countQueryAgent("select count(*) from TXN_COMPONENTS where tc_txnid=" + txnId1 +
        " and tc_operation_type='d'"));
    Assert.assertEquals(
      "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId1) + "): " +
        TxnDbUtil.queryToString("select * from TXN_COMPONENTS"),
      3,
      TxnDbUtil.countQueryAgent("select count(*) from TXN_COMPONENTS where tc_txnid=" + txnId1 +
        " and tc_operation_type='i'"));
    txnMgr.commitTxn();//commit T1
    Assert.assertEquals(
      "COMPLETED_TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId1) + "): " +
        TxnDbUtil.queryToString("select * from COMPLETED_TXN_COMPONENTS"),
      6,
      TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS where ctc_txnid=" + txnId1));
    Assert.assertEquals(
      "WRITE_SET mismatch(" + JavaUtils.txnIdToString(txnId1) + "): " +
        TxnDbUtil.queryToString("select * from WRITE_SET"),
      1,
      TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET where ws_txnid=" + txnId1 +
        " and ws_operation_type='u'"));
    Assert.assertEquals(
      "WRITE_SET mismatch(" + JavaUtils.txnIdToString(txnId1) + "): " +
        TxnDbUtil.queryToString("select * from WRITE_SET"),
      2,
      TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET where ws_txnid=" + txnId1 +
        " and ws_operation_type='d'"));

    //re-check locks which were in Waiting state - should now be Acquired
    ((DbLockManager)txnMgr2.getLockManager()).checkLock(extLockId);
    locks = getLocks(txnMgr2);
    Assert.assertEquals("Unexpected lock count", 5, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "target", null, locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "source2", null, locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "target", "p=1/q=2", locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "target", "p=1/q=3", locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "target", "p=2/q=2", locks);

    Assert.assertEquals(
      "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId2) + "): " +
        TxnDbUtil.queryToString("select * from TXN_COMPONENTS"),
      0,
      TxnDbUtil.countQueryAgent("select count(*) from TXN_COMPONENTS where tc_txnid=" + txnId2));
    //complete 2nd txn
    adp = new AddDynamicPartitions(txnId2, "default", "target",
      Collections.singletonList(cc ? "p=1/q=3" : "p=1/p=2"));//update clause
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    adp = new AddDynamicPartitions(txnId2, "default", "target",
      Arrays.asList("p=1/q=2","p=2/q=2"));//delete clause
    adp.setOperationType(DataOperationType.DELETE);
    txnHandler.addDynamicPartitions(adp);
    adp = new AddDynamicPartitions(txnId2, "default", "target",
      Arrays.asList("p=1/q=2","p=1/q=3","p=1/q=1"));//insert clause
    adp.setOperationType(DataOperationType.INSERT);
    txnHandler.addDynamicPartitions(adp);
    Assert.assertEquals(
      "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId2) + "): " +
        TxnDbUtil.queryToString("select * from TXN_COMPONENTS"),
      1,
      TxnDbUtil.countQueryAgent("select count(*) from TXN_COMPONENTS where tc_txnid=" + txnId2 +
        " and tc_operation_type='u'"));
    Assert.assertEquals(
      "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId2) + "): " +
        TxnDbUtil.queryToString("select * from TXN_COMPONENTS"),
      2,
      TxnDbUtil.countQueryAgent("select count(*) from TXN_COMPONENTS where tc_txnid=" + txnId2 +
        " and tc_operation_type='d'"));
    Assert.assertEquals(
      "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId2) + "): " +
        TxnDbUtil.queryToString("select * from TXN_COMPONENTS"),
      3,
      TxnDbUtil.countQueryAgent("select count(*) from TXN_COMPONENTS where tc_txnid=" + txnId2 +
        " and tc_operation_type='i'"));

    LockException expectedException = null;
    try {
      txnMgr2.commitTxn();//commit T2
    }
    catch (LockException e) {
      expectedException = e;
    }
    if(cc) {
      Assert.assertNotNull("didn't get exception", expectedException);
      Assert.assertEquals("Transaction manager has aborted the transaction txnid:3.  Reason: " +
        "Aborting [txnid:3,3] due to a write conflict on default/target/p=1/q=3 " +
        "committed by [txnid:2,3] u/u", expectedException.getMessage());
      Assert.assertEquals(
        "COMPLETED_TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId2) + "): " +
          TxnDbUtil.queryToString("select * from COMPLETED_TXN_COMPONENTS"),
        0,
        TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS where ctc_txnid=" + txnId2));
      Assert.assertEquals(
        "WRITE_SET mismatch(" + JavaUtils.txnIdToString(txnId2) + "): " +
          TxnDbUtil.queryToString("select * from WRITE_SET"),
        0,
        TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET where ws_txnid=" + txnId2));
    }
    else {
      Assert.assertNull("Unexpected exception " + expectedException, expectedException);
      Assert.assertEquals(
        "COMPLETED_TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId2) + "): " +
          TxnDbUtil.queryToString("select * from COMPLETED_TXN_COMPONENTS"),
        6,
        TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS where ctc_txnid=" + txnId2));
      Assert.assertEquals(
        "WRITE_SET mismatch(" + JavaUtils.txnIdToString(txnId2) + "): " +
          TxnDbUtil.queryToString("select * from WRITE_SET"),
        1,
        TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET where ws_txnid=" + txnId2 +
          " and ws_operation_type='u'"));
      Assert.assertEquals(
        "WRITE_SET mismatch(" + JavaUtils.txnIdToString(txnId2) + "): " +
          TxnDbUtil.queryToString("select * from WRITE_SET"),
        2,
        TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET where ws_txnid=" + txnId2 +
          " and ws_operation_type='d'"));
    }


  }
  @Test 
  public void testMergeUnpartitioned01() throws Exception {
    testMergeUnpartitioned(true);
  }
  @Test
  public void testMergeUnpartitioned02() throws Exception {
    testMergeUnpartitioned(false);
  }

  /**
   * run a merge statement using un-partitioned target table and a concurrent op on the target
   * Check that proper locks are acquired and Write conflict detection works and the state
   * of internal table.
   * @param causeConflict true to make 2 operations such that they update the same entity
   * @throws Exception
   */
  private void testMergeUnpartitioned(boolean causeConflict) throws Exception {
    dropTable(new String[] {"target","source"});
    checkCmdOnDriver(driver.run("create table target (a int, b int) " +
      "clustered by (a) into 2  buckets " +
      "stored as orc TBLPROPERTIES ('transactional'='true')"));
    checkCmdOnDriver(driver.run("insert into target values (1,2), (3,4), (5,6), (7,8)"));
    checkCmdOnDriver(driver.run("create table source (a int, b int)"));
    
    long txnid1 = txnMgr.openTxn(ctx, "T1");
    if(causeConflict) {
      checkCmdOnDriver(driver.compileAndRespond("update target set b = 2 where a=1"));
    }
    else {
      checkCmdOnDriver(driver.compileAndRespond("insert into target values(9,10),(11,12)"));
    }
    txnMgr.acquireLocks(driver.getPlan(), ctx, "T1");
    Assert.assertEquals(
      "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnid1) + "): " +
        TxnDbUtil.queryToString("select * from TXN_COMPONENTS"),
      1,//no DP, so it's populated from lock info
      TxnDbUtil.countQueryAgent("select count(*) from TXN_COMPONENTS where tc_txnid=" + txnid1));

    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(causeConflict ? LockType.SHARED_WRITE : LockType.SHARED_READ,
      LockState.ACQUIRED, "default", "target", null, locks);

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    //start a 2nd (overlapping) txn
    long txnid2 = txnMgr2.openTxn(ctx, "T2");
    checkCmdOnDriver(driver.compileAndRespond("merge into target t using source s " +
      "on t.a=s.a " +
      "when matched then delete " +
      "when not matched then insert values(s.a,s.b)"));
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "T2", false);
    locks = getLocks(txnMgr);

    Assert.assertEquals("Unexpected lock count", 3, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "target", null, locks);
    checkLock(LockType.SHARED_READ, causeConflict ? LockState.WAITING : LockState.ACQUIRED,
      "default", "source", null, locks);
    long extLockId = checkLock(LockType.SHARED_WRITE, causeConflict ? LockState.WAITING : LockState.ACQUIRED,
      "default", "target", null, locks).getLockid();

    txnMgr.commitTxn();//commit T1
    
    Assert.assertEquals("WRITE_SET mismatch(" + JavaUtils.txnIdToString(txnid1) + "): " +
        TxnDbUtil.queryToString("select * from WRITE_SET"),
      causeConflict ? 1 : 0,//Inserts are not tracked by WRITE_SET
      TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET where ws_txnid=" + txnid1 +
        " and ws_operation_type=" + (causeConflict ? "'u'" : "'i'")));


    //re-check locks which were in Waiting state - should now be Acquired
    ((DbLockManager)txnMgr2.getLockManager()).checkLock(extLockId);
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "source", null, locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "target", null, locks);

    Assert.assertEquals(
      "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnid2) + "): " +
        TxnDbUtil.queryToString("select * from TXN_COMPONENTS"),
      1,//
      TxnDbUtil.countQueryAgent("select count(*) from TXN_COMPONENTS where tc_txnid=" + txnid2));
    Assert.assertEquals(
      "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnid2) + "): " +
        TxnDbUtil.queryToString("select * from TXN_COMPONENTS"),
      1,//
      TxnDbUtil.countQueryAgent("select count(*) from TXN_COMPONENTS where tc_txnid=" + txnid2 +
      "and tc_operation_type='d'"));
    
    //complete T2 txn
    LockException expectedException = null;
    try {
      txnMgr2.commitTxn();
    }
    catch (LockException e) {
      expectedException = e;
    }
    if(causeConflict) {
      Assert.assertTrue("Didn't get exception", expectedException != null);
      Assert.assertEquals("Got wrong message code", ErrorMsg.TXN_ABORTED, expectedException.getCanonicalErrorMsg());
      Assert.assertEquals("Exception msg didn't match",
        "Aborting [txnid:3,3] due to a write conflict on default/target committed by [txnid:2,3] d/u",
        expectedException.getCause().getMessage());
    }
    else {
      Assert.assertEquals("WRITE_SET mismatch(" + JavaUtils.txnIdToString(txnid1) + "): " +
          TxnDbUtil.queryToString("select * from WRITE_SET"),
        1,//Unpartitioned table: 1 row for Delete; Inserts are not tracked in WRITE_SET
        TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET where ws_txnid=" + txnid2 +
          " and ws_operation_type='d'"));
    }
  }
  /**
   * Check that DP with partial spec properly updates TXN_COMPONENTS
   * @throws Exception
   */
  @Test
  public void testDynamicPartitionInsert() throws Exception {
    dropTable(new String[] {"target"});
    checkCmdOnDriver(driver.run("create table target (a int, b int) " +
      "partitioned by (p int, q int) clustered by (a) into 2  buckets " +
      "stored as orc TBLPROPERTIES ('transactional'='true')"));
    long txnid1 = txnMgr.openTxn(ctx, "T1");
    checkCmdOnDriver(driver.compileAndRespond("insert into target partition(p=1,q) values (1,2,2), (3,4,2), (5,6,3), (7,8,2)"));
    txnMgr.acquireLocks(driver.getPlan(), ctx, "T1");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    //table is empty, so can only lock the table
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "target", null, locks);
    Assert.assertEquals(
      "HIVE_LOCKS mismatch(" + JavaUtils.txnIdToString(txnid1) + "): " +
        TxnDbUtil.queryToString("select * from HIVE_LOCKS"),
      1,
      TxnDbUtil.countQueryAgent("select count(*) from HIVE_LOCKS where hl_txnid=" + txnid1));
    txnMgr.rollbackTxn();
    Assert.assertEquals(
      "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnid1) + "): " +
        TxnDbUtil.queryToString("select * from TXN_COMPONENTS"),
      0,
      TxnDbUtil.countQueryAgent("select count(*) from TXN_COMPONENTS where tc_txnid=" + txnid1));
    //now actually write to table to generate some partitions
    checkCmdOnDriver(driver.run("insert into target partition(p=1,q) values (1,2,2), (3,4,2), (5,6,3), (7,8,2)"));
    driver.run("select count(*) from target");
    List<String> r = new ArrayList<>();
    driver.getResults(r);
    Assert.assertEquals("", "4", r.get(0));
    Assert.assertEquals(//look in COMPLETED_TXN_COMPONENTS because driver.run() committed!!!!
      "COMPLETED_TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnid1 + 1) + "): " +
        TxnDbUtil.queryToString("select * from COMPLETED_TXN_COMPONENTS"),
      2,//2 distinct partitions created
      //txnid+1 because we want txn used by previous driver.run("insert....)
      TxnDbUtil.countQueryAgent("select count(*) from COMPLETED_TXN_COMPONENTS where ctc_txnid=" + (txnid1 + 1)));


    long txnid2 = txnMgr.openTxn(ctx, "T1");
    checkCmdOnDriver(driver.compileAndRespond("insert into target partition(p=1,q) values (10,2,2), (30,4,2), (50,6,3), (70,8,2)"));
    txnMgr.acquireLocks(driver.getPlan(), ctx, "T1");
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    //Plan is using DummyPartition, so can only lock the table... unfortunately
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "target", null, locks);
    AddDynamicPartitions adp = new AddDynamicPartitions(txnid2, "default", "target", Arrays.asList("p=1/q=2","p=1/q=2"));
    adp.setOperationType(DataOperationType.INSERT);
    txnHandler.addDynamicPartitions(adp);
    Assert.assertEquals(
      "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnid2) + "): " +
        TxnDbUtil.queryToString("select * from TXN_COMPONENTS"),
      2,//2 distinct partitions modified
      TxnDbUtil.countQueryAgent("select count(*) from TXN_COMPONENTS where tc_txnid=" + txnid2));
    txnMgr.commitTxn();
  }
  @Test
  public void testMergePartitioned01() throws Exception {
    testMergePartitioned(false);
  }
  @Test
  public void testMergePartitioned02() throws Exception {
    testMergePartitioned(true);
  }
  /**
   * "run" an Update and Merge concurrently; Check that correct locks are acquired.
   * Check state of auxiliary ACID tables.
   * @param causeConflict - true to make the operations cause a Write conflict
   * @throws Exception
   */
  private void testMergePartitioned(boolean causeConflict) throws Exception {
    dropTable(new String[] {"target","source"});
    checkCmdOnDriver(driver.run("create table target (a int, b int) " +
      "partitioned by (p int, q int) clustered by (a) into 2  buckets " +
      "stored as orc TBLPROPERTIES ('transactional'='true')"));
    checkCmdOnDriver(driver.run("insert into target partition(p,q) values (1,2,1,2), (3,4,1,2), (5,6,1,3), (7,8,2,2)"));
    checkCmdOnDriver(driver.run("create table source (a1 int, b1 int, p1 int, q1 int)"));

    long txnId1 = txnMgr.openTxn(ctx, "T1");
    checkCmdOnDriver(driver.compileAndRespond("update target set b = 2 where p=1"));
    txnMgr.acquireLocks(driver.getPlan(), ctx, "T1");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "target", "p=1/q=2", locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "target", "p=1/q=3", locks);

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    //start a 2nd (overlapping) txn
    long txnid2 = txnMgr2.openTxn(ctx, "T2");
    checkCmdOnDriver(driver.compileAndRespond("merge into target using source " +
      "on target.p=source.p1 and target.a=source.a1 " +
      "when matched then update set b = 11 " +
      "when not matched then insert values(a1,b1,p1,q1)"));
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "T2", false);
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 7, locks.size());
    /**
     * W locks from T1 are still there, so all locks from T2 block.
     * The Update part of Merge requests W locks for each existing partition in target.
     * The Insert part doesn't know which partitions may be written to: thus R lock on target table.
     * */
    checkLock(LockType.SHARED_READ, LockState.WAITING, "default", "source", null, locks);
    long extLockId = checkLock(LockType.SHARED_READ, LockState.WAITING, "default", "target", null, locks).getLockid();

    checkLock(LockType.SHARED_WRITE, LockState.WAITING, "default", "target", "p=1/q=2", locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "target", "p=1/q=2", locks);
    
    checkLock(LockType.SHARED_WRITE, LockState.WAITING, "default", "target", "p=1/q=3", locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "target", "p=1/q=3", locks);
    
    checkLock(LockType.SHARED_WRITE, LockState.WAITING, "default", "target", "p=2/q=2", locks);

    Assert.assertEquals(
      "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId1) + "): " +
        TxnDbUtil.queryToString("select * from TXN_COMPONENTS"),
      0,//because it's using a DP write
      TxnDbUtil.countQueryAgent("select count(*) from TXN_COMPONENTS where tc_txnid=" + txnId1));
    //complete T1 transaction (simulate writing to 2 partitions)
    AddDynamicPartitions adp = new AddDynamicPartitions(txnId1, "default", "target",
      Arrays.asList("p=1/q=2","p=1/q=3"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    Assert.assertEquals(
      "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId1) + "): " +
        TxnDbUtil.queryToString("select * from TXN_COMPONENTS"),
      2,
      TxnDbUtil.countQueryAgent("select count(*) from TXN_COMPONENTS where tc_txnid=" + txnId1 +
        " and tc_operation_type='u'"));
    txnMgr.commitTxn();//commit T1
    Assert.assertEquals("WRITE_SET mismatch(" + JavaUtils.txnIdToString(txnId1) + "): " +
      TxnDbUtil.queryToString("select * from WRITE_SET"),
      2,//2 partitions updated
      TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET where ws_txnid=" + txnId1 +
      " and ws_operation_type='u'"));
    

    //re-check locks which were in Waiting state - should now be Acquired
    ((DbLockManager)txnMgr2.getLockManager()).checkLock(extLockId);
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 5, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "source", null, locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "target", null, locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "target", "p=1/q=2", locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "target", "p=1/q=3", locks);
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "target", "p=2/q=2", locks);


    Assert.assertEquals(
      "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnid2) + "): " +
        TxnDbUtil.queryToString("select * from TXN_COMPONENTS"),
      0,//because it's using a DP write
      TxnDbUtil.countQueryAgent("select count(*) from TXN_COMPONENTS where tc_txnid=" + txnid2));
    //complete T2 txn
    //simulate Insert into 2 partitions
    adp = new AddDynamicPartitions(txnid2, "default", "target",
      Arrays.asList("p=1/q=2","p=1/q=3"));
    adp.setOperationType(DataOperationType.INSERT);
    txnHandler.addDynamicPartitions(adp);
    Assert.assertEquals(
      "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnid2) + "): " +
        TxnDbUtil.queryToString("select * from TXN_COMPONENTS"),
      2,
      TxnDbUtil.countQueryAgent("select count(*) from TXN_COMPONENTS where tc_txnid=" + txnid2 + " and tc_operation_type='i'"));
    //simulate Update of 1 partitions; depending on causeConflict, choose one of the partitions
    //which was modified by the T1 update stmt or choose a non-conflicting one
    adp = new AddDynamicPartitions(txnid2, "default", "target",
      Collections.singletonList(causeConflict ? "p=1/q=2" : "p=1/q=1"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    Assert.assertEquals(
      "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnid2) + "): " +
        TxnDbUtil.queryToString("select * from TXN_COMPONENTS"),
      1,
      TxnDbUtil.countQueryAgent("select count(*) from TXN_COMPONENTS where tc_txnid=" + txnid2 + " and tc_operation_type='u'"));
    
    
    LockException expectedException = null;
    try {
      txnMgr2.commitTxn();
    }
    catch (LockException e) {
      expectedException = e;
    }
    if(causeConflict) {
      Assert.assertTrue("Didn't get exception", expectedException != null);
      Assert.assertEquals("Got wrong message code", ErrorMsg.TXN_ABORTED, expectedException.getCanonicalErrorMsg());
      Assert.assertEquals("Exception msg didn't match",
        "Aborting [txnid:3,3] due to a write conflict on default/target/p=1/q=2 committed by [txnid:2,3] u/u",
        expectedException.getCause().getMessage());
    }
    else {
      Assert.assertEquals("WRITE_SET mismatch(" + JavaUtils.txnIdToString(txnid2) + "): " +
          TxnDbUtil.queryToString("select * from WRITE_SET"),
        1,//1 partitions updated
        TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET where ws_txnid=" + txnid2 +
          " and ws_operation_type='u'"));
      Assert.assertEquals("WRITE_SET mismatch(" + JavaUtils.txnIdToString(txnid2) + "): " +
          TxnDbUtil.queryToString("select * from WRITE_SET"),
        1,//1 partitions updated (and no other entries)
        TxnDbUtil.countQueryAgent("select count(*) from WRITE_SET where ws_txnid=" + txnid2));
    }
  }
  @Test
  public void testShowTablesLock() throws Exception {
    dropTable(new String[] {"T, T2"});
    CommandProcessorResponse cpr = driver.run(
      "create table if not exists T (a int, b int)");
    checkCmdOnDriver(cpr);

    long txnid1 = txnMgr.openTxn(ctx, "Fifer");
    checkCmdOnDriver(driver.compileAndRespond("insert into T values(1,3)"));
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer");
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "t", null, locks);

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    checkCmdOnDriver(driver.compileAndRespond("show tables"));
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "Fidler");
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "t", null, locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", null, null, locks);
    txnMgr.commitTxn();
    txnMgr2.releaseLocks(txnMgr2.getLockManager().getLocks(false, false));
    Assert.assertEquals("Lock remained", 0, getLocks().size());
    Assert.assertEquals("Lock remained", 0, getLocks(txnMgr2).size());


    cpr = driver.run(
      "create table if not exists T2 (a int, b int) partitioned by (p int) clustered by (a) " +
        "into 2  buckets stored as orc TBLPROPERTIES ('transactional'='false')");
    checkCmdOnDriver(cpr);

    txnid1 = txnMgr.openTxn(ctx, "Fifer");
    checkCmdOnDriver(driver.compileAndRespond("insert into T2 partition(p=1) values(1,3)"));
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer");
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "t2", "p=1", locks);

    txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    checkCmdOnDriver(driver.compileAndRespond("show tables"));
    ((DbTxnManager)txnMgr2).acquireLocks(driver.getPlan(), ctx, "Fidler", false);
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "t2", "p=1", locks);
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", null, null, locks);
    txnMgr.commitTxn();
    txnMgr2.releaseLocks(txnMgr2.getLockManager().getLocks(false, false));
    Assert.assertEquals("Lock remained", 0, getLocks().size());
    Assert.assertEquals("Lock remained", 0, getLocks(txnMgr2).size());
  }
}
