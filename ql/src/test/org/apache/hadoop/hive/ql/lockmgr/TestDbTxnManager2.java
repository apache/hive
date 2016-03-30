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

import junit.framework.Assert;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * See additional tests in {@link org.apache.hadoop.hive.ql.lockmgr.TestDbTxnManager}
 * Tests here are "end-to-end"ish and simulate concurrent queries.
 */
public class TestDbTxnManager2 {
  private static HiveConf conf = new HiveConf(Driver.class);
  private HiveTxnManager txnMgr;
  private Context ctx;
  private Driver driver;

  @BeforeClass
  public static void setUpClass() throws Exception {
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
    txnMgr = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    Assert.assertTrue(txnMgr instanceof DbTxnManager);
  }
  @After
  public void tearDown() throws Exception {
    driver.close();
    if (txnMgr != null) txnMgr.closeTxnManager();
    TxnDbUtil.cleanDb();
    TxnDbUtil.prepDb();
  }
  @Test
  public void createTable() throws Exception {
    CommandProcessorResponse cpr = driver.compileAndRespond("create table if not exists T (a int, b int)");
    checkCmdOnDriver(cpr);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer");
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", null, null, locks.get(0));
    txnMgr.getLockManager().releaseLocks(ctx.getHiveLocks());
    Assert.assertEquals("Lock remained", 0, getLocks().size());
  }
  @Test
  public void insertOverwriteCreate() throws Exception {
    CommandProcessorResponse cpr = driver.run("create table if not exists T2(a int)");
    checkCmdOnDriver(cpr);
    cpr = driver.run("create table if not exists T3(a int)");
    checkCmdOnDriver(cpr);
    cpr = driver.compileAndRespond("insert overwrite table T3 select a from T2");
    checkCmdOnDriver(cpr);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer");
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T2", null, locks.get(0));
    checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "T3", null, locks.get(1));
    txnMgr.getLockManager().releaseLocks(ctx.getHiveLocks());
    Assert.assertEquals("Lock remained", 0, getLocks().size());
    cpr = driver.run("drop table if exists T1");
    checkCmdOnDriver(cpr);
    cpr = driver.run("drop table if exists T2");
    checkCmdOnDriver(cpr);
  }
  @Test
  public void insertOverwritePartitionedCreate() throws Exception {
    CommandProcessorResponse cpr = driver.run("create table if not exists T4 (name string, gpa double) partitioned by (age int)");
    checkCmdOnDriver(cpr);
    cpr = driver.run("create table if not exists T5(name string, age int, gpa double)");
    checkCmdOnDriver(cpr);
    cpr = driver.compileAndRespond("INSERT OVERWRITE TABLE T4 PARTITION (age) SELECT name, age, gpa FROM T5");
    checkCmdOnDriver(cpr);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer");
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T5", null, locks.get(0));
    checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "T4", null, locks.get(1));
    txnMgr.getLockManager().releaseLocks(ctx.getHiveLocks());
    Assert.assertEquals("Lock remained", 0, getLocks().size());
    cpr = driver.run("drop table if exists T5");
    checkCmdOnDriver(cpr);
    cpr = driver.run("drop table if exists T4");
    checkCmdOnDriver(cpr);
  }
  @Test
  public void basicBlocking() throws Exception {
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
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T6", null, locks.get(0));
    checkLock(LockType.EXCLUSIVE, LockState.WAITING, "default", "T6", null, locks.get(1));
    txnMgr.getLockManager().releaseLocks(selectLocks);//release S on T6
    //attempt to X on T6 again - succeed
    lockState = ((DbLockManager)txnMgr.getLockManager()).checkLock(locks.get(1).getLockid());
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "T6", null, locks.get(0));
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
    CommandProcessorResponse cpr = driver.run("create database if not exists temp");
    checkCmdOnDriver(cpr);
    cpr = driver.run("create table if not exists temp.T7(a int, b int) clustered by(b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    cpr = driver.compileAndRespond("update temp.T7 set a = 5 where b = 6");
    checkCmdOnDriver(cpr);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer");
    List<HiveLock> updateLocks = ctx.getHiveLocks();
    cpr = driver.compileAndRespond("drop database if exists temp");
    LockState lockState = ((DbTxnManager) txnMgr).acquireLocks(driver.getPlan(), ctx, "Fiddler", false);//gets SS lock on T7
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "temp", "T7", null, locks.get(0));
    checkLock(LockType.EXCLUSIVE, LockState.WAITING, "temp", null, null, locks.get(1));
    txnMgr.getLockManager().releaseLocks(updateLocks);
    lockState = ((DbLockManager)txnMgr.getLockManager()).checkLock(locks.get(1).getLockid());
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "temp", null, null, locks.get(0));
    List<HiveLock> xLock = new ArrayList<HiveLock>(0);
    xLock.add(new DbLockManager.DbHiveLock(locks.get(0).getLockid()));
    txnMgr.getLockManager().releaseLocks(xLock);
  }
  @Test
  public void updateSelectUpdate() throws Exception {
    CommandProcessorResponse cpr = driver.run("create table T8(a int, b int) clustered by(b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    cpr = driver.compileAndRespond("delete from T8 where b = 89");
    checkCmdOnDriver(cpr);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer");//gets SS lock on T8
    List<HiveLock> deleteLocks = ctx.getHiveLocks();
    cpr = driver.compileAndRespond("select a from T8");//gets S lock on T8
    checkCmdOnDriver(cpr);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fiddler");
    cpr = driver.compileAndRespond("update T8 set a = 1 where b = 1");
    checkCmdOnDriver(cpr);
    LockState lockState = ((DbTxnManager) txnMgr).acquireLocks(driver.getPlan(), ctx, "Practical", false);//waits for SS lock on T8 from fifer
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 3, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T8", null, locks.get(0));
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "T8", null, locks.get(1));
    checkLock(LockType.SHARED_WRITE, LockState.WAITING, "default", "T8", null, locks.get(2));
    txnMgr.getLockManager().releaseLocks(deleteLocks);
    lockState = ((DbLockManager)txnMgr.getLockManager()).checkLock(locks.get(2).getLockid());
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T8", null, locks.get(0));
    checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "T8", null, locks.get(1));
    List<HiveLock> relLocks = new ArrayList<HiveLock>(2);
    relLocks.add(new DbLockManager.DbHiveLock(locks.get(0).getLockid()));
    relLocks.add(new DbLockManager.DbHiveLock(locks.get(1).getLockid()));
    txnMgr.getLockManager().releaseLocks(relLocks);
    cpr = driver.run("drop table if exists T6");
    locks = getLocks();
    Assert.assertEquals("Unexpected number of locks found", 0, locks.size());
    checkCmdOnDriver(cpr);
  }

  @Test
  public void testLockRetryLimit() throws Exception {
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
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T9", null, locks.get(0));
    
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
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T9", null, locks.get(0));
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
    CommandProcessorResponse cpr = driver.run("create table TAB_BLOCKED (a int, b int) clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    checkCmdOnDriver(cpr);
    cpr = driver.compileAndRespond("select * from TAB_BLOCKED");
    checkCmdOnDriver(cpr);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "I AM SAM");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "TAB_BLOCKED", null, locks.get(0));
    cpr = driver.compileAndRespond("drop table TAB_BLOCKED");
    checkCmdOnDriver(cpr);
    ((DbTxnManager)txnMgr).acquireLocks(driver.getPlan(), ctx, "SAM I AM", false);//make non-blocking
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "TAB_BLOCKED", null, locks.get(0));
    checkLock(LockType.EXCLUSIVE, LockState.WAITING, "default", "TAB_BLOCKED", null, locks.get(1));
    Assert.assertEquals("BlockedByExtId doesn't match", locks.get(0).getLockid(), locks.get(1).getBlockedByExtId());
    Assert.assertEquals("BlockedByIntId doesn't match", locks.get(0).getLockIdInternal(), locks.get(1).getBlockedByIntId());
  }

  @Test
  public void testDummyTxnManagerOnAcidTable() throws Exception {
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

  private void checkLock(LockType type, LockState state, String db, String table, String partition, ShowLocksResponseElement l) {
    Assert.assertEquals(l.toString(),l.getType(), type);
    Assert.assertEquals(l.toString(),l.getState(), state);
    Assert.assertEquals(l.toString(), normalizeCase(l.getDbname()), normalizeCase(db));
    Assert.assertEquals(l.toString(), normalizeCase(l.getTablename()), normalizeCase(table));
    Assert.assertEquals(l.toString(), normalizeCase(l.getPartname()), normalizeCase(partition));
  }
  private void checkCmdOnDriver(CommandProcessorResponse cpr) {
    Assert.assertTrue(cpr.toString(), cpr.getResponseCode() == 0);
  }
  private String normalizeCase(String s) {
    return s == null ? null : s.toLowerCase();
  }
  private List<ShowLocksResponseElement> getLocks() throws Exception {
    return getLocks(this.txnMgr);
  }
  private List<ShowLocksResponseElement> getLocks(HiveTxnManager txnMgr) throws Exception {
    ShowLocksResponse rsp = ((DbLockManager)txnMgr.getLockManager()).getLocks();
    return rsp.getLocks();
  }
}
