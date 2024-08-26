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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.MetastoreTaskThread;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.service.CompactionHouseKeeperService;
import org.apache.hadoop.hive.metastore.txn.service.AcidHouseKeeperService;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hive.common.util.ReflectionUtil;
import org.junit.Assert;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.ComparisonFailure;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hive.common.AcidConstants.SOFT_DELETE_TABLE_PATTERN;

import static org.apache.hadoop.hive.ql.TxnCommandsBaseForTests.runWorker;
import static org.apache.hadoop.hive.ql.TxnCommandsBaseForTests.runCleaner;

import static java.util.Arrays.asList;
import static org.apache.commons.collections.CollectionUtils.isEqualCollection;

/**
 * See additional tests in {@link org.apache.hadoop.hive.ql.lockmgr.TestDbTxnManager}
 * Tests here are "end-to-end"ish and simulate concurrent queries.
 *
 * The general approach is to use an instance of Driver to use Driver.run() to create tables
 * Use Driver.compileAndRespond() (which also starts a txn) to generate QueryPlan which can then be
 * passed to HiveTxnManager.acquireLocks().
 * Same HiveTxnManager is used to commitTxn()/rollback etc.  This can exercise almost the entire
 * code path that CLI would but with the advantage that you can create a 2nd HiveTxnManager and then
 * simulate interleaved transactional/locking operations but all from within a single thread.
 * The later not only controls concurrency precisely but is the only way to run in UT env with DerbyDB.
 *
 * A slightly different (and simpler) approach is to use "start transaction/(commit/rollback)"
 * command with the Driver.run().  This allows you to "see" the state of the Lock Manager after
 * each statement and can also simulate concurrent (but very controlled) work but w/o forking any
 * threads.  The limitation here is that not all statements are allowed in an explicit transaction.
 * For example, "drop table foo".  This approach will also cause the query to execute which will
 * make tests slower but will exercise the code path that is much closer to the actual user calls.
 *
 * In either approach, each logical "session" should use it's own Transaction Manager.  This requires
 * using {@link #swapTxnManager(HiveTxnManager)} since in the SessionState the TM is associated with
 * each thread.
 */
public class TestDbTxnManager2 extends DbTxnManagerEndToEndTestBase {

  /**
   * HIVE-16688
   */
  @Test
  public void testMetadataOperationLocks() throws Exception {
    boolean isStrict = conf.getBoolVar(HiveConf.ConfVars.HIVE_TXN_STRICT_LOCKING_MODE);
    //to make insert into non-acid take shared_read lock
    conf.setBoolVar(HiveConf.ConfVars.HIVE_TXN_STRICT_LOCKING_MODE, false);
    dropTable(new String[] {"T"});
    driver.run("create table if not exists T (a int, b int)");
    driver.compileAndRespond("insert into T values (1,2)", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer");
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    //since LM is using non strict mode we get shared_read lock
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T", null, locks);

    //simulate concurrent session
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver.compileAndRespond("alter table T SET TBLPROPERTIES ('transactional'='true')", true);
    ((DbTxnManager)txnMgr2).acquireLocks(driver.getPlan(), ctx, "Fiddler", false);
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T", null, locks);
    TestTxnDbUtil.checkLock(LockType.EXCLUSIVE, LockState.WAITING, "default", "T", null, locks);
    txnMgr2.rollbackTxn();
    txnMgr.releaseLocks(ctx.getHiveLocks());
    conf.setBoolVar(HiveConf.ConfVars.HIVE_TXN_STRICT_LOCKING_MODE, isStrict);
  }

  @Test
  public void testLocksInSubquery() throws Exception {
    testLocksInSubquery(false);
  }
  @Test
  public void testLocksInSubquerySharedWrite() throws Exception {
    testLocksInSubquery(true);
  }

  private void testLocksInSubquery(boolean sharedWrite) throws Exception {
    dropTable(new String[] {"T", "S", "R"});
    conf.setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, !sharedWrite);

    driver.run("create table if not exists T (a int, b int)");
    driver.run("create table if not exists S (a int, b int) " +
        "clustered by(b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("create table if not exists R (a int, b int) " +
        "clustered by(b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");

    driver.compileAndRespond("delete from S where a in (select a from T where b = 1)", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "one");
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T", null, locks);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        LockState.ACQUIRED, "default", "S", null, locks);
    txnMgr.rollbackTxn();

    driver.compileAndRespond("update S set a = 7 where a in (select a from T where b = 1)", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "one");
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T", null, locks);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        LockState.ACQUIRED, "default", "S", null, locks);
    txnMgr.rollbackTxn();

    driver.compileAndRespond("insert into R select * from S where a in (select a from T where b = 1)", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "three");
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 3, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T", null, locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "S", null, locks);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.SHARED_READ),
        LockState.ACQUIRED, "default", "R", null, locks);
    txnMgr.rollbackTxn();
  }

  @Test
  public void testCreateTable() throws Exception {
    dropTable(new String[] {"T"});
    driver.compileAndRespond("create table if not exists T (a int, b int)", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer");
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", null, null, locks);
    txnMgr.releaseLocks(ctx.getHiveLocks());
    Assert.assertEquals("Lock remained", 0, getLocks().size());
  }

  @Test
  public void testInsertOverwriteCreate() throws Exception {
    testInsertOverwriteCreate(false, false);
  }
  @Test
  public void testInsertOverwriteCreateAcid() throws Exception {
    testInsertOverwriteCreate(true, false);
  }
  @Test
  public void testInsertOverwriteCreateSharedWrite() throws Exception {
    testInsertOverwriteCreate(true, true);
  }

  private void testInsertOverwriteCreate(boolean isTransactional, boolean sharedWrite) throws Exception {
    dropTable(new String[] {"T2", "T3"});
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.CREATE_TABLES_AS_ACID, isTransactional);
    conf.setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, !sharedWrite);
    driver.run("create table if not exists T2(a int)");
    driver.run("create table T3(a int) stored as ORC");
    driver.compileAndRespond("insert overwrite table T3 select a from T2", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer");
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T2", null, locks);
    TestTxnDbUtil.checkLock((isTransactional && sharedWrite) ? LockType.EXCL_WRITE : LockType.EXCLUSIVE,
        LockState.ACQUIRED, "default", "T3", null, locks);
    if (isTransactional) {
      txnMgr.commitTxn();
    } else {
      txnMgr.releaseLocks(ctx.getHiveLocks());
    }
    Assert.assertEquals("Lock remained", 0, getLocks().size());
    driver.run("drop table if exists T1");
    driver.run("drop table if exists T2");
  }

  @Test
  public void testInsertOverwritePartitionedCreate() throws Exception {
    testInsertOverwritePartitionedCreate(false, false);
  }
  @Test
  public void testInsertOverwritePartitionedCreateAcid() throws Exception {
    testInsertOverwritePartitionedCreate(true, false);
  }
  @Test
  public void testInsertOverwritePartitionedCreateSharedWrite() throws Exception {
    testInsertOverwritePartitionedCreate(true, true);
  }

  private void testInsertOverwritePartitionedCreate(boolean isTransactional, boolean sharedWrite) throws Exception {
    dropTable(new String[] {"T4", "T5"});
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.CREATE_TABLES_AS_ACID, isTransactional);
    conf.setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, !sharedWrite);
    driver.run("create table T4 (name string, gpa double) partitioned by (age int) stored as ORC");
    driver.run("create table T5(name string, age int, gpa double)");
    driver.compileAndRespond("INSERT OVERWRITE TABLE T4 PARTITION (age) SELECT  name, age, gpa FROM T5", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer");
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T5", null, locks);
    TestTxnDbUtil.checkLock((isTransactional && sharedWrite) ? LockType.EXCL_WRITE : LockType.EXCLUSIVE,
        LockState.ACQUIRED, "default", "T4", null, locks);
    if (isTransactional) {
      txnMgr.commitTxn();
    } else {
      txnMgr.releaseLocks(ctx.getHiveLocks());
    }
    Assert.assertEquals("Lock remained", 0, getLocks().size());
    driver.run("drop table if exists T5");
    driver.run("drop table if exists T4");
  }

  @Test
  public void testBasicBlocking() throws Exception {
    dropTable(new String[] {"T6"});
    driver.run("create table if not exists T6(a int)");
    driver.compileAndRespond("select a from T6", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer"); //gets S lock on T6
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnMgr2.openTxn(ctx, "Fidler");
    swapTxnManager(txnMgr2);
    driver.compileAndRespond("drop table if exists T6", true);
    //tries to get X lock on T1 and gets Waiting state
    ((DbTxnManager) txnMgr2).acquireLocks(driver.getPlan(), new Context(conf), "Fiddler", false);
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 3, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T6", null, locks);
    TestTxnDbUtil.checkLock(LockType.EXCLUSIVE, LockState.WAITING, "default", "T6", null, locks);
    txnMgr.releaseLocks(ctx.getHiveLocks()); //release S on T6
    //attempt to X on T6 again - succeed
    ((DbLockManager)txnMgr.getLockManager()).checkLock(locks.get(1).getLockid());
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "T6", null, locks);
    txnMgr2.rollbackTxn();
    driver.run("drop table if exists T6");
    locks = getLocks();
    Assert.assertEquals("Unexpected number of locks found", 0, locks.size());
  }

  @Test
  public void testLockConflictDbTable() throws Exception {
    dropTable(new String[] {"temp.T7"});
    driver.run("create database if not exists temp");
    driver.run("create table if not exists temp.T7(a int, b int) " +
        "clustered by(b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.compileAndRespond("update temp.T7 set a = 5 where b = 6", true); //gets SS lock on T7
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer");
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver.compileAndRespond("drop database if exists temp", true);
    ((DbTxnManager)txnMgr2).acquireLocks(driver.getPlan(), ctx, "Fiddler", false);
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "temp", "T7", null, locks);
    TestTxnDbUtil.checkLock(LockType.EXCLUSIVE, LockState.WAITING, "temp", null, null, locks);
    txnMgr.commitTxn();
    ((DbLockManager)txnMgr2.getLockManager()).checkLock(locks.get(1).getLockid());
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "temp", null, null, locks);
    txnMgr2.commitTxn();
  }

  @Test
  public void testUpdateSelectUpdate() throws Exception {
    testUpdateSelectUpdate(false);
  }
  @Test
  public void testUpdateSelectUpdateSharedWrite() throws Exception {
    testUpdateSelectUpdate(true);
  }

  private void testUpdateSelectUpdate(boolean sharedWrite) throws Exception {
    dropTable(new String[] {"T8"});
    conf.setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, !sharedWrite);
    driver.run("create table T8(a int, b int) " +
        "clustered by(b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.compileAndRespond("delete from T8 where b = 89", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer"); //gets SS lock on T8
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver.run("start transaction");
    driver.compileAndRespond("select a from T8", true); //gets S lock on T8
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "Fiddler");
    driver.compileAndRespond("update T8 set a = 1 where b = 1", true);
    ((DbTxnManager) txnMgr2).acquireLocks(driver.getPlan(), ctx, "Practical", false); //waits for SS lock on T8 from fifer
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 3, locks.size());
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.SHARED_READ),
        LockState.ACQUIRED, "default", "T8", null, locks);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        LockState.ACQUIRED, "default", "T8", null, locks);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        (sharedWrite ? LockState.ACQUIRED : LockState.WAITING), "default", "T8", null, locks);
    driver.releaseLocksAndCommitOrRollback(false, txnMgr);
    ((DbLockManager)txnMgr2.getLockManager()).checkLock(locks.get(2).getLockid());
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T8", null, locks);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        LockState.ACQUIRED, "default", "T8", null, locks);
    driver.releaseLocksAndCommitOrRollback(true, txnMgr2);
    swapTxnManager(txnMgr);
    driver.run("drop table if exists T6");
    locks = getLocks();
    Assert.assertEquals("Unexpected number of locks found", 0, locks.size());
  }

  @Test
  public void testLockRetryLimit() throws Exception {
    dropTable(new String[] {"T9"});
    conf.setIntVar(HiveConf.ConfVars.HIVE_LOCK_NUMRETRIES, 2);
    conf.setBoolVar(HiveConf.ConfVars.TXN_MGR_DUMP_LOCK_STATE_ON_ACQUIRE_TIMEOUT, true);
    driver.run("create table T9(a int)");
    driver.compileAndRespond("select * from T9", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Vincent Vega");
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T9", null, locks);
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver.compileAndRespond("drop table T9", true);
    try {
      txnMgr2.acquireLocks(driver.getPlan(), ctx, "Winston Winnfield");
    }
    catch(LockException ex) {
      Assert.assertEquals("Got wrong lock exception", ErrorMsg.LOCK_ACQUIRE_TIMEDOUT, ex.getCanonicalErrorMsg());
    }
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T9", null, locks);
    txnMgr2.closeTxnManager();
  }

  /**
   * check that locks in Waiting state show what they are waiting on
   * This test is somewhat abusive in that it make DbLockManager retain locks for 2
   * different queries (which are not part of the same transaction) which can never
   * happen in real use cases... but it makes testing convenient.
   */
  @Test
  public void testLockBlockedBy() throws Exception {
    dropTable(new String[] {"TAB_BLOCKED"});
    driver.run("create table TAB_BLOCKED (a int, b int) " +
        "clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.compileAndRespond("select * from TAB_BLOCKED", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "I AM SAM");
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "TAB_BLOCKED", null, locks);
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver.compileAndRespond("drop table TAB_BLOCKED", true);
    ((DbTxnManager)txnMgr2).acquireLocks(driver.getPlan(), ctx, "SAM I AM", false); //make non-blocking
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 3, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "TAB_BLOCKED", null, locks);
    TestTxnDbUtil.checkLock(LockType.EXCLUSIVE, LockState.WAITING, "default", "TAB_BLOCKED", null, locks);
    Assert.assertEquals("BlockedByExtId doesn't match", locks.get(0).getLockid(), locks.get(2).getBlockedByExtId());
    Assert.assertEquals("BlockedByIntId doesn't match", locks.get(0).getLockIdInternal(), locks.get(2).getBlockedByIntId());
  }

  @Test
  public void testDummyTxnManagerOnAcidTable() throws Exception {
    dropTable(new String[] {"T10", "T11"});
    // Create an ACID table with DbTxnManager
    driver.run("create table T10 (a int, b int) " +
        "clustered by(b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("create table T11 (a int, b int) clustered by(b) into 2 buckets stored as orc");

    // All DML should fail with DummyTxnManager on ACID table
    useDummyTxnManagerTemporarily(conf);
    try {
      driver.compileAndRespond("select * from T10", true);
      assert false;
    } catch (CommandProcessorException e) {
      Assert.assertEquals(ErrorMsg.TXNMGR_NOT_ACID.getErrorCode(), e.getResponseCode());
      Assert.assertTrue(e.getMessage().contains("This command is not allowed on an ACID table"));
    }

    useDummyTxnManagerTemporarily(conf);
    try {
      driver.compileAndRespond("insert into table T10 values (1, 2)", true);
    } catch (CommandProcessorException e) {
      Assert.assertEquals(ErrorMsg.TXNMGR_NOT_ACID.getErrorCode(), e.getResponseCode());
      Assert.assertTrue(e.getMessage().contains("This command is not allowed on an ACID table"));
    }

    useDummyTxnManagerTemporarily(conf);
    try {
      driver.compileAndRespond("update T10 set a=0 where b=1", true);
    } catch (CommandProcessorException e) {
      Assert.assertEquals(ErrorMsg.ACID_OP_ON_NONACID_TXNMGR.getErrorCode(), e.getResponseCode());
      Assert.assertTrue(e.getMessage().contains(
          "Attempt to do update or delete using transaction manager that does not support these operations."));
    }

    useDummyTxnManagerTemporarily(conf);
    try {
      driver.compileAndRespond("delete from T10", true);
    } catch (CommandProcessorException e) {
      Assert.assertEquals(ErrorMsg.ACID_OP_ON_NONACID_TXNMGR.getErrorCode(), e.getResponseCode());
      Assert.assertTrue(e.getMessage().contains(
          "Attempt to do update or delete using transaction manager that does not support these operations."));
    }
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
   */
  @Test
  public void testMetastoreTablesCleanup() throws Exception {
    dropTable(new String[] {"temp.T10", "temp.T11", "temp.T12p", "temp.T13p"});
    driver.run("create database if not exists temp");

    // Create some ACID tables: T10, T11 - unpartitioned table, T12p, T13p - partitioned table
    driver.run("create table temp.T10 (a int, b int) " +
        "clustered by(b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("create table temp.T11 (a int, b int) " +
        "clustered by(b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("create table temp.T12p (a int, b int) partitioned by (ds string, hour string) " +
        "clustered by(b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("create table temp.T13p (a int, b int) partitioned by (ds string, hour string) " +
        "clustered by(b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");

    // Successfully insert some data into ACID tables, so that we have records in COMPLETED_TXN_COMPONENTS
    driver.run("insert into temp.T10 values (1, 1)");
    driver.run("insert into temp.T10 values (2, 2)");
    driver.run("insert into temp.T11 values (3, 3)");
    driver.run("insert into temp.T11 values (4, 4)");
    driver.run("insert into temp.T12p partition (ds='today', hour='1') values (5, 5)");
    driver.run("insert into temp.T12p partition (ds='tomorrow', hour='2') values (6, 6)");
    driver.run("insert into temp.T12p partition (ds='tomorrow', hour='2') values (13, 13)");
    driver.run("insert into temp.T13p partition (ds='today', hour='1') values (7, 7)");
    driver.run("insert into temp.T13p partition (ds='tomorrow', hour='2') values (8, 8)");
    int count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\" " +
        "where \"CTC_DATABASE\"='temp' and \"CTC_TABLE\" in ('t10', 't11')");
    Assert.assertEquals(4, count);
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\" " +
        "where \"CTC_DATABASE\"='temp' and \"CTC_TABLE\" in ('t12p', 't13p')");
    Assert.assertEquals(5, count);

    // Fail some inserts, so that we have records in TXN_COMPONENTS
    conf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, true);
    driver.run("insert into temp.T10 values (9, 9)");
    driver.run("insert into temp.T11 values (10, 10)");
    driver.run("insert into temp.T12p partition (ds='today', hour='1') values (11, 11)");
    driver.run("insert into temp.T13p partition (ds='today', hour='1') values (12, 12)");
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"TXN_COMPONENTS\" " +
        "where \"TC_DATABASE\"='temp' and \"TC_TABLE\" in ('t10', 't11', 't12p', 't13p')");
    Assert.assertEquals(4, count);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_ROLLBACK_TXN, false);

    // Drop a table/partition; corresponding records in TXN_COMPONENTS and COMPLETED_TXN_COMPONENTS should disappear
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"TXN_COMPONENTS\" " +
        "where \"TC_DATABASE\"='temp' and \"TC_TABLE\"='t10'");
    Assert.assertEquals(1, count);
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\" " +
        "where \"CTC_DATABASE\"='temp' and \"CTC_TABLE\"='t10'");
    Assert.assertEquals(2, count);
    driver.run("drop table temp.T10");
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"TXN_COMPONENTS\" " +
        "where \"TC_DATABASE\"='temp' and \"TC_TABLE\"='t10'");
    Assert.assertEquals(0, count);
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\" " +
        "where \"CTC_DATABASE\"='temp' and \"CTC_TABLE\"='t10'");
    Assert.assertEquals(0, count);

    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"TXN_COMPONENTS\" " +
        "where \"TC_DATABASE\"='temp' and \"TC_TABLE\"='t12p' and \"TC_PARTITION\"='ds=today/hour=1'");
    Assert.assertEquals(1, count);
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\" " +
        "where \"CTC_DATABASE\"='temp' and \"CTC_TABLE\"='t12p' and \"CTC_PARTITION\"='ds=today/hour=1'");
    Assert.assertEquals(1, count);
    driver.run("alter table temp.T12p drop partition (ds='today', hour='1')");
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"TXN_COMPONENTS\" " +
        "where \"TC_DATABASE\"='temp' and \"TC_TABLE\"='t12p' and \"TC_PARTITION\"='ds=today/hour=1'");
    Assert.assertEquals(0, count);
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\" " +
        "where \"CTC_DATABASE\"='temp' and \"CTC_TABLE\"='t12p' and \"CTC_PARTITION\"='ds=today/hour=1'");
    Assert.assertEquals(0, count);

    // Successfully perform compaction on a table/partition, so that we have successful records in COMPLETED_COMPACTIONS
    driver.run("alter table temp.T11 compact 'minor'");
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPACTION_QUEUE\" " +
        "where \"CQ_DATABASE\"='temp' and \"CQ_TABLE\"='t11' and \"CQ_STATE\"='i' and \"CQ_TYPE\"='i'");
    Assert.assertEquals(1, count);
    runWorker(conf);
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPACTION_QUEUE\" " +
        "where \"CQ_DATABASE\"='temp' and \"CQ_TABLE\"='t11' and \"CQ_STATE\"='r' and \"CQ_TYPE\"='i'");
    Assert.assertEquals(1, count);
    runCleaner(conf);
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPACTION_QUEUE\" " +
        "where \"CQ_DATABASE\"='temp' and \"CQ_TABLE\"='t11'");
    Assert.assertEquals(0, count);
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_COMPACTIONS\" " +
        "where \"CC_DATABASE\"='temp' and \"CC_TABLE\"='t11' and \"CC_STATE\"='s' and \"CC_TYPE\"='i'");
    Assert.assertEquals(1, count);

    driver.run("alter table temp.T12p partition (ds='tomorrow', hour='2') compact 'minor'");
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPACTION_QUEUE\" " +
        "where \"CQ_DATABASE\"='temp' and \"CQ_TABLE\"='t12p' and \"CQ_PARTITION\"='ds=tomorrow/hour=2' " +
        "and \"CQ_STATE\"='i' and \"CQ_TYPE\"='i'");
    Assert.assertEquals(1, count);
    runWorker(conf);
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPACTION_QUEUE\" " +
        "where \"CQ_DATABASE\"='temp' and \"CQ_TABLE\"='t12p' and \"CQ_PARTITION\"='ds=tomorrow/hour=2' " +
        "and \"CQ_STATE\"='r' and \"CQ_TYPE\"='i'");
    Assert.assertEquals(1, count);
    runCleaner(conf);
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPACTION_QUEUE\" " +
        "where \"CQ_DATABASE\"='temp' and \"CQ_TABLE\"='t12p'");
    Assert.assertEquals(0, count);
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_COMPACTIONS\" " +
        "where \"CC_DATABASE\"='temp' and \"CC_TABLE\"='t12p' and \"CC_STATE\"='s' and \"CC_TYPE\"='i'");
    Assert.assertEquals(1, count);

    // Fail compaction, so that we have failed records in COMPLETED_COMPACTIONS.
    // Tables need at least 2 delta files to compact, and minor compaction was just run, so insert
    driver.run("insert into temp.T11 values (14, 14)");
    driver.run("insert into temp.T12p partition (ds='tomorrow', hour='2') values (15, 15)");
    conf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_COMPACTION, true);
    driver.run("alter table temp.T11 compact 'major'");
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPACTION_QUEUE\" " +
        "where \"CQ_DATABASE\"='temp' and \"CQ_TABLE\"='t11' and \"CQ_STATE\"='i' and \"CQ_TYPE\"='a'");
    Assert.assertEquals(1, count);
    runWorker(conf); // will fail
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPACTION_QUEUE\" " +
        "where \"CQ_DATABASE\"='temp' and \"CQ_TABLE\"='t11' and \"CQ_STATE\"='i' and \"CQ_TYPE\"='a'");
    Assert.assertEquals(0, count);
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_COMPACTIONS\" " +
        "where \"CC_DATABASE\"='temp' and \"CC_TABLE\"='t11' and \"CC_STATE\"='f' and \"CC_TYPE\"='a'");
    Assert.assertEquals(1, count);

    driver.run("alter table temp.T12p partition (ds='tomorrow', hour='2') compact 'major'");
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPACTION_QUEUE\" " +
        "where \"CQ_DATABASE\"='temp' and \"CQ_TABLE\"='t12p' and \"CQ_PARTITION\"='ds=tomorrow/hour=2' " +
        "and \"CQ_STATE\"='i' and \"CQ_TYPE\"='a'");
    Assert.assertEquals(1, count);
    runWorker(conf); // will fail
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPACTION_QUEUE\" " +
        "where \"CQ_DATABASE\"='temp' and \"CQ_TABLE\"='t12p' and \"CQ_PARTITION\"='ds=tomorrow/hour=2' " +
        "and \"CQ_STATE\"='i' and \"CQ_TYPE\"='a'");
    Assert.assertEquals(0, count);
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_COMPACTIONS\" " +
        "where \"CC_DATABASE\"='temp' and \"CC_TABLE\"='t12p' and \"CC_STATE\"='f' and \"CC_TYPE\"='a'");
    Assert.assertEquals(1, count);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_COMPACTION, false);

    // Put 2 records into COMPACTION_QUEUE and do nothing
    driver.run("alter table temp.T11 compact 'major'");
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPACTION_QUEUE\" " +
        "where \"CQ_DATABASE\"='temp' and \"CQ_TABLE\"='t11' and \"CQ_STATE\"='i' and \"CQ_TYPE\"='a'");
    Assert.assertEquals(1, count);
    driver.run("alter table temp.T12p partition (ds='tomorrow', hour='2') compact 'major'");
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPACTION_QUEUE\" " +
        "where \"CQ_DATABASE\"='temp' and \"CQ_TABLE\"='t12p' and \"CQ_PARTITION\"='ds=tomorrow/hour=2' " +
        "and \"CQ_STATE\"='i' and \"CQ_TYPE\"='a'");
    Assert.assertEquals(1, count);

    // Drop a table/partition, corresponding records in COMPACTION_QUEUE and COMPLETED_COMPACTIONS should disappear
    driver.run("drop table temp.T11");
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPACTION_QUEUE\" " +
        "where \"CQ_DATABASE\"='temp' and \"CQ_TABLE\"='t11'");
    Assert.assertEquals(0, count);
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_COMPACTIONS\" " +
        "where \"CC_DATABASE\"='temp' and \"CC_TABLE\"='t11'");
    Assert.assertEquals(0, count);

    driver.run("alter table temp.T12p drop partition (ds='tomorrow', hour='2')");
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPACTION_QUEUE\" " +
        "where \"CQ_DATABASE\"='temp' and \"CQ_TABLE\"='t12p'");
    Assert.assertEquals(0, count);
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_COMPACTIONS\" " +
        "where \"CC_DATABASE\"='temp' and \"CC_TABLE\"='t12p'");
    Assert.assertEquals(0, count);

    // Put 1 record into COMPACTION_QUEUE and do nothing
    driver.run("alter table temp.T13p partition (ds='today', hour='1') compact 'major'");
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPACTION_QUEUE\" " +
        "where \"CQ_DATABASE\"='temp' and \"CQ_TABLE\"='t13p' and \"CQ_STATE\"='i' and \"CQ_TYPE\"='a'");
    Assert.assertEquals(1, count);

    // Drop database, everything in all 4 meta tables should disappear
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"TXN_COMPONENTS\" " +
        "where \"TC_DATABASE\"='temp' and \"TC_TABLE\" in ('t10', 't11', 't12p', 't13p')");
    Assert.assertEquals(1, count);
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\" " +
        "where \"CTC_DATABASE\"='temp' and \"CTC_TABLE\" in ('t10', 't11', 't12p', 't13p')");
    Assert.assertEquals(2, count);
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPACTION_QUEUE\" " +
        "where \"CQ_DATABASE\"='temp' and \"CQ_TABLE\" in ('t10', 't11', 't12p', 't13p')");
    Assert.assertEquals(1, count);
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_COMPACTIONS\" " +
        "where \"CC_DATABASE\"='temp' and \"CC_TABLE\" in ('t10', 't11', 't12p', 't13p')");
    Assert.assertEquals(0, count);
    driver.run("drop database if exists temp cascade");
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"TXN_COMPONENTS\" " +
        "where \"TC_DATABASE\"='temp' and \"TC_TABLE\" in ('t10', 't11', 't12p', 't13p')");
    Assert.assertEquals(0, count);
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\" " +
        "where \"CTC_DATABASE\"='temp' and \"CTC_TABLE\" in ('t10', 't11', 't12p', 't13p')");
    Assert.assertEquals(0, count);
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPACTION_QUEUE\" " +
        "where \"CQ_DATABASE\"='temp' and \"CQ_TABLE\" in ('t10', 't11', 't12p', 't13p')");
    Assert.assertEquals(0, count);
    count = TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_COMPACTIONS\" " +
        "where \"CC_DATABASE\"='temp' and \"CC_TABLE\" in ('t10', 't11', 't12p', 't13p')");
    Assert.assertEquals(0, count);
  }

  /**
   * Collection of queries where we ensure that we get the locks that are expected.
   */
  @Test
  public void testCheckExpectedLocks() throws Exception {
    testCheckExpectedLocks(false);
  }
  @Test
  public void testCheckExpectedLocksSharedWrite() throws Exception {
    testCheckExpectedLocks(true);
  }

  private void testCheckExpectedLocks(boolean sharedWrite) throws Exception {
    dropTable(new String[] {"acidPart", "nonAcidPart"});
    conf.setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, !sharedWrite);

    driver.run("create table acidPart(a int, b int) partitioned by (p string) " +
        "clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("create table nonAcidPart(a int, b int) partitioned by (p string) " +
        "stored as orc TBLPROPERTIES ('transactional'='false')");

    driver.compileAndRespond("insert into nonAcidPart partition(p) values(1,2,3)", true);
    ((DbTxnManager) txnMgr).acquireLocks(driver.getPlan(), ctx, "Practical", false);
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "nonAcidPart", null, locks);
    txnMgr.releaseLocks(ctx.getHiveLocks());

    driver.compileAndRespond("insert into nonAcidPart partition(p=1) values(5,6)", true);
    ((DbTxnManager) txnMgr).acquireLocks(driver.getPlan(), ctx, "Practical", false);
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "nonAcidPart", "p=1", locks);
    txnMgr.releaseLocks(ctx.getHiveLocks());

    driver.compileAndRespond("insert into acidPart partition(p) values(1,2,3)", true);
    ((DbTxnManager) txnMgr).acquireLocks(driver.getPlan(), ctx, "Practical", false);
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.SHARED_READ),
        LockState.ACQUIRED, "default", "acidPart", null, locks);
    txnMgr.rollbackTxn();

    driver.compileAndRespond("insert into acidPart partition(p=1) values(5,6)", true);
    ((DbTxnManager) txnMgr).acquireLocks(driver.getPlan(), ctx, "Practical", false);
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.SHARED_READ),
        LockState.ACQUIRED, "default", "acidPart", "p=1", locks);
    txnMgr.rollbackTxn();

    driver.compileAndRespond("update acidPart set b = 17 where a = 1", true);
    ((DbTxnManager) txnMgr).acquireLocks(driver.getPlan(), ctx, "Practical", false);
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        LockState.ACQUIRED, "default", "acidPart", null, locks);
    txnMgr.rollbackTxn();

    driver.compileAndRespond("update acidPart set b = 17 where p = 1", true);
    ((DbTxnManager) txnMgr).acquireLocks(driver.getPlan(), ctx, "Practical", false);
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    //https://issues.apache.org/jira/browse/HIVE-13212
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        LockState.ACQUIRED, "default", "acidPart", null, locks);
    txnMgr.rollbackTxn();
  }

  /**
   * Check to make sure we acquire proper locks for queries involving acid and non-acid tables
   */
  @Test
  public void testCheckExpectedLocks2() throws Exception {
    dropTable(new String[] {"tab_acid", "tab_not_acid"});
    driver.run("create table if not exists tab_acid (a int, b int) partitioned by (p string) " +
        "clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("create table if not exists tab_not_acid (na int, nb int) partitioned by (np string) " +
        "clustered by (na) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='false')");
    driver.run("insert into tab_acid partition(p) (a,b,p) values(1,2,'foo'),(3,4,'bar')");
    driver.run("insert into tab_not_acid partition(np) (na,nb,np) values(1,2,'blah'),(3,4,'doh')");
    txnMgr.openTxn(ctx, "T1");
    driver.compileAndRespond("select * from tab_acid inner join tab_not_acid on a = na", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "T1");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 4, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_acid", "p=bar", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_acid", "p=foo", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_not_acid", "np=blah", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_not_acid", "np=doh", locks);

    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnMgr2.openTxn(ctx, "T2");
    driver.compileAndRespond("insert into tab_not_acid partition(np='doh') values(5,6)", true);
    ((DbTxnManager)txnMgr2).acquireLocks(driver.getPlan(), ctx, "T2", false);
    locks = getLocks(txnMgr2);
    Assert.assertEquals("Unexpected lock count", 5, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_acid", "p=bar", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_acid", "p=foo", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_not_acid", "np=blah", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_not_acid", "np=doh", locks);
    TestTxnDbUtil.checkLock(LockType.EXCLUSIVE, LockState.WAITING, "default", "tab_not_acid", "np=doh", locks);

    // Test strict locking mode, i.e. backward compatible locking mode for non-ACID resources.
    // With non-strict mode, INSERT got SHARED_READ lock, instead of EXCLUSIVE with ACID semantics
    conf.setBoolVar(HiveConf.ConfVars.HIVE_TXN_STRICT_LOCKING_MODE, false);
    HiveTxnManager txnMgr3 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnMgr3.openTxn(ctx, "T3");
    driver.compileAndRespond("insert into tab_not_acid partition(np='blah') values(7,8)", true);
    ((DbTxnManager)txnMgr3).acquireLocks(driver.getPlan(), ctx, "T3", false);
    locks = getLocks(txnMgr3);
    Assert.assertEquals("Unexpected lock count", 6, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_acid", "p=bar", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_acid", "p=foo", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_not_acid", "np=blah", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_not_acid", "np=doh", locks);
    TestTxnDbUtil.checkLock(LockType.EXCLUSIVE, LockState.WAITING, "default", "tab_not_acid", "np=doh", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_not_acid", "np=blah", locks);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_TXN_STRICT_LOCKING_MODE, true);
  }

  /**
   * Check to make sure we acquire proper locks for queries involving non-strict locking
   */
  @Test
  public void testCheckExpectedReadLocksForNonAcidTables() throws Exception {
    dropTable(new String[] {"tab_acid", "tab_not_acid"});
    driver.run("create table if not exists tab_acid (a int, b int) partitioned by (p string) " +
        "clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("create table if not exists tab_not_acid (na int, nb int) partitioned by (np string) " +
        "clustered by (na) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='false')");
    driver.run("insert into tab_acid partition(p) (a,b,p) values(1,2,'foo'),(3,4,'bar')");
    driver.run("insert into tab_not_acid partition(np) (na,nb,np) values(1,2,'blah'),(3,4,'doh')");

    // Test non-acid read-locking mode - the read locks are only obtained for the ACID side
    conf.setBoolVar(HiveConf.ConfVars.HIVE_TXN_NONACID_READ_LOCKS, false);

    HiveTxnManager txnMgr1 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnMgr1.openTxn(ctx, "T1");
    driver.compileAndRespond("select * from tab_acid inner join tab_not_acid on a = na", true);
    txnMgr1.acquireLocks(driver.getPlan(), ctx, "T1");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr1);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_acid", "p=bar", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_acid", "p=foo", locks);

    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnMgr2.openTxn(ctx, "T2");
    driver.compileAndRespond("insert into tab_not_acid partition(np='doh') values(5,6)", true);
    ((DbTxnManager)txnMgr2).acquireLocks(driver.getPlan(), ctx, "T2", false);
    locks = getLocks(txnMgr2);
    Assert.assertEquals("Unexpected lock count", 3, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_acid", "p=bar", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_acid", "p=foo", locks);
    TestTxnDbUtil.checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "tab_not_acid", "np=doh", locks);

    HiveTxnManager txnMgr3 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnMgr3.openTxn(ctx, "T3");
    driver.compileAndRespond("insert into tab_not_acid partition(np='blah') values(7,8)", true);
    ((DbTxnManager)txnMgr3).acquireLocks(driver.getPlan(), ctx, "T3", false);
    locks = getLocks(txnMgr3);
    Assert.assertEquals("Unexpected lock count", 4, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_acid", "p=bar", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_acid", "p=foo", locks);
    TestTxnDbUtil.checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "tab_not_acid", "np=blah", locks);

    conf.setBoolVar(HiveConf.ConfVars.HIVE_TXN_NONACID_READ_LOCKS,
        HiveConf.ConfVars.HIVE_TXN_NONACID_READ_LOCKS.defaultBoolVal);
  }

  @Test
  public void testLockingOnInsertIntoNonNativeTables() throws Exception {
    dropTable(new String[] {"tab_not_acid"});
    driver.run("create table if not exists tab_not_acid (a int, b int) " +
        " STORED BY 'org.apache.hadoop.hive.ql.metadata.StorageHandlerMock'");
    txnMgr.openTxn(ctx, "T1");
    driver.compileAndRespond("insert into tab_not_acid values(1,2)", true);

    txnMgr.acquireLocks(driver.getPlan(), ctx, "T1");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_not_acid", null, locks);
  }

  @Test
  public void testLockingOnInsertOverwriteNonNativeTables() throws Exception {
    dropTable(new String[] {"tab_not_acid"});
    driver.run("create table if not exists tab_not_acid (a int, b int)  " +
        " STORED BY 'org.apache.hadoop.hive.ql.metadata.StorageHandlerMock'");
    txnMgr.openTxn(ctx, "T1");
    driver.compileAndRespond("insert overwrite table tab_not_acid values(1,2)", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "T1");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "tab_not_acid", null, locks);
    txnMgr.rollbackTxn();
    dropTable(new String[] {"tab_not_acid"});
  }

  @Test
  public void testLockingExternalInStrictModeInsert() throws Exception {
    dropTable(new String[] {"tab_not_acid"});
    driver.run("create external table if not exists tab_not_acid (na int, nb int) partitioned by (np string) " +
        "clustered by (na) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='false')");
    driver.run("insert into tab_not_acid partition(np) (na,nb,np) values(1,2,'blah'),(3,4,'doh')");

    conf.setBoolVar(HiveConf.ConfVars.HIVE_TXN_EXT_LOCKING_ENABLED, true);
    HiveTxnManager txnMgr = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnMgr.openTxn(ctx, "T1");
    driver.compileAndRespond("insert into tab_not_acid partition(np='blah') values(7,8)", true);
    ((DbTxnManager)txnMgr).acquireLocks(driver.getPlan(), ctx, "T1", false);
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "tab_not_acid", "np=blah", locks);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_TXN_EXT_LOCKING_ENABLED, false);
  }

  @Test
  public void testLockingExternalInStrictModeSelect() throws Exception {
    dropTable(new String[] {"tab_not_acid"});
    driver.run("create external table if not exists tab_not_acid (na int, nb int) " +
        "stored as orc TBLPROPERTIES ('transactional'='false')");
    driver.run("insert into tab_not_acid values(1,2),(3,4)");

    conf.setBoolVar(HiveConf.ConfVars.HIVE_TXN_EXT_LOCKING_ENABLED, true);
    HiveTxnManager txnMgr = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnMgr.openTxn(ctx, "T1");
    driver.compileAndRespond("select * from tab_not_acid", true);
    ((DbTxnManager)txnMgr).acquireLocks(driver.getPlan(), ctx, "T1", false);
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_not_acid", null, locks);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_TXN_EXT_LOCKING_ENABLED, false);
  }

  /**
   * SessionState is stored in ThreadLoacal; UnitTest runs in a single thread (otherwise Derby wedges)
   * {@link HiveTxnManager} instances are per SessionState.
   * So to be able to simulate concurrent locks/transactions s/o forking threads we just swap
   * the TxnManager instance in the session (hacky but nothing is actually threading so it allows us
   * to write good tests)
   */
  public static HiveTxnManager swapTxnManager(HiveTxnManager txnMgr) {
    return SessionState.get().setTxnMgr(txnMgr);
  }

  @Test
  public void testShowLocksFilterOptions() throws Exception {
    driver.run("drop table if exists db1.t14");
    driver.run("drop table if exists db2.t14"); // Note that db1 and db2 have a table with common name
    driver.run("drop table if exists db2.t15");
    driver.run("drop table if exists db2.t16");
    driver.run("drop database if exists db1");
    driver.run("drop database if exists db2");

    driver.run("create database if not exists db1");
    driver.run("create database if not exists db2");
    driver.run("create table if not exists db1.t14 (a int, b int) partitioned by (ds string) " +
        "clustered by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("create table if not exists db2.t14 (a int, b int) " +
        "clustered by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("create table if not exists db2.t15 (a int, b int) " +
        "clustered by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("create table if not exists db2.t16 (a int, b int) " +
        "clustered by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");

    conf.setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, true);
    // Acquire different locks at different levels

    HiveTxnManager txnMgr1 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr1);
    driver.compileAndRespond("insert into table db1.t14 partition (ds='today') values (1, 2)", true);
    txnMgr1.acquireLocks(driver.getPlan(), ctx, "Tom");

    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver.compileAndRespond("insert into table db1.t14 partition (ds='tomorrow') values (3, 4)", true);
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "Jerry");

    HiveTxnManager txnMgr3 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr3);
    driver.compileAndRespond("select * from db2.t15", true);
    txnMgr3.acquireLocks(driver.getPlan(), ctx, "Donald");

    HiveTxnManager txnMgr4 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr4);
    driver.compileAndRespond("select * from db2.t16", true);
    txnMgr4.acquireLocks(driver.getPlan(), ctx, "Hillary");

    HiveTxnManager txnMgr5 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr5);
    driver.compileAndRespond("select * from db2.t14", true);
    txnMgr5.acquireLocks(driver.getPlan(), ctx, "Obama");

    // Simulate SHOW LOCKS with different filter options

    // SHOW LOCKS (no filter)
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 5, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "db1", "t14", "ds=today", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "db1", "t14", "ds=tomorrow", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "db2", "t15", null, locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "db2", "t16", null, locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "db2", "t14", null, locks);

    // SHOW LOCKS db2
    locks = getLocksWithFilterOptions(txnMgr3, "db2", null, null);
    Assert.assertEquals("Unexpected lock count", 3, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "db2", "t15", null, locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "db2", "t16", null, locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "db2", "t14", null, locks);

    // SHOW LOCKS t14
    swapTxnManager(txnMgr);
    driver.run("use db1");
    locks = getLocksWithFilterOptions(txnMgr, null, "t14", null);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "db1", "t14", "ds=today", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "db1", "t14", "ds=tomorrow", locks);
    // Note that it shouldn't show t14 from db2

    // SHOW LOCKS t14 PARTITION ds='today'
    Map<String, String> partSpec = Collections.singletonMap("ds", "today");
    locks = getLocksWithFilterOptions(txnMgr, null, "t14", partSpec);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "db1", "t14", "ds=today", locks);

    // SHOW LOCKS t15
    driver.run("use db2");
    locks = getLocksWithFilterOptions(txnMgr3, null, "t15", null);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "db2", "t15", null, locks);
  }

  private List<ShowLocksResponseElement> getLocks() throws Exception {
    return getLocks(txnMgr);
  }

  private List<ShowLocksResponseElement> getLocks(HiveTxnManager txnMgr) throws Exception {
    ShowLocksResponse rsp = ((DbLockManager)txnMgr.getLockManager()).getLocks();
    return rsp.getLocks();
  }

  /**
   * txns update same resource but do not overlap in time - no conflict.
   */
  @Test
  public void testWriteSetTracking1() throws Exception {
    dropTable(new String[] {"TAB_PART"});
    driver.run("create table if not exists TAB_PART (a int, b int) " +
        "partitioned by (p string) clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");

    driver.compileAndRespond("select * from TAB_PART", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Nicholas");
    txnMgr.commitTxn();
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver.compileAndRespond("update TAB_PART set b = 7 where p = 'blah'", true);
    driver.compileAndRespond("update TAB_PART set b = 7 where p = 'blah'", true);
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "Alexandra");
    txnMgr2.commitTxn();
  }

  private void dropTable(String[] tabs) throws Exception {
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    for (String tab : tabs) {
      driver.run("drop table if exists " + tab);
    }
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
  }

  /**
   * txns overlap in time but do not update same resource - no conflict
   */
  @Test
  public void testWriteSetTracking2() throws Exception {
    dropTable(new String[] {"TAB_PART", "TAB2"});
    driver.run("create table if not exists TAB_PART (a int, b int) " +
        "partitioned by (p string) clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("create table if not exists TAB2 (a int, b int) partitioned by (p string) " +
        "clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");

    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnMgr.openTxn(ctx, "Peter");
    driver.compileAndRespond("update TAB_PART set b = 7 where p = 'blah'", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Peter");
    txnMgr2.openTxn(ctx, "Catherine");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    //note that "update" uses dynamic partitioning thus lock is on the table not partition
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB_PART", null, locks);
    txnMgr.commitTxn();
    driver.compileAndRespond("update TAB2 set b = 9 where p = 'doh'", true);
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "Catherine");
    txnMgr2.commitTxn();
  }

  /**
   * txns overlap and update the same resource - can't commit 2nd txn
   */
  @Test
  public void testWriteSetTracking3() throws Exception {
    dropTable(new String[] {"TAB_PART"});
    driver.run("create table if not exists TAB_PART (a int, b int) " +
        "partitioned by (p string) clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into TAB_PART partition(p='blah') values(1,2)");

    driver.compileAndRespond("update TAB_PART set b = 7 where p = 'blah'", true);
    long txnId = txnMgr.getCurrentTxnId();
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Known");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB_PART", "p=blah", locks);
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver.compileAndRespond("update TAB_PART set b = 7 where p = 'blah'", true);
    long txnId2 = txnMgr2.getCurrentTxnId();
    ((DbTxnManager)txnMgr2).acquireLocks(driver.getPlan(), ctx, "Unknown", false);
    locks = getLocks(txnMgr2); //should not matter which txnMgr is used here
    Assert.assertEquals("Unexpected lock count", 4, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB_PART", "p=blah", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB_PART", "p=blah", locks);
    long writeId = txnMgr.getTableWriteId("default", "TAB_PART");
    AddDynamicPartitions adp = new AddDynamicPartitions(txnId, writeId, "default", "TAB_PART",
      Collections.singletonList("p=blah"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr.commitTxn();

    adp.setTxnid(txnId2);
    writeId = txnMgr2.getTableWriteId("default", "TAB_PART");
    adp.setWriteid(writeId);
    txnHandler.addDynamicPartitions(adp);
    LockException expectedException = null;
    try {
      //with HIVE-15032 this should use static parts and thus not need addDynamicPartitions
      txnMgr2.commitTxn();
    }
    catch (LockException e) {
      expectedException = e;
    }
    Assert.assertNotNull("Didn't get exception", expectedException);
    Assert.assertEquals("Got wrong message code", ErrorMsg.TXN_ABORTED, expectedException.getCanonicalErrorMsg());
    Assert.assertEquals("Exception msg didn't match",
      "Aborting [txnid:"+txnId2+","+txnId2+"] due to a write conflict on default/tab_part/p=blah committed by [txnid:"+txnId+","+txnId2+"] u/u",
      expectedException.getCause().getMessage());
  }

  /**
   * txns overlap, update same resource, simulate multi-stmt txn case
   * Also tests that we kill txn when it tries to acquire lock if we already know it will not be committed
   */
  @Test
  public void testWriteSetTracking4() throws Exception {
    dropTable(new String[] {"TAB_PART", "TAB2"});
    Assert.assertEquals(0, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"WRITE_SET\""));
    driver.run("create table if not exists TAB_PART (a int, b int) " +
        "partitioned by (p string) clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("create table if not exists TAB2 (a int, b int) partitioned by (p string) " +
        "clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");

    txnMgr.openTxn(ctx, "Long Running");
    driver.compileAndRespond("select a from TAB_PART where p = 'blah'", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Long Running");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    //for some reason this just locks the table; if I alter table to add this partition, then
    //we end up locking both table and partition with share_read.  (Plan has 2 ReadEntities)...?
    //same for other locks below
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "TAB_PART", null, locks);

    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnMgr2.openTxn(ctx, "Short Running");
    driver.compileAndRespond("update TAB2 set b = 7 where p = 'blah'", true); //no such partition
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "Short Running");
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "TAB_PART", null, locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB2", null, locks);
    //update stmt has p=blah, thus nothing is actually update and we generate empty dyn part list
    Assert.assertEquals(0, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"WRITE_SET\""));

    AllocateTableWriteIdsRequest rqst = new AllocateTableWriteIdsRequest("default", "tab2");
    rqst.setTxnIds(Collections.singletonList(txnMgr2.getCurrentTxnId()));
    AllocateTableWriteIdsResponse writeIds = txnHandler.allocateTableWriteIds(rqst);
    Assert.assertEquals(txnMgr2.getCurrentTxnId(), writeIds.getTxnToWriteIds().get(0).getTxnId());

    AddDynamicPartitions adp = new AddDynamicPartitions(txnMgr2.getCurrentTxnId(), writeIds.getTxnToWriteIds().get(0).getWriteId(),
        "default", "tab2", Collections.EMPTY_LIST);
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr2.commitTxn();
    //Short Running updated nothing, so we expect 0 rows in WRITE_SET
    Assert.assertEquals(0, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"WRITE_SET\""));

    txnMgr2.openTxn(ctx, "T3");
    driver.compileAndRespond("update TAB2 set b = 7 where p = 'two'", true); //pretend this partition exists
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "T3");
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "TAB_PART", null, locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB2", null, locks); //since TAB2 is empty
    //update stmt has p=blah, thus nothing is actually update and we generate empty dyn part list
    Assert.assertEquals(0, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"WRITE_SET\""));

    rqst = new AllocateTableWriteIdsRequest("default", "tab2");
    rqst.setTxnIds(Collections.singletonList(txnMgr2.getCurrentTxnId()));
    writeIds = txnHandler.allocateTableWriteIds(rqst);
    Assert.assertEquals(txnMgr2.getCurrentTxnId(), writeIds.getTxnToWriteIds().get(0).getTxnId());

    adp = new AddDynamicPartitions(txnMgr2.getCurrentTxnId(), writeIds.getTxnToWriteIds().get(0).getWriteId(),
        "default", "tab2", Collections.singletonList("p=two"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp); //simulate partition update
    txnMgr2.commitTxn();
    Assert.assertEquals("WRITE_SET mismatch: " + TestTxnDbUtil.queryToString(conf, "select * from \"WRITE_SET\""),
        1, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"WRITE_SET\""));

    MetastoreTaskThread houseKeeper = new AcidHouseKeeperService();
    houseKeeper.setConf(conf);
    houseKeeper.run();
    //since T3 overlaps with Long Running (still open) GC does nothing
    Assert.assertEquals(1, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"WRITE_SET\""));
    driver.compileAndRespond("update TAB2 set b = 17 where a = 1", true); //no rows match
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Long Running");

    rqst = new AllocateTableWriteIdsRequest("default", "tab2");
    rqst.setTxnIds(Collections.singletonList(txnMgr.getCurrentTxnId()));
    writeIds = txnHandler.allocateTableWriteIds(rqst);
    Assert.assertEquals(txnMgr.getCurrentTxnId(), writeIds.getTxnToWriteIds().get(0).getTxnId());

    //so generate empty Dyn Part call
    adp = new AddDynamicPartitions(txnMgr.getCurrentTxnId(), writeIds.getTxnToWriteIds().get(0).getWriteId(),
        "default", "tab2", Collections.EMPTY_LIST);
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr.commitTxn();

    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 0, locks.size());
    /*
      The last transaction will always remain in the transaction table, so we will open an other one,
      wait for the timeout period to exceed, then start the initiator that will clean
     */
    txnMgr.openTxn(ctx, "Long Running");
    Thread.sleep(txnHandler.getOpenTxnTimeOutMillis());
    // Now we can clean the write_set
    houseKeeper.run();
    Assert.assertEquals(0, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"WRITE_SET\""));
  }

  /**
   * overlapping txns updating the same resource but 1st one rolls back; 2nd commits
   */
  @Test
  public void testWriteSetTracking5() throws Exception {
    dropTable(new String[] {"TAB_PART"});
    Assert.assertEquals(0, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"WRITE_SET\""));
    driver.run("create table if not exists TAB_PART (a int, b int) " +
      "partitioned by (p string) clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into TAB_PART partition(p='blah') values(1,2)");
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);

    txnMgr.openTxn(ctx, "Known");
    long txnId = txnMgr2.openTxn(ctx, "Unknown");
    driver.compileAndRespond("update TAB_PART set b = 7 where p = 'blah'", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Known");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB_PART", "p=blah", locks);
    driver.compileAndRespond("update TAB_PART set b = 7 where p = 'blah'", true);
    ((DbTxnManager)txnMgr2).acquireLocks(driver.getPlan(), ctx, "Unknown", false);
    locks = getLocks(txnMgr2); //should not matter which txnMgr is used here
    Assert.assertEquals("Unexpected lock count", 4, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB_PART", "p=blah", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB_PART", "p=blah", locks);
    txnMgr.rollbackTxn();

    AllocateTableWriteIdsRequest rqst = new AllocateTableWriteIdsRequest("default", "TAB_PART");
    rqst.setTxnIds(Collections.singletonList(txnId));
    AllocateTableWriteIdsResponse writeIds = txnHandler.allocateTableWriteIds(rqst);
    Assert.assertEquals(txnId, writeIds.getTxnToWriteIds().get(0).getTxnId());

    AddDynamicPartitions adp = new AddDynamicPartitions(txnId, writeIds.getTxnToWriteIds().get(0).getWriteId(),
        "default", "TAB_PART", Collections.singletonList("p=blah"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    Assert.assertEquals(0, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"WRITE_SET\""));
    txnMgr2.commitTxn(); //since conflicting txn rolled back, commit succeeds
    Assert.assertEquals(1, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"WRITE_SET\""));
  }

  /**
   * check that read query concurrent with txn works ok
   */
  @Test
  public void testWriteSetTracking6() throws Exception {
    dropTable(new String[] {"TAB2"});
    Assert.assertEquals(0, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"WRITE_SET\""));
    driver.run("create table if not exists TAB2(a int, b int) clustered " +
      "by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.compileAndRespond("select * from TAB2 where a = 113", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Works");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "TAB2", null, locks);
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver.compileAndRespond("update TAB2 set b = 17 where a = 101", true);
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "Horton");
    Assert.assertEquals(0, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"WRITE_SET\""));
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "TAB2", null, locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB2", null, locks);
    txnMgr2.commitTxn(); //no conflict
    Assert.assertEquals(1, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"WRITE_SET\""));
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "TAB2", null, locks);
    txnMgr.commitTxn();
    /*
     * The last transaction will always remain in the transaction table, so we will open an other one,
     * wait for the timeout period to exceed, then start the initiator that will clean
     */
    txnMgr.openTxn(ctx, "Long Running");
    Thread.sleep(txnHandler.getOpenTxnTimeOutMillis());
    // Now we can clean the write_set
    MetastoreTaskThread writeSetService = new AcidHouseKeeperService();
    writeSetService.setConf(conf);
    writeSetService.run();
    Assert.assertEquals(0, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"WRITE_SET\""));
  }

  /**
   * 2 concurrent txns update different partitions of the same table and succeed
   */
  @Test
  public void testWriteSetTracking7() throws Exception {
    dropTable(new String[] {"tab2", "TAB2"});
    Assert.assertEquals(0, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"WRITE_SET\""));
    driver.run("create table if not exists tab2 (a int, b int) " +
        "partitioned by (p string) clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into tab2 partition(p)(a,b,p) values(1,1,'one'),(2,2,'two')"); //txnid:1
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    //test with predicates such that partition pruning works
    driver.compileAndRespond("update tab2 set b = 7 where p='two'", true);
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "T2");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr2);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB2", "p=two", locks);

    //now start concurrent txn
    swapTxnManager(txnMgr);
    driver.compileAndRespond("update tab2 set b = 7 where p='one'", true);
    ((DbTxnManager)txnMgr).acquireLocks(driver.getPlan(), ctx, "T3", false);
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 4, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB2", "p=two", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB2", "p=one", locks);
    //this simulates the completion of txnid:2
    //this simulates the completion of txnid:idTxnUpdate1
    long writeId = txnMgr2.getTableWriteId("default", "tab2");
    AddDynamicPartitions adp = new AddDynamicPartitions(txnMgr2.getCurrentTxnId(), writeId, "default", "tab2",
        Collections.singletonList("p=two"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr2.commitTxn(); //txnid:idTxnUpdate1
    locks = getLocks(txnMgr2);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB2", "p=one", locks);
    //completion of txnid:idTxnUpdate2
    writeId = txnMgr.getTableWriteId("default", "tab2");
    adp = new AddDynamicPartitions(txnMgr.getCurrentTxnId(), writeId, "default", "tab2",
        Collections.singletonList("p=one"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr.commitTxn(); //txnid:idTxnUpdate2
    //now both txns concurrently updated TAB2 but different partitions.

    Assert.assertEquals("WRITE_SET mismatch: " +
        TestTxnDbUtil.queryToString(conf, "select * from \"WRITE_SET\""),
        1, TestTxnDbUtil.countQueryAgent(conf,
        "select count(*) from \"WRITE_SET\" where \"WS_PARTITION\"='p=one' and \"WS_OPERATION_TYPE\"='u'"));
    Assert.assertEquals("WRITE_SET mismatch: " +
        TestTxnDbUtil.queryToString(conf, "select * from \"WRITE_SET\""),
        1, TestTxnDbUtil.countQueryAgent(conf,
        "select count(*) from \"WRITE_SET\" where \"WS_PARTITION\"='p=two' and \"WS_OPERATION_TYPE\"='u'"));
    //2 from txnid:1, 1 from txnid:2, 1 from txnid:3
    Assert.assertEquals("COMPLETED_TXN_COMPONENTS mismatch: " +
        TestTxnDbUtil.queryToString(conf, "select * from \"COMPLETED_TXN_COMPONENTS\""),
        4, TestTxnDbUtil.countQueryAgent(conf,
        "select count(*) from \"COMPLETED_TXN_COMPONENTS\" where \"CTC_TABLE\"='tab2' and \"CTC_PARTITION\" is not null"));

    //================
    //test with predicates such that partition pruning doesn't kick in
    driver.run("drop table if exists tab1");
    driver.run("create table if not exists tab1 (a int, b int) partitioned by (p string) " +
      "clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into tab1 partition(p)(a,b,p) values(1,1,'one'),(2,2,'two')"); //txnid:4
    swapTxnManager(txnMgr2);
    driver.compileAndRespond("update tab1 set b = 7 where b=1", true);
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "T5");
    locks = getLocks(txnMgr2);
    Assert.assertEquals("Unexpected lock count", 3, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);

    //now start concurrent txn
    swapTxnManager(txnMgr);
    driver.compileAndRespond("update tab1 set b = 7 where b = 2", true);
    ((DbTxnManager)txnMgr).acquireLocks(driver.getPlan(), ctx, "T6", false);
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 6, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);

    //this simulates the completion of txnid:idTxnUpdate3
    writeId = txnMgr2.getTableWriteId("default", "tab1");
    adp = new AddDynamicPartitions(txnMgr2.getCurrentTxnId(), writeId, "default", "tab1",
        Collections.singletonList("p=one"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr2.commitTxn(); //txnid:idTxnUpdate3

    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 3, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);
    //completion of txnid:idTxnUpdate4
    writeId = txnMgr.getTableWriteId("default", "tab1");
    adp = new AddDynamicPartitions(txnMgr.getCurrentTxnId(), writeId, "default", "tab1",
        Collections.singletonList("p=two"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr.commitTxn(); //txnid:idTxnUpdate4

    Assert.assertEquals("WRITE_SET mismatch: " +
        TestTxnDbUtil.queryToString(conf, "select * from \"WRITE_SET\""),
        1, TestTxnDbUtil.countQueryAgent(conf,
        "select count(*) from \"WRITE_SET\" where \"WS_PARTITION\"='p=one' and \"WS_OPERATION_TYPE\"='u' and \"WS_TABLE\"='tab1'"));
    Assert.assertEquals("WRITE_SET mismatch: " +
        TestTxnDbUtil.queryToString(conf, "select * from \"WRITE_SET\""),
        1, TestTxnDbUtil.countQueryAgent(conf,
        "select count(*) from \"WRITE_SET\" where \"WS_PARTITION\"='p=two' and \"WS_OPERATION_TYPE\"='u' and \"WS_TABLE\"='tab1'"));
    //2 from insert + 1 for each update stmt
    Assert.assertEquals("COMPLETED_TXN_COMPONENTS mismatch: " +
        TestTxnDbUtil.queryToString(conf, "select * from \"COMPLETED_TXN_COMPONENTS\""),
        4, TestTxnDbUtil.countQueryAgent(conf,
        "select count(*) from \"COMPLETED_TXN_COMPONENTS\" where \"CTC_TABLE\"='tab1' and \"CTC_PARTITION\" is not null"));
  }

  /**
   * Concurrent updates with partition pruning predicate and w/o one
   */
  @Test
  public void testWriteSetTracking8() throws Exception {
    dropTable(new String[] {"tab1", "TAB1"});
    driver.run("create table if not exists tab1 (a int, b int) partitioned by (p string) " +
        "clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into tab1 partition(p)(a,b,p) values(1,1,'one'),(2,2,'two')");
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver.compileAndRespond("update tab1 set b = 7 where b=1", true);
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "T2");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr2);
    Assert.assertEquals("Unexpected lock count", 3, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);

    //now start concurrent txn
    swapTxnManager(txnMgr);
    driver.compileAndRespond("update tab1 set b = 7 where p='two'", true);
    ((DbTxnManager)txnMgr).acquireLocks(driver.getPlan(), ctx, "T3", false);
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 5, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);

    //this simulates the completion of txnid:idTxnUpdate1
    long writeId = txnMgr2.getTableWriteId("default", "tab1");
    AddDynamicPartitions adp = new AddDynamicPartitions(txnMgr2.getCurrentTxnId(), writeId, "default", "tab1",
        Collections.singletonList("p=one"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr2.commitTxn(); //txnid:idTxnUpdate1

    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    //completion of txnid:idTxnUpdate2
    writeId = txnMgr.getTableWriteId("default", "tab1");
    adp = new AddDynamicPartitions(txnMgr.getCurrentTxnId(), writeId, "default", "tab1",
        Collections.singletonList("p=two"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr.commitTxn(); //txnid:idTxnUpdate2

    Assert.assertEquals("WRITE_SET mismatch: " +
        TestTxnDbUtil.queryToString(conf, "select * from \"WRITE_SET\""),
        1, TestTxnDbUtil.countQueryAgent(conf,
        "select count(*) from \"WRITE_SET\" where \"WS_PARTITION\"='p=one' and \"WS_OPERATION_TYPE\"='u' and \"WS_TABLE\"='tab1'"));
    Assert.assertEquals("WRITE_SET mismatch: " +
        TestTxnDbUtil.queryToString(conf, "select * from \"WRITE_SET\""),
        1, TestTxnDbUtil.countQueryAgent(conf,
        "select count(*) from \"WRITE_SET\" where \"WS_PARTITION\"='p=two' and \"WS_OPERATION_TYPE\"='u' and \"WS_TABLE\"='tab1'"));
    Assert.assertEquals("COMPLETED_TXN_COMPONENTS mismatch: " +
        TestTxnDbUtil.queryToString(conf, "select * from \"COMPLETED_TXN_COMPONENTS\""),
        4, TestTxnDbUtil.countQueryAgent(conf,
        "select count(*) from \"COMPLETED_TXN_COMPONENTS\" where \"CTC_TABLE\"='tab1' and \"CTC_PARTITION\" is not null"));
  }

  /**
   * Concurrent update/delete of different partitions - should pass
   */
  @Test
  public void testWriteSetTracking9() throws Exception {
    dropTable(new String[] {"TAB1"});
    driver.run("create table if not exists tab1 (a int, b int) partitioned by (p string) " +
        "clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into tab1 partition(p)(a,b,p) values(1,1,'one'),(2,2,'two')");
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver.compileAndRespond("update tab1 set b = 7 where b=1", true);
    long idTxnUpdate1 = txnMgr2.getCurrentTxnId();
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "T2");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr2);
    Assert.assertEquals("Unexpected lock count", 3, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);

    //now start concurrent txn
    swapTxnManager(txnMgr);
    driver.compileAndRespond("delete from tab1 where p='two' and b=2", true);
    long idTxnDelete1 = txnMgr.getCurrentTxnId();
    ((DbTxnManager)txnMgr).acquireLocks(driver.getPlan(), ctx, "T3", false);
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 4, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);

    //this simulates the completion of txnid:idTxnUpdate1
    long writeId = txnMgr2.getTableWriteId("default", "tab1");
    AddDynamicPartitions adp = new AddDynamicPartitions(txnMgr2.getCurrentTxnId(), writeId, "default", "tab1",
        Collections.singletonList("p=one"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr2.commitTxn(); //txnid:idTxnUpdate1

    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    //completion of txnid:idTxnUpdate2
    writeId = txnMgr.getTableWriteId("default", "tab1");
    adp = new AddDynamicPartitions(txnMgr.getCurrentTxnId(), writeId, "default", "tab1",
        Collections.singletonList("p=two"));
    adp.setOperationType(DataOperationType.DELETE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr.commitTxn(); //txnid:idTxnUpdate2

    Assert.assertEquals("COMPLETED_TXN_COMPONENTS mismatch: " +
        TestTxnDbUtil.queryToString(conf, "select * from \"COMPLETED_TXN_COMPONENTS\""),
        2, TestTxnDbUtil.countQueryAgent(conf,
        "select count(*) from \"COMPLETED_TXN_COMPONENTS\" where \"CTC_TXNID\"=" + (idTxnUpdate1 - 1) +
          " and \"CTC_TABLE\"='tab1'"));
    Assert.assertEquals("COMPLETED_TXN_COMPONENTS mismatch: " +
        TestTxnDbUtil.queryToString(conf, "select * from \"COMPLETED_TXN_COMPONENTS\""),
        1, TestTxnDbUtil.countQueryAgent(conf,
        "select count(*) from \"COMPLETED_TXN_COMPONENTS\" where \"CTC_TXNID\"=" + idTxnUpdate1 +
          " and \"CTC_TABLE\"='tab1' and \"CTC_PARTITION\"='p=one'"));
    Assert.assertEquals("WRITE_SET mismatch: " +
        TestTxnDbUtil.queryToString(conf, "select * from \"COMPLETED_TXN_COMPONENTS\""),
        1, TestTxnDbUtil.countQueryAgent(conf,
        "select count(*) from \"COMPLETED_TXN_COMPONENTS\" where \"CTC_TXNID\"=" + idTxnDelete1 +
          " and \"CTC_TABLE\"='tab1' and \"CTC_PARTITION\"='p=two'"));
    Assert.assertEquals("WRITE_SET mismatch: " +
        TestTxnDbUtil.queryToString(conf, "select * from \"WRITE_SET\""),
        1, TestTxnDbUtil.countQueryAgent(conf,
        "select count(*) from \"WRITE_SET\" where \"WS_PARTITION\"='p=one' and \"WS_OPERATION_TYPE\"='u' and \"WS_TABLE\"='tab1'"));
    Assert.assertEquals("WRITE_SET mismatch: " +
        TestTxnDbUtil.queryToString(conf, "select * from \"WRITE_SET\""),
        1, TestTxnDbUtil.countQueryAgent(conf,
        "select count(*) from \"WRITE_SET\" where \"WS_PARTITION\"='p=two' and \"WS_OPERATION_TYPE\"='d' and \"WS_TABLE\"='tab1'"));
    Assert.assertEquals("COMPLETED_TXN_COMPONENTS mismatch: " +
        TestTxnDbUtil.queryToString(conf, "select * from \"COMPLETED_TXN_COMPONENTS\""),
        4, TestTxnDbUtil.countQueryAgent(conf,
        "select count(*) from \"COMPLETED_TXN_COMPONENTS\" where \"CTC_TABLE\"='tab1' and \"CTC_PARTITION\" is not null"));
  }

  /**
   * Concurrent update/delete of same partition - should fail to commit
   */
  @Test
  public void testWriteSetTracking10() throws Exception {
    dropTable(new String[] {"TAB1"});
    driver.run("create table if not exists tab1 (a int, b int) partitioned by (p string) " +
        "clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into tab1 partition(p)(a,b,p) values(1,1,'one'),(2,2,'two')"); //txnid:1
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver.compileAndRespond("update tab1 set b = 7 where b=2", true);
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "T2");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr2);
    Assert.assertEquals("Unexpected lock count", 3, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);

    //now start concurrent txn
    swapTxnManager(txnMgr);
    driver.compileAndRespond("delete from tab1 where p='two' and b=2", true);
    ((DbTxnManager)txnMgr).acquireLocks(driver.getPlan(), ctx, "T3", false);
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 4, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);

    //this simulates the completion of "Update tab2" txn
    long writeId = txnMgr2.getTableWriteId("default", "tab1");
    AddDynamicPartitions adp = new AddDynamicPartitions(txnMgr2.getCurrentTxnId(), writeId, "default", "tab1",
        Collections.singletonList("p=two"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr2.commitTxn(); //"Update tab2"

    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    //completion of "delete from tab1" txn
    writeId = txnMgr.getTableWriteId("default", "tab1");
    adp = new AddDynamicPartitions(txnMgr.getCurrentTxnId(), writeId, "default", "tab1",
        Collections.singletonList("p=two"));
    adp.setOperationType(DataOperationType.DELETE);
    txnHandler.addDynamicPartitions(adp);
    LockException exception = null;
    try {
      txnMgr.commitTxn(); //"delete from tab1"
    }
    catch(LockException e) {
      exception = e;
    }
    Assert.assertNotNull("Expected exception", exception);
    Assert.assertEquals("Exception msg doesn't match",
        "Aborting [txnid:4,4] due to a write conflict on default/tab1/p=two committed by [txnid:3,4] d/u",
        exception.getCause().getMessage());

    Assert.assertEquals("WRITE_SET mismatch: " +
        TestTxnDbUtil.queryToString(conf, "select * from \"WRITE_SET\""),
        1, TestTxnDbUtil.countQueryAgent(conf,
        "select count(*) from \"WRITE_SET\" where \"WS_PARTITION\"='p=two' and \"WS_OPERATION_TYPE\"='u' and \"WS_TABLE\"='tab1'"));
    Assert.assertEquals("COMPLETED_TXN_COMPONENTS mismatch: " +
        TestTxnDbUtil.queryToString(conf, "select * from \"COMPLETED_TXN_COMPONENTS\""),
        3, TestTxnDbUtil.countQueryAgent(conf,
        "select count(*) from \"COMPLETED_TXN_COMPONENTS\" where \"CTC_TABLE\"='tab1' and \"CTC_PARTITION\" is not null"));
  }

  /**
   * Concurrent delete/delete of same partition - should NOT pass
   */
  @Test
  public void testWriteSetTracking11() throws Exception {
    dropTable(new String[] {"TAB1"});
    driver.run("create table if not exists tab1 (a int, b int) partitioned by (p string) " +
        "clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into tab1 partition(p)(a,b,p) values(1,1,'one'),(2,2,'two')");
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver.compileAndRespond("delete from tab1 where b=2", true); //start "delete from tab1" txn
    long txnIdDelete = txnMgr2.getCurrentTxnId();
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "T2");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr2);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);

    //now start concurrent "select * from tab1" txn
    swapTxnManager(txnMgr);
    driver.run("start transaction"); //start explicit txn so that txnMgr knows it
    driver.compileAndRespond("select * from tab1 where b=1 and p='one'", true);
    long txnIdSelect = txnMgr.getCurrentTxnId();
    ((DbTxnManager)txnMgr).acquireLocks(driver.getPlan(), ctx, "T3", false);
    driver.compileAndRespond("delete from tab1 where p='two' and b=2", true);
    ((DbTxnManager)txnMgr).acquireLocks(driver.getPlan(), ctx, "T3", false);
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 4, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);

    //this simulates the completion of "delete from tab1" txn
    long writeId = txnMgr2.getTableWriteId("default", "tab1");
    AddDynamicPartitions adp = new AddDynamicPartitions(txnMgr2.getCurrentTxnId(), writeId, "default", "tab1",
        Collections.singletonList("p=two"));
    adp.setOperationType(DataOperationType.DELETE);
    txnHandler.addDynamicPartitions(adp);
    txnMgr2.commitTxn(); //"delete from tab1" txn

    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "TAB1", "p=one", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.ACQUIRED, "default", "TAB1", "p=two", locks);
    //completion of txnid:txnIdSelect
    writeId = txnMgr.getTableWriteId("default", "tab1");
    adp = new AddDynamicPartitions(txnMgr.getCurrentTxnId(), writeId, "default", "tab1",
        Collections.singletonList("p=two"));
    adp.setOperationType(DataOperationType.DELETE);
    txnHandler.addDynamicPartitions(adp);
    LockException expectedException = null;
    try {
      txnMgr.commitTxn(); //"select * from tab1" txn
    }
    catch(LockException ex) {
      expectedException = ex;
    }
    Assert.assertNotNull("Didn't get expected d/d conflict", expectedException);
    Assert.assertEquals("Transaction manager has aborted the transaction txnid:4.  " +
        "Reason: Aborting [txnid:4,4] due to a write conflict on default/tab1/p=two " +
        "committed by [txnid:3,4] d/d", expectedException.getMessage());
    Assert.assertEquals("WRITE_SET mismatch: " +
        TestTxnDbUtil.queryToString(conf, "select * from \"WRITE_SET\""),
        1, TestTxnDbUtil.countQueryAgent(conf,
            "select count(*) from \"WRITE_SET\" where \"WS_PARTITION\"='p=two' and \"WS_OPERATION_TYPE\"='d' " +
              "and \"WS_TABLE\"='tab1' and \"WS_TXNID\"=" + txnIdDelete));
    Assert.assertEquals("WRITE_SET mismatch: " +
        TestTxnDbUtil.queryToString(conf, "select * from \"WRITE_SET\""),
        0, TestTxnDbUtil.countQueryAgent(conf,
            "select count(*) from \"WRITE_SET\" where \"WS_PARTITION\"='p=two' and \"WS_OPERATION_TYPE\"='d' " +
              "and \"WS_TABLE\"='tab1' and \"WS_TXNID\"=" + txnIdSelect));
    Assert.assertEquals("COMPLETED_TXN_COMPONENTS mismatch: " +
        TestTxnDbUtil.queryToString(conf, "select * from \"COMPLETED_TXN_COMPONENTS\""),
        3, TestTxnDbUtil.countQueryAgent(conf,
        "select count(*) from \"COMPLETED_TXN_COMPONENTS\" where \"CTC_TABLE\"='tab1' and \"CTC_PARTITION\" is not null"));
  }

  @Test
  public void testCompletedTxnComponents() throws Exception {
    dropTable(new String[] {"TAB1", "tab_not_acid2"});
    driver.run("create table if not exists tab1 (a int, b int) partitioned by (p string) " +
        "clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("create table if not exists tab_not_acid2 (a int, b int)");
    driver.run("insert into tab_not_acid2 values(1,1),(2,2)");
    //writing both acid and non-acid resources in the same txn
    driver.run("from tab_not_acid2 insert into tab1 partition(p='two')(a,b) select a,b " +
        "insert into tab_not_acid2(a,b) select a,b "); //txnid:1
    Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from \"COMPLETED_TXN_COMPONENTS\""),
        1, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\""));
    //only expect transactional components to be in COMPLETED_TXN_COMPONENTS
    Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from \"COMPLETED_TXN_COMPONENTS\""),
        1, TestTxnDbUtil.countQueryAgent(conf,
        "select count(*) from \"COMPLETED_TXN_COMPONENTS\" where \"CTC_TXNID\"=2 and \"CTC_TABLE\"='tab1'"));
  }

  // TODO: multi-insert into txn table and non-tx table should be prevented,
  // TODO: concurrent insert/update of same partition - should pass

  @Test
  public void testMultiInsert() throws Exception {
    dropTable(new String[] {"TAB1", "tab1", "tab_not_acid"});
    driver.run("create table if not exists tab1 (a int, b int) partitioned by (p string) " +
        "clustered by (a) into 2  buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("create table if not exists tab_not_acid (a int, b int, p string)");
    driver.run("insert into tab_not_acid values(1,1,'one'),(2,2,'two')");
    driver.run("insert into tab1 partition(p) values(3,3,'one'),(4,4,'two')"); //txinid:2
    //writing both acid and non-acid resources in the same txn
    //tab1 write is a dynamic partition insert
    driver.run("from tab_not_acid insert into tab1 partition(p)(a,b,p) select a,b,p " +
        "insert into tab_not_acid(a,b) select a,b where p='two'"); //txnid:3
    Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from \"COMPLETED_TXN_COMPONENTS\""),
        4, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\""));
    //only expect transactional components to be in COMPLETED_TXN_COMPONENTS
    Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from \"COMPLETED_TXN_COMPONENTS\""),
        2, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\" where \"CTC_TXNID\"=3"));
    Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from \"COMPLETED_TXN_COMPONENTS\""),
        2, TestTxnDbUtil.countQueryAgent(conf,
        "select count(*) from \"COMPLETED_TXN_COMPONENTS\" where \"CTC_TXNID\"=3 and \"CTC_TABLE\"='tab1'"));
  }

  @Test
  public void testMultiInsertOnDynamicallyPartitionedMmTable() throws Exception {
    dropTable(new String[] {"tabMmDp", "tab_not_acid"});
    driver.run("create table if not exists tabMmDp (a int, b int) partitioned by (p string) "
        + "stored as orc "
        + "TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')");
    driver.run("create table if not exists tab_not_acid (a int, b int, p string)");
    driver.run("insert into tab_not_acid values (1 ,1, 'one'), (2, 2, 'two')");
    // insert 2 rows twice into the MM table
    driver.run("from tab_not_acid "
        + "insert into tabMmDp select a,b,p "
        + "insert into tabMmDp select a,b,p"); //txnid: 2 (2 drops, 2 creates, 2 inserts)

    final String completedTxnComponentsContents =
        TestTxnDbUtil.queryToString(conf, "select * from \"COMPLETED_TXN_COMPONENTS\"");
    Assert.assertEquals(completedTxnComponentsContents,
        4, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\""));
    Assert.assertEquals(completedTxnComponentsContents,
        4, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\" where \"CTC_TXNID\"=2"));
    Assert.assertEquals(completedTxnComponentsContents,
        4, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\" where \"CTC_TXNID\"=2 "
            + "and \"CTC_TABLE\"='tabmmdp'"));
    // ctc_update_delete value should be "N" for both partitions since these are inserts
    Assert.assertEquals(completedTxnComponentsContents,
        4, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\" where \"CTC_TXNID\"=2 "
            + "and \"CTC_TABLE\"='tabmmdp' and \"CTC_UPDATE_DELETE\"='N'"));
    dropTable(new String[] {"tabMmDp", "tab_not_acid"});
  }

  private List<ShowLocksResponseElement> getLocksWithFilterOptions(HiveTxnManager txnMgr,
      String dbName, String tblName, Map<String, String> partSpec) throws Exception {
    if (dbName == null && tblName != null) {
      dbName = SessionState.get().getCurrentDatabase();
    }
    ShowLocksRequest rqst = new ShowLocksRequest();
    rqst.setDbname(dbName);
    rqst.setTablename(tblName);
    if (partSpec != null) {
      String partName = FileUtils.makePartName(
          new ArrayList<>(partSpec.keySet()), new ArrayList<>(partSpec.values()));
      rqst.setPartname(partName);
    }
    ShowLocksResponse rsp = ((DbLockManager)txnMgr.getLockManager()).getLocks(rqst);
    return rsp.getLocks();
  }

  @Test
  public void testShowLocksAgentInfo() throws Exception {
    driver.run("create table if not exists XYZ (a int, b int)");
    driver.compileAndRespond("select a from XYZ where b = 8", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "XYZ");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "XYZ", null, locks);
    Assert.assertEquals("Wrong AgentInfo", driver.getPlan().getQueryId(), locks.get(0).getAgentInfo());
  }

  @Test
  public void testMerge3Way() throws Exception {
    testMerge3Way(false, false);
  }
  @Test
  public void testMerge3WayConflict() throws Exception {
    testMerge3Way(true, false);
  }
  @Test
  public void testMerge3WayConflictSharedWrite() throws Exception {
    testMerge3Way(true, true);
  }

  /**
   * @param causeConflict whether to cause a WW conflict or not
   */
  private void testMerge3Way(boolean causeConflict, boolean sharedWrite) throws Exception {
    dropTable(new String[]{"target", "source", "source2"});
    conf.setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, !sharedWrite);

    driver.run("create table target (a int, b int) " +
        "partitioned by (p int, q int) clustered by (a) into 2  buckets " +
        "stored as orc TBLPROPERTIES ('transactional'='true')");
    //in practice we don't really care about the data in any of these tables (except as far as
    //it creates partitions, the SQL being test is not actually executed and results of the
    //wrt ACID metadata is supplied manually via addDynamicPartitions().  But having data makes
    //it easier to follow the intent
    driver.run("insert into target partition(p,q) values (1,2,1,2), (3,4,1,2), (5,6,1,3), (7,8,2,2)");
    driver.run("create table source (a int, b int, p int, q int)");
    driver.run("insert into source values " +
        // I-(1/2)            D-(1/2)    I-(1/3)     U-(1/3)     D-(2/2)     I-(1/1) - new part
        "(9,10,1,2),        (3,4,1,2), (11,12,1,3), (5,13,1,3), (7,8,2,2), (14,15,1,1)");
    driver.run("create table source2 (a int, b int, p int, q int)");
    driver.run("insert into source2 values " +
        //cc ? -:U-(1/2)     D-(1/2)         cc ? U-(1/3):-       D-(2/2)       I-(1/1) - new part 2
        "(9,100,1,2),      (3,4,1,2),         (5,13,1,3),       (7,8,2,2), (14,15,2,1)");

    driver.compileAndRespond("merge into target t using source s on t.a=s.b " +
        "when matched and t.a=5 then update set b=s.b " + //updates p=1/q=3
        "when matched and t.a in (3,7) then delete " + //deletes from p=1/q=2, p=2/q=2
        "when not matched and t.a >= 8 then insert values(s.a, s.b, s.p, s.q)", true); //insert p=1/q=2, p=1/q=3 and new part 1/1
    long txnId1 = txnMgr.getCurrentTxnId();
    txnMgr.acquireLocks(driver.getPlan(), ctx, "T1");
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 5, locks.size());

    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.SHARED_READ),
        LockState.ACQUIRED, "default", "target", null, locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "source", null, locks);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        LockState.ACQUIRED, "default", "target", "p=1/q=2", locks);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        LockState.ACQUIRED, "default", "target", "p=1/q=3", locks);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        LockState.ACQUIRED, "default", "target", "p=2/q=2", locks);

    //start concurrent txn
    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver.compileAndRespond("merge into target t using source2 s on t.a=s.b " +
        //if conflict updates p=1/q=3 else update p=1/q=2
        "when matched and t.a=" + (causeConflict ? 5 : 9) + " then update set b=s.b " +
        //if cc deletes from p=1/q=2, p=2/q=2, else delete nothing
        "when matched and t.a in (" + (causeConflict ? "3,7" : "11, 13")  + ") then delete " +
        //insert p=1/q=2, p=1/q=3 and new part 1/1
        "when not matched and t.a >= 8 then insert values(s.a, s.b, s.p, s.q)", true);
    long txnId2 = txnMgr2.getCurrentTxnId();
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "T1", false);
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 10, locks.size());

    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.SHARED_READ),
        LockState.ACQUIRED, "default", "target", null, locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "source", null, locks);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        LockState.ACQUIRED, "default", "target", "p=1/q=2", locks);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        LockState.ACQUIRED, "default", "target", "p=1/q=3", locks);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        LockState.ACQUIRED, "default", "target", "p=2/q=2", locks);

    long extLockId = TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.SHARED_READ),
        (sharedWrite ? LockState.ACQUIRED : LockState.WAITING),
        "default", "target", null, locks, sharedWrite).getLockid();
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, (sharedWrite ? LockState.ACQUIRED : LockState.WAITING),
        "default", "source2", null, locks);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        (sharedWrite ? LockState.ACQUIRED : LockState.WAITING),
        "default", "target", "p=1/q=2", locks, sharedWrite);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        (sharedWrite ? LockState.ACQUIRED : LockState.WAITING),
        "default", "target", "p=1/q=3", locks, sharedWrite);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        (sharedWrite ? LockState.ACQUIRED : LockState.WAITING),
        "default", "target", "p=2/q=2", locks, sharedWrite);

    Assert.assertEquals(
        "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId1) + "): " +
        TestTxnDbUtil.queryToString(conf, "select * from \"TXN_COMPONENTS\""),
        1,
        TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"TXN_COMPONENTS\" where \"TC_TXNID\"=" + txnId1));

    //complete 1st txn
    long writeId = txnMgr.getTableWriteId("default", "target");
    AddDynamicPartitions adp = new AddDynamicPartitions(txnId1, writeId, "default", "target",
        Collections.singletonList("p=1/q=3")); //update clause
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    adp = new AddDynamicPartitions(txnId1, writeId, "default", "target",
        Arrays.asList("p=1/q=2", "p=2/q=2")); //delete clause
    adp.setOperationType(DataOperationType.DELETE);
    txnHandler.addDynamicPartitions(adp);
    adp = new AddDynamicPartitions(txnId1, writeId, "default", "target",
        Arrays.asList("p=1/q=2", "p=1/q=3", "p=1/q=1")); //insert clause
    adp.setOperationType(DataOperationType.INSERT);
    txnHandler.addDynamicPartitions(adp);
    Assert.assertEquals(
        "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId1) + "): " +
        TestTxnDbUtil.queryToString(conf, "select * from \"TXN_COMPONENTS\""),
        1,
        TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"TXN_COMPONENTS\" where \"TC_TXNID\"=" + txnId1 +
            " and \"TC_OPERATION_TYPE\"='u'"));
    Assert.assertEquals(
        "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId1) + "): " +
        TestTxnDbUtil.queryToString(conf, "select * from \"TXN_COMPONENTS\""),
        2,
        TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"TXN_COMPONENTS\" where \"TC_TXNID\"=" + txnId1 +
                " and \"TC_OPERATION_TYPE\"='d'"));
    Assert.assertEquals(
        "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId1) + "): " +
        TestTxnDbUtil.queryToString(conf, "select * from \"TXN_COMPONENTS\""),
        3,
        TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"TXN_COMPONENTS\" where \"TC_TXNID\"=" + txnId1 +
            " and \"TC_OPERATION_TYPE\"='i'"));

    txnMgr.commitTxn(); //commit T1
    Assert.assertEquals(
        "COMPLETED_TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId1) + "): " +
        TestTxnDbUtil.queryToString(conf, "select * from \"COMPLETED_TXN_COMPONENTS\""),
        6,
        TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\" where \"CTC_TXNID\"=" + txnId1));
    Assert.assertEquals(
        "WRITE_SET mismatch(" + JavaUtils.txnIdToString(txnId1) + "): " +
        TestTxnDbUtil.queryToString(conf, "select * from \"WRITE_SET\""),
        1,
        TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"WRITE_SET\" where \"WS_TXNID\"=" + txnId1 +
            " and \"WS_OPERATION_TYPE\"='u'"));
    Assert.assertEquals(
        "WRITE_SET mismatch(" + JavaUtils.txnIdToString(txnId1) + "): " +
        TestTxnDbUtil.queryToString(conf, "select * from \"WRITE_SET\""),
        2,
        TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"WRITE_SET\" where \"WS_TXNID\"=" + txnId1 +
            " and \"WS_OPERATION_TYPE\"='d'"));

    //re-check locks which were in Waiting state - should now be Acquired
    ((DbLockManager)txnMgr2.getLockManager()).checkLock(extLockId);
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 5, locks.size());

    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.SHARED_READ),
        LockState.ACQUIRED, "default", "target", null, locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "source2", null, locks);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        LockState.ACQUIRED, "default", "target", "p=1/q=2", locks);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        LockState.ACQUIRED, "default", "target", "p=1/q=3", locks);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        LockState.ACQUIRED, "default", "target", "p=2/q=2", locks);

    Assert.assertEquals(
        "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId2) + "): " +
        TestTxnDbUtil.queryToString(conf, "select * from \"TXN_COMPONENTS\""),
        1,
        TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"TXN_COMPONENTS\" where \"TC_TXNID\"=" + txnId2));

    //complete 2nd txn
    writeId = txnMgr2.getTableWriteId("default", "target");
    adp = new AddDynamicPartitions(txnId2, writeId, "default", "target",
        Collections.singletonList(causeConflict ? "p=1/q=3" : "p=1/p=2")); //update clause
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);

    if (causeConflict) {
      adp = new AddDynamicPartitions(txnId2, writeId, "default", "target",
          Arrays.asList("p=1/q=2", "p=2/q=2")); //delete clause
      adp.setOperationType(DataOperationType.DELETE);
      txnHandler.addDynamicPartitions(adp);
    }
    adp = new AddDynamicPartitions(txnId2, writeId, "default", "target",
        Arrays.asList("p=1/q=2", "p=1/q=3", "p=1/q=1")); //insert clause
    adp.setOperationType(DataOperationType.INSERT);
    txnHandler.addDynamicPartitions(adp);
    Assert.assertEquals(
        "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId2) + "): " +
        TestTxnDbUtil.queryToString(conf, "select * from \"TXN_COMPONENTS\""),
        1,
        TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"TXN_COMPONENTS\" where \"TC_TXNID\"=" + txnId2 +
            " and \"TC_OPERATION_TYPE\"='u'"));
    Assert.assertEquals(
        "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId2) + "): " +
        TestTxnDbUtil.queryToString(conf, "select * from \"TXN_COMPONENTS\""),
        (causeConflict ? 2 : 0),
        TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"TXN_COMPONENTS\" where \"TC_TXNID\"=" + txnId2 +
            " and \"TC_OPERATION_TYPE\"='d'"));
    Assert.assertEquals(
        "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId2) + "): " +
        TestTxnDbUtil.queryToString(conf, "select * from \"TXN_COMPONENTS\""),
        3,
        TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"TXN_COMPONENTS\" where \"TC_TXNID\"=" + txnId2 +
            " and \"TC_OPERATION_TYPE\"='i'"));

    LockException expectedException = null;
    try {
      txnMgr2.commitTxn(); //commit T2
    }
    catch (LockException e) {
      expectedException = e;
    }
    if (causeConflict && sharedWrite) {
      Assert.assertNotNull("Didn't get exception", expectedException);
      try {
        Assert.assertEquals("Transaction manager has aborted the transaction txnid:4.  Reason: " +
            "Aborting [txnid:4,4] due to a write conflict on default/target/p=1/q=2 " +
            "committed by [txnid:3,4] d/d", expectedException.getMessage());
      } catch (ComparisonFailure ex) {
        //the 2 txns have 3 conflicts between them so check for either failure since which one is
        //reported (among the 3) is not deterministic
        try {
          Assert.assertEquals("Transaction manager has aborted the transaction txnid:4.  Reason: "
              + "Aborting [txnid:4,4] due to a write conflict on default/target/p=2/q=2 "
              + "committed by [txnid:3,4] d/d", expectedException.getMessage());
        } catch (ComparisonFailure ex2) {
          Assert.assertEquals("Transaction manager has aborted the transaction txnid:4.  Reason: " +
              "Aborting [txnid:4,4] due to a write conflict on default/target/p=1/q=3 " +
              "committed by [txnid:3,4] u/u", expectedException.getMessage());
        }
      }
      Assert.assertEquals(
          "COMPLETED_TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId2) + "): " +
          TestTxnDbUtil.queryToString(conf, "select * from \"COMPLETED_TXN_COMPONENTS\""),
          0,
          TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\" where \"CTC_TXNID\"=" + txnId2));
      Assert.assertEquals(
          "WRITE_SET mismatch(" + JavaUtils.txnIdToString(txnId2) + "): " +
          TestTxnDbUtil.queryToString(conf, "select * from \"WRITE_SET\""),
          0,
          TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"WRITE_SET\" where \"WS_TXNID\"=" + txnId2));
    } else {
      Assert.assertNull("Unexpected exception " + expectedException, expectedException);
      Assert.assertEquals(
          "COMPLETED_TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId2) + "): " +
          TestTxnDbUtil.queryToString(conf, "select * from \"COMPLETED_TXN_COMPONENTS\""),
        causeConflict ? 6 : 4,
          TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\" where \"CTC_TXNID\"=" + txnId2));
      Assert.assertEquals(
          "WRITE_SET mismatch(" + JavaUtils.txnIdToString(txnId2) + "): " +
          TestTxnDbUtil.queryToString(conf, "select * from \"WRITE_SET\""),
          1,
          TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"WRITE_SET\" where \"WS_TXNID\"=" + txnId2 +
              " and \"WS_OPERATION_TYPE\"='u'"));
      Assert.assertEquals(
          "WRITE_SET mismatch(" + JavaUtils.txnIdToString(txnId2) + "): " +
          TestTxnDbUtil.queryToString(conf, "select * from \"WRITE_SET\""),
        causeConflict ? 2 : 0,
          TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"WRITE_SET\" where \"WS_TXNID\"=" + txnId2 +
              " and \"WS_OPERATION_TYPE\"='d'"));
    }
    dropTable(new String[]{"target", "source", "source2"});
  }

  @Test
  public void testMergeUnpartitioned() throws Exception {
    testMergeUnpartitioned(false, false);
  }
  @Test
  public void testMergeUnpartitionedConflict() throws Exception {
    testMergeUnpartitioned(true, false);
  }
  @Test
  public void testMergeUnpartitionedConflictSharedWrite() throws Exception {
    testMergeUnpartitioned(true, true);
  }

  /**
   * run a merge statement using un-partitioned target table and a concurrent op on the target
   * Check that proper locks are acquired and Write conflict detection works and the state
   * of internal table.
   * @param causeConflict true to make 2 operations such that they update the same entity
   */
  private void testMergeUnpartitioned(boolean causeConflict, boolean sharedWrite) throws Exception {
    dropTable(new String[] {"target","source"});
    conf.setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, !sharedWrite);

    driver.run("create table target (a int, b int) " +
        "clustered by (a) into 2  buckets " +
        "stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into target values (1,2), (3,4), (5,6), (7,8)");
    driver.run("create table source (a int, b int)");

    if (causeConflict) {
      driver.compileAndRespond("update target set b = 2 where a=1", true);
    } else {
      driver.compileAndRespond("insert into target values(9,10),(11,12)", true);
    }
    long txnid1 = txnMgr.getCurrentTxnId();
    txnMgr.acquireLocks(driver.getPlan(), ctx, "T1");
    Assert.assertEquals(
        "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnid1) + "): " +
        TestTxnDbUtil.queryToString(conf, "select * from \"TXN_COMPONENTS\""),
        1, //no DP, so it's populated from lock info
        TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"TXN_COMPONENTS\" where \"TC_TXNID\"=" + txnid1));

    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    if (causeConflict) {
      Assert.assertEquals("Unexpected lock count", 1, locks.size());
      TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
          LockState.ACQUIRED, "default", "target", null, locks);
    } else {
      Assert.assertEquals("Unexpected lock count", 1, locks.size());
      TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.SHARED_READ),
          LockState.ACQUIRED, "default", "target", null, locks);
    }

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    //start a 2nd (overlapping) txn
    driver.compileAndRespond("merge into target t using source s " +
        "on t.a=s.a " +
        "when matched then delete " +
        "when not matched then insert values(s.a,s.b)", true);
    long txnid2 = txnMgr2.getCurrentTxnId();
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "T2", false);
    locks = getLocks();

    Assert.assertEquals("Unexpected lock count", 3, locks.size());
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
          LockState.ACQUIRED, "default", "target", null, locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, (causeConflict && !sharedWrite) ? LockState.WAITING : LockState.ACQUIRED,
        "default", "source", null, locks);
    long extLockId = TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
          (causeConflict && !sharedWrite) ? LockState.WAITING : LockState.ACQUIRED,
        "default", "target", null, locks, sharedWrite).getLockid();
    txnMgr.commitTxn(); //commit T1

    Assert.assertEquals("WRITE_SET mismatch(" + JavaUtils.txnIdToString(txnid1) + "): " +
        TestTxnDbUtil.queryToString(conf, "select * from \"WRITE_SET\""),
        causeConflict ? 1 : 0, //Inserts are not tracked by WRITE_SET
        TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"WRITE_SET\" where \"WS_TXNID\"=" + txnid1 +
            " and \"WS_OPERATION_TYPE\"=" + (causeConflict ? "'d'" : "'i'")));

    //re-check locks which were in Waiting state - should now be Acquired
    ((DbLockManager)txnMgr2.getLockManager()).checkLock(extLockId);
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "source", null, locks);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        LockState.ACQUIRED, "default", "target", null, locks);

    Assert.assertEquals(
        "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnid2) + "): " +
        TestTxnDbUtil.queryToString(conf, "select * from \"TXN_COMPONENTS\""),
        1, //
        TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"TXN_COMPONENTS\" where \"TC_TXNID\"=" + txnid2));
    Assert.assertEquals(
        "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnid2) + "): " +
        TestTxnDbUtil.queryToString(conf, "select * from \"TXN_COMPONENTS\""),
        1, //
        TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"TXN_COMPONENTS\" where \"TC_TXNID\"=" + txnid2 +
            " and \"TC_OPERATION_TYPE\"='d'"));

    //complete T2 txn
    LockException expectedException = null;
    try {
      txnMgr2.commitTxn();
    }
    catch (LockException e) {
      expectedException = e;
    }
    if (causeConflict && sharedWrite) {
      Assert.assertNotNull("Didn't get exception", expectedException);
      Assert.assertEquals("Got wrong message code", ErrorMsg.TXN_ABORTED, expectedException.getCanonicalErrorMsg());
      Assert.assertEquals("Exception msg didn't match",
          "Aborting [txnid:4,4] due to a write conflict on default/target committed by [txnid:3,4] d/d",
          expectedException.getCause().getMessage());
    } else {
      Assert.assertEquals("WRITE_SET mismatch(" + JavaUtils.txnIdToString(txnid1) + "): " +
          TestTxnDbUtil.queryToString(conf, "select * from \"WRITE_SET\""),
          1, //Unpartitioned table: 1 row for Delete; Inserts are not tracked in WRITE_SET
          TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"WRITE_SET\" where \"WS_TXNID\"=" + txnid2 +
              " and \"WS_OPERATION_TYPE\"='d'"));
    }
  }

  @Test
  public void testInsertMergeInsertLocking() throws Exception {
    testMergeInsertLocking(false);
  }
  @Test
  public void testInsertMergeInsertLockingSharedWrite() throws Exception {
    testMergeInsertLocking(true);
  }

  private void testMergeInsertLocking(boolean sharedWrite) throws Exception {
    dropTable(new String[]{"target", "source"});
    conf.setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, !sharedWrite);

    driver.run("create table target (a int, b int) stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into target values (1,2), (3,4)");
    driver.run("create table source (a int, b int)");
    driver.run("insert into source values (5,6), (7,8)");

    driver.compileAndRespond("insert into target values (5, 6)");
    txnMgr.acquireLocks(driver.getPlan(), ctx, "T1");

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);

    driver.compileAndRespond("merge into target t using source s on t.a = s.a " +
        "when not matched then insert values (s.a, s.b)");
    txnMgr2.acquireLocks(driver.getPlan(), driver.getContext(), "T2", false);
    List<ShowLocksResponseElement> locks = getLocks();

    Assert.assertEquals("Unexpected lock count", 3, locks.size());
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.SHARED_READ),
        LockState.ACQUIRED, "default", "target", null, locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "source", null, locks);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        LockState.ACQUIRED, "default", "target", null, locks);
  }

  @Test
  public void testConcurrentInsertMergeInsertGenerateDuplicates() throws Exception {
    testConcurrentMergeInsert("insert into target values (5, 6)", false, false, true);
  }
  @Test
  public void testConcurrentInsertMergeInsertSharedWriteGenerateDuplicates() throws Exception {
    testConcurrentMergeInsert("insert into target values (5, 6)", true,false, true);
  }
  @Test
  public void testConcurrent2MergeInsertsNoDuplicates() throws Exception {
    testConcurrentMergeInsert("merge into target t using source s on t.a = s.a " +
        "when not matched then insert values (s.a, s.b)", false, false,false);
  }
  @Test
  public void testConcurrent2MergeInsertsSharedWriteNoDuplicates() throws Exception {
    testConcurrentMergeInsert("merge into target t using source s on t.a = s.a " +
        "when not matched then insert values (s.a, s.b)", true, false,false);
  }
  @Test
  public void testConcurrent2MergeInsertsNoDuplicatesSlowCompile() throws Exception {
    testConcurrentMergeInsert("merge into target t using source s on t.a = s.a " +
        "when not matched then insert values (s.a, s.b)", false, true,false);
  }
  @Test
  public void testConcurrent2MergeInsertsSharedWriteNoDuplicatesSlowCompile() throws Exception {
    testConcurrentMergeInsert("merge into target t using source s on t.a = s.a " +
        "when not matched then insert values (s.a, s.b)", true, true,false);
  }
  @Test
  public void testConcurrentInsertMergeInsertNoDuplicates() throws Exception {
    testConcurrentMergeInsert("insert into source values (3, 4)", false, false,false);
  }

  private void testConcurrentMergeInsert(String query, boolean sharedWrite, boolean slowCompile, boolean extectedDuplicates) throws Exception {
    dropTable(new String[]{"target", "source"});
    conf.setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, !sharedWrite);
    driver2.getConf().setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, !sharedWrite);

    driver.run("create table target (a int, b int) stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into target values (1,2), (3,4)");
    driver.run("create table source (a int, b int)");
    driver.run("insert into source values (5,6), (7,8)");

    if (!slowCompile) {
      driver.compileAndRespond(query);
    }

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver2.compileAndRespond("merge into target t using source s on t.a = s.a " +
      "when not matched then insert values (s.a, s.b)");

    swapTxnManager(txnMgr);
    if (!slowCompile) {
      driver.run();
    } else {
      driver.run(query);
    }

    swapTxnManager(txnMgr2);
    try {
      driver2.run();
    } catch (CommandProcessorException ex ){
      Assert.assertTrue(ex.getMessage().contains("write conflict on default/target"));
    }

    swapTxnManager(txnMgr);
    driver.run("select * from target");
    List<String> res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals("Duplicate records " + (extectedDuplicates ? "" : "not") + "found",
      extectedDuplicates ? 5 : 4, res.size());
    dropTable(new String[]{"target", "source"});
  }

  @Test
  public void testConcurrentUpdateMergeUpdateConflict() throws Exception {
    testConcurrentUpdateMergeUpdateConflict(false);
  }
  @Test
  public void testConcurrentUpdateMergeUpdateConflictSlowCompile() throws Exception {
    testConcurrentUpdateMergeUpdateConflict(true);
  }

  private void testConcurrentUpdateMergeUpdateConflict(boolean slowCompile) throws Exception {
    dropTable(new String[]{"target", "source"});
    driver2.getConf().setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, true);

    driver.run("create table target (a int, b int) stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into target values (1,2), (3,4)");
    driver.run("create table source (a int, b int)");
    driver.run("insert into source values (5,6), (7,8)");

    if (!slowCompile) {
      driver.compileAndRespond("update target set a=5 where a=1");
    }

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver2.compileAndRespond("merge into target t using source s on t.a = s.a " +
      "when matched then update set b=8");

    swapTxnManager(txnMgr);
    if (!slowCompile) {
      driver.run();
    } else {
      driver.run("update target set a=5 where a=1");
    }

    swapTxnManager(txnMgr2);
    driver2.run();

    swapTxnManager(txnMgr);
    driver.run("select * from target");
    List<String> res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals(2, res.size());
    Assert.assertEquals("Lost Update", "5\t8", res.get(1));
  }

  @Test
  public void testCtasLockingExclWrite() throws Exception {
    dropTable(new String[]{"target", "source"});
    conf.setBoolVar(HiveConf.ConfVars.TXN_CTAS_X_LOCK, true);
    
    driver.run("create table source (a int, b int) stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into source values (1,2), (3,4)");

    driver.compileAndRespond("create table target stored as orc TBLPROPERTIES ('transactional'='true') as select * from source");
    txnMgr.acquireLocks(driver.getPlan(), ctx, "T1");

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);

    driver.compileAndRespond("create table target stored as orc TBLPROPERTIES ('transactional'='true') as select * from source");
    try {
      //Query should fail with Table already exists exception
      txnMgr2.acquireLocks(driver.getPlan(), driver.getContext(), "T2", false);
    } catch (LockException e) {
      Assert.assertTrue(e.getMessage().contains("Failed to initiate a concurrent CTAS operation"));
    }
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 3, locks.size());

    TestTxnDbUtil.checkLock(LockType.EXCL_WRITE, LockState.ACQUIRED, "default", "target", null, locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "source", null, locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", null, null, locks);
  }

  @Test
  public void testConcurrent2MergeUpdatesConflict() throws Exception {
    testConcurrent2MergeUpdatesConflict(false);
  }
  @Test
  public void testConcurrent2MergeUpdatesConflictSlowCompile() throws Exception {
    testConcurrent2MergeUpdatesConflict(true);
  }

  private void testConcurrent2MergeUpdatesConflict(boolean slowCompile) throws Exception {
    dropTable(new String[]{"target", "source"});
    driver2.getConf().setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, true);

    driver.run("create table target (name string, age int) stored as orc TBLPROPERTIES('transactional'='true')");
    driver.run("insert into target values ('amy', 88), ('drake', 44), ('earl', 21)");
    driver.run("create table source (name string, age int) stored as orc TBLPROPERTIES('transactional'='true')");
    driver.run("insert into source values ('amy', 35), ('bob', 66), ('cal', 21)");

    if (!slowCompile) {
      driver.compileAndRespond("merge into target t using source s on t.name = s.name " +
        "when matched then update set age=10");
    }

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver2.compileAndRespond("merge into target t using source s on t.age = s.age " +
      "when matched then update set age=10");

    swapTxnManager(txnMgr);
    if (!slowCompile) {
      driver.run();
    } else {
      driver.run("merge into target t using source s on t.name = s.name " +
        "when matched then update set age=10");
    }

    swapTxnManager(txnMgr2);
    driver2.run();

    swapTxnManager(txnMgr);
    driver.run("select * from target where age=10");
    List<String> res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals(2, res.size());
    Assert.assertTrue("Lost Update", isEqualCollection(res, asList("earl\t10", "amy\t10")));
  }

  // The intent of this test is to cause multiple conflicts to the same query to test the conflict retry functionality.
  @Test
  public void testConcurrentConflictRetry() throws Exception {
    dropTable(new String[]{"target"});

    driver2 = Mockito.spy(driver2);
    driver2.getConf().setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, true);
    driver.run("create table target(i int) stored as orc tblproperties ('transactional'='true')");
    driver.run("insert into target values (1),(1)");

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);

    // This partial mock allows us to execute a transaction that conflicts with the driver2 query in a controlled
    // manner.
    AtomicInteger lockAndRespondCount = new AtomicInteger();
    Mockito.doAnswer((invocation) -> {
      lockAndRespondCount.getAndIncrement();
      // we want to make sure this transaction gets conflicted at least twice, to exercise the conflict retry loop
      if (lockAndRespondCount.get() <= 2) {
        swapTxnManager(txnMgr);
        try {
          // this should call a conflict with the current query being ran by driver2
          driver.run("update target set i = 1 where i = 1");
        } catch (Exception e) {
          // do nothing
        }
        swapTxnManager(txnMgr2);
      }
      invocation.callRealMethod();
      return null;
    }).when(driver2).lockAndRespond();

    driver2.run("update target set i = 1 where i = 1");

    // we expected lockAndRespond to be called 3 times.
    // 1 time after compilation, 2 more times due to the 2 conflicts
    Assert.assertEquals(3, lockAndRespondCount.get());
    swapTxnManager(txnMgr);
    // we expect two rows
    driver.run("select * from target");
    List<String> res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals(2, res.size());
  }

  @Test
  public void testConcurrentConflictMaxRetryCount() throws Exception {
    dropTable(new String[]{"target"});
    driver2 = Mockito.spy(driver2);
    driver2.getConf().setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, true);

    final int maxRetries = 4;
    driver2.getConf().setIntVar(HiveConf.ConfVars.HIVE_TXN_MAX_RETRYSNAPSHOT_COUNT, maxRetries);

    driver.run("create table target(i int) stored as orc tblproperties ('transactional'='true')");
    driver.run("insert into target values (1),(1)");

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);

    // This run should conflict with the above query and cause the "conflict lambda" to be execute,
    // which will then also conflict with the driver2 query and cause it to retry. The intent here is
    // to cause driver2's query to exceed HIVE_TXN_MAX_RETRYSNAPSHOT_COUNT and throw exception.
    AtomicInteger lockAndRespondCount = new AtomicInteger();
    Mockito.doAnswer((invocation) -> {
      lockAndRespondCount.getAndIncrement();
      // we want to make sure the transaction gets conflicted until it fails.
      // +1 is for the initial lockAndRespond after compilation
      if (lockAndRespondCount.get() <= 1 + maxRetries) {
        swapTxnManager(txnMgr);
        try {
          // this should call a conflict with the current query being ran by driver2
          driver.run("update target set i = 1 where i = 1");
        } catch (Exception e) {
          // do nothing
        }
        swapTxnManager(txnMgr2);
      }
      invocation.callRealMethod();
      return null;
    }).when(driver2).lockAndRespond();

    boolean exceptionThrown = false;
    // Start a query on driver2, we expect this query to never execute because the nature of the test it to conflict
    // until HIVE_TXN_MAX_RETRYSNAPSHOT_COUNT is exceeded.
    // We verify that it is never executed by counting the number of rows returned that have i = 1.
    try {
      driver2.run("update target set i = 2 where i = 1");
    } catch (CommandProcessorException cpe) {
      exceptionThrown = true;
      Assert.assertTrue(
          cpe.getMessage().contains("Operation could not be executed, snapshot was outdated when locks were acquired.")
      );
    }
    Assert.assertTrue(exceptionThrown);
    // +1 for the inital lockAndRespond after compilation, another +1 for the lockAndRespond that caused us
    // to exceed max retries.
    Assert.assertEquals(maxRetries+2, lockAndRespondCount.get());
    swapTxnManager(txnMgr);

    // we expect two rows
    driver.run("select * from target where i = 1");
    List<String> res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals(2, res.size());
  }

  @Test
  public void testMergeMultipleBranchesOptimistic() throws Exception {
    dropTable(new String[]{"target", "src1", "src2"});
    conf.setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, false);
    conf.setBoolVar(HiveConf.ConfVars.TXN_MERGE_INSERT_X_LOCK, false);
    
    driver2.getConf().setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, false);
    driver2.getConf().setBoolVar(HiveConf.ConfVars.TXN_MERGE_INSERT_X_LOCK, false);

    driver.run("create table target (id int, txt string) stored as orc TBLPROPERTIES('transactional'='true')");
    driver.run("insert into target values " +
      "('0', 'orig_FyZl'), " +
      "(5, 'orig_VsbLsaG'), " +
      "(10, 'orig_dhhCassOoV')");

    driver.run("create table src1 (id int, txt string) stored as orc TBLPROPERTIES('transactional'='true')");
    driver.run("insert into src1 values " +
      "(0, 'new1_tnlGat'), " +
      "(1, 'new1_KulBf'), " +
      "(2, 'new1_zkLGuU'), " +
      "(3, 'new1_jznZVac')," +
      "(4, 'new1_hdyazJXL')," +
      "(5, 'new1_gxclXFtP')," +
      "(6, 'new1_CNZr')," +
      "(7, 'new1_GoBjjuow')," +
      "(8, 'new1_vRfY')," +
      "(9, 'new1_bdnQA')," +
      "(10, 'new1_FNboL')");

    driver.run("create table src2 (id int, txt string) stored as orc TBLPROPERTIES('transactional'='true')");
    driver.run("insert into src2 values " +
      "(0, 'new2_Cjdj'), " +
      "(1, 'new2_GysxGF'), " +
      "(2, 'new2_ToHyf'), " +
      "(3, 'new2_HZjkahVJ')," +
      "(4, 'new2_qcySYYUul')," +
      "(5, 'new2_FupKyDcVcJ')," +
      "(6, 'new2_DAcCwakVr')," +
      "(7, 'new2_nZozPAZKI')," +
      "(8, 'new2_bjdEmdRp')," +
      "(9, 'new2_PkRAwdJeLX')," +
      "(10, 'new2_aGSuZHx')");

    driver.compileAndRespond("MERGE INTO target t USING src1 s ON t.id = s.id " +
      "WHEN MATCHED THEN UPDATE SET txt = CONCAT_WS(' ',t.txt,s.txt) " +
      "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.txt)");
    
    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver2.compileAndRespond("MERGE INTO target t USING src2 s ON t.id = s.id " +
      "WHEN MATCHED THEN UPDATE SET txt = CONCAT_WS(' ',t.txt,s.txt) " +
      "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.txt)");

    swapTxnManager(txnMgr);
    driver.run();

    swapTxnManager(txnMgr2);
    try {
      driver2.run();
    } catch (Exception ex) {
      Assert.assertTrue(ex.getCause() instanceof LockException);
      Assert.assertTrue(ex.getMessage().matches(".*Aborting .* due to a write conflict on default/target.*"));
    }
    swapTxnManager(txnMgr);
    driver.run("select * from target order by id");
    
    List<String> res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals(11, res.size());
    Assert.assertEquals(
      "[0\torig_FyZl new1_tnlGat, " +
      "1\tnew1_KulBf, " +
      "2\tnew1_zkLGuU, " +
      "3\tnew1_jznZVac, " +
      "4\tnew1_hdyazJXL, " +
      "5\torig_VsbLsaG new1_gxclXFtP, " +
      "6\tnew1_CNZr, " +
      "7\tnew1_GoBjjuow, " +
      "8\tnew1_vRfY, " +
      "9\tnew1_bdnQA, " +
      "10\torig_dhhCassOoV new1_FNboL]", res.toString());
  }
  
  @Test
  public void testConcurrent2InsertOverwritesDiffPartitions() throws Exception {
    testConcurrent2InsertOverwrites(false);
  }
  @Test
  public void testConcurrent2InsertOverwritesSamePartition() throws Exception {
    testConcurrent2InsertOverwrites(true);
  }

  private void testConcurrent2InsertOverwrites(boolean conflict) throws Exception {
    dropTable(new String[]{"target", "source"});
    driver2.getConf().setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, true);

    driver.run("create table target (a int) partitioned by (b int) stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into target values (1,2), (3,4)");
    driver.run("create table source (a int)");
    driver.run("insert into source values (5), (7)");

    driver.compileAndRespond("insert overwrite table target partition (b='2') select * from source");
    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver2.compileAndRespond("insert overwrite table target partition (b='" +
      (conflict ? 2 : 4) + "') select * from source");

    swapTxnManager(txnMgr);
    driver.run();

    swapTxnManager(txnMgr2);
    driver2.run();

    swapTxnManager(txnMgr);
    driver.run("select * from target");
    List<String> res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals(conflict ? 3 : 4, res.size());
  }

  /**
   * ValidTxnManager.isValidTxnListState can invalidate a snapshot if a relevant write transaction was committed
   * between a query compilation and lock acquisition. When this happens we have to recompile the given query,
   * otherwise we can miss reading partitions created between. The following three cases test these scenarios.
   * @throws Exception ex
   */
  @Test
  public void testInsertOverwriteMergeInsertDynamicPartitioningSequential() throws Exception {
    dropTable(new String[]{"target", "source"});
    conf.setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, false);

    // Create partition c=1
    driver.run("create table target (a int, b int) partitioned by (c int) stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into target values (1,1,1), (2,2,1)");
    //Create partition c=2
    driver.run("create table source (a int, b int) partitioned by (c int) stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into source values (3,3,2), (4,4,2)");

    // txn 1 inserts data to an old and a new partition
    driver.run("insert into source values (5,5,2), (6,6,3)");

    // txn 2 inserts into the target table into a new partition ( and a duplicate considering the source table)
    driver.run("insert overwrite table target partition (c=2) select 3, 3");

    // txn3 merge
    driver.run("merge into target t using source s on t.a = s.a " +
      "when not matched then insert values (s.a, s.b, s.c)");
    driver.run("select * from target");
    List<String> res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    // The merge should see all three partition and not create duplicates
    Assert.assertEquals("Duplicate records found", 6, res.size());
    Assert.assertTrue("Partition 3 was skipped", res.contains("6\t6\t3"));
    dropTable(new String[]{"target", "source"});
  }

  @Test
  public void testInsertOverwriteMergeInsertDynamicPartitioningConflict() throws Exception {
    testInsertOverwriteMergeInsertDynamicPartitioningConflict(false);
  }
  @Test
  public void testInsertOverwriteMergeInsertDynamicPartitioningConflictSlowCompile() throws Exception {
    testInsertOverwriteMergeInsertDynamicPartitioningConflict(true);
  }

  private void testInsertOverwriteMergeInsertDynamicPartitioningConflict(boolean slowCompile) throws Exception {
    conf.setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, true);
    conf.setBoolVar(HiveConf.ConfVars.TXN_MERGE_INSERT_X_LOCK, true);

    Driver driver3 = new Driver(new QueryState.Builder().withHiveConf(conf).build(), null);
    dropTable(new String[]{"target", "source"});

    // Create partition c=1
    driver.run("create table target (a int, b int) partitioned by (c int) stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into target values (1,1,1), (2,2,1)");
    //Create partition c=2
    driver.run("create table source (a int, b int) partitioned by (c int) stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into source values (3,3,2), (4,4,2)");

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(new HiveConf(conf));

    if (!slowCompile) {
      // txn 1 insert data to an old and a new partition
      driver.compileAndRespond("insert into source values (5,5,2), (6,6,3)");
      swapTxnManager(txnMgr2);
      // txn 2 insert into the target table into a new partition ( and a duplicate considering the source table)
      driver2.compileAndRespond("insert overwrite table target partition (c=2) select 3, 3");
    }

    DbTxnManager txnMgr3 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(new HiveConf(conf));
    swapTxnManager(txnMgr3);

    // Compile txn 3 with only 1 known partition
    driver3.compileAndRespond("merge into target t using source s on t.a = s.a " +
      "when not matched then insert values (s.a, s.b, s.c)");

    swapTxnManager(txnMgr);
    if (!slowCompile) {
      driver.run();
    } else {
      // txn 2 insert data to an old and a new partition
      driver.run("insert into source values (5,5,2), (6,6,3)");
    }

    swapTxnManager(txnMgr2);
    if (!slowCompile) {
      driver2.run();
      // Since txn2 was committed and it is part of txn3 snapshot, snapshot should be invalidated and query re-compiled
    } else {
      // txn 3 insert into the target table into a new partition ( and a duplicate considering the source table)
      driver2.run("insert overwrite table target partition (c=2) select 3, 3");
      // Since we were writing in the target table, txn 3 should break txn 1 snapshot regardless that it was opened later
    }

    swapTxnManager(txnMgr3);
    driver3.run();

    swapTxnManager(txnMgr);
    driver.run("select * from target");
    List<String> res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    // The merge should see all three partition and not create duplicates
    Assert.assertEquals("Duplicate records found", 6, res.size());
    Assert.assertTrue("Partition 3 was skipped", res.contains("6\t6\t3"));
    driver3.close();
    dropTable(new String[]{"target", "source"});
  }

  /**
   * Check that DP with partial spec properly updates TXN_COMPONENTS
   */
  @Test
  public void testDynamicPartitionInsert() throws Exception {
    testDynamicPartitionInsert(false);
  }
  @Test
  public void testDynamicPartitionInsertSharedWrite() throws Exception {
    testDynamicPartitionInsert(true);
  }

  private void testDynamicPartitionInsert(boolean sharedWrite) throws Exception {
    dropTable(new String[] {"target"});
    driver.run("create table target (a int, b int) " +
        "partitioned by (p int, q int) clustered by (a) into 2  buckets " +
        "stored as orc TBLPROPERTIES ('transactional'='true')");
    conf.setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, !sharedWrite);

    long txnid1 = txnMgr.openTxn(ctx, "T1");
    driver.compileAndRespond("insert into target partition(p=1,q) values (1,2,2), (3,4,2), (5,6,3), (7,8,2)", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "T1");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    //table is empty, so can only lock the table
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.SHARED_READ),
        LockState.ACQUIRED, "default", "target", null, locks);
    Assert.assertEquals(
        "HIVE_LOCKS mismatch(" + JavaUtils.txnIdToString(txnid1) + "): " +
        TestTxnDbUtil.queryToString(conf, "select * from \"HIVE_LOCKS\""),
        1,
        TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"HIVE_LOCKS\" where \"HL_TXNID\"=" + txnid1));
    txnMgr.rollbackTxn();
    Assert.assertEquals(
        "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnid1) + "): " +
        TestTxnDbUtil.queryToString(conf, "select * from \"TXN_COMPONENTS\""),
        1,
        TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"TXN_COMPONENTS\" where \"TC_TXNID\"=" + txnid1));
    //now actually write to table to generate some partitions
    driver.run("insert into target partition(p=1,q) values (1,2,2), (3,4,2), (5,6,3), (7,8,2)");
    driver.run("select count(*) from target");
    List<String> r = new ArrayList<>();
    driver.getResults(r);
    Assert.assertEquals("", "4", r.get(0));
    Assert.assertEquals(//look in COMPLETED_TXN_COMPONENTS because driver.run() committed!!!!
        "COMPLETED_TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnid1 + 1) + "): " +
        TestTxnDbUtil.queryToString(conf, "select * from \"COMPLETED_TXN_COMPONENTS\""),
        2, //2 distinct partitions created
        //txnid+1 because we want txn used by previous driver.run("insert....)
        TestTxnDbUtil
            .countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\" where \"CTC_TXNID\"=" + (txnid1 + 1)));

    long txnid2 = txnMgr.openTxn(ctx, "T1");
    driver.compileAndRespond("insert into target partition(p=1,q) values (10,2,2), (30,4,2), (50,6,3), (70,8,2)", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "T1");
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    //Plan is using DummyPartition, so can only lock the table... unfortunately
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.SHARED_READ),
        LockState.ACQUIRED, "default", "target", null, locks);
    long writeId = txnMgr.getTableWriteId("default", "target");
    AddDynamicPartitions adp = new AddDynamicPartitions(txnid2, writeId, "default", "target",
        Arrays.asList("p=1/q=2", "p=1/q=2"));
    adp.setOperationType(DataOperationType.INSERT);
    txnHandler.addDynamicPartitions(adp);
    Assert.assertEquals(
        "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnid2) + "): " +
        TestTxnDbUtil.queryToString(conf, "select * from \"TXN_COMPONENTS\""),
        2, //2 distinct partitions modified
        TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"TXN_COMPONENTS\" where \"TC_TXNID\"=" + txnid2));
    txnMgr.commitTxn();
  }

  @Test
  public void testMergePartitioned() throws Exception {
    testMergePartitioned(false, false);
  }
  @Test
  public void testMergePartitionedConflict() throws Exception {
    testMergePartitioned(true, false);
  }
  @Test
  public void testMergePartitionedConflictSharedWrite() throws Exception {
    testMergePartitioned(true, true);
  }

  /**
   * "run" an Update and Merge concurrently; Check that correct locks are acquired.
   * Check state of auxiliary ACID tables.
   * @param causeConflict - true to make the operations cause a Write conflict
   */
  private void testMergePartitioned(boolean causeConflict, boolean sharedWrite) throws Exception {
    dropTable(new String[] {"target","source"});
    conf.setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, !sharedWrite);

    driver.run("create table target (a int, b int) " +
        "partitioned by (p int, q int) clustered by (a) into 2  buckets " +
        "stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into target partition(p,q) values (1,2,1,2), (3,4,1,2), (5,6,1,3), (7,8,2,2)");
    driver.run("create table source (a1 int, b1 int, p1 int, q1 int)");

    driver.compileAndRespond("update target set b = 2 where p=1", true);
    long txnId1 = txnMgr.getCurrentTxnId();
    txnMgr.acquireLocks(driver.getPlan(), ctx, "T1");
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 3, locks.size());

    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        LockState.ACQUIRED, "default", "target", "p=1/q=2", locks);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        LockState.ACQUIRED, "default", "target", "p=1/q=3", locks);

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    //start a 2nd (overlapping) txn
    driver.compileAndRespond("merge into target using source " +
        "on target.p=source.p1 and target.a=source.a1 " +
        "when matched then update set b = 11 " +
        "when not matched then insert values(a1,b1,p1,q1)", true);
    long txnid2 = txnMgr2.getCurrentTxnId();
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "T2", false);
    locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 8, locks.size());
    /*
     * W locks from T1 are still there, so all locks from T2 block.
     * The Update part of Merge requests W locks for each existing partition in target.
     * The Insert part doesn't know which partitions may be written to: thus R lock on target table.
     */
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, (sharedWrite ? LockState.ACQUIRED : LockState.WAITING),
        "default", "source", null, locks);
    long extLockId = TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.SHARED_READ),
        (sharedWrite ? LockState.ACQUIRED : LockState.WAITING),
        "default", "target", null, locks, sharedWrite).getLockid();

    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        (sharedWrite ? LockState.ACQUIRED : LockState.WAITING),
        "default", "target", "p=1/q=2", locks, sharedWrite);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        LockState.ACQUIRED, "default", "target", "p=1/q=2", locks);

    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        (sharedWrite ? LockState.ACQUIRED : LockState.WAITING),
        "default", "target", "p=1/q=3", locks, sharedWrite);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        LockState.ACQUIRED, "default", "target", "p=1/q=3", locks);

    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        (sharedWrite ? LockState.ACQUIRED : LockState.WAITING),
        "default", "target", "p=2/q=2", locks);

    Assert.assertEquals(
        "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId1) + "): " +
        TestTxnDbUtil.queryToString(conf, "select * from \"TXN_COMPONENTS\""),
        1, //because it's using a DP write
        TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"TXN_COMPONENTS\" where \"TC_TXNID\"=" + txnId1));

    //complete T1 transaction (simulate writing to 2 partitions)
    long writeId = txnMgr.getTableWriteId("default", "target");
    AddDynamicPartitions adp = new AddDynamicPartitions(txnId1, writeId, "default", "target",
        Arrays.asList("p=1/q=2", "p=1/q=3"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    Assert.assertEquals(
        "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnId1) + "): " +
        TestTxnDbUtil.queryToString(conf, "select * from \"TXN_COMPONENTS\""),
        2,
        TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"TXN_COMPONENTS\" where \"TC_TXNID\"=" + txnId1 +
            " and \"TC_OPERATION_TYPE\"='u'"));

    txnMgr.commitTxn(); //commit T1
    Assert.assertEquals("WRITE_SET mismatch(" + JavaUtils.txnIdToString(txnId1) + "): " +
        TestTxnDbUtil.queryToString(conf, "select * from \"WRITE_SET\""),
        2, //2 partitions updated
        TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"WRITE_SET\" where \"WS_TXNID\"=" + txnId1 +
            " and \"WS_OPERATION_TYPE\"='u'"));

    //re-check locks which were in Waiting state - should now be Acquired
    ((DbLockManager)txnMgr2.getLockManager()).checkLock(extLockId);
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 5, locks.size());

    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "source", null, locks);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.SHARED_READ),
        LockState.ACQUIRED, "default", "target", null, locks);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        LockState.ACQUIRED, "default", "target", "p=1/q=2", locks);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        LockState.ACQUIRED, "default", "target", "p=1/q=3", locks);
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.SHARED_WRITE : LockType.EXCL_WRITE),
        LockState.ACQUIRED, "default", "target", "p=2/q=2", locks);

    Assert.assertEquals(
        "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnid2) + "): " +
        TestTxnDbUtil.queryToString(conf, "select * from \"TXN_COMPONENTS\""),
        1, //because it's using a DP write
        TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"TXN_COMPONENTS\" where \"TC_TXNID\"=" + txnid2));

    //complete T2 txn
    //simulate Insert into 2 partitions
    writeId = txnMgr2.getTableWriteId("default", "target");
    adp = new AddDynamicPartitions(txnid2, writeId, "default", "target",
        Arrays.asList("p=1/q=2", "p=1/q=3"));
    adp.setOperationType(DataOperationType.INSERT);
    txnHandler.addDynamicPartitions(adp);
    Assert.assertEquals(
        "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnid2) + "): " +
        TestTxnDbUtil.queryToString(conf, "select * from \"TXN_COMPONENTS\""),
        2,
        TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"TXN_COMPONENTS\" where \"TC_TXNID\"=" + txnid2 +
            " and \"TC_OPERATION_TYPE\"='i'"));
    //simulate Update of 1 partitions; depending on causeConflict, choose one of the partitions
    //which was modified by the T1 update stmt or choose a non-conflicting one
    adp = new AddDynamicPartitions(txnid2, writeId, "default", "target",
        Collections.singletonList(causeConflict ? "p=1/q=2" : "p=1/q=1"));
    adp.setOperationType(DataOperationType.UPDATE);
    txnHandler.addDynamicPartitions(adp);
    Assert.assertEquals(
        "TXN_COMPONENTS mismatch(" + JavaUtils.txnIdToString(txnid2) + "): " +
        TestTxnDbUtil.queryToString(conf, "select * from \"TXN_COMPONENTS\""),
        1,
        TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"TXN_COMPONENTS\" where \"TC_TXNID\"=" + txnid2 +
            " and \"TC_OPERATION_TYPE\"='u'"));

    LockException expectedException = null;
    try {
      txnMgr2.commitTxn();
    }
    catch (LockException e) {
      expectedException = e;
    }
    if (causeConflict && sharedWrite) {
      Assert.assertNotNull("Didn't get exception", expectedException);
      Assert.assertEquals("Got wrong message code", ErrorMsg.TXN_ABORTED, expectedException.getCanonicalErrorMsg());
      Assert.assertEquals("Exception msg didn't match",
          "Aborting [txnid:4,4] due to a write conflict on default/target/p=1/q=2 committed by [txnid:3,4] u/u",
          expectedException.getCause().getMessage());
    } else {
      Assert.assertEquals("WRITE_SET mismatch(" + JavaUtils.txnIdToString(txnid2) + "): " +
          TestTxnDbUtil.queryToString(conf, "select * from \"WRITE_SET\""),
          1, //1 partitions updated
          TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"WRITE_SET\" where \"WS_TXNID\"=" + txnid2 +
              " and \"WS_OPERATION_TYPE\"='u'"));
      Assert.assertEquals("WRITE_SET mismatch(" + JavaUtils.txnIdToString(txnid2) + "): " +
          TestTxnDbUtil.queryToString(conf, "select * from \"WRITE_SET\""),
          1, //1 partitions updated (and no other entries)
          TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"WRITE_SET\" where \"WS_TXNID\"=" + txnid2));
    }
    dropTable(new String[] {"target","source"});
  }

  /**
   * This test is mostly obsolete.  The logic in the Driver.java no longer acquires any locks for
   * "show tables".  Keeping the test for now in case we change that logic.
   */
  @Test
  public void testShowTablesLock() throws Exception {
    dropTable(new String[] {"T", "T2"});
    driver.run("create table T (a int, b int)");

    txnMgr.openTxn(ctx, "Fifer");
    driver.compileAndRespond("insert into T values(1,3)", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer");
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "t", null, locks);

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnMgr2.openTxn(ctx, "Fidler");
    swapTxnManager(txnMgr2);
    driver.compileAndRespond("show tables", true);
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "Fidler");
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "t", null, locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", null, null, locks);
    txnMgr.commitTxn();
    txnMgr2.rollbackTxn();
    Assert.assertEquals("Lock remained", 0, getLocks().size());
    Assert.assertEquals("Lock remained", 0, getLocks(txnMgr2).size());

    swapTxnManager(txnMgr);
    driver.run(
        "create table T2 (a int, b int) partitioned by (p int) clustered by (a) " +
        "into 2  buckets stored as orc TBLPROPERTIES ('transactional'='false')");

    driver.compileAndRespond("insert into T2 partition(p=1) values(1,3)", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer");
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "t2", "p=1", locks);

    txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnMgr2.openTxn(ctx, "Fidler");
    swapTxnManager(txnMgr2);
    driver.compileAndRespond("show tables", true);
    txnMgr2.acquireLocks(driver.getPlan(), new Context(conf), "Fidler", false);
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "t2", "p=1", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", null, null, locks);
    txnMgr.releaseLocks(ctx.getHiveLocks());
    txnMgr2.commitTxn();
    Assert.assertEquals("Lock remained", 0, getLocks().size());
    Assert.assertEquals("Lock remained", 0, getLocks(txnMgr2).size());
  }

  @Test
  public void testFairness() throws Exception {
    testFairness(false);
  }

  @Test
  public void testFairnessZeroWaitRead() throws Exception {
    testFairness(true);
  }

  private void testFairness(boolean zeroWaitRead) throws Exception {
    dropTable(new String[]{"T6"});
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.CREATE_TABLES_AS_ACID, zeroWaitRead);
    conf.setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, !zeroWaitRead);
    driver.run("create table if not exists T6 (a int) stored as ORC");
    driver.compileAndRespond("select a from T6", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer"); //gets S lock on T6
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver.compileAndRespond("drop table if exists T6", true);
    //tries to get X lock on T6 and gets Waiting state
    ((DbTxnManager) txnMgr2).acquireLocks(driver.getPlan(), ctx, "Fiddler", false);
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 3, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T6", null, locks);
    long extLockId = TestTxnDbUtil.checkLock(LockType.EXCLUSIVE, LockState.WAITING, "default", "T6", null, locks).getLockid();

    HiveTxnManager txnMgr3 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr3);
    //this should block behind the X lock on  T6
    //this is a contrived example, in practice this query would of course fail after drop table
    driver.compileAndRespond("select a from T6", true);
    try {
      ((DbTxnManager) txnMgr3).acquireLocks(driver.getPlan(), ctx, "Fifer", false); //gets S lock on T6
    } catch (LockException ex) {
      Assert.assertTrue(zeroWaitRead);
      Assert.assertEquals("Exception msg didn't match",
        ErrorMsg.LOCK_CANNOT_BE_ACQUIRED.getMsg() + " LockResponse(lockid:" + (extLockId + 1) +
          ", state:NOT_ACQUIRED, errorMessage:Unable to acquire read lock due to an existing exclusive lock" +
          " {lockid:" + extLockId + " intLockId:1 txnid:" + txnMgr2.getCurrentTxnId() +
          " db:default table:t6 partition:null state:WAITING type:EXCLUSIVE})",
        ex.getMessage());
    }
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", (zeroWaitRead ? 3 : 4), locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T6", null, locks);
    if (!zeroWaitRead) {
      TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.WAITING, "default", "T6", null, locks);
    }
    TestTxnDbUtil.checkLock(LockType.EXCLUSIVE, LockState.WAITING, "default", "T6", null, locks);
  }

  /**
   * T7 is a table with 2 partitions
   * 1. run select from T7
   * 2. run drop partition from T7
   * concurrently with 1 starting first so that 2 blocks
   * 3. start another concurrent select on T7 - it should block behind waiting X (from drop) - LM should be fair
   * 4. finish #1 so that drop unblocks
   * 5. rollback the drop to release its X lock
   * 6. # should unblock
   */
  @Test
  public void testFairness2() throws Exception {
    testFairness2(false);
  }

  @Test
  public void testFairness2ZeroWaitRead() throws Exception {
    testFairness2(true);
  }

  private void testFairness2(boolean zeroWaitRead) throws Exception {
    dropTable(new String[]{"T7"});
    conf.setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, !zeroWaitRead);
    driver.run("create table if not exists T7 (a int) " +
        "partitioned by (p int) stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into T7 partition(p) values(1,1),(1,2)"); //create 2 partitions
    driver.compileAndRespond("select a from T7 ", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer"); //gets S lock on T7
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver.compileAndRespond("alter table T7 drop partition (p=1)", true);
    //tries to get X lock on T7.p=1 and gets Waiting state
    ((DbTxnManager) txnMgr2).acquireLocks(driver.getPlan(), ctx, "Fiddler", false);
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 3, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T7", "p=1", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T7", "p=2", locks);
    long extLockId = TestTxnDbUtil.checkLock(LockType.EXCLUSIVE, LockState.WAITING, "default", "T7", "p=1", locks).getLockid();

    HiveTxnManager txnMgr3 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr3);
    //this should block behind the X lock on  T7.p=1
    driver.compileAndRespond("select a from T7", true);
    //tries to get S lock on T7, S on T7.p=1 and S on T7.p=2
    try {
      ((DbTxnManager) txnMgr3).acquireLocks(driver.getPlan(), ctx, "Fifer", false);
    } catch (LockException ex) {
      Assert.assertTrue(zeroWaitRead);
      Assert.assertEquals("Exception msg didn't match",
        ErrorMsg.LOCK_CANNOT_BE_ACQUIRED.getMsg() + " LockResponse(lockid:" + (extLockId + 1) +
          ", state:NOT_ACQUIRED, errorMessage:Unable to acquire read lock due to an existing exclusive lock" +
          " {lockid:" + extLockId + " intLockId:1 txnid:" + txnMgr2.getCurrentTxnId() +
          " db:default table:t7 partition:p=1 state:WAITING type:EXCLUSIVE})",
        ex.getMessage());
    }
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", (zeroWaitRead ? 3 : 5), locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T7", "p=1", locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T7", "p=2", locks);
    if (!zeroWaitRead) {
      TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.WAITING, "default", "T7", "p=1", locks);
      TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.WAITING, "default", "T7", "p=2", locks);
    }
    TestTxnDbUtil.checkLock(LockType.EXCLUSIVE, LockState.WAITING, "default", "T7", "p=1", locks);

    txnMgr.commitTxn(); //release locks from "select a from T7" - to unblock the drop partition
    //re-test the "drop partiton" X lock
    ((DbLockManager)txnMgr2.getLockManager()).checkLock(locks.get(zeroWaitRead ? 2 : 4).getLockid());
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", (zeroWaitRead ? 1 : 3), locks.size());
    TestTxnDbUtil.checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "T7", "p=1", locks);
    if (!zeroWaitRead) {
      TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.WAITING, "default", "T7", "p=1", locks);
      TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.WAITING, "default", "T7", "p=2", locks);

      txnMgr2.rollbackTxn(); //release the X lock on T7.p=1
      //re-test the locks
      ((DbLockManager) txnMgr2.getLockManager()).checkLock(locks.get(1).getLockid()); //S lock on T7
      locks = getLocks();
      Assert.assertEquals("Unexpected lock count", 2, locks.size());
      TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T7", "p=1", locks);
      TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "T7", "p=2", locks);
    } else {
      txnMgr2.rollbackTxn();
    }
    txnMgr3.rollbackTxn();
    dropTable(new String[]{"T7"});
  }

  @Test
  public void testValidWriteIdListSnapshot() throws Exception {
    dropTable(new String[] {"temp.T7"});
    driver.run("create database if not exists temp");
    // Create a transactional table
    driver.run("create table if not exists temp.T7(a int, b int) clustered by(b) into 2 buckets stored as orc " +
        "TBLPROPERTIES ('transactional'='true')");

    // Open a base txn which allocates write ID and then committed.
    long baseTxnId = txnMgr.openTxn(ctx, "u0");
    long baseWriteId = txnMgr.getTableWriteId("temp", "T7");
    Assert.assertEquals(1, baseWriteId);
    txnMgr.commitTxn(); // committed baseTxnId

    // Open a txn with no writes.
    HiveTxnManager txnMgr1 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    long underHwmOpenTxnId = txnMgr1.openTxn(ctx, "u1");
    Assert.assertTrue("Invalid txn ID", underHwmOpenTxnId > baseTxnId);

    // Open a txn to be tested for ValidWriteIdList. Get the ValidTxnList during open itself.
    // Verify the ValidWriteIdList with no open/aborted write txns on this table.
    // Write ID of committed txn should be valid.
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    long testTxnId = txnMgr2.openTxn(ctx, "u2");
    Assert.assertTrue("Invalid txn ID", testTxnId > underHwmOpenTxnId);
    String testValidTxns = txnMgr2.getValidTxns().toString();
    ValidWriteIdList testValidWriteIds = txnMgr2.getValidWriteIds(Collections.singletonList("temp.t7"), testValidTxns)
            .getTableValidWriteIdList("temp.t7");
    Assert.assertEquals(baseWriteId, testValidWriteIds.getHighWatermark());
    Assert.assertTrue("Invalid write ID list", testValidWriteIds.isWriteIdValid(baseWriteId));

    // Open a txn which allocate write ID and remain open state.
    HiveTxnManager txnMgr3 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    long aboveHwmOpenTxnId = txnMgr3.openTxn(ctx, "u3");
    Assert.assertTrue("Invalid txn ID", aboveHwmOpenTxnId > testTxnId);
    long aboveHwmOpenWriteId = txnMgr3.getTableWriteId("temp", "T7");
    Assert.assertEquals(2, aboveHwmOpenWriteId);

    // Allocate writeId to txn under HWM. This will get Id greater than a txn > HWM.
    long underHwmOpenWriteId = txnMgr1.getTableWriteId("temp", "T7");
    Assert.assertEquals(3, underHwmOpenWriteId);

    // Verify the ValidWriteIdList with one open txn on this table. Write ID of open txn should be invalid.
    testValidWriteIds = txnMgr2.getValidWriteIds(Collections.singletonList("temp.t7"), testValidTxns)
        .getTableValidWriteIdList("temp.t7");
    Assert.assertEquals(underHwmOpenWriteId, testValidWriteIds.getHighWatermark());
    Assert.assertTrue("Invalid write ID list", testValidWriteIds.isWriteIdValid(baseWriteId));
    Assert.assertFalse("Invalid write ID list", testValidWriteIds.isWriteIdValid(underHwmOpenWriteId));
    Assert.assertFalse("Invalid write ID list", testValidWriteIds.isWriteIdValid(aboveHwmOpenWriteId));

    // Commit the txn under HWM.
    // Verify the writeId of this committed txn should be invalid for test txn.
    txnMgr1.commitTxn();
    testValidWriteIds = txnMgr2.getValidWriteIds(Collections.singletonList("temp.t7"), testValidTxns)
        .getTableValidWriteIdList("temp.t7");
    Assert.assertEquals(underHwmOpenWriteId, testValidWriteIds.getHighWatermark());
    Assert.assertTrue("Invalid write ID list", testValidWriteIds.isWriteIdValid(baseWriteId));
    Assert.assertFalse("Invalid write ID list", testValidWriteIds.isWriteIdValid(underHwmOpenWriteId));
    Assert.assertFalse("Invalid write ID list", testValidWriteIds.isWriteIdValid(aboveHwmOpenWriteId));

    // Allocate writeId from test txn and then verify ValidWriteIdList.
    // Write Ids of committed and self test txn should be valid but writeId of open txn should be invalid.
    // WriteId of recently committed txn which was open when get ValidTxnList snapshot should be invalid as well.
    long testWriteId = txnMgr2.getTableWriteId("temp", "T7");
    Assert.assertEquals(4, testWriteId);

    testValidWriteIds = txnMgr2.getValidWriteIds(Collections.singletonList("temp.t7"), testValidTxns)
        .getTableValidWriteIdList("temp.t7");
    Assert.assertEquals(testWriteId, testValidWriteIds.getHighWatermark());
    Assert.assertTrue("Invalid write ID list", testValidWriteIds.isWriteIdValid(baseWriteId));
    Assert.assertTrue("Invalid write ID list", testValidWriteIds.isWriteIdValid(testWriteId));
    Assert.assertFalse("Invalid write ID list", testValidWriteIds.isWriteIdValid(underHwmOpenWriteId));
    Assert.assertFalse("Invalid write ID list", testValidWriteIds.isWriteIdValid(aboveHwmOpenWriteId));
    txnMgr2.commitTxn();
    txnMgr3.commitTxn();

    driver.run("drop database if exists temp cascade");
  }

  @Test
  public void testValidTxnList() throws Exception {
    long readTxnId = txnMgr.openTxn(ctx, "u0", TxnType.READ_ONLY);
    HiveTxnManager txnManager1 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnManager1.openTxn(ctx, "u0");
    //Excludes open read only txns by default
    ValidTxnList validTxns = txnManager1.getValidTxns();
    Assert.assertEquals(0, validTxns.getInvalidTransactions().length);

    //Exclude open repl created only txns
    validTxns = txnManager1.getValidTxns(Arrays.asList(TxnType.REPL_CREATED));
    Assert.assertEquals(1, validTxns.getInvalidTransactions().length);
    Assert.assertEquals(readTxnId, validTxns.getInvalidTransactions()[0]);
    txnManager1.commitTxn();
    txnMgr.commitTxn();

    long replTxnId = txnMgr.replOpenTxn("default.*", Arrays.asList(1L), "u0").get(0);
    txnManager1 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnManager1.openTxn(ctx, "u0");
    //Excludes open read only txns by default
    validTxns = txnManager1.getValidTxns();
    Assert.assertEquals(1, validTxns.getInvalidTransactions().length);
    Assert.assertEquals(replTxnId, validTxns.getInvalidTransactions()[0]);

    //Exclude open repl created only txns
    validTxns = txnManager1.getValidTxns(Arrays.asList(TxnType.REPL_CREATED));
    Assert.assertEquals(0, validTxns.getInvalidTransactions().length);

    //Exclude open read only txns
    validTxns = txnManager1.getValidTxns(Arrays.asList(TxnType.READ_ONLY));
    Assert.assertEquals(1, validTxns.getInvalidTransactions().length);
    Assert.assertEquals(replTxnId, validTxns.getInvalidTransactions()[0]);
    CommitTxnRequest commitTxnRequest = new CommitTxnRequest(1L);
    commitTxnRequest.setReplPolicy("default.*");
    commitTxnRequest.setTxn_type(TxnType.REPL_CREATED);
    txnMgr.replCommitTxn(commitTxnRequest);

    //Transaction is committed. So no open txn
    validTxns = txnManager1.getValidTxns();
    Assert.assertEquals(0, validTxns.getInvalidTransactions().length);

    //Exclude open read only txns
    validTxns = txnManager1.getValidTxns(Arrays.asList(TxnType.READ_ONLY));
    Assert.assertEquals(0, validTxns.getInvalidTransactions().length);
    txnManager1.commitTxn();
  }

  @Rule
  public TemporaryFolder exportFolder = new TemporaryFolder();

  @Test
  public void testLoadData() throws Exception {
    testLoadData(false);
  }
  @Test
  public void testLoadDataSharedWrite() throws Exception {
    testLoadData(true);
  }

  private void testLoadData(boolean sharedWrite) throws Exception {
    dropTable(new String[] {"T2"});
    conf.setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, !sharedWrite);
    driver.run("create table T2(a int) stored as ORC TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into T2 values(1)");
    String exportLoc = exportFolder.newFolder("1").toString();
    driver.run("export table T2 to '" + exportLoc + "/2'");
    driver.compileAndRespond("load data inpath '" + exportLoc + "/2/data' overwrite into table T2");
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer");
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock((sharedWrite ? LockType.EXCL_WRITE : LockType.EXCLUSIVE),
        LockState.ACQUIRED, "default", "T2", null, locks);
    txnMgr.commitTxn();
  }

  @Test
  public void testMmConversionLocks() throws Exception {
    dropTable(new String[] {"T"});
    driver.run("create table T (a int, b int) tblproperties('transactional'='false')");
    driver.run("insert into T values(0,2),(1,4)");
    driver.compileAndRespond("ALTER TABLE T set tblproperties" +
        "('transactional'='true', 'transactional_properties'='insert_only')", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer"); //gets X lock on T
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "T", null, locks);
  }

  @Test
  public void testTruncateWithBaseLockingExlWrite() throws Exception {
    testTruncate(true);
  }

  @Test
  public void testTruncateWithExl() throws Exception {
    testTruncate(false);
  }

  private void testTruncate(boolean useBaseDir) throws Exception {
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_TRUNCATE_USE_BASE, useBaseDir);
    dropTable(new String[] {"T"});
    driver.run("create table T (a int, b int) stored as orc tblproperties('transactional'='true')");
    driver.run("insert into T values(0,2),(1,4)");
    driver.run("truncate table T");
    driver.compileAndRespond("truncate table T", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fifer"); //gets X lock on T
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(useBaseDir ? LockType.EXCL_WRITE : LockType.EXCLUSIVE, LockState.ACQUIRED, "default", "T", null, locks);

    txnMgr.commitTxn();
    dropTable(new String[] {"T"});
  }

  @Test
  public void testAnalyze() throws Exception {
    dropTable(new String[] {"tab_acid", "tab_not_acid"});

    driver.run("create table tab_not_acid (key string, value string) partitioned by (ds string, hr string) " +
        "stored as textfile");
    driver.run("insert into tab_not_acid partition (ds='2008-04-08', hr='11') values ('238', 'val_238')");
    driver.run("analyze table tab_not_acid PARTITION (ds, hr) compute statistics");

    driver.run("create table tab_acid (key string, value string) partitioned by (ds string, hr string) " +
        "stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into tab_acid PARTITION (ds, hr) select * from tab_not_acid");
    driver.run("analyze table tab_acid PARTITION (ds, hr) compute statistics");

    conf.setBoolVar(HiveConf.ConfVars.HIVE_TXN_EXT_LOCKING_ENABLED, true);

    driver.compileAndRespond("analyze table tab_not_acid PARTITION(ds, hr) compute statistics", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "dummy");

    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_not_acid", "ds=2008-04-08/hr=11", locks);
    txnMgr.releaseLocks(ctx.getHiveLocks());

    driver.compileAndRespond("analyze table tab_acid PARTITION(ds, hr) compute statistics");
    txnMgr.acquireLocks(driver.getPlan(), ctx, "dummy");

    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_acid", "ds=2008-04-08/hr=11", locks);
  }

  @Test
  public void testFullTableReadLock() throws Exception {
    dropTable(new String[] {"tab_acid", "tab_not_acid"});
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_LOCKS_PARTITION_THRESHOLD, 2);

    driver.run("create table if not exists tab_acid (a int, b int) partitioned by (p string) " +
      "stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("create table if not exists tab_not_acid (na int, nb int) partitioned by (np string) " +
      "stored as orc TBLPROPERTIES ('transactional'='false')");
    driver.run("insert into tab_acid partition(p) (a,b,p) values(1,2,'foo'),(3,4,'bar')");
    driver.run("insert into tab_not_acid partition(np) (na,nb,np) values(1,2,'blah'),(3,4,'doh')");

    driver.compileAndRespond("select * from tab_acid inner join tab_not_acid on a = na", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "T1");
    List<ShowLocksResponseElement> locks = getLocks(txnMgr);
    Assert.assertEquals("Unexpected lock count", 2, locks.size());

    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_acid", null, locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_READ, LockState.ACQUIRED, "default", "tab_not_acid", null, locks);
  }

  @Test
  public void testRemoveDuplicateCompletedTxnComponents() throws Exception {
    dropTable(new String[] {"tab_acid"});
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_ON, true);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_CLEANER_ON, true);

    driver.run("create table if not exists tab_acid (a int) partitioned by (p string) " +
      "stored as orc TBLPROPERTIES ('transactional'='true')");

    driver.run("insert into tab_acid values(1,'foo'),(3,'bar')");
    driver.run("insert into tab_acid values(2,'foo'),(4,'bar')");
    driver.run("delete from tab_acid where a=2");

    Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from \"COMPLETED_TXN_COMPONENTS\""),
      5, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\""));

    MetastoreTaskThread houseKeeper = new AcidHouseKeeperService();
    MetastoreTaskThread compactionHouseKeeper = new CompactionHouseKeeperService();
    houseKeeper.setConf(conf);
    compactionHouseKeeper.setConf(conf);
    houseKeeper.run();
    compactionHouseKeeper.run();

    Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from \"COMPLETED_TXN_COMPONENTS\""),
      2, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\""));

    Assert.assertEquals(1, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\" " +
        "where \"CTC_PARTITION\"='p=bar' and \"CTC_TXNID\"=3"));
    Assert.assertEquals(1, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\" " +
        "where \"CTC_PARTITION\"='p=foo' and \"CTC_TXNID\"=4"));

    driver.run("insert into tab_acid values(3,'foo')");
    driver.run("insert into tab_acid values(4,'foo')");

    Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from \"COMPLETED_TXN_COMPONENTS\""),
      4, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\""));

    houseKeeper.run();
    compactionHouseKeeper.run();
    Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from \"COMPLETED_TXN_COMPONENTS\""),
      3, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\""));

    Assert.assertEquals(2, TestTxnDbUtil.countQueryAgent(conf, "select count(*) from \"COMPLETED_TXN_COMPONENTS\" " +
        "where \"CTC_PARTITION\"='p=foo' and \"CTC_TXNID\" IN (4,6)"));
  }

  @Test
  public void testSkipAcquireLocksForExplain() throws Exception {
    dropTable(new String[] {"tab_acid"});

    driver.run("create table if not exists tab_acid (a int) partitioned by (p string) " +
      "stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into tab_acid values(1,'foo'),(3,'bar')");

    driver.compileAndRespond("explain update tab_acid set a = a+2 where a > 2", true);
    driver.lockAndRespond();

    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 0, locks.size());
  }

  @Test
  public void testInsertSnapshotIsolationMinHistoryDisabled() throws Exception {
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.TXN_USE_MIN_HISTORY_LEVEL, false);
    testInsertSnapshotIsolation();
  }

  @Test
  public void testInsertSnapshotIsolation() throws Exception {
    dropTable(new String[] {"tab_acid"});

    driver.run("create table if not exists tab_acid (a int, b int) " +
        "stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.compileAndRespond("insert into tab_acid values(1,2)");

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver2.compileAndRespond("select * from tab_acid");
    swapTxnManager(txnMgr);

    driver.run();
    txnHandler.cleanTxnToWriteIdTable();
    swapTxnManager(txnMgr2);

    driver2.run();
    List<String> res = new ArrayList<>();
    driver2.getFetchTask().fetch(res);
    Assert.assertEquals(0, res.size());
  }

  @Test
  public void testUpdateSnapshotIsolationMinHistoryDisabled() throws Exception {
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.TXN_USE_MIN_HISTORY_LEVEL, false);
    testUpdateSnapshotIsolation();
  }

  @Test
  public void testUpdateSnapshotIsolation() throws Exception {
    dropTable(new String[] {"tab_acid"});

    driver.run("create table if not exists tab_acid (a int, b int) " +
        "stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into tab_acid values(1,2)");
    driver.compileAndRespond("update tab_acid set a=2");

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver2.compileAndRespond("select * from tab_acid");
    swapTxnManager(txnMgr);

    driver.run();
    txnHandler.cleanTxnToWriteIdTable();
    swapTxnManager(txnMgr2);

    driver2.run();
    List<String> res = new ArrayList<>();
    driver2.getFetchTask().fetch(res);
    Assert.assertEquals(1, res.size());
    Assert.assertEquals("1\t2", res.get(0));
  }

  @Test
  public void testDropPartitionNonBlocking() throws Exception {
    testDropPartition(false);
  }
  @Test
  public void testDropPartitionBlocking() throws Exception {
    testDropPartition(true);
  }

  private void testDropPartition(boolean blocking) throws Exception {
    dropTable(new String[] {"tab_acid"});
    FileSystem fs = FileSystem.get(conf);
    
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_LOCKS_PARTITION_THRESHOLD, 1);
    driver = Mockito.spy(driver);

    HiveConf.setBoolVar(driver2.getConf(), HiveConf.ConfVars.HIVE_ACID_DROP_PARTITION_USE_BASE, !blocking);
    driver2 = Mockito.spy(driver2);

    driver.run("create table if not exists tab_acid (a int, b int) partitioned by (p string) " +
      "stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into tab_acid partition(p) (a,b,p) values(1,2,'foo'),(3,4,'bar')");

    driver.compileAndRespond("select * from tab_acid");
    List<String> res = new ArrayList<>();

    driver.lockAndRespond();
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());

    TestTxnDbUtil.checkLock(LockType.SHARED_READ,
      LockState.ACQUIRED, "default", "tab_acid", null, locks);

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver2.compileAndRespond("alter table tab_acid drop partition (p='foo')");

    if (blocking) {
      txnMgr2.acquireLocks(driver2.getPlan(), ctx, null, false);
      locks = getLocks();

      ShowLocksResponseElement checkLock = TestTxnDbUtil.checkLock(LockType.EXCLUSIVE,
        LockState.WAITING, "default", "tab_acid", "p=foo", locks);

      swapTxnManager(txnMgr);
      Mockito.doNothing().when(driver).lockAndRespond();
      driver.run();

      driver.getFetchTask().fetch(res);
      swapTxnManager(txnMgr2);

      ReflectionUtil.setField(txnMgr2, "numStatements", 0);
      txnMgr2.getMS().unlock(checkLock.getLockid());
    }
    driver2.lockAndRespond();
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", blocking ? 1 : 2, locks.size());

    TestTxnDbUtil.checkLock(blocking ? LockType.EXCLUSIVE : LockType.EXCL_WRITE,
      LockState.ACQUIRED, "default", "tab_acid", "p=foo", locks);

    Mockito.doNothing().when(driver2).lockAndRespond();
    driver2.run();

    if (!blocking) {
      swapTxnManager(txnMgr);
      Mockito.doNothing().when(driver).lockAndRespond();
      driver.run();
    }
    Mockito.reset(driver, driver2);
    
    FileStatus[] stat = fs.listStatus(new Path(getWarehouseDir(), "tab_acid" + (blocking ? "" : "/p=foo")),
      (blocking ? path -> path.getName().equals("p=foo") : AcidUtils.baseFileFilter));
    if ((blocking ? 0 : 1) != stat.length) {
      Assert.fail("Partition data was " + (blocking ? "not " : "") + "removed from FS");
    }
    driver.getFetchTask().fetch(res);
    Assert.assertEquals("Expecting 2 rows and found " + res.size(), 2, res.size());
    
    driver.run("select * from tab_acid where p='foo'");
    res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals("Expecting 0 rows and found " + res.size(), 0, res.size());

    //re-create partition with the same name
    driver.run("insert into tab_acid partition(p) (a,b,p) values(1,2,'foo')");
    
    driver.run("select * from tab_acid where p='foo'");
    res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals("Expecting 1 rows and found " + res.size(), 1, res.size());
  }
  
  @Test
  public void testDropTableNonBlocking() throws Exception {
    testDropTable(false);
  }
  @Test
  public void testDropTableBlocking() throws Exception {
    testDropTable(true);
  }

  private void testDropTable(boolean blocking) throws Exception {
    dropTable(new String[] {"tab_acid"});
    FileSystem fs = FileSystem.get(conf);

    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED, !blocking);
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_LOCKS_PARTITION_THRESHOLD, 1);
    driver = Mockito.spy(driver);

    HiveConf.setBoolVar(driver2.getConf(), HiveConf.ConfVars.HIVE_ACID_CREATE_TABLE_USE_SUFFIX, !blocking);
    driver2 = Mockito.spy(driver2);

    driver.run("create table if not exists tab_acid (a int, b int) partitioned by (p string) " +
      "stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into tab_acid partition(p) (a,b,p) values(1,2,'foo'),(3,4,'bar')");

    driver.compileAndRespond("select * from tab_acid");
    List<String> res = new ArrayList<>();
    
    driver.lockAndRespond();
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", blocking ? 1 : 0, locks.size());

    if (blocking) {
      TestTxnDbUtil.checkLock(LockType.SHARED_READ,
        LockState.ACQUIRED, "default", "tab_acid", null, locks);
    }

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver2.compileAndRespond("drop table if exists tab_acid");

    if (blocking) {
      txnMgr2.acquireLocks(driver2.getPlan(), ctx, null, false);
      locks = getLocks();

      ShowLocksResponseElement checkLock = TestTxnDbUtil.checkLock(LockType.EXCLUSIVE,
        LockState.WAITING, "default", "tab_acid", null, locks);

      swapTxnManager(txnMgr);
      Mockito.doNothing().when(driver).lockAndRespond();
      driver.run();
      
      driver.getFetchTask().fetch(res);
      swapTxnManager(txnMgr2);

      ReflectionUtil.setField(txnMgr2, "numStatements", 0);
      txnMgr2.getMS().unlock(checkLock.getLockid());
    }
    driver2.lockAndRespond();
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    
    TestTxnDbUtil.checkLock(blocking ? LockType.EXCLUSIVE : LockType.EXCL_WRITE, 
      LockState.ACQUIRED, "default", "tab_acid", null, locks);
    
    Mockito.doNothing().when(driver2).lockAndRespond();
    driver2.run();

    if (!blocking) {
      swapTxnManager(txnMgr);
      Mockito.doNothing().when(driver).lockAndRespond();
      driver.run();
    }
    Mockito.reset(driver, driver2);
    
    FileStatus[] stat = fs.listStatus(new Path(getWarehouseDir()),
      t -> t.getName().matches("tab_acid" + (blocking ? "" : SOFT_DELETE_TABLE_PATTERN)));
    if ((blocking ? 0 : 1) != stat.length) {
      Assert.fail("Table data was " + (blocking ? "not " : "") + "removed from FS");
    }
    driver.getFetchTask().fetch(res);
    Assert.assertEquals("Expecting 2 rows and found " + res.size(), 2, res.size());

    try {
      driver.run("select * from tab_acid");
    } catch (CommandProcessorException ex) {
      Assert.assertEquals(ErrorMsg.INVALID_TABLE.getErrorCode(), ex.getResponseCode());
    }
    
    //re-create table with the same name
    driver.run("create table if not exists tab_acid (a int, b int) partitioned by (p string) " +
      "stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into tab_acid partition(p) (a,b,p) values(1,2,'foo'),(3,4,'bar')");
    
    driver.run("select * from tab_acid ");
    res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals("Expecting 2 rows and found " + res.size(), 2, res.size());
  }
  
  @Test
  public void testDropTableNonBlocking2() throws Exception {
    dropTable(new String[] {"tab_acid"});

    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED, true);
    HiveConf.setBoolVar(driver2.getConf(), HiveConf.ConfVars.HIVE_ACID_CREATE_TABLE_USE_SUFFIX, true);

    driver.run("create table if not exists tab_acid (a int, b int) partitioned by (p string) " +
      "stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into tab_acid partition(p) (a,b,p) values(1,2,'foo'),(3,4,'bar')");

    driver.compileAndRespond("select * from tab_acid");

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    // when running this, valid writeid list is not yet fetched by the `select` operation, 
    // so we should keep TXN_TO_WRITE_ID entries until the Cleaner runs. 
    driver2.run("drop table if exists tab_acid");

    swapTxnManager(txnMgr);
    driver.run();

    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stat = fs.listStatus(new Path(getWarehouseDir()),
      t -> t.getName().matches("tab_acid" + SOFT_DELETE_TABLE_PATTERN));
    if (1 != stat.length) {
      Assert.fail("Table data was removed from FS");
    }

    List<String> res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals("No records found", 2, res.size());
  }
  
  @Test
  public void testRenameTableNonBlocking() throws Exception {
    testRenameTable(false);
  }
  @Test
  public void testRenameTableBlocking() throws Exception {
    testRenameTable(true);
  }

  private void testRenameTable(boolean blocking) throws Exception {
    dropTable(new String[] {"tab_acid", "tab_acid_v2"});
    FileSystem fs = FileSystem.get(conf);

    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED, !blocking);
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_LOCKS_PARTITION_THRESHOLD, 1);
    driver = Mockito.spy(driver);

    HiveConf.setBoolVar(driver2.getConf(), HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED, !blocking);
    driver2 = Mockito.spy(driver2);

    driver.run("create table if not exists tab_acid (a int, b int) partitioned by (p string) " +
      "stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into tab_acid partition(p) (a,b,p) values(1,2,'foo'),(3,4,'bar')");

    driver.compileAndRespond("select * from tab_acid");
    List<String> res = new ArrayList<>();

    driver.lockAndRespond();
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", blocking ? 1 : 0, locks.size());

    if (blocking) {
      TestTxnDbUtil.checkLock(LockType.SHARED_READ,
        LockState.ACQUIRED, "default", "tab_acid", null, locks);
    }

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver2.compileAndRespond("alter table tab_acid rename to tab_acid_v2");

    if (blocking) {
      txnMgr2.acquireLocks(driver2.getPlan(), ctx, null, false);
      locks = getLocks();

      ShowLocksResponseElement checkLock = TestTxnDbUtil.checkLock(LockType.EXCLUSIVE,
        LockState.WAITING, "default", "tab_acid", null, locks);

      swapTxnManager(txnMgr);
      Mockito.doNothing().when(driver).lockAndRespond();
      driver.run();

      driver.getFetchTask().fetch(res);
      swapTxnManager(txnMgr2);

      ReflectionUtil.setField(txnMgr2, "numStatements", 0);
      txnMgr2.getMS().unlock(checkLock.getLockid());
    }
    driver2.lockAndRespond();
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ,
            LockState.ACQUIRED, "default", null, null, locks);//destination database
    TestTxnDbUtil.checkLock(blocking ? LockType.EXCLUSIVE : LockType.EXCL_WRITE,
      LockState.ACQUIRED, "default", "tab_acid", null, locks);//source table

    Mockito.doNothing().when(driver2).lockAndRespond();
    driver2.run();

    if (!blocking) {
      swapTxnManager(txnMgr);
      Mockito.doNothing().when(driver).lockAndRespond();
      driver.run();
    }
    Mockito.reset(driver, driver2);

    FileStatus[] stat = fs.listStatus(new Path(getWarehouseDir()),
      t -> t.getName().matches("tab_acid" + (blocking ? "_v2" : SOFT_DELETE_TABLE_PATTERN)));
    if (1 != stat.length) {
      Assert.fail("Table couldn't be found on FS");
    }
    driver.getFetchTask().fetch(res);
    Assert.assertEquals("Expecting 2 rows and found " + res.size(), 2, res.size());

    try {
      driver.run("select * from tab_acid");
    } catch (CommandProcessorException ex) {
      Assert.assertEquals(ErrorMsg.INVALID_TABLE.getErrorCode(), ex.getResponseCode());
    }

    driver.run("select * from tab_acid_v2");
    res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals("Expecting 2 rows and found " + res.size(), 2, res.size());

    //create table with the same name
    driver.run("create table if not exists tab_acid (a int, b int) partitioned by (p string) " +
      "stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into tab_acid partition(p) (a,b,p) values(1,2,'foo'),(3,4,'bar')");

    driver.run("select * from tab_acid ");
    res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals("Expecting 2 rows and found " + res.size(), 2, res.size());
  }

  @Test
  public void testDropAlterViewNoLocks() throws Exception {
    driver.run("drop view if exists v_tab_acid");
    dropTable(new String[] {"tab_acid"});

    driver.run("create table if not exists tab_acid (a int, b int) partitioned by (p string) " +
      "stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into tab_acid partition(p) (a,b,p) values(1,2,'foo'),(3,4,'bar')");

    driver.run("create view v_tab_acid partitioned on (p) " +
      "as select a, p from tab_acid where b > 1");

    driver.compileAndRespond("alter view v_tab_acid as select a, p from tab_acid where b < 5");
    
    driver.lockAndRespond();
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ,
      LockState.ACQUIRED, "default", "tab_acid", null, locks);
    // FIXME: redundant read-lock on db level
    TestTxnDbUtil.checkLock(LockType.SHARED_READ,
      LockState.ACQUIRED, "default", null, null, locks);
    driver.close();

    driver.compileAndRespond("drop view if exists v_tab_acid");

    driver.lockAndRespond();
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 0, locks.size());
  }

  @Test
  public void testDropMaterializedViewNonBlocking() throws Exception {
    testDropMaterializedView(false);
  }
  @Test
  public void testDropMaterializedViewBlocking() throws Exception {
    testDropMaterializedView(true);
  }

  private void testDropMaterializedView(boolean blocking) throws Exception {
    driver.run("drop materialized view if exists mv_tab_acid");
    dropTable(new String[] {"tab_acid"});
    FileSystem fs = FileSystem.get(conf);

    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED, !blocking);
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_LOCKS_PARTITION_THRESHOLD, 1);
    driver = Mockito.spy(driver);

    HiveConf.setBoolVar(driver2.getConf(), HiveConf.ConfVars.HIVE_ACID_CREATE_TABLE_USE_SUFFIX, !blocking);
    driver2 = Mockito.spy(driver2);

    driver.run("create table if not exists tab_acid (a int, b int) partitioned by (p string) " +
      "stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into tab_acid partition(p) (a,b,p) values(1,2,'foo'),(3,4,'bar')");

    driver.run("create materialized view mv_tab_acid partitioned on (p) " +
      "stored as orc TBLPROPERTIES ('transactional'='true') as select a, p from tab_acid where b > 1");

    driver.compileAndRespond("select a, p from tab_acid where b > 1");
    List<String> res = new ArrayList<>();

    driver.lockAndRespond();
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", blocking ? 2 : 0, locks.size());

    if (blocking) {
      TestTxnDbUtil.checkLock(LockType.SHARED_READ,
        LockState.ACQUIRED, "default", "tab_acid", null, locks);
      TestTxnDbUtil.checkLock(LockType.SHARED_READ,
        LockState.ACQUIRED, "default", "mv_tab_acid", null, locks);
    }

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver2.compileAndRespond("drop materialized view if exists mv_tab_acid");

    if (blocking) {
      txnMgr2.acquireLocks(driver2.getPlan(), ctx, null, false);
      locks = getLocks();

      ShowLocksResponseElement checkLock = TestTxnDbUtil.checkLock(LockType.EXCLUSIVE,
        LockState.WAITING, "default", "mv_tab_acid", null, locks);

      swapTxnManager(txnMgr);
      Mockito.doNothing().when(driver).lockAndRespond();
      driver.run();

      driver.getFetchTask().fetch(res);
      swapTxnManager(txnMgr2);

      ReflectionUtil.setField(txnMgr2, "numStatements", 0);
      txnMgr2.getMS().unlock(checkLock.getLockid());
    }
    driver2.lockAndRespond();
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());

    TestTxnDbUtil.checkLock(blocking ? LockType.EXCLUSIVE : LockType.EXCL_WRITE,
      LockState.ACQUIRED, "default", "mv_tab_acid", null, locks);

    Mockito.doNothing().when(driver2).lockAndRespond();
    driver2.run();

    if (!blocking) {
      swapTxnManager(txnMgr);
      Mockito.doNothing().when(driver).lockAndRespond();
      driver.run();
    }
    Mockito.reset(driver, driver2);

    FileStatus[] stat = fs.listStatus(new Path(getWarehouseDir()),
      t -> t.getName().matches("mv_tab_acid" + (blocking ? "" : SOFT_DELETE_TABLE_PATTERN)));
    if ((blocking ? 0 : 1) != stat.length) {
      Assert.fail("Table data was " + (blocking ? "not" : "") + "removed from FS");
    }
    driver.getFetchTask().fetch(res);
    Assert.assertEquals("Expecting 2 rows and found " + res.size(), 2, res.size());

    try {
      driver.run("select * from mv_tab_acid");
    } catch (CommandProcessorException ex) {
      Assert.assertEquals(ErrorMsg.INVALID_TABLE.getErrorCode(), ex.getResponseCode());
    }

    //re-create MV with the same name
    driver.run("create materialized view mv_tab_acid partitioned on (p) " +
      "stored as orc TBLPROPERTIES ('transactional'='true') as select a, p from tab_acid where b > 1");

    driver.run("select * from mv_tab_acid ");
    res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals("Expecting 2 rows and found " + res.size(), 2, res.size());
    driver.run("drop materialized view mv_tab_acid");
  }
  
  @Test
  public void testRenamePartitionNonBlocking() throws Exception {
    testRenamePartition(false);
  }
  @Test
  public void testRenamePartitionBlocking() throws Exception {
    testRenamePartition(true);
  }
  
  private void testRenamePartition(boolean blocking) throws Exception {
    dropTable(new String[] {"tab_acid"});
    FileSystem fs = FileSystem.get(conf);

    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_LOCKS_PARTITION_THRESHOLD, 1);
    driver = Mockito.spy(driver);

    HiveConf.setBoolVar(driver2.getConf(), HiveConf.ConfVars.HIVE_ACID_RENAME_PARTITION_MAKE_COPY, !blocking);
    driver2 = Mockito.spy(driver2);

    driver.run("create table if not exists tab_acid (a int, b int) partitioned by (p string) " +
      "stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into tab_acid partition(p) (a,b,p) values(1,2,'foo'),(3,4,'bar')");

    driver.compileAndRespond("select * from tab_acid");
    List<String> res = new ArrayList<>();

    driver.lockAndRespond();
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());

    TestTxnDbUtil.checkLock(LockType.SHARED_READ,
      LockState.ACQUIRED, "default", "tab_acid", null, locks);

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver2.compileAndRespond("alter table tab_acid partition (p='foo') rename to partition (p='baz')");

    if (blocking) {
      txnMgr2.acquireLocks(driver2.getPlan(), ctx, null, false);
      locks = getLocks();

      ShowLocksResponseElement checkLock = TestTxnDbUtil.checkLock(LockType.EXCLUSIVE,
        LockState.WAITING, "default", "tab_acid", "p=foo", locks);

      swapTxnManager(txnMgr);
      Mockito.doNothing().when(driver).lockAndRespond();
      driver.run();

      driver.getFetchTask().fetch(res);
      swapTxnManager(txnMgr2);

      ReflectionUtil.setField(txnMgr2, "numStatements", 0);
      txnMgr2.getMS().unlock(checkLock.getLockid());
    }
    driver2.lockAndRespond();
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", blocking ? 1 : 2, locks.size());

    TestTxnDbUtil.checkLock(blocking ? LockType.EXCLUSIVE : LockType.EXCL_WRITE,
      LockState.ACQUIRED, "default", "tab_acid", "p=foo", locks);

    Mockito.doNothing().when(driver2).lockAndRespond();
    driver2.run();

    if (!blocking) {
      swapTxnManager(txnMgr);
      Mockito.doNothing().when(driver).lockAndRespond();
      driver.run();
    }
    Mockito.reset(driver, driver2);

    FileStatus[] stat = fs.listStatus(new Path(getWarehouseDir(), "tab_acid" + (blocking ? "" : "/p=foo")),
      (blocking ? path -> path.getName().equals("p=foo") : AcidUtils.baseFileFilter));
    if ((blocking ? 0 : 1) != stat.length) {
      Assert.fail("Partition data was " + (blocking ? "not " : "") + "removed from FS");
    }
    driver.getFetchTask().fetch(res);
    Assert.assertEquals("Expecting 2 rows and found " + res.size(), 2, res.size());

    driver.run("select * from tab_acid where p='foo'");
    res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals("Expecting 0 rows and found " + res.size(), 0, res.size());

    driver.run("select * from tab_acid");
    res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals("Expecting 2 rows and found " + res.size(), 2, res.size());

    //re-create partition with the same name
    driver.run("insert into tab_acid partition(p) (a,b,p) values(1,2,'foo')");

    driver.run("select * from tab_acid where p='foo'");
    res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals("Expecting 1 rows and found " + res.size(), 1, res.size());
  }
  
  @Test
  public void testDbConnectorNoLocks() throws Exception {
    driver.run("DROP CONNECTOR IF EXISTS derby_auth");

    driver.run("CREATE CONNECTOR IF NOT EXISTS derby_auth " +
      "TYPE 'derby' " +
      "URL 'jdbc:derby:./target/tmp/junit_metastore_db;create=true' " +
      "WITH DCPROPERTIES ( " +
      "   'hive.sql.dbcp.username'='APP', " +
      "   'hive.sql.dbcp.password'='mine')");
    
    driver.compileAndRespond("DROP CONNECTOR derby_auth");
    
    driver.lockAndRespond();
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 0, locks.size());
  }

  @Test
  public void testDropDatabaseNonBlocking() throws Exception {
    dropDatabaseNonBlocking(false, false);
  }
  
  @Test
  public void testDropDatabaseCascadeAllTablesWithSuffix() throws Exception {
    dropDatabaseNonBlocking(true, true);
  }
  
  @Test
  public void testDropDatabaseCascadeMixed() throws Exception {
    dropDatabaseNonBlocking(false, true);
  }
  
  private void dropDatabaseNonBlocking(boolean allTablesWithSuffix, boolean cascade) throws Exception {
    String database = "mydb";
    String tableName = "tab_acid";
    
    driver.run("drop database if exists " + database + " cascade");
    driver.run("create database " + database);

    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED, allTablesWithSuffix);
    // Create transactional table/materialized view with lockless-reads feature disabled
    driver.run("create table " + database + "." + tableName + "1 (a int, b int) " +
      "partitioned by (ds string) stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("create materialized view " + database + ".mv_" + tableName + "1 " +
        "partitioned on (ds) stored as orc TBLPROPERTIES ('transactional'='true')" +
      "as select a, ds from " + database + "." + tableName + "1 where b > 1");
    
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED, true);
    // Create transactional table/materialized view with lockless-reads feature enabled
    driver.run("create table " + database + "." + tableName + "2 (a int, b int) " +
      "partitioned by (ds string) stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("create materialized view " + database + ".mv_" + tableName + "2 " +
        "partitioned on (ds) stored as orc TBLPROPERTIES ('transactional'='true')" +
      "as select a, ds from " + database + "." + tableName + "2 where b > 1");
    
    if (!allTablesWithSuffix) {
      // Create external table
      driver.run("create external table " + database + ".tab_ext (a int, b int) " +
        "partitioned by (ds string) stored as parquet");
      // Create managed table
      driver.run("create table " + database + ".tab_nonacid (a int, b int) " +
        "partitioned by (ds string) stored as parquet");
    }
    // Drop database cascade
    driver.compileAndRespond("drop database " + database + (cascade ? " cascade" : ""));

    driver.lockAndRespond();
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", cascade && !allTablesWithSuffix ? 6 : 1, locks.size());

    if (cascade && !allTablesWithSuffix) {
      TestTxnDbUtil.checkLock(LockType.EXCLUSIVE,
        LockState.ACQUIRED, database, tableName + "1", null, locks);
      TestTxnDbUtil.checkLock(LockType.EXCLUSIVE,
        LockState.ACQUIRED, database, "mv_" + tableName + "1", null, locks);
      TestTxnDbUtil.checkLock(LockType.EXCL_WRITE,
        LockState.ACQUIRED, database, tableName + "2", null, locks);
      TestTxnDbUtil.checkLock(LockType.EXCL_WRITE,
        LockState.ACQUIRED, database, "mv_" + tableName + "2", null, locks);
      TestTxnDbUtil.checkLock(LockType.EXCLUSIVE,
        LockState.ACQUIRED, database, "tab_nonacid", null, locks);
    
    } else {
      TestTxnDbUtil.checkLock(LockType.EXCL_WRITE,
        LockState.ACQUIRED, database, null, null, locks);
    }
  }

  @Test
  public void testAddDropConstraintNonBlocking() throws Exception {
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED, true);
    dropTable(new String[] {"tab_acid"});

    driver.run("create table if not exists tab_acid (a int, b int) " +
      "stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into tab_acid (a,b) values(1,2),(3,4)");

    driver.compileAndRespond("alter table tab_acid ADD CONSTRAINT a_PK PRIMARY KEY (`a`) DISABLE NOVALIDATE");
    driver.lockAndRespond();
    
    List<ShowLocksResponseElement> locks = getLocks();
    TestTxnDbUtil.checkLock(LockType.EXCL_WRITE,
      LockState.ACQUIRED, "default", "tab_acid", null, locks);
    driver.close();
    
    driver.compileAndRespond("alter table tab_acid DROP CONSTRAINT a_PK");
    driver.lockAndRespond();

    locks = getLocks();
    TestTxnDbUtil.checkLock(LockType.EXCL_WRITE,
      LockState.ACQUIRED, "default", "tab_acid", null, locks);
  }

  @Test
  public void testAddPartitionIfNotExists() throws Exception {
    dropTable(new String[] {"T", "Tstage"});
    
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_LOCKS_PARTITION_THRESHOLD, 1);
    driver = Mockito.spy(driver);
    driver2 = Mockito.spy(driver2);
    
    driver.run("create table if not exists T (a int, b int) partitioned by (p string) " +
      "stored as orc TBLPROPERTIES ('transactional'='true')");
    //bucketed just so that we get 2 files
    driver.run("create table if not exists Tstage (a int, b int) clustered by (a) into 2 " +
      "buckets stored as orc TBLPROPERTIES ('transactional'='false')");
    driver.run("insert into Tstage values(1,2),(3,4)");
    String exportLoc = exportFolder.newFolder("1").toString();
    driver.run("export table Tstage to '" + exportLoc + "'");
    
    driver.compileAndRespond("select * from T");
    
    driver.lockAndRespond();
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());

    TestTxnDbUtil.checkLock(LockType.SHARED_READ,
      LockState.ACQUIRED, "default", "T", null, locks);

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver2.compileAndRespond("alter table T add if not exists partition (p='foo') location '" + exportLoc + "/data'");
    
    driver2.lockAndRespond();
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());

    TestTxnDbUtil.checkLock(LockType.EXCL_WRITE,
      LockState.ACQUIRED, "default", "T", null, locks);

    Mockito.doNothing().when(driver2).lockAndRespond();
    driver2.run();
    
    swapTxnManager(txnMgr);
    Mockito.doNothing().when(driver).lockAndRespond();
    driver.run();
    Mockito.reset(driver, driver2);

    List<String> res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals("Expecting 0 rows and found " + res.size(), 0, res.size());
    
    driver.run("select * from T where p='foo'");
    res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals("Expecting 2 rows and found " + res.size(), 2, res.size());
  }

  @Test
  public void testAddColumnsNonBlocking() throws Exception {
    testAddColumns(false);
  }
  @Test
  public void testAddColumnsBlocking() throws Exception {
    testAddColumns(true);
  }

  private void testAddColumns(boolean blocking) throws Exception {
    dropTable(new String[] {"tab_acid"});

    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED, !blocking);
    driver = Mockito.spy(driver);

    HiveConf.setBoolVar(driver2.getConf(), HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED, !blocking);
    driver2 = Mockito.spy(driver2);

    driver.run("create table if not exists tab_acid (a int, b int) " +
      "stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into tab_acid (a,b) values(1,2),(3,4)");

    driver.compileAndRespond("select * from tab_acid");
    List<String> res = new ArrayList<>();

    driver.lockAndRespond();
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", blocking ? 1 : 0, locks.size());

    if (blocking) {
      TestTxnDbUtil.checkLock(LockType.SHARED_READ,
        LockState.ACQUIRED, "default", "tab_acid", null, locks);
    }

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver2.compileAndRespond("alter table tab_acid add columns (c int)");

    if (blocking) {
      txnMgr2.acquireLocks(driver2.getPlan(), ctx, null, false);
      locks = getLocks();

      ShowLocksResponseElement checkLock = TestTxnDbUtil.checkLock(LockType.EXCLUSIVE,
        LockState.WAITING, "default", "tab_acid", null, locks);

      swapTxnManager(txnMgr);
      Mockito.doNothing().when(driver).lockAndRespond();
      driver.run();

      driver.getFetchTask().fetch(res);
      swapTxnManager(txnMgr2);

      ReflectionUtil.setField(txnMgr2, "numStatements", 0);
      txnMgr2.getMS().unlock(checkLock.getLockid());
    }
    driver2.lockAndRespond();
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());

    TestTxnDbUtil.checkLock(blocking ? LockType.EXCLUSIVE : LockType.EXCL_WRITE,
      LockState.ACQUIRED, "default", "tab_acid", null, locks);

    Mockito.doNothing().when(driver2).lockAndRespond();
    driver2.run();

    if (!blocking) {
      swapTxnManager(txnMgr);
      Mockito.doNothing().when(driver).lockAndRespond();
      driver.run();
    }
    Mockito.reset(driver, driver2);
    
    driver.getFetchTask().fetch(res);
    Assert.assertTrue(isEqualCollection(res, asList("1\t2", "3\t4")));
    driver.run("insert into tab_acid values(5,6,7)");
    
    driver.run("select * from tab_acid");
    res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    Assert.assertTrue(isEqualCollection(res, asList("1\t2\tNULL", "3\t4\tNULL", "5\t6\t7")));
  }

  @Test
  public void testReplaceColumnsNonBlocking() throws Exception {
    testReplaceColumns(false);
  }
  @Test
  public void testReplaceColumnsBlocking() throws Exception {
    testReplaceColumns(true);
  }
  private void testReplaceColumns(boolean blocking) throws Exception {
    testReplaceRenameColumns(blocking, "replace columns (c string, a bigint)");
  }
  
  @Test
  public void testRenameColumnsNonBlocking() throws Exception {
    testRenameColumns(false);
  }
  @Test
  public void testRenameColumnsBlocking() throws Exception {
    testRenameColumns(true);
  }
  private void testRenameColumns(boolean blocking) throws Exception {
    testReplaceRenameColumns(blocking, "change column a c string");
  }
  
  private void testReplaceRenameColumns(boolean blocking, String alterSubQuery) throws Exception {
    dropTable(new String[] {"tab_acid"});

    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED, !blocking);
    driver = Mockito.spy(driver);

    HiveConf.setBoolVar(driver2.getConf(), HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED, !blocking);
    driver2 = Mockito.spy(driver2);

    driver.run("create table if not exists tab_acid (a int, b int) " +
      "stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into tab_acid (a,b) values(1,2),(3,4)");

    driver.compileAndRespond("select * from tab_acid");
    List<String> res = new ArrayList<>();

    driver.lockAndRespond();
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", blocking ? 1 : 0, locks.size());

    if (blocking) {
      TestTxnDbUtil.checkLock(LockType.SHARED_READ,
        LockState.ACQUIRED, "default", "tab_acid", null, locks);
    }

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver2.compileAndRespond("alter table tab_acid "+ alterSubQuery);

    if (blocking) {
      txnMgr2.acquireLocks(driver2.getPlan(), ctx, null, false);
      locks = getLocks();

      ShowLocksResponseElement checkLock = TestTxnDbUtil.checkLock(LockType.EXCLUSIVE,
        LockState.WAITING, "default", "tab_acid", null, locks);

      swapTxnManager(txnMgr);
      Mockito.doNothing().when(driver).lockAndRespond();
      driver.run();

      driver.getFetchTask().fetch(res);
      swapTxnManager(txnMgr2);

      ReflectionUtil.setField(txnMgr2, "numStatements", 0);
      txnMgr2.getMS().unlock(checkLock.getLockid());
    }
    driver2.lockAndRespond();
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());

    TestTxnDbUtil.checkLock(blocking ? LockType.EXCLUSIVE : LockType.EXCL_WRITE,
      LockState.ACQUIRED, "default", "tab_acid", null, locks);

    Mockito.doNothing().when(driver2).lockAndRespond();
    driver2.run();

    if (!blocking) {
      swapTxnManager(txnMgr);
      Mockito.doNothing().when(driver).lockAndRespond();
      driver.run();
    }
    Mockito.reset(driver, driver2);

    driver.getFetchTask().fetch(res);
    Assert.assertTrue(isEqualCollection(res, asList("1\t2", "3\t4")));
  }

  @Test
  public void testAlterTableClusteredBy() throws Exception {
    dropTable(new String[] {"tab_acid"});
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED, true);

    driver.run("create table if not exists tab_acid (a int, b int) " +
      "stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into tab_acid (a,b) values(1,2),(3,4)");

    driver.compileAndRespond("alter table tab_acid CLUSTERED BY(a) SORTED BY(b) INTO 32 BUCKETS", true);
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fiddler");
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.EXCL_WRITE, LockState.ACQUIRED, "default", "tab_acid", null, locks);

    //simulate concurrent session
    HiveTxnManager txnMgr2 = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver.compileAndRespond("insert into tab_acid (a,b) values(1,2),(3,4)", true);
    ((DbTxnManager)txnMgr2).acquireLocks(driver.getPlan(), ctx, "Fiddler", false);
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.EXCL_WRITE, LockState.ACQUIRED, "default", "tab_acid", null, locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.WAITING, "default", "tab_acid", null, locks);

    txnMgr2.rollbackTxn();
    txnMgr.commitTxn();
  }

  @Test
  public void testMsckRepair() throws Exception {
    dropTable(new String[] { "tab_acid", "tab_acid_msck"});

    driver.run("create table tab_acid (a int, b int) partitioned by (p string) " +
      "stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into tab_acid partition(p) values (1,1,'p1'),(2,2,'p1'),(3,3,'p1')");
    driver.run("insert into tab_acid partition(p) values (1,2,'p1'),(2,3,'p1'),(3,4,'p1')");

    // Create target table
    driver.run("create table tab_acid_msck (a int, b int) partitioned by (p string) " +
      " stored as orc TBLPROPERTIES ('transactional'='true')");

    // copy files on fs
    FileSystem fs = FileSystem.get(conf);
    FileUtil.copy(fs, new Path(getWarehouseDir() + "/tab_acid/p=p1"), fs,
      new Path(getWarehouseDir(), "tab_acid_msck"), false, conf);

    FileStatus[] fileStatuses = fs.listStatus(new Path(getWarehouseDir(), "tab_acid_msck/p=p1"));
    Assert.assertEquals(2, fileStatuses.length);

    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED, true);
    // call msck repair
    driver.compileAndRespond("msck repair table tab_acid_msck");
    txnMgr.acquireLocks(driver.getPlan(), ctx, "Fiddler");
    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.EXCL_WRITE, LockState.ACQUIRED, "default", "tab_acid_msck", null, locks);

    //simulate concurrent session
    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);
    driver.compileAndRespond("insert into tab_acid_msck partition(p) values (1,3,'p1'),(2,4,'p1'),(3,5,'p1')", true);
    txnMgr2.acquireLocks(driver.getPlan(), ctx, "Fiddler", false);
    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 2, locks.size());
    TestTxnDbUtil.checkLock(LockType.EXCL_WRITE, LockState.ACQUIRED, "default", "tab_acid_msck", null, locks);
    TestTxnDbUtil.checkLock(LockType.SHARED_WRITE, LockState.WAITING, "default", "tab_acid_msck", null, locks);

    txnMgr2.rollbackTxn();
    txnMgr.commitTxn();
  }

  @Test
  public void testAlterTableSetPropertiesNonBlocking() throws Exception {
    dropTable(new String[]{"tab_acid"});
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED, true);

    driver.run("create table tab_acid (a int, b int) partitioned by (p string) " +
      "stored as orc TBLPROPERTIES ('transactional'='true')");

    driver.compileAndRespond("alter table tab_acid set tblproperties ('DO_NOT_UPDATE_STATS'='true')");
    driver.lockAndRespond();

    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.SHARED_READ,
      LockState.ACQUIRED, "default", "tab_acid", null, locks);
    driver.close();

    driver.compileAndRespond("alter table tab_acid unset tblproperties ('transactional')");
    driver.lockAndRespond();

    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.EXCL_WRITE,
      LockState.ACQUIRED, "default", "tab_acid", null, locks);
  }

  @Test
  public void testSetSerdeAndFileFormatNonBlocking() throws Exception {
    dropTable(new String[] {"tab_acid"});

    driver.run("create table if not exists tab_acid (a int, b int) partitioned by (p string) " +
      "TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')");
    driver.run("insert into tab_acid partition(p) (a,b,p) values(1,2,'foo'),(3,4,'bar')");

    driver.compileAndRespond("select * from tab_acid");

    HiveConf.setBoolVar(driver2.getConf(), HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED, true);
    driver2 = Mockito.spy(driver2);

    DbTxnManager txnMgr2 = (DbTxnManager) TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(txnMgr2);

    driver2.compileAndRespond("alter table tab_acid set serde 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'");
    driver2.lockAndRespond();

    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.EXCL_WRITE,
      LockState.ACQUIRED, "default", "tab_acid", null, locks);

    Mockito.doNothing().when(driver2).lockAndRespond();
    driver2.run();
    Mockito.reset(driver2);

    driver2.compileAndRespond("alter table tab_acid set fileformat rcfile");
    driver2.lockAndRespond();

    locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.EXCL_WRITE,
      LockState.ACQUIRED, "default", "tab_acid", null, locks);

    Mockito.doNothing().when(driver2).lockAndRespond();
    driver2.run();
    Mockito.reset(driver2);

    driver2.run("insert into tab_acid partition(p) (a,b,p) values(1,2,'foo')");

    swapTxnManager(txnMgr);
    driver.run();

    List<String> res = new ArrayList<>();
    driver.getFetchTask().fetch(res);
    Assert.assertEquals("No records found", 2, res.size());
  }

  @Test
  public void testMaterializedViewRebuildNoLocks() throws Exception {
    driver.run("drop materialized view if exists mv_tab_acid");
    dropTable(new String[]{"tab_acid"});

    driver.run("create table if not exists tab_acid (a int, b int) partitioned by (p string) " +
      "stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into tab_acid partition(p) (a,b,p) values(1,2,'foo'),(3,4,'bar')");

    driver.run("create materialized view mv_tab_acid partitioned on (p) " +
      "stored as orc TBLPROPERTIES ('transactional'='true') as select a, p from tab_acid where b > 1");

    driver.compileAndRespond("alter materialized view mv_tab_acid rebuild");
    driver.lockAndRespond();
    List<ShowLocksResponseElement> locks = getLocks();
    // FIXME: two rebuilds should not run in parallel
    Assert.assertEquals("Unexpected lock count", 0, locks.size());
    // cleanup
    txnMgr.rollbackTxn();
    driver.run("drop materialized view mv_tab_acid");
  }

  @Test
  public void testMaterializedViewEnableRewriteNonBlocking() throws Exception {
    driver.run("drop materialized view if exists mv_tab_acid");
    dropTable(new String[]{"tab_acid"});
    
    driver.run("create table if not exists tab_acid (a int, b int) partitioned by (p string) " +
      "stored as orc TBLPROPERTIES ('transactional'='true')");
    driver.run("insert into tab_acid partition(p) (a,b,p) values(1,2,'foo'),(3,4,'bar')");

    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED, true);
    driver.run("create materialized view mv_tab_acid partitioned on (p) " +
      "stored as orc TBLPROPERTIES ('transactional'='true') as select a, p from tab_acid where b > 1");

    driver.compileAndRespond("alter materialized view mv_tab_acid enable rewrite");
    driver.lockAndRespond();

    List<ShowLocksResponseElement> locks = getLocks();
    Assert.assertEquals("Unexpected lock count", 1, locks.size());
    TestTxnDbUtil.checkLock(LockType.EXCL_WRITE,
      LockState.ACQUIRED, "default", "mv_tab_acid", null, locks);
    // cleanup
    txnMgr.rollbackTxn();
    driver.run("drop materialized view mv_tab_acid");
  }
}
