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
import org.apache.hadoop.hive.ql.metadata.Hive;
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
 * Tests here 
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
    conf.setBoolVar(HiveConf.ConfVars.HIVEENFORCEBUCKETING, true);
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
    ShowLocksResponse rsp = ((DbLockManager)txnMgr.getLockManager()).getLocks();
    return rsp.getLocks();
  }
}
