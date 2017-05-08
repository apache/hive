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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
import org.apache.hadoop.hive.metastore.api.TxnState;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.DummyPartition;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.txn.AcidHouseKeeperService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;

/**
 * Unit tests for {@link DbTxnManager}.
 * See additional tests in {@link org.apache.hadoop.hive.ql.lockmgr.TestDbTxnManager2}
 */
public class TestDbTxnManager {
  private static final int TEST_TIMED_OUT_TXN_ABORT_BATCH_SIZE = 1000;
  private final HiveConf conf = new HiveConf();
  private HiveTxnManager txnMgr;
  private AcidHouseKeeperService houseKeeperService = null;
  private final Context ctx;
  private int nextInput;
  HashSet<ReadEntity> readEntities;
  HashSet<WriteEntity> writeEntities;

  public TestDbTxnManager() throws Exception {
    conf
    .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    TxnDbUtil.setConfValues(conf);
    SessionState.start(conf);
    ctx = new Context(conf);
    tearDown();
  }

  @Test
  public void testSingleReadTable() throws Exception {
    addTableInput();
    QueryPlan qp = new MockQueryPlan(this);
    txnMgr.acquireLocks(qp, ctx, "fred");
    List<HiveLock> locks = ctx.getHiveLocks();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(1,
        TxnDbUtil.countLockComponents(((DbLockManager.DbHiveLock) locks.get(0)).lockId));
    txnMgr.getLockManager().unlock(locks.get(0));
    locks = txnMgr.getLockManager().getLocks(false, false);
    Assert.assertEquals(0, locks.size());
  }

  @Test
  public void testSingleReadPartition() throws Exception {
    addPartitionInput(newTable(true));
    QueryPlan qp = new MockQueryPlan(this);
    txnMgr.acquireLocks(qp, ctx, null);
    List<HiveLock> locks = ctx.getHiveLocks();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(1,
        TxnDbUtil.countLockComponents(((DbLockManager.DbHiveLock) locks.get(0)).lockId));
    txnMgr.getLockManager().unlock(locks.get(0));
    locks = txnMgr.getLockManager().getLocks(false, false);
    Assert.assertEquals(0, locks.size());

  }

  @Test
  public void testSingleReadMultiPartition() throws Exception {
    Table t = newTable(true);
    addPartitionInput(t);
    addPartitionInput(t);
    addPartitionInput(t);
    QueryPlan qp = new MockQueryPlan(this);
    txnMgr.acquireLocks(qp, ctx, "fred");
    List<HiveLock> locks = ctx.getHiveLocks();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(3,
        TxnDbUtil.countLockComponents(((DbLockManager.DbHiveLock) locks.get(0)).lockId));
    txnMgr.getLockManager().unlock(locks.get(0));
    locks = txnMgr.getLockManager().getLocks(false, false);
    Assert.assertEquals(0, locks.size());
  }

  @Test
  public void testJoin() throws Exception {
    Table t = newTable(true);
    addPartitionInput(t);
    addPartitionInput(t);
    addPartitionInput(t);
    addTableInput();
    QueryPlan qp = new MockQueryPlan(this);
    txnMgr.acquireLocks(qp, ctx, "fred");
    List<HiveLock> locks = ctx.getHiveLocks();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(4,
        TxnDbUtil.countLockComponents(((DbLockManager.DbHiveLock) locks.get(0)).lockId));
    txnMgr.getLockManager().unlock(locks.get(0));
    locks = txnMgr.getLockManager().getLocks(false, false);
    Assert.assertEquals(0, locks.size());
  }

  @Test
  public void testSingleWriteTable() throws Exception {
    WriteEntity we = addTableOutput(WriteEntity.WriteType.INSERT);
    QueryPlan qp = new MockQueryPlan(this);
    txnMgr.openTxn(ctx, "fred");
    txnMgr.acquireLocks(qp, ctx, "fred");
    List<HiveLock> locks = ctx.getHiveLocks();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(1,
        TxnDbUtil.countLockComponents(((DbLockManager.DbHiveLock) locks.get(0)).lockId));
    txnMgr.commitTxn();
    locks = txnMgr.getLockManager().getLocks(false, false);
    Assert.assertEquals(0, locks.size());
  }


  @Test
  public void testSingleWritePartition() throws Exception {
    WriteEntity we = addPartitionOutput(newTable(true), WriteEntity.WriteType.INSERT);
    QueryPlan qp = new MockQueryPlan(this);
    txnMgr.openTxn(ctx, "fred");
    txnMgr.acquireLocks(qp, ctx, "fred");
    List<HiveLock> locks = ctx.getHiveLocks();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(1,
        TxnDbUtil.countLockComponents(((DbLockManager.DbHiveLock) locks.get(0)).lockId));
    txnMgr.commitTxn();
    locks = txnMgr.getLockManager().getLocks(false, false);
    Assert.assertEquals(0, locks.size());
  }

  @Test
  public void testWriteDynamicPartition() throws Exception {
    WriteEntity we = addDynamicPartitionedOutput(newTable(true), WriteEntity.WriteType.INSERT);
    QueryPlan qp = new MockQueryPlan(this);
    txnMgr.openTxn(ctx, "fred");
    txnMgr.acquireLocks(qp, ctx, "fred");
    List<HiveLock> locks = ctx.getHiveLocks();
    Assert.assertEquals(1, locks.size());
    /*Assert.assertEquals(1,
        TxnDbUtil.countLockComponents(((DbLockManager.DbHiveLock) locks.get(0)).lockId));
    */// Make sure we're locking the whole table, since this is dynamic partitioning
    ShowLocksResponse rsp = ((DbLockManager)txnMgr.getLockManager()).getLocks();
    List<ShowLocksResponseElement> elms = rsp.getLocks();
    Assert.assertEquals(1, elms.size());
    Assert.assertNotNull(elms.get(0).getTablename());
    Assert.assertNull(elms.get(0).getPartname());
    txnMgr.commitTxn();
    locks = txnMgr.getLockManager().getLocks(false, false);
    Assert.assertEquals(0, locks.size());
  }

  /**
   * aborts timed out transactions
   */
  private void runReaper() throws Exception {
    int lastCount = houseKeeperService.getIsAliveCounter();
    houseKeeperService.start(conf);
    int maxIter = 10;
    int iterCount = 0;
    while(houseKeeperService.getIsAliveCounter() <= lastCount) {
      if(iterCount++ >= maxIter) {
        //prevent test hangs
        throw new IllegalStateException("Reaper didn't run after " + iterCount + " waits");
      }
      try {
        Thread.sleep(100);//make sure it has run at least once
      }
      catch(InterruptedException ex) {
        //...
      }
    }
    houseKeeperService.stop();
  }
  @Test
  public void testExceptions() throws Exception {
    addPartitionOutput(newTable(true), WriteEntity.WriteType.INSERT);
    QueryPlan qp = new MockQueryPlan(this);
    ((DbTxnManager) txnMgr).openTxn(ctx, "NicholasII", HiveConf.getTimeVar(conf, HiveConf.ConfVars.HIVE_TXN_TIMEOUT, TimeUnit.MILLISECONDS) * 2);
    Thread.sleep(HiveConf.getTimeVar(conf, HiveConf.ConfVars.HIVE_TXN_TIMEOUT, TimeUnit.MILLISECONDS));
    runReaper();
    LockException exception = null;
    try {
      txnMgr.commitTxn();
    }
    catch(LockException ex) {
      exception = ex;
    }
    Assert.assertNotNull("Expected exception1", exception);
    Assert.assertEquals("Wrong Exception1", ErrorMsg.TXN_ABORTED, exception.getCanonicalErrorMsg());

    exception = null;
    ((DbTxnManager) txnMgr).openTxn(ctx, "AlexanderIII", HiveConf.getTimeVar(conf, HiveConf.ConfVars.HIVE_TXN_TIMEOUT, TimeUnit.MILLISECONDS) * 2);
    Thread.sleep(HiveConf.getTimeVar(conf, HiveConf.ConfVars.HIVE_TXN_TIMEOUT, TimeUnit.MILLISECONDS));
    runReaper();//this will abort the txn
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    GetOpenTxnsInfoResponse txnsInfo = txnHandler.getOpenTxnsInfo();
    assertEquals(2, txnsInfo.getTxn_high_water_mark());
    assertEquals(2, txnsInfo.getOpen_txns().size());
    Assert.assertEquals(TxnState.ABORTED, txnsInfo.getOpen_txns().get(1).getState());
    txnMgr.rollbackTxn();//this is idempotent
  }

  @Test
  public void testLockTimeout() throws Exception {
    addPartitionInput(newTable(true));
    QueryPlan qp = new MockQueryPlan(this);
    //make sure it works with nothing to expire
    testLockExpiration(txnMgr, 0, true);

    //create a few read locks, all on the same resource
    for(int i = 0; i < 5; i++) {
      ((DbTxnManager)txnMgr).acquireLocks(qp, ctx, "PeterI" + i, true); // No heartbeat
    }
    testLockExpiration(txnMgr, 5, true);

    //create a lot of locks
    for(int i = 0; i < TEST_TIMED_OUT_TXN_ABORT_BATCH_SIZE + 17; i++) {
      ((DbTxnManager)txnMgr).acquireLocks(qp, ctx, "PeterI" + i, true); // No heartbeat
    }
    testLockExpiration(txnMgr, TEST_TIMED_OUT_TXN_ABORT_BATCH_SIZE + 17, true);

    // Create a lock, but send the heartbeat with a long delay. The lock will get expired.
    ((DbTxnManager)txnMgr).acquireLocksWithHeartbeatDelay(qp, ctx, "bob",
        HiveConf.getTimeVar(conf, HiveConf.ConfVars.HIVE_TXN_TIMEOUT, TimeUnit.MILLISECONDS) * 10);
    testLockExpiration(txnMgr, 1, true);

    // Create a lock and trigger a heartbeat. With heartbeat, the lock won't expire.
    txnMgr.acquireLocks(qp, ctx, "peter");
    testLockExpiration(txnMgr, 1, false);
  }

  private void testLockExpiration(HiveTxnManager txnMgr, int numLocksBefore, boolean shouldExpire) throws Exception {
    DbLockManager lockManager = (DbLockManager)txnMgr.getLockManager();
    ShowLocksResponse resp = lockManager.getLocks();
    Assert.assertEquals("Wrong number of locks before expire", numLocksBefore, resp.getLocks().size());
    Thread.sleep(HiveConf.getTimeVar(conf, HiveConf.ConfVars.HIVE_TXN_TIMEOUT, TimeUnit.MILLISECONDS));
    runReaper();
    resp = lockManager.getLocks();
    if (shouldExpire) {
      Assert.assertEquals("Expected all locks to expire", 0, resp.getLocks().size());
      lockManager.clearLocalLockRecords();
    } else {
      Assert.assertEquals("No lock should expire because there is heartbeating", numLocksBefore, resp.getLocks().size());
    }
  }

  @Test
  public void testReadWrite() throws Exception {
    Table t = newTable(true);
    addPartitionInput(t);
    addPartitionInput(t);
    addPartitionInput(t);
    WriteEntity we = addTableOutput(WriteEntity.WriteType.INSERT);
    QueryPlan qp = new MockQueryPlan(this);
    txnMgr.openTxn(ctx, "fred");
    txnMgr.acquireLocks(qp, ctx, "fred");
    List<HiveLock> locks = ctx.getHiveLocks();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(4,
        TxnDbUtil.countLockComponents(((DbLockManager.DbHiveLock) locks.get(0)).lockId));
    txnMgr.commitTxn();
    locks = txnMgr.getLockManager().getLocks(false, false);
    Assert.assertEquals(0, locks.size());
  }

  @Test
  public void testUpdate() throws Exception {
    WriteEntity we = addTableOutput(WriteEntity.WriteType.UPDATE);
    QueryPlan qp = new MockQueryPlan(this);
    txnMgr.openTxn(ctx, "fred");
    txnMgr.acquireLocks(qp, ctx, "fred");
    List<HiveLock> locks = ctx.getHiveLocks();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(1,
        TxnDbUtil.countLockComponents(((DbLockManager.DbHiveLock) locks.get(0)).lockId));
    txnMgr.commitTxn();
    locks = txnMgr.getLockManager().getLocks(false, false);
    Assert.assertEquals(0, locks.size());
  }

  @Test
  public void testDelete() throws Exception {
    WriteEntity we = addTableOutput(WriteEntity.WriteType.DELETE);
    QueryPlan qp = new MockQueryPlan(this);
    txnMgr.openTxn(ctx, "fred");
    txnMgr.acquireLocks(qp, ctx, "fred");
    List<HiveLock> locks = ctx.getHiveLocks();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(1,
        TxnDbUtil.countLockComponents(((DbLockManager.DbHiveLock) locks.get(0)).lockId));
    txnMgr.commitTxn();
    locks = txnMgr.getLockManager().getLocks(false, false);
    Assert.assertEquals(0, locks.size());
  }

  @Test
  public void testRollback() throws Exception {
    WriteEntity we = addTableOutput(WriteEntity.WriteType.DELETE);
    QueryPlan qp = new MockQueryPlan(this);
    txnMgr.openTxn(ctx, "fred");
    txnMgr.acquireLocks(qp, ctx, "fred");
    List<HiveLock> locks = ctx.getHiveLocks();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(1,
        TxnDbUtil.countLockComponents(((DbLockManager.DbHiveLock) locks.get(0)).lockId));
    txnMgr.rollbackTxn();
    locks = txnMgr.getLockManager().getLocks(false, false);
    Assert.assertEquals(0, locks.size());
  }

  @Test
  public void testDDLExclusive() throws Exception {
    WriteEntity we = addTableOutput(WriteEntity.WriteType.DDL_EXCLUSIVE);
    QueryPlan qp = new MockQueryPlan(this);
    txnMgr.acquireLocks(qp, ctx, "fred");
    List<HiveLock> locks = ctx.getHiveLocks();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(1,
        TxnDbUtil.countLockComponents(((DbLockManager.DbHiveLock) locks.get(0)).lockId));
    txnMgr.getLockManager().unlock(locks.get(0));
    locks = txnMgr.getLockManager().getLocks(false, false);
    Assert.assertEquals(0, locks.size());
  }

  @Test
  public void testDDLShared() throws Exception {
    WriteEntity we = addTableOutput(WriteEntity.WriteType.DDL_SHARED);
    QueryPlan qp = new MockQueryPlan(this);
    txnMgr.acquireLocks(qp, ctx, "fred");
    List<HiveLock> locks = ctx.getHiveLocks();
    Assert.assertEquals(1, locks.size());
    Assert.assertEquals(1,
        TxnDbUtil.countLockComponents(((DbLockManager.DbHiveLock) locks.get(0)).lockId));
    txnMgr.getLockManager().unlock(locks.get(0));
    locks = txnMgr.getLockManager().getLocks(false, false);
    Assert.assertEquals(0, locks.size());
  }

  @Test
  public void testDDLNoLock() throws Exception {
    WriteEntity we = addTableOutput(WriteEntity.WriteType.DDL_NO_LOCK);
    QueryPlan qp = new MockQueryPlan(this);
    txnMgr.acquireLocks(qp, ctx, "fred");
    List<HiveLock> locks = ctx.getHiveLocks();
    Assert.assertNull(locks);
  }

  @Test
  public void concurrencyFalse() throws Exception {
    HiveConf badConf = new HiveConf();
    badConf.setVar(HiveConf.ConfVars.HIVE_TXN_MANAGER,
        "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
    badConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    boolean sawException = false;
    try {
      TxnManagerFactory.getTxnManagerFactory().getTxnManager(badConf);
    } catch (RuntimeException e) {
      sawException = true;
    }
    Assert.assertTrue(sawException);
  }

  @Test
  public void testLockAcquisitionAndRelease() throws Exception {
    addTableInput();
    QueryPlan qp = new MockQueryPlan(this);
    txnMgr.acquireLocks(qp, ctx, "fred");
    List<HiveLock> locks = ctx.getHiveLocks();
    Assert.assertEquals(1, locks.size());
    txnMgr.releaseLocks(locks);
    locks = txnMgr.getLockManager().getLocks(false, false);
    Assert.assertEquals(0, locks.size());
  }

  @Test
  public void testHeartbeater() throws Exception {
    Assert.assertTrue(txnMgr instanceof DbTxnManager);

    addTableInput();
    LockException exception = null;
    QueryPlan qp = new MockQueryPlan(this);

    // Case 1: If there's no delay for the heartbeat, txn should be able to commit
    txnMgr.openTxn(ctx, "fred");
    txnMgr.acquireLocks(qp, ctx, "fred"); // heartbeat started..
    runReaper();
    try {
      txnMgr.commitTxn();
    } catch (LockException e) {
      exception = e;
    }
    Assert.assertNull("Txn commit should be successful", exception);
    exception = null;

    // Case 2: If there's delay for the heartbeat, but the delay is within the reaper's tolerance,
    //         then txt should be able to commit
    // Start the heartbeat after a delay, which is shorter than  the HIVE_TXN_TIMEOUT
    ((DbTxnManager) txnMgr).openTxn(ctx, "tom",
        HiveConf.getTimeVar(conf, HiveConf.ConfVars.HIVE_TXN_TIMEOUT, TimeUnit.MILLISECONDS) / 2);
    txnMgr.acquireLocks(qp, ctx, "tom");
    runReaper();
    try {
      txnMgr.commitTxn();
    } catch (LockException e) {
      exception = e;
    }
    Assert.assertNull("Txn commit should also be successful", exception);
    exception = null;

    // Case 3: If there's delay for the heartbeat, and the delay is long enough to trigger the reaper,
    //         then the txn will time out and be aborted.
    //         Here we just don't send the heartbeat at all - an infinite delay.
    // Start the heartbeat after a delay, which exceeds the HIVE_TXN_TIMEOUT
    ((DbTxnManager) txnMgr).openTxn(ctx, "jerry", HiveConf.getTimeVar(conf, HiveConf.ConfVars.HIVE_TXN_TIMEOUT, TimeUnit.MILLISECONDS) * 2);
    txnMgr.acquireLocks(qp, ctx, "jerry");
    Thread.sleep(HiveConf.getTimeVar(conf, HiveConf.ConfVars.HIVE_TXN_TIMEOUT, TimeUnit.MILLISECONDS));
    runReaper();
    try {
      txnMgr.commitTxn();
    } catch (LockException e) {
      exception = e;
    }
    Assert.assertNotNull("Txn should have been aborted", exception);
    Assert.assertEquals(ErrorMsg.TXN_ABORTED, exception.getCanonicalErrorMsg());
  }

  @Before
  public void setUp() throws Exception {
    TxnDbUtil.prepDb();
    txnMgr = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    txnMgr.getLockManager();//init lock manager
    Assert.assertTrue(txnMgr instanceof DbTxnManager);
    nextInput = 1;
    readEntities = new HashSet<ReadEntity>();
    writeEntities = new HashSet<WriteEntity>();
    conf.setTimeVar(HiveConf.ConfVars.HIVE_TIMEDOUT_TXN_REAPER_START, 0, TimeUnit.SECONDS);
    conf.setTimeVar(HiveConf.ConfVars.HIVE_TXN_TIMEOUT, 10, TimeUnit.SECONDS);
    houseKeeperService = new AcidHouseKeeperService();
  }

  @After
  public void tearDown() throws Exception {
    if(houseKeeperService != null) houseKeeperService.stop();
    if (txnMgr != null) txnMgr.closeTxnManager();
    TxnDbUtil.cleanDb();
  }

  private static class MockQueryPlan extends QueryPlan {
    private final HashSet<ReadEntity> inputs = new HashSet<>();
    private final HashSet<WriteEntity> outputs = new HashSet<>();
    private final String queryId;
    
    MockQueryPlan(TestDbTxnManager test) {
      inputs.addAll(test.readEntities);
      outputs.addAll(test.writeEntities);
      queryId = makeQueryId();
    }

    @Override
    public HashSet<ReadEntity> getInputs() {
      return inputs;
    }

    @Override
    public HashSet<WriteEntity> getOutputs() {
      return outputs;
    }
    @Override
    public String getQueryId() {
      return queryId;
    }
  }

  private Table newTable(boolean isPartitioned) {
    Table t = new Table("default", "table" + Integer.toString(nextInput++));
    if (isPartitioned) {
      FieldSchema fs = new FieldSchema();
      fs.setName("version");
      fs.setType("String");
      List<FieldSchema> partCols = new ArrayList<FieldSchema>(1);
      partCols.add(fs);
      t.setPartCols(partCols);
    }
    Map<String, String> tblProps = t.getParameters();
    if(tblProps == null) {
      tblProps = new HashMap<>();
    }
    tblProps.put(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, "true");
    t.setParameters(tblProps);
    return t;
  }

  private void addTableInput() {
    ReadEntity re = new ReadEntity(newTable(false));
    readEntities.add(re);
  }

  private void addPartitionInput(Table t) throws Exception {
    Map<String, String> partSpec = new HashMap<String, String>();
    partSpec.put("version", Integer.toString(nextInput++));
    Partition p = new Partition(t, partSpec, new Path("/dev/null"));
    ReadEntity re = new ReadEntity(p);
    readEntities.add(re);
  }

  private WriteEntity addTableOutput(WriteEntity.WriteType writeType) {
    WriteEntity we = new WriteEntity(newTable(false), writeType);
    writeEntities.add(we);
    return we;
  }

  private WriteEntity addPartitionOutput(Table t, WriteEntity.WriteType writeType)
      throws Exception {
    Map<String, String> partSpec = new HashMap<String, String>();
    partSpec.put("version", Integer.toString(nextInput++));
    Partition p = new Partition(t, partSpec, new Path("/dev/null"));
    WriteEntity we = new WriteEntity(p, writeType);
    writeEntities.add(we);
    return we;
  }

  private WriteEntity addDynamicPartitionedOutput(Table t, WriteEntity.WriteType writeType)
      throws Exception {
    DummyPartition dp = new DummyPartition(t, "no clue what I should call this");
    WriteEntity we = new WriteEntity(dp, writeType, false);
    writeEntities.add(we);
    return we;
  }
}
