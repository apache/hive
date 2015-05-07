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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.DummyPartition;import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

/**
 * Unit tests for {@link DbTxnManager}.
 * See additional tests in {@link org.apache.hadoop.hive.ql.lockmgr.TestDbTxnManager}
 */
public class TestDbTxnManager {

  private HiveConf conf = new HiveConf();
  private HiveTxnManager txnMgr;
  private Context ctx;
  private int nextInput;
  private int nextOutput;
  HashSet<ReadEntity> readEntities;
  HashSet<WriteEntity> writeEntities;

  public TestDbTxnManager() throws Exception {
    TxnDbUtil.setConfValues(conf);
    SessionState.start(conf);
    ctx = new Context(conf);
    LogManager.getRootLogger().setLevel(Level.DEBUG);
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
    txnMgr.openTxn("fred");
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
    txnMgr.openTxn("fred");
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
    txnMgr.openTxn("fred");
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

  @Test
  public void testReadWrite() throws Exception {
    Table t = newTable(true);
    addPartitionInput(t);
    addPartitionInput(t);
    addPartitionInput(t);
    WriteEntity we = addTableOutput(WriteEntity.WriteType.INSERT);
    QueryPlan qp = new MockQueryPlan(this);
    txnMgr.openTxn("fred");
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
    txnMgr.openTxn("fred");
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
    txnMgr.openTxn("fred");
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
    txnMgr.openTxn("fred");
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


  @Before
  public void setUp() throws Exception {
    TxnDbUtil.prepDb();
    txnMgr = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    Assert.assertTrue(txnMgr instanceof DbTxnManager);
    nextInput = 1;
    nextOutput = 1;
    readEntities = new HashSet<ReadEntity>();
    writeEntities = new HashSet<WriteEntity>();
  }

  @After
  public void tearDown() throws Exception {
    if (txnMgr != null) txnMgr.closeTxnManager();
    TxnDbUtil.cleanDb();
  }

  private static class MockQueryPlan extends QueryPlan {
    private HashSet<ReadEntity> inputs;
    private HashSet<WriteEntity> outputs;

    MockQueryPlan(TestDbTxnManager test) {
      HashSet<ReadEntity> r = test.readEntities;
      HashSet<WriteEntity> w = test.writeEntities;
      inputs = (r == null) ? new HashSet<ReadEntity>() : r;
      outputs = (w == null) ? new HashSet<WriteEntity>() : w;
    }

    @Override
    public HashSet<ReadEntity> getInputs() {
      return inputs;
    }

    @Override
    public HashSet<WriteEntity> getOutputs() {
      return outputs;
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
