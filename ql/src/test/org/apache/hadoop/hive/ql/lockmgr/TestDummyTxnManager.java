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

import static org.mockito.Mockito.*;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.Driver.LockedDriverState;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject.HiveLockObjectData;
import org.apache.hadoop.hive.ql.lockmgr.zookeeper.ZooKeeperHiveLock;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class TestDummyTxnManager {
  private final HiveConf conf = new HiveConf();
  private HiveTxnManager txnMgr;
  private Context ctx;
  private int nextInput = 1;

  @Mock
  HiveLockManager mockLockManager;

  @Mock
  HiveLockManagerCtx mockLockManagerCtx;

  @Mock
  QueryPlan mockQueryPlan;

  @Before
  public void setUp() throws Exception {
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
    conf.setVar(HiveConf.ConfVars.HIVE_TXN_MANAGER, DummyTxnManager.class.getName());
    conf
    .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    SessionState.start(conf);
    ctx = new Context(conf);

    txnMgr = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    Assert.assertTrue(txnMgr instanceof DummyTxnManager);

    // Use reflection to set LockManager since creating the object using the
    // relection in DummyTxnManager won't take Mocked object
    Field field = DummyTxnManager.class.getDeclaredField("lockMgr");
    field.setAccessible(true);
    field.set(txnMgr, mockLockManager);

    Field field2 = DummyTxnManager.class.getDeclaredField("lockManagerCtx");
    field2.setAccessible(true);
    field2.set(txnMgr, mockLockManagerCtx);

  }

  @After
  public void tearDown() throws Exception {
    if (txnMgr != null) {
      txnMgr.closeTxnManager();
    }
  }

  /**
   * Verifies the current database object is not locked if the table read is against different database
   * @throws Exception
   */
  @Test
  public void testSingleReadTable() throws Exception {
    // Setup
    SessionState.get().setCurrentDatabase("db1");

    List<HiveLock> expectedLocks = new ArrayList<HiveLock>();
    expectedLocks.add(new ZooKeeperHiveLock("default", new HiveLockObject(), HiveLockMode.SHARED));
    expectedLocks.add(new ZooKeeperHiveLock("default.table1", new HiveLockObject(), HiveLockMode.SHARED));
    LockedDriverState lDrvState = new LockedDriverState();
    LockedDriverState lDrvInp = new LockedDriverState();
    lDrvInp.abort();
    LockException lEx = new LockException(ErrorMsg.LOCK_ACQUIRE_CANCELLED.getMsg());
    when(mockLockManager.lock(anyListOf(HiveLockObj.class), eq(false), eq(lDrvState))).thenReturn(expectedLocks);
    when(mockLockManager.lock(anyListOf(HiveLockObj.class), eq(false), eq(lDrvInp))).thenThrow(lEx);
    doNothing().when(mockLockManager).setContext(any(HiveLockManagerCtx.class));
    doNothing().when(mockLockManager).close();
    ArgumentCaptor<List> lockObjsCaptor = ArgumentCaptor.forClass(List.class);

    when(mockQueryPlan.getInputs()).thenReturn(createReadEntities());
    when(mockQueryPlan.getOutputs()).thenReturn(new HashSet<WriteEntity>());

    // Execute
    txnMgr.acquireLocks(mockQueryPlan, ctx, "fred", lDrvState);

    // Verify
    Assert.assertEquals("db1", SessionState.get().getCurrentDatabase());
    List<HiveLock> resultLocks = ctx.getHiveLocks();
    Assert.assertEquals(expectedLocks.size(), resultLocks.size());
    Assert.assertEquals(expectedLocks.get(0).getHiveLockMode(), resultLocks.get(0).getHiveLockMode());
    Assert.assertEquals(expectedLocks.get(0).getHiveLockObject().getName(), resultLocks.get(0).getHiveLockObject().getName());
    Assert.assertEquals(expectedLocks.get(1).getHiveLockMode(), resultLocks.get(1).getHiveLockMode());
    Assert.assertEquals(expectedLocks.get(0).getHiveLockObject().getName(), resultLocks.get(0).getHiveLockObject().getName());

    verify(mockLockManager).lock(lockObjsCaptor.capture(), eq(false), eq(lDrvState));
    List<HiveLockObj> lockObjs = lockObjsCaptor.getValue();
    Assert.assertEquals(2, lockObjs.size());
    Assert.assertEquals("default", lockObjs.get(0).getName());
    Assert.assertEquals(HiveLockMode.SHARED, lockObjs.get(0).mode);
    Assert.assertEquals("default/table1", lockObjs.get(1).getName());
    Assert.assertEquals(HiveLockMode.SHARED, lockObjs.get(1).mode);

    // Execute
    try {
      txnMgr.acquireLocks(mockQueryPlan, ctx, "fred", lDrvInp);
      Assert.fail();
    } catch(LockException le) {
      Assert.assertEquals(le.getMessage(), ErrorMsg.LOCK_ACQUIRE_CANCELLED.getMsg());
    }

  }

  @Test
  public void testDedupLockObjects() {
    List<HiveLockObj> lockObjs = new ArrayList<HiveLockObj>();
    String path1 = "path1";
    String path2 = "path2";
    HiveLockObjectData lockData1 = new HiveLockObjectData(
        "query1", "1", "IMPLICIT", "drop table table1", conf);
    HiveLockObjectData lockData2 = new HiveLockObjectData(
        "query1", "1", "IMPLICIT", "drop table table1", conf);

    // Start with the following locks:
    // [path1, shared]
    // [path1, exclusive]
    // [path2, shared]
    // [path2, shared]
    // [path2, shared]
    lockObjs.add(new HiveLockObj(new HiveLockObject(path1, lockData1), HiveLockMode.SHARED));
    String name1 = lockObjs.get(lockObjs.size() - 1).getName();
    lockObjs.add(new HiveLockObj(new HiveLockObject(path1, lockData1), HiveLockMode.EXCLUSIVE));
    lockObjs.add(new HiveLockObj(new HiveLockObject(path2, lockData2), HiveLockMode.SHARED));
    String name2 = lockObjs.get(lockObjs.size() - 1).getName();
    lockObjs.add(new HiveLockObj(new HiveLockObject(path2, lockData2), HiveLockMode.SHARED));
    lockObjs.add(new HiveLockObj(new HiveLockObject(path2, lockData2), HiveLockMode.SHARED));

    DummyTxnManager.dedupLockObjects(lockObjs);

    // After dedup we should be left with 2 locks:
    // [path1, exclusive]
    // [path2, shared]
    Assert.assertEquals("Locks should be deduped", 2, lockObjs.size());

    Comparator<HiveLockObj> cmp = new Comparator<HiveLockObj>() {
      @Override
      public int compare(HiveLockObj lock1, HiveLockObj lock2) {
        return lock1.getName().compareTo(lock2.getName());
      }
    };
    Collections.sort(lockObjs, cmp);

    HiveLockObj lockObj = lockObjs.get(0);
    Assert.assertEquals(name1, lockObj.getName());
    Assert.assertEquals(HiveLockMode.EXCLUSIVE, lockObj.getMode());

    lockObj = lockObjs.get(1);
    Assert.assertEquals(name2, lockObj.getName());
    Assert.assertEquals(HiveLockMode.SHARED, lockObj.getMode());
  }

  private HashSet<ReadEntity> createReadEntities() {
    HashSet<ReadEntity> readEntities = new HashSet<ReadEntity>();
    ReadEntity re = new ReadEntity(newTable(false));
    readEntities.add(re);

    return readEntities;
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
}
