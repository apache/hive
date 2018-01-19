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

import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnInfo;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.TxnState;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.util.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

/**
 * Tests for TxnHandler.
 */
public class TestTxnHandler {
  static final private String CLASS_NAME = TxnHandler.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  private HiveConf conf = new HiveConf();
  private TxnStore txnHandler;

  public TestTxnHandler() throws Exception {
    TxnDbUtil.setConfValues(conf);
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    Configuration conf = ctx.getConfiguration();
    conf.getLoggerConfig(CLASS_NAME).setLevel(Level.DEBUG);
    ctx.updateLoggers(conf);
    tearDown();
  }

  @Test
  public void testValidTxnsEmpty() throws Exception {
    GetOpenTxnsInfoResponse txnsInfo = txnHandler.getOpenTxnsInfo();
    assertEquals(0L, txnsInfo.getTxn_high_water_mark());
    assertTrue(txnsInfo.getOpen_txns().isEmpty());
    GetOpenTxnsResponse txns = txnHandler.getOpenTxns();
    assertEquals(0L, txns.getTxn_high_water_mark());
    assertTrue(txns.getOpen_txns().isEmpty());
  }

  @Test
  public void testOpenTxn() throws Exception {
    long first = openTxn();
    assertEquals(1L, first);
    long second = openTxn();
    assertEquals(2L, second);
    GetOpenTxnsInfoResponse txnsInfo = txnHandler.getOpenTxnsInfo();
    assertEquals(2L, txnsInfo.getTxn_high_water_mark());
    assertEquals(2, txnsInfo.getOpen_txns().size());
    assertEquals(1L, txnsInfo.getOpen_txns().get(0).getId());
    assertEquals(TxnState.OPEN, txnsInfo.getOpen_txns().get(0).getState());
    assertEquals(2L, txnsInfo.getOpen_txns().get(1).getId());
    assertEquals(TxnState.OPEN, txnsInfo.getOpen_txns().get(1).getState());
    assertEquals("me", txnsInfo.getOpen_txns().get(1).getUser());
    assertEquals("localhost", txnsInfo.getOpen_txns().get(1).getHostname());

    GetOpenTxnsResponse txns = txnHandler.getOpenTxns();
    assertEquals(2L, txns.getTxn_high_water_mark());
    assertEquals(2, txns.getOpen_txns().size());
    boolean[] saw = new boolean[3];
    for (int i = 0; i < saw.length; i++) saw[i] = false;
    for (Long tid : txns.getOpen_txns()) {
      saw[tid.intValue()] = true;
    }
    for (int i = 1; i < saw.length; i++) assertTrue(saw[i]);
  }

  @Test
  public void testAbortTxn() throws Exception {
    OpenTxnsResponse openedTxns = txnHandler.openTxns(new OpenTxnRequest(3, "me", "localhost"));
    List<Long> txnList = openedTxns.getTxn_ids();
    long first = txnList.get(0);
    assertEquals(1L, first);
    long second = txnList.get(1);
    assertEquals(2L, second);
    txnHandler.abortTxn(new AbortTxnRequest(1));
    List<String> parts = new ArrayList<String>();
    parts.add("p=1");
    AddDynamicPartitions adp = new AddDynamicPartitions(3, "default", "T", parts);
    adp.setOperationType(DataOperationType.INSERT);
    txnHandler.addDynamicPartitions(adp);
    GetOpenTxnsInfoResponse txnsInfo = txnHandler.getOpenTxnsInfo();
    assertEquals(3, txnsInfo.getTxn_high_water_mark());
    assertEquals(3, txnsInfo.getOpen_txns().size());
    assertEquals(1L, txnsInfo.getOpen_txns().get(0).getId());
    assertEquals(TxnState.ABORTED, txnsInfo.getOpen_txns().get(0).getState());
    assertEquals(2L, txnsInfo.getOpen_txns().get(1).getId());
    assertEquals(TxnState.OPEN, txnsInfo.getOpen_txns().get(1).getState());
    assertEquals(3, txnsInfo.getOpen_txns().get(2).getId());
    assertEquals(TxnState.OPEN, txnsInfo.getOpen_txns().get(2).getState());

    GetOpenTxnsResponse txns = txnHandler.getOpenTxns();
    assertEquals(3, txns.getTxn_high_water_mark());
    assertEquals(3, txns.getOpen_txns().size());
    boolean[] saw = new boolean[4];
    for (int i = 0; i < saw.length; i++) saw[i] = false;
    for (Long tid : txns.getOpen_txns()) {
      saw[tid.intValue()] = true;
    }
    for (int i = 1; i < saw.length; i++) assertTrue(saw[i]);
    txnHandler.commitTxn(new CommitTxnRequest(2));
    //this succeeds as abortTxn is idempotent
    txnHandler.abortTxn(new AbortTxnRequest(1));
    boolean gotException = false;
    try {
      txnHandler.abortTxn(new AbortTxnRequest(2));
    }
    catch(NoSuchTxnException ex) {
      gotException = true;
      //if this wasn't an empty txn, we'd get a better msg
      Assert.assertEquals("No such transaction " + JavaUtils.txnIdToString(2), ex.getMessage());
    }
    Assert.assertTrue(gotException);
    gotException = false;
    txnHandler.commitTxn(new CommitTxnRequest(3));
    try {
      txnHandler.abortTxn(new AbortTxnRequest(3));
    }
    catch(NoSuchTxnException ex) {
      gotException = true;
      //txn 3 is not empty txn, so we get a better msg
      Assert.assertEquals("Transaction " + JavaUtils.txnIdToString(3) + " is already committed.", ex.getMessage());
    }
    Assert.assertTrue(gotException);
    
    gotException = false;
    try {
      txnHandler.abortTxn(new AbortTxnRequest(4));
    }
    catch(NoSuchTxnException ex) {
      gotException = true;
      Assert.assertEquals("No such transaction " + JavaUtils.txnIdToString(4), ex.getMessage());
    }
    Assert.assertTrue(gotException);
  }

  @Test
  public void testAbortInvalidTxn() throws Exception {
    boolean caught = false;
    try {
      txnHandler.abortTxn(new AbortTxnRequest(195L));
    } catch (NoSuchTxnException e) {
      caught = true;
    }
    assertTrue(caught);
  }

  @Test
  public void testValidTxnsNoneOpen() throws Exception {
    txnHandler.openTxns(new OpenTxnRequest(2, "me", "localhost"));
    txnHandler.commitTxn(new CommitTxnRequest(1));
    txnHandler.commitTxn(new CommitTxnRequest(2));
    GetOpenTxnsInfoResponse txnsInfo = txnHandler.getOpenTxnsInfo();
    assertEquals(2L, txnsInfo.getTxn_high_water_mark());
    assertEquals(0, txnsInfo.getOpen_txns().size());
    GetOpenTxnsResponse txns = txnHandler.getOpenTxns();
    assertEquals(2L, txns.getTxn_high_water_mark());
    assertEquals(0, txns.getOpen_txns().size());
  }

  @Test
  public void testValidTxnsSomeOpen() throws Exception {
    txnHandler.openTxns(new OpenTxnRequest(3, "me", "localhost"));
    txnHandler.abortTxn(new AbortTxnRequest(1));
    txnHandler.commitTxn(new CommitTxnRequest(2));
    GetOpenTxnsInfoResponse txnsInfo = txnHandler.getOpenTxnsInfo();
    assertEquals(3L, txnsInfo.getTxn_high_water_mark());
    assertEquals(2, txnsInfo.getOpen_txns().size());
    assertEquals(1L, txnsInfo.getOpen_txns().get(0).getId());
    assertEquals(TxnState.ABORTED, txnsInfo.getOpen_txns().get(0).getState());
    assertEquals(3L, txnsInfo.getOpen_txns().get(1).getId());
    assertEquals(TxnState.OPEN, txnsInfo.getOpen_txns().get(1).getState());

    GetOpenTxnsResponse txns = txnHandler.getOpenTxns();
    assertEquals(3L, txns.getTxn_high_water_mark());
    assertEquals(2, txns.getOpen_txns().size());
    boolean[] saw = new boolean[4];
    for (int i = 0; i < saw.length; i++) saw[i] = false;
    for (Long tid : txns.getOpen_txns()) {
      saw[tid.intValue()] = true;
    }
    assertTrue(saw[1]);
    assertFalse(saw[2]);
    assertTrue(saw[3]);
  }

  @Test
  public void testLockDifferentDBs() throws Exception {
    // Test that two different databases don't collide on their locks
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setOperationType(DataOperationType.NO_TXN);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);

    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "yourdb");
    comp.setOperationType(DataOperationType.NO_TXN);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);
  }

  @Test
  public void testLockSameDB() throws Exception {
    // Test that two different databases don't collide on their locks
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setOperationType(DataOperationType.NO_TXN);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);

    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setOperationType(DataOperationType.NO_TXN);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.WAITING);
  }

  @Test
  public void testLockDbLocksTable() throws Exception {
    // Test that locking a database prevents locking of tables in the database
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setOperationType(DataOperationType.NO_TXN);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);

    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setOperationType(DataOperationType.NO_TXN);
    comp.setTablename("mytable");
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.WAITING);
  }

  @Test
  public void testLockDbDoesNotLockTableInDifferentDB() throws Exception {
    // Test that locking a database prevents locking of tables in the database
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setOperationType(DataOperationType.NO_TXN);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);

    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "yourdb");
    comp.setOperationType(DataOperationType.NO_TXN);
    comp.setTablename("mytable");
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);
  }

  @Test
  public void testLockDifferentTables() throws Exception {
    // Test that two different tables don't collide on their locks
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setOperationType(DataOperationType.NO_TXN);
    comp.setTablename("mytable");
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);

    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setOperationType(DataOperationType.NO_TXN);
    comp.setTablename("yourtable");
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);
  }

  @Test
  public void testLockSameTable() throws Exception {
    // Test that two different tables don't collide on their locks
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setOperationType(DataOperationType.NO_TXN);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);

    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setOperationType(DataOperationType.NO_TXN);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.WAITING);
  }

  @Test
  public void testLockTableLocksPartition() throws Exception {
    // Test that locking a table prevents locking of partitions of the table
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setOperationType(DataOperationType.NO_TXN);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);

    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.NO_TXN);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.WAITING);
  }

  @Test
  public void testLockDifferentTableDoesntLockPartition() throws Exception {
    // Test that locking a table prevents locking of partitions of the table
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setOperationType(DataOperationType.NO_TXN);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);

    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("yourtable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.NO_TXN);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);
  }

  @Test
  public void testLockDifferentPartitions() throws Exception {
    // Test that two different partitions don't collide on their locks
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.NO_TXN);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);

    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("yourpartition");
    comp.setOperationType(DataOperationType.NO_TXN);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);
  }

  @Test
  public void testLockSamePartition() throws Exception {
    // Test that two different partitions don't collide on their locks
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.NO_TXN);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);

    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.NO_TXN);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.WAITING);
  }

  @Test
  public void testLockSRSR() throws Exception {
    // Test that two shared read locks can share a partition
    LockComponent comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.INSERT);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);

    comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.SELECT);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);
  }

  @Test
  public void testLockESRSR() throws Exception {
    // Test that exclusive lock blocks shared reads
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.NO_TXN);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);

    comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.INSERT);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.WAITING);

    comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.SELECT);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.WAITING);
  }

  @Test
  public void testLockSRSW() throws Exception {
    // Test that write can acquire after read
    LockComponent comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.INSERT);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);

    comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.DELETE);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    req.setTxnid(openTxn());
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);
  }

  @Test
  public void testLockESRSW() throws Exception {
    // Test that exclusive lock blocks read and write
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.NO_TXN);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);

    comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.SELECT);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.WAITING);

    comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.UPDATE);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    req.setTxnid(openTxn());
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.WAITING);
  }

  @Test
  public void testLockSRE() throws Exception {
    // Test that read blocks exclusive
    LockComponent comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.SELECT);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);

    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.NO_TXN);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.WAITING);
  }

  @Test
  public void testLockESRE() throws Exception {
    // Test that exclusive blocks read and exclusive
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.NO_TXN);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);

    comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.SELECT);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.WAITING);

    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.NO_TXN);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.WAITING);
  }

  @Test
  public void testLockSWSR() throws Exception {
    // Test that read can acquire after write
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(openTxn());
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);

    comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.SELECT);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);
  }

  @Test
  public void testLockSWSWSR() throws Exception {
    // Test that write blocks write but read can still acquire
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(openTxn());
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);

    comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.DELETE);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    req.setTxnid(openTxn());
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.WAITING);

    comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.INSERT);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);
  }
  @Ignore("now that every op has a txn ctx, we don't produce the error expected here....")
  @Test
  public void testWrongLockForOperation() throws Exception {
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.NO_TXN);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(openTxn());
    Exception expectedError = null;
    try {
      LockResponse res = txnHandler.lock(req);
    }
    catch(Exception e) {
      expectedError = e;
    }
    Assert.assertTrue(expectedError != null && expectedError.getMessage().contains("Unexpected DataOperationType"));
  }
  @Test
  public void testLockSWSWSW() throws Exception {
    // Test that write blocks two writes
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.DELETE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(openTxn());
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);

    comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.DELETE);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    req.setTxnid(openTxn());
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.WAITING);

    comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.DELETE);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    req.setTxnid(openTxn());
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.WAITING);
  }

  @Test
  public void testLockEESW() throws Exception {
    // Test that exclusive blocks exclusive and write
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.NO_TXN);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);

    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.NO_TXN);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.WAITING);

    comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.DELETE);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    req.setTxnid(openTxn());
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.WAITING);
  }

  @Test
  public void testLockEESR() throws Exception {
    // Test that exclusive blocks exclusive and read
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.NO_TXN);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);

    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.NO_TXN);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.WAITING);

    comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.SELECT);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.WAITING);
  }

  @Test
  public void testCheckLockAcquireAfterWaiting() throws Exception {
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.DELETE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    long txnId = openTxn();
    req.setTxnid(txnId);
    LockResponse res = txnHandler.lock(req);
    long lockid1 = res.getLockid();
    assertTrue(res.getState() == LockState.ACQUIRED);

    comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.UPDATE);
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    req.setTxnid(openTxn());
    res = txnHandler.lock(req);
    long lockid2 = res.getLockid();
    assertTrue(res.getState() == LockState.WAITING);

    txnHandler.abortTxn(new AbortTxnRequest(txnId));
    res = txnHandler.checkLock(new CheckLockRequest(lockid2));
    assertTrue(res.getState() == LockState.ACQUIRED);
  }

  @Test
  public void testCheckLockNoSuchLock() throws Exception {
    try {
      txnHandler.checkLock(new CheckLockRequest(23L));
      fail("Allowed to check lock on non-existent lock");
    } catch (NoSuchLockException e) {
    }
  }

  @Test
  public void testCheckLockTxnAborted() throws Exception {
    // Test that when a transaction is aborted, the heartbeat fails
    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.DELETE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    long lockid = res.getLockid();
    txnHandler.abortTxn(new AbortTxnRequest(txnid));
    try {
      // This will throw NoSuchLockException (even though it's the
      // transaction we've closed) because that will have deleted the lock.
      txnHandler.checkLock(new CheckLockRequest(lockid));
      fail("Allowed to check lock on aborted transaction.");
    } catch (NoSuchLockException e) {
    }
  }

  @Test
  public void testMultipleLock() throws Exception {
    // Test more than one lock can be handled in a lock request
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.NO_TXN);
    List<LockComponent> components = new ArrayList<LockComponent>(2);
    components.add(comp);

    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("anotherpartition");
    comp.setOperationType(DataOperationType.NO_TXN);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);
    long lockid = res.getLockid();
    assertTrue(res.getState() == LockState.ACQUIRED);
    res = txnHandler.checkLock(new CheckLockRequest(lockid));
    assertTrue(res.getState() == LockState.ACQUIRED);
    txnHandler.unlock(new UnlockRequest(lockid));
    assertEquals(0, txnHandler.numLocksInLockTable());
  }

  @Test
  public void testMultipleLockWait() throws Exception {
    // Test that two shared read locks can share a partition
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.NO_TXN);
    List<LockComponent> components = new ArrayList<LockComponent>(2);
    components.add(comp);

    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("anotherpartition");
    comp.setOperationType(DataOperationType.NO_TXN);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);
    long lockid1 = res.getLockid();
    assertTrue(res.getState() == LockState.ACQUIRED);


    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.NO_TXN);
    components = new ArrayList<LockComponent>(1);
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    res = txnHandler.lock(req);
    long lockid2 = res.getLockid();
    assertTrue(res.getState() == LockState.WAITING);

    txnHandler.unlock(new UnlockRequest(lockid1));

    res = txnHandler.checkLock(new CheckLockRequest(lockid2));
    assertTrue(res.getState() == LockState.ACQUIRED);
  }

  @Test
  public void testUnlockOnCommit() throws Exception {
    // Test that committing unlocks
    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB,  "mydb");
    comp.setTablename("mytable");
    comp.setOperationType(DataOperationType.DELETE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));
    assertEquals(0, txnHandler.numLocksInLockTable());
  }

  @Test
  public void testUnlockOnAbort() throws Exception {
    // Test that committing unlocks
    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);
    txnHandler.abortTxn(new AbortTxnRequest(txnid));
    assertEquals(0, txnHandler.numLocksInLockTable());
  }

  @Test
  public void testUnlockWithTxn() throws Exception {
    LOG.debug("Starting testUnlockWithTxn");
    // Test that attempting to unlock locks associated with a transaction
    // generates an error
    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.DELETE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    long lockid = res.getLockid();
    try {
      txnHandler.unlock(new UnlockRequest(lockid));
      fail("Allowed to unlock lock associated with transaction.");
    } catch (TxnOpenException e) {
    }
  }

  @Test
  public void testHeartbeatTxnAborted() throws Exception {
    // Test that when a transaction is aborted, the heartbeat fails
    openTxn();
    txnHandler.abortTxn(new AbortTxnRequest(1));
    HeartbeatRequest h = new HeartbeatRequest();
    h.setTxnid(1);
    try {
      txnHandler.heartbeat(h);
      fail("Told there was a txn, when it should have been aborted.");
    } catch (TxnAbortedException e) {
    }
 }

  @Test
  public void testHeartbeatNoTxn() throws Exception {
    // Test that when a transaction is aborted, the heartbeat fails
    HeartbeatRequest h = new HeartbeatRequest();
    h.setTxnid(939393L);
    try {
      txnHandler.heartbeat(h);
      fail("Told there was a txn, when there wasn't.");
    } catch (NoSuchTxnException e) {
    }
  }

  @Test
  public void testHeartbeatLock() throws Exception {
    conf.setTimeVar(HiveConf.ConfVars.HIVE_TXN_TIMEOUT, 1, TimeUnit.SECONDS);
    HeartbeatRequest h = new HeartbeatRequest();
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.NO_TXN);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);
    h.setLockid(res.getLockid());
    for (int i = 0; i < 30; i++) {
      try {
        txnHandler.heartbeat(h);
      } catch (NoSuchLockException e) {
        fail("Told there was no lock, when the heartbeat should have kept it.");
      }
    }
  }

  @Test
  public void heartbeatTxnRange() throws Exception {
    long txnid = openTxn();
    assertEquals(1, txnid);
    txnid = openTxn();
    txnid = openTxn();
    HeartbeatTxnRangeResponse rsp =
        txnHandler.heartbeatTxnRange(new HeartbeatTxnRangeRequest(1, 3));
    assertEquals(0, rsp.getAborted().size());
    assertEquals(0, rsp.getNosuch().size());
  }

  @Test
  public void heartbeatTxnRangeOneCommitted() throws Exception {
    long txnid = openTxn();
    assertEquals(1, txnid);
    txnHandler.commitTxn(new CommitTxnRequest(1));
    txnid = openTxn();
    txnid = openTxn();
    HeartbeatTxnRangeResponse rsp =
      txnHandler.heartbeatTxnRange(new HeartbeatTxnRangeRequest(1, 3));
    assertEquals(1, rsp.getNosuchSize());
    Long txn = rsp.getNosuch().iterator().next();
    assertEquals(1L, (long)txn);
    assertEquals(0, rsp.getAborted().size());
  }

  @Test
  public void heartbeatTxnRangeOneAborted() throws Exception {
    long txnid = openTxn();
    assertEquals(1, txnid);
    txnid = openTxn();
    txnid = openTxn();
    txnHandler.abortTxn(new AbortTxnRequest(3));
    HeartbeatTxnRangeResponse rsp =
      txnHandler.heartbeatTxnRange(new HeartbeatTxnRangeRequest(1, 3));
    assertEquals(1, rsp.getAbortedSize());
    Long txn = rsp.getAborted().iterator().next();
    assertEquals(3L, (long)txn);
    assertEquals(0, rsp.getNosuch().size());
  }

  @Test
  public void testLockTimeout() throws Exception {
    long timeout = txnHandler.setTimeout(1);
    try {
      LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
      comp.setTablename("mytable");
      comp.setPartitionname("mypartition");
      comp.setOperationType(DataOperationType.NO_TXN);
      List<LockComponent> components = new ArrayList<LockComponent>(1);
      components.add(comp);
      LockRequest req = new LockRequest(components, "me", "localhost");
      LockResponse res = txnHandler.lock(req);
      assertTrue(res.getState() == LockState.ACQUIRED);
      Thread.sleep(10);
      txnHandler.performTimeOuts();
      txnHandler.checkLock(new CheckLockRequest(res.getLockid()));
      fail("Told there was a lock, when it should have timed out.");
    } catch (NoSuchLockException e) {
    } finally {
      txnHandler.setTimeout(timeout);
    }
  }

  @Test
  public void testRecoverManyTimeouts() throws Exception {
    long timeout = txnHandler.setTimeout(1);
    try {
      txnHandler.openTxns(new OpenTxnRequest(503, "me", "localhost"));
      Thread.sleep(10);
      txnHandler.performTimeOuts();
      GetOpenTxnsInfoResponse rsp = txnHandler.getOpenTxnsInfo();
      int numAborted = 0;
      for (TxnInfo txnInfo : rsp.getOpen_txns()) {
        assertEquals(TxnState.ABORTED, txnInfo.getState());
        numAborted++;
      }
      assertEquals(503, numAborted);
    } finally {
      txnHandler.setTimeout(timeout);
    }


  }

  @Test
  public void testHeartbeatNoLock() throws Exception {
    HeartbeatRequest h = new HeartbeatRequest();
    h.setLockid(29389839L);
    try {
      txnHandler.heartbeat(h);
      fail("Told there was a lock, when there wasn't.");
    } catch (NoSuchLockException e) {
    }
  }

  @Test
  public void testCompactMajorWithPartition() throws Exception {
    CompactionRequest rqst = new CompactionRequest("foo", "bar", CompactionType.MAJOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    assertEquals(1, compacts.size());
    ShowCompactResponseElement c = compacts.get(0);
    assertEquals("foo", c.getDbname());
    assertEquals("bar", c.getTablename());
    assertEquals("ds=today", c.getPartitionname());
    assertEquals(CompactionType.MAJOR, c.getType());
    assertEquals("initiated", c.getState());
    assertEquals(0L, c.getStart());
  }

  /**
   * Once a Compaction for a given resource is scheduled/working, we should not
   * schedule another one to prevent concurrent compactions for the same resource.
   * @throws Exception
   */
  @Test
  public void testCompactWhenAlreadyCompacting() throws Exception {
    CompactionRequest rqst = new CompactionRequest("foo", "bar", CompactionType.MAJOR);
    rqst.setPartitionname("ds=today");
    CompactionResponse resp = txnHandler.compact(rqst);
    Assert.assertEquals(resp, new CompactionResponse(1, TxnStore.INITIATED_RESPONSE, true));

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    assertEquals(1, compacts.size());

    rqst.setType(CompactionType.MINOR);
    resp = txnHandler.compact(rqst);
    Assert.assertEquals(resp, new CompactionResponse(1, TxnStore.INITIATED_RESPONSE, false));

    rsp = txnHandler.showCompact(new ShowCompactRequest());
    compacts = rsp.getCompacts();
    assertEquals(1, compacts.size());
    ShowCompactResponseElement c = compacts.get(0);
    assertEquals("foo", c.getDbname());
    assertEquals("bar", c.getTablename());
    assertEquals("ds=today", c.getPartitionname());
    assertEquals(CompactionType.MAJOR, c.getType());
    assertEquals("initiated", c.getState());
    assertEquals(0L, c.getStart());
  }

  @Test
  public void testCompactMinorNoPartition() throws Exception {
    CompactionRequest rqst = new CompactionRequest("foo", "bar", CompactionType.MINOR);
    rqst.setRunas("fred");
    txnHandler.compact(rqst);

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    assertEquals(1, compacts.size());
    ShowCompactResponseElement c = compacts.get(0);
    assertEquals("foo", c.getDbname());
    assertEquals("bar", c.getTablename());
    assertNull(c.getPartitionname());
    assertEquals(CompactionType.MINOR, c.getType());
    assertEquals("initiated", c.getState());
    assertEquals(0L, c.getStart());
    assertEquals("fred", c.getRunAs());
  }

  @Test
  public void showLocks() throws Exception {
    long begining = System.currentTimeMillis();
    LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, "mydb");
    comp.setOperationType(DataOperationType.NO_TXN);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);

    // Open txn
    long txnid = openTxn();
    comp = new LockComponent(LockType.SHARED_READ, LockLevel.TABLE, "mydb");
    comp.setTablename("mytable");
    comp.setOperationType(DataOperationType.SELECT);
    components = new ArrayList<LockComponent>(1);
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    res = txnHandler.lock(req);

    // Locks not associated with a txn
    components = new ArrayList<LockComponent>(1);
    comp = new LockComponent(LockType.SHARED_READ, LockLevel.PARTITION, "yourdb");
    comp.setTablename("yourtable");
    comp.setPartitionname("yourpartition");
    comp.setOperationType(DataOperationType.INSERT);
    components.add(comp);
    req = new LockRequest(components, "you", "remotehost");
    res = txnHandler.lock(req);

    ShowLocksResponse rsp = txnHandler.showLocks(new ShowLocksRequest());
    List<ShowLocksResponseElement> locks = rsp.getLocks();
    assertEquals(3, locks.size());
    boolean[] saw = new boolean[locks.size()];
    for (int i = 0; i < saw.length; i++) saw[i] = false;
    for (ShowLocksResponseElement lock : locks) {
      if (lock.getLockid() == 1) {
        assertEquals(0, lock.getTxnid());
        assertEquals("mydb", lock.getDbname());
        assertNull(lock.getTablename());
        assertNull(lock.getPartname());
        assertEquals(LockState.ACQUIRED, lock.getState());
        assertEquals(LockType.EXCLUSIVE, lock.getType());
        assertTrue(lock.toString(), 0 != lock.getLastheartbeat());
        assertTrue("Expected acquired at " + lock.getAcquiredat() + " to be between " + begining
            + " and " + System.currentTimeMillis(),
            begining <= lock.getAcquiredat() && System.currentTimeMillis() >= lock.getAcquiredat());
        assertEquals("me", lock.getUser());
        assertEquals("localhost", lock.getHostname());
        saw[0] = true;
      } else if (lock.getLockid() == 2) {
        assertEquals(1, lock.getTxnid());
        assertEquals("mydb", lock.getDbname());
        assertEquals("mytable", lock.getTablename());
        assertNull(lock.getPartname());
        assertEquals(LockState.WAITING, lock.getState());
        assertEquals(LockType.SHARED_READ, lock.getType());
        assertTrue(lock.toString(), 0 == lock.getLastheartbeat() &&
          lock.getTxnid() != 0);
        assertEquals(0, lock.getAcquiredat());
        assertEquals("me", lock.getUser());
        assertEquals("localhost", lock.getHostname());
        saw[1] = true;
      } else if (lock.getLockid() == 3) {
        assertEquals(0, lock.getTxnid());
        assertEquals("yourdb", lock.getDbname());
        assertEquals("yourtable", lock.getTablename());
        assertEquals("yourpartition", lock.getPartname());
        assertEquals(LockState.ACQUIRED, lock.getState());
        assertEquals(LockType.SHARED_READ, lock.getType());
        assertTrue(lock.toString(), begining <= lock.getLastheartbeat() &&
            System.currentTimeMillis() >= lock.getLastheartbeat());
        assertTrue(begining <= lock.getAcquiredat() &&
            System.currentTimeMillis() >= lock.getAcquiredat());
        assertEquals("you", lock.getUser());
        assertEquals("remotehost", lock.getHostname());
        saw[2] = true;
      } else {
        fail("Unknown lock id");
      }
    }
    for (int i = 0; i < saw.length; i++) assertTrue("Didn't see lock id " + i, saw[i]);
  }

  @Test
  @Ignore("Wedges Derby")
  public void deadlockDetected() throws Exception {
    LOG.debug("Starting deadlock test");
    if (txnHandler instanceof TxnHandler) {
      final TxnHandler tHndlr = (TxnHandler)txnHandler;
      Connection conn = tHndlr.getDbConn(Connection.TRANSACTION_SERIALIZABLE);
      Statement stmt = conn.createStatement();
      long now = tHndlr.getDbTime(conn);
      stmt.executeUpdate("insert into TXNS (txn_id, txn_state, txn_started, txn_last_heartbeat, " +
          "txn_user, txn_host) values (1, 'o', " + now + ", " + now + ", 'shagy', " +
          "'scooby.com')");
      stmt.executeUpdate("insert into HIVE_LOCKS (hl_lock_ext_id, hl_lock_int_id, hl_txnid, " +
          "hl_db, hl_table, hl_partition, hl_lock_state, hl_lock_type, hl_last_heartbeat, " +
          "hl_user, hl_host) values (1, 1, 1, 'mydb', 'mytable', 'mypartition', '" +
          tHndlr.LOCK_WAITING + "', '" + tHndlr.LOCK_EXCLUSIVE + "', " + now + ", 'fred', " +
          "'scooby.com')");
      conn.commit();
      tHndlr.closeDbConn(conn);

      final AtomicBoolean sawDeadlock = new AtomicBoolean();

      final Connection conn1 = tHndlr.getDbConn(Connection.TRANSACTION_SERIALIZABLE);
      final Connection conn2 = tHndlr.getDbConn(Connection.TRANSACTION_SERIALIZABLE);
      try {

        for (int i = 0; i < 5; i++) {
          Thread t1 = new Thread() {
            @Override
            public void run() {
              try {
                try {
                  updateTxns(conn1);
                  updateLocks(conn1);
                  Thread.sleep(1000);
                  conn1.commit();
                  LOG.debug("no exception, no deadlock");
                } catch (SQLException e) {
                  try {
                    tHndlr.checkRetryable(conn1, e, "thread t1");
                    LOG.debug("Got an exception, but not a deadlock, SQLState is " +
                        e.getSQLState() + " class of exception is " + e.getClass().getName() +
                        " msg is <" + e.getMessage() + ">");
                  } catch (TxnHandler.RetryException de) {
                    LOG.debug("Forced a deadlock, SQLState is " + e.getSQLState() + " class of " +
                        "exception is " + e.getClass().getName() + " msg is <" + e
                        .getMessage() + ">");
                    sawDeadlock.set(true);
                  }
                }
                conn1.rollback();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
          };

          Thread t2 = new Thread() {
            @Override
            public void run() {
              try {
                try {
                  updateLocks(conn2);
                  updateTxns(conn2);
                  Thread.sleep(1000);
                  conn2.commit();
                  LOG.debug("no exception, no deadlock");
                } catch (SQLException e) {
                  try {
                    tHndlr.checkRetryable(conn2, e, "thread t2");
                    LOG.debug("Got an exception, but not a deadlock, SQLState is " +
                        e.getSQLState() + " class of exception is " + e.getClass().getName() +
                        " msg is <" + e.getMessage() + ">");
                  } catch (TxnHandler.RetryException de) {
                    LOG.debug("Forced a deadlock, SQLState is " + e.getSQLState() + " class of " +
                        "exception is " + e.getClass().getName() + " msg is <" + e
                        .getMessage() + ">");
                    sawDeadlock.set(true);
                  }
                }
                conn2.rollback();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
          };

          t1.start();
          t2.start();
          t1.join();
          t2.join();
          if (sawDeadlock.get()) break;
        }
        assertTrue(sawDeadlock.get());
      } finally {
        conn1.rollback();
        tHndlr.closeDbConn(conn1);
        conn2.rollback();
        tHndlr.closeDbConn(conn2);
      }
    }
  }

  /**
   * This cannnot be run against Derby (thus in UT) but it can run againt MySQL.
   * 1. add to metastore/pom.xml
   *     <dependency>
   *      <groupId>mysql</groupId>
   *      <artifactId>mysql-connector-java</artifactId>
   *      <version>5.1.30</version>
   *     </dependency>
   * 2. Hack in the c'tor of this class
   *     conf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, "jdbc:mysql://localhost/metastore");
   *      conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME, "hive");
   *      conf.setVar(HiveConf.ConfVars.METASTOREPWD, "hive");
   *      conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER, "com.mysql.jdbc.Driver");
   * 3. Remove TxnDbUtil.prepDb(); in TxnHandler.checkQFileTestHack()
   *      
   */
  @Ignore("multiple threads wedge Derby")
  @Test
  public void testMutexAPI() throws Exception {
    final TxnStore.MutexAPI api =  txnHandler.getMutexAPI();
    final AtomicInteger stepTracker = new AtomicInteger(0);
    /**
     * counter = 0;
     * Thread1 counter=1, lock, wait 3s, check counter(should be 2), counter=3, unlock 
     * Thread2 counter=2, lock (and block), inc counter, should be 4
     */
    Thread t1 = new Thread("MutexTest1") {
      public void run() {
        try {
          stepTracker.incrementAndGet();//now 1
          TxnStore.MutexAPI.LockHandle handle = api.acquireLock(TxnHandler.MUTEX_KEY.HouseKeeper.name());
          Thread.sleep(4000);
          //stepTracker should now be 2 which indicates t2 has started
          Assert.assertEquals("Thread2 should have started by now but not done work", 2, stepTracker.get());
          stepTracker.incrementAndGet();//now 3
          handle.releaseLocks();
        }
        catch(Exception ex) {
          throw new RuntimeException(ex.getMessage(), ex);
        }
      }
    };
    t1.setDaemon(true);
    ErrorHandle ueh1 = new ErrorHandle();
    t1.setUncaughtExceptionHandler(ueh1);
    Thread t2 = new Thread("MutexTest2") {
      public void run() {
        try {
          stepTracker.incrementAndGet();//now 2
          //this should block until t1 unlocks
          TxnStore.MutexAPI.LockHandle handle = api.acquireLock(TxnHandler.MUTEX_KEY.HouseKeeper.name());
          stepTracker.incrementAndGet();//now 4
          Assert.assertEquals(4, stepTracker.get());
          handle.releaseLocks();
          stepTracker.incrementAndGet();//now 5
        }
        catch(Exception ex) {
          throw new RuntimeException(ex.getMessage(), ex);
        }
      }
    };
    t2.setDaemon(true);
    ErrorHandle ueh2 = new ErrorHandle();
    t2.setUncaughtExceptionHandler(ueh2);
    t1.start();
    try {
      Thread.sleep(1000);
    }
    catch(InterruptedException ex) {
      LOG.info("Sleep was interrupted");
    }
    t2.start();
    t1.join(6000);//so that test doesn't block
    t2.join(6000);

    if(ueh1.error != null) {
      Assert.assertTrue("Unexpected error from t1: " + StringUtils.stringifyException(ueh1.error), false);
    }
    if (ueh2.error != null) {
      Assert.assertTrue("Unexpected error from t2: " + StringUtils.stringifyException(ueh2.error), false);
    }
    Assert.assertEquals("5 means both threads have completed", 5, stepTracker.get());
  }
  private final static class ErrorHandle implements Thread.UncaughtExceptionHandler {
    Throwable error = null;
    @Override
    public void uncaughtException(Thread t, Throwable e) {
      LOG.error("Uncaught exception from " + t.getName() + ": " + e.getMessage());
      error = e;
    }
  }

  @Test
  public void testRetryableRegex() throws Exception {
    SQLException sqlException = new SQLException("ORA-08177: can't serialize access for this transaction", "72000");
    // Note that we have 3 regex'es below
    conf.setVar(HiveConf.ConfVars.HIVE_TXN_RETRYABLE_SQLEX_REGEX, "^Deadlock detected, roll back,.*08177.*,.*08178.*");
    boolean result = TxnHandler.isRetryable(conf, sqlException);
    Assert.assertTrue("regex should be retryable", result);

    sqlException = new SQLException("This error message, has comma in it");
    conf.setVar(HiveConf.ConfVars.HIVE_TXN_RETRYABLE_SQLEX_REGEX, ".*comma.*");
    result = TxnHandler.isRetryable(conf, sqlException);
    Assert.assertTrue("regex should be retryable", result);
  }

  private void updateTxns(Connection conn) throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.executeUpdate("update TXNS set txn_last_heartbeat = txn_last_heartbeat + 1");
  }

  private void updateLocks(Connection conn) throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.executeUpdate("update HIVE_LOCKS set hl_last_heartbeat = hl_last_heartbeat + 1");
  }

  @Before
  public void setUp() throws Exception {
    TxnDbUtil.prepDb(conf);
    txnHandler = TxnUtils.getTxnStore(conf);
  }

  @After
  public void tearDown() throws Exception {
    TxnDbUtil.cleanDb(conf);
  }

  private long openTxn() throws MetaException {
    List<Long> txns = txnHandler.openTxns(new OpenTxnRequest(1, "me", "localhost")).getTxn_ids();
    return txns.get(0);
  }

}
