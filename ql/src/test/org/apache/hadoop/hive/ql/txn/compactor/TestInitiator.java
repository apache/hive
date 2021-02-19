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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TransactionalValidationListener;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import org.mockito.Mockito;

/**
 * Tests for the compactor Initiator thread.
 */
public class TestInitiator extends CompactorTest {
  private final String INITIATED_METRICS_KEY = MetricsConstants.COMPACTION_STATUS_PREFIX + TxnStore.INITIATED_RESPONSE;

  @Test
  public void nothing() throws Exception {
    // Test that the whole things works when there's nothing in the queue.  This is just a
    // survival test.
    startInitiator();
  }

  @Test
  public void recoverFailedLocalWorkers() throws Exception {
    Table t = newTable("default", "rflw1", false);
    CompactionRequest rqst = new CompactionRequest("default", "rflw1", CompactionType.MINOR);
    txnHandler.compact(rqst);

    t = newTable("default", "rflw2", false);
    rqst = new CompactionRequest("default", "rflw2", CompactionType.MINOR);
    txnHandler.compact(rqst);

    txnHandler.findNextToCompact(Worker.hostname() + "-193892");
    txnHandler.findNextToCompact("nosuchhost-193892");

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(2, compacts.size());
    boolean sawInitiated = false;
    for (ShowCompactResponseElement c : compacts) {
      if (c.getState().equals("working")) {
        Assert.assertEquals("nosuchhost-193892", c.getWorkerid());
      } else if (c.getState().equals("initiated")) {
        sawInitiated = true;
      } else {
        Assert.fail("Unexpected state");
      }
    }
    Assert.assertTrue(sawInitiated);
  }

  @Test
  public void recoverFailedRemoteWorkers() throws Exception {
    Table t = newTable("default", "rfrw1", false);
    CompactionRequest rqst = new CompactionRequest("default", "rfrw1", CompactionType.MINOR);
    txnHandler.compact(rqst);

    txnHandler.findNextToCompact("nosuchhost-193892");

    conf.setTimeVar(HiveConf.ConfVars.HIVE_COMPACTOR_WORKER_TIMEOUT, 1L, TimeUnit.MILLISECONDS);

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("initiated", compacts.get(0).getState());
  }

  @Test
  public void majorCompactOnTableTooManyAborts() throws Exception {
    Table t = newTable("default", "mcottma", false);

    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD, 10);

    for (int i = 0; i < 11; i++) {
      long txnid = openTxn();
      LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE,  "default");
      comp.setTablename("mcottma");
      comp.setOperationType(DataOperationType.UPDATE);
      List<LockComponent> components = new ArrayList<LockComponent>(1);
      components.add(comp);
      LockRequest req = new LockRequest(components, "me", "localhost");
      req.setTxnid(txnid);
      LockResponse res = txnHandler.lock(req);
      txnHandler.abortTxn(new AbortTxnRequest(txnid));
    }

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("initiated", compacts.get(0).getState());
    Assert.assertEquals("mcottma", compacts.get(0).getTablename());
    Assert.assertEquals(CompactionType.MAJOR, compacts.get(0).getType());
  }

  @Test
  public void majorCompactOnPartitionTooManyAborts() throws Exception {
    Table t = newTable("default", "mcoptma", true);
    Partition p = newPartition(t, "today");

    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD, 10);

    for (int i = 0; i < 11; i++) {
      long txnid = openTxn();
      LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
      comp.setTablename("mcoptma");
      comp.setPartitionname("ds=today");
      comp.setOperationType(DataOperationType.DELETE);
      List<LockComponent> components = new ArrayList<LockComponent>(1);
      components.add(comp);
      LockRequest req = new LockRequest(components, "me", "localhost");
      req.setTxnid(txnid);
      LockResponse res = txnHandler.lock(req);
      txnHandler.abortTxn(new AbortTxnRequest(txnid));
    }

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("initiated", compacts.get(0).getState());
    Assert.assertEquals("mcoptma", compacts.get(0).getTablename());
    Assert.assertEquals("ds=today", compacts.get(0).getPartitionname());
    Assert.assertEquals(CompactionType.MAJOR, compacts.get(0).getType());
  }

  @Test
  public void noCompactOnManyDifferentPartitionAborts() throws Exception {
    Table t = newTable("default", "ncomdpa", true);
    for (int i = 0; i < 11; i++) {
      Partition p = newPartition(t, "day-" + i);
    }

    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD, 10);

    for (int i = 0; i < 11; i++) {
      long txnid = openTxn();
      LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE,  "default");
      comp.setTablename("ncomdpa");
      comp.setPartitionname("ds=day-" + i);
      comp.setOperationType(DataOperationType.UPDATE);
      List<LockComponent> components = new ArrayList<LockComponent>(1);
      components.add(comp);
      LockRequest req = new LockRequest(components, "me", "localhost");
      req.setTxnid(txnid);
      LockResponse res = txnHandler.lock(req);
      txnHandler.abortTxn(new AbortTxnRequest(txnid));
    }

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(0, rsp.getCompactsSize());
  }

  /**
   * Test that HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_TIME_THRESHOLD triggers compaction.
   *
   * @throws Exception
   */
  @Test
  public void compactExpiredAbortedTxns() throws Exception {
    Table t = newTable("default", "expiredAbortedTxns", false);
    // abort a txn
    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
    comp.setOperationType(DataOperationType.DELETE);
    comp.setTablename("expiredAbortedTxns");
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    txnHandler.lock(req);
    txnHandler.abortTxn(new AbortTxnRequest(txnid));

    // before setting, check that no compaction is queued
    initiateAndVerifyCompactionQueueLength(0);

    // negative number disables threshold check
    conf.setTimeVar(HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_TIME_THRESHOLD, -1,
        TimeUnit.MILLISECONDS);
    Thread.sleep(1L);
    initiateAndVerifyCompactionQueueLength(0);

    // set to 1 ms, wait 1 ms, and check that minor compaction is queued
    conf.setTimeVar(HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_TIME_THRESHOLD, 1, TimeUnit.MILLISECONDS);
    Thread.sleep(1L);
    ShowCompactResponse rsp = initiateAndVerifyCompactionQueueLength(1);
    Assert.assertEquals(CompactionType.MINOR, rsp.getCompacts().get(0).getType());
  }

  private ShowCompactResponse initiateAndVerifyCompactionQueueLength(int expectedLength)
      throws Exception {
    startInitiator();
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(expectedLength, rsp.getCompactsSize());
    return rsp;
  }

  @Test
  public void noCompactWhenNoCompactSet() throws Exception {
    Map<String, String> parameters = new HashMap<String, String>(1);
    parameters.put("NO_AUTO_COMPACTION", "true");
    Table t = newTable("default", "ncwncs", false, parameters);

    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD, 10);

    for (int i = 0; i < 11; i++) {
      long txnid = openTxn();
      LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
      comp.setTablename("ncwncs");
      comp.setOperationType(DataOperationType.UPDATE);
      List<LockComponent> components = new ArrayList<LockComponent>(1);
      components.add(comp);
      LockRequest req = new LockRequest(components, "me", "localhost");
      req.setTxnid(txnid);
      LockResponse res = txnHandler.lock(req);
      txnHandler.abortTxn(new AbortTxnRequest(txnid));
    }

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(0, rsp.getCompactsSize());
  }

  @Test
  public void noCompactWhenNoCompactSetLowerCase() throws Exception {
    Map<String, String> parameters = new HashMap<String, String>(1);
    parameters.put("no_auto_compaction", "true");
    Table t = newTable("default", "ncwncs", false, parameters);

    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD, 10);

    for (int i = 0; i < 11; i++) {
      long txnid = openTxn();
      LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
      comp.setOperationType(DataOperationType.DELETE);
      comp.setTablename("ncwncs");
      List<LockComponent> components = new ArrayList<LockComponent>(1);
      components.add(comp);
      LockRequest req = new LockRequest(components, "me", "localhost");
      req.setTxnid(txnid);
      LockResponse res = txnHandler.lock(req);
      txnHandler.abortTxn(new AbortTxnRequest(txnid));
    }

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(0, rsp.getCompactsSize());
  }

  @Test
  public void noCompactWhenCompactAlreadyScheduled() throws Exception {
    Table t = newTable("default", "ncwcas", false);

    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD, 10);

    for (int i = 0; i < 11; i++) {
      long txnid = openTxn();
      LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE,  "default");
      comp.setTablename("ncwcas");
      comp.setOperationType(DataOperationType.UPDATE);
      List<LockComponent> components = new ArrayList<LockComponent>(1);
      components.add(comp);
      LockRequest req = new LockRequest(components, "me", "localhost");
      req.setTxnid(txnid);
      LockResponse res = txnHandler.lock(req);
      txnHandler.abortTxn(new AbortTxnRequest(txnid));
    }

    CompactionRequest rqst = new CompactionRequest("default", "ncwcas", CompactionType.MAJOR);
    txnHandler.compact(rqst);

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("initiated", compacts.get(0).getState());
    Assert.assertEquals("ncwcas", compacts.get(0).getTablename());

    startInitiator();

    rsp = txnHandler.showCompact(new ShowCompactRequest());
    compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("initiated", compacts.get(0).getState());
    Assert.assertEquals("ncwcas", compacts.get(0).getTablename());
    Assert.assertEquals(CompactionType.MAJOR, compacts.get(0).getType());
  }

  @Test
  public void compactTableHighDeltaPct() throws Exception {
    Table t = newTable("default", "cthdp", false);

    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);

    burnThroughTransactions("default", "cthdp", 23);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
    comp.setTablename("cthdp");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    long writeid = allocateWriteId("default", "cthdp", txnid);
    Assert.assertEquals(24, writeid);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("initiated", compacts.get(0).getState());
    Assert.assertEquals("cthdp", compacts.get(0).getTablename());
    Assert.assertEquals(CompactionType.MAJOR, compacts.get(0).getType());
  }

  @Test
  public void compactPartitionHighDeltaPct() throws Exception {
    Table t = newTable("default", "cphdp", true);
    Partition p = newPartition(t, "today");

    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 22L, 2);
    addDeltaFile(t, p, 23L, 24L, 2);

    burnThroughTransactions("default", "cphdp", 23);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.PARTITION, "default");
    comp.setTablename("cphdp");
    comp.setPartitionname("ds=today");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    long writeid = allocateWriteId("default", "cphdp", txnid);
    Assert.assertEquals(24, writeid);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("initiated", compacts.get(0).getState());
    Assert.assertEquals("cphdp", compacts.get(0).getTablename());
    Assert.assertEquals("ds=today", compacts.get(0).getPartitionname());
    Assert.assertEquals(CompactionType.MAJOR, compacts.get(0).getType());
  }

  @Test
  public void compactCamelCasePartitionValue() throws Exception {
    Table t = newTable("default", "test_table", true);
    Partition p = newPartition(t, "ToDay");

    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 22L, 2);
    addDeltaFile(t, p, 23L, 24L, 2);

    burnThroughTransactions("default", "test_table", 23);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.PARTITION, "default");
    comp.setTablename("test_table");
    comp.setPartitionname("dS=ToDay");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    long writeid = allocateWriteId("default", "test_table", txnid);
    Assert.assertEquals(24, writeid);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("initiated", compacts.get(0).getState());
    Assert.assertEquals("test_table", compacts.get(0).getTablename());
    Assert.assertEquals("ds=ToDay", compacts.get(0).getPartitionname());
    Assert.assertEquals(CompactionType.MAJOR, compacts.get(0).getType());
  }

  @Test
  public void noCompactTableDeltaPctNotHighEnough() throws Exception {
    Table t = newTable("default", "nctdpnhe", false);

    addBaseFile(t, null, 50L, 50);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);

    burnThroughTransactions("default", "nctdpnhe", 53);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
    comp.setTablename("nctdpnhe");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    long writeid = allocateWriteId("default", "nctdpnhe", txnid);
    Assert.assertEquals(54, writeid);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(0, rsp.getCompactsSize());
  }

  @Test
  public void compactTableTooManyDeltas() throws Exception {
    Table t = newTable("default", "cttmd", false);

    addBaseFile(t, null, 200L, 200);
    addDeltaFile(t, null, 201L, 201L, 1);
    addDeltaFile(t, null, 202L, 202L, 1);
    addDeltaFile(t, null, 203L, 203L, 1);
    addDeltaFile(t, null, 204L, 204L, 1);
    addDeltaFile(t, null, 205L, 205L, 1);
    addDeltaFile(t, null, 206L, 206L, 1);
    addDeltaFile(t, null, 207L, 207L, 1);
    addDeltaFile(t, null, 208L, 208L, 1);
    addDeltaFile(t, null, 209L, 209L, 1);
    addDeltaFile(t, null, 210L, 210L, 1);
    addDeltaFile(t, null, 211L, 211L, 1);

    burnThroughTransactions("default", "cttmd", 210);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
    comp.setTablename("cttmd");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    long writeid = allocateWriteId("default", "cttmd", txnid);
    Assert.assertEquals(211, writeid);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("initiated", compacts.get(0).getState());
    Assert.assertEquals("cttmd", compacts.get(0).getTablename());
    Assert.assertEquals(CompactionType.MINOR, compacts.get(0).getType());
  }

  @Test
  public void compactPartitionTooManyDeltas() throws Exception {
    Table t = newTable("default", "cptmd", true);
    Partition p = newPartition(t, "today");

    addBaseFile(t, p, 200L, 200);
    addDeltaFile(t, p, 201L, 201L, 1);
    addDeltaFile(t, p, 202L, 202L, 1);
    addDeltaFile(t, p, 203L, 203L, 1);
    addDeltaFile(t, p, 204L, 204L, 1);
    addDeltaFile(t, p, 205L, 205L, 1);
    addDeltaFile(t, p, 206L, 206L, 1);
    addDeltaFile(t, p, 207L, 207L, 1);
    addDeltaFile(t, p, 208L, 208L, 1);
    addDeltaFile(t, p, 209L, 209L, 1);
    addDeltaFile(t, p, 210L, 210L, 1);
    addDeltaFile(t, p, 211L, 211L, 1);

    burnThroughTransactions("default", "cptmd", 210);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.PARTITION, "default");
    comp.setTablename("cptmd");
    comp.setPartitionname("ds=today");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    long writeid = allocateWriteId("default", "cptmd", txnid);
    Assert.assertEquals(211, writeid);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("initiated", compacts.get(0).getState());
    Assert.assertEquals("cptmd", compacts.get(0).getTablename());
    Assert.assertEquals("ds=today", compacts.get(0).getPartitionname());
    Assert.assertEquals(CompactionType.MINOR, compacts.get(0).getType());
  }

  @Test
  public void noCompactTableNotEnoughDeltas() throws Exception {
    Table t = newTable("default", "nctned", false);

    addBaseFile(t, null, 200L, 200);
    addDeltaFile(t, null, 201L, 205L, 5);
    addDeltaFile(t, null, 206L, 211L, 6);

    burnThroughTransactions("default", "nctned", 210);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
    comp.setTablename("nctned");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    long writeid = allocateWriteId("default", "nctned", txnid);
    Assert.assertEquals(211, writeid);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(0, rsp.getCompactsSize());
  }

  @Test
  public void chooseMajorOverMinorWhenBothValid() throws Exception {
    Table t = newTable("default", "cmomwbv", false);

    addBaseFile(t, null, 200L, 200);
    addDeltaFile(t, null, 201L, 211L, 11);
    addDeltaFile(t, null, 212L, 222L, 11);
    addDeltaFile(t, null, 223L, 233L, 11);
    addDeltaFile(t, null, 234L, 244L, 11);
    addDeltaFile(t, null, 245L, 255L, 11);
    addDeltaFile(t, null, 256L, 266L, 11);
    addDeltaFile(t, null, 267L, 277L, 11);
    addDeltaFile(t, null, 278L, 288L, 11);
    addDeltaFile(t, null, 289L, 299L, 11);
    addDeltaFile(t, null, 300L, 310L, 11);
    addDeltaFile(t, null, 311L, 321L, 11);

    burnThroughTransactions("default", "cmomwbv", 320);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
    comp.setTablename("cmomwbv");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    long writeid = allocateWriteId("default", "cmomwbv", txnid);
    Assert.assertEquals(321, writeid);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("initiated", compacts.get(0).getState());
    Assert.assertEquals("cmomwbv", compacts.get(0).getTablename());
    Assert.assertEquals(CompactionType.MAJOR, compacts.get(0).getType());
  }

  @Test
  public void enoughDeltasNoBase() throws Exception {
    Table t = newTable("default", "ednb", true);
    Partition p = newPartition(t, "today");

    addDeltaFile(t, p, 1L, 201L, 200);
    addDeltaFile(t, p, 202L, 202L, 1);
    addDeltaFile(t, p, 203L, 203L, 1);
    addDeltaFile(t, p, 204L, 204L, 1);
    addDeltaFile(t, p, 205L, 205L, 1);
    addDeltaFile(t, p, 206L, 206L, 1);
    addDeltaFile(t, p, 207L, 207L, 1);
    addDeltaFile(t, p, 208L, 208L, 1);
    addDeltaFile(t, p, 209L, 209L, 1);
    addDeltaFile(t, p, 210L, 210L, 1);
    addDeltaFile(t, p, 211L, 211L, 1);

    burnThroughTransactions("default", "ednb", 210);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.PARTITION, "default");
    comp.setTablename("ednb");
    comp.setPartitionname("ds=today");
    comp.setOperationType(DataOperationType.DELETE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    long writeid = allocateWriteId("default", "ednb", txnid);
    Assert.assertEquals(211, writeid);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("initiated", compacts.get(0).getState());
    Assert.assertEquals("ednb", compacts.get(0).getTablename());
    Assert.assertEquals("ds=today", compacts.get(0).getPartitionname());
    Assert.assertEquals(CompactionType.MAJOR, compacts.get(0).getType());
  }

  @Test
  public void twoTxnsOnSamePartitionGenerateOneCompactionRequest() throws Exception {
    Table t = newTable("default", "ttospgocr", true);
    Partition p = newPartition(t, "today");

    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 22L, 2);
    addDeltaFile(t, p, 23L, 24L, 2);

    burnThroughTransactions("default", "ttospgocr", 23);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.PARTITION, "default");
    comp.setTablename("ttospgocr");
    comp.setPartitionname("ds=today");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    long writeid = allocateWriteId("default", "ttospgocr", txnid);
    Assert.assertEquals(24, writeid);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    txnid = openTxn();
    comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.PARTITION, "default");
    comp.setTablename("ttospgocr");
    comp.setPartitionname("ds=today");
    comp.setOperationType(DataOperationType.UPDATE);
    components = new ArrayList<LockComponent>(1);
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    res = txnHandler.lock(req);
    writeid = allocateWriteId("default", "ttospgocr", txnid);
    Assert.assertEquals(25, writeid);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("initiated", compacts.get(0).getState());
    Assert.assertEquals("ttospgocr", compacts.get(0).getTablename());
    Assert.assertEquals("ds=today", compacts.get(0).getPartitionname());
    Assert.assertEquals(CompactionType.MAJOR, compacts.get(0).getType());
  }

  @Test
  public void noCompactTableDynamicPartitioning() throws Exception {
    Table t = newTable("default", "nctdp", true);
    Partition p = newPartition(t, "today");

    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 22L, 2);
    addDeltaFile(t, p, 23L, 24L, 2);

    burnThroughTransactions("default", "nctdp", 23);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
    comp.setTablename("nctdp");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    long writeid = allocateWriteId("default", "nctdp", txnid);
    Assert.assertEquals(24, writeid);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(0, compacts.size());
  }

  @Test
  public void dropTable() throws Exception {
    Table t = newTable("default", "dt", false);

    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);

    burnThroughTransactions("default", "dt", 23);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.PARTITION, "default");
    comp.setTablename("dt");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    long writeid = allocateWriteId("default", "dt", txnid);
    Assert.assertEquals(24, writeid);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    ms.dropTable("default", "dt");

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(0, compacts.size());
  }

  @Test
  public void dropPartition() throws Exception {
    Table t = newTable("default", "dp", true);
    Partition p = newPartition(t, "today");

    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 22L, 2);
    addDeltaFile(t, p, 23L, 24L, 2);

    burnThroughTransactions("default", "dp", 23);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.PARTITION, "default");
    comp.setTablename("dp");
    comp.setPartitionname("ds=today");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    long writeid = allocateWriteId("default", "dp", txnid);
    Assert.assertEquals(24, writeid);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    ms.dropPartition("default", "dp", Collections.singletonList("today"), true);

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(0, compacts.size());
  }

  @Test
  public void processCompactionCandidatesInParallel() throws Exception {
    Table t = newTable("default", "dp", true);
    List<LockComponent> components = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      Partition p = newPartition(t, "part" + (i + 1));
      addBaseFile(t, p, 20L, 20);
      addDeltaFile(t, p, 21L, 22L, 2);
      addDeltaFile(t, p, 23L, 24L, 2);

      LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.PARTITION, "default");
      comp.setTablename("dp");
      comp.setPartitionname("ds=part" + (i + 1));
      comp.setOperationType(DataOperationType.UPDATE);
      components.add(comp);
    }
    burnThroughTransactions("default", "dp", 23);
    long txnid = openTxn();

    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    Assert.assertEquals(LockState.ACQUIRED, res.getState());

    long writeid = allocateWriteId("default", "dp", txnid);
    Assert.assertEquals(24, writeid);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_REQUEST_QUEUE, 3);
    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(10, compacts.size());
  }

  @Test
  public void testInitiatorMetricsEnabled() throws Exception {
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED, true);
    Metrics.initialize(conf);
    int originalValue = Metrics.getOrCreateGauge(INITIATED_METRICS_KEY).intValue();
    Table t = newTable("default", "ime", true);
    List<LockComponent> components = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      Partition p = newPartition(t, "part" + (i + 1));
      addBaseFile(t, p, 20L, 20);
      addDeltaFile(t, p, 21L, 22L, 2);
      addDeltaFile(t, p, 23L, 24L, 2);

      LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.PARTITION, "default");
      comp.setTablename("ime");
      comp.setPartitionname("ds=part" + (i + 1));
      comp.setOperationType(DataOperationType.UPDATE);
      components.add(comp);
    }
    burnThroughTransactions("default", "ime", 23);
    long txnid = openTxn();

    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    Assert.assertEquals(LockState.ACQUIRED, res.getState());

    long writeid = allocateWriteId("default", "ime", txnid);
    Assert.assertEquals(24, writeid);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(10, compacts.size());

    // The metrics will appear after the next Initiator run
    startInitiator();

    Assert.assertEquals(originalValue + 10,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_STATUS_PREFIX + TxnStore.INITIATED_RESPONSE).intValue());
  }

  @Test
  public void testInitiatorMetricsDisabled() throws Exception {
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED, false);
    Metrics.initialize(conf);
    int originalValue = Metrics.getOrCreateGauge(INITIATED_METRICS_KEY).intValue();
    Table t = newTable("default", "imd", true);
    List<LockComponent> components = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      Partition p = newPartition(t, "part" + (i + 1));
      addBaseFile(t, p, 20L, 20);
      addDeltaFile(t, p, 21L, 22L, 2);
      addDeltaFile(t, p, 23L, 24L, 2);

      LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.PARTITION, "default");
      comp.setTablename("imd");
      comp.setPartitionname("ds=part" + (i + 1));
      comp.setOperationType(DataOperationType.UPDATE);
      components.add(comp);
    }
    burnThroughTransactions("default", "imd", 23);
    long txnid = openTxn();

    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    Assert.assertEquals(LockState.ACQUIRED, res.getState());

    long writeid = allocateWriteId("default", "imd", txnid);
    Assert.assertEquals(24, writeid);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(10, compacts.size());

    // The metrics will appear after the next Initiator run
    startInitiator();

    Assert.assertEquals(originalValue,
        Metrics.getOrCreateGauge(INITIATED_METRICS_KEY).intValue());
  }

  @Test
  public void testUpdateCompactionMetrics() {
    Metrics.initialize(conf);
    ShowCompactResponse scr = new ShowCompactResponse();
    List<ShowCompactResponseElement> elements = new ArrayList<>();
    elements.add(generateElement(1,"db", "tb", null, CompactionType.MAJOR, TxnStore.FAILED_RESPONSE));
    // Check for overwrite
    elements.add(generateElement(2,"db", "tb", null, CompactionType.MAJOR, TxnStore.INITIATED_RESPONSE));
    elements.add(generateElement(3,"db", "tb2", null, CompactionType.MINOR, TxnStore.INITIATED_RESPONSE));
    elements.add(generateElement(5,"db", "tb3", "p1", CompactionType.MINOR, TxnStore.DID_NOT_INITIATE_RESPONSE));
    // Check for overwrite where the order is different
    elements.add(generateElement(4,"db", "tb3", "p1", CompactionType.MINOR, TxnStore.FAILED_RESPONSE));

    elements.add(generateElement(6,"db1", "tb", null, CompactionType.MINOR, TxnStore.FAILED_RESPONSE));
    elements.add(generateElement(7,"db1", "tb2", null, CompactionType.MINOR, TxnStore.FAILED_RESPONSE));
    elements.add(generateElement(8,"db1", "tb3", null, CompactionType.MINOR, TxnStore.FAILED_RESPONSE));

    elements.add(generateElement(9,"db2", "tb", null, CompactionType.MINOR, TxnStore.SUCCEEDED_RESPONSE));
    elements.add(generateElement(10,"db2", "tb2", null, CompactionType.MINOR, TxnStore.SUCCEEDED_RESPONSE));
    elements.add(generateElement(11,"db2", "tb3", null, CompactionType.MINOR, TxnStore.SUCCEEDED_RESPONSE));
    elements.add(generateElement(12,"db2", "tb4", null, CompactionType.MINOR, TxnStore.SUCCEEDED_RESPONSE));

    elements.add(generateElement(13,"db3", "tb3", null, CompactionType.MINOR, TxnStore.WORKING_RESPONSE));
    elements.add(generateElement(14,"db3", "tb4", null, CompactionType.MINOR, TxnStore.WORKING_RESPONSE));
    elements.add(generateElement(15,"db3", "tb5", null, CompactionType.MINOR, TxnStore.WORKING_RESPONSE));
    elements.add(generateElement(16,"db3", "tb6", null, CompactionType.MINOR, TxnStore.WORKING_RESPONSE));
    elements.add(generateElement(17,"db3", "tb7", null, CompactionType.MINOR, TxnStore.WORKING_RESPONSE));

    scr.setCompacts(elements);
    Initiator.updateCompactionMetrics(scr);

    Assert.assertEquals(1,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_STATUS_PREFIX + TxnStore.DID_NOT_INITIATE_RESPONSE).intValue());
    Assert.assertEquals(2,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_STATUS_PREFIX + TxnStore.INITIATED_RESPONSE).intValue());
    Assert.assertEquals(3,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_STATUS_PREFIX + TxnStore.FAILED_RESPONSE).intValue());
    Assert.assertEquals(4,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_STATUS_PREFIX + TxnStore.SUCCEEDED_RESPONSE).intValue());
    Assert.assertEquals(5,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_STATUS_PREFIX + TxnStore.WORKING_RESPONSE).intValue());
    Assert.assertEquals(0,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_STATUS_PREFIX + TxnStore.CLEANING_RESPONSE).intValue());
  }

  @Test
  public void testAgeMetricsNotSet() {
    Metrics.initialize(conf);
    ShowCompactResponse scr = new ShowCompactResponse();
    List<ShowCompactResponseElement> elements = new ArrayList<>();
    elements.add(generateElement(1, "db", "tb", null, CompactionType.MAJOR, TxnStore.FAILED_RESPONSE, 1L));
    elements.add(generateElement(5, "db", "tb3", "p1", CompactionType.MINOR, TxnStore.DID_NOT_INITIATE_RESPONSE, 2L));
    elements.add(generateElement(9, "db2", "tb", null, CompactionType.MINOR, TxnStore.SUCCEEDED_RESPONSE, 3L));
    elements.add(generateElement(13, "db3", "tb3", null, CompactionType.MINOR, TxnStore.WORKING_RESPONSE, 4L));
    elements.add(generateElement(14, "db3", "tb4", null, CompactionType.MINOR, TxnStore.CLEANING_RESPONSE, 5L));

    scr.setCompacts(elements);
    Initiator.updateCompactionMetrics(scr);
    // Check that it is not set
    Assert.assertEquals(0, Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_OLDEST_ENQUEUE_AGE).intValue());
  }

  @Test
  public void testAgeMetricsAge() {
    Metrics.initialize(conf);
    ShowCompactResponse scr = new ShowCompactResponse();
    List<ShowCompactResponseElement> elements = new ArrayList<>();
    long start = System.currentTimeMillis() - 1000L;
    elements.add(generateElement(15,"db3", "tb5", null, CompactionType.MINOR, TxnStore.INITIATED_RESPONSE, start));

    scr.setCompacts(elements);
    Initiator.updateCompactionMetrics(scr);
    long diff = (System.currentTimeMillis() - start)/1000;
    // Check that we have at least 1s old compaction age, but not more than expected
    Assert.assertTrue(Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_OLDEST_ENQUEUE_AGE).intValue() <= diff);
    Assert.assertTrue(Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_OLDEST_ENQUEUE_AGE).intValue() >= 1);
  }

  @Test
  public void testAgeMetricsOrder() {
    Metrics.initialize(conf);
    ShowCompactResponse scr = new ShowCompactResponse();
    long start = System.currentTimeMillis();
    List<ShowCompactResponseElement> elements = new ArrayList<>();
    elements.add(generateElement(15,"db3", "tb5", null, CompactionType.MINOR, TxnStore.INITIATED_RESPONSE,
        start - 1000L));
    elements.add(generateElement(16,"db3", "tb6", null, CompactionType.MINOR, TxnStore.INITIATED_RESPONSE,
        start - 100000L));

    scr.setCompacts(elements);
    Initiator.updateCompactionMetrics(scr);
    // Check that the age is older than 10s
    Assert.assertTrue(Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_OLDEST_ENQUEUE_AGE).intValue() > 10);

    // Check the reverse order
    elements = new ArrayList<>();
    elements.add(generateElement(16,"db3", "tb6", null, CompactionType.MINOR, TxnStore.INITIATED_RESPONSE,
        start - 100000L));
    elements.add(generateElement(15,"db3", "tb5", null, CompactionType.MINOR, TxnStore.INITIATED_RESPONSE,
        start - 1000L));

    // Check that the age is older than 10s
    Assert.assertTrue(Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_OLDEST_ENQUEUE_AGE).intValue() > 10);
  }

  private ShowCompactResponseElement generateElement(long id, String db, String table, String partition,
      CompactionType type, String state) {
    return generateElement(id, db, table, partition, type, state, System.currentTimeMillis());
  }

  private ShowCompactResponseElement generateElement(long id, String db, String table, String partition,
      CompactionType type, String state, long enqueueTime) {
    ShowCompactResponseElement element = new ShowCompactResponseElement(db, table, type, state);
    element.setId(id);
    element.setPartitionname(partition);
    element.setEnqueueTime(enqueueTime);
    return element;
  }

  @Test
  public void compactTableWithMultipleBase() throws Exception {
    Table t = newTable("default", "nctdpnhe", false);

    addBaseFile(t, null, 50L, 50);
    addBaseFile(t, null, 100L, 50);

    burnThroughTransactions("default", "nctdpnhe", 102);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
    comp.setTablename("nctdpnhe");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    long writeid = allocateWriteId("default", "nctdpnhe", txnid);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals("initiated",rsp.getCompacts().get(0).getState());

    startWorker();
    Thread.sleep(1L);
    ShowCompactResponse response = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("ready for cleaning",response.getCompacts().get(0).getState());
  }

  /**
   * Tests org.apache.hadoop.hive.ql.txn.compactor.CompactorThread#findUserToRunAs(java.lang.String, org.apache.hadoop
   * .hive.metastore.api.Table).
   * Used by Worker and Initiator.
   * Initiator caches this via Initiator#resolveUserToRunAs.
   * @throws Exception
   */
  @Test
  public void testFindUserToRunAs() throws Exception {
    Table t = newTable("default", "tfutra", false);

    CompactorThread initiator = new Initiator();
    initiator.setConf(conf);
    
    String userFromConf = "randomUser123";

    // user set in config
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.COMPACTOR_RUN_AS_USER, userFromConf);
    initiator.setConf(conf);
    Assert.assertEquals(userFromConf, initiator.findUserToRunAs(t.getSd().getLocation(), t));

    // table dir owner (is probably not "randomUser123")
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.COMPACTOR_RUN_AS_USER, "");
    // simulate restarting Initiator
    initiator.setConf(conf);
    Assert.assertNotEquals(userFromConf, initiator.findUserToRunAs(t.getSd().getLocation(), t));
  }

  /**
   * Tests org.apache.hadoop.hive.ql.txn.compactor.Initiator#resolveUserToRunAs(java.util.Map, 
   * org.apache.hadoop.hive.metastore.api.Table, org.apache.hadoop.hive.metastore.api.Partition)
   * Used by Initiator only.
   * @throws Exception
   */
  @Test
  public void resolveUserToRunAs() throws Exception {
    Table t = newTable("default", "tfutra", false);

    Map<String, String> tblNameOwners = new HashMap<>();
    Initiator initiator = new Initiator();

    String userFromConf = "randomUser123";

    // user set in config
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.COMPACTOR_RUN_AS_USER, userFromConf);
    initiator.setConf(conf);
    Assert.assertEquals(userFromConf, initiator.resolveUserToRunAs(tblNameOwners, t, null));

    
    // table dir owner (is probably not "randomUser123")
    // config changes can happen on Initiator restart; a restart would clear cache
    tblNameOwners = new HashMap<>();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.COMPACTOR_RUN_AS_USER, "");
    initiator.setConf(conf);
    Assert.assertNotEquals(userFromConf, initiator.resolveUserToRunAs(tblNameOwners, t, null));
    // table dir owner again, retrieved from cache
    Assert.assertNotEquals(userFromConf, initiator.resolveUserToRunAs(tblNameOwners, t, null));
  }

  @Test public void testInitiatorFailure() throws Exception {
    String tableName = "my_table";
    Table t = newTable("default", tableName, false);

    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD, 1);

    // 2 aborts
    for (int i = 0; i < 2; i++) {
      long txnid = openTxn();
      LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
      comp.setTablename(tableName);
      comp.setOperationType(DataOperationType.UPDATE);
      List<LockComponent> components = new ArrayList<LockComponent>(1);
      components.add(comp);
      LockRequest req = new LockRequest(components, "me", "localhost");
      req.setTxnid(txnid);
      LockResponse res = txnHandler.lock(req);
      txnHandler.abortTxn(new AbortTxnRequest(txnid));
    }

    // run and fail initiator
    Initiator initiator = Mockito.spy(new Initiator());
    initiator.setThreadId((int) t.getId());
    initiator.setConf(conf);
    initiator.init(new AtomicBoolean(true));
    doThrow(new RuntimeException("This was thrown on purpose by testInitiatorFailure"))
        .when(initiator).resolveTable(any());
    initiator.run();

    // verify status of table compaction
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("did not initiate", compacts.get(0).getState());
    Assert.assertEquals(tableName, compacts.get(0).getTablename());
  }

  @Test
  public void noCompactForInsertOnly() throws Exception {
    Map<String, String> parameters = new HashMap<String, String>(1);
    parameters.put(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES,
        TransactionalValidationListener.INSERTONLY_TRANSACTIONAL_PROPERTY);
    newTable("default", "ncfio", false, parameters);

    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_COMPACT_MM, false);
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD, 1);

    // 2 aborts
    for (int i = 0; i < 2; i++) {
      long txnid = openTxn();
      LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
      comp.setTablename("ncfio");
      comp.setOperationType(DataOperationType.UPDATE);
      List<LockComponent> components = new ArrayList<LockComponent>(1);
      components.add(comp);
      LockRequest req = new LockRequest(components, "me", "localhost");
      req.setTxnid(txnid);
      txnHandler.lock(req);
      txnHandler.abortTxn(new AbortTxnRequest(txnid));
    }

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(0, rsp.getCompactsSize());
  }

  @Override
  boolean useHive130DeltaDirName() {
    return false;
  }

  @After
  public void tearDown() throws Exception {
    compactorTestCleanup();
  }
}
