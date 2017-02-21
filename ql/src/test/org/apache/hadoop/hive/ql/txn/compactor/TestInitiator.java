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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests for the compactor Initiator thread.
 */
public class TestInitiator extends CompactorTest {
  static final private String CLASS_NAME = TestInitiator.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  public TestInitiator() throws Exception {
    super();
  }

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

  @Test
  public void cleanEmptyAbortedTxns() throws Exception {
    // Test that we are cleaning aborted transactions with no components left in txn_components.
    // Put one aborted transaction with an entry in txn_components to make sure we don't
    // accidently clean it too.
    Table t = newTable("default", "ceat", false);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
    comp.setTablename("ceat");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    txnHandler.abortTxn(new AbortTxnRequest(txnid));

    for (int i = 0; i < TxnStore.TIMED_OUT_TXN_ABORT_BATCH_SIZE  + 50; i++) {
      txnid = openTxn();
      txnHandler.abortTxn(new AbortTxnRequest(txnid));
    }
    GetOpenTxnsResponse openTxns = txnHandler.getOpenTxns();
    Assert.assertEquals(TxnStore.TIMED_OUT_TXN_ABORT_BATCH_SIZE + 50 + 1, openTxns.getOpen_txnsSize());

    startInitiator();

    openTxns = txnHandler.getOpenTxns();
    Assert.assertEquals(1, openTxns.getOpen_txnsSize());
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

    burnThroughTransactions(23);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
    comp.setTablename("cthdp");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
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

    burnThroughTransactions(23);

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
  public void noCompactTableDeltaPctNotHighEnough() throws Exception {
    Table t = newTable("default", "nctdpnhe", false);

    addBaseFile(t, null, 50L, 50);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);

    burnThroughTransactions(53);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
    comp.setTablename("nctdpnhe");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
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

    burnThroughTransactions(210);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
    comp.setTablename("cttmd");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
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

    burnThroughTransactions(210);

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

    burnThroughTransactions(210);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
    comp.setTablename("nctned");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
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

    burnThroughTransactions(320);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
    comp.setTablename("cmomwbv");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
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

    burnThroughTransactions(210);

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

    burnThroughTransactions(23);

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

    burnThroughTransactions(23);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
    comp.setTablename("nctdp");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
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

    burnThroughTransactions(23);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.PARTITION, "default");
    comp.setTablename("dt");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
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

    burnThroughTransactions(23);

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
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    ms.dropPartition("default", "dp", Collections.singletonList("today"), true);

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(0, compacts.size());
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
