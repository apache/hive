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

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for the compactor Initiator thread.
 */
public class TestInitiator extends CompactorTest {
  static final private String CLASS_NAME = TestInitiator.class.getName();
  static final private Log LOG = LogFactory.getLog(CLASS_NAME);

  public TestInitiator() throws Exception {
    super();
  }

  @Test
  public void nothing() throws Exception {
    // Test that the whole things works when there's nothing in the queue.  This is just a
    // survival test.
    startInitiator(new HiveConf());
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

    startInitiator(new HiveConf());

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

    HiveConf conf = new HiveConf();
    HiveConf.setLongVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_WORKER_TIMEOUT, 1L);

    startInitiator(conf);

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("initiated", compacts.get(0).getState());
  }

  @Test
  public void majorCompactOnTableTooManyAborts() throws Exception {
    Table t = newTable("default", "mcottma", false);

    HiveConf conf = new HiveConf();
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD, 10);

    for (int i = 0; i < 11; i++) {
      long txnid = openTxn();
      LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE,  "default");
      comp.setTablename("mcottma");
      List<LockComponent> components = new ArrayList<LockComponent>(1);
      components.add(comp);
      LockRequest req = new LockRequest(components, "me", "localhost");
      req.setTxnid(txnid);
      LockResponse res = txnHandler.lock(req);
      txnHandler.abortTxn(new AbortTxnRequest(txnid));
    }

    startInitiator(conf);

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

    HiveConf conf = new HiveConf();
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD, 10);

    for (int i = 0; i < 11; i++) {
      long txnid = openTxn();
      LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
      comp.setTablename("mcoptma");
      comp.setPartitionname("ds=today");
      List<LockComponent> components = new ArrayList<LockComponent>(1);
      components.add(comp);
      LockRequest req = new LockRequest(components, "me", "localhost");
      req.setTxnid(txnid);
      LockResponse res = txnHandler.lock(req);
      txnHandler.abortTxn(new AbortTxnRequest(txnid));
    }

    startInitiator(conf);

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

    HiveConf conf = new HiveConf();
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD, 10);

    for (int i = 0; i < 11; i++) {
      long txnid = openTxn();
      LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE,  "default");
      comp.setTablename("ncomdpa");
      comp.setPartitionname("ds=day-" + i);
      List<LockComponent> components = new ArrayList<LockComponent>(1);
      components.add(comp);
      LockRequest req = new LockRequest(components, "me", "localhost");
      req.setTxnid(txnid);
      LockResponse res = txnHandler.lock(req);
      txnHandler.abortTxn(new AbortTxnRequest(txnid));
    }

    startInitiator(conf);

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertNull(rsp.getCompacts());
  }

  @Test
  public void cleanEmptyAbortedTxns() throws Exception {
    // Test that we are cleaning aborted transactions with no components left in txn_components.
    // Put one aborted transaction with an entry in txn_components to make sure we don't
    // accidently clean it too.
    Table t = newTable("default", "ceat", false);

    HiveConf conf = new HiveConf();

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
    comp.setTablename("ceat");
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    txnHandler.abortTxn(new AbortTxnRequest(txnid));

    for (int i = 0; i < 100; i++) {
      txnid = openTxn();
      txnHandler.abortTxn(new AbortTxnRequest(txnid));
    }
    GetOpenTxnsResponse openTxns = txnHandler.getOpenTxns();
    Assert.assertEquals(101, openTxns.getOpen_txnsSize());

    startInitiator(conf);

    openTxns = txnHandler.getOpenTxns();
    Assert.assertEquals(1, openTxns.getOpen_txnsSize());
  }

  @Test
  public void noCompactWhenNoCompactSet() throws Exception {
    Map<String, String> parameters = new HashMap<String, String>(1);
    parameters.put("NO_AUTO_COMPACTION", "true");
    Table t = newTable("default", "ncwncs", false, parameters);

    HiveConf conf = new HiveConf();
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD, 10);

    for (int i = 0; i < 11; i++) {
      long txnid = openTxn();
      LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
      comp.setTablename("ncwncs");
      List<LockComponent> components = new ArrayList<LockComponent>(1);
      components.add(comp);
      LockRequest req = new LockRequest(components, "me", "localhost");
      req.setTxnid(txnid);
      LockResponse res = txnHandler.lock(req);
      txnHandler.abortTxn(new AbortTxnRequest(txnid));
    }

    startInitiator(conf);

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertNull(rsp.getCompacts());
  }

  @Test
  public void noCompactWhenCompactAlreadyScheduled() throws Exception {
    Table t = newTable("default", "ncwcas", false);

    HiveConf conf = new HiveConf();
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD, 10);

    for (int i = 0; i < 11; i++) {
      long txnid = openTxn();
      LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE,  "default");
      comp.setTablename("ncwcas");
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

    startInitiator(conf);

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

    HiveConf conf = new HiveConf();

    addBaseFile(conf, t, null, 20L, 20);
    addDeltaFile(conf, t, null, 21L, 22L, 2);
    addDeltaFile(conf, t, null, 23L, 24L, 2);

    burnThroughTransactions(23);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
    comp.setTablename("cthdp");
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    startInitiator(conf);

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

    HiveConf conf = new HiveConf();

    addBaseFile(conf, t, p, 20L, 20);
    addDeltaFile(conf, t, p, 21L, 22L, 2);
    addDeltaFile(conf, t, p, 23L, 24L, 2);

    burnThroughTransactions(23);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.PARTITION, "default");
    comp.setTablename("cphdp");
    comp.setPartitionname("ds=today");
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    startInitiator(conf);

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

    HiveConf conf = new HiveConf();

    addBaseFile(conf, t, null, 50L, 50);
    addDeltaFile(conf, t, null, 21L, 22L, 2);
    addDeltaFile(conf, t, null, 23L, 24L, 2);

    burnThroughTransactions(53);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
    comp.setTablename("nctdpnhe");
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    startInitiator(conf);

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertNull(rsp.getCompacts());
  }

  @Test
  public void compactTableTooManyDeltas() throws Exception {
    Table t = newTable("default", "cttmd", false);

    HiveConf conf = new HiveConf();

    addBaseFile(conf, t, null, 200L, 200);
    addDeltaFile(conf, t, null, 201L, 201L, 1);
    addDeltaFile(conf, t, null, 202L, 202L, 1);
    addDeltaFile(conf, t, null, 203L, 203L, 1);
    addDeltaFile(conf, t, null, 204L, 204L, 1);
    addDeltaFile(conf, t, null, 205L, 205L, 1);
    addDeltaFile(conf, t, null, 206L, 206L, 1);
    addDeltaFile(conf, t, null, 207L, 207L, 1);
    addDeltaFile(conf, t, null, 208L, 208L, 1);
    addDeltaFile(conf, t, null, 209L, 209L, 1);
    addDeltaFile(conf, t, null, 210L, 210L, 1);
    addDeltaFile(conf, t, null, 211L, 211L, 1);

    burnThroughTransactions(210);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
    comp.setTablename("cttmd");
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    startInitiator(conf);

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

    HiveConf conf = new HiveConf();

    addBaseFile(conf, t, p, 200L, 200);
    addDeltaFile(conf, t, p, 201L, 201L, 1);
    addDeltaFile(conf, t, p, 202L, 202L, 1);
    addDeltaFile(conf, t, p, 203L, 203L, 1);
    addDeltaFile(conf, t, p, 204L, 204L, 1);
    addDeltaFile(conf, t, p, 205L, 205L, 1);
    addDeltaFile(conf, t, p, 206L, 206L, 1);
    addDeltaFile(conf, t, p, 207L, 207L, 1);
    addDeltaFile(conf, t, p, 208L, 208L, 1);
    addDeltaFile(conf, t, p, 209L, 209L, 1);
    addDeltaFile(conf, t, p, 210L, 210L, 1);
    addDeltaFile(conf, t, p, 211L, 211L, 1);

    burnThroughTransactions(210);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.PARTITION, "default");
    comp.setTablename("cptmd");
    comp.setPartitionname("ds=today");
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    startInitiator(conf);

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

    HiveConf conf = new HiveConf();

    addBaseFile(conf, t, null, 200L, 200);
    addDeltaFile(conf, t, null, 201L, 205L, 5);
    addDeltaFile(conf, t, null, 206L, 211L, 6);

    burnThroughTransactions(210);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
    comp.setTablename("nctned");
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    startInitiator(conf);

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertNull(rsp.getCompacts());
  }

  @Test
  public void chooseMajorOverMinorWhenBothValid() throws Exception {
    Table t = newTable("default", "cmomwbv", false);

    HiveConf conf = new HiveConf();

    addBaseFile(conf, t, null, 200L, 200);
    addDeltaFile(conf, t, null, 201L, 211L, 11);
    addDeltaFile(conf, t, null, 212L, 222L, 11);
    addDeltaFile(conf, t, null, 223L, 233L, 11);
    addDeltaFile(conf, t, null, 234L, 244L, 11);
    addDeltaFile(conf, t, null, 245L, 255L, 11);
    addDeltaFile(conf, t, null, 256L, 266L, 11);
    addDeltaFile(conf, t, null, 267L, 277L, 11);
    addDeltaFile(conf, t, null, 278L, 288L, 11);
    addDeltaFile(conf, t, null, 289L, 299L, 11);
    addDeltaFile(conf, t, null, 300L, 310L, 11);
    addDeltaFile(conf, t, null, 311L, 321L, 11);

    burnThroughTransactions(320);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
    comp.setTablename("cmomwbv");
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    startInitiator(conf);

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

    HiveConf conf = new HiveConf();

    addDeltaFile(conf, t, p, 1L, 201L, 200);
    addDeltaFile(conf, t, p, 202L, 202L, 1);
    addDeltaFile(conf, t, p, 203L, 203L, 1);
    addDeltaFile(conf, t, p, 204L, 204L, 1);
    addDeltaFile(conf, t, p, 205L, 205L, 1);
    addDeltaFile(conf, t, p, 206L, 206L, 1);
    addDeltaFile(conf, t, p, 207L, 207L, 1);
    addDeltaFile(conf, t, p, 208L, 208L, 1);
    addDeltaFile(conf, t, p, 209L, 209L, 1);
    addDeltaFile(conf, t, p, 210L, 210L, 1);
    addDeltaFile(conf, t, p, 211L, 211L, 1);

    burnThroughTransactions(210);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.PARTITION, "default");
    comp.setTablename("ednb");
    comp.setPartitionname("ds=today");
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    startInitiator(conf);

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

    HiveConf conf = new HiveConf();

    addBaseFile(conf, t, p, 20L, 20);
    addDeltaFile(conf, t, p, 21L, 22L, 2);
    addDeltaFile(conf, t, p, 23L, 24L, 2);

    burnThroughTransactions(23);

    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.PARTITION, "default");
    comp.setTablename("ttospgocr");
    comp.setPartitionname("ds=today");
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
    components = new ArrayList<LockComponent>(1);
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    res = txnHandler.lock(req);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    startInitiator(conf);

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("initiated", compacts.get(0).getState());
    Assert.assertEquals("ttospgocr", compacts.get(0).getTablename());
    Assert.assertEquals("ds=today", compacts.get(0).getPartitionname());
    Assert.assertEquals(CompactionType.MAJOR, compacts.get(0).getType());
  }

  // TODO test compactions with legacy file types

  @Before
  public void setUpTxnDb() throws Exception {
    TxnDbUtil.setConfValues(new HiveConf());
  }
}
