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
package org.apache.hadoop.hive.metastore.txn;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static junit.framework.Assert.*;

/**
 * Tests for TxnHandler.
 */
public class TestCompactionTxnHandler {

  private HiveConf conf = new HiveConf();
  private CompactionTxnHandler txnHandler;

  public TestCompactionTxnHandler() throws Exception {
    TxnDbUtil.setConfValues(conf);
    LogManager.getLogger(TxnHandler.class.getName()).setLevel(Level.DEBUG);
    tearDown();
  }

  @Test
  public void testFindNextToCompact() throws Exception {
    CompactionRequest rqst = new CompactionRequest("foo", "bar", CompactionType.MINOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);
    long now = System.currentTimeMillis();
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    assertNotNull(ci);
    assertEquals("foo", ci.dbname);
    assertEquals("bar", ci.tableName);
    assertEquals("ds=today", ci.partName);
    assertEquals(CompactionType.MINOR, ci.type);
    assertNull(ci.runAs);
    assertNull(txnHandler.findNextToCompact("fred"));

    txnHandler.setRunAs(ci.id, "bob");

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    assertEquals(1, compacts.size());
    ShowCompactResponseElement c = compacts.get(0);
    assertEquals("foo", c.getDbname());
    assertEquals("bar", c.getTablename());
    assertEquals("ds=today", c.getPartitionname());
    assertEquals(CompactionType.MINOR, c.getType());
    assertEquals("working", c.getState());
    assertTrue(c.getStart() - 5000 < now && c.getStart() + 5000 > now);
    assertEquals("fred", c.getWorkerid());
    assertEquals("bob", c.getRunAs());
  }

  @Test
  public void testFindNextToCompact2() throws Exception {
    CompactionRequest rqst = new CompactionRequest("foo", "bar", CompactionType.MINOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);

    rqst = new CompactionRequest("foo", "bar", CompactionType.MINOR);
    rqst.setPartitionname("ds=yesterday");
    txnHandler.compact(rqst);

    long now = System.currentTimeMillis();
    boolean expectToday = false;
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    assertNotNull(ci);
    assertEquals("foo", ci.dbname);
    assertEquals("bar", ci.tableName);
    if ("ds=today".equals(ci.partName)) expectToday = false;
    else if ("ds=yesterday".equals(ci.partName)) expectToday = true;
    else fail("partition name should have been today or yesterday but was " + ci.partName);
    assertEquals(CompactionType.MINOR, ci.type);

    ci = txnHandler.findNextToCompact("fred");
    assertNotNull(ci);
    assertEquals("foo", ci.dbname);
    assertEquals("bar", ci.tableName);
    if (expectToday) assertEquals("ds=today", ci.partName);
    else assertEquals("ds=yesterday", ci.partName);
    assertEquals(CompactionType.MINOR, ci.type);

    assertNull(txnHandler.findNextToCompact("fred"));

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    assertEquals(2, compacts.size());
    for (ShowCompactResponseElement e : compacts) {
      assertEquals("working", e.getState());
      assertTrue(e.getStart() - 5000 < now && e.getStart() + 5000 > now);
      assertEquals("fred", e.getWorkerid());
    }
  }

  @Test
  public void testFindNextToCompactNothingToCompact() throws Exception {
    assertNull(txnHandler.findNextToCompact("fred"));
  }

  @Test
  public void testMarkCompacted() throws Exception {
    CompactionRequest rqst = new CompactionRequest("foo", "bar", CompactionType.MINOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    assertNotNull(ci);

    txnHandler.markCompacted(ci);
    assertNull(txnHandler.findNextToCompact("fred"));



    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    assertEquals(1, compacts.size());
    ShowCompactResponseElement c = compacts.get(0);
    assertEquals("foo", c.getDbname());
    assertEquals("bar", c.getTablename());
    assertEquals("ds=today", c.getPartitionname());
    assertEquals(CompactionType.MINOR, c.getType());
    assertEquals("ready for cleaning", c.getState());
    assertNull(c.getWorkerid());
  }

  @Test
  public void testFindNextToClean() throws Exception {
    CompactionRequest rqst = new CompactionRequest("foo", "bar", CompactionType.MINOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);
    assertEquals(0, txnHandler.findReadyToClean().size());
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    assertNotNull(ci);

    assertEquals(0, txnHandler.findReadyToClean().size());
    txnHandler.markCompacted(ci);
    assertNull(txnHandler.findNextToCompact("fred"));

    List<CompactionInfo> toClean = txnHandler.findReadyToClean();
    assertEquals(1, toClean.size());
    assertNull(txnHandler.findNextToCompact("fred"));

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    assertEquals(1, compacts.size());
    ShowCompactResponseElement c = compacts.get(0);
    assertEquals("foo", c.getDbname());
    assertEquals("bar", c.getTablename());
    assertEquals("ds=today", c.getPartitionname());
    assertEquals(CompactionType.MINOR, c.getType());
    assertEquals("ready for cleaning", c.getState());
    assertNull(c.getWorkerid());
  }

  @Test
  public void testMarkCleaned() throws Exception {
    CompactionRequest rqst = new CompactionRequest("foo", "bar", CompactionType.MINOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);
    assertEquals(0, txnHandler.findReadyToClean().size());
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    assertNotNull(ci);

    assertEquals(0, txnHandler.findReadyToClean().size());
    txnHandler.markCompacted(ci);
    assertNull(txnHandler.findNextToCompact("fred"));

    List<CompactionInfo> toClean = txnHandler.findReadyToClean();
    assertEquals(1, toClean.size());
    assertNull(txnHandler.findNextToCompact("fred"));
    txnHandler.markCleaned(ci);
    assertNull(txnHandler.findNextToCompact("fred"));
    assertEquals(0, txnHandler.findReadyToClean().size());

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    assertEquals(0, rsp.getCompactsSize());
  }

  @Test
  public void testRevokeFromLocalWorkers() throws Exception {
    CompactionRequest rqst = new CompactionRequest("foo", "bar", CompactionType.MINOR);
    txnHandler.compact(rqst);
    rqst = new CompactionRequest("foo", "baz", CompactionType.MINOR);
    txnHandler.compact(rqst);
    rqst = new CompactionRequest("foo", "bazzoo", CompactionType.MINOR);
    txnHandler.compact(rqst);
    assertNotNull(txnHandler.findNextToCompact("fred-193892"));
    assertNotNull(txnHandler.findNextToCompact("bob-193892"));
    assertNotNull(txnHandler.findNextToCompact("fred-193893"));
    txnHandler.revokeFromLocalWorkers("fred");

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    assertEquals(3, compacts.size());
    boolean sawWorkingBob = false;
    int initiatedCount = 0;
    for (ShowCompactResponseElement c : compacts) {
      if (c.getState().equals("working")) {
        assertEquals("bob-193892", c.getWorkerid());
        sawWorkingBob = true;
      } else if (c.getState().equals("initiated")) {
        initiatedCount++;
      } else {
        fail("Unexpected state");
      }
    }
    assertTrue(sawWorkingBob);
    assertEquals(2, initiatedCount);
  }

  @Test
  public void testRevokeTimedOutWorkers() throws Exception {
    CompactionRequest rqst = new CompactionRequest("foo", "bar", CompactionType.MINOR);
    txnHandler.compact(rqst);
    rqst = new CompactionRequest("foo", "baz", CompactionType.MINOR);
    txnHandler.compact(rqst);

    assertNotNull(txnHandler.findNextToCompact("fred-193892"));
    Thread.sleep(200);
    assertNotNull(txnHandler.findNextToCompact("fred-193892"));
    txnHandler.revokeTimedoutWorkers(100);

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    assertEquals(2, compacts.size());
    boolean sawWorking = false, sawInitiated = false;
    for (ShowCompactResponseElement c : compacts) {
      if (c.getState().equals("working"))  sawWorking = true;
      else if (c.getState().equals("initiated")) sawInitiated = true;
      else fail("Unexpected state");
    }
    assertTrue(sawWorking);
    assertTrue(sawInitiated);
  }

  @Test
  public void testLockNoWait() throws Exception {
    // Test that we can acquire the lock alone
     LockComponent comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB,
        "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lockNoWait(req);
    assertTrue(res.getState() == LockState.ACQUIRED);
    txnHandler.unlock(new UnlockRequest(res.getLockid()));

    // test that another lock blocks it
    comp = new LockComponent(LockType.SHARED_READ, LockLevel.DB,
        "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    res = txnHandler.lock(req);
    assertEquals(LockState.ACQUIRED, res.getState());

    comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB,
        "mydb");
    comp.setTablename("mytable");
    comp.setPartitionname("mypartition");
    components.clear();
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    res = txnHandler.lockNoWait(req);
    assertEquals(LockState.NOT_ACQUIRED, res.getState());
    assertEquals(1, TxnDbUtil.findNumCurrentLocks());
  }

  @Test
  public void testFindPotentialCompactions() throws Exception {
    // Test that committing unlocks
    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB,
        "mydb");
    comp.setTablename("mytable");
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB,
        "mydb");
    comp.setTablename("yourtable");
    comp.setPartitionname("mypartition");
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));
    assertEquals(0, txnHandler.numLocksInLockTable());

    Set<CompactionInfo> potentials = txnHandler.findPotentialCompactions(100);
    assertEquals(2, potentials.size());
    boolean sawMyTable = false, sawYourTable = false;
    for (CompactionInfo ci : potentials) {
      sawMyTable |= (ci.dbname.equals("mydb") && ci.tableName.equals("mytable") &&
          ci.partName ==  null);
      sawYourTable |= (ci.dbname.equals("mydb") && ci.tableName.equals("yourtable") &&
          ci.partName.equals("mypartition"));
    }
    assertTrue(sawMyTable);
    assertTrue(sawYourTable);
  }

  // TODO test changes to mark cleaned to clean txns and txn_components

  @Test
  public void testMarkCleanedCleansTxnsAndTxnComponents()
      throws Exception {
    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB,
        "mydb");
    comp.setTablename("mytable");
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);
    txnHandler.abortTxn(new AbortTxnRequest(txnid));

    txnid = openTxn();
    comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("yourtable");
    components = new ArrayList<LockComponent>(1);
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);
    txnHandler.abortTxn(new AbortTxnRequest(txnid));

    txnid = openTxn();
    comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("foo");
    comp.setPartitionname("bar");
    components = new ArrayList<LockComponent>(1);
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);

    comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("foo");
    comp.setPartitionname("baz");
    components = new ArrayList<LockComponent>(1);
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);
    txnHandler.abortTxn(new AbortTxnRequest(txnid));

    CompactionInfo ci = new CompactionInfo();

    // Now clean them and check that they are removed from the count.
    CompactionRequest rqst = new CompactionRequest("mydb", "mytable", CompactionType.MAJOR);
    txnHandler.compact(rqst);
    assertEquals(0, txnHandler.findReadyToClean().size());
    ci = txnHandler.findNextToCompact("fred");
    assertNotNull(ci);
    txnHandler.markCompacted(ci);

    List<CompactionInfo> toClean = txnHandler.findReadyToClean();
    assertEquals(1, toClean.size());
    txnHandler.markCleaned(ci);

    // Check that we are cleaning up the empty aborted transactions
    GetOpenTxnsResponse txnList = txnHandler.getOpenTxns();
    assertEquals(3, txnList.getOpen_txnsSize());
    txnHandler.cleanEmptyAbortedTxns();
    txnList = txnHandler.getOpenTxns();
    assertEquals(2, txnList.getOpen_txnsSize());

    rqst = new CompactionRequest("mydb", "foo", CompactionType.MAJOR);
    rqst.setPartitionname("bar");
    txnHandler.compact(rqst);
    assertEquals(0, txnHandler.findReadyToClean().size());
    ci = txnHandler.findNextToCompact("fred");
    assertNotNull(ci);
    txnHandler.markCompacted(ci);

    toClean = txnHandler.findReadyToClean();
    assertEquals(1, toClean.size());
    txnHandler.markCleaned(ci);

    txnHandler.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    txnHandler.cleanEmptyAbortedTxns();
    txnList = txnHandler.getOpenTxns();
    assertEquals(3, txnList.getOpen_txnsSize());
  }

  @Before
  public void setUp() throws Exception {
    TxnDbUtil.prepDb();
    txnHandler = new CompactionTxnHandler(conf);
  }

  @After
  public void tearDown() throws Exception {
    TxnDbUtil.cleanDb();
  }

  private long openTxn() throws MetaException {
    List<Long> txns = txnHandler.openTxns(new OpenTxnRequest(1, "me", "localhost")).getTxn_ids();
    return txns.get(0);
  }

}
