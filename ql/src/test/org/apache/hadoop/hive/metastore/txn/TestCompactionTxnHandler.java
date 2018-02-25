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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

/**
 * Tests for TxnHandler.
 */
public class TestCompactionTxnHandler {

  private HiveConf conf = new HiveConf();
  private TxnStore txnHandler;

  public TestCompactionTxnHandler() throws Exception {
    TxnDbUtil.setConfValues(conf);
    // Set config so that TxnUtils.buildQueryWithINClauseStrings() will
    // produce multiple queries
    conf.setIntVar(HiveConf.ConfVars.METASTORE_DIRECT_SQL_MAX_QUERY_LENGTH, 1);
    conf.setIntVar(HiveConf.ConfVars.METASTORE_DIRECT_SQL_MAX_ELEMENTS_IN_CLAUSE, 10);
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
    assertEquals(1, rsp.getCompactsSize());
    assertTrue(TxnHandler.SUCCEEDED_RESPONSE.equals(rsp.getCompacts().get(0).getState()));
  }

  @Test
  public void testMarkFailed() throws Exception {
    CompactionRequest rqst = new CompactionRequest("foo", "bar", CompactionType.MINOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);
    assertEquals(0, txnHandler.findReadyToClean().size());
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    assertNotNull(ci);

    assertEquals(0, txnHandler.findReadyToClean().size());
    txnHandler.markFailed(ci);
    assertNull(txnHandler.findNextToCompact("fred"));
    boolean failedCheck = txnHandler.checkFailedCompactions(ci);
    assertFalse(failedCheck);
    try {
      // The first call to markFailed() should have removed the record from
      // COMPACTION_QUEUE, so a repeated call should fail
      txnHandler.markFailed(ci);
      fail("The first call to markFailed() must have failed as this call did "
          + "not throw the expected exception");
    } catch (IllegalStateException e) {
      // This is expected
      assertTrue(e.getMessage().contains("No record with CQ_ID="));
    }

    // There are not enough failed compactions yet so checkFailedCompactions() should return false.
    // But note that any sql error will also result in a return of false.
    assertFalse(txnHandler.checkFailedCompactions(ci));

    // Add more failed compactions so that the total is exactly COMPACTOR_INITIATOR_FAILED_THRESHOLD
    for (int i = 1 ; i <  conf.getIntVar(HiveConf.ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD); i++) {
      addFailedCompaction("foo", "bar", CompactionType.MINOR, "ds=today");
    }
    // Now checkFailedCompactions() will return true
    assertTrue(txnHandler.checkFailedCompactions(ci));

    // Now add enough failed compactions to ensure purgeCompactionHistory() will attempt delete;
    // HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_ATTEMPTED is enough for this.
    // But we also want enough to tickle the code in TxnUtils.buildQueryWithINClauseStrings()
    // so that it produces multiple queries. For that we need at least 290.
    for (int i = 0 ; i < 300; i++) {
      addFailedCompaction("foo", "bar", CompactionType.MINOR, "ds=today");
    }
    txnHandler.purgeCompactionHistory();
  }

  private void addFailedCompaction(String dbName, String tableName, CompactionType type,
      String partitionName) throws MetaException {
    CompactionRequest rqst;
    CompactionInfo ci;
    rqst = new CompactionRequest(dbName, tableName, type);
    rqst.setPartitionname(partitionName);
    txnHandler.compact(rqst);
    ci = txnHandler.findNextToCompact("fred");
    assertNotNull(ci);
    txnHandler.markFailed(ci);
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
  public void testFindPotentialCompactions() throws Exception {
    // Test that committing unlocks
    long txnid = openTxn();
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB,
        "mydb");
    comp.setTablename("mytable");
    comp.setOperationType(DataOperationType.UPDATE);
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB,
        "mydb");
    comp.setTablename("yourtable");
    comp.setPartitionname("mypartition");
    comp.setOperationType(DataOperationType.UPDATE);
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
    comp.setOperationType(DataOperationType.INSERT);
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
    comp.setOperationType(DataOperationType.DELETE);
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
    comp.setOperationType(DataOperationType.UPDATE);
    components = new ArrayList<LockComponent>(1);
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);

    comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("foo");
    comp.setPartitionname("baz");
    comp.setOperationType(DataOperationType.UPDATE);
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

  @Test
  public void addDynamicPartitions() throws Exception {
    String dbName = "default";
    String tableName = "adp_table";
    OpenTxnsResponse openTxns = txnHandler.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId = openTxns.getTxn_ids().get(0);
    // lock a table, as in dynamic partitions
    LockComponent lc = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, dbName);
    lc.setIsDynamicPartitionWrite(true);
    lc.setTablename(tableName);
    DataOperationType dop = DataOperationType.UPDATE; 
    lc.setOperationType(dop);
    LockRequest lr = new LockRequest(Arrays.asList(lc), "me", "localhost");
    lr.setTxnid(txnId);
    LockResponse lock = txnHandler.lock(lr);
    assertEquals(LockState.ACQUIRED, lock.getState());

    AddDynamicPartitions adp = new AddDynamicPartitions(txnId, dbName, tableName,
      Arrays.asList("ds=yesterday", "ds=today"));
    adp.setOperationType(dop);
    txnHandler.addDynamicPartitions(adp);
    txnHandler.commitTxn(new CommitTxnRequest(txnId));

    Set<CompactionInfo> potentials = txnHandler.findPotentialCompactions(1000);
    assertEquals(2, potentials.size());
    SortedSet<CompactionInfo> sorted = new TreeSet<CompactionInfo>(potentials);

    int i = 0;
    for (CompactionInfo ci : sorted) {
      assertEquals(dbName, ci.dbname);
      assertEquals(tableName, ci.tableName);
      switch (i++) {
        case 0: assertEquals("ds=today", ci.partName); break;
        case 1: assertEquals("ds=yesterday", ci.partName); break;
      default: throw new RuntimeException("What?");
      }
    }
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
