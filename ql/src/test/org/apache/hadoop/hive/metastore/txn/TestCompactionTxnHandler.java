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
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionInfoStruct;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.FindNextCompactRequest;
import org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoRequest;
import org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoResponse;
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
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hive.common.util.HiveVersionInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.COMPACTOR_FETCH_SIZE;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.COMPACTOR_INITIATOR_FAILED_RETRY_TIME;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for CompactionTxnHandler.
 */
public class TestCompactionTxnHandler {

  public static final String WORKER_VERSION = HiveVersionInfo.getShortVersion();
  private HiveConf conf = new HiveConf();
  private TxnStore txnHandler;

  public TestCompactionTxnHandler() throws Exception {
    TestTxnDbUtil.setConfValues(conf);
    TestTxnDbUtil.prepDb(conf);
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
    CompactionInfo ci = txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION));
    assertNotNull(ci);
    assertEquals("foo", ci.dbname);
    assertEquals("bar", ci.tableName);
    assertEquals("ds=today", ci.partName);
    assertEquals(CompactionType.MINOR, ci.type);
    assertNull(ci.runAs);
    assertNull(txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION)));
    ci.runAs = "bob";
    txnHandler.updateCompactorState(ci, openTxn());

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
    CompactionInfo ci = txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION));
    assertNotNull(ci);
    assertEquals("foo", ci.dbname);
    assertEquals("bar", ci.tableName);
    if ("ds=today".equals(ci.partName)) expectToday = false;
    else if ("ds=yesterday".equals(ci.partName)) expectToday = true;
    else fail("partition name should have been today or yesterday but was " + ci.partName);
    assertEquals(CompactionType.MINOR, ci.type);

    ci = txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION));
    assertNotNull(ci);
    assertEquals("foo", ci.dbname);
    assertEquals("bar", ci.tableName);
    if (expectToday) assertEquals("ds=today", ci.partName);
    else assertEquals("ds=yesterday", ci.partName);
    assertEquals(CompactionType.MINOR, ci.type);

    assertNull(txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION)));

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
    assertNull(txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION)));
  }

  @Test
  public void testMarkCompacted() throws Exception {
    CompactionRequest rqst = new CompactionRequest("foo", "bar", CompactionType.MINOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION));
    assertNotNull(ci);

    txnHandler.markCompacted(ci);
    assertNull(txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION)));



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
    assertEquals(0, txnHandler.findReadyToClean(0, 0).size());
    CompactionInfo ci = txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION));
    assertNotNull(ci);
    
    ci.highestWriteId = 41;
    txnHandler.updateCompactorState(ci, 0);
    txnHandler.markCompacted(ci);
    assertNull(txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION)));

    List<CompactionInfo> toClean = txnHandler.findReadyToClean(0, 0);
    assertEquals(1, toClean.size());
    assertNull(txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION)));

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
    assertEquals(0, txnHandler.findReadyToClean(0, 0).size());
    CompactionInfo ci = txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION));
    assertNotNull(ci);
    
    ci.highestWriteId = 41;
    txnHandler.updateCompactorState(ci, 0);
    txnHandler.markCompacted(ci);
    assertNull(txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION)));

    List<CompactionInfo> toClean = txnHandler.findReadyToClean(0, 0);
    assertEquals(1, toClean.size());
    assertNull(txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION)));
    txnHandler.markCleaned(ci);
    assertNull(txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION)));
    assertEquals(0, txnHandler.findReadyToClean(0, 0).size());

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    assertEquals(1, rsp.getCompactsSize());
    assertTrue(TxnHandler.SUCCEEDED_RESPONSE.equals(rsp.getCompacts().get(0).getState()));
  }

  @Test
  public void testShowCompactions() throws Exception {
    final String dbName = "foo";
    final String tableName = "bar";
    final String partitionName = "ds=today";
    CompactionRequest rqst = new CompactionRequest(dbName, tableName, CompactionType.MINOR);
    rqst.setPartitionname(partitionName);
    txnHandler.compact(rqst);
    ShowCompactResponse showCompactResponse = txnHandler.showCompact(new ShowCompactRequest());
    showCompactResponse.getCompacts().forEach(e -> {
      assertEquals(dbName, e.getDbname());
      assertEquals(tableName, e.getTablename());
      assertEquals(partitionName, e.getPartitionname());
      assertEquals("initiated", e.getState());
      assertEquals(CompactionType.MINOR, e.getType());
      assertEquals(1, e.getId());
    });
  }

  @Test
  public void testGetLatestCommittedCompaction() throws Exception {
    final String dbName = "foo";
    final String tableName = "bar";
    final String errorMessage = "Dummy error";
    addSucceededCompaction(dbName, tableName, null, CompactionType.MINOR);
    addFailedCompaction(dbName, tableName, CompactionType.MINOR, null, errorMessage);
    GetLatestCommittedCompactionInfoRequest rqst = new GetLatestCommittedCompactionInfoRequest();
    rqst.setDbname(dbName);
    rqst.setTablename(tableName);
    GetLatestCommittedCompactionInfoResponse response = txnHandler.getLatestCommittedCompactionInfo(rqst);

    assertNotNull(response);
    assertEquals("Expecting a single compaction record", 1, response.getCompactionsSize());
    CompactionInfoStruct lci = response.getCompactions().get(0);
    assertEquals("Expecting the first succeeded compaction record", 1, lci.getId());
    assertNull("Expecting null partitionname for a non-partitioned table", lci.getPartitionname());
    assertEquals(CompactionType.MINOR, lci.getType());

    rqst.setPartitionnames(new ArrayList<>());
    response = txnHandler.getLatestCommittedCompactionInfo(rqst);

    assertNotNull(response);
    assertEquals("Expecting a single compaction record", 1, response.getCompactionsSize());
    lci = response.getCompactions().get(0);
    assertEquals("Expecting the first succeeded compaction record", 1, lci.getId());
    assertEquals(dbName, lci.getDbname());
    assertEquals(tableName, lci.getTablename());
    assertNull("Expecting null partitionname for a non-partitioned table", lci.getPartitionname());
    assertEquals(CompactionType.MINOR, lci.getType());
  }

  @Test
  public void testGetLatestCommittedCompactionPartition() throws Exception {
    final String dbName = "foo";
    final String tableName = "bar";
    final String partitionName = "ds=today";
    final String errorMessage = "Dummy error";
    addSucceededCompaction(dbName, tableName, partitionName, CompactionType.MINOR);
    addFailedCompaction(dbName, tableName, CompactionType.MINOR, partitionName, errorMessage);
    GetLatestCommittedCompactionInfoRequest rqst = new GetLatestCommittedCompactionInfoRequest();
    rqst.setDbname(dbName);
    rqst.setTablename(tableName);
    rqst.addToPartitionnames(partitionName);
    GetLatestCommittedCompactionInfoResponse response = txnHandler.getLatestCommittedCompactionInfo(rqst);

    assertNotNull(response);
    assertEquals("Expecting a single compaction record", 1, response.getCompactionsSize());
    CompactionInfoStruct lci = response.getCompactions().get(0);
    assertEquals("Expecting the first succeeded compaction record", 1, lci.getId());
    assertEquals(partitionName, lci.getPartitionname());
    assertEquals(CompactionType.MINOR, lci.getType());

    final String anotherPartitionName = "ds=yesterday";
    addWaitingForCleaningCompaction(dbName, tableName, CompactionType.MINOR, anotherPartitionName, errorMessage);
    rqst.addToPartitionnames(anotherPartitionName);
    response = txnHandler.getLatestCommittedCompactionInfo(rqst);

    assertNotNull(response);
    assertEquals("Expecting a single compaction record for each partition", 2, response.getCompactionsSize());
    CompactionInfoStruct lci1 = response.getCompactions().stream()
        .filter(c -> c.getPartitionname().equals(partitionName)).findFirst().get();
    assertEquals(1, lci1.getId());
    CompactionInfoStruct lci2 = response.getCompactions().stream().
        filter(c -> c.getPartitionname().equals(anotherPartitionName)).findFirst().get();
    assertEquals(3, lci2.getId());

    // check the result is correct without setting partition names
    rqst.unsetPartitionnames();
    response = txnHandler.getLatestCommittedCompactionInfo(rqst);
    assertNotNull(response);
    assertEquals("Expecting a single compaction record for each partition", 2, response.getCompactionsSize());
    lci1 = response.getCompactions().stream()
        .filter(c -> c.getPartitionname().equals(partitionName)).findFirst().get();
    assertEquals(1, lci1.getId());
    lci2 = response.getCompactions().stream().
        filter(c -> c.getPartitionname().equals(anotherPartitionName)).findFirst().get();
    assertEquals(3, lci2.getId());
  }

  @Test
  public void testGetLatestSucceededCompaction() throws Exception {
    final String dbName = "foo";
    final String tableName = "bar";
    final String partitionName = "ds=today";
    final String errorMessage = "Dummy error";
    addSucceededCompaction(dbName, tableName, partitionName, CompactionType.MINOR);
    addSucceededCompaction(dbName, tableName, partitionName, CompactionType.MINOR);
    GetLatestCommittedCompactionInfoRequest rqst = new GetLatestCommittedCompactionInfoRequest();
    rqst.setDbname(dbName);
    rqst.setTablename(tableName);
    rqst.addToPartitionnames(partitionName);
    txnHandler.getLatestCommittedCompactionInfo(rqst);
    GetLatestCommittedCompactionInfoResponse response = txnHandler.getLatestCommittedCompactionInfo(rqst);

    assertNotNull(response);
    assertEquals("Expecting a single record", 1, response.getCompactionsSize());
    CompactionInfoStruct lci = response.getCompactions().get(0);
    assertEquals("Expecting the second succeeded compaction record", 2, lci.getId());
    assertEquals(partitionName, lci.getPartitionname());
    assertEquals(CompactionType.MINOR, lci.getType());

    addWaitingForCleaningCompaction(dbName, tableName, CompactionType.MINOR, partitionName, errorMessage);
    response = txnHandler.getLatestCommittedCompactionInfo(rqst);

    assertNotNull(response);
    assertEquals("Expecting a single record", 1, response.getCompactionsSize());
    lci = response.getCompactions().get(0);
    assertEquals("Expecting the last compaction record waiting for cleaning", 3, lci.getId());
    assertEquals(partitionName, lci.getPartitionname());
    assertEquals(CompactionType.MINOR, lci.getType());
  }

  @Test
  public void testGetLatestCompactionWithIdFilter() throws Exception {
    final String dbName = "foo";
    final String tableName = "bar";
    final String partitionName = "ds=today";
    addSucceededCompaction(dbName, tableName, partitionName, CompactionType.MINOR);
    addSucceededCompaction(dbName, tableName, partitionName, CompactionType.MINOR);
    GetLatestCommittedCompactionInfoRequest rqst = new GetLatestCommittedCompactionInfoRequest();
    rqst.setDbname(dbName);
    rqst.setTablename(tableName);
    rqst.addToPartitionnames(partitionName);
    GetLatestCommittedCompactionInfoResponse response = txnHandler.getLatestCommittedCompactionInfo(rqst);

    assertNotNull(response);
    assertEquals("Expecting a single record", 1, response.getCompactionsSize());
    CompactionInfoStruct lci = response.getCompactions().get(0);
    assertEquals("Expecting the second succeeded compaction record", 2, lci.getId());
    assertEquals(partitionName, lci.getPartitionname());
    assertEquals(CompactionType.MINOR, lci.getType());

    // response should contain the latest compaction
    rqst.setLastCompactionId(1);
    response = txnHandler.getLatestCommittedCompactionInfo(rqst);
    assertNotNull(response);
    assertEquals("Expecting a single record", 1, response.getCompactionsSize());
    lci = response.getCompactions().get(0);
    assertEquals("Expecting the second succeeded compaction record", 2, lci.getId());
    assertEquals(partitionName, lci.getPartitionname());
    assertEquals(CompactionType.MINOR, lci.getType());

    // response should only include compaction with id > 2
    rqst.setLastCompactionId(2);
    response = txnHandler.getLatestCommittedCompactionInfo(rqst);
    assertNotNull(response);
    assertEquals("Expecting no record", 0, response.getCompactionsSize());
  }

  @Test
  public void testGetNoCompaction() throws Exception {
    final String dbName = "foo";
    final String tableName = "bar";
    final String errorMessage = "Dummy error";
    GetLatestCommittedCompactionInfoRequest rqst = new GetLatestCommittedCompactionInfoRequest();
    rqst.setDbname(dbName);
    rqst.setTablename(tableName);
    GetLatestCommittedCompactionInfoResponse response = txnHandler.getLatestCommittedCompactionInfo(rqst);

    assertEquals(0, response.getCompactionsSize());

    addFailedCompaction(dbName, tableName, CompactionType.MINOR, null, errorMessage);
    response = txnHandler.getLatestCommittedCompactionInfo(rqst);

    assertEquals(0, response.getCompactionsSize());
  }

  @Test
  public void testMarkFailed() throws Exception {
    final String dbName = "foo";
    final String tableName = "bar";
    final String partitionName = "ds=today";
    final String workerId = "fred";
    final String status = "failed";
    final String errorMessage = "Dummy error";
    CompactionRequest rqst = new CompactionRequest(dbName, tableName, CompactionType.MINOR);
    rqst.setPartitionname(partitionName);
    txnHandler.compact(rqst);
    assertEquals(0, txnHandler.findReadyToClean(0, 0).size());
    CompactionInfo ci = txnHandler.findNextToCompact(aFindNextCompactRequest(workerId, WORKER_VERSION));
    assertNotNull(ci);

    assertEquals(0, txnHandler.findReadyToClean(0, 0).size());
    ci.errorMessage = errorMessage;
    txnHandler.markFailed(ci);
    assertNull(txnHandler.findNextToCompact(aFindNextCompactRequest(workerId, WORKER_VERSION)));
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
    for (int i = 1; i < MetastoreConf.getIntVar(conf, COMPACTOR_INITIATOR_FAILED_THRESHOLD); i++) {
      addFailedCompaction(dbName, tableName, CompactionType.MINOR, partitionName, errorMessage);
    }
    // Now checkFailedCompactions() will return true
    assertTrue(txnHandler.checkFailedCompactions(ci));
    MetastoreConf.setTimeVar(conf, COMPACTOR_INITIATOR_FAILED_RETRY_TIME, 1, TimeUnit.MILLISECONDS);
    // Now checkFailedCompactions() should ignore the threshold
    assertFalse(txnHandler.checkFailedCompactions(ci));
    MetastoreConf.setTimeVar(conf, COMPACTOR_INITIATOR_FAILED_RETRY_TIME, 7, TimeUnit.DAYS);
    // Check the output of show compactions
    checkShowCompaction(dbName, tableName, partitionName, status, errorMessage);
    // Now add enough failed compactions to ensure purgeCompactionHistory() will attempt delete;
    // HiveConf.ConfVars.COMPACTOR_HISTORY_RETENTION_DID_NOT_INITIATE is enough for this.
    // But we also want enough to tickle the code in TxnUtils.buildQueryWithINClauseStrings()
    // so that it produces multiple queries. For that we need at least 290.
    for (int i = 0 ; i < 300; i++) {
      addFailedCompaction(dbName, tableName, CompactionType.MINOR, partitionName, errorMessage);
    }
    checkShowCompaction(dbName, tableName, partitionName, status, errorMessage);
    txnHandler.purgeCompactionHistory();
  }

  private void checkShowCompaction(String dbName, String tableName, String partition,
      String status, String errorMessage) throws MetaException {
    ShowCompactResponse showCompactResponse = txnHandler.showCompact(new ShowCompactRequest());
    showCompactResponse.getCompacts().forEach(e -> {
      assertEquals(dbName, e.getDbname());
      assertEquals(tableName, e.getTablename());
      assertEquals(partition, e.getPartitionname());
      assertEquals(status, e.getState());
      assertEquals(errorMessage, e.getErrorMessage());
    });
  }

  private void addFailedCompaction(String dbName, String tableName, CompactionType type,
      String partitionName, String errorMessage) throws MetaException {
    CompactionRequest rqst;
    CompactionInfo ci;
    rqst = new CompactionRequest(dbName, tableName, type);
    if (partitionName != null) {
      rqst.setPartitionname(partitionName);
    }
    txnHandler.compact(rqst);
    ci = txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION));
    assertNotNull(ci);
    ci.errorMessage = errorMessage;
    txnHandler.markFailed(ci);
  }

  private void addSucceededCompaction(String dbName, String tableName, String partitionName, CompactionType type) throws MetaException {
    CompactionRequest rqst = new CompactionRequest(dbName, tableName, type);
    CompactionInfo ci;
    if (partitionName != null) {
      rqst.setPartitionname(partitionName);
    }
    txnHandler.compact(rqst);
    ci = txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION));
    assertNotNull(ci);
    txnHandler.markCleaned(ci);
  }

  private void addWaitingForCleaningCompaction(String dbName, String tableName, CompactionType type,
      String partitionName, String errorMessage) throws MetaException {
    CompactionRequest rqst = new CompactionRequest(dbName, tableName, type);
    CompactionInfo ci;
    if (partitionName != null) {
      rqst.setPartitionname(partitionName);
    }
    txnHandler.compact(rqst);
    ci = txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION));
    assertNotNull(ci);
    ci.errorMessage = errorMessage;
    txnHandler.markCompacted(ci);
  }

  private void addDidNotInitiateCompaction(String dbName, String tableName, String partitionName,
          CompactionType type, String errorMessage) throws MetaException {
    CompactionInfo ci = new CompactionInfo(dbName, tableName, partitionName, type);
    ci.errorMessage = errorMessage;
    ci.id = 0;
    txnHandler.markFailed(ci);
  }

  private void addRefusedCompaction(String dbName, String tableName, String partitionName,
                                           CompactionType type, String errorMessage) throws MetaException {
    CompactionInfo ci = new CompactionInfo(dbName, tableName, partitionName, type);
    ci.errorMessage = errorMessage;
    ci.id = 0;
    txnHandler.markRefused(ci);
  }


  @Test
  public void testPurgeCompactionHistory() throws Exception {
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_SUCCEEDED, 2);
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_DID_NOT_INITIATE, 2);
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED, 2);
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_REFUSED, 2);
    txnHandler.setConf(conf);

    String dbName = "default";
    String tableName = "tpch";
    String part1 = "(p=1)";
    String part2 = "(p=2)";

    // 3 successful compactions on p=1
    addSucceededCompaction(dbName, tableName, part1, CompactionType.MAJOR);
    addSucceededCompaction(dbName, tableName, part1, CompactionType.MAJOR);
    addSucceededCompaction(dbName, tableName, part1, CompactionType.MAJOR);

    // 3 failed on p=1
    addFailedCompaction(dbName, tableName, CompactionType.MAJOR, part1, "message");
    addFailedCompaction(dbName, tableName, CompactionType.MAJOR, part1, "message");
    addFailedCompaction(dbName, tableName, CompactionType.MAJOR, part1, "message");
    //4 failed on p=2
    addFailedCompaction(dbName, tableName, CompactionType.MAJOR, part2, "message");
    addFailedCompaction(dbName, tableName, CompactionType.MAJOR, part2, "message");
    addFailedCompaction(dbName, tableName, CompactionType.MAJOR, part2, "message");
    addFailedCompaction(dbName, tableName, CompactionType.MAJOR, part2, "message");

    // 3 did not initiate on p=1
    addDidNotInitiateCompaction(dbName, tableName, part1, CompactionType.MAJOR, "message");
    addDidNotInitiateCompaction(dbName, tableName, part1, CompactionType.MAJOR, "message");
    addDidNotInitiateCompaction(dbName, tableName, part1, CompactionType.MAJOR, "message");

    // 3 refused on p=1
    addRefusedCompaction(dbName, tableName, part1, CompactionType.MAJOR, "message");
    addRefusedCompaction(dbName, tableName, part1, CompactionType.MAJOR, "message");
    addRefusedCompaction(dbName, tableName, part1, CompactionType.MAJOR, "message");

    countCompactionsInHistory(dbName, tableName, part1, 3, 3, 3, 3);
    countCompactionsInHistory(dbName, tableName, part2, 0, 4, 0, 0);

    txnHandler.purgeCompactionHistory();

    countCompactionsInHistory(dbName, tableName, part1, 2, 2, 2, 2);
    countCompactionsInHistory(dbName, tableName, part2, 0, 2, 0, 0);
  }

  @Test
  public void testPurgeCompactionHistoryTimeout() throws Exception {
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_SUCCEEDED, 2);
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_DID_NOT_INITIATE, 2);
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED, 2);
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_REFUSED, 2);
    MetastoreConf.setTimeVar(conf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_TIMEOUT, 1, TimeUnit.MILLISECONDS);
    txnHandler.setConf(conf);

    String dbName = "default";
    String tableName = "tpch";
    String part1 = "(p=1)";

    addFailedCompaction(dbName, tableName, CompactionType.MINOR, part1, "message");
    addDidNotInitiateCompaction(dbName, tableName, part1, CompactionType.MINOR, "message");
    addRefusedCompaction(dbName, tableName, part1, CompactionType.MINOR, "message");
    addSucceededCompaction(dbName, tableName, part1, CompactionType.MINOR);
    addFailedCompaction(dbName, tableName, CompactionType.MINOR, part1, "message");
    addDidNotInitiateCompaction(dbName, tableName, part1, CompactionType.MINOR, "message");
    addRefusedCompaction(dbName, tableName, part1, CompactionType.MINOR, "message");

    countCompactionsInHistory(dbName, tableName, part1, 1, 2, 2, 2);

    txnHandler.purgeCompactionHistory();

    // the oldest 3 compactions should be cleaned
    countCompactionsInHistory(dbName, tableName, part1, 1, 1, 1, 1);

    addSucceededCompaction(dbName, tableName, part1, CompactionType.MAJOR);

    txnHandler.purgeCompactionHistory();

    // only 2 succeeded compactions should be left
    countCompactionsInHistory(dbName, tableName, part1, 2, 0, 0, 0);

    addFailedCompaction(dbName, tableName, CompactionType.MAJOR, part1, "message");
    addDidNotInitiateCompaction(dbName, tableName, part1, CompactionType.MAJOR, "message");
    addRefusedCompaction(dbName, tableName, part1, CompactionType.MAJOR, "message");
    addSucceededCompaction(dbName, tableName, part1, CompactionType.MINOR);

    // succeeded minor compaction shouldn't cause cleanup, but the oldest succeeded will be cleaned up
    txnHandler.purgeCompactionHistory();
    countCompactionsInHistory(dbName, tableName, part1, 2, 1, 1, 1);

    addFailedCompaction(dbName, tableName, CompactionType.MAJOR, part1, "message");
    addDidNotInitiateCompaction(dbName, tableName, part1, CompactionType.MAJOR, "message");
    addRefusedCompaction(dbName, tableName, part1, CompactionType.MAJOR, "message");
    addSucceededCompaction(dbName, tableName, part1, CompactionType.MAJOR);

    // only 2 succeeded compactions should be left
    txnHandler.purgeCompactionHistory();
    countCompactionsInHistory(dbName, tableName, part1, 2, 0, 0, 0);
    checkShowCompaction(dbName, tableName, part1, "succeeded", null);
  }

  private void countCompactionsInHistory(String dbName, String tableName, String partition,
          int expectedSucceeded, int expectedFailed, int expectedDidNotInitiate, int expextedRefused)
          throws MetaException {
    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> filteredToPartition = resp.getCompacts().stream()
            .filter(e -> e.getDbname().equals(dbName) && e.getTablename().equals(tableName) &&
                    (partition == null || partition.equals(e.getPartitionname()))).collect(Collectors.toList());

    assertEquals(expectedSucceeded, filteredToPartition.stream().filter(e -> e.getState().equals(TxnStore.SUCCEEDED_RESPONSE)).count());
    assertEquals(expectedFailed, filteredToPartition.stream().filter(e -> e.getState().equals(TxnStore.FAILED_RESPONSE)).count());
    assertEquals(expectedDidNotInitiate, filteredToPartition.stream().filter(e -> e.getState().equals(TxnStore.DID_NOT_INITIATE_RESPONSE)).count());
    assertEquals(expextedRefused, filteredToPartition.stream().filter(e -> e.getState().equals(TxnStore.REFUSED_RESPONSE)).count());
  }

  @Test
  public void testRevokeFromLocalWorkers() throws Exception {
    CompactionRequest rqst = new CompactionRequest("foo", "bar", CompactionType.MINOR);
    txnHandler.compact(rqst);
    rqst = new CompactionRequest("foo", "baz", CompactionType.MINOR);
    txnHandler.compact(rqst);
    rqst = new CompactionRequest("foo", "bazzoo", CompactionType.MINOR);
    txnHandler.compact(rqst);
    assertNotNull(txnHandler.findNextToCompact(aFindNextCompactRequest("fred-193892", WORKER_VERSION)));
    assertNotNull(txnHandler.findNextToCompact(aFindNextCompactRequest("bob-193892", WORKER_VERSION)));
    assertNotNull(txnHandler.findNextToCompact(aFindNextCompactRequest("fred-193893", WORKER_VERSION)));
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

    assertNotNull(txnHandler.findNextToCompact(aFindNextCompactRequest("fred-193892", WORKER_VERSION)));
    Thread.sleep(200);
    assertNotNull(txnHandler.findNextToCompact(aFindNextCompactRequest("fred-193892", WORKER_VERSION)));
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

    List<LockComponent> components = new ArrayList<>();

    components.add(createLockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb", "mytable", null, DataOperationType.UPDATE));
    components.add(createLockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb", "yourtable", "mypartition=myvalue", DataOperationType.UPDATE));

    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));
    assertEquals(0, txnHandler.getNumLocks());

    Set<CompactionInfo> potentials = txnHandler.findPotentialCompactions(100, -1L);
    assertEquals(2, potentials.size());
    boolean sawMyTable = false, sawYourTable = false;
    for (CompactionInfo ci : potentials) {
      sawMyTable |= (ci.dbname.equals("mydb") && ci.tableName.equals("mytable") &&
          ci.partName ==  null);
      sawYourTable |= (ci.dbname.equals("mydb") && ci.tableName.equals("yourtable") &&
          ci.partName.equals("mypartition=myvalue"));
    }
    assertTrue(sawMyTable);
    assertTrue(sawYourTable);

    potentials = txnHandler.findPotentialCompactions(100, -1, 
      Instant.now().minusSeconds(1).toEpochMilli());
    assertEquals(2, potentials.size());

    //simulate auto-compaction interval
    TimeUnit.SECONDS.sleep(2);

    potentials = txnHandler.findPotentialCompactions(100, -1, 
      Instant.now().minusSeconds(1).toEpochMilli());
    assertEquals(0, potentials.size());

    //simulate prev failed compaction
    CompactionRequest rqst = new CompactionRequest("mydb", "mytable", CompactionType.MINOR);
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION));
    txnHandler.markFailed(ci);

    potentials = txnHandler.findPotentialCompactions(100, -1, 
      Instant.now().minusSeconds(1).toEpochMilli());
    assertEquals(1, potentials.size());
  }

  // TODO test changes to mark cleaned to clean txns and txn_components

  @Test
  public void testMarkCleanedCleansTxnsAndTxnComponents()
      throws Exception {
    long txnid = openTxn();
    long mytableWriteId = allocateTableWriteIds("mydb", "mytable", txnid);
    
    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
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
    long fooWriteId = allocateTableWriteIds("mydb", "foo", txnid);
    
    comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("foo");
    comp.setPartitionname("bar=compact");
    comp.setOperationType(DataOperationType.UPDATE);
    components = new ArrayList<LockComponent>(1);
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);

    comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb");
    comp.setTablename("foo");
    comp.setPartitionname("baz=compact");
    comp.setOperationType(DataOperationType.UPDATE);
    components = new ArrayList<LockComponent>(1);
    components.add(comp);
    req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    res = txnHandler.lock(req);
    assertTrue(res.getState() == LockState.ACQUIRED);
    txnHandler.abortTxn(new AbortTxnRequest(txnid));

    CompactionInfo ci;

    // Now clean them and check that they are removed from the count.
    CompactionRequest rqst = new CompactionRequest("mydb", "mytable", CompactionType.MAJOR);
    txnHandler.compact(rqst);
    assertEquals(0, txnHandler.findReadyToClean(0, 0).size());
    ci = txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION));
    assertNotNull(ci);
    
    ci.highestWriteId = mytableWriteId;
    txnHandler.updateCompactorState(ci, 0);
    txnHandler.markCompacted(ci);
    
    Thread.sleep(txnHandler.getOpenTxnTimeOutMillis());
    List<CompactionInfo> toClean = txnHandler.findReadyToClean(0, 0);
    assertEquals(1, toClean.size());
    txnHandler.markCleaned(ci);

    // Check that we are cleaning up the empty aborted transactions
    GetOpenTxnsResponse txnList = txnHandler.getOpenTxns();
    assertEquals(3, txnList.getOpen_txnsSize());
    // Create one aborted for low water mark
    txnid = openTxn();
    txnHandler.abortTxn(new AbortTxnRequest(txnid));
    Thread.sleep(txnHandler.getOpenTxnTimeOutMillis());
    txnHandler.cleanEmptyAbortedAndCommittedTxns();
    txnList = txnHandler.getOpenTxns();
    assertEquals(3, txnList.getOpen_txnsSize());

    rqst = new CompactionRequest("mydb", "foo", CompactionType.MAJOR);
    rqst.setPartitionname("bar");
    txnHandler.compact(rqst);
    assertEquals(0, txnHandler.findReadyToClean(0, 0).size());
    ci = txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION));
    assertNotNull(ci);

    ci.highestWriteId = fooWriteId;
    txnHandler.updateCompactorState(ci, 0);
    txnHandler.markCompacted(ci);

    toClean = txnHandler.findReadyToClean(0, 0);
    assertEquals(1, toClean.size());
    txnHandler.markCleaned(ci);

    txnHandler.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    // The open txn will became the low water mark
    Thread.sleep(txnHandler.getOpenTxnTimeOutMillis());
    txnHandler.setOpenTxnTimeOutMillis(1);
    txnHandler.cleanEmptyAbortedAndCommittedTxns();
    txnList = txnHandler.getOpenTxns();
    assertEquals(3, txnList.getOpen_txnsSize());
    txnHandler.setOpenTxnTimeOutMillis(1000);
  }

  @Test
  public void addDynamicPartitions() throws Exception {
    String dbName = "default";
    String tableName = "adp_table";
    OpenTxnsResponse openTxns = txnHandler.openTxns(new OpenTxnRequest(1, "me", "localhost"));
    long txnId = openTxns.getTxn_ids().get(0);

    AllocateTableWriteIdsRequest rqst = new AllocateTableWriteIdsRequest(dbName, tableName);
    rqst.setTxnIds(openTxns.getTxn_ids());
    AllocateTableWriteIdsResponse writeIds = txnHandler.allocateTableWriteIds(rqst);
    long writeId = writeIds.getTxnToWriteIds().get(0).getWriteId();
    assertEquals(txnId, writeIds.getTxnToWriteIds().get(0).getTxnId());
    assertEquals(1, writeId);

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

    AddDynamicPartitions adp = new AddDynamicPartitions(txnId, writeId, dbName, tableName,
      Arrays.asList("ds=yesterday", "ds=today"));
    adp.setOperationType(dop);
    txnHandler.addDynamicPartitions(adp);
    txnHandler.commitTxn(new CommitTxnRequest(txnId));

    Set<CompactionInfo> potentials = txnHandler.findPotentialCompactions(1000, -1L);
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

  @Test
  public void testEnqueueTimeEvenAfterFailed() throws Exception {
    final String dbName = "foo";
    final String tableName = "bar";
    final String partitionName = "ds=today";
    CompactionRequest rqst = new CompactionRequest(dbName, tableName, CompactionType.MINOR);
    rqst.setPartitionname(partitionName);
    long before = System.currentTimeMillis();
    txnHandler.compact(rqst);
    long after = System.currentTimeMillis();
    ShowCompactResponse showCompactResponse = txnHandler.showCompact(new ShowCompactRequest());
    ShowCompactResponseElement element = showCompactResponse.getCompacts().get(0);
    assertTrue(element.isSetEnqueueTime());
    long enqueueTime = element.getEnqueueTime();
    assertTrue(enqueueTime <= after);
    assertTrue(enqueueTime >= before);

    CompactionInfo ci = txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION));
    txnHandler.markFailed(ci);

    checkEnqueueTime(enqueueTime);
  }

  @Test
  public void testEnqueueTimeThroughLifeCycle() throws Exception {
    final String dbName = "foo";
    final String tableName = "bar";
    final String partitionName = "ds=today";
    CompactionRequest rqst = new CompactionRequest(dbName, tableName, CompactionType.MINOR);
    rqst.setPartitionname(partitionName);
    long before = System.currentTimeMillis();
    txnHandler.compact(rqst);
    long after = System.currentTimeMillis();
    ShowCompactResponse showCompactResponse = txnHandler.showCompact(new ShowCompactRequest());
    ShowCompactResponseElement element = showCompactResponse.getCompacts().get(0);
    assertTrue(element.isSetEnqueueTime());
    long enqueueTime = element.getEnqueueTime();
    assertTrue(enqueueTime <= after);
    assertTrue(enqueueTime >= before);

    CompactionInfo ci = txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION));
    ci.runAs = "bob";
    txnHandler.updateCompactorState(ci, openTxn());
    checkEnqueueTime(enqueueTime);

    txnHandler.markCompacted(ci);
    checkEnqueueTime(enqueueTime);

    txnHandler.markCleaned(ci);
    checkEnqueueTime(enqueueTime);
  }

  @Test
  public void testFindPotentialCompactions_limitFetchSize() throws Exception {
    MetastoreConf.setLongVar(conf, COMPACTOR_FETCH_SIZE, 1);

    long txnId = openTxn();

    List<LockComponent> components = new ArrayList<>();
    components.add(createLockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb", "mytable", "mypartition=myvalue", DataOperationType.UPDATE));
    components.add(createLockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb", "yourtable", "mypartition=myvalue", DataOperationType.UPDATE));

    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnId);
    LockResponse res = txnHandler.lock(req);
    assertSame(res.getState(), LockState.ACQUIRED);
    txnHandler.commitTxn(new CommitTxnRequest(txnId));
    assertEquals(0, txnHandler.getNumLocks());

    Set<CompactionInfo> potentials = txnHandler.findPotentialCompactions(100, -1L);
    assertEquals(1, potentials.size());
  }

  @Test
  public void testFindNextToClean_limitFetchSize() throws Exception {
    MetastoreConf.setLongVar(conf, COMPACTOR_FETCH_SIZE, 1);

    createAReadyToCleanCompaction("foo", "bar", "ds=today", CompactionType.MINOR);
    createAReadyToCleanCompaction("foo2", "bar2", "ds=today", CompactionType.MINOR);

    assertNull(txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION)));

    List<CompactionInfo> toClean = txnHandler.findReadyToClean(0, 0);
    assertEquals(1, toClean.size());
    assertNull(txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION)));
  }

  @Test
  public void testFindReadyToCleanAborts_limitFetchSize() throws Exception {
    MetastoreConf.setLongVar(conf, COMPACTOR_FETCH_SIZE, 1);

    long txnId = openTxn();

    List<LockComponent> components = new ArrayList<>();
    components.add(createLockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb", "mytable", "mypartition=myvalue", DataOperationType.UPDATE));
    components.add(createLockComponent(LockType.SHARED_WRITE, LockLevel.DB, "mydb", "yourtable", "mypartition=myvalue", DataOperationType.UPDATE));

    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnId);
    LockResponse res = txnHandler.lock(req);
    assertSame(res.getState(), LockState.ACQUIRED);

    txnHandler.abortTxn(new AbortTxnRequest((txnId)));

    List<CompactionInfo> potentials = txnHandler.findReadyToCleanAborts(1, 0);
    assertEquals(1, potentials.size());
  }

  private static FindNextCompactRequest aFindNextCompactRequest(String workerId, String workerVersion) {
    FindNextCompactRequest request = new FindNextCompactRequest();
    request.setWorkerId(workerId);
    request.setWorkerVersion(workerVersion);
    return request;
  }

  private void checkEnqueueTime(long enqueueTime) throws MetaException {
    ShowCompactResponse showCompactResponse = txnHandler.showCompact(new ShowCompactRequest());
    ShowCompactResponseElement element = showCompactResponse.getCompacts().get(0);
    assertTrue(element.isSetEnqueueTime());
    assertEquals(enqueueTime, element.getEnqueueTime());
  }

  @Before
  public void setUp() throws Exception {
    txnHandler = TxnUtils.getTxnStore(conf);
  }

  @After
  public void tearDown() throws Exception {
    TestTxnDbUtil.cleanDb(conf);
  }

  private long openTxn() throws MetaException {
    List<Long> txns = txnHandler.openTxns(new OpenTxnRequest(1, "me", "localhost")).getTxn_ids();
    return txns.get(0);
  }
  
  private long allocateTableWriteIds (String dbName, String tblName, long txnid) throws Exception {
    AllocateTableWriteIdsRequest rqst = new AllocateTableWriteIdsRequest(dbName, tblName);
    rqst.setTxnIds(Collections.singletonList(txnid));
    AllocateTableWriteIdsResponse writeIds = txnHandler.allocateTableWriteIds(rqst);
    return writeIds.getTxnToWriteIds().get(0).getWriteId();
  }

  private void createAReadyToCleanCompaction(String dbName, String tableName, String partitionName, CompactionType compactionType) throws MetaException {
    CompactionRequest rqst = new CompactionRequest(dbName, tableName, compactionType);
    rqst.setPartitionname(partitionName);
    txnHandler.compact(rqst);

    CompactionInfo ci = txnHandler.findNextToCompact(aFindNextCompactRequest("fred", WORKER_VERSION));
    assertNotNull(ci);

    ci.highestWriteId = 41;
    txnHandler.updateCompactorState(ci, 0);
    txnHandler.markCompacted(ci);
  }

  private LockComponent createLockComponent(LockType lockType, LockLevel lockLevel, String dbName, String tableName, String partitionName, DataOperationType dataOperationType){
    LockComponent lockComponent = new LockComponent(lockType, lockLevel, dbName);
    lockComponent.setTablename(tableName);
    lockComponent.setPartitionname(partitionName);
    lockComponent.setOperationType(dataOperationType);

    return lockComponent;
  }
}
