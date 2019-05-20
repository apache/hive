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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * Tests for the compactor Cleaner thread
 */
public class TestCleaner extends CompactorTest {

  @Test
  public void nothing() throws Exception {
    // Test that the whole things works when there's nothing in the queue.  This is just a
    // survival test.
    startCleaner();
  }

  @Test
  public void cleanupAfterMajorTableCompaction() throws Exception {
    Table t = newTable("default", "camtc", false);

    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);
    addBaseFile(t, null, 25L, 25);

    burnThroughTransactions("default", "camtc", 25);

    CompactionRequest rqst = new CompactionRequest("default", "camtc", CompactionType.MAJOR);
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    ci.runAs = System.getProperty("user.name");
    txnHandler.updateCompactorState(ci, openTxn());
    txnHandler.markCompacted(ci);

    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertTrue(TxnStore.SUCCEEDED_RESPONSE.equals(rsp.getCompacts().get(0).getState()));

    // Check that the files are removed
    List<Path> paths = getDirectories(conf, t, null);
    Assert.assertEquals(1, paths.size());
    Assert.assertEquals("base_25", paths.get(0).getName());
  }

  @Test
  public void cleanupAfterMajorPartitionCompaction() throws Exception {
    Table t = newTable("default", "campc", true);
    Partition p = newPartition(t, "today");

    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 22L, 2);
    addDeltaFile(t, p, 23L, 24L, 2);
    addBaseFile(t, p, 25L, 25);

    burnThroughTransactions("default", "campc", 25);

    CompactionRequest rqst = new CompactionRequest("default", "campc", CompactionType.MAJOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    ci.runAs = System.getProperty("user.name");
    txnHandler.updateCompactorState(ci, openTxn());
    txnHandler.markCompacted(ci);

    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertTrue(TxnStore.SUCCEEDED_RESPONSE.equals(rsp.getCompacts().get(0).getState()));

    // Check that the files are removed
    List<Path> paths = getDirectories(conf, t, p);
    Assert.assertEquals(1, paths.size());
    Assert.assertEquals("base_25", paths.get(0).getName());
  }

  @Test
  public void cleanupAfterMinorTableCompaction() throws Exception {
    Table t = newTable("default", "camitc", false);

    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);
    addDeltaFile(t, null, 21L, 24L, 4);

    burnThroughTransactions("default", "camitc", 25);

    CompactionRequest rqst = new CompactionRequest("default", "camitc", CompactionType.MINOR);
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    ci.runAs = System.getProperty("user.name");
    txnHandler.updateCompactorState(ci, openTxn());
    txnHandler.markCompacted(ci);

    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertTrue(TxnStore.SUCCEEDED_RESPONSE.equals(rsp.getCompacts().get(0).getState()));

    // Check that the files are removed
    List<Path> paths = getDirectories(conf, t, null);
    Assert.assertEquals(2, paths.size());
    boolean sawBase = false, sawDelta = false;
    for (Path p : paths) {
      if (p.getName().equals("base_20")) sawBase = true;
      else if (p.getName().equals(makeDeltaDirName(21, 24))) sawDelta = true;
      else Assert.fail("Unexpected file " + p.getName());
    }
    Assert.assertTrue(sawBase);
    Assert.assertTrue(sawDelta);
  }

  @Test
  public void cleanupAfterMinorPartitionCompaction() throws Exception {
    Table t = newTable("default", "camipc", true);
    Partition p = newPartition(t, "today");

    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 22L, 2);
    addDeltaFile(t, p, 23L, 24L, 2);
    addDeltaFile(t, p, 21L, 24L, 4);

    burnThroughTransactions("default", "camipc", 25);

    CompactionRequest rqst = new CompactionRequest("default", "camipc", CompactionType.MINOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    ci.runAs = System.getProperty("user.name");
    txnHandler.updateCompactorState(ci, openTxn());
    txnHandler.markCompacted(ci);

    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertTrue(TxnStore.SUCCEEDED_RESPONSE.equals(rsp.getCompacts().get(0).getState()));

    // Check that the files are removed
    List<Path> paths = getDirectories(conf, t, p);
    Assert.assertEquals(2, paths.size());
    boolean sawBase = false, sawDelta = false;
    for (Path path : paths) {
      if (path.getName().equals("base_20")) sawBase = true;
      else if (path.getName().equals(makeDeltaDirNameCompacted(21, 24))) sawDelta = true;
      else Assert.fail("Unexpected file " + path.getName());
    }
    Assert.assertTrue(sawBase);
    Assert.assertTrue(sawDelta);
  }

  @Test
  public void cleanupAfterMajorPartitionCompactionNoBase() throws Exception {
    Table t = newTable("default", "campcnb", true);
    Partition p = newPartition(t, "today");

    addDeltaFile(t, p, 1L, 22L, 22);
    addDeltaFile(t, p, 23L, 24L, 2);
    addBaseFile(t, p, 25L, 25);

    burnThroughTransactions("default", "campcnb", 25);

    CompactionRequest rqst = new CompactionRequest("default", "campcnb", CompactionType.MAJOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    txnHandler.markCompacted(ci);
    ci.runAs = System.getProperty("user.name");
    txnHandler.updateCompactorState(ci, openTxn());

    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertTrue(TxnStore.SUCCEEDED_RESPONSE.equals(rsp.getCompacts().get(0).getState()));

    // Check that the files are removed
    List<Path> paths = getDirectories(conf, t, p);
    Assert.assertEquals(1, paths.size());
    Assert.assertEquals("base_25", paths.get(0).getName());
  }

  @Test
  public void droppedTable() throws Exception {
    Table t = newTable("default", "dt", false);

    addDeltaFile(t, null, 1L, 22L, 22);
    addDeltaFile(t, null, 23L, 24L, 2);
    addBaseFile(t, null, 25L, 25);

    burnThroughTransactions("default", "dt", 25);

    CompactionRequest rqst = new CompactionRequest("default", "dt", CompactionType.MINOR);
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    ci.runAs = System.getProperty("user.name");
    txnHandler.updateCompactorState(ci, openTxn());
    txnHandler.markCompacted(ci);

    // Drop table will clean the table entry from the compaction queue and hence cleaner have no effect
    ms.dropTable("default", "dt");

    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(0, rsp.getCompactsSize());
  }

  @Test
  public void droppedPartition() throws Exception {
    Table t = newTable("default", "dp", true);
    Partition p = newPartition(t, "today");

    addDeltaFile(t, p, 1L, 22L, 22);
    addDeltaFile(t, p, 23L, 24L, 2);
    addBaseFile(t, p, 25L, 25);

    burnThroughTransactions("default", "dp", 25);

    CompactionRequest rqst = new CompactionRequest("default", "dp", CompactionType.MAJOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    ci.runAs = System.getProperty("user.name");
    txnHandler.updateCompactorState(ci, openTxn());
    txnHandler.markCompacted(ci);

    // Drop partition will clean the partition entry from the compaction queue and hence cleaner have no effect
    ms.dropPartition("default", "dp", Collections.singletonList("today"), true);

    startCleaner();

    // Check there are no compactions requests left.
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
