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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
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
    long compactionTxn = openTxn();
    ci.runAs = System.getProperty("user.name");
    txnHandler.updateCompactorState(ci, compactionTxn);
    txnHandler.markCompacted(ci);
    txnHandler.commitTxn(new CommitTxnRequest(compactionTxn));

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
    long compactionTxn = openTxn();
    ci.runAs = System.getProperty("user.name");
    txnHandler.updateCompactorState(ci, compactionTxn);
    txnHandler.markCompacted(ci);
    txnHandler.commitTxn(new CommitTxnRequest(compactionTxn));

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
    long compactionTxn = openTxn();
    ci.runAs = System.getProperty("user.name");
    txnHandler.updateCompactorState(ci, compactionTxn);
    txnHandler.markCompacted(ci);
    txnHandler.commitTxn(new CommitTxnRequest(compactionTxn));

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
    long compactionTxn = openTxn();
    ci.runAs = System.getProperty("user.name");
    txnHandler.updateCompactorState(ci, compactionTxn);
    txnHandler.markCompacted(ci);
    txnHandler.commitTxn(new CommitTxnRequest(compactionTxn));

    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());

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
    long compactionTxn = openTxn();
    txnHandler.markCompacted(ci);
    ci.runAs = System.getProperty("user.name");
    txnHandler.updateCompactorState(ci, compactionTxn);
    txnHandler.commitTxn(new CommitTxnRequest(compactionTxn));

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
    long compactionTxn = openTxn();
    ci.runAs = System.getProperty("user.name");
    txnHandler.updateCompactorState(ci, compactionTxn);
    txnHandler.markCompacted(ci);
    txnHandler.commitTxn(new CommitTxnRequest(compactionTxn));

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
    long compactionTxn = openTxn();
    ci.runAs = System.getProperty("user.name");
    txnHandler.updateCompactorState(ci, compactionTxn);
    txnHandler.markCompacted(ci);
    txnHandler.commitTxn(new CommitTxnRequest(compactionTxn));

    // Drop partition will clean the partition entry from the compaction queue and hence cleaner have no effect
    ms.dropPartition("default", "dp", Collections.singletonList("today"), true);

    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(0, rsp.getCompactsSize());
  }

  @Test
  public void processCompactionCandidatesInParallel() throws Exception {
    Table t = newTable("default", "camipc", true);
    List<Partition> partitions = new ArrayList<>();
    Partition p = null;
    for(int i = 0; i < 10; i++) {
      p = newPartition(t, "today" + i);

      addBaseFile(t, p, 20L, 20);
      addDeltaFile(t, p, 21L, 22L, 2);
      addDeltaFile(t, p, 23L, 24L, 2);
      addDeltaFile(t, p, 21L, 24L, 4);
      partitions.add(p);
    }

    burnThroughTransactions("default", "camipc", 25);
    for(int i = 0; i < 10; i++) {
      CompactionRequest rqst = new CompactionRequest("default", "camipc", CompactionType.MINOR);
      rqst.setPartitionname("ds=today"+i);
      txnHandler.compact(rqst);
      CompactionInfo ci = txnHandler.findNextToCompact("fred");
      long compactionTxn = openTxn();
      ci.runAs = System.getProperty("user.name");
      txnHandler.updateCompactorState(ci, compactionTxn);
      txnHandler.markCompacted(ci);
      txnHandler.commitTxn(new CommitTxnRequest(compactionTxn));
    }

    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_CLEANER_THREADS_NUM, 3);
    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(10, rsp.getCompactsSize());
    Assert.assertTrue(TxnStore.SUCCEEDED_RESPONSE.equals(rsp.getCompacts().get(0).getState()));

    // Check that the files are removed
    for (Partition pa : partitions) {
      List<Path> paths = getDirectories(conf, t, pa);
      Assert.assertEquals(2, paths.size());
      boolean sawBase = false, sawDelta = false;
      for (Path path : paths) {
        if (path.getName().equals("base_20")) {
          sawBase = true;
        } else if (path.getName().equals(makeDeltaDirNameCompacted(21, 24))) {
          sawDelta = true;
        } else {
          Assert.fail("Unexpected file " + path.getName());
        }
      }
      Assert.assertTrue(sawBase);
      Assert.assertTrue(sawDelta);
    }
  }

  /**
   * If there's an open txn blocking the cleaner, the cleaner shouldn't run until all the files rendered obsolete by
   * that compaction will be all cleaned up.
   * Arbitrarily picked minor compaction to run here.
   * @throws Exception
   */
  @Test public void testCleanerStaysInQueueUntilItCanRun() throws Exception {
    Table t = newTable("default", "camtc", false);

    // open a txn, leave it open
    long openTxn = openTxn();

    // write into table
    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);
    addDeltaFile(t, null, 24L, 25L, 2);
    burnThroughTransactions("default", "camtc", 25);

    // compact and clean
    CompactionRequest rqst = new CompactionRequest("default", "camtc", CompactionType.MINOR);
    txnHandler.compact(rqst);
    startWorker();
    startCleaner();

    // Check that the files are NOT removed, and there are 2 new deltas from compaction (a delta and a delete_delta)
    List<Path> paths = getDirectories(conf, t, null);
    Assert.assertEquals(6, paths.size());

    // Check that the compaction is still in the queue, with status "ready for cleaning" 
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, rsp.getCompacts().get(0).getState());

    // abort txn 1 (doesn't matter if aborted or committed, the point is it's no longer open).
    // Then start Cleaner again
    txnHandler.abortTxn(new AbortTxnRequest(openTxn));
    startCleaner();

    // Check there are no compactions requests left.
    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertTrue(TxnStore.SUCCEEDED_RESPONSE.equals(rsp.getCompacts().get(0).getState()));

    // Check that the obsolete files (3 deltas) are removed. What's left: base_20, delta_21_25, delete_delta_21_25
    paths = getDirectories(conf, t, null);
    Assert.assertEquals(3, paths.size());
    Collections.sort(paths);
    Assert.assertEquals("base_20", paths.get(0).getName());
    Assert.assertEquals("delete_delta_0000021_0000025_v0000027", paths.get(1).getName());
    Assert.assertEquals("delta_0000021_0000025_v0000027", paths.get(2).getName());
  }

  /**
   * Same as testCleanerStaysInQueueUntilItCanRun, except open txn is opened after inserts and we open a txn after 
   * compaction (which shouldn't impact cleaning).
   */
  @Test public void testCleanerStaysInQueueUntilItCanRun2() throws Exception {
    Table t = newTable("default", "camtc", false);

    // write into table
    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);
    addDeltaFile(t, null, 24L, 25L, 2);
    burnThroughTransactions("default", "camtc", 25);

    // open a txn, leave it open
    long openTxn = openTxn();

    // compact
    CompactionRequest rqst = new CompactionRequest("default", "camtc", CompactionType.MAJOR);
    txnHandler.compact(rqst);
    startWorker();

    // open another txn, leave it open. shouldn't affect anything – cleaner should run.
    long openTxn2 = openTxn();

    // Clean. Check that the files are NOT removed
    startCleaner();
    List<Path> paths = getDirectories(conf, t, null);
    Assert.assertEquals(5, paths.size());

    // Check that the compaction is still in the queue, with status "ready for cleaning" 
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, rsp.getCompacts().get(0).getState());

    // abort open txn 1 (doesn't matter if aborted or committed, the point is it's no longer open)
    txnHandler.abortTxn(new AbortTxnRequest(openTxn));

    // Clean again
    startCleaner();

    // Check there are no compactions requests left and that the files are removed, leaving only the base dir
    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());
    paths = getDirectories(conf, t, null);
    Assert.assertEquals(1, paths.size());
    Assert.assertEquals("base_0000025_v0000027", paths.get(0).getName());

    //cleanup
    txnHandler.abortTxn(new AbortTxnRequest(openTxn2));
  }

  /**
   * Same as previous, except we run 2 compactions, then start the cleaning process. Test that the first CQ entry gets 
   * cleaned up to its base dir, then the second gets cleaned up to its base dir.
   * @throws Exception
   */
  @Test public void testCleanerStaysInQueueUntilItCanRunMultipleCompactions() throws Exception {
    Table t = newTable("default", "camtc", false);

    // write into table
    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);
    addDeltaFile(t, null, 24L, 25L, 2);
    burnThroughTransactions("default", "camtc", 25);

    // compact, don't clean
    CompactionRequest rqst = new CompactionRequest("default", "camtc", CompactionType.MAJOR);
    txnHandler.compact(rqst);
    startWorker();

    // open a txn, leave it open
    long openTxn = openTxn();

    // write another couple deltas
    addDeltaFile(t, null, 26L, 27L, 2);
    addDeltaFile(t, null, 28L, 29L, 2);
    burnThroughTransactions("default", "camtc", 4);

    // compact
    txnHandler.compact(rqst);
    startWorker();

    // check directories before cleaning
    List<Path> paths = getDirectories(conf, t, null);
    Assert.assertEquals(8, paths.size());
    Collections.sort(paths);
    Assert.assertEquals("base_0000025_v0000026", paths.get(0).getName());
    Assert.assertEquals("base_0000029_v0000032", paths.get(1).getName());
    Assert.assertEquals("base_20", paths.get(2).getName());
    Assert.assertEquals("delta_0000021_0000022", paths.get(3).getName());
    Assert.assertEquals("delta_0000023_0000024", paths.get(4).getName());
    Assert.assertEquals("delta_0000024_0000025", paths.get(5).getName());
    Assert.assertEquals("delta_0000026_0000027", paths.get(6).getName());
    Assert.assertEquals("delta_0000028_0000029", paths.get(7).getName());

    // clean
    startCleaner();

    // Check that the new files are NOT removed, but the old ones (writeId<=25) are removed
    paths = getDirectories(conf, t, null);
    Assert.assertEquals(4, paths.size());
    Collections.sort(paths);
    Assert.assertEquals("base_0000025_v0000026", paths.get(0).getName());
    Assert.assertEquals("base_0000029_v0000032", paths.get(1).getName());
    Assert.assertEquals("delta_0000026_0000027", paths.get(2).getName());
    Assert.assertEquals("delta_0000028_0000029", paths.get(3).getName());
    

    // Check that the second compaction is still in the queue, with status "ready for cleaning" 
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(2, rsp.getCompactsSize());
    List<ShowCompactResponseElement> showCompactResponseElements = rsp.getCompacts();
    Collections.sort(showCompactResponseElements);
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, showCompactResponseElements.get(0).getState());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, showCompactResponseElements.get(1).getState());

    // abort the open txn (doesn't matter if aborted or committed, the point is it's no longer open)
    txnHandler.abortTxn(new AbortTxnRequest(openTxn));

    // clean again
    startCleaner();

    // Check there are no compactions requests left.
    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(2, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(1).getState());

    // Check that all files are removed except the last base
    paths = getDirectories(conf, t, null);
    Assert.assertEquals(1, paths.size());
    Assert.assertEquals("base_0000029_v0000032", paths.get(0).getName());
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
