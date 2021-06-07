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
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_COMPACTOR_CLEANER_RETENTION_TIME;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_COMPACTOR_DELAYED_CLEANUP_ENABLED;

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


    burnThroughTransactions("default", "camtc", 25);

    CompactionRequest rqst = new CompactionRequest("default", "camtc", CompactionType.MAJOR);
    long compactTxn = compactInTxn(rqst);
    addBaseFile(t, null, 25L, 25, compactTxn);

    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());

    // Check that the files are removed
    List<Path> paths = getDirectories(conf, t, null);
    Assert.assertEquals(1, paths.size());
    Assert.assertEquals("base_25_v26", paths.get(0).getName());
  }


  @Test
  public void cleanupAfterMajorTableCompactionWithLongRunningQuery() throws Exception {
    Table t = newTable("default", "camtc", false);

    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);
    addBaseFile(t, null, 25L, 25, 26);

    burnThroughTransactions("default", "camtc", 25);

    CompactionRequest rqst = new CompactionRequest("default", "camtc", CompactionType.MAJOR);
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact("fred", "4.0.0");
    ci.runAs = System.getProperty("user.name");
    long compactTxn = openTxn(TxnType.COMPACTION);
    txnHandler.updateCompactorState(ci, compactTxn);
    txnHandler.markCompacted(ci);
    // Open a query during compaction
    long longQuery = openTxn();
    txnHandler.commitTxn(new CommitTxnRequest(compactTxn));

    startCleaner();

    // The long running query should prevent the cleanup
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, rsp.getCompacts().get(0).getState());

    // Check that the files are not removed
    List<Path> paths = getDirectories(conf, t, null);
    Assert.assertEquals(4, paths.size());

    // After the commit cleaning can proceed
    txnHandler.commitTxn(new CommitTxnRequest(longQuery));
    Thread.sleep(MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.TXN_OPENTXN_TIMEOUT, TimeUnit.MILLISECONDS));
    startCleaner();

    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());

    // Check that the files are removed
    paths = getDirectories(conf, t, null);
    Assert.assertEquals(1, paths.size());
    Assert.assertEquals("base_25_v26", paths.get(0).getName());
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
    compactInTxn(rqst);

    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());

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
    compactInTxn(rqst);

    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());

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
    compactInTxn(rqst);

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
    compactInTxn(rqst);

    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());

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
    compactInTxn(rqst);

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
    compactInTxn(rqst);

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
    Partition p;
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
      compactInTxn(rqst);
    }

    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_CLEANER_THREADS_NUM, 3);
    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(10, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());

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

  @Test
  public void delayedCleanupAfterMajorCompaction() throws Exception {
    Table t = newTable("default", "dcamc", false);

    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);
    addBaseFile(t, null, 25L, 25);

    burnThroughTransactions("default", "dcamc", 25);

    CompactionRequest rqst = new CompactionRequest("default", "dcamc", CompactionType.MAJOR);
    compactInTxn(rqst);

    conf.setBoolVar(HIVE_COMPACTOR_DELAYED_CLEANUP_ENABLED, true);
    conf.setTimeVar(HIVE_COMPACTOR_CLEANER_RETENTION_TIME, 5, TimeUnit.SECONDS);

    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, rsp.getCompacts().get(0).getState());

    // putting current thread to sleep to get pass the retention time
    Thread.sleep(conf.getTimeVar(HIVE_COMPACTOR_CLEANER_RETENTION_TIME, TimeUnit.MILLISECONDS));

    startCleaner();
    // Check there are no compactions requests left.
    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());

    // Check that the files are removed
    List<Path> paths = getDirectories(conf, t, null);
    Assert.assertEquals(1, paths.size());
    Assert.assertEquals("base_25", paths.get(0).getName());
  }

  @Test
  public void delayedCleanupAfterMinorCompactionOnPartition() throws Exception {
    Table t = newTable("default", "dcamicop", true);
    Partition p = newPartition(t, "today");

    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 22L, 2);
    addDeltaFile(t, p, 23L, 24L, 2);
    addDeltaFile(t, p, 21L, 24L, 4);

    burnThroughTransactions("default", "dcamicop", 25);

    CompactionRequest rqst = new CompactionRequest("default", "dcamicop", CompactionType.MINOR);
    rqst.setPartitionname("ds=today");
    compactInTxn(rqst);

    conf.setBoolVar(HIVE_COMPACTOR_DELAYED_CLEANUP_ENABLED, true);
    conf.setTimeVar(HIVE_COMPACTOR_CLEANER_RETENTION_TIME, 5, TimeUnit.SECONDS);

    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, rsp.getCompacts().get(0).getState());

    // putting current thread to sleep to get pass the retention time
    Thread.sleep(conf.getTimeVar(HIVE_COMPACTOR_CLEANER_RETENTION_TIME, TimeUnit.MILLISECONDS));

    startCleaner();

    // Check there are no compactions requests left.
    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());

    // Check that the files are removed
    List<Path> paths = getDirectories(conf, t, p);
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

  @Test
  public void delayedCleanupAfterMinorAndMajorCompaction() throws Exception {
    Table t = newTable("default", "dcamimcop", true);
    Partition p = newPartition(t, "today");

    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 21L, 1);
    addDeltaFile(t, p, 22L, 22L, 1);

    burnThroughTransactions("default", "dcamimcop", 22);

    CompactionRequest rqst = new CompactionRequest("default", "dcamimcop", CompactionType.MINOR);
    rqst.setPartitionname("ds=today");
    compactInTxn(rqst);
    addDeltaFile(t, p, 21L, 22L, 2);

    // one more delta after compact
    addDeltaFile(t, p, 23L, 23L, 1);
    burnThroughTransactions("default", "dcamimcop", 1);

    conf.setBoolVar(HIVE_COMPACTOR_DELAYED_CLEANUP_ENABLED, true);
    conf.setTimeVar(HIVE_COMPACTOR_CLEANER_RETENTION_TIME, 5, TimeUnit.SECONDS);

    // putting current thread to sleep to get pass the retention time
    Thread.sleep(conf.getTimeVar(HIVE_COMPACTOR_CLEANER_RETENTION_TIME, TimeUnit.MILLISECONDS));

    rqst = new CompactionRequest("default", "dcamimcop", CompactionType.MAJOR);
    rqst.setPartitionname("ds=today");
    long compactTxn = compactInTxn(rqst);
    addBaseFile(t, p, 23L, 23, compactTxn);

    // This should clean the minor and leave the major, thus it should leave delta_23
    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(2, rsp.getCompactsSize());
    for (ShowCompactResponseElement c : rsp.getCompacts()) {
      if (c.getType() == CompactionType.MAJOR) {
        Assert.assertEquals(TxnStore.CLEANING_RESPONSE, c.getState());
      } else {
        Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, c.getState());
      }
    }

    // Check that the files are removed
    List<Path> paths = getDirectories(conf, t, p);
    // base_20, minor delta, delta_23 and base_23
    Assert.assertEquals(4, paths.size());

    // putting current thread to sleep to get pass the retention time
    Thread.sleep(conf.getTimeVar(HIVE_COMPACTOR_CLEANER_RETENTION_TIME, TimeUnit.MILLISECONDS));

    startCleaner();

    // Check there are no compactions requests left.
    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(2, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(1).getState());

    // Check that the files are removed
    paths = getDirectories(conf, t, p);
    Assert.assertEquals(1, paths.size());
    Assert.assertEquals("base_23_v25", paths.get(0).getName());
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
