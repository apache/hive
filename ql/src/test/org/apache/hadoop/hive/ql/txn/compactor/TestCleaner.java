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
import org.apache.hadoop.hive.common.ValidCompactorWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.FindNextCompactRequest;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnCommonUtils;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.txn.compactor.handler.TaskHandler;
import org.apache.hadoop.hive.ql.txn.compactor.handler.TaskHandlerFactory;
import org.apache.hive.common.util.ReflectionUtil;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_COMPACTOR_CLEANER_RETENTION_TIME;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_COMPACTOR_DELAYED_CLEANUP_ENABLED;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.HIVE_COMPACTOR_CLEANER_RETRY_RETENTION_TIME;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.getTimeVar;
import static org.apache.hadoop.hive.ql.io.AcidUtils.addVisibilitySuffix;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

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
  public void testRetryAfterFailedCleanupDelayEnabled() throws Exception {
    testRetryAfterFailedCleanup(true);
  }

  @Test
  public void testRetryAfterFailedCleanupDelayDisabled() throws Exception {
    testRetryAfterFailedCleanup(false);
  }

  public void testRetryAfterFailedCleanup(boolean delayEnabled) throws Exception {
    HiveConf.setBoolVar(conf, HIVE_COMPACTOR_DELAYED_CLEANUP_ENABLED, delayEnabled);
    HiveConf.setTimeVar(conf, HIVE_COMPACTOR_CLEANER_RETENTION_TIME, 2, TimeUnit.SECONDS);
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.HIVE_COMPACTOR_CLEANER_MAX_RETRY_ATTEMPTS, 3);
    MetastoreConf.setTimeVar(conf, MetastoreConf.ConfVars.HIVE_COMPACTOR_CLEANER_RETRY_RETENTION_TIME, 100, TimeUnit.MILLISECONDS);
    String errorMessage = "No cleanup here!";

    //Prevent cleaner from marking the compaction as cleaned
    TxnStore mockedHandler = spy(txnHandler);
    MetadataCache metadataCache = new MetadataCache(true);
    FSRemover fsRemover = new FSRemover(conf, ReplChangeManager.getInstance(conf), metadataCache);
    List<TaskHandler> taskHandlers = TaskHandlerFactory.getInstance()
            .getHandlers(conf, mockedHandler, metadataCache, false, fsRemover);
    doThrow(new RuntimeException(errorMessage)).when(mockedHandler).markCleaned(nullable(CompactionInfo.class));

    Table t = newTable("default", "retry_test", false);

    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);
    burnThroughTransactions("default", "retry_test", 25);

    CompactionRequest rqst = new CompactionRequest("default", "retry_test", CompactionType.MAJOR);
    long compactTxn = compactInTxn(rqst);
    addBaseFile(t, null, 25L, 25, compactTxn);

    //delayed start retention time
    long retentionTime = delayEnabled ? conf.getTimeVar(HIVE_COMPACTOR_CLEANER_RETENTION_TIME, TimeUnit.MILLISECONDS) : 0;
    //retry retention time
    long retryRetentionTime = getTimeVar(conf, HIVE_COMPACTOR_CLEANER_RETRY_RETENTION_TIME, TimeUnit.MILLISECONDS);
    // Sleep 100ms longer than the actual retention to make sure the compaction will be picked by the cleaner
    Thread.sleep(retentionTime + 100);

    for (int i = 1; i < 4; i++) {
      Cleaner cleaner = new Cleaner();
      cleaner.setConf(conf);
      cleaner.init(new AtomicBoolean(true));
      cleaner.setCleanupHandlers(taskHandlers);

      ReflectionUtil.setInAllFields(cleaner, "txnHandler", mockedHandler);

      cleaner.run();

      // Sleep 100ms longer than the actual retry retention to make sure the compaction will be picked up again by the cleaner
      Thread.sleep(retryRetentionTime * (long) Math.pow(2, i - 1) + 100);

      // Check retry attempts updated
      Optional<CompactionInfo> compactionByTxnId = txnHandler.getCompactionByTxnId(compactTxn);
      Assert.assertTrue("Expected compactionInfo, but got nothing returned", compactionByTxnId.isPresent());
      CompactionInfo ci = compactionByTxnId.get();

      // Check if state is still 'ready for cleaning'
      Assert.assertEquals(String.format("Expected 'r' (ready for cleaning) state, but got: '%c'", ci.state), 'r', ci.state);
      // Check if error message was set correctly
      Assert.assertEquals(String.format("Expected error message: '%s', but got '%s'", errorMessage, ci.errorMessage),
          errorMessage, ci.errorMessage);
      // Check if retentionTime was set correctly
      int cleanAttempts = (int)(Math.log(ci.retryRetention / retryRetentionTime) / Math.log(2)) + 1;
      Assert.assertEquals(String.format("Expected %d clean attempts, but got %d", i, cleanAttempts), i, cleanAttempts);
    }

    //Do a final run to reach the maximum retry attempts, so the state finally should be set to failed
    Cleaner cleaner = new Cleaner();
    cleaner.setConf(conf);
    cleaner.init(new AtomicBoolean(true));
    cleaner.setCleanupHandlers(taskHandlers);
    ReflectionUtil.setInAllFields(cleaner, "txnHandler", mockedHandler);

    cleaner.run();

    ShowCompactResponse scr = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(String.format("Expected %d CompactionInfo, but got %d", 1, scr.getCompactsSize()),
        1, scr.getCompactsSize());
    ShowCompactResponseElement scre = scr.getCompacts().get(0);
    //The state finally should be set to failed.
    Assert.assertEquals(String.format("Expected '%s' state, but got '%s'", "failed", scre.getState()),
        "failed", scre.getState());
    Assert.assertEquals(String.format("Expected error message: '%s', but got '%s'", errorMessage, scre.getErrorMessage()),
        errorMessage, scre.getErrorMessage());
  }

  @Test
  public void testRetentionAfterFailedCleanup() throws Exception {
    conf.setBoolVar(HIVE_COMPACTOR_DELAYED_CLEANUP_ENABLED, false);

    Table t = newTable("default", "retry_test", false);

    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);
    burnThroughTransactions("default", "retry_test", 25);

    CompactionRequest rqst = new CompactionRequest("default", "retry_test", CompactionType.MAJOR);
    long compactTxn = compactInTxn(rqst);
    addBaseFile(t, null, 25L, 25, compactTxn);

    //Prevent cleaner from marking the compaction as cleaned
    TxnStore mockedHandler = spy(txnHandler);
    MetadataCache metadataCache = new MetadataCache(true);
    FSRemover fsRemover = new FSRemover(conf, ReplChangeManager.getInstance(conf), metadataCache);
    List<TaskHandler> taskHandlers = TaskHandlerFactory.getInstance()
            .getHandlers(conf, mockedHandler, metadataCache, false, fsRemover);
    doThrow(new RuntimeException()).when(mockedHandler).markCleaned(nullable(CompactionInfo.class));

    //Do a run to fail the clean and set the retention time
    Cleaner cleaner = new Cleaner();
    cleaner.setConf(conf);
    cleaner.init(new AtomicBoolean(true));
    cleaner.setCleanupHandlers(taskHandlers);
    ReflectionUtil.setInAllFields(cleaner, "txnHandler", mockedHandler);

    cleaner.run();

    AtomicReference<List<CompactionInfo>> reference = new AtomicReference<>();
    doAnswer(invocation -> {
      Object o = invocation.callRealMethod();
      reference.set((List<CompactionInfo>) o);
      return o;
    }).when(mockedHandler).findReadyToClean(anyLong(), anyLong());

    //Do a final run and check if the compaction is not picked up again
    cleaner = new Cleaner();
    cleaner.setConf(conf);
    cleaner.init(new AtomicBoolean(true));
    cleaner.setCleanupHandlers(taskHandlers);
    ReflectionUtil.setInAllFields(cleaner, "txnHandler", mockedHandler);

    cleaner.run();

    Assert.assertEquals(0, reference.get().size());
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
    Assert.assertEquals(addVisibilitySuffix("base_25", 26), paths.get(0).getName());
  }

  @Test
  public void cleanupAfterIOWAndMajorTableCompaction() throws Exception {
    Table t = newTable("default", "camtc", false);

    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);
    addBaseFile(t, null, 25L, 25); //IOW

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
    Assert.assertEquals(addVisibilitySuffix("base_25", 26), paths.get(0).getName());
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

    FindNextCompactRequest findNextCompactRequest = new FindNextCompactRequest();
    findNextCompactRequest.setWorkerId("fred");
    findNextCompactRequest.setWorkerVersion(WORKER_VERSION);
    CompactionInfo ci = txnHandler.findNextToCompact(findNextCompactRequest);
    ci.runAs = System.getProperty("user.name");
    long compactTxn = openTxn(TxnType.COMPACTION);

    ValidTxnList validTxnList = TxnCommonUtils.createValidReadTxnList(
        txnHandler.getOpenTxns(Collections.singletonList(TxnType.READ_ONLY)), compactTxn);
    GetValidWriteIdsRequest validWriteIdsRqst = new GetValidWriteIdsRequest(Collections.singletonList(ci.getFullTableName()));
    validWriteIdsRqst.setValidTxnList(validTxnList.writeToString());

    ValidCompactorWriteIdList tblValidWriteIds = TxnUtils.createValidCompactWriteIdList(
        txnHandler.getValidWriteIds(validWriteIdsRqst).getTblValidWriteIds().get(0));
    ci.highestWriteId = tblValidWriteIds.getHighWatermark();
    txnHandler.updateCompactorState(ci, compactTxn);

    txnHandler.markCompacted(ci);
    // Open a query during compaction
    long longQuery = openTxn();
    if (useMinHistoryWriteId()) {
      allocateTableWriteId("default", "camtc", longQuery);
    }
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
    Assert.assertEquals(addVisibilitySuffix("base_25", 26), paths.get(0).getName());
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
      if (p.getName().equals("base_20")) {
        sawBase = true;
      } else if (p.getName().equals(makeDeltaDirName(21, 24))) {
        sawDelta = true;
      } else {
        Assert.fail("Unexpected file " + p.getName());
      }
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
    for (int i = 0; i < 10; i++) {
      p = newPartition(t, "today" + i);

      addBaseFile(t, p, 20L, 20);
      addDeltaFile(t, p, 21L, 22L, 2);
      addDeltaFile(t, p, 23L, 24L, 2);
      addDeltaFile(t, p, 21L, 24L, 4);
      partitions.add(p);
    }

    burnThroughTransactions("default", "camipc", 25);
    for (int i = 0; i < 10; i++) {
      CompactionRequest rqst = new CompactionRequest("default", "camipc", CompactionType.MINOR);
      rqst.setPartitionname("ds=today" + i);
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
    Assert.assertEquals(addVisibilitySuffix("base_23", 25), paths.get(0).getName());
  }

  @Test
  public void testReadyForCleaningPileup() throws Exception {
    String dbName = "default";
    String tblName = "trfcp";
    String partName = "ds=today";
    Table t = newTable(dbName, tblName, true);
    Partition p = newPartition(t, "today");

    // block cleaner with an open txn
    long blockingTxn = openTxn();

    // minor compaction
    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 21L, 1);
    addDeltaFile(t, p, 22L, 22L, 1);
    burnThroughTransactions(dbName, tblName, 22);
    CompactionRequest rqst = new CompactionRequest(dbName, tblName, CompactionType.MINOR);
    rqst.setPartitionname(partName);
    long compactTxn = compactInTxn(rqst);
    addDeltaFile(t, p, 21, 22, 2);

    txnHandler.addWriteIdsToMinHistory(1, Collections.singletonMap("default.trfcp", 23L));
    startCleaner();

    // make sure cleaner didn't remove anything, and cleaning is still queued
    List<Path> paths = getDirectories(conf, t, p);
    Assert.assertEquals("Expected 4 files after minor compaction, instead these files were present " + paths,
      4, paths.size());
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Expected 1 compaction in queue, got: " + rsp.getCompacts(), 1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, rsp.getCompacts().get(0).getState());
    Assert.assertEquals(CompactionType.MINOR, rsp.getCompacts().get(0).getType());

    // major compaction
    addDeltaFile(t, p, 23L, 23L, 1);
    addDeltaFile(t, p, 24L, 24L, 1);
    burnThroughTransactions(dbName, tblName, 2, null, new HashSet<>(Collections.singletonList(compactTxn + 1)));
    rqst = new CompactionRequest(dbName, tblName, CompactionType.MAJOR);
    rqst.setPartitionname(partName);
    compactTxn = compactInTxn(rqst);
    addBaseFile(t, p, 24, 24, compactTxn);
    startCleaner();

    // make sure cleaner didn't remove anything, and 2 cleaning are still queued
    paths = getDirectories(conf, t, p);
    Assert.assertEquals("Expected 7 files after minor compaction, instead these files were present " + paths,
      7, paths.size());
    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Expected 2 compactions in queue, got: " + rsp.getCompacts(), 2, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, rsp.getCompacts().get(0).getState());
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, rsp.getCompacts().get(1).getState());

    // unblock the cleaner and run again
    txnHandler.commitTxn(new CommitTxnRequest(blockingTxn));
    startCleaner();
    startCleaner();

    // make sure cleaner removed everything below base_24, and both compactions are successful
    paths = getDirectories(conf, t, p);
    Assert.assertEquals(1, paths.size());
    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Expected 2 compactions in queue, got: " + rsp.getCompacts(), 2, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(1).getState());
  }


  @Override
  boolean useHive130DeltaDirName() {
    return false;
  }

  @AfterEach
  public void tearDown() throws Exception {
    compactorTestCleanup();
  }

  @Test
  public void noCleanupAfterMajorCompaction() throws Exception {
    Map<String, String> parameters = new HashMap<>();

    //With no cleanup true
    parameters.put("no_cleanup", "true");
    Table t = newTable("default", "dcamc", false, parameters);

    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);
    addBaseFile(t, null, 25L, 25);

    burnThroughTransactions("default", "dcamc", 25);

    CompactionRequest rqst = new CompactionRequest("default", "dcamc", CompactionType.MAJOR);
    compactInTxn(rqst);

    startCleaner();
    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.REFUSED_RESPONSE, rsp.getCompacts().get(0).getState());

    // Check that the files are not removed
    List<Path> paths = getDirectories(conf, t, null);
    Assert.assertEquals(4, paths.size());

    //With no clean up false
    t = ms.getTable(new GetTableRequest("default", "dcamc"));
    t.getParameters().put("no_cleanup", "false");
    ms.alter_table("default", "dcamc", t);
    rqst = new CompactionRequest("default", "dcamc", CompactionType.MAJOR);
    compactInTxn(rqst);

    startCleaner();
    // Check there are no compactions requests left.
    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(2, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());

    // Check that the files are not removed
    paths = getDirectories(conf, t, null);
    Assert.assertEquals(1, paths.size());
    Assert.assertEquals("base_25", paths.get(0).getName());
  }

  @Test
  public void noCleanupAfterMinorCompactionOnPartition() throws Exception {
    Map<String, String> parameters = new HashMap<>();
    parameters.put("NO_CLEANUP", "True");
    Table t = newTable("default", "dcamicop", true);
    Partition p = newPartition(t, "today", null, parameters);

    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 22L, 2);
    addDeltaFile(t, p, 23L, 24L, 2);
    addDeltaFile(t, p, 21L, 24L, 4);

    burnThroughTransactions("default", "dcamicop", 25);

    CompactionRequest rqst = new CompactionRequest("default", "dcamicop", CompactionType.MINOR);
    rqst.setPartitionname("ds=today");
    compactInTxn(rqst);

    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.REFUSED_RESPONSE, rsp.getCompacts().get(0).getState());

    // Check that the files are not removed
    List<Path> paths = getDirectories(conf, t, p);
    Assert.assertEquals(4, paths.size());

    // compaction with no cleanup false
    ArrayList<String> list = new ArrayList<>();
    list.add("ds=today");
    p = ms.getPartition("default", "dcamicop", "ds=today");
    p.getParameters().put("NO_CLEANUP", "false");
    ms.alter_partition("default", "dcamicop", p);
    rqst = new CompactionRequest("default", "dcamicop", CompactionType.MINOR);
    rqst.setPartitionname("ds=today");
    compactInTxn(rqst);

    startCleaner();

    // Check there are no compactions requests left.
    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(2, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());

    // Check that the files are removed
    paths = getDirectories(conf, t, p);
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
  public void withSingleBaseCleanerSucceeds() throws Exception {
    Map<String, String> parameters = new HashMap<>();

    Table t = newTable("default", "dcamc", false, parameters);

    addBaseFile(t, null, 25L, 25);

    burnThroughTransactions("default", "dcamc", 25);

    CompactionRequest rqst = new CompactionRequest("default", "dcamc", CompactionType.MAJOR);
    compactInTxn(rqst);

    startCleaner();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());
  }

  @Test
  public void withNewerBaseCleanerSucceeds() throws Exception {
    Map<String, String> parameters = new HashMap<>();

    Table t = newTable("default", "dcamc", false, parameters);

    addBaseFile(t, null, 25L, 25);

    burnThroughTransactions("default", "dcamc", 25);

    CompactionRequest rqst = new CompactionRequest("default", "dcamc", CompactionType.MAJOR);
    compactInTxn(rqst);

    burnThroughTransactions("default", "dcamc", 1);
    addBaseFile(t, null, 26L, 26);

    startCleaner();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());

    List<Path> paths = getDirectories(conf, t, null);
    // we should retain both 25 and 26
    Assert.assertEquals(2, paths.size());
  }

  @Test
  public void withNotYetVisibleBase() throws Exception {

    String dbName = "default";
    String tableName = "camtc";
    Table t = newTable(dbName, tableName, false);

    addBaseFile(t, null, 20L, 20);
    burnThroughTransactions(dbName, tableName, 25);

    CompactionRequest rqst = new CompactionRequest(dbName, tableName, CompactionType.MAJOR);

    long compactTxn = compactInTxn(rqst);
    addBaseFile(t, null, 25L, 25, compactTxn);
    startCleaner();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());
  }

  @Test
  public void cleanMultipleTimesWithSameWatermark() throws Exception {
    String dbName = "default";
    String tableName = "camtc";
    Table t = newTable(dbName, tableName, false);

    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    burnThroughTransactions(dbName, tableName, 22);

    CompactionRequest rqst = new CompactionRequest(dbName, tableName, CompactionType.MAJOR);
    addBaseFile(t, null, 22L, 22);
    compactInTxn(rqst);

    CompactionResponse response = txnHandler.compact(rqst);

    Assert.assertFalse(response.isAccepted());
    Assert.assertEquals("Compaction is already scheduled with state='ready for cleaning' and id=1", response.getErrormessage());

    startCleaner();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());

    List<Path> paths = getDirectories(conf, t, null);
    Assert.assertEquals(1, paths.size());
    Assert.assertEquals("base_22", paths.get(0).getName());
  }

  @Test
  public void nothingToCleanAfterAbortsBase() throws Exception {
    String dbName = "default";
    String tableName = "camtc";
    Table t = newTable(dbName, tableName, false);

    addBaseFile(t, null, 20L, 1);
    addDeltaFile(t, null, 21L, 21L, 2);
    addDeltaFile(t, null, 22L, 22L, 2);
    burnThroughTransactions(dbName, tableName, 22, null, new HashSet<Long>(Arrays.asList(21L, 22L)));

    CompactionRequest rqst = new CompactionRequest(dbName, tableName, CompactionType.MAJOR);

    compactInTxn(rqst);
    CompactionResponse response = txnHandler.compact(rqst);

    Assert.assertFalse(response.isAccepted());
    Assert.assertEquals("Compaction is already scheduled with state='ready for cleaning' and id=1", response.getErrormessage());

    startCleaner();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());

    List<Path> paths = getDirectories(conf, t, null);
    Assert.assertEquals(1, paths.size());
    Assert.assertEquals("base_20", paths.get(0).getName());
  }

  @Test
  public void nothingToCleanAfterAbortsDelta() throws Exception {
    String dbName = "default";
    String tableName = "camtc";
    Table t = newTable(dbName, tableName, false);

    addDeltaFile(t, null, 20L, 20L, 1);
    addDeltaFile(t, null, 21L, 21L, 2);
    addDeltaFile(t, null, 22L, 22L, 2);
    burnThroughTransactions(dbName, tableName, 22, null, new HashSet<Long>(Arrays.asList(21L, 22L)));

    CompactionRequest rqst = new CompactionRequest(dbName, tableName, CompactionType.MAJOR);

    compactInTxn(rqst);
    CompactionResponse response = txnHandler.compact(rqst);

    Assert.assertFalse(response.isAccepted());
    Assert.assertEquals("Compaction is already scheduled with state='ready for cleaning' and id=1", response.getErrormessage());

    startCleaner();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());

    List<Path> paths = getDirectories(conf, t, null);
    Assert.assertEquals(1, paths.size());
    Assert.assertEquals(makeDeltaDirName(20,20), paths.get(0).getName());
  }

  @Test
  public void testReady() throws Exception {
    String dbName = "default";
    String tblName = "trfcp";
    String partName = "ds=today";
    Table t = newTable(dbName, tblName, true);
    Partition p = newPartition(t, "today");

    // minor compaction
    addBaseFile(t, p, 19L, 19);
    addDeltaFile(t, p, 20L, 20L, 1);
    addDeltaFile(t, p, 21L, 21L, 1);
    addDeltaFile(t, p, 22L, 22L, 1);
    burnThroughTransactions(dbName, tblName, 22);

    // block cleaner with an open txn
    long txnId = openTxn();
    if (useMinHistoryWriteId()) {
      allocateTableWriteId(dbName, tblName, txnId);
    }
    CompactionRequest rqst = new CompactionRequest(dbName, tblName, CompactionType.MINOR);
    rqst.setPartitionname(partName);
    long ctxnid = compactInTxn(rqst);
    addDeltaFile(t, p, 20, 22, 2, ctxnid);
    startCleaner();

    // make sure cleaner didn't remove anything, and cleaning is still queued
    List<Path> paths = getDirectories(conf, t, p);
    Assert.assertEquals("Expected 5 files after minor compaction, instead these files were present " + paths, 5,
        paths.size());
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Expected 1 compaction in queue, got: " + rsp.getCompacts(), 1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, rsp.getCompacts().get(0).getState());
  }

  @Test
  public void testCompactionHighWatermarkIsHonored() throws Exception {
    String dbName = "default";
    String tblName = "trfcp";
    String partName = "ds=today";
    Table t = newTable(dbName, tblName, true);
    Partition p = newPartition(t, "today");

    // minor compaction
    addBaseFile(t, p, 19L, 19);
    addDeltaFile(t, p, 20L, 20L, 1);
    addDeltaFile(t, p, 21L, 21L, 1);
    addDeltaFile(t, p, 22L, 22L, 1);
    burnThroughTransactions(dbName, tblName, 22);

    CompactionRequest rqst = new CompactionRequest(dbName, tblName, CompactionType.MINOR);
    rqst.setPartitionname(partName);
    long ctxnid = compactInTxn(rqst);
    addDeltaFile(t, p, 20, 22, 3, ctxnid);

    // block cleaner with an open txn
    long openTxnId = openTxn();

    //2nd minor
    addDeltaFile(t, p, 23L, 23L, 1);
    addDeltaFile(t, p, 24L, 24L, 1);
    burnThroughTransactions(dbName, tblName, 2);

    rqst = new CompactionRequest(dbName, tblName, CompactionType.MINOR);
    rqst.setPartitionname(partName);
    ctxnid = compactInTxn(rqst);
    addDeltaFile(t, p, 20, 24, 5, ctxnid);

    startCleaner();
    txnHandler.abortTxn(new AbortTxnRequest(openTxnId));

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(2, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, rsp.getCompacts().get(1).getState());

    List<String> actualDirs = getDirectories(conf, t, p).stream()
      .map(Path::getName).sorted()
      .collect(Collectors.toList());
    
    List<String> expectedDirs = Arrays.asList(
      "base_19",
      addVisibilitySuffix(makeDeltaDirName(20, 22), 23),
      addVisibilitySuffix(makeDeltaDirName(20, 24), 27),
      makeDeltaDirName(23, 23),
      makeDeltaDirName(24, 24)
    );
    Assert.assertEquals("Directories do not match", expectedDirs, actualDirs);
  }

  private void allocateTableWriteId(String dbName, String tblName, long txnId) throws Exception {
    AllocateTableWriteIdsRequest awiRqst = new AllocateTableWriteIdsRequest(dbName, tblName);
    awiRqst.setTxnIds(Collections.singletonList(txnId));
    AllocateTableWriteIdsResponse awiResp = txnHandler.allocateTableWriteIds(awiRqst);

    txnHandler.addWriteIdsToMinHistory(txnId, Collections.singletonMap(dbName + "." + tblName,
      awiResp.getTxnToWriteIds().get(0).getWriteId()));
  }
}
