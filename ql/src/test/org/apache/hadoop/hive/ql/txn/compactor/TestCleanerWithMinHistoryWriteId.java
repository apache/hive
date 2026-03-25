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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.hadoop.hive.metastore.txn.TxnStore.FAILED_RESPONSE;
import static org.apache.hadoop.hive.metastore.txn.TxnStore.SUCCEEDED_RESPONSE;
import static org.apache.hadoop.hive.metastore.txn.TxnStore.WORKING_RESPONSE;
import static org.apache.hadoop.hive.metastore.txn.TxnStore.INITIATED_STATE;
import static org.apache.hadoop.hive.metastore.txn.TxnStore.WORKING_STATE;

import static org.apache.hadoop.hive.ql.io.AcidUtils.addVisibilitySuffix;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestCleanerWithMinHistoryWriteId extends TestCleaner {

  @BeforeEach
  @Override
  public void setup() throws Exception {
    super.setup();
    MetastoreConf.setLongVar(conf, ConfVars.HIVE_COMPACTOR_CLEANER_MAX_RETRY_ATTEMPTS, 0);
  }

  @Override
  protected boolean useMinHistoryWriteId() {
    return true;
  }

  @Test
  public void cleanupAfterAbortedAndRetriedMajorCompaction() throws Exception {
    Table t = prepareTestTable();
    CompactionRequest rqst = new CompactionRequest("default", "camtc", CompactionType.MAJOR);
    long compactTxn = compactInTxn(rqst, CommitAction.ABORT);
    addBaseFile(t, null, 25L, 25, compactTxn);

    txnHandler.revokeTimedoutWorkers(1L);
    compactTxn = compactInTxn(rqst);
    addBaseFile(t, null, 25L, 25, compactTxn);

    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    assertEquals(1, rsp.getCompactsSize());
    assertEquals(SUCCEEDED_RESPONSE, rsp.getCompacts().getFirst().getState());

    // Check that the files are removed
    List<Path> paths = getDirectories(conf, t, null);
    assertEquals(1, paths.size());
    assertEquals(addVisibilitySuffix("base_25", 27), paths.getFirst().getName());
  }

  @Test
  public void cleanupAfterKilledAndRetriedMajorCompaction() throws Exception {
    Table t = prepareTestTable();
    CompactionRequest rqst = new CompactionRequest("default", "camtc", CompactionType.MAJOR);
    long compactTxn = compactInTxn(rqst, CommitAction.NONE);
    addBaseFile(t, null, 25L, 25, compactTxn);

    txnHandler.revokeTimedoutWorkers(1L);
    // an open txn should prevent the retry
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    assertEquals(1, rsp.getCompactsSize());
    assertEquals(WORKING_RESPONSE, rsp.getCompacts().getFirst().getState());

    // force retry
    revokeTimedoutWorkers(conf);
    compactTxn = compactInTxn(rqst);
    addBaseFile(t, null, 25L, 25, compactTxn);

    startCleaner();

    // Validate that the cleanup attempt has failed.
    rsp = txnHandler.showCompact(new ShowCompactRequest());
    assertEquals(1, rsp.getCompactsSize());
    assertEquals(FAILED_RESPONSE, rsp.getCompacts().getFirst().getState());
    assertEquals("txnid:26 is open and <= hwm: 27", rsp.getCompacts().getFirst().getErrorMessage());

    // Check that the files are not removed
    List<Path> paths = getDirectories(conf, t, null);
    assertEquals(6, paths.size());
  }

  private static void revokeTimedoutWorkers(Configuration conf) throws Exception {
    TestTxnDbUtil.executeUpdate(conf, """
          UPDATE "COMPACTION_QUEUE"
          SET "CQ_WORKER_ID" = NULL, "CQ_START" = NULL, "CQ_STATE" = '%c'
          WHERE "CQ_STATE" = '%c'
        """.formatted(INITIATED_STATE, WORKING_STATE));
  }

  @Test
  public void cleanupAfterMajorCompactionWithQueryWaitingToLockTheSnapshot() throws Exception {
    Table t = prepareTestTable();
    CompactionRequest rqst = new CompactionRequest("default", "camtc", CompactionType.MAJOR);
    long compactTxn = compactInTxn(rqst, CommitAction.MARK_COMPACTED);
    addBaseFile(t, null, 25L, 25, compactTxn);

    // Open a query during compaction,
    // Do not register minOpenWriteId (i.e. simulate delay locking the snapshot)
    openTxn();

    txnHandler.commitTxn(new CommitTxnRequest(compactTxn));
    startCleaner();

    // Validate that the cleanup attempt has failed.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    assertEquals(1, rsp.getCompactsSize());
    assertEquals(FAILED_RESPONSE, rsp.getCompacts().getFirst().getState());
    assertEquals("txnid:27 is open and <= hwm: 27", rsp.getCompacts().getFirst().getErrorMessage());

    // Check that the files are not removed
    List<Path> paths = getDirectories(conf, t, null);
    assertEquals(5, paths.size());
  }

  private Table prepareTestTable() throws Exception {
    Table t = newTable("default", "camtc", false);

    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);
    addDeltaFile(t, null, 25L, 25, 2);

    burnThroughTransactions("default", "camtc", 25);
    return t;
  }

}
