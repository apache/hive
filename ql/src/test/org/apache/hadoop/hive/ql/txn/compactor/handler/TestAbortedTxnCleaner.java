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
package org.apache.hadoop.hive.ql.txn.compactor.handler;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.txn.compactor.Cleaner;
import org.apache.hadoop.hive.ql.txn.compactor.CleanupRequest;
import org.apache.hadoop.hive.ql.txn.compactor.FSRemover;
import org.apache.hadoop.hive.ql.txn.compactor.MetadataCache;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;

public class TestAbortedTxnCleaner extends TestHandler {

  @Test
  public void testCleaningOfAbortedDirectoriesForUnpartitionedTables() throws Exception {
    String dbName = "default", tableName = "handler_unpart_test";
    Table t = newTable(dbName, tableName, false);

    // 3-aborted deltas & one committed delta
    addDeltaFileWithTxnComponents(t, null, 2, true);
    addDeltaFileWithTxnComponents(t, null, 2, true);
    addDeltaFileWithTxnComponents(t, null, 2, false);
    addDeltaFileWithTxnComponents(t, null, 2, true);

    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD, 0);
    MetadataCache metadataCache = new MetadataCache(true);
    FSRemover mockedFSRemover = Mockito.spy(new FSRemover(conf, ReplChangeManager.getInstance(conf), metadataCache));
    TaskHandler mockedTaskHandler = Mockito.spy(new AbortedTxnCleaner(conf, txnHandler, metadataCache,
            false, mockedFSRemover));
    Cleaner cleaner = new Cleaner();
    cleaner.setConf(conf);
    cleaner.init(new AtomicBoolean(true));
    cleaner.setCleanupHandlers(Arrays.asList(mockedTaskHandler));
    cleaner.run();

    Mockito.verify(mockedFSRemover, Mockito.times(1)).clean(any(CleanupRequest.class));
    Mockito.verify(mockedTaskHandler, Mockito.times(1)).getTasks();

    List<Path> directories = getDirectories(conf, t, null);
    // All aborted directories removed, hence 1 committed delta directory must be present
    Assert.assertEquals(1, directories.size());
  }

  @Test
  public void testCleaningOfAbortedDirectoriesForSinglePartition() throws Exception {
    String dbName = "default", tableName = "handler_part_single_test", partName = "today";
    Table t = newTable(dbName, tableName, true);
    Partition p = newPartition(t, partName);

    // 3-aborted deltas & one committed delta
    addDeltaFileWithTxnComponents(t, p, 2, true);
    addDeltaFileWithTxnComponents(t, p, 2, true);
    addDeltaFileWithTxnComponents(t, p, 2, false);
    addDeltaFileWithTxnComponents(t, p, 2, true);

    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD, 0);
    MetadataCache metadataCache = new MetadataCache(true);
    FSRemover mockedFSRemover = Mockito.spy(new FSRemover(conf, ReplChangeManager.getInstance(conf), metadataCache));
    TaskHandler mockedTaskHandler = Mockito.spy(new AbortedTxnCleaner(conf, txnHandler, metadataCache,
            false, mockedFSRemover));
    Cleaner cleaner = new Cleaner();
    cleaner.setConf(conf);
    cleaner.init(new AtomicBoolean(true));
    cleaner.setCleanupHandlers(Arrays.asList(mockedTaskHandler));
    cleaner.run();

    Mockito.verify(mockedFSRemover, Mockito.times(1)).clean(any(CleanupRequest.class));
    Mockito.verify(mockedTaskHandler, Mockito.times(1)).getTasks();

    List<Path> directories = getDirectories(conf, t, p);
    // All aborted directories removed, hence 1 committed delta directory must be present
    Assert.assertEquals(1, directories.size());
  }

  @Test
  public void testCleaningOfAbortedDirectoriesForMultiplePartitions() throws Exception {
    String dbName = "default", tableName = "handler_part_multiple_test", partName1 = "today1", partName2 = "today2";
    Table t = newTable(dbName, tableName, true);
    Partition p1 = newPartition(t, partName1);
    Partition p2 = newPartition(t, partName2);

    // 3-aborted deltas & one committed delta for partition-1
    addDeltaFileWithTxnComponents(t, p1, 2, true);
    addDeltaFileWithTxnComponents(t, p1, 2, true);
    addDeltaFileWithTxnComponents(t, p1, 2, false);
    addDeltaFileWithTxnComponents(t, p1, 2, true);

    // 3-aborted deltas & one committed delta for partition-2
    addDeltaFileWithTxnComponents(t, p2, 2, true);
    addDeltaFileWithTxnComponents(t, p2, 2, true);
    addDeltaFileWithTxnComponents(t, p2, 2, false);
    addDeltaFileWithTxnComponents(t, p2, 2, true);

    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD, 0);
    MetadataCache metadataCache = new MetadataCache(true);
    FSRemover mockedFSRemover = Mockito.spy(new FSRemover(conf, ReplChangeManager.getInstance(conf), metadataCache));
    TaskHandler mockedTaskHandler = Mockito.spy(new AbortedTxnCleaner(conf, txnHandler, metadataCache,
            false, mockedFSRemover));
    Cleaner cleaner = new Cleaner();
    cleaner.setConf(conf);
    cleaner.init(new AtomicBoolean(true));
    cleaner.setCleanupHandlers(Arrays.asList(mockedTaskHandler));
    cleaner.run();

    Mockito.verify(mockedFSRemover, Mockito.times(2)).clean(any(CleanupRequest.class));
    Mockito.verify(mockedTaskHandler, Mockito.times(1)).getTasks();

    List<Path> directories = getDirectories(conf, t, p1);
    // All aborted directories removed, hence 1 committed delta directory must be present
    Assert.assertEquals(1, directories.size());

    directories = getDirectories(conf, t, p2);
    // All aborted directories removed, hence 1 committed delta directory must be present
    Assert.assertEquals(1, directories.size());
  }

  @Test
  public void testCleaningOfAbortedDirectoriesWithLongRunningOpenWriteTxn() throws Exception {
    String dbName = "default", tableName = "handler_unpart_open_test";
    Table t = newTable(dbName, tableName, false);

    // 3-aborted deltas & one committed delta
    addDeltaFileWithTxnComponents(t, null, 2, true);
    addDeltaFileWithTxnComponents(t, null, 2, true);
    addDeltaFileWithTxnComponents(t, null, 2, false);
    addDeltaFileWithTxnComponents(t, null, 2, true);

    // Open a long-running transaction
    long openTxnId = openTxn();
    long writeId = ms.allocateTableWriteId(openTxnId, t.getDbName(), t.getTableName());
    acquireLock(t, null, openTxnId);
    addDeltaFile(t, null, writeId, writeId, 2);

    // Add an aborted write after open txn
    addDeltaFileWithTxnComponents(t, null, 2, true);

    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD, 0);
    MetadataCache metadataCache = new MetadataCache(true);
    FSRemover mockedFSRemover = Mockito.spy(new FSRemover(conf, ReplChangeManager.getInstance(conf), metadataCache));
    TaskHandler mockedTaskHandler = Mockito.spy(new AbortedTxnCleaner(conf, txnHandler, metadataCache,
            false, mockedFSRemover));
    Cleaner cleaner = new Cleaner();
    cleaner.setConf(conf);
    cleaner.init(new AtomicBoolean(true));
    cleaner.setCleanupHandlers(Arrays.asList(mockedTaskHandler));
    cleaner.run();

    Mockito.verify(mockedFSRemover, Mockito.times(1)).clean(any(CleanupRequest.class));
    Mockito.verify(mockedTaskHandler, Mockito.times(1)).getTasks();

    List<Path> directories = getDirectories(conf, t, null);
    // All aborted directories below min open write ID are removed,
    // hence 1 open, 1 committed, 1 aborted delta directory must be present
    Assert.assertEquals(3, directories.size());

    // Commit the long open txn
    txnHandler.commitTxn(new CommitTxnRequest(openTxnId));
  }

  @Test
  public void testCleaningOfAbortedDirectoriesOnTopOfBase() throws Exception {
    String dbName = "default", tableName = "handler_unpart_top_test";
    Table t = newTable(dbName, tableName, false);

    // Add 4 committed deltas
    addDeltaFileWithTxnComponents(t, null, 2, false);
    addDeltaFileWithTxnComponents(t, null, 2, false);
    addDeltaFileWithTxnComponents(t, null, 2, false);
    addDeltaFileWithTxnComponents(t, null, 2, false);

    CompactionRequest cr = new CompactionRequest(dbName, tableName, CompactionType.MAJOR);
    txnHandler.compact(cr);

    // Run compaction
    startWorker();

    // Check if there is a one base file
    List<Path> directories = getDirectories(conf, t, null);
    // Both base and delta files are present since we haven't cleaned yet.
    Assert.assertEquals(5, directories.size());
    Assert.assertEquals(1, directories.stream().filter(dir -> dir.getName().startsWith(AcidUtils.BASE_PREFIX)).count());

    // 3 aborted deltas
    addDeltaFileWithTxnComponents(t, null, 2, true);
    addDeltaFileWithTxnComponents(t, null, 2, true);
    addDeltaFileWithTxnComponents(t, null, 2, true);

    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD, 0);
    MetadataCache metadataCache = new MetadataCache(true);
    FSRemover mockedFSRemover = Mockito.spy(new FSRemover(conf, ReplChangeManager.getInstance(conf), metadataCache));
    TaskHandler mockedTaskHandler = Mockito.spy(new AbortedTxnCleaner(conf, txnHandler, metadataCache,
            false, mockedFSRemover));
    Cleaner cleaner = new Cleaner();
    cleaner.setConf(conf);
    cleaner.init(new AtomicBoolean(true));
    cleaner.setCleanupHandlers(Arrays.asList(mockedTaskHandler));
    cleaner.run();

    Mockito.verify(mockedFSRemover, Mockito.times(1)).clean(any(CleanupRequest.class));
    Mockito.verify(mockedTaskHandler, Mockito.times(1)).getTasks();

    directories = getDirectories(conf, t, null);
    Assert.assertEquals(1, directories.size());
    Assert.assertTrue(directories.get(0).getName().startsWith(AcidUtils.BASE_PREFIX));
  }

  @Test
  public void testCleaningOfAbortedDirectoriesBelowBase() throws Exception {
    String dbName = "default", tableName = "handler_unpart_below_test";
    Table t = newTable(dbName, tableName, false);

    // Add 2 committed deltas and 2 aborted deltas
    addDeltaFileWithTxnComponents(t, null, 2, false);
    addDeltaFileWithTxnComponents(t, null, 2, true);
    addDeltaFileWithTxnComponents(t, null, 2, true);
    addDeltaFileWithTxnComponents(t, null, 2, false);

    CompactionRequest cr = new CompactionRequest(dbName, tableName, CompactionType.MAJOR);
    txnHandler.compact(cr);

    // Run compaction
    startWorker();

    // Check if there is a one base file
    List<Path> directories = getDirectories(conf, t, null);
    // Both base and delta files are present since we haven't cleaned yet.
    Assert.assertEquals(5, directories.size());
    Assert.assertEquals(1, directories.stream().filter(dir -> dir.getName().startsWith(AcidUtils.BASE_PREFIX)).count());

    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD, 0);
    MetadataCache metadataCache = new MetadataCache(true);
    FSRemover mockedFSRemover = Mockito.spy(new FSRemover(conf, ReplChangeManager.getInstance(conf), metadataCache));
    TaskHandler mockedTaskHandler = Mockito.spy(new AbortedTxnCleaner(conf, txnHandler, metadataCache,
            false, mockedFSRemover));
    Cleaner cleaner = new Cleaner();
    cleaner.setConf(conf);
    cleaner.init(new AtomicBoolean(true));
    cleaner.setCleanupHandlers(Arrays.asList(mockedTaskHandler));
    cleaner.run();

    Mockito.verify(mockedFSRemover, Mockito.times(1)).clean(any(CleanupRequest.class));
    Mockito.verify(mockedTaskHandler, Mockito.times(1)).getTasks();

    directories = getDirectories(conf, t, null);
    // The table is already compacted, so we must see 1 base delta
    Assert.assertEquals(1, directories.size());
  }

  @Test
  public void testAbortedCleaningWithThreeTxnsWithDiffWriteIds() throws Exception {
    String dbName = "default", tableName = "handler_unpart_writeid_test";
    Table t = newTable(dbName, tableName, false);

    // Add 2 committed deltas and 2 aborted deltas
    addDeltaFileWithTxnComponents(t, null, 2, false);
    addDeltaFileWithTxnComponents(t, null, 2, true);
    addDeltaFileWithTxnComponents(t, null, 2, true);
    addDeltaFileWithTxnComponents(t, null, 2, false);

    long openTxnId1 = openTxn();
    long openTxnId2 = openTxn();
    long openTxnId3 = openTxn();
    long writeId2 = ms.allocateTableWriteId(openTxnId2, t.getDbName(), t.getTableName());
    long writeId3 = ms.allocateTableWriteId(openTxnId3, t.getDbName(), t.getTableName());
    long writeId1 = ms.allocateTableWriteId(openTxnId1, t.getDbName(), t.getTableName());
    assert writeId2 < writeId1 && writeId2 < writeId3;
    acquireLock(t, null, openTxnId3);
    acquireLock(t, null, openTxnId2);
    acquireLock(t, null, openTxnId1);
    addDeltaFile(t, null, writeId3, writeId3, 2);
    addDeltaFile(t, null, writeId1, writeId1, 2);
    addDeltaFile(t, null, writeId2, writeId2, 2);

    ms.abortTxns(Collections.singletonList(openTxnId2));
    ms.commitTxn(openTxnId3);

    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD, 0);
    MetadataCache metadataCache = new MetadataCache(true);
    FSRemover mockedFSRemover = Mockito.spy(new FSRemover(conf, ReplChangeManager.getInstance(conf), metadataCache));
    TaskHandler mockedTaskHandler = Mockito.spy(new AbortedTxnCleaner(conf, txnHandler, metadataCache,
            false, mockedFSRemover));
    Cleaner cleaner = new Cleaner();
    cleaner.setConf(conf);
    cleaner.init(new AtomicBoolean(true));
    cleaner.setCleanupHandlers(Arrays.asList(mockedTaskHandler));
    cleaner.run();

    List<Path> directories = getDirectories(conf, t, null);
    Assert.assertEquals(5, directories.size());
  }
}
