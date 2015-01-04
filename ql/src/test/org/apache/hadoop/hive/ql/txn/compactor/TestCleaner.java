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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreThread;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tests for the compactor Cleaner thread
 */
public class TestCleaner extends CompactorTest {

  static final private Log LOG = LogFactory.getLog(TestCleaner.class.getName());

  public TestCleaner() throws Exception {
    super();
  }

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

    burnThroughTransactions(25);

    CompactionRequest rqst = new CompactionRequest("default", "camtc", CompactionType.MAJOR);
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    txnHandler.markCompacted(ci);
    txnHandler.setRunAs(ci.id, System.getProperty("user.name"));

    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(0, rsp.getCompactsSize());

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

    burnThroughTransactions(25);

    CompactionRequest rqst = new CompactionRequest("default", "campc", CompactionType.MAJOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    txnHandler.markCompacted(ci);
    txnHandler.setRunAs(ci.id, System.getProperty("user.name"));

    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(0, rsp.getCompactsSize());

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

    burnThroughTransactions(25);

    CompactionRequest rqst = new CompactionRequest("default", "camitc", CompactionType.MINOR);
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    txnHandler.markCompacted(ci);
    txnHandler.setRunAs(ci.id, System.getProperty("user.name"));

    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(0, rsp.getCompactsSize());

    // Check that the files are removed
    List<Path> paths = getDirectories(conf, t, null);
    Assert.assertEquals(2, paths.size());
    boolean sawBase = false, sawDelta = false;
    for (Path p : paths) {
      if (p.getName().equals("base_20")) sawBase = true;
      else if (p.getName().equals("delta_21_24")) sawDelta = true;
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

    burnThroughTransactions(25);

    CompactionRequest rqst = new CompactionRequest("default", "camipc", CompactionType.MINOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    txnHandler.markCompacted(ci);
    txnHandler.setRunAs(ci.id, System.getProperty("user.name"));

    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(0, rsp.getCompactsSize());

    // Check that the files are removed
    List<Path> paths = getDirectories(conf, t, p);
    Assert.assertEquals(2, paths.size());
    boolean sawBase = false, sawDelta = false;
    for (Path path : paths) {
      if (path.getName().equals("base_20")) sawBase = true;
      else if (path.getName().equals("delta_21_24")) sawDelta = true;
      else Assert.fail("Unexpected file " + path.getName());
    }
    Assert.assertTrue(sawBase);
    Assert.assertTrue(sawDelta);
  }

  @Test
  public void blockedByLockTable() throws Exception {
    Table t = newTable("default", "bblt", false);

    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);
    addDeltaFile(t, null, 21L, 24L, 4);

    burnThroughTransactions(25);

    CompactionRequest rqst = new CompactionRequest("default", "bblt", CompactionType.MINOR);
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    txnHandler.markCompacted(ci);
    txnHandler.setRunAs(ci.id, System.getProperty("user.name"));

    LockComponent comp = new LockComponent(LockType.SHARED_READ, LockLevel.TABLE, "default");
    comp.setTablename("bblt");
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);

    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("ready for cleaning", compacts.get(0).getState());
    Assert.assertEquals("bblt", compacts.get(0).getTablename());
    Assert.assertEquals(CompactionType.MINOR, compacts.get(0).getType());
  }

  @Test
  public void blockedByLockPartition() throws Exception {
    Table t = newTable("default", "bblp", true);
    Partition p = newPartition(t, "today");

    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 22L, 2);
    addDeltaFile(t, p, 23L, 24L, 2);
    addDeltaFile(t, p, 21L, 24L, 4);

    burnThroughTransactions(25);

    CompactionRequest rqst = new CompactionRequest("default", "bblp", CompactionType.MINOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    txnHandler.markCompacted(ci);
    txnHandler.setRunAs(ci.id, System.getProperty("user.name"));

    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.PARTITION, "default");
    comp.setTablename("bblp");
    comp.setPartitionname("ds=today");
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);

    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());
    Assert.assertEquals("ready for cleaning", compacts.get(0).getState());
    Assert.assertEquals("bblp", compacts.get(0).getTablename());
    Assert.assertEquals("ds=today", compacts.get(0).getPartitionname());
    Assert.assertEquals(CompactionType.MINOR, compacts.get(0).getType());
  }

  @Test
  public void notBlockedBySubsequentLock() throws Exception {
    Table t = newTable("default", "bblt", false);

    // Set the run frequency low on this test so it doesn't take long
    conf.setTimeVar(HiveConf.ConfVars.HIVE_COMPACTOR_CLEANER_RUN_INTERVAL, 100,
        TimeUnit.MILLISECONDS);

    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);
    addDeltaFile(t, null, 21L, 24L, 4);

    burnThroughTransactions(25);

    CompactionRequest rqst = new CompactionRequest("default", "bblt", CompactionType.MINOR);
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    txnHandler.markCompacted(ci);
    txnHandler.setRunAs(ci.id, System.getProperty("user.name"));

    LockComponent comp = new LockComponent(LockType.SHARED_READ, LockLevel.TABLE, "default");
    comp.setTablename("bblt");
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);

    AtomicBoolean looped = new AtomicBoolean();
    looped.set(false);
    startCleaner(looped);

    // Make sure the compactor has a chance to run once
    while (!looped.get()) {
      Thread.currentThread().sleep(100);
    }

    // There should still be one request, as the locks still held.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());

    // obtain a second lock.  This shouldn't block cleaner as it was acquired after the initial
    // clean request
    LockComponent comp2 = new LockComponent(LockType.SHARED_READ, LockLevel.TABLE, "default");
    comp2.setTablename("bblt");
    List<LockComponent> components2 = new ArrayList<LockComponent>(1);
    components2.add(comp2);
    LockRequest req2 = new LockRequest(components, "me", "localhost");
    LockResponse res2 = txnHandler.lock(req2);

    // Unlock the previous lock
    txnHandler.unlock(new UnlockRequest(res.getLockid()));
    looped.set(false);

    while (!looped.get()) {
      Thread.currentThread().sleep(100);
    }
    stopThread();
    Thread.currentThread().sleep(200);


    // Check there are no compactions requests left.
    rsp = txnHandler.showCompact(new ShowCompactRequest());
    compacts = rsp.getCompacts();
    Assert.assertEquals(0, compacts.size());
  }

  @Test
  public void partitionNotBlockedBySubsequentLock() throws Exception {
    Table t = newTable("default", "bblt", true);
    Partition p = newPartition(t, "today");

    // Set the run frequency low on this test so it doesn't take long
    conf.setTimeVar(HiveConf.ConfVars.HIVE_COMPACTOR_CLEANER_RUN_INTERVAL, 100,
        TimeUnit.MILLISECONDS);

    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 22L, 2);
    addDeltaFile(t, p, 23L, 24L, 2);
    addDeltaFile(t, p, 21L, 24L, 4);

    burnThroughTransactions(25);

    CompactionRequest rqst = new CompactionRequest("default", "bblt", CompactionType.MINOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    txnHandler.markCompacted(ci);
    txnHandler.setRunAs(ci.id, System.getProperty("user.name"));

    LockComponent comp = new LockComponent(LockType.SHARED_READ, LockLevel.PARTITION, "default");
    comp.setTablename("bblt");
    comp.setPartitionname("ds=today");
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);

    AtomicBoolean looped = new AtomicBoolean();
    looped.set(false);
    startCleaner(looped);

    // Make sure the compactor has a chance to run once
    while (!looped.get()) {
      Thread.currentThread().sleep(100);
    }

    // There should still be one request, as the locks still held.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(1, compacts.size());


    // obtain a second lock.  This shouldn't block cleaner as it was acquired after the initial
    // clean request
    LockComponent comp2 = new LockComponent(LockType.SHARED_READ, LockLevel.PARTITION, "default");
    comp2.setTablename("bblt");
    comp2.setPartitionname("ds=today");
    List<LockComponent> components2 = new ArrayList<LockComponent>(1);
    components2.add(comp2);
    LockRequest req2 = new LockRequest(components, "me", "localhost");
    LockResponse res2 = txnHandler.lock(req2);

    // Unlock the previous lock
    txnHandler.unlock(new UnlockRequest(res.getLockid()));
    looped.set(false);

    while (!looped.get()) {
      Thread.currentThread().sleep(100);
    }
    stopThread();
    Thread.currentThread().sleep(200);


    // Check there are no compactions requests left.
    rsp = txnHandler.showCompact(new ShowCompactRequest());
    compacts = rsp.getCompacts();
    Assert.assertEquals(0, compacts.size());
  }

  @Test
  public void cleanupAfterMajorPartitionCompactionNoBase() throws Exception {
    Table t = newTable("default", "campcnb", true);
    Partition p = newPartition(t, "today");

    addDeltaFile(t, p, 1L, 22L, 22);
    addDeltaFile(t, p, 23L, 24L, 2);
    addBaseFile(t, p, 25L, 25);

    burnThroughTransactions(25);

    CompactionRequest rqst = new CompactionRequest("default", "campcnb", CompactionType.MAJOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    txnHandler.markCompacted(ci);
    txnHandler.setRunAs(ci.id, System.getProperty("user.name"));

    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(0, rsp.getCompactsSize());

    // Check that the files are removed
    List<Path> paths = getDirectories(conf, t, p);
    Assert.assertEquals(1, paths.size());
    Assert.assertEquals("base_25", paths.get(0).getName());
  }
}
