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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for the compactor Cleaner thread
 */
public class TestCleaner extends CompactorTest {
  public TestCleaner() throws Exception {
    super();
  }

  @Test
  public void nothing() throws Exception {
    // Test that the whole things works when there's nothing in the queue.  This is just a
    // survival test.
    startCleaner(new HiveConf());
  }

  @Test
  public void cleanupAfterMajorTableCompaction() throws Exception {
    Table t = newTable("default", "camtc", false);

    HiveConf conf = new HiveConf();

    addBaseFile(conf, t, null, 20L, 20);
    addDeltaFile(conf, t, null, 21L, 22L, 2);
    addDeltaFile(conf, t, null, 23L, 24L, 2);
    addBaseFile(conf, t, null, 25L, 25);

    burnThroughTransactions(25);

    CompactionRequest rqst = new CompactionRequest("default", "camtc", CompactionType.MAJOR);
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    txnHandler.markCompacted(ci);
    txnHandler.setRunAs(ci.id, System.getProperty("user.name"));

    startCleaner(conf);

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertNull(rsp.getCompacts());

    // Check that the files are removed
    List<Path> paths = getDirectories(conf, t, null);
    Assert.assertEquals(1, paths.size());
    Assert.assertEquals("base_25", paths.get(0).getName());
  }

  @Test
  public void cleanupAfterMajorPartitionCompaction() throws Exception {
    Table t = newTable("default", "campc", true);
    Partition p = newPartition(t, "today");

    HiveConf conf = new HiveConf();

    addBaseFile(conf, t, p, 20L, 20);
    addDeltaFile(conf, t, p, 21L, 22L, 2);
    addDeltaFile(conf, t, p, 23L, 24L, 2);
    addBaseFile(conf, t, p, 25L, 25);

    burnThroughTransactions(25);

    CompactionRequest rqst = new CompactionRequest("default", "campc", CompactionType.MAJOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    txnHandler.markCompacted(ci);
    txnHandler.setRunAs(ci.id, System.getProperty("user.name"));

    startCleaner(conf);

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertNull(rsp.getCompacts());

    // Check that the files are removed
    List<Path> paths = getDirectories(conf, t, p);
    Assert.assertEquals(1, paths.size());
    Assert.assertEquals("base_25", paths.get(0).getName());
  }

  @Test
  public void cleanupAfterMinorTableCompaction() throws Exception {
    Table t = newTable("default", "camitc", false);

    HiveConf conf = new HiveConf();

    addBaseFile(conf, t, null, 20L, 20);
    addDeltaFile(conf, t, null, 21L, 22L, 2);
    addDeltaFile(conf, t, null, 23L, 24L, 2);
    addDeltaFile(conf, t, null, 21L, 24L, 4);

    burnThroughTransactions(25);

    CompactionRequest rqst = new CompactionRequest("default", "camitc", CompactionType.MINOR);
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    txnHandler.markCompacted(ci);
    txnHandler.setRunAs(ci.id, System.getProperty("user.name"));

    startCleaner(conf);

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertNull(rsp.getCompacts());

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

    HiveConf conf = new HiveConf();

    addBaseFile(conf, t, p, 20L, 20);
    addDeltaFile(conf, t, p, 21L, 22L, 2);
    addDeltaFile(conf, t, p, 23L, 24L, 2);
    addDeltaFile(conf, t, p, 21L, 24L, 4);

    burnThroughTransactions(25);

    CompactionRequest rqst = new CompactionRequest("default", "camipc", CompactionType.MINOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    txnHandler.markCompacted(ci);
    txnHandler.setRunAs(ci.id, System.getProperty("user.name"));

    startCleaner(conf);

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertNull(rsp.getCompacts());

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

    HiveConf conf = new HiveConf();

    addBaseFile(conf, t, null, 20L, 20);
    addDeltaFile(conf, t, null, 21L, 22L, 2);
    addDeltaFile(conf, t, null, 23L, 24L, 2);
    addDeltaFile(conf, t, null, 21L, 24L, 4);

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

    startCleaner(conf);

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

    HiveConf conf = new HiveConf();

    addBaseFile(conf, t, p, 20L, 20);
    addDeltaFile(conf, t, p, 21L, 22L, 2);
    addDeltaFile(conf, t, p, 23L, 24L, 2);
    addDeltaFile(conf, t, p, 21L, 24L, 4);

    burnThroughTransactions(25);

    CompactionRequest rqst = new CompactionRequest("default", "bblp", CompactionType.MINOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    txnHandler.markCompacted(ci);
    txnHandler.setRunAs(ci.id, System.getProperty("user.name"));

    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "default");
    comp.setTablename("bblp");
    comp.setPartitionname("ds=today");
    List<LockComponent> components = new ArrayList<LockComponent>(1);
    components.add(comp);
    LockRequest req = new LockRequest(components, "me", "localhost");
    LockResponse res = txnHandler.lock(req);

    startCleaner(conf);

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
  public void cleanupAfterMajorPartitionCompactionNoBase() throws Exception {
    Table t = newTable("default", "campcnb", true);
    Partition p = newPartition(t, "today");

    HiveConf conf = new HiveConf();

    addDeltaFile(conf, t, p, 1L, 22L, 22);
    addDeltaFile(conf, t, p, 23L, 24L, 2);
    addBaseFile(conf, t, p, 25L, 25);

    burnThroughTransactions(25);

    CompactionRequest rqst = new CompactionRequest("default", "campcnb", CompactionType.MAJOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);
    CompactionInfo ci = txnHandler.findNextToCompact("fred");
    txnHandler.markCompacted(ci);
    txnHandler.setRunAs(ci.id, System.getProperty("user.name"));

    startCleaner(conf);

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertNull(rsp.getCompacts());

    // Check that the files are removed
    List<Path> paths = getDirectories(conf, t, p);
    Assert.assertEquals(1, paths.size());
    Assert.assertEquals("base_25", paths.get(0).getName());
  }

  @Before
  public void setUpTxnDb() throws Exception {
    TxnDbUtil.setConfValues(new HiveConf());
  }
}
