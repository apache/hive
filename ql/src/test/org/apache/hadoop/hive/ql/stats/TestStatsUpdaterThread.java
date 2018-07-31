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

package org.apache.hadoop.hive.ql.stats;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.ql.DriverUtils;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestStatsUpdaterThread {
  @SuppressWarnings("unused")
  static final private Logger LOG = LoggerFactory.getLogger(TestStatsUpdaterThread.class);
  private static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") +
    File.separator + TestStatsUpdaterThread.class.getCanonicalName()
    + "-" + System.currentTimeMillis()
  ).getPath().replaceAll("\\\\", "/");
  private HiveConf hiveConf;
  private SessionState ss;

  String getTestDataDir() {
    return TEST_DATA_DIR;
  }

  @SuppressWarnings("deprecation")
  @Before
  public void setUp() throws Exception {
    this.hiveConf = new HiveConf(TestStatsUpdaterThread.class);
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, getTestDataDir());
    hiveConf.setVar(HiveConf.ConfVars.HIVEINPUTFORMAT, HiveInputFormat.class.getName());
    hiveConf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
       "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
//    hiveConf.setBoolVar(HiveConf.ConfVars.MERGE_CARDINALITY_VIOLATION_CHECK, true);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER, true);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSCOLAUTOGATHER, true);
    hiveConf.set(MetastoreConf.ConfVars.STATS_AUTO_UPDATE.getVarname(), "all");
    TxnDbUtil.setConfValues(hiveConf);
    TxnDbUtil.prepDb(hiveConf);
    File f = new File(getTestDataDir());
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    if (!(new File(getTestDataDir()).mkdirs())) {
      throw new RuntimeException("Could not create " + getTestDataDir());
    }
    this.ss = DriverUtils.setUpSessionState(hiveConf, "hive", true);
    cleanUp();
  }

  @After
  public void cleanUp() throws HiveException {
    executeQuery("drop table simple_stats");
    executeQuery("drop table simple_stats2");
    executeQuery("drop table simple_stats3");
  }

  @Test(timeout=40000)
  public void testSimpleUpdateWithThreads() throws Exception {
    StatsUpdaterThread su = createUpdater();
    su.startWorkers();
    IMetaStoreClient msClient = new HiveMetaStoreClient(hiveConf);

    executeQuery("create table simple_stats (i int, s string)");
    executeQuery("insert into simple_stats (i, s) values (1, 'test')");
    verifyAndUnsetColStats("simple_stats", Lists.newArrayList("i"), msClient);

    assertTrue(su.runOneIteration());
    su.waitForQueuedCommands();
    verifyStatsUpToDate("simple_stats", Lists.newArrayList("i"), msClient, true);

    msClient.close();
  }

  @Test(timeout=40000)
  public void testMultipleTables() throws Exception {
    StatsUpdaterThread su = createUpdater();
    IMetaStoreClient msClient = new HiveMetaStoreClient(hiveConf);

    executeQuery("create table simple_stats (s string)");
    executeQuery("insert into simple_stats (s) values ('test')");
    executeQuery("create table simple_stats2 (s string)");
    executeQuery("insert into simple_stats2 (s) values ('test2')");
    verifyAndUnsetColStats("simple_stats", Lists.newArrayList("s"), msClient);
    verifyAndUnsetColStats("simple_stats2", Lists.newArrayList("s"), msClient);

    assertTrue(su.runOneIteration());
    drainWorkQueue(su);
    verifyAndUnsetColStats("simple_stats", Lists.newArrayList("s"), msClient);
    verifyAndUnsetColStats("simple_stats2", Lists.newArrayList("s"), msClient);

    setTableSkipProperty(msClient, "simple_stats", "true");
    assertTrue(su.runOneIteration());
    drainWorkQueue(su);
    verifyStatsUpToDate("simple_stats", Lists.newArrayList("i"), msClient, false);
    verifyAndUnsetColStats("simple_stats2", Lists.newArrayList("s"), msClient);

    msClient.close();
  }

  @Test(timeout=80000)
  public void testTxnTable() throws Exception {
    StatsUpdaterThread su = createUpdater();
    IMetaStoreClient msClient = new HiveMetaStoreClient(hiveConf);

    executeQuery("create table simple_stats (s string) TBLPROPERTIES "
        + "(\"transactional\"=\"true\", \"transactional_properties\"=\"insert_only\")");
    executeQuery("insert into simple_stats (s) values ('test')");
    List<String> cols = Lists.newArrayList("s");
    String dbName = ss.getCurrentDatabase(), tblName = "simple_stats", fqName = dbName + "." + tblName;
    ValidWriteIdList initialWriteIds = msClient.getValidWriteIds(fqName);
    verifyStatsUpToDate(tblName, cols, msClient, initialWriteIds.toString(), true);
    assertFalse(su.runOneIteration());
    drainWorkQueue(su, 0);

    executeQuery("insert overwrite table simple_stats values ('test2')");
    ValidWriteIdList nextWriteIds = msClient.getValidWriteIds(fqName);
    verifyStatsUpToDate(tblName, cols, msClient, nextWriteIds.toString(), true);
    assertFalse(su.runOneIteration());
    drainWorkQueue(su, 0);
    String currentWriteIds = msClient.getValidWriteIds(fqName).toString();

    // Overwrite the txn state to refer to an open txn.
    long badTxnId = msClient.openTxn("moo");
    long badWriteId = msClient.allocateTableWriteId(badTxnId, dbName, tblName);

    Table tbl = msClient.getTable(dbName, tblName);
    tbl.setWriteId(badWriteId);
    msClient.alter_table(
        null, dbName, tblName, tbl, new EnvironmentContext(), initialWriteIds.toString());

    // Stats should not be valid.
    verifyStatsUpToDate(tblName, cols, msClient, currentWriteIds, false);

    // Analyze should not be able to set valid stats for a running txn.
    assertTrue(su.runOneIteration());
    drainWorkQueue(su);

    currentWriteIds = msClient.getValidWriteIds(fqName).toString();
    verifyStatsUpToDate(tblName, cols, msClient, currentWriteIds, false);

    msClient.abortTxns(Lists.newArrayList(badTxnId));

    // Analyze should be able to override stats of an aborted txn.
    assertTrue(su.runOneIteration());
    drainWorkQueue(su);

    // Stats will now be valid.
    currentWriteIds = msClient.getValidWriteIds(fqName).toString();
    verifyStatsUpToDate(tblName, cols, msClient, currentWriteIds, true);

    // Verify that incorrect stats from a valid write ID are also handled.
    badTxnId = msClient.openTxn("moo");
    badWriteId = msClient.allocateTableWriteId(badTxnId, dbName, tblName);
    tbl = msClient.getTable(dbName, tblName);
    tbl.setWriteId(badWriteId);
    StatsSetupConst.setBasicStatsState(tbl.getParameters(), StatsSetupConst.FALSE);
    msClient.alter_table(null, dbName, tblName, tbl, new EnvironmentContext(), initialWriteIds.toString());

    // Stats should not be valid.
    verifyStatsUpToDate(tblName, cols, msClient, currentWriteIds, false);

    // Analyze should not be able to set valid stats for a running txn.
    assertTrue(su.runOneIteration());
    drainWorkQueue(su);

    currentWriteIds = msClient.getValidWriteIds(fqName).toString();
    verifyStatsUpToDate(tblName, cols, msClient, currentWriteIds, false);

    msClient.commitTxn(badTxnId);

    // Analyze should be able to override stats of an committed txn.
    assertTrue(su.runOneIteration());
    drainWorkQueue(su);

    // Stats will now be valid.
    currentWriteIds = msClient.getValidWriteIds(fqName).toString();
    verifyStatsUpToDate(tblName, cols, msClient, currentWriteIds, true);

    msClient.close();
  }


  @Test
  public void testTxnPartitions() throws Exception {
    StatsUpdaterThread su = createUpdater();
    IMetaStoreClient msClient = new HiveMetaStoreClient(hiveConf);

    executeQuery("create table simple_stats (s string) partitioned by (p int) TBLPROPERTIES "
        + "(\"transactional\"=\"true\", \"transactional_properties\"=\"insert_only\")");
    executeQuery("insert into simple_stats partition(p=1) values ('test')");
    executeQuery("insert into simple_stats partition(p=2) values ('test2')");
    executeQuery("insert into simple_stats partition(p=3) values ('test3')");
    assertFalse(su.runOneIteration());
    drainWorkQueue(su, 0);

    executeQuery("insert overwrite table simple_stats partition(p=1) values ('test2')");
    executeQuery("insert overwrite table simple_stats partition(p=2) values ('test3')");
    assertFalse(su.runOneIteration());
    drainWorkQueue(su, 0);

    // Overwrite the txn state to refer to an aborted txn on some partitions.
    String dbName = ss.getCurrentDatabase(), tblName = "simple_stats", fqName = dbName + "." + tblName;
    long badTxnId = msClient.openTxn("moo");
    long badWriteId = msClient.allocateTableWriteId(badTxnId, dbName, tblName);
    msClient.abortTxns(Lists.newArrayList(badTxnId));

    Partition part1 = msClient.getPartition(dbName, tblName, "p=1");
    Partition part2 = msClient.getPartition(dbName, tblName, "p=2");
    part1.setWriteId(badWriteId);
    part2.setWriteId(badWriteId);
    String currentWriteIds = msClient.getValidWriteIds(fqName).toString();
    // To update write ID we need to specify the write ID list to validate concurrent writes.
    msClient.alter_partitions(dbName, tblName,
        Lists.newArrayList(part1), null, currentWriteIds, badWriteId);
    msClient.alter_partitions(dbName, tblName,
        Lists.newArrayList(part2), null, currentWriteIds, badWriteId);

    // We expect two partitions to be updated.
    Map<String, List<ColumnStatisticsObj>> stats = msClient.getPartitionColumnStatistics(
        dbName, tblName, Lists.newArrayList("p=1", "p=2", "p=3"),
        Lists.newArrayList("s"), currentWriteIds);
    assertEquals(1, stats.size());

    assertTrue(su.runOneIteration());
    drainWorkQueue(su, 2);
    // Analyze treats stats like data (new write ID), so stats still should not be valid.
    stats = msClient.getPartitionColumnStatistics(
        dbName, tblName, Lists.newArrayList("p=1", "p=2", "p=3"),
        Lists.newArrayList("s"), currentWriteIds);
    assertEquals(1, stats.size());

    // New reader.
    currentWriteIds = msClient.getValidWriteIds(fqName).toString();
    stats = msClient.getPartitionColumnStatistics(
        dbName, tblName, Lists.newArrayList("p=1", "p=2", "p=3"),
        Lists.newArrayList("s"), currentWriteIds);
    assertEquals(3, stats.size());

    msClient.close();
  }

  @Test(timeout=40000)
  public void testExistingOnly() throws Exception {
    hiveConf.set(MetastoreConf.ConfVars.STATS_AUTO_UPDATE.getVarname(), "existing");
    StatsUpdaterThread su = createUpdater();
    IMetaStoreClient msClient = new HiveMetaStoreClient(hiveConf);

    executeQuery("create table simple_stats (i int, s string)");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER, false);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSCOLAUTOGATHER, false);
    executeQuery("insert into simple_stats (i, s) values (1, 'test')");
    executeQuery("analyze table simple_stats compute statistics for columns i");
    verifyStatsUpToDate("simple_stats", Lists.newArrayList("s"), msClient, false);
    verifyAndUnsetColStats("simple_stats", Lists.newArrayList("i"), msClient);

    assertTrue(su.runOneIteration());
    drainWorkQueue(su);
    verifyStatsUpToDate("simple_stats", Lists.newArrayList("i"), msClient, true);
    verifyStatsUpToDate("simple_stats", Lists.newArrayList("s"), msClient, false);

    msClient.close();
  }

  @Test(timeout=80000)
  public void testQueueingWithThreads() throws Exception {
    final int PART_COUNT = 12;
    hiveConf.setInt(MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX.getVarname(), 5);
    hiveConf.setInt(MetastoreConf.ConfVars.STATS_AUTO_UPDATE_WORKER_COUNT.getVarname(), 2);
    StatsUpdaterThread su = createUpdater();
    su.startWorkers();
    IMetaStoreClient msClient = new HiveMetaStoreClient(hiveConf);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER, false);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSCOLAUTOGATHER, false);
    executeQuery("create table simple_stats (s string) partitioned by (i int)");
    for (int i = 0; i < PART_COUNT; ++i) {
      executeQuery("insert into simple_stats partition(i='" + i + "') values ('test')");
    }
    verifyPartStatsUpToDate(PART_COUNT, 0, msClient, "simple_stats", false);

    // Set one of the partitions to be skipped, so that a command is created for every other one.
    setPartitionSkipProperty(msClient, "simple_stats", "i=0", "true");


    assertTrue(su.runOneIteration());
    su.waitForQueuedCommands();
    verifyStatsUpToDate("simple_stats", "i=0", Lists.newArrayList("s"), msClient, false);
    verifyPartStatsUpToDate(PART_COUNT, 1, msClient, "simple_stats", true);

    assertFalse(su.runOneIteration());
    drainWorkQueue(su, 0); // Nothing else is updated after the first update.

    msClient.close();
  }

  @Test(timeout=40000)
  public void testAllPartitions() throws Exception {
    final int PART_COUNT = 3;
    StatsUpdaterThread su = createUpdater();
    IMetaStoreClient msClient = new HiveMetaStoreClient(hiveConf);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER, false);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSCOLAUTOGATHER, false);
    executeQuery("create table simple_stats (s string) partitioned by (i int)");
    for (int i = 0; i < PART_COUNT; ++i) {
      executeQuery("insert into simple_stats partition(i='" + i + "') values ('test')");
    }
    verifyPartStatsUpToDate(PART_COUNT, 0, msClient, "simple_stats", false);

    assertTrue(su.runOneIteration());
    drainWorkQueue(su, 1); // All the partitions need to be updated; a single command can be used.
    verifyPartStatsUpToDate(PART_COUNT, 0, msClient, "simple_stats", true);

    assertFalse(su.runOneIteration());
    drainWorkQueue(su, 0); // Nothing else is updated after the first update.

    msClient.close();
  }

  @Test(timeout=40000)
  public void testPartitionSubset() throws Exception {
    final int NONSTAT_PART_COUNT = 3;
    StatsUpdaterThread su = createUpdater();
    IMetaStoreClient msClient = new HiveMetaStoreClient(hiveConf);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER, false);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSCOLAUTOGATHER, false);
    executeQuery("create table simple_stats (s string) partitioned by (i int)");
    for (int i = 0; i < NONSTAT_PART_COUNT; ++i) {
      executeQuery("insert into simple_stats partition(i='" + i + "') values ('test')");
    }
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER, true);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSCOLAUTOGATHER, true);
    executeQuery("insert into simple_stats partition(i='"
        + NONSTAT_PART_COUNT + "') values ('test')");
    verifyPartStatsUpToDate(NONSTAT_PART_COUNT, 0, msClient, "simple_stats", false);
    verifyStatsUpToDate("simple_stats",
        "i=" + NONSTAT_PART_COUNT, Lists.newArrayList("s"), msClient, true);

    final int EXCLUDED_PART = 1;
    setPartitionSkipProperty(msClient, "simple_stats", "i=" + EXCLUDED_PART, "true");

    assertTrue(su.runOneIteration());
    // 1 is excluded via property, 3 already has stats, so we only expect two updates.
    drainWorkQueue(su, NONSTAT_PART_COUNT - 1);
    for (int i = 0; i < NONSTAT_PART_COUNT; ++i) {
      verifyStatsUpToDate("simple_stats",
          "i=" + i, Lists.newArrayList("s"), msClient, i != EXCLUDED_PART);
    }
    verifyStatsUpToDate("simple_stats", "i=" + EXCLUDED_PART,
        Lists.newArrayList("s"), msClient, false);

    msClient.close();
  }

  @Test(timeout=40000)
  public void testPartitionsWithDifferentColsAll() throws Exception {
    StatsUpdaterThread su = createUpdater();
    IMetaStoreClient msClient = new HiveMetaStoreClient(hiveConf);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER, false);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSCOLAUTOGATHER, false);
    executeQuery("create table simple_stats (s string, t string, u string) partitioned by (i int)");
    executeQuery("insert into simple_stats partition(i=0) values ('test', '0', 'foo')");
    executeQuery("insert into simple_stats partition(i=1) values ('test', '1', 'bar')");
    executeQuery("analyze table simple_stats partition(i=0) compute statistics for columns s");
    executeQuery("analyze table simple_stats partition(i=1) compute statistics for columns s, u");
    verifyStatsUpToDate("simple_stats", "i=0", Lists.newArrayList("s"), msClient, true);
    verifyStatsUpToDate("simple_stats", "i=0", Lists.newArrayList("t", "u"), msClient, false);
    verifyStatsUpToDate("simple_stats", "i=1", Lists.newArrayList("s", "u"), msClient, true);
    verifyStatsUpToDate("simple_stats", "i=1", Lists.newArrayList("t"), msClient, false);

    assertTrue(su.runOneIteration());
    // Different columns means different commands have to be run.
    drainWorkQueue(su, 2);
    verifyStatsUpToDate("simple_stats", "i=0", Lists.newArrayList("s", "t", "u"), msClient, true);
    verifyStatsUpToDate("simple_stats", "i=1", Lists.newArrayList("s", "t", "u"), msClient, true);

    assertFalse(su.runOneIteration());
    drainWorkQueue(su, 0); // Nothing else is updated after the first update.

    msClient.close();
  }


  @Test(timeout=45000)
  public void testPartitionsWithDifferentColsExistingOnly() throws Exception {
    hiveConf.set(MetastoreConf.ConfVars.STATS_AUTO_UPDATE.getVarname(), "existing");
    StatsUpdaterThread su = createUpdater();
    IMetaStoreClient msClient = new HiveMetaStoreClient(hiveConf);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER, false);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSCOLAUTOGATHER, false);
    executeQuery("create table simple_stats (s string, t string, u string) partitioned by (i int)");
    executeQuery("insert into simple_stats partition(i=0) values ('test', '0', 'foo')");
    executeQuery("insert into simple_stats partition(i=1) values ('test', '1', 'bar')");
    executeQuery("insert into simple_stats partition(i=2) values ('test', '2', 'baz')");
    executeQuery("analyze table simple_stats partition(i=0) compute statistics for columns s, t");
    executeQuery("analyze table simple_stats partition(i=1) compute statistics for columns");
    executeQuery("analyze table simple_stats partition(i=2) compute statistics for columns s");
    verifyStatsUpToDate("simple_stats", "i=0", Lists.newArrayList("s", "t"), msClient, true);
    verifyStatsUpToDate("simple_stats", "i=0", Lists.newArrayList("u"), msClient, false);
    verifyStatsUpToDate("simple_stats", "i=1", Lists.newArrayList("s", "t", "u"), msClient, true);
    verifyStatsUpToDate("simple_stats", "i=2", Lists.newArrayList("s"), msClient, true);
    verifyStatsUpToDate("simple_stats", "i=2", Lists.newArrayList("u", "t"), msClient, false);

    // We will unset s on i=0, and t on i=1. Only these should be updated; and nothing for 2.
    verifyAndUnsetColStats("simple_stats", "i=0", Lists.newArrayList("s"), msClient);
    verifyAndUnsetColStats("simple_stats", "i=1", Lists.newArrayList("t"), msClient);

    assertTrue(su.runOneIteration());
    drainWorkQueue(su, 2);
    // Exact same state as above.
    verifyStatsUpToDate("simple_stats", "i=0", Lists.newArrayList("s", "t"), msClient, true);
    verifyStatsUpToDate("simple_stats", "i=0", Lists.newArrayList("u"), msClient, false);
    verifyStatsUpToDate("simple_stats", "i=1", Lists.newArrayList("s", "t", "u"), msClient, true);
    verifyStatsUpToDate("simple_stats", "i=2", Lists.newArrayList("s"), msClient, true);
    verifyStatsUpToDate("simple_stats", "i=2", Lists.newArrayList("u", "t"), msClient, false);

    msClient.close();
  }

  @Test(timeout=40000)
  public void testParallelOps() throws Exception {
    // Set high worker count so we get a longer queue.
    hiveConf.setInt(MetastoreConf.ConfVars.STATS_AUTO_UPDATE_WORKER_COUNT.getVarname(), 4);
    StatsUpdaterThread su = createUpdater();
    IMetaStoreClient msClient = new HiveMetaStoreClient(hiveConf);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER, false);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSCOLAUTOGATHER, false);
    executeQuery("create table simple_stats (s string)");
    executeQuery("create table simple_stats2 (s string) partitioned by (i int)");
    executeQuery("create table simple_stats3 (s string) partitioned by (i int)");
    executeQuery("insert into simple_stats values ('test')");
    executeQuery("insert into simple_stats2 partition(i=0) values ('test')");
    executeQuery("insert into simple_stats3 partition(i=0) values ('test')");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER, true);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSCOLAUTOGATHER, true);
    executeQuery("insert into simple_stats3 partition(i=1) values ('test')");

    assertTrue(su.runOneIteration());
    assertEquals(3, su.getQueueLength());
    // Nothing updated yet.
    verifyStatsUpToDate("simple_stats", Lists.newArrayList("s"), msClient, false);
    verifyPartStatsUpToDate(1, 0, msClient, "simple_stats2", false);
    verifyStatsUpToDate("simple_stats3", "i=0", Lists.newArrayList("s"), msClient, false);
    verifyStatsUpToDate("simple_stats3", "i=1", Lists.newArrayList("s"), msClient, true);

    assertFalse(su.runOneIteration());
    assertEquals(3, su.getQueueLength()); // Nothing new added to the queue while analyze runs.

    // Add another partition without stats.
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER, false);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSCOLAUTOGATHER, false);
    executeQuery("insert into simple_stats3 partition(i=2) values ('test')");

    assertTrue(su.runOneIteration());
    assertEquals(4, su.getQueueLength()); // An item for new partition is queued now.

    drainWorkQueue(su, 4);

    verifyStatsUpToDate("simple_stats", Lists.newArrayList("s"), msClient, true);
    verifyPartStatsUpToDate(1, 0, msClient, "simple_stats2", true);
    verifyPartStatsUpToDate(3, 0, msClient, "simple_stats3", true);

    assertFalse(su.runOneIteration());
    drainWorkQueue(su, 0); // Nothing else is updated after the first update.

    msClient.close();
  }

  private void verifyPartStatsUpToDate(int partCount, int skip,
      IMetaStoreClient msClient, String tbl, boolean isUpToDate) throws Exception {
    for (int i = skip; i < partCount; ++i) {
      verifyStatsUpToDate(tbl, "i=" + i, Lists.newArrayList("s"), msClient, isUpToDate);
    }
  }

  private void drainWorkQueue(StatsUpdaterThread su) throws InterruptedException {
    while (su.runOneWorkerIteration(ss, ss.getUserName(), ss.getConf(), false)) {}
  }

  private void drainWorkQueue(StatsUpdaterThread su, int expectedReqs) throws InterruptedException {
    int actualReqs = 0;
    while (su.runOneWorkerIteration(ss, ss.getUserName(), ss.getConf(), false)) {
      ++actualReqs;
    }
    assertEquals(expectedReqs, actualReqs);
  }

  private void setTableSkipProperty(
      IMetaStoreClient msClient, String tbl, String val) throws Exception {
    Table table = msClient.getTable(ss.getCurrentDatabase(), tbl);
    table.getParameters().put(StatsUpdaterThread.SKIP_STATS_AUTOUPDATE_PROPERTY, val);
    msClient.alter_table(table.getDbName(), table.getTableName(), table);
  }

  private void setPartitionSkipProperty(
      IMetaStoreClient msClient, String tblName, String partName, String val) throws Exception {
    Partition part = msClient.getPartition(ss.getCurrentDatabase(), tblName, partName);
    part.getParameters().put(StatsUpdaterThread.SKIP_STATS_AUTOUPDATE_PROPERTY, val);
    msClient.alter_partition(part.getCatName(), part.getDbName(), tblName, part);
  }

  private void verifyAndUnsetColStats(
      String tblName, List<String> cols, IMetaStoreClient msClient) throws Exception {
    Table tbl = msClient.getTable(ss.getCurrentDatabase(), tblName);
    verifyAndUnsetColStatsVal(tbl.getParameters(), cols);
    EnvironmentContext ec = new EnvironmentContext();
    // Make sure metastore doesn't mess with our bogus stats updates.
    ec.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);
    msClient.alter_table_with_environmentContext(tbl.getDbName(), tbl.getTableName(), tbl, ec);
    // Double-check.
    tbl = msClient.getTable(ss.getCurrentDatabase(), tblName);
    for (String col : cols) {
      assertFalse(StatsSetupConst.areColumnStatsUptoDate(tbl.getParameters(), col));
    }
  }

  private void verifyAndUnsetColStatsVal(Map<String, String> params, List<String> cols) {
    assertTrue(StatsSetupConst.areBasicStatsUptoDate(params));
    for (String col : cols) {
      assertTrue(StatsSetupConst.areColumnStatsUptoDate(params, col));
    }
    StatsSetupConst.removeColumnStatsState(params, cols);
    StatsSetupConst.setBasicStatsState(params, StatsSetupConst.TRUE);
  }

  private void verifyAndUnsetColStats(String tblName, String partName, List<String> cols,
      IMetaStoreClient msClient) throws Exception {
    Partition part = msClient.getPartition(ss.getCurrentDatabase(), tblName, partName);
    verifyAndUnsetColStatsVal(part.getParameters(), cols);
    EnvironmentContext ec = new EnvironmentContext();
    // Make sure metastore doesn't mess with our bogus stats updates.
    ec.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);
    msClient.alter_partition(part.getCatName(), part.getDbName(), tblName, part, ec);
    // Double-check.
    part = msClient.getPartition(ss.getCurrentDatabase(), tblName, partName);
    for (String col : cols) {
      assertFalse(StatsSetupConst.areColumnStatsUptoDate(part.getParameters(), col));
    }
  }

  private void verifyStatsUpToDate(String tbl, List<String> cols, IMetaStoreClient msClient,
      boolean isUpToDate) throws Exception {
    Table table = msClient.getTable(ss.getCurrentDatabase(), tbl);
    verifyStatsUpToDate(table.getParameters(), cols, isUpToDate);
  }

  private void verifyStatsUpToDate(String tbl, List<String> cols, IMetaStoreClient msClient,
      String validWriteIds, boolean isUpToDate) throws Exception {
    Table table = msClient.getTable(null, ss.getCurrentDatabase(), tbl, validWriteIds);
    verifyStatsUpToDate(table.getParameters(), cols, isUpToDate);
  }

  private void verifyStatsUpToDate(Map<String, String> params, List<String> cols,
      boolean isUpToDate) {
    if (isUpToDate) {
      assertTrue(StatsSetupConst.areBasicStatsUptoDate(params));
    }
    for (String col : cols) {
      assertEquals(isUpToDate, StatsSetupConst.areColumnStatsUptoDate(params, col));
    }
  }

  private void verifyStatsUpToDate(String tbl, String part, ArrayList<String> cols,
      IMetaStoreClient msClient, boolean isUpToDate) throws Exception {
    Partition partition = msClient.getPartition(ss.getCurrentDatabase(), tbl, part);
    verifyStatsUpToDate(partition.getParameters(), cols, isUpToDate);
  }

  private void executeQuery(String query) throws HiveException {
    DriverUtils.runOnDriver(hiveConf, ss.getUserName(), ss, query, null);
  }

  private StatsUpdaterThread createUpdater() throws MetaException {
    StatsUpdaterThread su = new StatsUpdaterThread();
    su.setConf(hiveConf);
    su.init(new AtomicBoolean(false), null);
    return su;
  }
}
