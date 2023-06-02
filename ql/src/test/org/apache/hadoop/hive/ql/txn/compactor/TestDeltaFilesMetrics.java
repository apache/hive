/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.txn.compactor;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class TestDeltaFilesMetrics extends CompactorTest  {

  private void setUpHiveConf() {
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.METASTORE_DELTAMETRICS_DELTA_NUM_THRESHOLD, 1);
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.METASTORE_DELTAMETRICS_OBSOLETE_DELTA_NUM_THRESHOLD, 1);
    MetastoreConf.setTimeVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_CHECK_INTERVAL, 1,
        TimeUnit.SECONDS);
    MetastoreConf.setDoubleVar(conf, MetastoreConf.ConfVars.METASTORE_DELTAMETRICS_DELTA_PCT_THRESHOLD, 0.15f);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED, true);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_GATHER_STATS, false);
  }

  @Override
  @Before
  public void setup() throws Exception {
    this.conf = new HiveConf();
    setUpHiveConf();
    setup(conf);
    MetricsFactory.init(conf);
  }

  @After
  public void tearDown() throws Exception {
    MetricsFactory.close();
    compactorTestCleanup();
  }

  private static void verifyDeltaMetricsMatch(Map<String, Integer> expected, String metricName) throws Exception {
    verifyDeltaMetricsMatch(
        expected,
        gaugeToMap(metricName),
        Metrics.getOrCreateMapMetrics(metricName).get());
  }

  private static void verifyDeltaMetricsMatch(Map<String, Integer> expected,
      Map<String, Integer> actualMBeanMetric, Map<String, Integer> actualMapMetric) {
    assertThat("Actual mBean metrics " + actualMBeanMetric + " don't match expected: " + expected,
        actualMapMetric, is(expected));
    assertThat("Actual map metrics " + actualMapMetric + " don't match expected: " + expected,
        actualMapMetric, is(expected));
  }

  @Override
  boolean useHive130DeltaDirName() {
    return false;
  }

  @Test
  public void testDeltaFileMetricPartitionedTable() throws Exception {
    String dbName = "default";
    String tblName = "dp";
    String partName = "ds=part1";

    Table t = newTable(dbName, tblName, true);
    List<LockComponent> components = new ArrayList<>();

    Partition p = newPartition(t, "part1");
    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 22L, 2);
    addDeltaFile(t, p, 23L, 24L, 20);

    components.add(createLockComponent(dbName, tblName, partName));

    burnThroughTransactions(dbName, tblName, 23);
    long txnId = openTxn();

    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnId);
    LockResponse res = txnHandler.lock(req);
    Assert.assertEquals(LockState.ACQUIRED, res.getState());

    long writeId = allocateWriteId(dbName, tblName, txnId);
    Assert.assertEquals(24, writeId);
    txnHandler.commitTxn(new CommitTxnRequest(txnId));

    startInitiator();

    TimeUnit.SECONDS.sleep(2);
    // 2 active deltas
    // 1 small delta
    // 0 obsolete deltas
    verifyDeltaMetricsMatch(
        ImmutableMap.of(dbName + "." + tblName + Path.SEPARATOR + partName, 2),
        MetricsConstants.COMPACTION_NUM_DELTAS);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(dbName + "." + tblName + Path.SEPARATOR + partName, 1),
        MetricsConstants.COMPACTION_NUM_SMALL_DELTAS);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(),
        MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS);

    startWorker();

    TimeUnit.SECONDS.sleep(2);
    // 0 active deltas
    // 0 small delta
    // 2 obsolete deltas
    verifyDeltaMetricsMatch(
        ImmutableMap.of(),
        MetricsConstants.COMPACTION_NUM_DELTAS);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(),
        MetricsConstants.COMPACTION_NUM_SMALL_DELTAS);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(dbName + "." + tblName + Path.SEPARATOR + partName, 2),
        MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS);

    addDeltaFile(t, p, 25L, 26L, 2);
    addDeltaFile(t, p, 27L, 28L, 20);
    addDeltaFile(t, p, 29L, 30L, 2);

    burnThroughTransactions(dbName, tblName, 30);
    txnId = openTxn();

    req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnId);
    res = txnHandler.lock(req);
    Assert.assertEquals(LockState.ACQUIRED, res.getState());

    writeId = allocateWriteId(dbName, tblName, txnId);
    Assert.assertEquals(55, writeId);
    txnHandler.commitTxn(new CommitTxnRequest(txnId));
    // Change these params to initiate MINOR compaction
    HiveConf.setFloatVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_PCT_THRESHOLD, 1.8f);
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 2);
    startInitiator();

    TimeUnit.SECONDS.sleep(2);
    // 3 active deltas
    // 2 small deltas
    // 2 obsolete deltas
    verifyDeltaMetricsMatch(
        ImmutableMap.of(dbName + "." + tblName + Path.SEPARATOR + partName, 3),
        MetricsConstants.COMPACTION_NUM_DELTAS);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(dbName + "." + tblName + Path.SEPARATOR + partName, 2),
        MetricsConstants.COMPACTION_NUM_SMALL_DELTAS);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(dbName + "." + tblName + Path.SEPARATOR + partName, 2),
        MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS);

    startCleaner();

    TimeUnit.SECONDS.sleep(2);
    // 3 active deltas
    // 2 small deltas
    // 0 obsolete delta
    verifyDeltaMetricsMatch(
        ImmutableMap.of(dbName + "." + tblName + Path.SEPARATOR + partName, 3),
        MetricsConstants.COMPACTION_NUM_DELTAS);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(dbName + "." + tblName + Path.SEPARATOR + partName, 2),
        MetricsConstants.COMPACTION_NUM_SMALL_DELTAS);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(),
        MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS);

    startWorker();

    TimeUnit.SECONDS.sleep(2);
    // 1 active delta
    // 0 small delta
    // 3 obsolete deltas
    verifyDeltaMetricsMatch(
        ImmutableMap.of(dbName + "." + tblName + Path.SEPARATOR + partName, 1),
        MetricsConstants.COMPACTION_NUM_DELTAS);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(),
        MetricsConstants.COMPACTION_NUM_SMALL_DELTAS);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(dbName + "." + tblName + Path.SEPARATOR + partName, 3),
        MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS);

    startCleaner();

    TimeUnit.SECONDS.sleep(2);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(dbName + "." + tblName + Path.SEPARATOR + partName, 1),
        MetricsConstants.COMPACTION_NUM_DELTAS);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(),
        MetricsConstants.COMPACTION_NUM_SMALL_DELTAS);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(),
        MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS);
  }

  @Test
  public void testDeltaFileMetricMultiPartitionedTable() throws Exception {
    String dbName = "default";
    String tblName = "dp";
    String part1Name = "ds=part1";
    String part2Name = "ds=part2";
    String part3Name = "ds=part3";
    Table t = newTable(dbName, tblName, true);
    List<LockComponent> components = new ArrayList<>();


    Partition p1 = newPartition(t, "part1");
    addDeltaFile(t, p1, 1L, 2L, 2);
    addDeltaFile(t, p1, 3L, 4L, 4);

    Partition p2 = newPartition(t, "part2");
    addBaseFile(t, p2, 5L, 20);
    addDeltaFile(t, p2, 6L, 7L, 2);
    addDeltaFile(t, p2, 8L, 9L, 3);
    addDeltaFile(t, p2, 10L, 11L, 1);

    Partition p3 = newPartition(t, "part3");
    addDeltaFile(t, p3, 12L, 13L, 3);
    addDeltaFile(t, p3, 14L, 15L, 20);
    addDeltaFile(t, p3, 16L, 17L, 50);
    addDeltaFile(t, p3, 18L, 19L, 2);

    components.add(createLockComponent(dbName, tblName, part1Name));
    components.add(createLockComponent(dbName, tblName, part2Name));
    components.add(createLockComponent(dbName, tblName, part3Name));

    burnThroughTransactions(dbName, tblName, 19);
    long txnId = openTxn();

    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnId);
    LockResponse res = txnHandler.lock(req);
    Assert.assertEquals(LockState.ACQUIRED, res.getState());

    allocateWriteId(dbName, tblName, txnId);
    txnHandler.commitTxn(new CommitTxnRequest(txnId));

    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 2);
    HiveConf.setFloatVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_PCT_THRESHOLD, 0.4f);
    startInitiator();

    TimeUnit.SECONDS.sleep(2);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(
            dbName + "." + tblName + Path.SEPARATOR + part1Name, 2,
            dbName + "." + tblName + Path.SEPARATOR + part2Name, 3,
            dbName + "." + tblName + Path.SEPARATOR + part3Name, 4),
        MetricsConstants.COMPACTION_NUM_DELTAS);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(
            dbName + "." + tblName + Path.SEPARATOR + part2Name, 2),
      MetricsConstants.COMPACTION_NUM_SMALL_DELTAS);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(),
        MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS);

    ShowCompactResponse showCompactResponse = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = showCompactResponse.getCompacts();
    Assert.assertEquals(2, compacts.size());

    // Need to run two worker sessions, to compact all resources in the compaction queue
    startWorker();
    startWorker();

    TimeUnit.SECONDS.sleep(2);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(
            dbName + "." + tblName + Path.SEPARATOR + part1Name, 2,
            dbName + "." + tblName + Path.SEPARATOR + part2Name, 1),
      MetricsConstants.COMPACTION_NUM_DELTAS);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(),
        MetricsConstants.COMPACTION_NUM_SMALL_DELTAS);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(
            dbName + "." + tblName + Path.SEPARATOR + part2Name, 3,
            dbName + "." + tblName + Path.SEPARATOR + part3Name, 4),
        MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS);

    startCleaner();
    startCleaner();

    TimeUnit.SECONDS.sleep(2);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(
          dbName + "." + tblName + Path.SEPARATOR + part1Name, 2,
          dbName + "." + tblName + Path.SEPARATOR + part2Name, 1),
      MetricsConstants.COMPACTION_NUM_DELTAS);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(),
        MetricsConstants.COMPACTION_NUM_SMALL_DELTAS);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(),
        MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS);
  }

  @Test
  public void testDeltaFileMetricUnpartitionedTable() throws Exception {
    String dbName = "default";
    String tblName = "dp";
    Table t = newTable(dbName, tblName, false);
    List<LockComponent> components = new ArrayList<>();

    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 20);

    components.add(createLockComponent(dbName, tblName, null));
    burnThroughTransactions(dbName, tblName, 24);
    long txnId = openTxn();

    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnId);
    LockResponse res = txnHandler.lock(req);
    Assert.assertEquals(LockState.ACQUIRED, res.getState());

    long writeId = allocateWriteId(dbName, tblName, txnId);
    Assert.assertEquals(25, writeId);
    txnHandler.commitTxn(new CommitTxnRequest(txnId));

    startInitiator();

    TimeUnit.SECONDS.sleep(2);
    // 2 active deltas
    // 1 small delta
    // 0 obsolete deltas
    verifyDeltaMetricsMatch(
        ImmutableMap.of(dbName + "." + tblName, 2),
        MetricsConstants.COMPACTION_NUM_DELTAS);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(dbName + "." + tblName, 1),
        MetricsConstants.COMPACTION_NUM_SMALL_DELTAS);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(),
        MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS);

    startWorker();

    TimeUnit.SECONDS.sleep(2);
    // 0 active delta
    // 0 small delta
    // 2 obsolete delta
    verifyDeltaMetricsMatch(
        ImmutableMap.of(),
        MetricsConstants.COMPACTION_NUM_DELTAS);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(),
        MetricsConstants.COMPACTION_NUM_SMALL_DELTAS);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(dbName + "." + tblName, 2),
        MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS);

    startCleaner();

    TimeUnit.SECONDS.sleep(2);
    // 0 active delta
    // 0 small delta
    // 0 obsolete delta
    verifyDeltaMetricsMatch(
        ImmutableMap.of(),
        MetricsConstants.COMPACTION_NUM_DELTAS);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(),
        MetricsConstants.COMPACTION_NUM_SMALL_DELTAS);
    verifyDeltaMetricsMatch(
        ImmutableMap.of(),
        MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS);
  }

  private LockComponent createLockComponent(String dbName, String tblName, String partName) {
    LockComponent component = new LockComponent(LockType.SHARED_WRITE, LockLevel.PARTITION, dbName);
    component.setTablename(tblName);
    if (partName != null) {
      component.setPartitionname(partName);
    }
    component.setOperationType(DataOperationType.UPDATE);
    return component;
  }
}
