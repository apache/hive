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

import com.codahale.metrics.Gauge;
import org.apache.commons.lang3.tuple.Pair;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.common.ServerUtils;
import org.apache.hadoop.hive.common.metrics.MetricsTestUtils;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HMSMetricsListener;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
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
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.AcidMetricService;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.txn.ThrowingTxnHandler;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.txn.compactor.metrics.DeltaFilesMetricReporter;
import org.apache.tez.common.counters.TezCounters;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hive.ql.txn.compactor.metrics.DeltaFilesMetricReporter.DeltaFilesMetricType.NUM_DELTAS;
import static org.apache.hadoop.hive.ql.txn.compactor.metrics.DeltaFilesMetricReporter.DeltaFilesMetricType.NUM_SMALL_DELTAS;
import static org.apache.hadoop.hive.ql.txn.compactor.metrics.DeltaFilesMetricReporter.DeltaFilesMetricType.NUM_OBSOLETE_DELTAS;
import static org.apache.hadoop.hive.metastore.metrics.AcidMetricService.replaceWhitespace;

public class TestCompactionMetrics  extends CompactorTest {

  private static final String INITIATED_METRICS_KEY = MetricsConstants.COMPACTION_STATUS_PREFIX + TxnStore.INITIATED_RESPONSE;
  private static final String INITIATOR_CYCLE_KEY = MetricsConstants.API_PREFIX + MetricsConstants.COMPACTION_INITIATOR_CYCLE;
  private static final String CLEANER_CYCLE_KEY = MetricsConstants.API_PREFIX + MetricsConstants.COMPACTION_CLEANER_CYCLE;
  private static final String WORKER_CYCLE_KEY = MetricsConstants.API_PREFIX + MetricsConstants.COMPACTION_WORKER_CYCLE;

  @Before
  public void setUp() throws Exception {
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED, true);
    // re-initialize metrics
    Metrics.shutdown();
    Metrics.initialize(conf);
  }

  @Test
  public void testInitiatorPerfMetricsEnabled() throws Exception {
    Metrics.getOrCreateGauge(INITIATED_METRICS_KEY).set(0);
    long initiatorCycles = Objects.requireNonNull(Metrics.getOrCreateTimer(INITIATOR_CYCLE_KEY)).getCount();
    Table t = newTable("default", "ime", true);
    List<LockComponent> components = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      Partition p = newPartition(t, "part" + (i + 1));
      addBaseFile(t, p, 20L, 20);
      addDeltaFile(t, p, 21L, 22L, 2);
      addDeltaFile(t, p, 23L, 24L, 2);

      LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.PARTITION, "default");
      comp.setTablename("ime");
      comp.setPartitionname("ds=part" + (i + 1));
      comp.setOperationType(DataOperationType.UPDATE);
      components.add(comp);
    }
    burnThroughTransactions("default", "ime", 23);
    long txnid = openTxn();

    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    Assert.assertEquals(LockState.ACQUIRED, res.getState());

    long writeid = allocateWriteId("default", "ime", txnid);
    Assert.assertEquals(24, writeid);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(10, compacts.size());
    Assert.assertEquals(initiatorCycles + 1,
        Objects.requireNonNull(Metrics.getOrCreateTimer(INITIATOR_CYCLE_KEY)).getCount());

    runAcidMetricService();

    Assert.assertEquals(10,
        Metrics.getOrCreateGauge(INITIATED_METRICS_KEY).intValue());
  }

  @Test
  public void testInitiatorPerfMetricsDisabled() throws Exception {
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED, false);
    Metrics.initialize(conf);
    int originalValue = Metrics.getOrCreateGauge(INITIATED_METRICS_KEY).intValue();
    long initiatorCycles = Objects.requireNonNull(Metrics.getOrCreateTimer(INITIATOR_CYCLE_KEY)).getCount();
    Table t = newTable("default", "imd", true);
    List<LockComponent> components = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      Partition p = newPartition(t, "part" + (i + 1));
      addBaseFile(t, p, 20L, 20);
      addDeltaFile(t, p, 21L, 22L, 2);
      addDeltaFile(t, p, 23L, 24L, 2);

      LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.PARTITION, "default");
      comp.setTablename("imd");
      comp.setPartitionname("ds=part" + (i + 1));
      comp.setOperationType(DataOperationType.UPDATE);
      components.add(comp);
    }
    burnThroughTransactions("default", "imd", 23);
    long txnid = openTxn();

    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnid);
    LockResponse res = txnHandler.lock(req);
    Assert.assertEquals(LockState.ACQUIRED, res.getState());

    long writeid = allocateWriteId("default", "imd", txnid);
    Assert.assertEquals(24, writeid);
    txnHandler.commitTxn(new CommitTxnRequest(txnid));

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals(10, compacts.size());
    Assert.assertEquals(initiatorCycles,
        Objects.requireNonNull(Metrics.getOrCreateTimer(INITIATOR_CYCLE_KEY)).getCount());

    runAcidMetricService();

    Assert.assertEquals(originalValue,
        Metrics.getOrCreateGauge(INITIATED_METRICS_KEY).intValue());
  }

  @Test
  public void testOldestReadyForCleaningAge() throws Exception {
    conf.setIntVar(HiveConf.ConfVars.COMPACTOR_MAX_NUM_DELTA, 1);

    long oldStart = System.currentTimeMillis();
    Table old = newTable("default", "old_rfc", true);
    Partition oldP = newPartition(old, "part");
    addBaseFile(old, oldP, 20L, 20);
    addDeltaFile(old, oldP, 21L, 22L, 2);
    addDeltaFile(old, oldP, 23L, 24L, 2);
    burnThroughTransactions("default", "old_rfc", 25);
    CompactionRequest rqst = new CompactionRequest("default", "old_rfc", CompactionType.MINOR);
    rqst.setPartitionname("ds=part");
    txnHandler.compact(rqst);
    startWorker();

    long youngStart = System.currentTimeMillis();
    Table young = newTable("default", "young_rfc", true);
    Partition youngP = newPartition(young, "part");
    addBaseFile(young, youngP, 20L, 20);
    addDeltaFile(young, youngP, 21L, 22L, 2);
    addDeltaFile(young, youngP, 23L, 24L, 2);
    burnThroughTransactions("default", "young_rfc", 25);
    rqst = new CompactionRequest("default", "young_rfc", CompactionType.MINOR);
    rqst.setPartitionname("ds=part");
    txnHandler.compact(rqst);
    startWorker();

    runAcidMetricService();
    long oldDiff = (System.currentTimeMillis() - oldStart)/1000;
    long youngDiff = (System.currentTimeMillis() - youngStart)/1000;

    long threshold = 1000;
    Assert.assertTrue(Metrics.getOrCreateGauge(MetricsConstants.OLDEST_READY_FOR_CLEANING_AGE).intValue() <= oldDiff + threshold);
    Assert.assertTrue(Metrics.getOrCreateGauge(MetricsConstants.OLDEST_READY_FOR_CLEANING_AGE).intValue() >= youngDiff);
  }

  @Test
  public void  testInitiatorNoFailure() throws Exception {
    startInitiator();
    Pair<AtomicInteger, AtomicInteger> ratio =
        Metrics.getOrCreateRatio(MetricsConstants.COMPACTION_FAILED_INITIATOR_RATIO);
    Assert.assertEquals("numerator mismatch", 0, ratio.getLeft().get());
    Assert.assertEquals("denominator mismatch", 1, ratio.getRight().get());
  }

  @Test
  public void  testCleanerNoFailure() throws Exception {
    startCleaner();
    Pair<AtomicInteger, AtomicInteger> ratio =
        Metrics.getOrCreateRatio(MetricsConstants.COMPACTION_FAILED_CLEANER_RATIO);
    Assert.assertEquals("numerator mismatch", 0, ratio.getLeft().get());
    Assert.assertEquals("denominator mismatch", 1, ratio.getRight().get());
  }

  @Test
  public void  testInitiatorFailure() throws Exception {
    ThrowingTxnHandler.doThrow = true;
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.TXN_STORE_IMPL, "org.apache.hadoop.hive.metastore.txn.ThrowingTxnHandler");
    startInitiator();
    Pair<AtomicInteger, AtomicInteger> ratio =
        Metrics.getOrCreateRatio(MetricsConstants.COMPACTION_FAILED_INITIATOR_RATIO);
    Assert.assertEquals("numerator mismatch", 1, ratio.getLeft().get());
    Assert.assertEquals("denominator mismatch", 1, ratio.getRight().get());
  }

  @Test
  public void  testCleanerFailure() throws Exception {
    ThrowingTxnHandler.doThrow = true;
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.TXN_STORE_IMPL, "org.apache.hadoop.hive.metastore.txn.ThrowingTxnHandler");
    startCleaner();
    Pair<AtomicInteger, AtomicInteger> ratio =
        Metrics.getOrCreateRatio(MetricsConstants.COMPACTION_FAILED_CLEANER_RATIO);
    Assert.assertEquals("numerator mismatch", 1, ratio.getLeft().get());
    Assert.assertEquals("denominator mismatch", 1, ratio.getRight().get());
  }

  @Test
  public void  testInitiatorAuxFailure() throws Exception {
    TxnStore.MutexAPI.LockHandle handle = null;
    try {
      handle = txnHandler.getMutexAPI().acquireLock(TxnStore.MUTEX_KEY.Initiator.name());
      final Thread main = Thread.currentThread();
      interruptThread(5000, main);
      startInitiator();
    } finally {
      if (handle != null) {
        handle.releaseLocks();
      }
    }
    // the lock timeout on AUX lock, should be ignored.
    Pair<AtomicInteger, AtomicInteger> ratio =
        Metrics.getOrCreateRatio(MetricsConstants.COMPACTION_FAILED_INITIATOR_RATIO);
    Assert.assertEquals(0, ratio.getLeft().get());
    Assert.assertEquals("numerator mismatch", 0, ratio.getLeft().get());
    Assert.assertEquals("denominator mismatch", 0, ratio.getRight().get());
  }

  @Test
  public void  testCleanerAuxFailure() throws Exception {
    TxnStore.MutexAPI.LockHandle handle = null;
    try {
      handle = txnHandler.getMutexAPI().acquireLock(TxnStore.MUTEX_KEY.Cleaner.name());
      final Thread main = Thread.currentThread();
      interruptThread(5000, main);
      startCleaner();
    } finally {
      if (handle != null) {
        handle.releaseLocks();
      }
    }
    // the lock timeout on AUX lock, should be ignored.
    Pair<AtomicInteger, AtomicInteger> ratio =
        Metrics.getOrCreateRatio(MetricsConstants.COMPACTION_FAILED_CLEANER_RATIO);
    Assert.assertEquals("numerator mismatch", 0, ratio.getLeft().get());
    Assert.assertEquals("denominator mismatch", 0, ratio.getRight().get());
  }

  @Test
  public void testCleanerPerfMetricsEnabled() throws Exception {
    long cleanerCyclesMinor = Objects.requireNonNull(
        Metrics.getOrCreateTimer(CLEANER_CYCLE_KEY + "_" + CompactionType.MINOR)).getCount();
    long cleanerCyclesMajor = Objects.requireNonNull(
        Metrics.getOrCreateTimer(CLEANER_CYCLE_KEY + "_" + CompactionType.MAJOR)).getCount();

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

    Assert.assertEquals(cleanerCyclesMinor + 10, Objects.requireNonNull(
        Metrics.getOrCreateTimer(CLEANER_CYCLE_KEY + "_" + CompactionType.MINOR)).getCount());

    for (int i = 0; i < 10; i++) {
      p = partitions.get(i);
      addBaseFile(t, p, 25L, 25, 26 + i);

      CompactionRequest rqst = new CompactionRequest("default", "camipc", CompactionType.MAJOR);
      rqst.setPartitionname("ds=today" + i);
      compactInTxn(rqst);
    }

    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_CLEANER_THREADS_NUM, 3);
    startCleaner();

    // Check there are no compactions requests left.
    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(20, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());

    Assert.assertEquals(cleanerCyclesMajor + 10, Objects.requireNonNull(
        Metrics.getOrCreateTimer(CLEANER_CYCLE_KEY + "_" + CompactionType.MAJOR)).getCount());
  }

  @Test
  public void testCleanerPerfMetricsDisabled() throws Exception {
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED, false);
    Metrics.initialize(conf);

    long cleanerCyclesMinor = Objects.requireNonNull(
      Metrics.getOrCreateTimer(CLEANER_CYCLE_KEY + "_" + CompactionType.MAJOR)).getCount();

    Table t = newTable("default", "camipc", true);
    Partition p = newPartition(t, "today");

    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 22L, 2);
    addDeltaFile(t, p, 23L, 24L, 2);

    burnThroughTransactions("default", "camipc", 25);

    CompactionRequest rqst = new CompactionRequest("default", "camipc", CompactionType.MAJOR);
    rqst.setPartitionname("ds=today");
    long compactTxn = compactInTxn(rqst);
    addBaseFile(t, p, 25L, 25, compactTxn);

    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts().get(0).getState());

    Assert.assertEquals(cleanerCyclesMinor, Objects.requireNonNull(
        Metrics.getOrCreateTimer(CLEANER_CYCLE_KEY + "_" + CompactionType.MAJOR)).getCount());
  }

  @Test
  public void testWorkerPerfMetrics() throws Exception {
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED, true);
    MetricsFactory.close();
    MetricsFactory.init(conf);

    conf.setIntVar(HiveConf.ConfVars.COMPACTOR_MAX_NUM_DELTA, 1);
    Table t = newTable("default", "mapwb", true);
    Partition p = newPartition(t, "today");

    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 22L, 2);
    addDeltaFile(t, p, 23L, 24L, 2);

    burnThroughTransactions("default", "mapwb", 25);

    CompactionRequest rqst = new CompactionRequest("default", "mapwb", CompactionType.MINOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);

    startWorker();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, rsp.getCompacts().get(0).getState());

    CodahaleMetrics metrics = (CodahaleMetrics) MetricsFactory.getInstance();
    String json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.TIMER,
        WORKER_CYCLE_KEY + "_" + CompactionType.MINOR, 1);

    rqst = new CompactionRequest("default", "mapwb", CompactionType.MAJOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);

    startWorker();

    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(2, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, rsp.getCompacts().get(0).getState());

    json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.TIMER,
        WORKER_CYCLE_KEY + "_" + CompactionType.MAJOR, 1);
  }

  @Test
  public void testUpdateCompactionMetrics() {
    ShowCompactResponse scr = new ShowCompactResponse();
    List<ShowCompactResponseElement> elements = new ArrayList<>();
    elements.add(generateElement(1,"db", "tb", null, CompactionType.MAJOR, TxnStore.FAILED_RESPONSE));
    // Check for overwrite
    elements.add(generateElement(2,"db", "tb", null, CompactionType.MAJOR, TxnStore.INITIATED_RESPONSE));
    elements.add(generateElement(3,"db", "tb2", null, CompactionType.MINOR, TxnStore.INITIATED_RESPONSE));
    elements.add(generateElement(5,"db", "tb3", "p1", CompactionType.MINOR, TxnStore.DID_NOT_INITIATE_RESPONSE));
    // Check for overwrite where the order is different
    elements.add(generateElement(4,"db", "tb3", "p1", CompactionType.MINOR, TxnStore.FAILED_RESPONSE));

    elements.add(generateElement(6,"db1", "tb", null, CompactionType.MINOR, TxnStore.FAILED_RESPONSE));
    elements.add(generateElement(7,"db1", "tb2", null, CompactionType.MINOR, TxnStore.FAILED_RESPONSE));
    elements.add(generateElement(8,"db1", "tb3", null, CompactionType.MINOR, TxnStore.FAILED_RESPONSE));

    elements.add(generateElement(9,"db2", "tb", null, CompactionType.MINOR, TxnStore.SUCCEEDED_RESPONSE));
    elements.add(generateElement(10,"db2", "tb2", null, CompactionType.MINOR, TxnStore.SUCCEEDED_RESPONSE));
    elements.add(generateElement(11,"db2", "tb3", null, CompactionType.MINOR, TxnStore.SUCCEEDED_RESPONSE));
    elements.add(generateElement(12,"db2", "tb4", null, CompactionType.MINOR, TxnStore.SUCCEEDED_RESPONSE));

    elements.add(generateElement(13,"db3", "tb3", null, CompactionType.MINOR, TxnStore.WORKING_RESPONSE));
    elements.add(generateElement(14,"db3", "tb4", null, CompactionType.MINOR, TxnStore.WORKING_RESPONSE));
    elements.add(generateElement(15,"db3", "tb5", null, CompactionType.MINOR, TxnStore.WORKING_RESPONSE));
    elements.add(generateElement(16,"db3", "tb6", null, CompactionType.MINOR, TxnStore.WORKING_RESPONSE));
    elements.add(generateElement(17,"db3", "tb7", null, CompactionType.MINOR, TxnStore.WORKING_RESPONSE));

    scr.setCompacts(elements);
    AcidMetricService.updateMetricsFromShowCompact(scr);

    Assert.assertEquals(1,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_STATUS_PREFIX +
            replaceWhitespace(TxnStore.DID_NOT_INITIATE_RESPONSE)).intValue());
    Assert.assertEquals(2,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_STATUS_PREFIX + TxnStore.INITIATED_RESPONSE).intValue());
    Assert.assertEquals(3,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_STATUS_PREFIX + TxnStore.FAILED_RESPONSE).intValue());
    Assert.assertEquals(4,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_STATUS_PREFIX + TxnStore.SUCCEEDED_RESPONSE).intValue());
    Assert.assertEquals(5,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_STATUS_PREFIX + TxnStore.WORKING_RESPONSE).intValue());
    Assert.assertEquals(0,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_STATUS_PREFIX +
            replaceWhitespace(TxnStore.CLEANING_RESPONSE)).intValue());

    Assert.assertEquals(2,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_NUM_INITIATORS).intValue());
    Assert.assertEquals(2,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_NUM_WORKERS).intValue());
    Assert.assertEquals(1,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_NUM_INITIATOR_VERSIONS).intValue());
    Assert.assertEquals(1,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_NUM_WORKER_VERSIONS).intValue());
  }

  @Test
  public void testAgeMetricsNotSet() {
    ShowCompactResponse scr = new ShowCompactResponse();
    List<ShowCompactResponseElement> elements = new ArrayList<>();
    elements.add(generateElement(1, "db", "tb", null, CompactionType.MAJOR, TxnStore.FAILED_RESPONSE, 1L));
    elements.add(generateElement(5, "db", "tb3", "p1", CompactionType.MINOR, TxnStore.DID_NOT_INITIATE_RESPONSE, 2L));
    elements.add(generateElement(9, "db2", "tb", null, CompactionType.MINOR, TxnStore.SUCCEEDED_RESPONSE, 3L));
    elements.add(generateElement(13, "db3", "tb3", null, CompactionType.MINOR, TxnStore.WORKING_RESPONSE, 4L));
    elements.add(generateElement(14, "db3", "tb4", null, CompactionType.MINOR, TxnStore.CLEANING_RESPONSE, 5L));

    scr.setCompacts(elements);
    AcidMetricService.updateMetricsFromShowCompact(scr);
    // Check that it is not set
    Assert.assertEquals(0, Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_OLDEST_ENQUEUE_AGE).intValue());
  }

  @Test
  public void testAgeMetricsAge() {
    ShowCompactResponse scr = new ShowCompactResponse();
    List<ShowCompactResponseElement> elements = new ArrayList<>();
    long start = System.currentTimeMillis() - 1000L;
    elements.add(generateElement(15,"db3", "tb5", null, CompactionType.MINOR, TxnStore.INITIATED_RESPONSE, start));

    scr.setCompacts(elements);
    AcidMetricService.updateMetricsFromShowCompact(scr);
    long diff = (System.currentTimeMillis() - start)/1000;
    // Check that we have at least 1s old compaction age, but not more than expected
    Assert.assertTrue(Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_OLDEST_ENQUEUE_AGE).intValue() <= diff);
    Assert.assertTrue(Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_OLDEST_ENQUEUE_AGE).intValue() >= 1);
  }

  @Test
  public void testAgeMetricsOrder() {
    ShowCompactResponse scr = new ShowCompactResponse();
    long start = System.currentTimeMillis();
    List<ShowCompactResponseElement> elements = new ArrayList<>();
    elements.add(generateElement(15,"db3", "tb5", null, CompactionType.MINOR, TxnStore.INITIATED_RESPONSE,
        start - 1000L));
    elements.add(generateElement(16,"db3", "tb6", null, CompactionType.MINOR, TxnStore.INITIATED_RESPONSE,
        start - 100000L));

    scr.setCompacts(elements);
    AcidMetricService.updateMetricsFromShowCompact(scr);
    // Check that the age is older than 10s
    Assert.assertTrue(Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_OLDEST_ENQUEUE_AGE).intValue() > 10);

    // Check the reverse order
    elements = new ArrayList<>();
    elements.add(generateElement(16,"db3", "tb6", null, CompactionType.MINOR, TxnStore.INITIATED_RESPONSE,
        start - 100000L));
    elements.add(generateElement(15,"db3", "tb5", null, CompactionType.MINOR, TxnStore.INITIATED_RESPONSE,
        start - 1000L));

    // Check that the age is older than 10s
    Assert.assertTrue(Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_OLDEST_ENQUEUE_AGE).intValue() > 10);
  }

  @Test
  public void testDBMetrics() throws Exception {
    String dbName = "default";
    String tblName = "dcamc";
    Table t = newTable(dbName, tblName, false);

    long start = System.currentTimeMillis();
    burnThroughTransactions(t.getDbName(), t.getTableName(), 24, new HashSet<>(Arrays.asList(22L, 23L, 24L)), null);
    openTxn(TxnType.REPL_CREATED);

    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, t.getDbName());
    comp.setTablename(t.getTableName());
    comp.setOperationType(DataOperationType.UPDATE);

    LockRequest req = new LockRequest(Lists.newArrayList(comp), "me", "localhost");
    req.setTxnid(22);
    LockResponse res = txnHandler.lock(req);
    Assert.assertEquals(LockState.ACQUIRED, res.getState());
    txnHandler.commitTxn(new CommitTxnRequest(22));

    req.setTxnid(23);
    res = txnHandler.lock(req);
    Assert.assertEquals(LockState.ACQUIRED, res.getState());
    Thread.sleep(1000);

    runAcidMetricService();
    long diff = (System.currentTimeMillis() - start) / 1000;

    Assert.assertEquals(24,
        Metrics.getOrCreateGauge(MetricsConstants.NUM_TXN_TO_WRITEID).intValue());
    Assert.assertEquals(1,
        Metrics.getOrCreateGauge(MetricsConstants.NUM_COMPLETED_TXN_COMPONENTS).intValue());

    Assert.assertEquals(2,
        Metrics.getOrCreateGauge(MetricsConstants.NUM_OPEN_NON_REPL_TXNS).intValue());
    Assert.assertEquals(1,
        Metrics.getOrCreateGauge(MetricsConstants.NUM_OPEN_REPL_TXNS).intValue());

    Assert.assertEquals(23,
        Metrics.getOrCreateGauge(MetricsConstants.OLDEST_OPEN_NON_REPL_TXN_ID).longValue());
    Assert.assertTrue(Metrics.getOrCreateGauge(MetricsConstants.OLDEST_OPEN_NON_REPL_TXN_AGE).intValue() <= diff);
    Assert.assertTrue(Metrics.getOrCreateGauge(MetricsConstants.OLDEST_OPEN_NON_REPL_TXN_AGE).intValue() >= 1);

    Assert.assertEquals(25,
        Metrics.getOrCreateGauge(MetricsConstants.OLDEST_OPEN_REPL_TXN_ID).longValue());
    Assert.assertTrue(Metrics.getOrCreateGauge(MetricsConstants.OLDEST_OPEN_REPL_TXN_AGE).intValue() <= diff);
    Assert.assertTrue(Metrics.getOrCreateGauge(MetricsConstants.OLDEST_OPEN_REPL_TXN_AGE).intValue() >= 1);

    Assert.assertEquals(1,
        Metrics.getOrCreateGauge(MetricsConstants.NUM_LOCKS).intValue());
    Assert.assertTrue(Metrics.getOrCreateGauge(MetricsConstants.OLDEST_LOCK_AGE).intValue() <= diff);
    Assert.assertTrue(Metrics.getOrCreateGauge(MetricsConstants.OLDEST_LOCK_AGE).intValue() >= 1);

    txnHandler.cleanTxnToWriteIdTable();
    runAcidMetricService();
    Assert.assertEquals(2,
        Metrics.getOrCreateGauge(MetricsConstants.NUM_TXN_TO_WRITEID).intValue());

    start = System.currentTimeMillis();
    burnThroughTransactions(dbName, tblName, 3, null, new HashSet<>(Arrays.asList(26L, 28L)));
    Thread.sleep(1000);

    runAcidMetricService();
    diff = (System.currentTimeMillis() - start) / 1000;

    Assert.assertTrue(Metrics.getOrCreateGauge(MetricsConstants.OLDEST_ABORTED_TXN_AGE).intValue() <= diff);
    Assert.assertTrue(Metrics.getOrCreateGauge(MetricsConstants.OLDEST_ABORTED_TXN_AGE).intValue() >= 1);

    Assert.assertEquals(26,
        Metrics.getOrCreateGauge(MetricsConstants.OLDEST_ABORTED_TXN_ID).longValue());
    Assert.assertEquals(2,
        Metrics.getOrCreateGauge(MetricsConstants.NUM_ABORTED_TXNS).intValue());
  }

  @Test
  public void testTxnHandlerCounters() throws Exception {
    String dbName = "default";
    String tblName = "txnhandlercounters";
    Table t = newTable(dbName, tblName, false);

    burnThroughTransactions(t.getDbName(), t.getTableName(), 3, null, new HashSet<>(Arrays.asList(2L, 3L)));
    Assert.assertEquals(MetricsConstants.TOTAL_NUM_ABORTED_TXNS + " value incorrect",
            2, Metrics.getOrCreateCounter(MetricsConstants.TOTAL_NUM_ABORTED_TXNS).getCount());
    Assert.assertEquals(MetricsConstants.TOTAL_NUM_COMMITTED_TXNS + " value incorrect",
            1, Metrics.getOrCreateCounter(MetricsConstants.TOTAL_NUM_COMMITTED_TXNS).getCount());

    burnThroughTransactions(t.getDbName(), t.getTableName(), 3, null, new HashSet<>(Collections.singletonList(4L)));
    Assert.assertEquals(MetricsConstants.TOTAL_NUM_ABORTED_TXNS + " value incorrect",
            3, Metrics.getOrCreateCounter(MetricsConstants.TOTAL_NUM_ABORTED_TXNS).getCount());
    Assert.assertEquals(MetricsConstants.TOTAL_NUM_COMMITTED_TXNS + " value incorrect",
            3, Metrics.getOrCreateCounter(MetricsConstants.TOTAL_NUM_COMMITTED_TXNS).getCount());
  }

  @Test
  public void testDeltaFilesMetric() throws Exception {
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED, true);
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_MAX_CACHE_SIZE, 2);

    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_OBSOLETE_DELTA_NUM_THRESHOLD, 100);
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_DELTA_NUM_THRESHOLD, 100);
    HiveConf.setTimeVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_CACHE_DURATION, 5, TimeUnit.SECONDS);

    MetricsFactory.close();
    MetricsFactory.init(conf);

    HiveConf.setTimeVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_REPORTING_INTERVAL, 1, TimeUnit.SECONDS);
    DeltaFilesMetricReporter.init(conf);

    TezCounters tezCounters = new TezCounters();
    tezCounters.findCounter(NUM_OBSOLETE_DELTAS + "", "acid/p=1").setValue(200);
    tezCounters.findCounter(NUM_OBSOLETE_DELTAS + "", "acid/p=2").setValue(100);
    tezCounters.findCounter(NUM_OBSOLETE_DELTAS + "", "acid/p=3").setValue(150);
    tezCounters.findCounter(NUM_OBSOLETE_DELTAS + "", "acid_v2").setValue(250);

    tezCounters.findCounter(NUM_DELTAS + "", "acid/p=1").setValue(150);
    tezCounters.findCounter(NUM_DELTAS + "", "acid/p=2").setValue(100);
    tezCounters.findCounter(NUM_DELTAS + "", "acid/p=3").setValue(250);
    tezCounters.findCounter(NUM_DELTAS + "", "acid_v2").setValue(200);

    tezCounters.findCounter(NUM_SMALL_DELTAS + "", "acid/p=1").setValue(250);
    tezCounters.findCounter(NUM_SMALL_DELTAS + "", "acid/p=2").setValue(200);
    tezCounters.findCounter(NUM_SMALL_DELTAS + "", "acid/p=3").setValue(150);
    tezCounters.findCounter(NUM_SMALL_DELTAS + "", "acid_v2").setValue(100);

    DeltaFilesMetricReporter.getInstance().submit(tezCounters);
    Thread.sleep(1000);

    CodahaleMetrics metrics = (CodahaleMetrics) MetricsFactory.getInstance();
    Map<String, Gauge> gauges = metrics.getMetricRegistry().getGauges();

    Assert.assertTrue(
      equivalent(
        new HashMap<String, String>() {{
          put("acid_v2", "250");
          put("acid/p=1", "200");
        }}, gaugeToMap(MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS, gauges)));

    Assert.assertTrue(
      equivalent(
        new HashMap<String, String>() {{
          put("acid_v2", "200");
          put("acid/p=3", "250");
        }}, gaugeToMap(MetricsConstants.COMPACTION_NUM_DELTAS, gauges)));

    Assert.assertTrue(
      equivalent(
        new HashMap<String, String>() {{
          put("acid/p=1", "250");
          put("acid/p=2", "200");
        }}, gaugeToMap(MetricsConstants.COMPACTION_NUM_SMALL_DELTAS, gauges)));

    //time-out existing entries
    Thread.sleep(5000);

    tezCounters = new TezCounters();
    tezCounters.findCounter(NUM_DELTAS + "", "acid/p=2").setValue(150);
    DeltaFilesMetricReporter.getInstance().submit(tezCounters);
    Thread.sleep(1000);

    Assert.assertTrue(
      equivalent(
        new HashMap<String, String>() {{
          put("acid/p=2", "150");
        }}, gaugeToMap(MetricsConstants.COMPACTION_NUM_DELTAS, gauges)));

    DeltaFilesMetricReporter.close();
  }

  static boolean equivalent(Map<String, String> lhs, Map<String, String> rhs) {
    return lhs.size() == rhs.size() && Maps.difference(lhs, rhs).areEqual();
  }

  static Map<String, String> gaugeToMap(String metric, Map<String, Gauge> gauges) {
    String value = (String) gauges.get(metric).getValue();
    return value.isEmpty()? Collections.emptyMap() : Splitter.on(',').withKeyValueSeparator("->").split(value);
  }

  @Test
  public void testTablesWithXAbortedTxns() throws Exception {
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_TABLES_WITH_ABORTED_TXNS_THRESHOLD, 14);

    String dbName = "default";
    String tblName1 = "table1";
    String tblName2 = "table2";
    String tblName3 = "table3";
    Table t1 = newTable(dbName, tblName1, false);
    Table t2 = newTable(dbName, tblName2, false);
    Table t3 = newTable(dbName, tblName3, false);
    Set<Long> abort1 = LongStream.range(1, 16).boxed().collect(Collectors.toSet());
    Set<Long> abort2 = LongStream.range(21, 31).boxed().collect(Collectors.toSet());
    Set<Long> abort3 = LongStream.range(41, 61).boxed().collect(Collectors.toSet());

    burnThroughTransactions(t1.getDbName(), t1.getTableName(), 20, null, abort1);
    burnThroughTransactions(t2.getDbName(), t2.getTableName(), 20, null, abort2);
    burnThroughTransactions(t3.getDbName(), t3.getTableName(), 30, null, abort3);

    runAcidMetricService();

    Assert.assertEquals(MetricsConstants.TABLES_WITH_X_ABORTED_TXNS + " value incorrect",
        2, Metrics.getOrCreateGauge(MetricsConstants.TABLES_WITH_X_ABORTED_TXNS).intValue());
  }

  @Test
  public void testWritesToDisabledCompactionTable() throws Exception {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.TRANSACTIONAL_EVENT_LISTENERS, HMSMetricsListener.class.getName());
    txnHandler = TxnUtils.getTxnStore(conf);

    String dbName = "default";

    Map<String, String> params = new HashMap<>();
    params.put(hive_metastoreConstants.TABLE_NO_AUTO_COMPACT, "true");
    Table disabledTbl = newTable(dbName, "comp_disabled", false, params);
    burnThroughTransactions(disabledTbl.getDbName(), disabledTbl.getTableName(), 1, null, null);
    burnThroughTransactions(disabledTbl.getDbName(), disabledTbl.getTableName(), 1, null, new HashSet<>(Arrays.asList(2L)));

    Table enabledTbl = newTable(dbName, "comp_enabled", false);
    burnThroughTransactions(enabledTbl.getDbName(), enabledTbl.getTableName(), 1, null, null);

    Assert.assertEquals(MetricsConstants.WRITES_TO_DISABLED_COMPACTION_TABLE + " value incorrect",
        2, Metrics.getOrCreateGauge(MetricsConstants.WRITES_TO_DISABLED_COMPACTION_TABLE).intValue());
  }

  private ShowCompactResponseElement generateElement(long id, String db, String table, String partition,
      CompactionType type, String state) {
    return generateElement(id, db, table, partition, type, state, System.currentTimeMillis());
  }

  private ShowCompactResponseElement generateElement(long id, String db, String table, String partition,
      CompactionType type, String state, long enqueueTime) {
    ShowCompactResponseElement element = new ShowCompactResponseElement(db, table, type, state);
    element.setId(id);
    element.setPartitionname(partition);
    element.setEnqueueTime(enqueueTime);

    String runtimeId = ServerUtils.hostname() + "-" + ThreadLocalRandom.current().nextInt();
    element.setInitiatorId(runtimeId);
    element.setWorkerid(runtimeId);
    element.setInitiatorVersion("4.0.0");
    element.setWorkerVersion("4.0.0");
    return element;
  }

  private void interruptThread(long timeout, Thread target) {
    Thread t = new Thread(() -> {
      try {
        Thread.sleep(timeout);
        target.interrupt();
      } catch (Exception e) {}
    });
    t.setDaemon(true);
    t.start();
  }

  @Override
  boolean useHive130DeltaDirName() {
    return false;
  }
}
