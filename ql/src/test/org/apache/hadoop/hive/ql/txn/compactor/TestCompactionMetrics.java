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

import com.codahale.metrics.Counter;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.common.ServerUtils;
import org.apache.hadoop.hive.common.metrics.MetricsTestUtils;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HMSMetricsListener;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
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
import org.apache.hadoop.hive.metastore.txn.TxnHandler;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.ThrowingTxnHandler;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
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
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.hadoop.hive.metastore.metrics.AcidMetricService.replaceWhitespace;

public class TestCompactionMetrics  extends CompactorTest {

  private static final String INITIATED_METRICS_KEY = MetricsConstants.COMPACTION_STATUS_PREFIX + TxnStore.INITIATED_RESPONSE;
  private static final String INITIATOR_CYCLE_KEY = MetricsConstants.API_PREFIX + MetricsConstants.COMPACTION_INITIATOR_CYCLE;
  private static final String CLEANER_CYCLE_KEY = MetricsConstants.API_PREFIX + MetricsConstants.COMPACTION_CLEANER_CYCLE;
  private static final String WORKER_CYCLE_KEY = MetricsConstants.API_PREFIX + MetricsConstants.COMPACTION_WORKER_CYCLE;

  @Before
  public void setUp() throws Exception {
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED, true);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.TXN_USE_MIN_HISTORY_LEVEL, true);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_ON, true);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_CLEANER_ON, true);
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

    final String DB_NAME = "default";
    final String OLD_TABLE_NAME = "old_rfc";
    final String OLD_PARTITION_NAME = "part";
    final String YOUNG_TABLE_NAME = "young_rfc";
    final String YOUNG_PARTITION_NAME = "part";

    long oldTableStart = System.currentTimeMillis();
    Table old = newTable(DB_NAME, OLD_TABLE_NAME, true);
    Partition oldP = newPartition(old, OLD_PARTITION_NAME);
    addBaseFile(old, oldP, 20L, 20);
    addDeltaFile(old, oldP, 21L, 22L, 2);
    addDeltaFile(old, oldP, 23L, 24L, 2);
    burnThroughTransactions(DB_NAME, OLD_TABLE_NAME, 25);
    doCompaction(DB_NAME, OLD_TABLE_NAME, OLD_PARTITION_NAME, CompactionType.MINOR);
    long oldTableEnd = System.currentTimeMillis();

    Table young = newTable(DB_NAME, YOUNG_TABLE_NAME, true);
    Partition youngP = newPartition(young, YOUNG_PARTITION_NAME);
    addBaseFile(young, youngP, 20L, 20);
    addDeltaFile(young, youngP, 21L, 22L, 2);
    addDeltaFile(young, youngP, 23L, 24L, 2);
    burnThroughTransactions(DB_NAME, YOUNG_TABLE_NAME, 25);
    doCompaction(DB_NAME, YOUNG_TABLE_NAME, YOUNG_PARTITION_NAME, CompactionType.MINOR);

    long acidMetricsStart = System.currentTimeMillis();
    runAcidMetricService();
    long now = System.currentTimeMillis();
    long acidMetricsDuration = now - acidMetricsStart;

    int oldestAgeInSeconds = Metrics.getOrCreateGauge(MetricsConstants.OLDEST_READY_FOR_CLEANING_AGE)
        .intValue();

    long ageInMillies = oldestAgeInSeconds * 1000L;

    long DB_ROUNDING_DOWN_ERROR = 1000L;
    long readError = acidMetricsDuration + DB_ROUNDING_DOWN_ERROR;

    long oldStartShiftedToNow = oldTableStart + ageInMillies;
    long oldEndShiftedToNow = (oldTableEnd + ageInMillies) + readError;
    Assert.assertTrue((oldStartShiftedToNow < now) && (now < oldEndShiftedToNow));
  }

  @Test
  public void  testInitiatorNoFailure() throws Exception {
    startInitiator();
    Counter counter = Metrics.getOrCreateCounter(MetricsConstants.COMPACTION_INITIATOR_FAILURE_COUNTER);
    Assert.assertEquals("Count incorrect", 0, counter.getCount());
  }

  @Test
  public void  testCleanerNoFailure() throws Exception {
    startCleaner();
    Counter counter = Metrics.getOrCreateCounter(MetricsConstants.COMPACTION_CLEANER_FAILURE_COUNTER);
    Assert.assertEquals("Count incorrect", 0, counter.getCount());
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
    Counter failureCounter = Metrics.getOrCreateCounter(MetricsConstants.COMPACTION_INITIATOR_FAILURE_COUNTER);
    Assert.assertEquals("count mismatch", 0, failureCounter.getCount());
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
    Counter failureCounter = Metrics.getOrCreateCounter(MetricsConstants.COMPACTION_CLEANER_FAILURE_COUNTER);
    Assert.assertEquals("count mismatch", 0, failureCounter.getCount());
  }

  @Test
  public void testCleanerPerfMetricsEnabled() throws Exception {
    long cleanerCyclesMinor = Objects.requireNonNull(
        Metrics.getOrCreateTimer(CLEANER_CYCLE_KEY + "_" + CompactionType.MINOR.toString().toLowerCase())).getCount();
    long cleanerCyclesMajor = Objects.requireNonNull(
        Metrics.getOrCreateTimer(CLEANER_CYCLE_KEY + "_" + CompactionType.MAJOR.toString().toLowerCase())).getCount();

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
        Metrics.getOrCreateTimer(CLEANER_CYCLE_KEY + "_" + CompactionType.MINOR.toString().toLowerCase())).getCount());

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
        Metrics.getOrCreateTimer(CLEANER_CYCLE_KEY + "_" + CompactionType.MAJOR.toString().toLowerCase())).getCount());
  }

  @Test
  public void testCleanerPerfMetricsDisabled() throws Exception {
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED, false);
    Metrics.initialize(conf);

    long cleanerCyclesMinor = Objects.requireNonNull(
      Metrics.getOrCreateTimer(CLEANER_CYCLE_KEY + "_" + CompactionType.MAJOR.toString().toLowerCase())).getCount();

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
        Metrics.getOrCreateTimer(CLEANER_CYCLE_KEY + "_" + CompactionType.MAJOR.toString().toLowerCase())).getCount());
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
        WORKER_CYCLE_KEY + "_" + CompactionType.MINOR.toString().toLowerCase(), 1);

    startCleaner();

    rqst = new CompactionRequest("default", "mapwb", CompactionType.MAJOR);
    rqst.setPartitionname("ds=today");
    txnHandler.compact(rqst);

    startWorker();

    rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(2, rsp.getCompactsSize());
    Assert.assertEquals(TxnStore.SUCCEEDED_RESPONSE, rsp.getCompacts()
        .stream()
        .filter(c -> c.getType().equals(CompactionType.MINOR))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("Could not found minor compaction")).getState());
    Assert.assertEquals(TxnStore.CLEANING_RESPONSE, rsp.getCompacts()
        .stream()
        .filter(c -> c.getType().equals(CompactionType.MAJOR))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("Could not found minor compaction")).getState());

    json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.TIMER,
        WORKER_CYCLE_KEY + "_" + CompactionType.MAJOR.toString().toLowerCase(), 1);
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

    elements.add(generateElement(6,"db1", "tb", null, CompactionType.MINOR, TxnStore.FAILED_RESPONSE,
            System.currentTimeMillis(), true, WORKER_VERSION, WORKER_VERSION, 10));
    elements.add(generateElement(7,"db1", "tb2", null, CompactionType.MINOR, TxnStore.FAILED_RESPONSE));
    elements.add(generateElement(8,"db1", "tb3", null, CompactionType.MINOR, TxnStore.FAILED_RESPONSE));

    elements.add(generateElement(9,"db2", "tb", null, CompactionType.MINOR, TxnStore.SUCCEEDED_RESPONSE));
    elements.add(generateElement(10,"db2", "tb2", null, CompactionType.MINOR, TxnStore.SUCCEEDED_RESPONSE));
    elements.add(generateElement(11,"db2", "tb3", null, CompactionType.MINOR, TxnStore.SUCCEEDED_RESPONSE));
    elements.add(generateElement(12,"db2", "tb4", null, CompactionType.MINOR, TxnStore.SUCCEEDED_RESPONSE));

    elements.add(generateElement(13,"db3", "tb3", null, CompactionType.MINOR, TxnStore.WORKING_RESPONSE));
    // test null initiator version and worker version
    elements.add(generateElement(14,"db3", "tb4", null, CompactionType.MINOR, TxnStore.WORKING_RESPONSE,
            System.currentTimeMillis(), false, null, null,20));
    elements.add(generateElement(15,"db3", "tb5", null, CompactionType.MINOR, TxnStore.WORKING_RESPONSE,
            System.currentTimeMillis(),true, WORKER_VERSION, WORKER_VERSION, 30));
    elements.add(generateElement(16,"db3", "tb6", null, CompactionType.MINOR, TxnStore.WORKING_RESPONSE));
    elements.add(generateElement(17,"db3", "tb7", null, CompactionType.MINOR, TxnStore.WORKING_RESPONSE,
            System.currentTimeMillis(),true, WORKER_VERSION, WORKER_VERSION,40));

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

    Assert.assertEquals(1,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_NUM_INITIATORS).intValue());
    Assert.assertEquals(1,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_NUM_WORKERS).intValue());
    Assert.assertEquals(1,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_NUM_INITIATOR_VERSIONS).intValue());
    Assert.assertEquals(1,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_NUM_WORKER_VERSIONS).intValue());
  }

  @Test
  public void testAgeMetricsNotSet() {
    List<ShowCompactResponseElement> elements = ImmutableList.of(
        generateElement(1, "db", "tb", null, CompactionType.MAJOR, TxnStore.FAILED_RESPONSE, 1L),
        generateElement(5, "db", "tb3", "p1", CompactionType.MINOR, TxnStore.DID_NOT_INITIATE_RESPONSE, 2L),
        generateElement(9, "db2", "tb", null, CompactionType.MINOR, TxnStore.SUCCEEDED_RESPONSE, 3L)
    );

    ShowCompactResponse scr = new ShowCompactResponse();
    scr.setCompacts(elements);
    AcidMetricService.updateMetricsFromShowCompact(scr);

    // Check that it is not set
    Assert.assertEquals(0, Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_OLDEST_ENQUEUE_AGE).intValue());
    Assert.assertEquals(0, Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_OLDEST_WORKING_AGE).intValue());
    Assert.assertEquals(0, Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_OLDEST_CLEANING_AGE).intValue());
  }

  @Test
  public void testInitiatedAgeMetrics() {
    ShowCompactResponse scr = new ShowCompactResponse();
    long start = System.currentTimeMillis() - 1000L;
    List<ShowCompactResponseElement> elements = ImmutableList.of(
        generateElement(15, "db3", "tb5", null, CompactionType.MINOR, TxnStore.INITIATED_RESPONSE, start)
    );

    scr.setCompacts(elements);
    AcidMetricService.updateMetricsFromShowCompact(scr);
    long diff = (System.currentTimeMillis() - start) / 1000;

    // Check that we have at least 1s old compaction age, but not more than expected
    int age = Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_OLDEST_ENQUEUE_AGE).intValue();
    Assert.assertTrue(age <= diff);
    Assert.assertTrue(age >= 1);
  }

  @Test
  public void testWorkingAgeMetrics() {
    ShowCompactResponse scr = new ShowCompactResponse();

    long start = System.currentTimeMillis() - 1000L;
    List<ShowCompactResponseElement> elements = ImmutableList.of(
        generateElement(17, "db3", "tb7", null, CompactionType.MINOR, TxnStore.WORKING_RESPONSE,
            System.currentTimeMillis(), true, WORKER_VERSION, WORKER_VERSION, start)
    );

    scr.setCompacts(elements);
    AcidMetricService.updateMetricsFromShowCompact(scr);
    long diff = (System.currentTimeMillis() - start) / 1000;

    // Check that we have at least 1s old compaction age, but not more than expected
    int age = Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_OLDEST_WORKING_AGE).intValue();
    Assert.assertTrue(age <= diff);
    Assert.assertTrue(age >= 1);
  }

  @Test
  public void testCleaningAgeMetrics() {
    ShowCompactResponse scr = new ShowCompactResponse();

    long start = System.currentTimeMillis() - 1000L;
    List<ShowCompactResponseElement> elements = ImmutableList.of(
        generateElement(19, "db3", "tb7", null, CompactionType.MINOR, TxnStore.CLEANING_RESPONSE,
            System.currentTimeMillis(), true, WORKER_VERSION, WORKER_VERSION, -1L, start)
    );

    scr.setCompacts(elements);
    AcidMetricService.updateMetricsFromShowCompact(scr);
    long diff = (System.currentTimeMillis() - start) / 1000;

    // Check that we have at least 1s old compaction age, but not more than expected
    int age = Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_OLDEST_CLEANING_AGE).intValue();
    Assert.assertTrue(age <= diff);
    Assert.assertTrue(age >= 1);
  }

  @Test
  public void testInitiatedAgeMetricsOrder() {
    ShowCompactResponse scr = new ShowCompactResponse();
    long start = System.currentTimeMillis();

    List<ShowCompactResponseElement> elements = ImmutableList.of(
        generateElement(15, "db3", "tb5", null, CompactionType.MINOR, TxnStore.INITIATED_RESPONSE,
            start - 1_000L),
        generateElement(16, "db3", "tb6", null, CompactionType.MINOR, TxnStore.INITIATED_RESPONSE,
            start - 15_000L)
    );

    scr.setCompacts(elements);
    AcidMetricService.updateMetricsFromShowCompact(scr);
    // Check that the age is older than 10s
    Assert.assertTrue(Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_OLDEST_ENQUEUE_AGE).intValue() > 10);

    // Check the reverse order
    elements = ImmutableList.of(
        generateElement(16, "db3", "tb6", null, CompactionType.MINOR, TxnStore.INITIATED_RESPONSE,
            start - 25_000L),
        generateElement(15, "db3", "tb5", null, CompactionType.MINOR, TxnStore.INITIATED_RESPONSE,
            start - 1_000L)
    );
    scr.setCompacts(elements);
    AcidMetricService.updateMetricsFromShowCompact(scr);

    // Check that the age is older than 20s
    Assert.assertTrue(Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_OLDEST_ENQUEUE_AGE).intValue() > 20);
  }

  @Test
  public void testWorkingAgeMetricsOrder() {
    ShowCompactResponse scr = new ShowCompactResponse();
    long start = System.currentTimeMillis();

    List<ShowCompactResponseElement> elements = ImmutableList.of(
        generateElement(15, "db3", "tb5", null, CompactionType.MINOR, TxnStore.WORKING_RESPONSE,
            start, false, WORKER_VERSION, WORKER_VERSION, start - 1_000L),
        generateElement(16, "db3", "tb6", null, CompactionType.MINOR, TxnStore.WORKING_RESPONSE,
            start, false, WORKER_VERSION, WORKER_VERSION, start - 15_000L)
    );

    scr.setCompacts(elements);
    AcidMetricService.updateMetricsFromShowCompact(scr);
    // Check that the age is older than 10s
    Assert.assertTrue(Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_OLDEST_WORKING_AGE).intValue() > 10);

    // Check the reverse order
    elements = ImmutableList.of(
        generateElement(16, "db3", "tb6", null, CompactionType.MINOR, TxnStore.WORKING_RESPONSE,
            start, false, WORKER_VERSION, WORKER_VERSION, start - 25_000L),
        generateElement(15, "db3", "tb5", null, CompactionType.MINOR, TxnStore.WORKING_RESPONSE,
            start, false, WORKER_VERSION, WORKER_VERSION, start - 1_000L)
    );
    scr.setCompacts(elements);
    AcidMetricService.updateMetricsFromShowCompact(scr);

    // Check that the age is older than 20s
    Assert.assertTrue(Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_OLDEST_WORKING_AGE).intValue() > 20);
  }

  @Test
  public void testCleaningAgeMetricsOrder() {
    ShowCompactResponse scr = new ShowCompactResponse();
    long start = System.currentTimeMillis();

    List<ShowCompactResponseElement> elements = ImmutableList.of(
        generateElement(15, "db3", "tb5", null, CompactionType.MINOR, TxnStore.CLEANING_RESPONSE,
            start, false, WORKER_VERSION, WORKER_VERSION, -1L, start - 1_000L),
        generateElement(16, "db3", "tb6", null, CompactionType.MINOR, TxnStore.CLEANING_RESPONSE,
            start, false, WORKER_VERSION, WORKER_VERSION, -1L, start - 15_000L)
    );

    scr.setCompacts(elements);
    AcidMetricService.updateMetricsFromShowCompact(scr);
    // Check that the age is older than 10s
    Assert.assertTrue(Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_OLDEST_CLEANING_AGE).intValue() > 10);

    // Check the reverse order
    elements = ImmutableList.of(
        generateElement(16, "db3", "tb6", null, CompactionType.MINOR, TxnStore.CLEANING_RESPONSE,
            start, false, WORKER_VERSION, WORKER_VERSION, -1L, start - 25_000L),
        generateElement(15, "db3", "tb5", null, CompactionType.MINOR, TxnStore.CLEANING_RESPONSE,
            start, false, WORKER_VERSION, WORKER_VERSION, -1L, start - 1_000L)
    );
    scr.setCompacts(elements);
    AcidMetricService.updateMetricsFromShowCompact(scr);

    // Check that the age is older than 20s
    Assert.assertTrue(Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_OLDEST_CLEANING_AGE).intValue() > 20);
  }

  @Test
  public void testDBMetrics() throws Exception {
    String dbName = "default";
    String tblName = "dcamc";
    Table t = newTable(dbName, tblName, false);

    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.TXN_USE_MIN_HISTORY_LEVEL, false);
    TxnHandler.ConfVars.setUseMinHistoryLevel(false);
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
    Assert.assertEquals(3,
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

    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, dbName);
    comp.setOperationType(DataOperationType.INSERT);
    LockRequest lockReq = new LockRequest(Lists.newArrayList(comp), "me", "localhost");

    comp.setTablename(t1.getTableName());
    burnThroughTransactions(t1.getDbName(), t1.getTableName(), 20, null, abort1, lockReq);
    comp.setTablename(t2.getTableName());
    burnThroughTransactions(t2.getDbName(), t2.getTableName(), 20, null, abort2, lockReq);
    comp.setTablename(t3.getTableName());
    burnThroughTransactions(t3.getDbName(), t3.getTableName(), 30, null, abort3, lockReq);

    runAcidMetricService();

    Assert.assertEquals(MetricsConstants.TABLES_WITH_X_ABORTED_TXNS + " value incorrect",
        2, Metrics.getOrCreateGauge(MetricsConstants.TABLES_WITH_X_ABORTED_TXNS).intValue());
  }

  @Test
  public void testPartTablesWithXAbortedTxns() throws Exception {
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_TABLES_WITH_ABORTED_TXNS_THRESHOLD, 4);

    String dbName = "default";
    String tblName = "table";

    String part1 = "p1";
    String part2 = "p2";
    String part3 = "p3";

    Table t = newTable(dbName, tblName, true);
    newPartition(t, part1);
    newPartition(t, part2);
    newPartition(t, part3);
    String partPattern = t.getPartitionKeys().get(0).getName() + "=%s";

    Set<Long> abort1 = LongStream.range(1, 6).boxed().collect(Collectors.toSet());
    Set<Long> abort2 = LongStream.range(11, 16).boxed().collect(Collectors.toSet());

    LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.PARTITION, dbName);
    comp.setTablename(tblName);
    comp.setOperationType(DataOperationType.INSERT);
    LockRequest lockReq = new LockRequest(Lists.newArrayList(comp), "me", "localhost");


    comp.setPartitionname(String.format(partPattern, part1));
    burnThroughTransactions(t.getDbName(), t.getTableName(), 10, null, abort1, lockReq);

    comp.setPartitionname(String.format(partPattern, part2));
    burnThroughTransactions(t.getDbName(), t.getTableName(), 10, null, abort2, lockReq);

    comp.setPartitionname(String.format(partPattern, part3));
    burnThroughTransactions(t.getDbName(), t.getTableName(), 10, null, null, lockReq);

    runAcidMetricService();

    Assert.assertEquals(MetricsConstants.TABLES_WITH_X_ABORTED_TXNS + " value incorrect",
        2, Metrics.getOrCreateGauge(MetricsConstants.TABLES_WITH_X_ABORTED_TXNS).intValue());
  }

  /**
   * See also org.apache.hadoop.hive.ql.TestTxnCommands3#testWritesToDisabledCompactionTableCtas()
   */
  @Test
  public void testWritesToDisabledCompactionTable() throws Exception {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.TRANSACTIONAL_EVENT_LISTENERS, HMSMetricsListener.class.getName());
    txnHandler = TxnUtils.getTxnStore(conf);

    String dbName = "default";

    Map<String, String> params = new HashMap<>();
    params.put(hive_metastoreConstants.NO_AUTO_COMPACT, "true");
    Table disabledTbl = newTable(dbName, "comp_disabled", false, params);
    burnThroughTransactions(disabledTbl.getDbName(), disabledTbl.getTableName(), 1, null, null);
    burnThroughTransactions(disabledTbl.getDbName(), disabledTbl.getTableName(), 1, null, new HashSet<>(
        Collections.singletonList(2L)));

    Table enabledTbl = newTable(dbName, "comp_enabled", false);
    burnThroughTransactions(enabledTbl.getDbName(), enabledTbl.getTableName(), 1, null, null);

    Assert.assertEquals(MetricsConstants.WRITES_TO_DISABLED_COMPACTION_TABLE + " value incorrect",
        2, Metrics.getOrCreateGauge(MetricsConstants.WRITES_TO_DISABLED_COMPACTION_TABLE).intValue());
  }

  @Test
  public void testInitiatorDurationMeasuredCorrectly() throws Exception {
    final String DEFAULT_DB = "default";
    final String TABLE_NAME = "x_table";
    final String PARTITION_NAME = "part";

    List<LockComponent> components = new ArrayList<>();

    Table table = newTable(DEFAULT_DB, TABLE_NAME, true);

    for (int i = 0; i < 10; i++) {
      String partitionName = PARTITION_NAME + i;
      Partition p = newPartition(table, partitionName);

      addBaseFile(table, p, 20L, 20);
      addDeltaFile(table, p, 21L, 22L, 2);
      addDeltaFile(table, p, 23L, 24L, 2);
      addDeltaFile(table, p, 21L, 24L, 4);

      LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.PARTITION, DEFAULT_DB);
      comp.setTablename(TABLE_NAME);
      comp.setPartitionname("ds=" + partitionName);
      comp.setOperationType(DataOperationType.UPDATE);
      components.add(comp);
    }
    burnThroughTransactions(DEFAULT_DB, TABLE_NAME, 25);

    long txnId = openTxn();

    LockRequest req = new LockRequest(components, "me", "localhost");
    req.setTxnid(txnId);
    LockResponse res = txnHandler.lock(req);
    Assert.assertEquals(LockState.ACQUIRED, res.getState());

    allocateWriteId(DEFAULT_DB, TABLE_NAME, txnId);
    txnHandler.commitTxn(new CommitTxnRequest(txnId));

    long initiatorStart = System.currentTimeMillis();
    startInitiator();
    long durationUpperLimit = System.currentTimeMillis() - initiatorStart;
    int initiatorDurationFromMetric = Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_INITIATOR_CYCLE_DURATION)
        .intValue();
    Assert.assertTrue("Initiator duration must be withing the limits",
        (0 < initiatorDurationFromMetric) && (initiatorDurationFromMetric <= durationUpperLimit));
  }

  @Test
  public void testCleanerDurationMeasuredCorrectly() throws Exception {
    conf.setIntVar(HiveConf.ConfVars.COMPACTOR_MAX_NUM_DELTA, 1);

    final String DB_NAME = "default";
    final String TABLE_NAME = "x_table";
    final String PARTITION_NAME = "part";

    Table table = newTable(DB_NAME, TABLE_NAME, true);
    Partition partition = newPartition(table, PARTITION_NAME);
    addBaseFile(table, partition, 20L, 20);
    addDeltaFile(table, partition, 21L, 22L, 2);
    addDeltaFile(table, partition, 23L, 24L, 2);
    burnThroughTransactions(DB_NAME, TABLE_NAME, 25);
    doCompaction(DB_NAME, TABLE_NAME, PARTITION_NAME, CompactionType.MINOR);

    long cleanerStart = System.currentTimeMillis();
    startCleaner();
    long durationUpperLimit = System.currentTimeMillis() - cleanerStart;
    int cleanerDurationFromMetric = Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_CLEANER_CYCLE_DURATION)
        .intValue();
    Assert.assertTrue("Cleaner duration must be withing the limits",
        (0 < cleanerDurationFromMetric) && (cleanerDurationFromMetric <= durationUpperLimit));
  }

  @Test
  public void testInitiatorFailuresCountedCorrectly() throws Exception {
    final String DEFAULT_DB = "default";
    final String SUCCESS_TABLE_NAME = "success_table";
    final String FAILING_TABLE_NAME = "failing_table";
    final String PARTITION_NAME = "part";
    final long EXPECTED_SUCCESS_COUNT = 10;
    final long EXPECTED_FAIL_COUNT = 6;

    ControlledFailingTxHandler.failedTableName = FAILING_TABLE_NAME;
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.TXN_STORE_IMPL,
        "org.apache.hadoop.hive.ql.txn.compactor.TestCompactionMetrics$ControlledFailingTxHandler");

    Table failedTable = newTable(DEFAULT_DB, FAILING_TABLE_NAME, true);
    Table succeededTable = newTable(DEFAULT_DB, SUCCESS_TABLE_NAME, true);

    for (Table table : new Table[] { succeededTable, failedTable }) {
      List<LockComponent> components = new ArrayList<>();

      String tableName = table.getTableName();

      long partitionCount = FAILING_TABLE_NAME.equals(tableName) ? EXPECTED_FAIL_COUNT : EXPECTED_SUCCESS_COUNT;
      for (int i = 0; i < partitionCount; i++) {
        String partitionName = PARTITION_NAME + i;
        Partition p = newPartition(table, partitionName);

        addBaseFile(table, p, 20L, 20);
        addDeltaFile(table, p, 21L, 22L, 2);
        addDeltaFile(table, p, 23L, 24L, 2);
        addDeltaFile(table, p, 21L, 24L, 4);

        LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.PARTITION, DEFAULT_DB);
        comp.setTablename(tableName);
        comp.setPartitionname("ds=" + partitionName);
        comp.setOperationType(DataOperationType.UPDATE);
        components.add(comp);
      }

      burnThroughTransactions(DEFAULT_DB, tableName, 25);

      long txnid = openTxn();

      LockRequest req = new LockRequest(components, "me", "localhost");
      req.setTxnid(txnid);
      LockResponse res = txnHandler.lock(req);
      Assert.assertEquals(LockState.ACQUIRED, res.getState());

      long writeid = allocateWriteId(DEFAULT_DB, tableName, txnid);
      Assert.assertEquals(26, writeid);
      txnHandler.commitTxn(new CommitTxnRequest(txnid));
    }

    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_REQUEST_QUEUE, 5);
    startInitiator();

    // Check if all the compaction have initiated
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(EXPECTED_FAIL_COUNT + EXPECTED_SUCCESS_COUNT, rsp.getCompactsSize());

    Assert.assertEquals(EXPECTED_FAIL_COUNT,
        Metrics.getOrCreateCounter(MetricsConstants.COMPACTION_INITIATOR_FAILURE_COUNTER)
            .getCount());
  }

  @Test
  public void testCleanerFailuresCountedCorrectly() throws Exception {
    final String DEFAULT_DB = "default";
    final String SUCCESS_TABLE_NAME = "success_table";
    final String FAILING_TABLE_NAME = "failing_table";
    final String PARTITION_NAME = "part";
    final long EXPECTED_SUCCESS_COUNT = 10;
    final long EXPECTED_FAIL_COUNT = 6;

    ControlledFailingTxHandler.failedTableName = FAILING_TABLE_NAME;
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.TXN_STORE_IMPL,
        "org.apache.hadoop.hive.ql.txn.compactor.TestCompactionMetrics$ControlledFailingTxHandler");

    Table failedTable = newTable(DEFAULT_DB, FAILING_TABLE_NAME, true);
    Table succeededTable = newTable(DEFAULT_DB, SUCCESS_TABLE_NAME, true);

    for (Table table : new Table[] { succeededTable, failedTable }) {

      String tableName = table.getTableName();

      long partitionCount = FAILING_TABLE_NAME.equals(tableName) ? EXPECTED_FAIL_COUNT : EXPECTED_SUCCESS_COUNT;
      for (int i = 0; i < partitionCount; i++) {
        Partition p = newPartition(table, PARTITION_NAME + i);

        addBaseFile(table, p, 20L, 20);
        addDeltaFile(table, p, 21L, 22L, 2);
        addDeltaFile(table, p, 23L, 24L, 2);
        addDeltaFile(table, p, 21L, 24L, 4);
      }

      burnThroughTransactions(DEFAULT_DB, tableName, 25);
      for (int i = 0; i < partitionCount; i++) {
        CompactionRequest rqst = new CompactionRequest(DEFAULT_DB, tableName, CompactionType.MINOR);
        rqst.setPartitionname("ds=" + PARTITION_NAME + i);
        compactInTxn(rqst);
      }
    }

    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_CLEANER_THREADS_NUM, 5);
    startCleaner();

    // Check there are no compactions requests left.
    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(EXPECTED_FAIL_COUNT + EXPECTED_SUCCESS_COUNT, rsp.getCompactsSize());

    Assert.assertEquals(EXPECTED_FAIL_COUNT,
        Metrics.getOrCreateCounter(MetricsConstants.COMPACTION_CLEANER_FAILURE_COUNTER)
            .getCount());
  }

  private ShowCompactResponseElement generateElement(long id, String db, String table, String partition,
      CompactionType type, String state) {
    return generateElement(id, db, table, partition, type, state, System.currentTimeMillis());
  }

  private ShowCompactResponseElement generateElement(long id, String db, String table, String partition,
      CompactionType type, String state, long enqueueTime) {
    return generateElement(id, db, table, partition, type, state, enqueueTime, false);
  }

  private ShowCompactResponseElement generateElement(long id, String db, String table, String partition,
      CompactionType type, String state, long enqueueTime, boolean manuallyInitiatedCompaction) {
    return generateElement(id, db, table, partition, type, state, enqueueTime, manuallyInitiatedCompaction,
            null, null, -1);
  }

  private ShowCompactResponseElement generateElement(long id, String db, String table, String partition,
          CompactionType type, String state, long enqueueTime, boolean manuallyInitiatedCompaction,
          String initiatorVersion, String workerVersion, long startTime) {
    return generateElement(id, db, table, partition, type, state, enqueueTime, manuallyInitiatedCompaction,
        initiatorVersion, workerVersion, startTime, -1L);
  }

  private ShowCompactResponseElement generateElement(long id, String db, String table, String partition,
          CompactionType type, String state, long enqueueTime, boolean manuallyInitiatedCompaction,
          String initiatorVersion, String workerVersion, long startTime, long cleanerStartTime) {
    ShowCompactResponseElement element = new ShowCompactResponseElement(db, table, type, state);
    element.setId(id);
    element.setPartitionname(partition);
    element.setEnqueueTime(enqueueTime);

    String runtimeId;
    if (manuallyInitiatedCompaction) {
      runtimeId ="hs2-host-" +
          ThreadLocalRandom.current().nextInt(999) + "-" + HiveMetaStoreClient.MANUALLY_INITIATED_COMPACTION;
    } else {
      runtimeId = ServerUtils.hostname() + "-" + ThreadLocalRandom.current().nextInt(999);
    }
    String workerId = "hs2-host-" + ThreadLocalRandom.current().nextInt(999);
    element.setInitiatorId(runtimeId);
    element.setWorkerid(workerId);
    element.setInitiatorVersion(initiatorVersion);
    element.setWorkerVersion(workerVersion);
    element.setStart(startTime);
    element.setCleanerStart(cleanerStartTime);
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

  private void doCompaction(String dbName, String tableName, String partitionName, CompactionType type)
      throws Exception {
    CompactionRequest rqst = new CompactionRequest(dbName, tableName, type);
    rqst.setPartitionname("ds=" + partitionName);
    txnHandler.compact(rqst);
    startWorker();
  }

  public static class ControlledFailingTxHandler extends ThrowingTxnHandler {
    public static volatile String failedTableName;

    public ControlledFailingTxHandler() {
    }

    @Override
    public GetValidWriteIdsResponse getValidWriteIds(GetValidWriteIdsRequest rqst) throws MetaException {
      if (rqst.getFullTableNames()
          .stream()
          .anyMatch(t -> t.endsWith("." + failedTableName))) {
        throw new RuntimeException("TxnHandler fails during getValidWriteIds");
      }
      return super.getValidWriteIds(rqst);
    }

    @Override
    public void markCleanerStart(CompactionInfo info) throws MetaException {
      if (failedTableName.equals(info.tableName)) {
        throw new RuntimeException("TxnHandler fails during MarkCleaned");
      }
      super.markCleanerStart(info);
    }
  }

  @Override
  boolean useHive130DeltaDirName() {
    return false;
  }
}
