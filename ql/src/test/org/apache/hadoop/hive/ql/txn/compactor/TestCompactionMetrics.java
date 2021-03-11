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

import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
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
import org.apache.hadoop.hive.metastore.metrics.AcidMetricService;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestCompactionMetrics  extends CompactorTest {
  private final String INITIATED_METRICS_KEY = MetricsConstants.COMPACTION_STATUS_PREFIX + TxnStore.INITIATED_RESPONSE;

  @Test
  public void testInitiatorMetricsEnabled() throws Exception {
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED, true);
    Metrics.initialize(conf);
    int originalValue = Metrics.getOrCreateGauge(INITIATED_METRICS_KEY).intValue();
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

    // The metrics will appear after the next AcidMetricsService run
    runAcidMetricService();

    Assert.assertEquals(originalValue + 10,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_STATUS_PREFIX + TxnStore.INITIATED_RESPONSE).intValue());
  }

  @Test
  public void testInitiatorMetricsDisabled() throws Exception {
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED, false);
    Metrics.initialize(conf);
    int originalValue = Metrics.getOrCreateGauge(INITIATED_METRICS_KEY).intValue();
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

    // The metrics will appear after the next AcidMetricsService run
    runAcidMetricService();

    Assert.assertEquals(originalValue,
        Metrics.getOrCreateGauge(INITIATED_METRICS_KEY).intValue());
  }

  @Test
  public void testUpdateCompactionMetrics() {
    Metrics.initialize(conf);
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
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_STATUS_PREFIX + TxnStore.DID_NOT_INITIATE_RESPONSE).intValue());
    Assert.assertEquals(2,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_STATUS_PREFIX + TxnStore.INITIATED_RESPONSE).intValue());
    Assert.assertEquals(3,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_STATUS_PREFIX + TxnStore.FAILED_RESPONSE).intValue());
    Assert.assertEquals(4,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_STATUS_PREFIX + TxnStore.SUCCEEDED_RESPONSE).intValue());
    Assert.assertEquals(5,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_STATUS_PREFIX + TxnStore.WORKING_RESPONSE).intValue());
    Assert.assertEquals(0,
        Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_STATUS_PREFIX + TxnStore.CLEANING_RESPONSE).intValue());
  }

  @Test
  public void testAgeMetricsNotSet() {
    Metrics.initialize(conf);
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
    Metrics.initialize(conf);
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
    Metrics.initialize(conf);
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
    return element;
  }

  @Override
  boolean useHive130DeltaDirName() {
    return false;
  }
}
