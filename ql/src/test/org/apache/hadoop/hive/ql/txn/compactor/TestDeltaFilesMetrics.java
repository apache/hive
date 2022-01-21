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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.txn.compactor.metrics.DeltaFilesMetricReporter;
import org.apache.tez.common.counters.TezCounters;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hive.ql.txn.compactor.metrics.DeltaFilesMetricReporter.DeltaFilesMetricType.NUM_DELTAS;
import static org.apache.hadoop.hive.ql.txn.compactor.metrics.DeltaFilesMetricReporter.DeltaFilesMetricType.NUM_OBSOLETE_DELTAS;
import static org.apache.hadoop.hive.ql.txn.compactor.metrics.DeltaFilesMetricReporter.DeltaFilesMetricType.NUM_SMALL_DELTAS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestDeltaFilesMetrics extends CompactorTest  {

  private void setUpHiveConf() {
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED, true);
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_MAX_CACHE_SIZE, 2);
    HiveConf.setTimeVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_CACHE_DURATION, 7200, TimeUnit.SECONDS);
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_OBSOLETE_DELTA_NUM_THRESHOLD, 100);
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_DELTA_NUM_THRESHOLD, 100);
    HiveConf.setTimeVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_REPORTING_INTERVAL, 1, TimeUnit.SECONDS);
  }

  private void initAndCollectFirstMetrics() throws Exception {
    MetricsFactory.close();
    MetricsFactory.init(conf);

    DeltaFilesMetricReporter.init(conf);

    TezCounters tezCounters = new TezCounters();
    tezCounters.findCounter(NUM_OBSOLETE_DELTAS + "", "default.acid/p=1").setValue(200);
    tezCounters.findCounter(NUM_OBSOLETE_DELTAS + "", "default.acid/p=2").setValue(100);
    tezCounters.findCounter(NUM_OBSOLETE_DELTAS + "", "default.acid/p=3").setValue(150);
    tezCounters.findCounter(NUM_OBSOLETE_DELTAS + "", "default.acid_v2").setValue(250);

    tezCounters.findCounter(NUM_DELTAS + "", "default.acid/p=1").setValue(150);
    tezCounters.findCounter(NUM_DELTAS + "", "default.acid/p=2").setValue(100);
    tezCounters.findCounter(NUM_DELTAS + "", "default.acid/p=3").setValue(250);
    tezCounters.findCounter(NUM_DELTAS + "", "default.acid_v2").setValue(200);

    tezCounters.findCounter(NUM_SMALL_DELTAS + "", "default.acid/p=1").setValue(250);
    tezCounters.findCounter(NUM_SMALL_DELTAS + "", "default.acid/p=2").setValue(200);
    tezCounters.findCounter(NUM_SMALL_DELTAS + "", "default.acid/p=3").setValue(150);
    tezCounters.findCounter(NUM_SMALL_DELTAS + "", "default.acid_v2").setValue(100);

    DeltaFilesMetricReporter.getInstance().submit(tezCounters, null);
    Thread.sleep(1000);
  }

  @After
  public void tearDown() {
    DeltaFilesMetricReporter.close();
  }

  @Test
  public void testDeltaFilesMetric() throws Exception {
    setUpHiveConf();
    initAndCollectFirstMetrics();

    verifyMetricsMatch(new HashMap<String, String>() {{
      put("default.acid/p=1", "200");
      put("default.acid/p=2", "100");
      put("default.acid/p=3", "150");
      put("default.acid_v2", "250");
    }}, gaugeToMap(MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS));

    verifyMetricsMatch(new HashMap<String, String>() {{
      put("default.acid/p=1", "150");
      put("default.acid/p=2", "100");
      put("default.acid/p=3", "250");
      put("default.acid_v2", "200");
    }}, gaugeToMap(MetricsConstants.COMPACTION_NUM_DELTAS));

    verifyMetricsMatch(new HashMap<String, String>() {{
      put("default.acid/p=1", "250");
      put("default.acid/p=2", "200");
      put("default.acid/p=3", "150");
      put("default.acid_v2", "100");
    }}, gaugeToMap(MetricsConstants.COMPACTION_NUM_SMALL_DELTAS));
  }

  @Test
  public void testDeltaFilesMetricUpdate() throws Exception {
    setUpHiveConf();
    initAndCollectFirstMetrics();

    TezCounters tezCounters = new TezCounters();
    tezCounters.findCounter(NUM_OBSOLETE_DELTAS + "", "default.acid/p=1").setValue(50);
    tezCounters.findCounter(NUM_DELTAS + "", "default.acid/p=1").setValue(50);
    tezCounters.findCounter(NUM_DELTAS + "", "default.acid/p=3").setValue(0);
    tezCounters.findCounter(NUM_SMALL_DELTAS + "", "default.acid/p=1").setValue(50);
    tezCounters.findCounter(NUM_SMALL_DELTAS + "", "default.acid/p=2").setValue(0);
    tezCounters.findCounter(NUM_SMALL_DELTAS + "", "default.acid/p=3").setValue(50);

    // the next pass will be from a query that touches only acid/p=1 and acid/p=3
    ReadEntity p1 = getReadEntity("default@acid@p=1");
    // don't update p2
    ReadEntity p3 = getReadEntity("default@acid@p=3");

    DeltaFilesMetricReporter.getInstance().submit(tezCounters, ImmutableSet.of(p1, p3));
    Thread.sleep(1000);

    verifyMetricsMatch(new HashMap<String, String>() {{
      put("default.acid/p=1", "50"); // updated
      put("default.acid/p=2", "100");
      // p=3 was removed since the query touched it and it didn't have enough deltas to be included in counters
      put("default.acid_v2", "250");
    }}, gaugeToMap(MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS));

    verifyMetricsMatch(new HashMap<String, String>() {{
      put("default.acid/p=1", "50"); // updated
      put("default.acid/p=2", "100");
      // p=3 was removed since the query touched it and it didn't have enough deltas (0) to be included in counters
      put("default.acid_v2", "200");
    }}, gaugeToMap(MetricsConstants.COMPACTION_NUM_DELTAS));

    verifyMetricsMatch(new HashMap<String, String>() {{
      put("default.acid/p=1", "50");  // updated
      put("default.acid/p=2", "200"); // not updated since the query didn't touch p=2
      put("default.acid/p=3", "50");  // updated
      put("default.acid_v2", "100");
    }}, gaugeToMap(MetricsConstants.COMPACTION_NUM_SMALL_DELTAS));
  }

  @Test
  public void testDeltaFilesMetricTimeout() throws Exception {
    setUpHiveConf();
    HiveConf.setTimeVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_CACHE_DURATION, 5, TimeUnit.SECONDS);
    initAndCollectFirstMetrics();
    Thread.sleep(5000);

    TezCounters tezCounters = new TezCounters();
    tezCounters.findCounter(NUM_DELTAS + "", "default.acid/p=2").setValue(150);
    DeltaFilesMetricReporter.getInstance().submit(tezCounters, null);
    Thread.sleep(1000);

    verifyMetricsMatch(new HashMap<String, String>() {{
      put("default.acid/p=2", "150");
    }}, gaugeToMap(MetricsConstants.COMPACTION_NUM_DELTAS));
  }

  @Test
  public void testMergeDeltaFilesStatsNullData() throws Exception {
    setUpHiveConf();
    MetricsFactory.close();
    MetricsFactory.init(conf);
    DeltaFilesMetricReporter.init(conf);

    AcidDirectory dir = new AcidDirectory(new Path("/"), FileSystem.get(conf), null, false);
    long checkThresholdInSec = HiveConf.getTimeVar(conf,
        HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_DELTA_CHECK_THRESHOLD, TimeUnit.SECONDS);
    float deltaPctThreshold = HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_DELTA_PCT_THRESHOLD);
    int deltasThreshold = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_DELTA_NUM_THRESHOLD);
    int obsoleteDeltasThreshold = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_OBSOLETE_DELTA_NUM_THRESHOLD);
    int maxCacheSize = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_MAX_CACHE_SIZE);
    EnumMap<DeltaFilesMetricReporter.DeltaFilesMetricType, Queue<Pair<String, Integer>>> deltaFilesStats =
        new EnumMap<>(DeltaFilesMetricReporter.DeltaFilesMetricType.class);

    //conf.get(JOB_CONF_DELTA_FILES_METRICS_METADATA) will not have a value assigned; this test checks for an NPE
    DeltaFilesMetricReporter.mergeDeltaFilesStats(dir,checkThresholdInSec, deltaPctThreshold, deltasThreshold,
        obsoleteDeltasThreshold, maxCacheSize, deltaFilesStats, conf);
  }

  static void verifyMetricsMatch(Map<String, String> expected, Map<String, String> actual) {
    Assert.assertTrue("Actual metrics " + actual + " don't match expected: " + expected,
        equivalent(expected, actual));
  }

  private static boolean equivalent(Map<String, String> lhs, Map<String, String> rhs) {
    return lhs.size() == rhs.size() && Maps.difference(lhs, rhs).areEqual();
  }

  static Map<String, String> gaugeToMap(String metric) throws Exception {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName oname = new ObjectName(DeltaFilesMetricReporter.OBJECT_NAME_PREFIX + metric);
    MBeanInfo mbeanInfo = mbs.getMBeanInfo(oname);

    Map<String, String> result = new HashMap<>();
    for (MBeanAttributeInfo attr : mbeanInfo.getAttributes()) {
      result.put(attr.getName(), String.valueOf(mbs.getAttribute(oname, attr.getName())));
    }
    return result;
  }

  @NotNull
  private ReadEntity getReadEntity(String s) {
    ReadEntity p3 = mock(ReadEntity.class);
    when(p3.getName()).thenReturn(s);
    return p3;
  }

  @Override
  boolean useHive130DeltaDirName() {
    return false;
  }
}
