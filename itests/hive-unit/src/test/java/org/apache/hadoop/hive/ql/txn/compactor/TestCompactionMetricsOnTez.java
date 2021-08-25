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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.ql.txn.compactor.metrics.DeltaFilesMetricReporter;
import org.apache.tez.dag.api.TezConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hive.ql.txn.compactor.TestDeltaFilesMetrics.gaugeToMap;
import static org.apache.hadoop.hive.ql.txn.compactor.TestDeltaFilesMetrics.verifyMetricsMatch;
import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactor.executeStatementOnDriver;

public class TestCompactionMetricsOnTez extends CompactorOnTezTest {

  /**
   * Use {@link CompactorOnTezTest#setupWithConf(org.apache.hadoop.hive.conf.HiveConf)} when HiveConf is
   * configured to your liking.
   */
  @Override
  public void setup() {
  }

  @After
  public void tearDown() {
    DeltaFilesMetricReporter.close();
  }

  /**
   * Note: Does not initialize DeltaFilesMetricReporter.
   * @param hiveConf
   * @throws Exception
   */
  @Override
  protected void setupWithConf(HiveConf hiveConf) throws Exception {
    HiveConf.setIntVar(hiveConf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_OBSOLETE_DELTA_NUM_THRESHOLD, 0);
    HiveConf.setIntVar(hiveConf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_DELTA_NUM_THRESHOLD, 0);
    HiveConf.setTimeVar(hiveConf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_REPORTING_INTERVAL, 1, TimeUnit.SECONDS);
    HiveConf.setTimeVar(hiveConf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_DELTA_CHECK_THRESHOLD, 0, TimeUnit.SECONDS);
    HiveConf.setFloatVar(hiveConf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_DELTA_PCT_THRESHOLD, 0.7f);
    super.setupWithConf(hiveConf);
    MetricsFactory.close();
    MetricsFactory.init(hiveConf);
  }

  @Test
  public void testDeltaFilesMetric() throws Exception {
    HiveConf conf = new HiveConf();
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED, true);
    setupWithConf(conf);
    DeltaFilesMetricReporter.init(conf);

    String dbName = "default", tableName = "test_metrics";
    String partitionToday = "ds=today", partitionTomorrow = "ds=tomorrow", partitionYesterday = "ds=yesterday";

    CompactorOnTezTest.TestDataProvider testDataProvider = new CompactorOnTezTest.TestDataProvider();
    testDataProvider.createFullAcidTable(tableName, true, false);
    testDataProvider.insertTestDataPartitioned(tableName);

    executeStatementOnDriver("select avg(b) from " + tableName, driver);
    Thread.sleep(1000);

    verifyMetricsMatch(new HashMap<String, String>() {{
          put(tableName + Path.SEPARATOR + partitionTomorrow, "3");
          put(tableName + Path.SEPARATOR + partitionYesterday, "4");
          put(tableName + Path.SEPARATOR + partitionToday, "5");
        }}, gaugeToMap(MetricsConstants.COMPACTION_NUM_DELTAS));

    Assert.assertEquals(0, gaugeToMap(MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS).size());
    Assert.assertEquals(0, gaugeToMap(MetricsConstants.COMPACTION_NUM_SMALL_DELTAS).size());

    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MAJOR, true,
        partitionToday, partitionTomorrow, partitionYesterday);

    executeStatementOnDriver("select avg(b) from " + tableName, driver);
    Thread.sleep(1000);

    verifyMetricsMatch(new HashMap<String, String>() {{
          put(tableName + Path.SEPARATOR + partitionTomorrow, "3");
          put(tableName + Path.SEPARATOR + partitionYesterday, "4");
          put(tableName + Path.SEPARATOR + partitionToday, "5");
        }}, gaugeToMap(MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS));

    Assert.assertEquals(0, gaugeToMap(MetricsConstants.COMPACTION_NUM_DELTAS).size());

    String insertQry = MessageFormat.format("insert into " + tableName + " partition (ds=''today'') values " +
      "(''{0}'',1),(''{0}'',2),(''{0}'',3),(''{0}'',4),(''{0}'',5),(''{0}'',6),(''{0}'',7), (''{0}'',8),(''{0}'',9)," +
      "(''{0}'',10),(''{0}'',11),(''{0}'',12)", RandomStringUtils.random(4096, false, true));
    for (int i = 0; i < 10; i++) {
      executeStatementOnDriver(insertQry, driver);
    }
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MAJOR, true,
        partitionToday);
    executeStatementOnDriver("insert into " + tableName + " partition (ds='today') values('1',2)", driver);

    executeStatementOnDriver("select avg(b) from " + tableName, driver);
    Thread.sleep(1000);

    verifyMetricsMatch(new HashMap<String, String>() {{
          put(tableName + Path.SEPARATOR + partitionToday, "1");
        }}, gaugeToMap(MetricsConstants.COMPACTION_NUM_SMALL_DELTAS));
  }

  /**
   * Queries shouldn't fail, but metrics should be 0, if tez.counters.max limit is passed.
   * @throws Exception
   */
  @Test
  public void testDeltaFilesMetricTezMaxCounters() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setInt(TezConfiguration.TEZ_COUNTERS_MAX, 50);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED, true);
    setupWithConf(conf);

    DeltaFilesMetricReporter.init(conf);

    String tableName = "test_metrics";
    CompactorOnTezTest.TestDataProvider testDataProvider = new CompactorOnTezTest.TestDataProvider();
    testDataProvider.createFullAcidTable(tableName, true, false);
    // Create 51 partitions
    for (int i = 0; i < 51; i++) {
      executeStatementOnDriver("insert into " + tableName + " values('1', " + i * i + ", '" + i + "')", driver);
    }

    // Touch all partitions
    executeStatementOnDriver("select avg(b) from " + tableName, driver);
    Thread.sleep(1000);

    Assert.assertEquals(0, gaugeToMap(MetricsConstants.COMPACTION_NUM_DELTAS).size());
    Assert.assertEquals(0, gaugeToMap(MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS).size());
    Assert.assertEquals(0, gaugeToMap(MetricsConstants.COMPACTION_NUM_SMALL_DELTAS).size());
  }

  /**
   * Queries should succeed if additional acid metrics are disabled.
   * @throws Exception
   */
  @Test(expected = javax.management.InstanceNotFoundException.class)
  public void testDeltaFilesMetricWithMetricsDisabled() throws Exception {
    HiveConf conf = new HiveConf();
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED, false);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_EXT_ON, true);

    verifyQueryRuns(conf);
  }


  /**
   * Queries should succeed if extended metrics are disabled.
   * @throws Exception
   */
  @Test(expected = javax.management.InstanceNotFoundException.class)
  public void testDeltaFilesMetricWithExtMetricsDisabled() throws Exception {
    HiveConf conf = new HiveConf();
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED, true);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_EXT_ON, false);

    verifyQueryRuns(conf);
  }

  private void verifyQueryRuns(HiveConf conf) throws Exception {
    setupWithConf(conf);
    // DeltaFilesMetricReporter is not instantiated because either metrics or extended metrics are disabled.

    String tableName = "test_metrics";
    TestDataProvider testDataProvider = new TestDataProvider();
    testDataProvider.createFullAcidTable(tableName, true, false);
    testDataProvider.insertTestDataPartitioned(tableName);

    executeStatementOnDriver("select avg(b) from " + tableName, driver);
    Assert.assertEquals(0, gaugeToMap(MetricsConstants.COMPACTION_NUM_DELTAS).size());
  }
}
