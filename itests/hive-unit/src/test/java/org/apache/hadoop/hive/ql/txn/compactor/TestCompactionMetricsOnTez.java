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

import com.codahale.metrics.Gauge;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.ql.txn.compactor.metrics.DeltaFilesMetricReporter;
import org.junit.Assert;
import org.junit.Test;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactionMetrics.equivalent;
import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactionMetrics.gaugeToMap;
import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactor.executeStatementOnDriver;

public class TestCompactionMetricsOnTez extends CompactorOnTezTest {

  @Test
  public void testDeltaFilesMetric() throws Exception {
    MetricsFactory.close();
    HiveConf conf = driver.getConf();

    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED, true);
    MetricsFactory.init(conf);

    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_OBSOLETE_DELTA_NUM_THRESHOLD, 0);
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_DELTA_NUM_THRESHOLD, 0);
    HiveConf.setTimeVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_REPORTING_INTERVAL, 1, TimeUnit.SECONDS);
    HiveConf.setTimeVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_DELTA_CHECK_THRESHOLD, 0, TimeUnit.SECONDS);
    HiveConf.setFloatVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_DELTA_PCT_THRESHOLD, 0.7f);
    DeltaFilesMetricReporter.init(conf);

    String dbName = "default", tableName = "test_metrics";
    String partitionToday = "ds=today", partitionTomorrow = "ds=tomorrow", partitionYesterday = "ds=yesterday";

    CompactorOnTezTest.TestDataProvider testDataProvider = new CompactorOnTezTest.TestDataProvider();
    testDataProvider.createFullAcidTable(tableName, true, false);
    testDataProvider.insertTestDataPartitioned(tableName);

    executeStatementOnDriver("select avg(b) from " + tableName, driver);
    Thread.sleep(1000);
    CodahaleMetrics metrics = (CodahaleMetrics) MetricsFactory.getInstance();
    Map<String, Gauge> gauges = metrics.getMetricRegistry().getGauges();

    Assert.assertTrue(
      equivalent(
        new HashMap<String, String>() {{
          put(tableName + Path.SEPARATOR + partitionTomorrow, "3");
          put(tableName + Path.SEPARATOR + partitionYesterday, "4");
          put(tableName + Path.SEPARATOR + partitionToday, "5");
        }}, gaugeToMap(MetricsConstants.COMPACTION_NUM_DELTAS, gauges)));

    Assert.assertEquals(gaugeToMap(MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS, gauges).size(), 0);
    Assert.assertEquals(gaugeToMap(MetricsConstants.COMPACTION_NUM_SMALL_DELTAS, gauges).size(), 0);

    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MAJOR, true,
        partitionToday, partitionTomorrow, partitionYesterday);

    executeStatementOnDriver("select avg(b) from " + tableName, driver);
    Thread.sleep(1000);

    Assert.assertTrue(
      equivalent(
        new HashMap<String, String>() {{
          put(tableName + Path.SEPARATOR + partitionTomorrow, "3");
          put(tableName + Path.SEPARATOR + partitionYesterday, "4");
          put(tableName + Path.SEPARATOR + partitionToday, "5");
        }}, gaugeToMap(MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS, gauges)));

    Assert.assertEquals(gaugeToMap(MetricsConstants.COMPACTION_NUM_DELTAS, gauges).size(), 0);

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

    Assert.assertTrue(
      equivalent(
        new HashMap<String, String>() {{
          put(tableName + Path.SEPARATOR + partitionToday, "1");
        }}, gaugeToMap(MetricsConstants.COMPACTION_NUM_SMALL_DELTAS, gauges)));

    DeltaFilesMetricReporter.close();
  }
}
