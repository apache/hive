package org.apache.hadoop.hive.ql.txn.compactor;

import com.codahale.metrics.Gauge;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.ql.txn.compactor.metrics.DeltaFilesMetricReporter;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

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

    Assert.assertEquals(new TreeMap() {{
      put(tableName + Path.SEPARATOR + partitionTomorrow, 3);
      put(tableName + Path.SEPARATOR + partitionYesterday, 4);
      put(tableName + Path.SEPARATOR + partitionToday, 5);
    }}, gauges.get(MetricsConstants.COMPACTION_NUM_DELTAS).getValue());

    Assert.assertEquals(((Map)gauges.get(MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS).getValue()).size(), 0);

    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MAJOR, true,
        partitionToday, partitionTomorrow, partitionYesterday);

    executeStatementOnDriver("select avg(b) from " + tableName, driver);
    Thread.sleep(1000);

    Assert.assertEquals(new TreeMap() {{
      put(tableName + Path.SEPARATOR + partitionTomorrow, 3);
      put(tableName + Path.SEPARATOR + partitionYesterday, 4);
      put(tableName + Path.SEPARATOR + partitionToday, 5);
    }}, gauges.get(MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS).getValue());

    Assert.assertEquals(((Map)gauges.get(MetricsConstants.COMPACTION_NUM_DELTAS).getValue()).size(), 0);

    DeltaFilesMetricReporter.close();
  }
}
