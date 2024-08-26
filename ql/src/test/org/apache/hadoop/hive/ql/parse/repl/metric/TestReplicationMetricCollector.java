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

package org.apache.hadoop.hive.ql.parse.repl.metric;

import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.messaging.MessageSerializer;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.repl.ReplStatsTracker;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.dump.metric.BootstrapDumpMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.dump.metric.IncrementalDumpMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.dump.metric.OptimizedBootstrapDumpMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.dump.metric.PreOptimizedBootstrapDumpMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.load.FailoverMetaData;
import org.apache.hadoop.hive.ql.parse.repl.load.metric.BootstrapLoadMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.load.metric.IncrementalLoadMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Status;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.ReplicationMetric;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Metadata;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Progress;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Stage;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Metric;
import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit Test class for In Memory Replication Metric Collection.
 */

@RunWith(MockitoJUnitRunner.class)
public class TestReplicationMetricCollector {

  HiveConf conf;

  private FailoverMetaData fmd;

  private MetricSink metricSinkInstance;

  MockedStatic<MetricSink> metricSinkMockedStatic;

  @Before
  public void setup() throws Exception {
    fmd = Mockito.mock(FailoverMetaData.class);
    metricSinkInstance = Mockito.mock(MetricSink.class);

    conf = new HiveConf();
    conf.set(Constants.SCHEDULED_QUERY_SCHEDULENAME, "repl");
    conf.set(Constants.SCHEDULED_QUERY_EXECUTIONID, "1");
    MetricCollector.getInstance().init(conf);
    Mockito.when(fmd.getFailoverEventId()).thenReturn(10L);
    Mockito.when(fmd.getFilePath()).thenReturn("dummyDir");
    disableBackgroundThreads();
  }

  private void disableBackgroundThreads() {
    metricSinkMockedStatic = Mockito.mockStatic(MetricSink.class);
    metricSinkMockedStatic.when(MetricSink::getInstance).thenReturn(metricSinkInstance);
  }

  @After
  public void finalize() {
    MetricCollector.getInstance().deinit();
    metricSinkMockedStatic.close();
  }

  @Test
  public void testFailureCacheHardLimit() throws Exception {
    MetricCollector.getInstance().deinit();
    conf = new HiveConf();
    MetricCollector collector = MetricCollector.getInstance();
    MetricCollector metricCollectorSpy = Mockito.spy(collector);
    Mockito.doReturn(1L).when(metricCollectorSpy).getMaxSize(Mockito.any());
    metricCollectorSpy.init(conf);
    metricCollectorSpy.addMetric(new ReplicationMetric(1, "repl",
        0, null));
    try {
      metricCollectorSpy.addMetric(new ReplicationMetric(2, "repl",
          0, null));
      Assert.fail();
    } catch (SemanticException e) {
      Assert.assertEquals("Metrics are not getting collected. ", e.getMessage());
    }
  }

  @Test
  public void testFailureNoScheduledId() throws Exception {
    MetricCollector.getInstance().deinit();
    conf = new HiveConf();
    MetricCollector.getInstance().init(conf);
    ReplicationMetricCollector bootstrapDumpMetricCollector = new BootstrapDumpMetricCollector("db",
        "dummyDir", conf, 0L);
    Map<String, Long> metricMap = new HashMap<>();
    metricMap.put(ReplUtils.MetricName.TABLES.name(), (long) 10);
    metricMap.put(ReplUtils.MetricName.FUNCTIONS.name(), (long) 1);
    bootstrapDumpMetricCollector.reportStageStart("dump", metricMap);
    bootstrapDumpMetricCollector.reportStageEnd("dump", Status.SUCCESS);
    Assert.assertEquals(0, MetricCollector.getInstance().getMetrics().size());
  }

  @Test
  public void testFailureNoPolicyId() throws Exception {
    MetricCollector.getInstance().deinit();
    conf = new HiveConf();
    MetricCollector.getInstance().init(conf);
    ReplicationMetricCollector bootstrapDumpMetricCollector = new BootstrapDumpMetricCollector("db",
        "dummyDir", conf, 0L);
    Map<String, Long> metricMap = new HashMap<>();
    metricMap.put(ReplUtils.MetricName.TABLES.name(), (long) 10);
    metricMap.put(ReplUtils.MetricName.FUNCTIONS.name(), (long) 1);
    bootstrapDumpMetricCollector.reportStageStart("dump", metricMap);
    bootstrapDumpMetricCollector.reportStageEnd("dump", Status.SUCCESS);
    Assert.assertEquals(0, MetricCollector.getInstance().getMetrics().size());
  }

  @Test
  public void testSuccessBootstrapDumpMetrics() throws Exception {
    ReplicationMetricCollector bootstrapDumpMetricCollector = new BootstrapDumpMetricCollector("db",
        "dummyDir", conf, 0L);
    Map<String, Long> metricMap = new HashMap<>();
    metricMap.put(ReplUtils.MetricName.TABLES.name(), (long) 10);
    metricMap.put(ReplUtils.MetricName.FUNCTIONS.name(), (long) 1);
    bootstrapDumpMetricCollector.reportStageStart("dump", metricMap);
    bootstrapDumpMetricCollector.reportStageProgress("dump", ReplUtils.MetricName.TABLES.name(), 1);
    List<ReplicationMetric> actualMetrics = MetricCollector.getInstance().getMetrics();
    Assert.assertEquals(1, actualMetrics.size());

    bootstrapDumpMetricCollector.reportStageProgress("dump", ReplUtils.MetricName.TABLES.name(), 2);
    bootstrapDumpMetricCollector.reportStageProgress("dump", ReplUtils.MetricName.FUNCTIONS.name(), 1);
    actualMetrics = MetricCollector.getInstance().getMetrics();
    Assert.assertEquals(1, actualMetrics.size());

    bootstrapDumpMetricCollector.reportStageEnd("dump", Status.SUCCESS, 10, new SnapshotUtils.ReplSnapshotCount(),
        new ReplStatsTracker(0));
    bootstrapDumpMetricCollector.reportEnd(Status.SUCCESS);
    actualMetrics = MetricCollector.getInstance().getMetrics();
    Assert.assertEquals(1, actualMetrics.size());

    Metadata expectedMetadata = new Metadata("db", Metadata.ReplicationType.BOOTSTRAP, "dummyDir");
    expectedMetadata.setLastReplId(10);
    Progress expectedProgress = new Progress();
    expectedProgress.setStatus(Status.SUCCESS);
    Stage dumpStage = new Stage("dump", Status.SUCCESS, 0);
    dumpStage.setEndTime(0);
    Metric expectedTableMetric = new Metric(ReplUtils.MetricName.TABLES.name(), 10);
    expectedTableMetric.setCurrentCount(3);
    Metric expectedFuncMetric = new Metric(ReplUtils.MetricName.FUNCTIONS.name(), 1);
    expectedFuncMetric.setCurrentCount(1);
    dumpStage.addMetric(expectedTableMetric);
    dumpStage.addMetric(expectedFuncMetric);
    expectedProgress.addStage(dumpStage);
    ReplicationMetric expectedMetric = new ReplicationMetric(1, "repl", 0, expectedMetadata);
    expectedMetric.setProgress(expectedProgress);
    checkSuccess(actualMetrics.get(0), expectedMetric, "dump",
        Arrays.asList(ReplUtils.MetricName.TABLES.name(), ReplUtils.MetricName.FUNCTIONS.name()));
  }

  @Test
  public void testSuccessIncrDumpMetrics() throws Exception {
    ReplicationMetricCollector incrDumpMetricCollector = new IncrementalDumpMetricCollector("db",
        "dummyDir", conf, 0L);
    Map<String, Long> metricMap = new HashMap<>();
    metricMap.put(ReplUtils.MetricName.TABLES.name(), (long) 10);
    metricMap.put(ReplUtils.MetricName.FUNCTIONS.name(), (long) 1);
    incrDumpMetricCollector.reportStageStart("dump", metricMap);
    incrDumpMetricCollector.reportStageProgress("dump", ReplUtils.MetricName.TABLES.name(), 1);
    List<ReplicationMetric> actualMetrics = MetricCollector.getInstance().getMetrics();
    Assert.assertEquals(1, actualMetrics.size());

    incrDumpMetricCollector.reportStageProgress("dump", ReplUtils.MetricName.TABLES.name(), 2);
    incrDumpMetricCollector.reportStageProgress("dump", ReplUtils.MetricName.FUNCTIONS.name(), 1);
    actualMetrics = MetricCollector.getInstance().getMetrics();
    Assert.assertEquals(1, actualMetrics.size());

    incrDumpMetricCollector.reportStageEnd("dump", Status.SUCCESS, 10, new SnapshotUtils.ReplSnapshotCount(),
        new ReplStatsTracker(0));
    incrDumpMetricCollector.reportEnd(Status.SUCCESS);
    actualMetrics = MetricCollector.getInstance().getMetrics();
    Assert.assertEquals(1, actualMetrics.size());

    Metadata expectedMetadata = new Metadata("db", Metadata.ReplicationType.INCREMENTAL, "dummyDir");
    expectedMetadata.setLastReplId(10);
    Progress expectedProgress = new Progress();
    expectedProgress.setStatus(Status.SUCCESS);
    Stage dumpStage = new Stage("dump", Status.SUCCESS, 0);
    dumpStage.setEndTime(0);
    Metric expectedTableMetric = new Metric(ReplUtils.MetricName.TABLES.name(), 10);
    expectedTableMetric.setCurrentCount(3);
    Metric expectedFuncMetric = new Metric(ReplUtils.MetricName.FUNCTIONS.name(), 1);
    expectedFuncMetric.setCurrentCount(1);
    dumpStage.addMetric(expectedTableMetric);
    dumpStage.addMetric(expectedFuncMetric);
    expectedProgress.addStage(dumpStage);
    ReplicationMetric expectedMetric = new ReplicationMetric(1, "repl", 0,
        expectedMetadata);
    expectedMetric.setProgress(expectedProgress);
    checkSuccess(actualMetrics.get(0), expectedMetric, "dump",
        Arrays.asList(ReplUtils.MetricName.TABLES.name(), ReplUtils.MetricName.FUNCTIONS.name()));
  }

  @Test
  public void testSuccessPreOptimizedBootstrapDumpMetrics() throws Exception {
    ReplicationMetricCollector preOptimizedBootstrapDumpMetricCollector = new PreOptimizedBootstrapDumpMetricCollector("db",
            "dummyDir", conf, (long) -1, MetaStoreUtils.FailoverEndpoint.SOURCE.toString(), ReplConst.FailoverType.UNPLANNED.toString());
    Map<String, Long> metricMap = new HashMap<>();
    metricMap.put(ReplUtils.MetricName.TABLES.name(), (long) 0);
    metricMap.put(ReplUtils.MetricName.FUNCTIONS.name(), (long) 0);
    preOptimizedBootstrapDumpMetricCollector.reportStageStart("dump", metricMap);
    preOptimizedBootstrapDumpMetricCollector.reportStageProgress("dump", ReplUtils.MetricName.TABLES.name(), 0);
    List<ReplicationMetric> actualMetrics = MetricCollector.getInstance().getMetrics();
    Assert.assertEquals(1, actualMetrics.size());

    preOptimizedBootstrapDumpMetricCollector.reportStageEnd("dump", Status.SUCCESS, -1, new SnapshotUtils.ReplSnapshotCount(),
            new ReplStatsTracker(0));
    preOptimizedBootstrapDumpMetricCollector.reportEnd(Status.SUCCESS);
    actualMetrics = MetricCollector.getInstance().getMetrics();
    Assert.assertEquals(1, actualMetrics.size());

    Metadata expectedMetadata = new Metadata("db", Metadata.ReplicationType.PRE_OPTIMIZED_BOOTSTRAP, "dummyDir");
    expectedMetadata.setLastReplId(-1);
    expectedMetadata.setFailoverEndPoint(MetaStoreUtils.FailoverEndpoint.SOURCE.toString());
    expectedMetadata.setFailoverType(ReplConst.FailoverType.UNPLANNED.toString());
    Progress expectedProgress = new Progress();
    expectedProgress.setStatus(Status.SUCCESS);
    Stage dumpStage = new Stage("dump", Status.SUCCESS, 0);
    dumpStage.setEndTime(0);
    Metric expectedTableMetric = new Metric(ReplUtils.MetricName.TABLES.name(), 0);
    expectedTableMetric.setCurrentCount(0);
    Metric expectedFuncMetric = new Metric(ReplUtils.MetricName.FUNCTIONS.name(), 0);
    expectedFuncMetric.setCurrentCount(0);
    dumpStage.addMetric(expectedTableMetric);
    dumpStage.addMetric(expectedFuncMetric);
    expectedProgress.addStage(dumpStage);
    ReplicationMetric expectedMetric = new ReplicationMetric(1, "repl", -1, expectedMetadata);
    expectedMetric.setProgress(expectedProgress);
    checkSuccess(actualMetrics.get(0), expectedMetric, "dump",
            Arrays.asList(ReplUtils.MetricName.TABLES.name(), ReplUtils.MetricName.FUNCTIONS.name()));
  }

  @Test
  public void testSuccessOptimizedBootstrapDumpMetrics() throws Exception {
    ReplicationMetricCollector optimizedBootstrapDumpMetricCollector = new OptimizedBootstrapDumpMetricCollector("db",
            "dummyDir", conf, 0L, MetaStoreUtils.FailoverEndpoint.SOURCE.toString(), ReplConst.FailoverType.UNPLANNED.toString());
    Map<String, Long> metricMap = new HashMap<>();
    metricMap.put(ReplUtils.MetricName.TABLES.name(), (long) 10);
    metricMap.put(ReplUtils.MetricName.FUNCTIONS.name(), (long) 1);
    optimizedBootstrapDumpMetricCollector.reportStageStart("dump", metricMap);
    optimizedBootstrapDumpMetricCollector.reportStageProgress("dump", ReplUtils.MetricName.TABLES.name(), 1);
    List<ReplicationMetric> actualMetrics = MetricCollector.getInstance().getMetrics();
    Assert.assertEquals(1, actualMetrics.size());

    optimizedBootstrapDumpMetricCollector.reportStageProgress("dump", ReplUtils.MetricName.TABLES.name(), 2);
    optimizedBootstrapDumpMetricCollector.reportStageProgress("dump", ReplUtils.MetricName.FUNCTIONS.name(), 1);
    actualMetrics = MetricCollector.getInstance().getMetrics();
    Assert.assertEquals(1, actualMetrics.size());

    optimizedBootstrapDumpMetricCollector.reportStageEnd("dump", Status.SUCCESS, 10, new SnapshotUtils.ReplSnapshotCount(),
            new ReplStatsTracker(0));
    optimizedBootstrapDumpMetricCollector.reportEnd(Status.SUCCESS);
    actualMetrics = MetricCollector.getInstance().getMetrics();
    Assert.assertEquals(1, actualMetrics.size());

    Metadata expectedMetadata = new Metadata("db", Metadata.ReplicationType.OPTIMIZED_BOOTSTRAP, "dummyDir");
    expectedMetadata.setLastReplId(10);
    expectedMetadata.setFailoverEndPoint(MetaStoreUtils.FailoverEndpoint.SOURCE.toString());
    expectedMetadata.setFailoverType(ReplConst.FailoverType.UNPLANNED.toString());
    Progress expectedProgress = new Progress();
    expectedProgress.setStatus(Status.SUCCESS);
    Stage dumpStage = new Stage("dump", Status.SUCCESS, 0);
    dumpStage.setEndTime(0);
    Metric expectedTableMetric = new Metric(ReplUtils.MetricName.TABLES.name(), 10);
    expectedTableMetric.setCurrentCount(3);
    Metric expectedFuncMetric = new Metric(ReplUtils.MetricName.FUNCTIONS.name(), 1);
    expectedFuncMetric.setCurrentCount(1);
    dumpStage.addMetric(expectedTableMetric);
    dumpStage.addMetric(expectedFuncMetric);
    expectedProgress.addStage(dumpStage);
    ReplicationMetric expectedMetric = new ReplicationMetric(1, "repl", 0,
            expectedMetadata);
    expectedMetric.setProgress(expectedProgress);
    checkSuccess(actualMetrics.get(0), expectedMetric, "dump",
            Arrays.asList(ReplUtils.MetricName.TABLES.name(), ReplUtils.MetricName.FUNCTIONS.name()));
  }

  @Test
  public void testFailoverReadyDumpMetrics() throws Exception {
    ReplicationMetricCollector incrDumpMetricCollector = new IncrementalDumpMetricCollector("db",
            "dummyDir", conf, 0L);
    Map<String, Long> metricMap = new HashMap<>();
    metricMap.put(ReplUtils.MetricName.EVENTS.name(), (long) 10);
    incrDumpMetricCollector.reportFailoverStart("dump", metricMap, fmd, MetaStoreUtils.FailoverEndpoint.SOURCE.toString(), ReplConst.FailoverType.PLANNED.toString());
    incrDumpMetricCollector.reportStageProgress("dump", ReplUtils.MetricName.EVENTS.name(), 2);
    List<ReplicationMetric> actualMetrics = MetricCollector.getInstance().getMetrics();
    Assert.assertEquals(1, actualMetrics.size());

    incrDumpMetricCollector.reportStageEnd("dump", Status.SUCCESS, 10, new SnapshotUtils.ReplSnapshotCount(),
            new ReplStatsTracker(0));
    incrDumpMetricCollector.reportEnd(Status.FAILOVER_READY);
    actualMetrics = MetricCollector.getInstance().getMetrics();
    Assert.assertEquals(1, actualMetrics.size());

    Metadata expectedMetadata = new Metadata("db", Metadata.ReplicationType.INCREMENTAL, "dummyDir");
    expectedMetadata.setLastReplId(10);
    expectedMetadata.setFailoverEventId(10);
    expectedMetadata.setFailoverMetadataLoc("dummyDir");
    expectedMetadata.setFailoverEndPoint(MetaStoreUtils.FailoverEndpoint.SOURCE.toString());
    expectedMetadata.setFailoverType(ReplConst.FailoverType.PLANNED.toString());
    Progress expectedProgress = new Progress();
    expectedProgress.setStatus(Status.FAILOVER_READY);
    Stage dumpStage = new Stage("dump", Status.SUCCESS, 0);
    dumpStage.setEndTime(0);
    Metric expectedEventMetric = new Metric(ReplUtils.MetricName.EVENTS.name(), 10);
    expectedEventMetric.setCurrentCount(2);
    dumpStage.addMetric(expectedEventMetric);
    expectedProgress.addStage(dumpStage);
    ReplicationMetric expectedMetric = new ReplicationMetric(1, "repl", 0,
            expectedMetadata);
    expectedMetric.setProgress(expectedProgress);
    checkSuccess(actualMetrics.get(0), expectedMetric, "dump",
            Arrays.asList(ReplUtils.MetricName.EVENTS.name()));
  }

  @Test
  public void testSuccessBootstrapLoadMetrics() throws Exception {
    ReplicationMetricCollector bootstrapLoadMetricCollector = new BootstrapLoadMetricCollector("db",
        "dummyDir", 1, conf);
    Map<String, Long> metricMap = new HashMap<>();
    metricMap.put(ReplUtils.MetricName.TABLES.name(), (long) 10);
    metricMap.put(ReplUtils.MetricName.FUNCTIONS.name(), (long) 1);
    bootstrapLoadMetricCollector.reportStageStart("dump", metricMap);
    bootstrapLoadMetricCollector.reportStageProgress("dump", ReplUtils.MetricName.TABLES.name(), 1);
    List<ReplicationMetric> actualMetrics = MetricCollector.getInstance().getMetrics();
    Assert.assertEquals(1, actualMetrics.size());

    bootstrapLoadMetricCollector.reportStageProgress("dump", ReplUtils.MetricName.TABLES.name(), 2);
    bootstrapLoadMetricCollector.reportStageProgress("dump", ReplUtils.MetricName.FUNCTIONS.name(), 1);
    actualMetrics = MetricCollector.getInstance().getMetrics();
    Assert.assertEquals(1, actualMetrics.size());

    bootstrapLoadMetricCollector
        .reportStageEnd("dump", Status.SUCCESS, 10, new SnapshotUtils.ReplSnapshotCount(),
            new ReplStatsTracker(0));
    bootstrapLoadMetricCollector.reportEnd(Status.SUCCESS);
    actualMetrics = MetricCollector.getInstance().getMetrics();
    Assert.assertEquals(1, actualMetrics.size());

    Metadata expectedMetadata = new Metadata("db", Metadata.ReplicationType.BOOTSTRAP, "dummyDir");
    expectedMetadata.setLastReplId(10);
    Progress expectedProgress = new Progress();
    expectedProgress.setStatus(Status.SUCCESS);
    Stage dumpStage = new Stage("dump", Status.SUCCESS, 0);
    dumpStage.setEndTime(0);
    Metric expectedTableMetric = new Metric(ReplUtils.MetricName.TABLES.name(), 10);
    expectedTableMetric.setCurrentCount(3);
    Metric expectedFuncMetric = new Metric(ReplUtils.MetricName.FUNCTIONS.name(), 1);
    expectedFuncMetric.setCurrentCount(1);
    dumpStage.addMetric(expectedTableMetric);
    dumpStage.addMetric(expectedFuncMetric);
    expectedProgress.addStage(dumpStage);
    ReplicationMetric expectedMetric = new ReplicationMetric(1, "repl", 1,
        expectedMetadata);
    expectedMetric.setProgress(expectedProgress);
    checkSuccess(actualMetrics.get(0), expectedMetric, "dump",
        Arrays.asList(ReplUtils.MetricName.TABLES.name(), ReplUtils.MetricName.FUNCTIONS.name()));
  }

  @Test
  public void testSuccessIncrLoadMetrics() throws Exception {
    ReplicationMetricCollector incrLoadMetricCollector = new IncrementalLoadMetricCollector("db",
        "dummyDir", 1, conf);
    Map<String, Long> metricMap = new HashMap<>();
    metricMap.put(ReplUtils.MetricName.TABLES.name(), (long) 10);
    metricMap.put(ReplUtils.MetricName.FUNCTIONS.name(), (long) 1);
    incrLoadMetricCollector.reportStageStart("dump", metricMap);
    incrLoadMetricCollector.reportStageProgress("dump", ReplUtils.MetricName.TABLES.name(), 1);
    List<ReplicationMetric> actualMetrics = MetricCollector.getInstance().getMetrics();
    Assert.assertEquals(1, actualMetrics.size());

    incrLoadMetricCollector.reportStageProgress("dump", ReplUtils.MetricName.TABLES.name(), 2);
    incrLoadMetricCollector.reportStageProgress("dump", ReplUtils.MetricName.FUNCTIONS.name(), 1);
    actualMetrics = MetricCollector.getInstance().getMetrics();
    Assert.assertEquals(1, actualMetrics.size());

    incrLoadMetricCollector.reportStageEnd("dump", Status.SUCCESS, 10, new SnapshotUtils.ReplSnapshotCount(),
        new ReplStatsTracker(0));
    incrLoadMetricCollector.reportEnd(Status.SUCCESS);
    actualMetrics = MetricCollector.getInstance().getMetrics();
    Assert.assertEquals(1, actualMetrics.size());

    Metadata expectedMetadata = new Metadata("db", Metadata.ReplicationType.INCREMENTAL, "dummyDir");
    expectedMetadata.setLastReplId(10);
    Progress expectedProgress = new Progress();
    expectedProgress.setStatus(Status.SUCCESS);
    Stage dumpStage = new Stage("dump", Status.SUCCESS, 0);
    dumpStage.setEndTime(0);
    Metric expectedTableMetric = new Metric(ReplUtils.MetricName.TABLES.name(), 10);
    expectedTableMetric.setCurrentCount(3);
    Metric expectedFuncMetric = new Metric(ReplUtils.MetricName.FUNCTIONS.name(), 1);
    expectedFuncMetric.setCurrentCount(1);
    dumpStage.addMetric(expectedTableMetric);
    dumpStage.addMetric(expectedFuncMetric);
    expectedProgress.addStage(dumpStage);
    ReplicationMetric expectedMetric = new ReplicationMetric(1, "repl", 1,
        expectedMetadata);
    expectedMetric.setProgress(expectedProgress);
    checkSuccess(actualMetrics.get(0), expectedMetric, "dump",
        Arrays.asList(ReplUtils.MetricName.TABLES.name(), ReplUtils.MetricName.FUNCTIONS.name()));
  }

  private void checkSuccess(ReplicationMetric actual, ReplicationMetric expected, String stageName,
                            List<String> metricNames) {
    Assert.assertEquals(expected.getDumpExecutionId(), actual.getDumpExecutionId());
    Assert.assertEquals(expected.getPolicy(), actual.getPolicy());
    Assert.assertEquals(expected.getScheduledExecutionId(), actual.getScheduledExecutionId());
    Assert.assertEquals(expected.getMetadata().getReplicationType(), actual.getMetadata().getReplicationType());
    Assert.assertEquals(expected.getMetadata().getDbName(), actual.getMetadata().getDbName());
    Assert.assertEquals(expected.getMetadata().getStagingDir(), actual.getMetadata().getStagingDir());
    Assert.assertEquals(expected.getMetadata().getLastReplId(), actual.getMetadata().getLastReplId());
    Assert.assertEquals(expected.getMetadata().getFailoverEndPoint(), actual.getMetadata().getFailoverEndPoint());
    Assert.assertEquals(expected.getMetadata().getFailoverType(), actual.getMetadata().getFailoverType());
    Assert.assertEquals(expected.getProgress().getStatus(), actual.getProgress().getStatus());
    Assert.assertEquals(expected.getProgress().getStageByName(stageName).getStatus(),
        actual.getProgress().getStageByName(stageName).getStatus());
    for (String metricName : metricNames) {
      Assert.assertEquals(expected.getProgress().getStageByName(stageName).getMetricByName(metricName).getTotalCount(),
          actual.getProgress().getStageByName(stageName).getMetricByName(metricName).getTotalCount());
      Assert.assertEquals(expected.getProgress().getStageByName(stageName).getMetricByName(metricName)
          .getCurrentCount(), actual.getProgress()
          .getStageByName(stageName).getMetricByName(metricName).getCurrentCount());
    }
  }

  @Test
  public void testSuccessStageFailure() throws Exception {
    ReplicationMetricCollector bootstrapDumpMetricCollector = new BootstrapDumpMetricCollector("db",
      "dummyDir", conf, 0L);
    Map<String, Long> metricMap = new HashMap<>();
    metricMap.put(ReplUtils.MetricName.TABLES.name(), (long) 10);
    metricMap.put(ReplUtils.MetricName.FUNCTIONS.name(), (long) 1);
    bootstrapDumpMetricCollector.reportStageStart("dump", metricMap);
    bootstrapDumpMetricCollector.reportStageEnd("dump", Status.FAILED);
    List<ReplicationMetric> metricList = MetricCollector.getInstance().getMetrics();
    Assert.assertEquals(1, metricList.size());
    ReplicationMetric actualMetric = metricList.get(0);
    Assert.assertEquals(Status.FAILED, actualMetric.getProgress().getStatus());
  }

  @Test
  public void testSuccessStageFailedAdmin() throws Exception {
    ReplicationMetricCollector bootstrapDumpMetricCollector = new BootstrapDumpMetricCollector("db",
      "dummyDir", conf, 0L);
    Map<String, Long> metricMap = new HashMap<>();
    metricMap.put(ReplUtils.MetricName.TABLES.name(), (long) 10);
    metricMap.put(ReplUtils.MetricName.FUNCTIONS.name(), (long) 1);
    bootstrapDumpMetricCollector.reportStageStart("dump", metricMap);
    bootstrapDumpMetricCollector.reportStageEnd("dump", Status.FAILED_ADMIN, "errorlogpath");
    List<ReplicationMetric> metricList = MetricCollector.getInstance().getMetrics();
    Assert.assertEquals(1, metricList.size());
    ReplicationMetric actualMetric = metricList.get(0);
    Assert.assertEquals(Status.FAILED_ADMIN, actualMetric.getProgress().getStatus());
    Assert.assertEquals("errorlogpath", actualMetric.getProgress()
      .getStageByName("dump").getErrorLogPath());
  }

  @Test
  public void testReplStatsTracker() throws Exception {
    ReplStatsTracker repl = new ReplStatsTracker(5);
    repl.addEntry("EVENT_ADD_PARTITION", "1", 2345);
    repl.addEntry("EVENT_ADD_PARTITION", "2", 23451);
    repl.addEntry("EVENT_ADD_PARTITION", "3", 23451);
    repl.addEntry("EVENT_ADD_DATABASE", "4", 234544);
    repl.addEntry("EVENT_ALTER_PARTITION", "5", 2145);
    repl.addEntry("EVENT_CREATE_TABLE", "6", 2245);
    repl.addEntry("EVENT_ADD_PARTITION", "7", 1245);
    repl.addEntry("EVENT_ADD_PARTITION", "8", 23425);
    repl.addEntry("EVENT_ALTER_PARTITION", "9", 21345);
    repl.addEntry("EVENT_CREATE_TABLE", "10", 1345);
    repl.addEntry("EVENT_ADD_DATABASE", "11", 345);
    repl.addEntry("EVENT_ADD_DATABASE", "12", 12345);
    repl.addEntry("EVENT_ADD_DATABASE", "13", 3345);
    repl.addEntry("EVENT_ALTER_PARTITION", "14", 2645);
    repl.addEntry("EVENT_ALTER_PARTITION", "15", 2555);
    repl.addEntry("EVENT_CREATE_TABLE", "16", 23765);
    repl.addEntry("EVENT_ADD_PARTITION", "17", 23435);
    repl.addEntry("EVENT_DROP_PARTITION", "18", 2205);
    repl.addEntry("EVENT_CREATE_TABLE", "19", 2195);
    repl.addEntry("EVENT_DROP_PARTITION", "20", 2225);
    repl.addEntry("EVENT_DROP_PARTITION", "21", 2225);
    repl.addEntry("EVENT_DROP_PARTITION", "22", 23485);
    repl.addEntry("EVENT_CREATE_TABLE", "23", 2385);
    repl.addEntry("EVENT_DROP_PARTITION", "24", 234250);
    repl.addEntry("EVENT_DROP_PARTITION", "25", 15);
    repl.addEntry("EVENT_CREATE_TABLE", "26", 23425);
    repl.addEntry("EVENT_CREATE_TABLE", "27", 23445);

    // Check the total number of entries in the TopKEvents is equal to the number of events fed in.
    assertEquals(5, repl.getTopKEvents().size());

    // Check the timing & number of events for ADD_PARTITION
    assertArrayEquals(repl.getTopKEvents().get("EVENT_ADD_PARTITION").valueList().toString(),
        new Long[] { 23451L, 23451L, 23435L, 23425L, 2345L },
        repl.getTopKEvents().get("EVENT_ADD_PARTITION").valueList().toArray());

    assertEquals(6, repl.getDescMap().get("EVENT_ADD_PARTITION").getN());

    // Check the timing & number of events for DROP_PARTITION
    assertArrayEquals(repl.getTopKEvents().get("EVENT_DROP_PARTITION").valueList().toString(),
        new Long[] { 234250L, 23485L, 2225L, 2225L, 2205L },
        repl.getTopKEvents().get("EVENT_DROP_PARTITION").valueList().toArray());

    assertEquals(6, repl.getDescMap().get("EVENT_DROP_PARTITION").getN());

    // Check the timing & number of events for CREATE_TABLE
    assertArrayEquals(repl.getTopKEvents().get("EVENT_CREATE_TABLE").valueList().toString(),
        new Long[] { 23765L, 23445L, 23425L, 2385L, 2245L },
        repl.getTopKEvents().get("EVENT_CREATE_TABLE").valueList().toArray());

    assertEquals(7, repl.getDescMap().get("EVENT_CREATE_TABLE").getN());

    // Check the timing & number of events for ALTER_PARTITION
    assertArrayEquals(repl.getTopKEvents().get("EVENT_ALTER_PARTITION").valueList().toString(),
        new Long[] { 21345L, 2645L, 2555L, 2145L },
        repl.getTopKEvents().get("EVENT_ALTER_PARTITION").valueList().toArray());

    assertEquals(4, repl.getDescMap().get("EVENT_ALTER_PARTITION").getN());

    // Check the timing & number of events for ADD_DATABASE
    assertArrayEquals(repl.getTopKEvents().get("EVENT_ADD_DATABASE").valueList().toString(),
        new Long[] { 234544L, 12345L, 3345L, 345L },
        repl.getTopKEvents().get("EVENT_ADD_DATABASE").valueList().toArray());

    assertEquals(4, repl.getDescMap().get("EVENT_ADD_DATABASE").getN());
  }

  @Test
  public void testReplStatsTrackerLimit() {
    MessageSerializer serializer = MessageFactory.getDefaultInstanceForReplMetrics(conf).getSerializer();
    ReplStatsTracker repl = new ReplStatsTracker(10);
    // Check for k=10
    generateStatsString(10, repl);
    String replStatsTracker = repl.toString();
    String gzipSerialized = serializer.serialize(replStatsTracker);
    assertTrue("ReplStat string is " + gzipSerialized.length(), gzipSerialized.length() < ReplStatsTracker.RM_PROGRESS_LENGTH);
    // Check for k=5
    repl = new ReplStatsTracker(5);
    generateStatsString(5, repl);
    replStatsTracker = repl.toString();
    gzipSerialized = serializer.serialize(replStatsTracker);
    assertTrue("ReplStat string is " + gzipSerialized.length(), gzipSerialized.length() < ReplStatsTracker.RM_PROGRESS_LENGTH);
    // Check for k=2 & check NaN values doesn't get messed up due to formatter
    repl = new ReplStatsTracker(2);
    generateStatsString(2, repl);
    assertTrue(repl.toString().contains("NaN"));
  }

  private void generateStatsString(int k, ReplStatsTracker repl) {
    DumpType[] types = DumpType.values();
    for (DumpType type : types) {
      for (int i = 0; i < k; i++) {
        int eventId = 1000000 + i * type.ordinal();
        repl.addEntry(type.toString(), Integer.toString(eventId), 10000 + i + ( i * 1234));
      }
    }
  }
}
