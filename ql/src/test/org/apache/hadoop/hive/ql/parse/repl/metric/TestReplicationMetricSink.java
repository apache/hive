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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.GetReplicationMetricsRequest;
import org.apache.hadoop.hive.metastore.api.ReplicationMetricList;
import org.apache.hadoop.hive.metastore.api.ReplicationMetrics;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.repl.ReplStatsTracker;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.repl.dump.metric.BootstrapDumpMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.dump.metric.IncrementalDumpMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.load.FailoverMetaData;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Stage;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Status;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Progress;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Metric;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Metadata;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.ReplicationMetric;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.ProgressMapper;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.StageMapper;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * Unit Test class for In Memory Replication Metric Collection.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestReplicationMetricSink {

  //Deserializer to decode and decompress the input string.
  MessageDeserializer deserializer;
  HiveConf conf;

  @Mock
  private FailoverMetaData fmd;

  @Before
  public void setup() throws Exception {
    conf = new HiveConf();
    conf.set(Constants.SCHEDULED_QUERY_SCHEDULENAME, "repl");
    conf.set(Constants.SCHEDULED_QUERY_EXECUTIONID, "1");
    MetricSink metricSinkSpy = Mockito.spy(MetricSink.getInstance());
    Mockito.doReturn(1L).when(metricSinkSpy).getFrequencyInSecs();
    metricSinkSpy.init(conf);
    deserializer = MessageFactory.getDefaultInstanceForReplMetrics(conf).getDeserializer();
  }

  private String deSerialize(String msg) {
    return deserializer.deSerializeGenericString(msg);
  }

  @org.junit.Ignore("HIVE-26262")
  @Test
  public void testSuccessBootstrapDumpMetrics() throws Exception {
    ReplicationMetricCollector bootstrapDumpMetricCollector = new BootstrapDumpMetricCollector(
      "testAcidTablesReplLoadBootstrapIncr_1592205875387",
        "hdfs://localhost:65158/tmp/org_apache_hadoop_hive_ql_parse_TestReplicationScenarios_245261428230295"
          + "/hrepl0/dGVzdGFjaWR0YWJsZXNyZXBsbG9hZGJvb3RzdHJhcGluY3JfMTU5MjIwNTg3NTM4Nw==/0/hive", conf, 0L);
    Map<String, Long> metricMap = new HashMap<>();
    metricMap.put(ReplUtils.MetricName.TABLES.name(), (long) 10);
    metricMap.put(ReplUtils.MetricName.FUNCTIONS.name(), (long) 1);
    bootstrapDumpMetricCollector.reportStageStart("dump", metricMap);
    bootstrapDumpMetricCollector.reportStageProgress("dump", ReplUtils.MetricName.TABLES.name(), 1);
    bootstrapDumpMetricCollector.reportStageProgress("dump", ReplUtils.MetricName.TABLES.name(), 2);
    bootstrapDumpMetricCollector.reportStageProgress("dump", ReplUtils.MetricName.FUNCTIONS.name(), 1);
    bootstrapDumpMetricCollector
        .reportStageEnd("dump", Status.SUCCESS, 10, new SnapshotUtils.ReplSnapshotCount(),
            new ReplStatsTracker(0));
    bootstrapDumpMetricCollector.reportEnd(Status.SUCCESS);

    Metadata expectedMetadata = new Metadata("testAcidTablesReplLoadBootstrapIncr_1592205875387",
      Metadata.ReplicationType.BOOTSTRAP, "dummyDir");
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
    Thread.sleep(1000 * 20);
    GetReplicationMetricsRequest metricsRequest = new GetReplicationMetricsRequest();
    metricsRequest.setPolicy("repl");
    ReplicationMetricList actualReplicationMetrics = Hive.get(conf).getMSC().getReplicationMetrics(metricsRequest);
    ReplicationMetrics actualThriftMetric = actualReplicationMetrics.getReplicationMetricList().get(0);
    ObjectMapper mapper = new ObjectMapper();
    ReplicationMetric actualMetric = new ReplicationMetric(actualThriftMetric.getScheduledExecutionId(),
        actualThriftMetric.getPolicy(), actualThriftMetric.getDumpExecutionId(),
        mapper.readValue(actualThriftMetric.getMetadata(), Metadata.class));
    actualMetric.setMessageFormat(actualThriftMetric.getMessageFormat());
    ProgressMapper progressMapper = mapper.readValue(deSerialize(actualThriftMetric.getProgress()), ProgressMapper.class);
    Progress progress = new Progress();
    progress.setStatus(progressMapper.getStatus());
    for (StageMapper stageMapper : progressMapper.getStages()) {
      Stage stage = new Stage();
      stage.setName(stageMapper.getName());
      stage.setStatus(stageMapper.getStatus());
      stage.setStartTime(stageMapper.getStartTime());
      stage.setEndTime(stageMapper.getEndTime());
      for (Metric metric : stageMapper.getMetrics()) {
        stage.addMetric(metric);
      }
      progress.addStage(stage);
    }
    actualMetric.setProgress(progress);
    checkSuccess(actualMetric, expectedMetric, "dump",
        Arrays.asList(ReplUtils.MetricName.TABLES.name(), ReplUtils.MetricName.FUNCTIONS.name()));

    //Incremental
    conf.set(Constants.SCHEDULED_QUERY_EXECUTIONID, "2");
    ReplicationMetricCollector incrementDumpMetricCollector = new IncrementalDumpMetricCollector(
      "testAcidTablesReplLoadBootstrapIncr_1592205875387",
      "hdfs://localhost:65158/tmp/org_apache_hadoop_hive_ql_parse_TestReplicationScenarios_245261428230295"
        + "/hrepl0/dGVzdGFjaWR0YWJsZXNyZXBsbG9hZGJvb3RzdHJhcGluY3JfMTU5MjIwNTg3NTM4Nw==/0/hive", conf, 0L);
    metricMap = new HashMap<>();
    metricMap.put(ReplUtils.MetricName.EVENTS.name(), (long) 10);
    incrementDumpMetricCollector.reportStageStart("dump", metricMap);
    incrementDumpMetricCollector.reportStageProgress("dump", ReplUtils.MetricName.EVENTS.name(), 10);
    incrementDumpMetricCollector.reportStageEnd("dump", Status.SUCCESS, 10, new SnapshotUtils.ReplSnapshotCount(),
        new ReplStatsTracker(0));
    incrementDumpMetricCollector.reportEnd(Status.SUCCESS);

    expectedMetadata = new Metadata("testAcidTablesReplLoadBootstrapIncr_1592205875387",
      Metadata.ReplicationType.INCREMENTAL, "dummyDir");
    expectedMetadata.setLastReplId(10);
    expectedProgress = new Progress();
    expectedProgress.setStatus(Status.SUCCESS);
    dumpStage = new Stage("dump", Status.SUCCESS, 0);
    dumpStage.setEndTime(0);
    Metric expectedEventsMetric = new Metric(ReplUtils.MetricName.EVENTS.name(), 10);
    expectedEventsMetric.setCurrentCount(10);
    dumpStage.addMetric(expectedEventsMetric);
    expectedProgress.addStage(dumpStage);
    expectedMetric = new ReplicationMetric(2, "repl", 0,
      expectedMetadata);
    expectedMetric.setProgress(expectedProgress);
    Thread.sleep(1000 * 20);
    metricsRequest = new GetReplicationMetricsRequest();
    metricsRequest.setPolicy("repl");
    actualReplicationMetrics = Hive.get(conf).getMSC().getReplicationMetrics(metricsRequest);
    Assert.assertEquals(2, actualReplicationMetrics.getReplicationMetricListSize());
    actualThriftMetric = actualReplicationMetrics.getReplicationMetricList().get(0);
    mapper = new ObjectMapper();
    actualMetric = new ReplicationMetric(actualThriftMetric.getScheduledExecutionId(),
      actualThriftMetric.getPolicy(), actualThriftMetric.getDumpExecutionId(),
      mapper.readValue(actualThriftMetric.getMetadata(), Metadata.class));
    actualMetric.setMessageFormat(actualThriftMetric.getMessageFormat());
    progressMapper = mapper.readValue(deSerialize(actualThriftMetric.getProgress()), ProgressMapper.class);
    progress = new Progress();
    progress.setStatus(progressMapper.getStatus());
    for (StageMapper stageMapper : progressMapper.getStages()) {
      Stage stage = new Stage();
      stage.setName(stageMapper.getName());
      stage.setStatus(stageMapper.getStatus());
      stage.setStartTime(stageMapper.getStartTime());
      stage.setEndTime(stageMapper.getEndTime());
      for (Metric metric : stageMapper.getMetrics()) {
        stage.addMetric(metric);
      }
      progress.addStage(stage);
    }
    actualMetric.setProgress(progress);
    checkSuccessIncremental(actualMetric, expectedMetric, "dump",
      Arrays.asList(ReplUtils.MetricName.EVENTS.name()));

    //Failover Metrics Sink
    Mockito.when(fmd.getFailoverEventId()).thenReturn(100L);
    Mockito.when(fmd.getFilePath()).thenReturn("hdfs://localhost:65158/tmp/org_apache_hadoop_hive_ql_parse_TestReplicationScenarios_245261428230295"
            + "/hrepl0/dGVzdGFjaWR0YWJsZXNyZXBsbG9hZGJvb3RzdHJhcGluY3JfMTU5MjIwNTg3NTM4Nw==/0/hive/");
    conf.set(Constants.SCHEDULED_QUERY_EXECUTIONID, "3");
    String stagingDir = "hdfs://localhost:65158/tmp/org_apache_hadoop_hive_ql_parse_TestReplicationScenarios_245261428230295"
            + "/hrepl0/dGVzdGFjaWR0YWJsZXNyZXBsbG9hZGJvb3RzdHJhcGluY3JfMTU5MjIwNTg3NTM4Nw==/0/hive/";
    ReplicationMetricCollector failoverDumpMetricCollector = new IncrementalDumpMetricCollector(
            "testAcidTablesReplLoadBootstrapIncr_1592205875387", stagingDir, conf, 0L);
    metricMap = new HashMap<String, Long>(){{put(ReplUtils.MetricName.EVENTS.name(), (long) 10);}};

    failoverDumpMetricCollector.reportFailoverStart("dump", metricMap, fmd, MetaStoreUtils.FailoverEndpoint.SOURCE.toString(), ReplConst.FailoverType.PLANNED.toString());
    failoverDumpMetricCollector.reportStageProgress("dump", ReplUtils.MetricName.EVENTS.name(), 10);
    failoverDumpMetricCollector.reportStageEnd("dump", Status.SUCCESS, 10, new SnapshotUtils.ReplSnapshotCount(),
            new ReplStatsTracker(0));
    failoverDumpMetricCollector.reportEnd(Status.FAILOVER_READY);

    expectedMetadata = new Metadata("testAcidTablesReplLoadBootstrapIncr_1592205875387",
            Metadata.ReplicationType.INCREMENTAL, "dummyDir");
    expectedMetadata.setLastReplId(10);
    expectedMetadata.setFailoverEventId(100);
    expectedMetadata.setFailoverMetadataLoc(stagingDir + FailoverMetaData.FAILOVER_METADATA);
    expectedMetadata.setFailoverEndPoint(MetaStoreUtils.FailoverEndpoint.SOURCE.toString());
    expectedMetadata.setFailoverType(ReplConst.FailoverType.PLANNED.toString());
    expectedProgress = new Progress();
    expectedProgress.setStatus(Status.FAILOVER_READY);
    dumpStage = new Stage("dump", Status.SUCCESS, 0);
    dumpStage.setEndTime(0);
    expectedEventsMetric = new Metric(ReplUtils.MetricName.EVENTS.name(), 10);
    expectedEventsMetric.setCurrentCount(10);
    dumpStage.addMetric(expectedEventsMetric);
    expectedProgress.addStage(dumpStage);
    expectedMetric = new ReplicationMetric(3, "repl", 0,
            expectedMetadata);
    expectedMetric.setProgress(expectedProgress);
    Thread.sleep(1000 * 20);
    metricsRequest = new GetReplicationMetricsRequest();
    metricsRequest.setPolicy("repl");
    actualReplicationMetrics = Hive.get(conf).getMSC().getReplicationMetrics(metricsRequest);
    Assert.assertEquals(3, actualReplicationMetrics.getReplicationMetricListSize());
    actualThriftMetric = actualReplicationMetrics.getReplicationMetricList().get(0);
    mapper = new ObjectMapper();
    actualMetric = new ReplicationMetric(actualThriftMetric.getScheduledExecutionId(),
            actualThriftMetric.getPolicy(), actualThriftMetric.getDumpExecutionId(),
            mapper.readValue(actualThriftMetric.getMetadata(), Metadata.class));
    actualMetric.setMessageFormat(actualThriftMetric.getMessageFormat());
    progressMapper = mapper.readValue(deSerialize(actualThriftMetric.getProgress()), ProgressMapper.class);
    progress = new Progress();
    progress.setStatus(progressMapper.getStatus());
    for (StageMapper stageMapper : progressMapper.getStages()) {
      Stage stage = new Stage();
      stage.setName(stageMapper.getName());
      stage.setStatus(stageMapper.getStatus());
      stage.setStartTime(stageMapper.getStartTime());
      stage.setEndTime(stageMapper.getEndTime());
      for (Metric metric : stageMapper.getMetrics()) {
        stage.addMetric(metric);
      }
      progress.addStage(stage);
    }
    actualMetric.setProgress(progress);
    checkSuccessIncremental(actualMetric, expectedMetric, "dump",
            Arrays.asList(ReplUtils.MetricName.EVENTS.name()));
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

  private void checkSuccessIncremental(ReplicationMetric actual, ReplicationMetric expected, String stageName,
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

  @org.junit.Ignore("HIVE-26262")
  @Test
  public void testReplStatsInMetrics() throws HiveException, InterruptedException, TException {
    int origRMProgress = ReplStatsTracker.RM_PROGRESS_LENGTH;
    ReplStatsTracker.RM_PROGRESS_LENGTH = 10;
    ReplicationMetricCollector incrementDumpMetricCollector =
        new IncrementalDumpMetricCollector("testAcidTablesReplLoadBootstrapIncr_1592205875387",
            "hdfs://localhost:65158/tmp/org_apache_hadoop_hive_ql_parse_TestReplicationScenarios_245261428230295"
                + "/hrepl0/dGVzdGFjaWR0YWJsZXNyZXBsbG9hZGJvb3RzdHJhcGluY3JfMTU5MjIwNTg3NTM4Nw==/0/hive", conf, 0L);
    Map<String, Long> metricMap = new HashMap<>();
    ReplStatsTracker repl = Mockito.mock(ReplStatsTracker.class);

    Mockito.when(repl.toString()).thenReturn(RandomStringUtils.randomAlphabetic(1000));
    metricMap.put(ReplUtils.MetricName.EVENTS.name(), (long) 10);
    incrementDumpMetricCollector.reportStageStart("dump", metricMap);
    incrementDumpMetricCollector.reportStageProgress("dump", ReplUtils.MetricName.EVENTS.name(), 10);
    incrementDumpMetricCollector
        .reportStageEnd("dump", Status.SUCCESS, 10, new SnapshotUtils.ReplSnapshotCount(), repl);
    Thread.sleep(1000 * 20);
    GetReplicationMetricsRequest metricsRequest = new GetReplicationMetricsRequest();
    metricsRequest.setPolicy("repl");
    ReplicationMetricList actualReplicationMetrics = Hive.get(conf).getMSC().getReplicationMetrics(metricsRequest);
    String progress = deSerialize(actualReplicationMetrics.getReplicationMetricList().get(0).getProgress());
    assertTrue(progress, progress.contains("ERROR: RM_PROGRESS LIMIT EXCEEDED."));
    ReplStatsTracker.RM_PROGRESS_LENGTH = origRMProgress;

    //Testing K_MAX
    repl = new ReplStatsTracker(15);
    Assert.assertEquals(ReplStatsTracker.TOP_K_MAX, repl.getK());
  }
}
