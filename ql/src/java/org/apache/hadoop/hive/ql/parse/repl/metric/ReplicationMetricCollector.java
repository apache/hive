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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AtomicDouble;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.hadoop.hive.ql.exec.repl.NoOpReplStatsTracker;
import org.apache.hadoop.hive.ql.exec.repl.ReplLoadWork;
import org.apache.hadoop.hive.ql.exec.repl.ReplStatsTracker;
import org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.load.FailoverMetaData;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.ReplicationMetric;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Metadata;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Progress;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Stage;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Status;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Metric;
import org.apache.hadoop.metrics2.util.MBeans;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.util.Map;

/**
 * Abstract class for Replication Metric Collection.
 */
public abstract class ReplicationMetricCollector {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationMetricCollector.class);
  private ReplicationMetric replicationMetric;
  private MetricCollector metricCollector;
  private boolean isEnabled;
  private static boolean enableForTests;
  private static long scheduledExecutionIdForTests = 0L;
  private HiveConf conf;

  public void setMetricsMBean(ObjectName metricsMBean) {
    this.metricsMBean = metricsMBean;
  }

  private ObjectName metricsMBean;

  private AtomicDouble sizeOfDataReplicatedInKB = new AtomicDouble(0);

  public ReplicationMetricCollector(String dbName, Metadata.ReplicationType replicationType,
                             String stagingDir, long dumpExecutionId, HiveConf conf) {
    this.conf = conf;
    checkEnabledForTests(conf);
    String policy = conf.get(Constants.SCHEDULED_QUERY_SCHEDULENAME);
    long executionId = conf.getLong(Constants.SCHEDULED_QUERY_EXECUTIONID, 0L);
    if (!StringUtils.isEmpty(policy) && executionId > 0) {
      isEnabled = true;
      metricCollector = MetricCollector.getInstance().init(conf);
      MetricSink.getInstance().init(conf);
      Metadata metadata = new Metadata(dbName, replicationType, getStagingDir(stagingDir));
      replicationMetric = new ReplicationMetric(executionId, policy, dumpExecutionId, metadata);
    }
  }

  public ReplicationMetricCollector(String dbName, Metadata.ReplicationType replicationType,
                                    String stagingDir, long dumpExecutionId, HiveConf conf,
                                    String failoverEndpoint, String failoverType) {
    this.conf = conf;
    checkEnabledForTests(conf);
    String policy = conf.get(Constants.SCHEDULED_QUERY_SCHEDULENAME);
    long executionId = conf.getLong(Constants.SCHEDULED_QUERY_EXECUTIONID, 0L);
    if (!StringUtils.isEmpty(policy) && executionId > 0) {
      isEnabled = true;
      metricCollector = MetricCollector.getInstance().init(conf);
      MetricSink.getInstance().init(conf);
      Metadata metadata = new Metadata(dbName, replicationType, getStagingDir(stagingDir));
      metadata.setFailoverEndPoint(failoverEndpoint);
      metadata.setFailoverType(failoverType);
      replicationMetric = new ReplicationMetric(executionId, policy, dumpExecutionId, metadata);
    }
  }

  public void incrementSizeOfDataReplicated(long bytesCount) {
    sizeOfDataReplicatedInKB.addAndGet((double)bytesCount/1024);
  }

  public void reportStageStart(String stageName, Map<String, Long> metricMap) throws SemanticException {
    if (isEnabled) {
      LOG.debug("Stage Started {}, {}, {}", stageName, metricMap.size(), metricMap );
      Progress progress = replicationMetric.getProgress();
      progress.setStatus(Status.IN_PROGRESS);
      Stage stage = new Stage(stageName, Status.IN_PROGRESS, getCurrentTimeInMillis());
      for (Map.Entry<String, Long> metric : metricMap.entrySet()) {
        stage.addMetric(new Metric(metric.getKey(), metric.getValue()));
      }
      progress.addStage(stage);
      replicationMetric.setProgress(progress);
      metricCollector.addMetric(replicationMetric);
    }
  }

  public void reportFailoverStart(String stageName, Map<String, Long> metricMap,
                                  FailoverMetaData failoverMd, String failoverEndpoint,
                                  String failoverType) throws SemanticException {
    if (isEnabled) {
      LOG.info("Failover Stage Started {}, {}, {}", stageName, metricMap.size(), metricMap);
      Progress progress = replicationMetric.getProgress();
      progress.setStatus(Status.FAILOVER_IN_PROGRESS);
      Stage stage = new Stage(stageName, Status.IN_PROGRESS, getCurrentTimeInMillis());
      for (Map.Entry<String, Long> metric : metricMap.entrySet()) {
        stage.addMetric(new Metric(metric.getKey(), metric.getValue()));
      }
      progress.addStage(stage);
      replicationMetric.setProgress(progress);
      Metadata metadata = replicationMetric.getMetadata();
      metadata.setFailoverMetadataLoc(failoverMd.getFilePath());
      metadata.setFailoverEventId(failoverMd.getFailoverEventId());
      metadata.setFailoverEndPoint(failoverEndpoint);
      metadata.setFailoverType(failoverType);
      replicationMetric.setMetadata(metadata);
      metricCollector.addMetric(replicationMetric);
    }
  }

  public void reportStageEnd(String stageName, Status status, long lastReplId,
      SnapshotUtils.ReplSnapshotCount replSnapshotCount, ReplStatsTracker replStatsTracker) throws SemanticException {
    unRegisterMBeanSafe();
    if (isEnabled) {
      LOG.debug("Stage ended {}, {}, {}", stageName, status, lastReplId );
      Progress progress = replicationMetric.getProgress();
      Stage stage = progress.getStageByName(stageName);
      if(stage == null){
        stage = new Stage(stageName, status, -1L);
      }
      stage.setStatus(status);
      stage.setEndTime(getCurrentTimeInMillis());
      stage.setReplSnapshotsCount(replSnapshotCount);
      if (replStatsTracker != null && !(replStatsTracker instanceof NoOpReplStatsTracker)) {
        String replStatString = replStatsTracker.toString();
        LOG.info("Replication Statistics are: {}", replStatString);
        stage.setReplStats(replStatString);
      }
      progress.addStage(stage);
      replicationMetric.setProgress(progress);
      Metadata metadata = replicationMetric.getMetadata();
      metadata.setLastReplId(lastReplId);
      metadata.setReplicatedDBSizeInKB(sizeOfDataReplicatedInKB.get());
      replicationMetric.setMetadata(metadata);
      metricCollector.addMetric(replicationMetric);
      if (Status.FAILED == status || Status.FAILED_ADMIN == status) {
        reportEnd(status);
      }
    }
  }

  public void reportStageEnd(String stageName, Status status, String errorLogPath) throws SemanticException {
    unRegisterMBeanSafe();
    if (isEnabled) {
      LOG.debug("Stage Ended {}, {}", stageName, status );
      Progress progress = replicationMetric.getProgress();
      Stage stage = progress.getStageByName(stageName);
      if(stage == null){
        stage = new Stage(stageName, status, -1L);
      }
      stage.setStatus(status);
      stage.setEndTime(getCurrentTimeInMillis());
      if (errorLogPath != null) {
        stage.setErrorLogPath(errorLogPath);
      }
      progress.addStage(stage);
      replicationMetric.setProgress(progress);
      metricCollector.addMetric(replicationMetric);
      if (Status.FAILED == status || Status.FAILED_ADMIN == status || Status.SKIPPED == status) {
        reportEnd(status);
      }
    }
  }

  public void reportStageEnd(String stageName, Status status) throws SemanticException {
    unRegisterMBeanSafe();
    if (isEnabled) {
      LOG.debug("Stage Ended {}, {}", stageName, status );
      Progress progress = replicationMetric.getProgress();
      Stage stage = progress.getStageByName(stageName);
      if(stage == null){
        stage = new Stage(stageName, status, -1L);
      }
      stage.setStatus(status);
      stage.setEndTime(getCurrentTimeInMillis());
      progress.addStage(stage);
      replicationMetric.setProgress(progress);
      Metadata metadata = replicationMetric.getMetadata();
      metadata.setReplicatedDBSizeInKB(sizeOfDataReplicatedInKB.get());
      metricCollector.addMetric(replicationMetric);
      if (Status.FAILED == status || Status.FAILED_ADMIN == status) {
        reportEnd(status);
      }
    }
  }

  public void reportStageProgress(String stageName, String metricName, long count) throws SemanticException {
    if (isEnabled) {
      LOG.debug("Stage progress {}, {}, {}", stageName, metricName, count );
      Progress progress = replicationMetric.getProgress();
      Stage stage = progress.getStageByName(stageName);
      Metric metric = stage.getMetricByName(metricName);
      metric.setCurrentCount(metric.getCurrentCount() + count);
      if (metric.getCurrentCount() > metric.getTotalCount()) {
        metric.setTotalCount(metric.getCurrentCount());
      }
      stage.addMetric(metric);
      replicationMetric.setProgress(progress);
      metricCollector.addMetric(replicationMetric);
    }
  }

  public void reportEnd(Status status) throws SemanticException {
    if (isEnabled) {
      LOG.info("End {}", status );
      Progress progress = replicationMetric.getProgress();
      progress.setStatus(status);
      replicationMetric.setProgress(progress);
      metricCollector.addMetric(replicationMetric);
    }
  }

  // Utility methods to enable metrics without running scheduler for testing.
  @VisibleForTesting
  public static void isMetricsEnabledForTests(boolean enable) {
    enableForTests = enable;
  }

  private void checkEnabledForTests(HiveConf conf) {
    if (enableForTests) {
      conf.set(Constants.SCHEDULED_QUERY_SCHEDULENAME, "pol");
      conf.setLong(Constants.SCHEDULED_QUERY_EXECUTIONID, ++scheduledExecutionIdForTests);
    }
  }

  private void unRegisterMBeanSafe() {
    if (metricsMBean != null && !ReplLoadWork.disableMbeanUnregistrationForTests) {
      try {
        MBeans.unregister(metricsMBean);
      } catch (Exception e) {
        LOG.warn("Unable to unregister MBean {}", metricsMBean, e);
      }
    }
  }

  private boolean testingModeEnabled() {
    return conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST) || conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST_REPL);
  }

  private long getCurrentTimeInMillis() {
    return testingModeEnabled() ? 0L : System.currentTimeMillis();
  }

  private String getStagingDir(String stagingDir) {
    return testingModeEnabled() ? "dummyDir" : stagingDir;
  }
}
