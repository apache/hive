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
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.hadoop.hive.ql.exec.repl.NoOpReplStatsTracker;
import org.apache.hadoop.hive.ql.exec.repl.ReplStatsTracker;
import org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.ReplicationMetric;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Metadata;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Progress;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Stage;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Status;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Metric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public ReplicationMetricCollector(String dbName, Metadata.ReplicationType replicationType,
                             String stagingDir, long dumpExecutionId, HiveConf conf) {
    checkEnabledForTests(conf);
    String policy = conf.get(Constants.SCHEDULED_QUERY_SCHEDULENAME);
    long executionId = conf.getLong(Constants.SCHEDULED_QUERY_EXECUTIONID, 0L);
    if (!StringUtils.isEmpty(policy) && executionId > 0) {
      isEnabled = true;
      metricCollector = MetricCollector.getInstance().init(conf);
      MetricSink.getInstance().init(conf);
      Metadata metadata = new Metadata(dbName, replicationType, stagingDir);
      replicationMetric = new ReplicationMetric(executionId, policy, dumpExecutionId, metadata);
    }
  }

  public void reportStageStart(String stageName, Map<String, Long> metricMap) throws SemanticException {
    if (isEnabled) {
      LOG.debug("Stage Started {}, {}, {}", stageName, metricMap.size(), metricMap );
      Progress progress = replicationMetric.getProgress();
      progress.setStatus(Status.IN_PROGRESS);
      Stage stage = new Stage(stageName, Status.IN_PROGRESS, System.currentTimeMillis());
      for (Map.Entry<String, Long> metric : metricMap.entrySet()) {
        stage.addMetric(new Metric(metric.getKey(), metric.getValue()));
      }
      progress.addStage(stage);
      replicationMetric.setProgress(progress);
      metricCollector.addMetric(replicationMetric);
    }
  }

  public void reportStageEnd(String stageName, Status status, long lastReplId,
      SnapshotUtils.ReplSnapshotCount replSnapshotCount, ReplStatsTracker replStatsTracker) throws SemanticException {
    if (isEnabled) {
      LOG.debug("Stage ended {}, {}, {}", stageName, status, lastReplId );
      Progress progress = replicationMetric.getProgress();
      Stage stage = progress.getStageByName(stageName);
      if(stage == null){
        stage = new Stage(stageName, status, -1L);
      }
      stage.setStatus(status);
      stage.setEndTime(System.currentTimeMillis());
      stage.setReplSnapshotsCount(replSnapshotCount);
      if (replStatsTracker != null && !(replStatsTracker instanceof NoOpReplStatsTracker)) {
        stage.setReplStats(replStatsTracker.toString());
      }
      progress.addStage(stage);
      replicationMetric.setProgress(progress);
      Metadata metadata = replicationMetric.getMetadata();
      metadata.setLastReplId(lastReplId);
      replicationMetric.setMetadata(metadata);
      metricCollector.addMetric(replicationMetric);
      if (Status.FAILED == status || Status.FAILED_ADMIN == status) {
        reportEnd(status);
      }
    }
  }

  public void reportStageEnd(String stageName, Status status, String errorLogPath) throws SemanticException {
    if (isEnabled) {
      LOG.debug("Stage Ended {}, {}", stageName, status );
      Progress progress = replicationMetric.getProgress();
      Stage stage = progress.getStageByName(stageName);
      if(stage == null){
        stage = new Stage(stageName, status, -1L);
      }
      stage.setStatus(status);
      stage.setEndTime(System.currentTimeMillis());
      stage.setErrorLogPath(errorLogPath);
      progress.addStage(stage);
      replicationMetric.setProgress(progress);
      metricCollector.addMetric(replicationMetric);
      if (Status.FAILED == status || Status.FAILED_ADMIN == status) {
        reportEnd(status);
      }
    }
  }

  public void reportStageEnd(String stageName, Status status) throws SemanticException {
    if (isEnabled) {
      LOG.debug("Stage Ended {}, {}", stageName, status );
      Progress progress = replicationMetric.getProgress();
      Stage stage = progress.getStageByName(stageName);
      if(stage == null){
        stage = new Stage(stageName, status, -1L);
      }
      stage.setStatus(status);
      stage.setEndTime(System.currentTimeMillis());
      progress.addStage(stage);
      replicationMetric.setProgress(progress);
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
      conf.setLong(Constants.SCHEDULED_QUERY_EXECUTIONID, 1L);
    }
  }
}
