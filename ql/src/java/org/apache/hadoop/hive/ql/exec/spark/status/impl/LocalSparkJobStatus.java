/**
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
package org.apache.hadoop.hive.ql.exec.spark.status.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.spark.Statistic.SparkStatistics;
import org.apache.hadoop.hive.ql.exec.spark.Statistic.SparkStatisticsBuilder;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobStatus;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkStageProgress;
import org.apache.hive.spark.client.MetricsCollection;
import org.apache.hive.spark.client.metrics.Metrics;
import org.apache.hive.spark.counter.SparkCounters;
import org.apache.spark.JobExecutionStatus;
import org.apache.spark.SparkJobInfo;
import org.apache.spark.SparkStageInfo;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.executor.TaskMetrics;

public class LocalSparkJobStatus implements SparkJobStatus {

  private final JavaSparkContext sparkContext;
  private static final Logger LOG = LoggerFactory.getLogger(LocalSparkJobStatus.class.getName());
  private int jobId;
  // After SPARK-2321, we only use JobMetricsListener to get job metrics
  // TODO: remove it when the new API provides equivalent functionality
  private JobMetricsListener jobMetricsListener;
  private SparkCounters sparkCounters;
  private JavaFutureAction<Void> future;
  private Set<Integer> cachedRDDIds;
  private Throwable error;

  public LocalSparkJobStatus(JavaSparkContext sparkContext, int jobId,
      JobMetricsListener jobMetricsListener, SparkCounters sparkCounters,
      Set<Integer> cachedRDDIds, JavaFutureAction<Void> future) {
    this.sparkContext = sparkContext;
    this.jobId = jobId;
    this.jobMetricsListener = jobMetricsListener;
    this.sparkCounters = sparkCounters;
    this.cachedRDDIds = cachedRDDIds;
    this.future = future;
    this.error = null;
  }

  @Override
  public String getAppID() {
    return sparkContext.sc().applicationId();
  }

  @Override
  public int getJobId() {
    return jobId;
  }

  @Override
  public JobExecutionStatus getState() {
    SparkJobInfo sparkJobInfo = getJobInfo();
    // For spark job with empty source data, it's not submitted actually, so we would never
    // receive JobStart/JobEnd event in JobStateListener, use JavaFutureAction to get current
    // job state.
    if (sparkJobInfo == null && future.isDone()) {
      try {
        future.get();
      } catch (Exception e) {
        LOG.error("Failed to run job " + jobId, e);
        return JobExecutionStatus.FAILED;
      }
      return JobExecutionStatus.SUCCEEDED;
    }
    return sparkJobInfo == null ? null : sparkJobInfo.status();
  }

  @Override
  public int[] getStageIds() {
    SparkJobInfo sparkJobInfo = getJobInfo();
    return sparkJobInfo == null ? new int[0] : sparkJobInfo.stageIds();
  }

  @Override
  public Map<String, SparkStageProgress> getSparkStageProgress() {
    Map<String, SparkStageProgress> stageProgresses = new HashMap<String, SparkStageProgress>();
    for (int stageId : getStageIds()) {
      SparkStageInfo sparkStageInfo = getStageInfo(stageId);
      if (sparkStageInfo != null) {
        int runningTaskCount = sparkStageInfo.numActiveTasks();
        int completedTaskCount = sparkStageInfo.numCompletedTasks();
        int failedTaskCount = sparkStageInfo.numFailedTasks();
        int totalTaskCount = sparkStageInfo.numTasks();
        SparkStageProgress sparkStageProgress = new SparkStageProgress(
            totalTaskCount, completedTaskCount, runningTaskCount, failedTaskCount);
        stageProgresses.put(String.valueOf(sparkStageInfo.stageId()) + "_"
          + sparkStageInfo.currentAttemptId(), sparkStageProgress);
      }
    }
    return stageProgresses;
  }

  @Override
  public SparkCounters getCounter() {
    return sparkCounters;
  }

  @Override
  public SparkStatistics getSparkStatistics() {
    SparkStatisticsBuilder sparkStatisticsBuilder = new SparkStatisticsBuilder();
    // add Hive operator level statistics.
    sparkStatisticsBuilder.add(sparkCounters);
    // add spark job metrics.
    String jobIdentifier = "Spark Job[" + jobId + "] Metrics";
    Map<Integer, List<TaskMetrics>> jobMetric = jobMetricsListener.getJobMetric(jobId);
    if (jobMetric == null) {
      return null;
    }

    MetricsCollection metricsCollection = new MetricsCollection();
    Set<Integer> stageIds = jobMetric.keySet();
    for (int stageId : stageIds) {
      List<TaskMetrics> taskMetrics = jobMetric.get(stageId);
      for (TaskMetrics taskMetric : taskMetrics) {
        Metrics metrics = new Metrics(taskMetric);
        metricsCollection.addMetrics(jobId, stageId, 0, metrics);
      }
    }
    Map<String, Long> flatJobMetric = SparkMetricsUtils.collectMetrics(metricsCollection
        .getAllMetrics());
    for (Map.Entry<String, Long> entry : flatJobMetric.entrySet()) {
      sparkStatisticsBuilder.add(jobIdentifier, entry.getKey(), Long.toString(entry.getValue()));
    }

    return  sparkStatisticsBuilder.build();
  }

  @Override
  public void cleanup() {
    jobMetricsListener.cleanup(jobId);
    if (cachedRDDIds != null) {
      for (Integer cachedRDDId: cachedRDDIds) {
        sparkContext.sc().unpersistRDD(cachedRDDId, false);
      }
    }
  }

  @Override
  public Throwable getError() {
    if (error != null) {
      return error;
    }
    if (future.isDone()) {
      try {
        future.get();
      } catch (Throwable e) {
        return e;
      }
    }
    return null;
  }

  @Override
  public void setError(Throwable e) {
    this.error = e;
  }

  private SparkJobInfo getJobInfo() {
    return sparkContext.statusTracker().getJobInfo(jobId);
  }

  private SparkStageInfo getStageInfo(int stageId) {
    return sparkContext.statusTracker().getStageInfo(stageId);
  }
}
