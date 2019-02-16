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

package org.apache.hadoop.hive.ql.exec.spark.status.impl;

import org.apache.hadoop.hive.ql.exec.spark.SparkUtilities;
import org.apache.hadoop.hive.ql.exec.spark.Statistic.SparkStatisticsNames;

import org.apache.hadoop.hive.ql.exec.spark.status.SparkStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.spark.Statistic.SparkStatistics;
import org.apache.hadoop.hive.ql.exec.spark.Statistic.SparkStatisticsBuilder;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobStatus;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkStageProgress;
import org.apache.hive.spark.client.MetricsCollection;
import org.apache.hive.spark.client.Job;
import org.apache.hive.spark.client.JobContext;
import org.apache.hive.spark.client.JobHandle;
import org.apache.hive.spark.client.SparkClient;
import org.apache.hive.spark.counter.SparkCounters;
import org.apache.spark.JobExecutionStatus;
import org.apache.spark.SparkJobInfo;
import org.apache.spark.SparkStageInfo;
import org.apache.spark.api.java.JavaFutureAction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Used with remove spark client.
 */
public class RemoteSparkJobStatus implements SparkJobStatus {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteSparkJobStatus.class.getName());
  private final SparkClient sparkClient;
  private final JobHandle<Serializable> jobHandle;
  private Throwable error;
  private final transient long sparkClientTimeoutInSeconds;

  public RemoteSparkJobStatus(SparkClient sparkClient, JobHandle<Serializable> jobHandle, long timeoutInSeconds) {
    this.sparkClient = sparkClient;
    this.jobHandle = jobHandle;
    this.error = null;
    this.sparkClientTimeoutInSeconds = timeoutInSeconds;
  }

  @Override
  public String getAppID() {
    Future<String> getAppID = sparkClient.run(new GetAppIDJob());
    try {
      return getAppID.get(sparkClientTimeoutInSeconds, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.warn("Failed to get APP ID.", e);
      if (Thread.interrupted()) {
        error = e;
      }
      return null;
    }
  }

  @Override
  public int getJobId() {
    return jobHandle.getSparkJobIds().size() == 1 ? jobHandle.getSparkJobIds().get(0) : -1;
  }

  @Override
  public JobExecutionStatus getState() throws HiveException {
    SparkJobInfo sparkJobInfo = getSparkJobInfo();
    return sparkJobInfo != null ? sparkJobInfo.status() : null;
  }

  @Override
  public int[] getStageIds() throws HiveException {
    SparkJobInfo sparkJobInfo = getSparkJobInfo();
    return sparkJobInfo != null ? sparkJobInfo.stageIds() : new int[0];
  }

  @Override
  public Map<SparkStage, SparkStageProgress> getSparkStageProgress() throws HiveException {
    List<SparkStageInfo> sparkStagesInfo = getSparkStagesInfo();
    Map<SparkStage, SparkStageProgress> stageProgresses = new HashMap<SparkStage, SparkStageProgress>();
    if (sparkStagesInfo != null) {
      for (SparkStageInfo sparkStageInfo : sparkStagesInfo) {
        if (sparkStageInfo != null && sparkStageInfo.name() != null) {
          int runningTaskCount = sparkStageInfo.numActiveTasks();
          int completedTaskCount = sparkStageInfo.numCompletedTasks();
          int failedTaskCount = sparkStageInfo.numFailedTasks();
          int totalTaskCount = sparkStageInfo.numTasks();
          SparkStageProgress sparkStageProgress =
              new SparkStageProgress(totalTaskCount, completedTaskCount, runningTaskCount, failedTaskCount);
          SparkStage stage = new SparkStage(sparkStageInfo.stageId(), sparkStageInfo.currentAttemptId());
          stageProgresses.put(stage, sparkStageProgress);
        }
      }
    }
    return stageProgresses;
  }

  @Override
  public SparkCounters getCounter() {
    return jobHandle.getSparkCounters();
  }

  @Override
  public SparkStatistics getSparkStatistics() {
    MetricsCollection metricsCollection = jobHandle.getMetrics();
    if (metricsCollection == null || getCounter() == null) {
      return null;
    }

    SparkStatisticsBuilder sparkStatisticsBuilder = new SparkStatisticsBuilder();

    // add Hive operator level statistics. - e.g. RECORDS_IN, RECORDS_OUT
    sparkStatisticsBuilder.add(getCounter());

    // add spark job metrics. - e.g. metrics collected by Spark itself (JvmGCTime,
    // ExecutorRunTime, etc.)
    Map<String, Long> flatJobMetric = SparkMetricsUtils.collectMetrics(
        metricsCollection.getAllMetrics());
    for (Map.Entry<String, Long> entry : flatJobMetric.entrySet()) {
      sparkStatisticsBuilder.add(SparkStatisticsNames.SPARK_GROUP_NAME, entry.getKey(),
              Long.toString(entry.getValue()));
    }

    return sparkStatisticsBuilder.build();
  }

  @Override
  public String getWebUIURL() {
    Future<String> getWebUIURL = sparkClient.run(new GetWebUIURLJob());
    try {
      return getWebUIURL.get(sparkClientTimeoutInSeconds, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.warn("Failed to get web UI URL.", e);
      if (Thread.interrupted()) {
        error = e;
      }
      return "UNKNOWN";
    }
  }

  @Override
  public void cleanup() {

  }

  @Override
  public Throwable getMonitorError() {
    return error;
  }

  @Override
  public void setMonitorError(Throwable e) {
    this.error = e;
  }

  @Override
  public Throwable getSparkJobException() {
    return jobHandle.getError();
  }

  /**
   * Indicates whether the remote context is active. SparkJobMonitor can use this to decide whether
   * to stop monitoring.
   */
  public boolean isRemoteActive() {
    return sparkClient.isActive();
  }

  private SparkJobInfo getSparkJobInfo() throws HiveException {
    Integer sparkJobId = jobHandle.getSparkJobIds().size() == 1
      ? jobHandle.getSparkJobIds().get(0) : null;
    if (sparkJobId == null) {
      return null;
    }
    Future<SparkJobInfo> getJobInfo = sparkClient.run(
        new GetJobInfoJob(jobHandle.getClientJobId(), sparkJobId));
    try {
      return getJobInfo.get(sparkClientTimeoutInSeconds, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      throw new HiveException(e, ErrorMsg.SPARK_GET_JOB_INFO_TIMEOUT,
          Long.toString(sparkClientTimeoutInSeconds));
    } catch (InterruptedException e) {
      throw new HiveException(e, ErrorMsg.SPARK_GET_JOB_INFO_INTERRUPTED);
    } catch (ExecutionException e) {
      throw new HiveException(e, ErrorMsg.SPARK_GET_JOB_INFO_EXECUTIONERROR,
          Throwables.getRootCause(e).getMessage());
    }
  }

  private List<SparkStageInfo> getSparkStagesInfo()throws HiveException {

    Integer sparkJobId = jobHandle.getSparkJobIds().size() == 1
        ? jobHandle.getSparkJobIds().get(0) : null;
    if (sparkJobId == null) {
      return null;
    }
    Future<ArrayList<SparkStageInfo>> getStagesInfo = sparkClient.run(
        new GetSparkStagesInfoJob(jobHandle.getClientJobId(), sparkJobId));
    try {
      return getStagesInfo.get(sparkClientTimeoutInSeconds, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      throw new HiveException(e, ErrorMsg.SPARK_GET_STAGES_INFO_TIMEOUT,
          Long.toString(sparkClientTimeoutInSeconds));
    } catch (InterruptedException e) {
      throw new HiveException(e, ErrorMsg.SPARK_GET_STAGES_INFO_INTERRUPTED);
    } catch (ExecutionException e) {
      throw new HiveException(e, ErrorMsg.SPARK_GET_STAGES_INFO_EXECUTIONERROR,
          Throwables.getRootCause(e).getMessage());
    }
  }

  public JobHandle.State getRemoteJobState() {
    if (error != null) {
      return JobHandle.State.FAILED;
    }
    return jobHandle.getState();
  }

  private static class GetSparkStagesInfoJob implements Job<ArrayList<SparkStageInfo>> {
    private final String clientJobId;
    private final int sparkJobId;

    private GetSparkStagesInfoJob() {
      // For serialization.
      this(null, -1);
    }

    GetSparkStagesInfoJob(String clientJobId, int sparkJobId) {
      this.clientJobId = clientJobId;
      this.sparkJobId = sparkJobId;
    }
    @Override
    public ArrayList<SparkStageInfo> call(JobContext jc) throws Exception {
      SparkJobInfo jobInfo = jc.sc().statusTracker().getJobInfo(sparkJobId);
      if (jobInfo == null) {
        ArrayList<JavaFutureAction<?>> list = new ArrayList<>(jc.getMonitoredJobs().get(clientJobId));
        if (list != null && list.size() == 1) {
          JavaFutureAction<?> futureAction = list.get(0);
          if (futureAction.isDone()) {
            boolean futureSucceed = true;
            try {
              futureAction.get();
            } catch (Exception e) {
              LOG.error("Failed to run job " + sparkJobId, e);
              futureSucceed = false;
            }
            jobInfo = getDefaultJobInfo(sparkJobId,
                futureSucceed ? JobExecutionStatus.SUCCEEDED : JobExecutionStatus.FAILED);
          }
        }
      }
      if (jobInfo == null) {
        jobInfo = getDefaultJobInfo(sparkJobId, JobExecutionStatus.UNKNOWN);
      }
      ArrayList<SparkStageInfo> sparkStageInfos = new ArrayList<>();
      int[] stageIds = jobInfo.stageIds();
      for(Integer stageid : stageIds) {
        SparkStageInfo stageInfo = jc.sc().statusTracker().getStageInfo(stageid);
        sparkStageInfos.add(stageInfo);
      }
      return sparkStageInfos;
    }
  }
  private static class GetJobInfoJob implements Job<SparkJobInfo> {
    private final String clientJobId;
    private final int sparkJobId;

    private GetJobInfoJob() {
      // For serialization.
      this(null, -1);
    }

    GetJobInfoJob(String clientJobId, int sparkJobId) {
      this.clientJobId = clientJobId;
      this.sparkJobId = sparkJobId;
    }

    @Override
    public SparkJobInfo call(JobContext jc) throws Exception {
      SparkJobInfo jobInfo = jc.sc().statusTracker().getJobInfo(sparkJobId);
      if (jobInfo == null) {
        List<JavaFutureAction<?>> list = jc.getMonitoredJobs().get(clientJobId);
        if (list != null && list.size() == 1) {
          JavaFutureAction<?> futureAction = list.get(0);
          if (futureAction.isDone()) {
            boolean futureSucceed = true;
            try {
              futureAction.get();
            } catch (Exception e) {
              LOG.error("Failed to run job " + sparkJobId, e);
              futureSucceed = false;
            }
            jobInfo = getDefaultJobInfo(sparkJobId,
                futureSucceed ? JobExecutionStatus.SUCCEEDED : JobExecutionStatus.FAILED);
          }
        }
      }
      if (jobInfo == null) {
        jobInfo = getDefaultJobInfo(sparkJobId, JobExecutionStatus.UNKNOWN);
      }
      return jobInfo;
    }
  }

  private static SparkJobInfo getDefaultJobInfo(final Integer jobId,
      final JobExecutionStatus status) {
    return new SparkJobInfo() {

      @Override
      public int jobId() {
        return jobId == null ? -1 : jobId;
      }

      @Override
      public int[] stageIds() {
        return new int[0];
      }

      @Override
      public JobExecutionStatus status() {
        return status;
      }
    };
  }

  private static class GetAppIDJob implements Job<String> {

    public GetAppIDJob() {
    }

    @Override
    public String call(JobContext jc) throws Exception {
      return jc.sc().sc().applicationId();
    }
  }

  private static class GetWebUIURLJob implements Job<String> {

    public GetWebUIURLJob() {
    }

    @Override
    public String call(JobContext jc) throws Exception {
      if (jc.sc().sc().uiWebUrl().isDefined()) {
        return SparkUtilities.reverseDNSLookupURL(jc.sc().sc().uiWebUrl().get());
      }
      return "UNDEFINED";
    }
  }
}
