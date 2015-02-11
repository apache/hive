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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.spark.Statistic.SparkStatistics;
import org.apache.hadoop.hive.ql.exec.spark.Statistic.SparkStatisticsBuilder;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.spark.client.MetricsCollection;
import org.apache.hive.spark.client.metrics.Metrics;
import org.apache.hive.spark.client.metrics.ShuffleReadMetrics;
import org.apache.hive.spark.counter.SparkCounters;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobStatus;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkStageProgress;
import org.apache.hive.spark.client.Job;
import org.apache.hive.spark.client.JobContext;
import org.apache.hive.spark.client.JobHandle;
import org.apache.hive.spark.client.SparkClient;
import org.apache.spark.JobExecutionStatus;
import org.apache.spark.SparkJobInfo;
import org.apache.spark.SparkStageInfo;
import org.apache.spark.api.java.JavaFutureAction;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Used with remove spark client.
 */
public class RemoteSparkJobStatus implements SparkJobStatus {
  private static final Log LOG = LogFactory.getLog(RemoteSparkJobStatus.class.getName());
  private final SparkClient sparkClient;
  private final JobHandle<Serializable> jobHandle;
  private final transient long sparkClientTimeoutInSeconds;

  public RemoteSparkJobStatus(SparkClient sparkClient, JobHandle<Serializable> jobHandle, long timeoutInSeconds) {
    this.sparkClient = sparkClient;
    this.jobHandle = jobHandle;
    this.sparkClientTimeoutInSeconds = timeoutInSeconds;
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
  public Map<String, SparkStageProgress> getSparkStageProgress() throws HiveException {
    Map<String, SparkStageProgress> stageProgresses = new HashMap<String, SparkStageProgress>();
    for (int stageId : getStageIds()) {
      SparkStageInfo sparkStageInfo = getSparkStageInfo(stageId);
      if (sparkStageInfo != null && sparkStageInfo.name() != null) {
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
    return jobHandle.getSparkCounters();
  }

  @Override
  public SparkStatistics getSparkStatistics() {
    MetricsCollection metricsCollection = jobHandle.getMetrics();
    if (metricsCollection == null || getCounter() == null) {
      return null;
    }
    SparkStatisticsBuilder sparkStatisticsBuilder = new SparkStatisticsBuilder();
    // add Hive operator level statistics.
    sparkStatisticsBuilder.add(getCounter());
    // add spark job metrics.
    String jobIdentifier = "Spark Job[" + jobHandle.getClientJobId() + "] Metrics";

    Map<String, Long> flatJobMetric = extractMetrics(metricsCollection);
    for (Map.Entry<String, Long> entry : flatJobMetric.entrySet()) {
      sparkStatisticsBuilder.add(jobIdentifier, entry.getKey(), Long.toString(entry.getValue()));
    }

    return sparkStatisticsBuilder.build();
  }

  @Override
  public void cleanup() {

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
    } catch (Exception e) {
      LOG.warn("Failed to get job info.", e);
      throw new HiveException(e);
    }
  }

  private SparkStageInfo getSparkStageInfo(int stageId) {
    Future<SparkStageInfo> getStageInfo = sparkClient.run(new GetStageInfoJob(stageId));
    try {
      return getStageInfo.get(sparkClientTimeoutInSeconds, TimeUnit.SECONDS);
    } catch (Throwable t) {
      LOG.warn("Error getting stage info", t);
      return null;
    }
  }

  public JobHandle.State getRemoteJobState() {
    return jobHandle.getState();
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

  private static class GetStageInfoJob implements Job<SparkStageInfo> {
    private final int stageId;

    private GetStageInfoJob() {
      // For serialization.
      this(-1);
    }

    GetStageInfoJob(int stageId) {
      this.stageId = stageId;
    }

    @Override
    public SparkStageInfo call(JobContext jc) throws Exception {
      return jc.sc().statusTracker().getStageInfo(stageId);
    }
  }

  private Map<String, Long> extractMetrics(MetricsCollection metricsCollection) {
    Map<String, Long> results = new LinkedHashMap<String, Long>();
    Metrics allMetrics = metricsCollection.getAllMetrics();

    results.put("EexcutorDeserializeTime", allMetrics.executorDeserializeTime);
    results.put("ExecutorRunTime", allMetrics.executorRunTime);
    results.put("ResultSize", allMetrics.resultSize);
    results.put("JvmGCTime", allMetrics.jvmGCTime);
    results.put("ResultSerializationTime", allMetrics.resultSerializationTime);
    results.put("MemoryBytesSpilled", allMetrics.memoryBytesSpilled);
    results.put("DiskBytesSpilled", allMetrics.diskBytesSpilled);
    if (allMetrics.inputMetrics != null) {
      results.put("BytesRead", allMetrics.inputMetrics.bytesRead);
    }
    if (allMetrics.shuffleReadMetrics != null) {
      ShuffleReadMetrics shuffleReadMetrics = allMetrics.shuffleReadMetrics;
      long rbf = shuffleReadMetrics.remoteBlocksFetched;
      long lbf = shuffleReadMetrics.localBlocksFetched;
      results.put("RemoteBlocksFetched", rbf);
      results.put("LocalBlocksFetched", lbf);
      results.put("TotalBlocksFetched", lbf + rbf);
      results.put("FetchWaitTime", shuffleReadMetrics.fetchWaitTime);
      results.put("RemoteBytesRead", shuffleReadMetrics.remoteBytesRead);
    }
    if (allMetrics.shuffleWriteMetrics != null) {
      results.put("ShuffleBytesWritten", allMetrics.shuffleWriteMetrics.shuffleBytesWritten);
      results.put("ShuffleWriteTime", allMetrics.shuffleWriteMetrics.shuffleWriteTime);
    }

    return results;
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
}
