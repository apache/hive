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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.hadoop.hive.ql.exec.spark.Statistic.SparkStatistics;
import org.apache.hadoop.hive.ql.exec.spark.Statistic.SparkStatisticsBuilder;
import org.apache.hadoop.hive.ql.exec.spark.counter.SparkCounters;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobState;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobStatus;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkStageProgress;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.executor.InputMetrics;
import org.apache.spark.executor.ShuffleReadMetrics;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.ui.jobs.JobProgressListener;
import org.apache.spark.ui.jobs.UIData;

import scala.Option;
import scala.Tuple2;

import static scala.collection.JavaConversions.bufferAsJavaList;
import static scala.collection.JavaConversions.mutableMapAsJavaMap;

public class SimpleSparkJobStatus implements SparkJobStatus {

  private int jobId;
  private JobStateListener jobStateListener;
  private JobProgressListener jobProgressListener;
  private SparkCounters sparkCounters;
  private JavaFutureAction<Void> future;

  public SimpleSparkJobStatus(
    int jobId,
    JobStateListener stateListener,
    JobProgressListener progressListener,
    SparkCounters sparkCounters,
    JavaFutureAction<Void> future) {

    this.jobId = jobId;
    this.jobStateListener = stateListener;
    this.jobProgressListener = progressListener;
    this.sparkCounters = sparkCounters;
    this.future = future;
  }

  @Override
  public int getJobId() {
    return jobId;
  }

  @Override
  public SparkJobState getState() {
    // For spark job with empty source data, it's not submitted actually, so we would never
    // receive JobStart/JobEnd event in JobStateListener, use JavaFutureAction to get current
    // job state.
    if (future.isDone()) {
      return SparkJobState.SUCCEEDED;
    } else {
      return jobStateListener.getJobState(jobId);
    }
  }

  @Override
  public int[] getStageIds() {
    return jobStateListener.getStageIds(jobId);
  }

  @Override
  public Map<String, SparkStageProgress> getSparkStageProgress() {
    Map<String, SparkStageProgress> stageProgresses = new HashMap<String, SparkStageProgress>();
    int[] stageIds = jobStateListener.getStageIds(jobId);
    if (stageIds != null) {
      for (int stageId : stageIds) {
        List<StageInfo> stageInfos = getStageInfo(stageId);
        for (StageInfo stageInfo : stageInfos) {
          Tuple2<Object, Object> tuple2 = new Tuple2<Object, Object>(stageInfo.stageId(),
            stageInfo.attemptId());
          UIData.StageUIData uiData = jobProgressListener.stageIdToData().get(tuple2).get();
          if (uiData != null) {
            int runningTaskCount = uiData.numActiveTasks();
            int completedTaskCount = uiData.numCompleteTasks();
            int failedTaskCount = uiData.numFailedTasks();
            int totalTaskCount = stageInfo.numTasks();
            int killedTaskCount = 0;
            long costTime;
            Option<Object> startOption = stageInfo.submissionTime();
            Option<Object> completeOption = stageInfo.completionTime();
            if (startOption.isEmpty()) {
              costTime = 0;
            } else if (completeOption.isEmpty()) {
              long startTime = (Long)startOption.get();
              costTime = System.currentTimeMillis() - startTime;
            } else {
              long startTime = (Long)startOption.get();
              long completeTime = (Long)completeOption.get();
              costTime = completeTime - startTime;
            }
            SparkStageProgress stageProgress = new SparkStageProgress(
              totalTaskCount,
              completedTaskCount,
              runningTaskCount,
              failedTaskCount,
              killedTaskCount,
              costTime);
            stageProgresses.put(stageInfo.stageId() + "_" + stageInfo.attemptId(), stageProgress);
          }
        }
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
    Map<String, List<TaskMetrics>> jobMetric = jobStateListener.getJobMetric(jobId);
    if (jobMetric == null) {
      return null;
    }

    Map<String, Long> flatJobMetric = combineJobLevelMetrics(jobMetric);
    for (Map.Entry<String, Long> entry : flatJobMetric.entrySet()) {
      sparkStatisticsBuilder.add(jobIdentifier, entry.getKey(), Long.toString(entry.getValue()));
    }

    return  sparkStatisticsBuilder.build();
  }

  @Override
  public void cleanup() {
    jobStateListener.cleanup(jobId);
  }

  private Map<String, Long> combineJobLevelMetrics(Map<String, List<TaskMetrics>> jobMetric) {
    Map<String, Long> results = Maps.newLinkedHashMap();

    long executorDeserializeTime = 0;
    long executorRunTime = 0;
    long resultSize = 0;
    long jvmGCTime = 0;
    long resultSerializationTime = 0;
    long memoryBytesSpilled = 0;
    long diskBytesSpilled = 0;
    long bytesRead = 0;
    long remoteBlocksFetched = 0;
    long localBlocksFetched = 0;
    long fetchWaitTime = 0;
    long remoteBytesRead = 0;
    long shuffleBytesWritten = 0;
    long shuffleWriteTime = 0;
    boolean inputMetricExist = false;
    boolean shuffleReadMetricExist = false;
    boolean shuffleWriteMetricExist = false;

    for (List<TaskMetrics> stageMetric : jobMetric.values()) {
      if (stageMetric != null) {
        for (TaskMetrics taskMetrics : stageMetric) {
          if (taskMetrics != null) {
            executorDeserializeTime += taskMetrics.executorDeserializeTime();
            executorRunTime += taskMetrics.executorRunTime();
            resultSize += taskMetrics.resultSize();
            jvmGCTime += taskMetrics.jvmGCTime();
            resultSerializationTime += taskMetrics.resultSerializationTime();
            memoryBytesSpilled += taskMetrics.memoryBytesSpilled();
            diskBytesSpilled += taskMetrics.diskBytesSpilled();
            if (!taskMetrics.inputMetrics().isEmpty()) {
              inputMetricExist = true;
              bytesRead += taskMetrics.inputMetrics().get().bytesRead();
            }
            Option<ShuffleReadMetrics> shuffleReadMetricsOption = taskMetrics.shuffleReadMetrics();
            if (!shuffleReadMetricsOption.isEmpty()) {
              shuffleReadMetricExist = true;
              remoteBlocksFetched += shuffleReadMetricsOption.get().remoteBlocksFetched();
              localBlocksFetched += shuffleReadMetricsOption.get().localBlocksFetched();
              fetchWaitTime += shuffleReadMetricsOption.get().fetchWaitTime();
              remoteBytesRead += shuffleReadMetricsOption.get().remoteBytesRead();
            }
            Option<ShuffleWriteMetrics> shuffleWriteMetricsOption = taskMetrics.shuffleWriteMetrics();
            if (!shuffleWriteMetricsOption.isEmpty()) {
              shuffleWriteMetricExist = true;
              shuffleBytesWritten += shuffleWriteMetricsOption.get().shuffleBytesWritten();
              shuffleWriteTime += shuffleWriteMetricsOption.get().shuffleWriteTime();
            }
          }
        }
      }
    }

    results.put("EexcutorDeserializeTime", executorDeserializeTime);
    results.put("ExecutorRunTime", executorRunTime);
    results.put("ResultSize", resultSize);
    results.put("JvmGCTime", jvmGCTime);
    results.put("ResultSerializationTime", resultSerializationTime);
    results.put("MemoryBytesSpilled", memoryBytesSpilled);
    results.put("DiskBytesSpilled", diskBytesSpilled);
    if (inputMetricExist) {
      results.put("BytesRead", bytesRead);
    }
    if (shuffleReadMetricExist) {
      results.put("RemoteBlocksFetched", remoteBlocksFetched);
      results.put("LocalBlocksFetched", localBlocksFetched);
      results.put("TotalBlocksFetched", localBlocksFetched + remoteBlocksFetched);
      results.put("FetchWaitTime", fetchWaitTime);
      results.put("RemoteBytesRead", remoteBytesRead);
    }
    if (shuffleWriteMetricExist) {
      results.put("ShuffleBytesWritten", shuffleBytesWritten);
      results.put("ShuffleWriteTime", shuffleWriteTime);
    }
    return results;
  }

  private List<StageInfo> getStageInfo(int stageId) {
    List<StageInfo> stageInfos = new LinkedList<StageInfo>();

    Map<Object, StageInfo> activeStages = mutableMapAsJavaMap(jobProgressListener.activeStages());
    List<StageInfo> completedStages = bufferAsJavaList(jobProgressListener.completedStages());
    List<StageInfo> failedStages = bufferAsJavaList(jobProgressListener.failedStages());

    if (activeStages.containsKey(stageId)) {
      stageInfos.add(activeStages.get(stageId));
    } else {
      for (StageInfo stageInfo : completedStages) {
        if (stageInfo.stageId() == stageId) {
          stageInfos.add(stageInfo);
        }
      }

      for (StageInfo stageInfo : failedStages) {
        if (stageInfo.stageId() == stageId) {
          stageInfos.add(stageInfo);
        }
      }
    }

    return stageInfos;
  }
}
