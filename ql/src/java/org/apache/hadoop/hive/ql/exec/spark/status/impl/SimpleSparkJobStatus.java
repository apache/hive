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

import org.apache.hadoop.hive.ql.exec.spark.counter.SparkCounters;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobState;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobStatus;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkStageProgress;
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

  public SimpleSparkJobStatus(
    int jobId,
    JobStateListener stateListener,
    JobProgressListener progressListener,
    SparkCounters sparkCounters) {

    this.jobId = jobId;
    this.jobStateListener = stateListener;
    this.jobProgressListener = progressListener;
    this.sparkCounters = sparkCounters;
  }

  @Override
  public int getJobId() {
    return jobId;
  }

  @Override
  public SparkJobState getState() {
    return jobStateListener.getJobState(jobId);
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
