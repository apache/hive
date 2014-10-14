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

import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobState;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobStatus;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkProgress;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.ui.jobs.JobProgressListener;
import org.apache.spark.ui.jobs.UIData;

import scala.Tuple2;

import static scala.collection.JavaConversions.bufferAsJavaList;
import static scala.collection.JavaConversions.mutableMapAsJavaMap;

public class SimpleSparkJobStatus implements SparkJobStatus {

  private int jobId;
  private JobStateListener jobStateListener;
  private JobProgressListener jobProgressListener;

  public SimpleSparkJobStatus(
    int jobId,
    JobStateListener stateListener,
    JobProgressListener progressListener) {

    this.jobId = jobId;
    this.jobStateListener = stateListener;
    this.jobProgressListener = progressListener;
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
  public SparkProgress getSparkJobProgress() {
    Map<String, SparkProgress> stageProgresses = getSparkStageProgress();

    int totalTaskCount = 0;
    int runningTaskCount = 0;
    int completedTaskCount = 0;
    int failedTaskCount = 0;
    int killedTaskCount = 0;

    for (SparkProgress sparkProgress : stageProgresses.values()) {
      totalTaskCount += sparkProgress.getTotalTaskCount();
      runningTaskCount += sparkProgress.getRunningTaskCount();
      completedTaskCount += sparkProgress.getSucceededTaskCount();
      failedTaskCount += sparkProgress.getFailedTaskCount();
      killedTaskCount += sparkProgress.getKilledTaskCount();
    }

    return new SparkProgress(
      totalTaskCount, completedTaskCount, runningTaskCount, failedTaskCount, killedTaskCount);
  }

  @Override
  public int[] getStageIds() {
    return jobStateListener.getStageIds(jobId);
  }

  @Override
  public Map<String, SparkProgress> getSparkStageProgress() {
    Map<String, SparkProgress> stageProgresses = new HashMap<String, SparkProgress>();
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
            SparkProgress stageProgress = new SparkProgress(
              totalTaskCount,
              completedTaskCount,
              runningTaskCount,
              failedTaskCount,
              killedTaskCount);
            stageProgresses.put(stageInfo.stageId() + "_" + stageInfo.attemptId(), stageProgress);
          }
        }
      }
    }
    return stageProgresses;
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
