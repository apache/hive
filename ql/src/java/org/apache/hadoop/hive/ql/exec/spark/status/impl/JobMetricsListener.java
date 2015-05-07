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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerBlockManagerAdded;
import org.apache.spark.scheduler.SparkListenerBlockManagerRemoved;
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate;
import org.apache.spark.scheduler.SparkListenerExecutorMetricsUpdate;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.SparkListenerTaskGettingResult;
import org.apache.spark.scheduler.SparkListenerTaskStart;
import org.apache.spark.scheduler.SparkListenerUnpersistRDD;
import org.apache.spark.scheduler.SparkListenerExecutorRemoved;
import org.apache.spark.scheduler.SparkListenerExecutorAdded;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class JobMetricsListener implements SparkListener {

  private static final Log LOG = LogFactory.getLog(JobMetricsListener.class);

  private final Map<Integer, int[]> jobIdToStageId = Maps.newHashMap();
  private final Map<Integer, Integer> stageIdToJobId = Maps.newHashMap();
  private final Map<Integer, Map<String, List<TaskMetrics>>> allJobMetrics = Maps.newHashMap();

  @Override
  public void onExecutorRemoved(SparkListenerExecutorRemoved removed) {

  }

  @Override
  public void onExecutorAdded(SparkListenerExecutorAdded added) {

  }

  @Override
  public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {

  }

  @Override
  public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {

  }

  @Override
  public void onTaskStart(SparkListenerTaskStart taskStart) {

  }

  @Override
  public void onTaskGettingResult(SparkListenerTaskGettingResult taskGettingResult) {

  }

  @Override
  public synchronized void onTaskEnd(SparkListenerTaskEnd taskEnd) {
    int stageId = taskEnd.stageId();
    int stageAttemptId = taskEnd.stageAttemptId();
    String stageIdentifier = stageId + "_" + stageAttemptId;
    Integer jobId = stageIdToJobId.get(stageId);
    if (jobId == null) {
      LOG.warn("Can not find job id for stage[" + stageId + "].");
    } else {
      Map<String, List<TaskMetrics>> jobMetrics = allJobMetrics.get(jobId);
      if (jobMetrics == null) {
        jobMetrics = Maps.newHashMap();
        allJobMetrics.put(jobId, jobMetrics);
      }
      List<TaskMetrics> stageMetrics = jobMetrics.get(stageIdentifier);
      if (stageMetrics == null) {
        stageMetrics = Lists.newLinkedList();
        jobMetrics.put(stageIdentifier, stageMetrics);
      }
      stageMetrics.add(taskEnd.taskMetrics());
    }
  }

  @Override
  public synchronized void onJobStart(SparkListenerJobStart jobStart) {
    int jobId = jobStart.jobId();
    int size = jobStart.stageIds().size();
    int[] intStageIds = new int[size];
    for (int i = 0; i < size; i++) {
      Integer stageId = (Integer) jobStart.stageIds().apply(i);
      intStageIds[i] = stageId;
      stageIdToJobId.put(stageId, jobId);
    }
    jobIdToStageId.put(jobId, intStageIds);
  }

  @Override
  public synchronized void onJobEnd(SparkListenerJobEnd jobEnd) {

  }

  @Override
  public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate environmentUpdate) {

  }

  @Override
  public void onBlockManagerAdded(SparkListenerBlockManagerAdded blockManagerAdded) {

  }

  @Override
  public void onBlockManagerRemoved(SparkListenerBlockManagerRemoved blockManagerRemoved) {

  }

  @Override
  public void onUnpersistRDD(SparkListenerUnpersistRDD unpersistRDD) {

  }

  @Override
  public void onApplicationStart(SparkListenerApplicationStart applicationStart) {

  }

  @Override
  public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {

  }

  @Override
  public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate executorMetricsUpdate) {

  }

  public synchronized  Map<String, List<TaskMetrics>> getJobMetric(int jobId) {
    return allJobMetrics.get(jobId);
  }

  public synchronized void cleanup(int jobId) {
    allJobMetrics.remove(jobId);
    jobIdToStageId.remove(jobId);
    Iterator<Map.Entry<Integer, Integer>> iterator = stageIdToJobId.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Integer, Integer> entry = iterator.next();
      if (entry.getValue() == jobId) {
        iterator.remove();
      }
    }
  }
}
