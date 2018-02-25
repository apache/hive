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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerTaskEnd;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class JobMetricsListener extends SparkListener {

  private static final Logger LOG = LoggerFactory.getLogger(JobMetricsListener.class);

  private final Map<Integer, int[]> jobIdToStageId = Maps.newHashMap();
  private final Map<Integer, Integer> stageIdToJobId = Maps.newHashMap();
  private final Map<Integer, Map<Integer, List<TaskMetrics>>> allJobMetrics = Maps.newHashMap();

  @Override
  public synchronized void onTaskEnd(SparkListenerTaskEnd taskEnd) {
    int stageId = taskEnd.stageId();
    Integer jobId = stageIdToJobId.get(stageId);
    if (jobId == null) {
      LOG.warn("Can not find job id for stage[" + stageId + "].");
    } else {
      Map<Integer, List<TaskMetrics>> jobMetrics = allJobMetrics.get(jobId);
      if (jobMetrics == null) {
        jobMetrics = Maps.newHashMap();
        allJobMetrics.put(jobId, jobMetrics);
      }
      List<TaskMetrics> stageMetrics = jobMetrics.get(stageId);
      if (stageMetrics == null) {
        stageMetrics = Lists.newLinkedList();
        jobMetrics.put(stageId, stageMetrics);
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

  public synchronized  Map<Integer, List<TaskMetrics>> getJobMetric(int jobId) {
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
