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

import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobState;
import org.apache.spark.scheduler.JobSucceeded;
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

import scala.collection.JavaConversions;

public class JobStateListener implements SparkListener {

  private Map<Integer, SparkJobState> jobIdToStates = new HashMap<Integer, SparkJobState>();
  private Map<Integer, int[]> jobIdToStageId = new HashMap<Integer, int[]>();

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
  public void onTaskEnd(SparkListenerTaskEnd taskEnd) {

  }

  @Override
  public synchronized void onJobStart(SparkListenerJobStart jobStart) {
    jobIdToStates.put(jobStart.jobId(), SparkJobState.RUNNING);
    List<Object> ids = JavaConversions.asJavaList(jobStart.stageIds());
    int[] intStageIds = new int[ids.size()];
    for(int i=0; i<ids.size(); i++) {
      intStageIds[i] = (Integer)ids.get(i);
    }
    jobIdToStageId.put(jobStart.jobId(), intStageIds);
  }

  @Override
  public synchronized void onJobEnd(SparkListenerJobEnd jobEnd) {
    // JobSucceeded is a scala singleton object, so we need to add a dollar at the second part.
    if (jobEnd.jobResult().getClass().getName().equals(JobSucceeded.class.getName() + "$")) {
      jobIdToStates.put(jobEnd.jobId(), SparkJobState.SUCCEEDED);
    } else {
      jobIdToStates.put(jobEnd.jobId(), SparkJobState.FAILED);
    }
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

  public synchronized SparkJobState getJobState(int jobId) {
    return jobIdToStates.get(jobId);
  }

  public synchronized int[] getStageIds(int jobId) {
    return jobIdToStageId.get(jobId);
  }
}
