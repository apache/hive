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
import org.apache.hadoop.hive.ql.exec.spark.counter.SparkCounters;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobStatus;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkStageProgress;
import org.apache.hive.spark.client.Job;
import org.apache.hive.spark.client.JobContext;
import org.apache.hive.spark.client.JobHandle;
import org.apache.hive.spark.client.SparkClient;
import org.apache.hive.spark.client.status.HiveSparkJobInfo;
import org.apache.hive.spark.client.status.HiveSparkStageInfo;
import org.apache.spark.JobExecutionStatus;
import org.apache.spark.SparkJobInfo;
import org.apache.spark.SparkStageInfo;
import org.apache.spark.api.java.JavaFutureAction;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Used with remove spark client.
 */
public class RemoteSparkJobStatus implements SparkJobStatus {
  private static final Log LOG = LogFactory.getLog(RemoteSparkJobStatus.class.getName());
  // time (in seconds) to wait for a spark job to be submitted on remote cluster
  // after this period, we decide the job submission has failed so that client won't hang forever
  private static final int WAIT_SUBMISSION_TIMEOUT = 30;
  // remember when the monitor starts
  private final long startTime;
  private final SparkClient sparkClient;
  private final JobHandle<Serializable> jobHandle;

  public RemoteSparkJobStatus(SparkClient sparkClient, JobHandle<Serializable> jobHandle) {
    this.sparkClient = sparkClient;
    this.jobHandle = jobHandle;
    startTime = System.currentTimeMillis();
  }

  @Override
  public int getJobId() {
    return jobHandle.getSparkJobIds().size() == 1 ? jobHandle.getSparkJobIds().get(0) : -1;
  }

  @Override
  public JobExecutionStatus getState() {
    SparkJobInfo sparkJobInfo = getSparkJobInfo();
    return sparkJobInfo != null ? sparkJobInfo.status() : JobExecutionStatus.UNKNOWN;
  }

  @Override
  public int[] getStageIds() {
    SparkJobInfo sparkJobInfo = getSparkJobInfo();
    return sparkJobInfo != null ? sparkJobInfo.stageIds() : new int[0];
  }

  @Override
  public Map<String, SparkStageProgress> getSparkStageProgress() {
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
        stageProgresses.put(String.valueOf(sparkStageInfo.stageId()) + "_" +
            sparkStageInfo.currentAttemptId(), sparkStageProgress);
      }
    }
    return stageProgresses;
  }

  @Override
  public SparkCounters getCounter() {
    return null;
  }

  @Override
  public SparkStatistics getSparkStatistics() {
    return null;
  }

  @Override
  public void cleanup() {

  }

  private SparkJobInfo getSparkJobInfo() {
    Integer sparkJobId = jobHandle.getSparkJobIds().size() == 1 ?
        jobHandle.getSparkJobIds().get(0) : null;
    if (sparkJobId == null) {
      int duration = (int) ((System.currentTimeMillis() - startTime) / 1000);
      if (duration <= WAIT_SUBMISSION_TIMEOUT) {
        return null;
      } else {
        LOG.info("Job hasn't been submitted after " + duration + "s. Aborting it.");
        jobHandle.cancel(false);
        return new SparkJobInfo() {
          @Override
          public int jobId() {
            return -1;
          }

          @Override
          public int[] stageIds() {
            return new int[0];
          }

          @Override
          public JobExecutionStatus status() {
            return JobExecutionStatus.FAILED;
          }
        };
      }
    }
    JobHandle<HiveSparkJobInfo> getJobInfo = sparkClient.submit(
        new GetJobInfoJob(jobHandle.getClientJobId(), sparkJobId));
    try {
      return getJobInfo.get();
    } catch (Throwable t) {
      LOG.warn("Error getting job info", t);
      return null;
    }
  }

  private SparkStageInfo getSparkStageInfo(int stageId) {
    JobHandle<HiveSparkStageInfo> getStageInfo = sparkClient.submit(new GetStageInfoJob(stageId));
    try {
      return getStageInfo.get();
    } catch (Throwable t) {
      LOG.warn("Error getting stage info", t);
      return null;
    }
  }

  private static class GetJobInfoJob implements Job<HiveSparkJobInfo> {
    private final String clientJobId;
    private final int sparkJobId;

    GetJobInfoJob(String clientJobId, int sparkJobId) {
      this.clientJobId = clientJobId;
      this.sparkJobId = sparkJobId;
    }

    @Override
    public HiveSparkJobInfo call(JobContext jc) throws Exception {
      SparkJobInfo jobInfo = jc.sc().statusTracker().getJobInfo(sparkJobId);
      if (jobInfo == null) {
        List<JavaFutureAction<?>> list = jc.getMonitoredJobs().get(clientJobId);
        if (list != null && list.size() == 1) {
          JavaFutureAction<?> futureAction = list.get(0);
          if (futureAction.isDone()) {
            jobInfo = new SparkJobInfo() {
              @Override
              public int jobId() {
                return sparkJobId;
              }

              @Override
              public int[] stageIds() {
                return new int[0];
              }

              @Override
              public JobExecutionStatus status() {
                return JobExecutionStatus.SUCCEEDED;
              }
            };
          }
        }
      }
      if(jobInfo == null) {
        jobInfo = new SparkJobInfo() {
          @Override
          public int jobId() {
            return -1;
          }

          @Override
          public int[] stageIds() {
            return new int[0];
          }

          @Override
          public JobExecutionStatus status() {
            return JobExecutionStatus.UNKNOWN;
          }
        };
      }
      return new HiveSparkJobInfo(jobInfo);
    }
  }

  private static class GetStageInfoJob implements Job<HiveSparkStageInfo>{
    private final int stageId;

    GetStageInfoJob(int stageId){
      this.stageId=stageId;
    }

    @Override
    public HiveSparkStageInfo call(JobContext jc) throws Exception {
      SparkStageInfo stageInfo = jc.sc().statusTracker().getStageInfo(stageId);
      return stageInfo != null ? new HiveSparkStageInfo(stageInfo) : new HiveSparkStageInfo();
    }
  }
}
