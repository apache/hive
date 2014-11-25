/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.spark.client.status;

import org.apache.spark.JobExecutionStatus;
import org.apache.spark.SparkJobInfo;

import java.io.Serializable;

/**
 * Wrapper of SparkJobInfo
 */
public class HiveSparkJobInfo implements SparkJobInfo, Serializable {
  private final int jobId;
  private final int[] stageIds;
  private final JobExecutionStatus status;

  public HiveSparkJobInfo(SparkJobInfo jobInfo) {
    this.jobId = jobInfo.jobId();
    this.stageIds = jobInfo.stageIds();
    this.status = jobInfo.status();
  }

  public HiveSparkJobInfo(int jobId, int[] stageIds, JobExecutionStatus status) {
    this.jobId = jobId;
    this.stageIds = stageIds;
    this.status = status;
  }

  public HiveSparkJobInfo() {
    this(-1, new int[0], JobExecutionStatus.UNKNOWN);
  }

  @Override
  public int jobId() {
    return jobId;
  }

  @Override
  public int[] stageIds() {
    return stageIds;
  }

  @Override
  public JobExecutionStatus status() {
    return status;
  }

}
