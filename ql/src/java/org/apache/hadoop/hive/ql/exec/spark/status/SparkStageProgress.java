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
package org.apache.hadoop.hive.ql.exec.spark.status;

public class SparkStageProgress {

  private int totalTaskCount;
  private int succeededTaskCount;
  private int runningTaskCount;
  private int failedTaskCount;

  public SparkStageProgress(
    int totalTaskCount,
    int succeededTaskCount,
    int runningTaskCount,
    int failedTaskCount) {

    this.totalTaskCount = totalTaskCount;
    this.succeededTaskCount = succeededTaskCount;
    this.runningTaskCount = runningTaskCount;
    this.failedTaskCount = failedTaskCount;
  }

  public int getTotalTaskCount() {
    return totalTaskCount;
  }

  public int getSucceededTaskCount() {
    return succeededTaskCount;
  }

  public int getRunningTaskCount() {
    return runningTaskCount;
  }

  public int getFailedTaskCount() {
    return failedTaskCount;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SparkStageProgress) {
      SparkStageProgress other = (SparkStageProgress) obj;
      return getTotalTaskCount() == other.getTotalTaskCount()
        && getSucceededTaskCount() == other.getSucceededTaskCount()
        && getRunningTaskCount() == other.getRunningTaskCount()
        && getFailedTaskCount() == other.getFailedTaskCount();
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("TotalTasks: ");
    sb.append(getTotalTaskCount());
    sb.append(" Succeeded: ");
    sb.append(getSucceededTaskCount());
    sb.append(" Running: ");
    sb.append(getRunningTaskCount());
    sb.append(" Failed: ");
    sb.append(getFailedTaskCount());
    return sb.toString();
  }
}
