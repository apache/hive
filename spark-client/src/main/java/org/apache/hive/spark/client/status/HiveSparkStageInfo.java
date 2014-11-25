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

import org.apache.spark.SparkStageInfo;

import java.io.Serializable;

/**
 * Wrapper of SparkStageInfo
 */
public class HiveSparkStageInfo implements SparkStageInfo, Serializable {
  private final int stageId;
  private final int currentAttemptId;
  private final String name;
  private final int numTasks;
  private final int numActiveTasks;
  private final int numCompletedTasks;
  private final int numFailedTasks;

  public HiveSparkStageInfo(SparkStageInfo stageInfo) {
    stageId = stageInfo.stageId();
    currentAttemptId = stageInfo.currentAttemptId();
    name = stageInfo.name();
    numTasks = stageInfo.numTasks();
    numActiveTasks = stageInfo.numActiveTasks();
    numCompletedTasks = stageInfo.numCompletedTasks();
    numFailedTasks = stageInfo.numFailedTasks();
  }

  public HiveSparkStageInfo(int stageId, int currentAttemptId, String name,
      int numTasks, int numActiveTasks, int numCompletedTasks, int numFailedTasks) {
    this.stageId = stageId;
    this.currentAttemptId = currentAttemptId;
    this.name = name;
    this.numTasks = numTasks;
    this.numActiveTasks = numActiveTasks;
    this.numCompletedTasks = numCompletedTasks;
    this.numFailedTasks = numFailedTasks;
  }

  public HiveSparkStageInfo() {
    this(-1, -1, null, -1, -1, -1, -1);
  }

  @Override
  public int stageId() {
    return stageId;
  }

  @Override
  public int currentAttemptId() {
    return currentAttemptId;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public int numTasks() {
    return numTasks;
  }

  @Override
  public int numActiveTasks() {
    return numActiveTasks;
  }

  @Override
  public int numCompletedTasks() {
    return numCompletedTasks;
  }

  @Override
  public int numFailedTasks() {
    return numFailedTasks;
  }

}
