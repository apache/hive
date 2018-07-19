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

package org.apache.hadoop.hive.ql.exec.spark.status;

import org.apache.hadoop.hive.common.log.ProgressMonitor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.common.log.InPlaceUpdate;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

abstract class SparkJobMonitor {

  protected static final String CLASS_NAME = SparkJobMonitor.class.getName();
  protected static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  protected transient final SessionState.LogHelper console;
  protected final PerfLogger perfLogger = SessionState.getPerfLogger();
  protected final int checkInterval = 1000;
  protected final long monitorTimeoutInterval;
  final RenderStrategy.UpdateFunction updateFunction;
  protected long startTime;

  protected enum StageState {
    PENDING, RUNNING, FINISHED
  }

  protected final boolean inPlaceUpdate;

  protected SparkJobMonitor(HiveConf hiveConf) {
    monitorTimeoutInterval = hiveConf.getTimeVar(HiveConf.ConfVars.SPARK_JOB_MONITOR_TIMEOUT, TimeUnit.SECONDS);
    inPlaceUpdate = InPlaceUpdate.canRenderInPlace(hiveConf) && !SessionState.getConsole().getIsSilent();
    console = new SessionState.LogHelper(LOG);
    updateFunction = updateFunction();
  }

  public abstract int startMonitor();

  protected int getTotalTaskCount(Map<SparkStage, SparkStageProgress> progressMap) {
    int totalTasks = 0;
    for (SparkStageProgress progress : progressMap.values()) {
      totalTasks += progress.getTotalTaskCount();
    }

    return totalTasks;
  }

  protected int getStageMaxTaskCount(Map<SparkStage, SparkStageProgress> progressMap) {
    int stageMaxTasks = 0;
    for (SparkStageProgress progress : progressMap.values()) {
      int tasks = progress.getTotalTaskCount();
      if (tasks > stageMaxTasks) {
        stageMaxTasks = tasks;
      }
    }

    return stageMaxTasks;
  }

  ProgressMonitor getProgressMonitor(Map<SparkStage, SparkStageProgress> progressMap) {
    return new SparkProgressMonitor(progressMap, startTime);
  }

  private RenderStrategy.UpdateFunction updateFunction() {
    return inPlaceUpdate && !SessionState.get().isHiveServerQuery() ? new RenderStrategy.InPlaceUpdateFunction(
        this) : new RenderStrategy.LogToFileFunction(this);
  }
}
