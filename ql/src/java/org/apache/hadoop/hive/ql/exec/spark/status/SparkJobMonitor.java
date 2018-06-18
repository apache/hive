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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.common.log.InPlaceUpdate;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

abstract class SparkJobMonitor {

  protected static final String CLASS_NAME = SparkJobMonitor.class.getName();
  protected static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  protected transient final SessionState.LogHelper console;
  protected final PerfLogger perfLogger = SessionState.getPerfLogger();
  protected final int checkInterval = 1000;
  protected final long monitorTimeoutInterval;
  private final InPlaceUpdate inPlaceUpdateFn;

  private final Set<String> completed = new HashSet<String>();
  private final int printInterval = 3000;
  private long lastPrintTime;

  protected long startTime;

  protected enum StageState {
    PENDING,
    RUNNING,
    FINISHED
  }

  protected final boolean inPlaceUpdate;

  protected SparkJobMonitor(HiveConf hiveConf) {
    monitorTimeoutInterval = hiveConf.getTimeVar(
        HiveConf.ConfVars.SPARK_JOB_MONITOR_TIMEOUT, TimeUnit.SECONDS);
    inPlaceUpdate = InPlaceUpdate.canRenderInPlace(hiveConf) && !SessionState.getConsole().getIsSilent();
    console = new SessionState.LogHelper(LOG);
    inPlaceUpdateFn = new InPlaceUpdate(SessionState.LogHelper.getInfoStream());
  }

  public abstract int startMonitor();

  private void printStatusInPlace(Map<SparkStage, SparkStageProgress> progressMap) {
    inPlaceUpdateFn.render(getProgressMonitor(progressMap));
  }

  protected void printStatus(Map<SparkStage, SparkStageProgress> progressMap,
      Map<SparkStage, SparkStageProgress> lastProgressMap) {

    // do not print duplicate status while still in middle of print interval.
    boolean isDuplicateState = isSameAsPreviousProgress(progressMap, lastProgressMap);
    boolean withinInterval = System.currentTimeMillis() <= lastPrintTime + printInterval;
    if (isDuplicateState && withinInterval) {
      return;
    }

    String report = getReport(progressMap);
    if (inPlaceUpdate) {
      printStatusInPlace(progressMap);
      console.logInfo(report);
    } else {
      console.printInfo(report);
    }

    lastPrintTime = System.currentTimeMillis();
  }

  protected int getTotalTaskCount(Map<SparkStage, SparkStageProgress> progressMap) {
    int totalTasks = 0;
    for (SparkStageProgress progress: progressMap.values() ) {
      totalTasks += progress.getTotalTaskCount();
    }

    return totalTasks;
  }

  protected int getStageMaxTaskCount(Map<SparkStage, SparkStageProgress> progressMap) {
    int stageMaxTasks = 0;
    for (SparkStageProgress progress: progressMap.values() ) {
      int tasks = progress.getTotalTaskCount();
      if (tasks > stageMaxTasks) {
        stageMaxTasks = tasks;
      }
    }

    return stageMaxTasks;
  }

  private String getReport(Map<SparkStage, SparkStageProgress> progressMap) {
    StringBuilder reportBuffer = new StringBuilder();
    SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
    String currentDate = dt.format(new Date());
    reportBuffer.append(currentDate + "\t");

    // Num of total and completed tasks
    int sumTotal = 0;
    int sumComplete = 0;

    SortedSet<SparkStage> keys = new TreeSet<SparkStage>(progressMap.keySet());
    for (SparkStage stage : keys) {
      SparkStageProgress progress = progressMap.get(stage);
      final int complete = progress.getSucceededTaskCount();
      final int total = progress.getTotalTaskCount();
      final int running = progress.getRunningTaskCount();
      final int failed = progress.getFailedTaskCount();
      sumTotal += total;
      sumComplete += complete;
      String s = stage.toString();
      String stageName = "Stage-" + s;
      if (total <= 0) {
        reportBuffer.append(String.format("%s: -/-\t", stageName));
      } else {
        if (complete == total && !completed.contains(s)) {
          completed.add(s);

          if (!perfLogger.startTimeHasMethod(PerfLogger.SPARK_RUN_STAGE + s)) {
            perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.SPARK_RUN_STAGE + s);
          }
          perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.SPARK_RUN_STAGE + s);
        }
        if (complete < total && (complete > 0 || running > 0 || failed > 0)) {
          /* stage is started, but not complete */
          if (!perfLogger.startTimeHasMethod(PerfLogger.SPARK_RUN_STAGE + s)) {
            perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.SPARK_RUN_STAGE + s);
          }
          if (failed > 0) {
            reportBuffer.append(
                String.format(
                    "%s: %d(+%d,-%d)/%d\t", stageName, complete, running, failed, total));
          } else {
            reportBuffer.append(
                String.format("%s: %d(+%d)/%d\t", stageName, complete, running, total));
          }
        } else {
          /* stage is waiting for input/slots or complete */
          if (failed > 0) {
            /* tasks finished but some failed */
            reportBuffer.append(
                String.format(
                    "%s: %d(-%d)/%d Finished with failed tasks\t",
                    stageName, complete, failed, total));
          } else {
            if (complete == total) {
              reportBuffer.append(
                  String.format("%s: %d/%d Finished\t", stageName, complete, total));
            } else {
              reportBuffer.append(String.format("%s: %d/%d\t", stageName, complete, total));
            }
          }
        }
      }
    }

    if (SessionState.get() != null) {
      final float progress = (sumTotal == 0) ? 1.0f : (float) sumComplete / (float) sumTotal;
      SessionState.get().updateProgressedPercentage(progress);
    }
    return reportBuffer.toString();
  }

  private boolean isSameAsPreviousProgress(
      Map<SparkStage, SparkStageProgress> progressMap,
      Map<SparkStage, SparkStageProgress> lastProgressMap) {

    if (lastProgressMap == null) {
      return false;
    }

    if (progressMap.isEmpty()) {
      return lastProgressMap.isEmpty();
    } else {
      if (lastProgressMap.isEmpty()) {
        return false;
      } else {
        if (progressMap.size() != lastProgressMap.size()) {
          return false;
        }
        for (SparkStage key : progressMap.keySet()) {
          if (!lastProgressMap.containsKey(key)
              || !progressMap.get(key).equals(lastProgressMap.get(key))) {
            return false;
          }
        }
      }
    }
    return true;
  }

  private SparkProgressMonitor getProgressMonitor(Map<SparkStage, SparkStageProgress> progressMap) {
    return new SparkProgressMonitor(progressMap, startTime);
  }
}
