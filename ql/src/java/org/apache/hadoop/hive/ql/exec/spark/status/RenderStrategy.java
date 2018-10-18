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

import org.apache.hadoop.hive.common.log.InPlaceUpdate;
import org.apache.hadoop.hive.common.log.ProgressMonitor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to render progress bar for Hive on Spark job status.
 * Based on the configuration, appropriate render strategy is selected
 * to show the progress bar on beeline or Hive CLI, as well as for logging
 * the report String.
 */
class RenderStrategy {

  interface UpdateFunction {
    void printStatus(Map<SparkStage, SparkStageProgress> progressMap,
        Map<SparkStage, SparkStageProgress> lastProgressMap);
  }

  private abstract static class BaseUpdateFunction implements UpdateFunction {
    protected final SparkJobMonitor monitor;
    private final PerfLogger perfLogger;
    private long lastPrintTime;
    private static final int PRINT_INTERVAL = 3000;
    private final Set<String> completed = new HashSet<String>();
    private String lastReport = null;

    BaseUpdateFunction(SparkJobMonitor monitor) {
      this.monitor = monitor;
      this.perfLogger = SessionState.getPerfLogger();
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
              perfLogger.PerfLogBegin(SparkJobMonitor.CLASS_NAME, PerfLogger.SPARK_RUN_STAGE + s);
            }
            perfLogger.PerfLogEnd(SparkJobMonitor.CLASS_NAME, PerfLogger.SPARK_RUN_STAGE + s);
          }
          if (complete < total && (complete > 0 || running > 0 || failed > 0)) {
            /* stage is started, but not complete */
            if (!perfLogger.startTimeHasMethod(PerfLogger.SPARK_RUN_STAGE + s)) {
              perfLogger.PerfLogBegin(SparkJobMonitor.CLASS_NAME, PerfLogger.SPARK_RUN_STAGE + s);
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
          for (Map.Entry<SparkStage, SparkStageProgress> entry : progressMap.entrySet()) {
            if (!lastProgressMap.containsKey(entry.getKey())
                || !progressMap.get(entry.getKey()).equals(lastProgressMap.get(entry.getKey()))) {
              return false;
            }
          }
        }
      }
      return true;
    }


    private boolean showReport(String report) {
      return !report.equals(lastReport) || System.currentTimeMillis() >= lastPrintTime + PRINT_INTERVAL;
    }

    @Override
    public void printStatus(Map<SparkStage, SparkStageProgress> progressMap,
        Map<SparkStage, SparkStageProgress> lastProgressMap) {
      // do not print duplicate status while still in middle of print interval.
      boolean isDuplicateState = isSameAsPreviousProgress(progressMap, lastProgressMap);
      boolean withinInterval = System.currentTimeMillis() <= lastPrintTime + PRINT_INTERVAL;
      if (isDuplicateState && withinInterval) {
        return;
      }

      String report = getReport(progressMap);
      renderProgress(monitor.getProgressMonitor(progressMap));
      if (showReport(report)) {
        renderReport(report);
        lastReport = report;
        lastPrintTime = System.currentTimeMillis();
      }
    }

    abstract void renderProgress(ProgressMonitor monitor);

    abstract void renderReport(String report);
  }

  /**
   * This is used to show progress bar on Beeline while using HiveServer2.
   */
  static class LogToFileFunction extends BaseUpdateFunction {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogToFileFunction.class);
    private boolean hiveServer2InPlaceProgressEnabled =
        SessionState.get().getConf().getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_INPLACE_PROGRESS);

    LogToFileFunction(SparkJobMonitor monitor) {
      super(monitor);
    }

    @Override
    void renderProgress(ProgressMonitor monitor) {
      SessionState.get().updateProgressMonitor(monitor);
    }

    @Override
    void renderReport(String report) {
      if (hiveServer2InPlaceProgressEnabled) {
        LOGGER.info(report);
      } else {
        monitor.console.printInfo(report);
      }
    }
  }

  /**
   * This is used to show progress bar on Hive CLI.
   */
  static class InPlaceUpdateFunction extends BaseUpdateFunction {
    /**
     * Have to use the same instance to render else the number lines printed earlier is lost and the
     * screen will print the table again and again.
     */
    private final InPlaceUpdate inPlaceUpdate;

    InPlaceUpdateFunction(SparkJobMonitor monitor) {
      super(monitor);
      inPlaceUpdate = new InPlaceUpdate(SessionState.LogHelper.getInfoStream());
    }

    @Override
    void renderProgress(ProgressMonitor monitor) {
      inPlaceUpdate.render(monitor);
    }

    @Override
    void renderReport(String report) {
      monitor.console.logInfo(report);
    }
  }
}


