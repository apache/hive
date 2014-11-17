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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.spark.JobExecutionStatus;

/**
 * SparkJobMonitor monitor a single Spark job status in a loop until job finished/failed/killed.
 * It print current job status to console and sleep current thread between monitor interval.
 */
public class SparkJobMonitor {

  private static final String CLASS_NAME = SparkJobMonitor.class.getName();
  private static final Log LOG = LogFactory.getLog(CLASS_NAME);

  private transient LogHelper console;
  private final int checkInterval = 200;
  private final int maxRetryInterval = 2500;
  private final int printInterval = 3000;
  private long lastPrintTime;
  private Set<String> completed;

  private SparkJobStatus sparkJobStatus;

  public SparkJobMonitor(SparkJobStatus sparkJobStatus) {
    this.sparkJobStatus = sparkJobStatus;
    console = new LogHelper(LOG);
  }

  public int startMonitor() {
    completed = new HashSet<String>();

    boolean running = false;
    boolean done = false;
    int failedCounter = 0;
    int rc = 0;
    JobExecutionStatus lastState = null;
    Map<String, SparkStageProgress> lastProgressMap = null;
    long startTime = 0;

    while (true) {
      try {
        JobExecutionStatus state = sparkJobStatus.getState();
        if (state != null && (state != lastState || state == JobExecutionStatus.RUNNING)) {
          lastState = state;
          Map<String, SparkStageProgress> progressMap = sparkJobStatus.getSparkStageProgress();

          switch (state) {
          case RUNNING:
            if (!running) {
              // print job stages.
              console.printInfo("\nQuery Hive on Spark job[" +
                sparkJobStatus.getJobId() + "] stages:");
              for (int stageId : sparkJobStatus.getStageIds()) {
                console.printInfo(Integer.toString(stageId));
              }

              console.printInfo("\nStatus: Running (Hive on Spark job[" +
                sparkJobStatus.getJobId() + "])");
              startTime = System.currentTimeMillis();
              running = true;

              console.printInfo("Job Progress Format\nCurrentTime StageId_StageAttemptId: " +
                "SucceededTasksCount(+RunningTasksCount-FailedTasksCount)/TotalTasksCount [StageCost]");
            }


            printStatus(progressMap, lastProgressMap);
            lastProgressMap = progressMap;
            break;
          case SUCCEEDED:
            printStatus(progressMap, lastProgressMap);
            lastProgressMap = progressMap;
            double duration = (System.currentTimeMillis() - startTime) / 1000.0;
            console.printInfo("Status: Finished successfully in " +
              String.format("%.2f seconds", duration));
            running = false;
            done = true;
            break;
          case FAILED:
            console.printError("Status: Failed");
            running = false;
            done = true;
            rc = 2;
            break;
          }
        }
        if (!done) {
          Thread.sleep(checkInterval);
        }
      } catch (Exception e) {
        console.printInfo("Exception: " + e.getMessage());
        if (++failedCounter % maxRetryInterval / checkInterval == 0
          || e instanceof InterruptedException) {
          console.printInfo("Killing Job...");
          console.printError("Execution has failed.");
          rc = 1;
          done = true;
        } else {
          console.printInfo("Retrying...");
        }
      } finally {
        if (done) {
          break;
        }
      }
    }
    return rc;
  }

  private void printStatus(Map<String, SparkStageProgress> progressMap, Map<String, SparkStageProgress> lastProgressMap) {

    // do not print duplicate status while still in middle of print interval.
    boolean isDuplicateState = isSameAsPreviousProgress(progressMap, lastProgressMap);
    boolean isPassedInterval = System.currentTimeMillis() <= lastPrintTime + printInterval;
    if (isDuplicateState && isPassedInterval) {
      return;
    }

    StringBuffer reportBuffer = new StringBuffer();
    SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
    String currentDate = dt.format(new Date());
    reportBuffer.append(currentDate + "\t");

    SortedSet<String> keys = new TreeSet<String>(progressMap.keySet());
    for (String s : keys) {
      SparkStageProgress progress = progressMap.get(s);
      final int complete = progress.getSucceededTaskCount();
      final int total = progress.getTotalTaskCount();
      final int running = progress.getRunningTaskCount();
      final int failed = progress.getFailedTaskCount();
      String stageName = "Stage-" + s;
      if (total <= 0) {
        reportBuffer.append(String.format("%s: -/-\t", stageName, complete, total));
      } else {
        if (complete == total && !completed.contains(s)) {
          completed.add(s);
        }
        if (complete < total && (complete > 0 || running > 0 || failed > 0)) {
          /* stage is started, but not complete */
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

    lastPrintTime = System.currentTimeMillis();
    console.printInfo(reportBuffer.toString());
  }

  private boolean isSameAsPreviousProgress(
    Map<String, SparkStageProgress> progressMap,
    Map<String, SparkStageProgress> lastProgressMap) {

    if (lastProgressMap == null) {
      return false;
    }

    if (progressMap.isEmpty()) {
      if (lastProgressMap.isEmpty()) {
        return true;
      } else {
        return false;
      }
    } else {
      if (lastProgressMap.isEmpty()) {
        return false;
      } else {
        if (progressMap.size() != lastProgressMap.size()) {
          return false;
        }
        for (String key : progressMap.keySet()) {
          if (!lastProgressMap.containsKey(key) ||
            !progressMap.get(key).equals(lastProgressMap.get(key))) {
            return false;
          }
        }
      }
    }
    return true;
  }
}
