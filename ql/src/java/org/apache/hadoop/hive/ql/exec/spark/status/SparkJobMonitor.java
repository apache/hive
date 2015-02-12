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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.spark.status.impl.LocalSparkJobStatus;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState;

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
  protected static final Log LOG = LogFactory.getLog(CLASS_NAME);
  protected static SessionState.LogHelper console = new SessionState.LogHelper(LOG);
  protected final PerfLogger perfLogger = PerfLogger.getPerfLogger();
  protected final int checkInterval = 1000;
  protected final long monitorTimeoutInteval;

  private Set<String> completed = new HashSet<String>();
  private final int printInterval = 3000;
  private long lastPrintTime;

  protected SparkJobMonitor(HiveConf hiveConf) {
    monitorTimeoutInteval = hiveConf.getTimeVar(HiveConf.ConfVars.SPARK_JOB_MONITOR_TIMEOUT, TimeUnit.SECONDS);
  }

  public abstract int startMonitor();

  protected void printStatus(Map<String, SparkStageProgress> progressMap,
    Map<String, SparkStageProgress> lastProgressMap) {

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

          if (!perfLogger.startTimeHasMethod(PerfLogger.SPARK_RUN_STAGE + s)) {
            perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.SPARK_RUN_STAGE);
          }
          perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.SPARK_RUN_STAGE);
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
      return lastProgressMap.isEmpty();
    } else {
      if (lastProgressMap.isEmpty()) {
        return false;
      } else {
        if (progressMap.size() != lastProgressMap.size()) {
          return false;
        }
        for (String key : progressMap.keySet()) {
          if (!lastProgressMap.containsKey(key)
            || !progressMap.get(key).equals(lastProgressMap.get(key))) {
            return false;
          }
        }
      }
    }
    return true;
  }
}
