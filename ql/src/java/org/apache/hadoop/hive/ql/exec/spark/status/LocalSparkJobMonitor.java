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

import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.spark.JobExecutionStatus;

/**
 * LocalSparkJobMonitor monitor a single Spark job status in a loop until job finished/failed/killed.
 * It print current job status to console and sleep current thread between monitor interval.
 */
public class LocalSparkJobMonitor extends SparkJobMonitor {

  private SparkJobStatus sparkJobStatus;

  public LocalSparkJobMonitor(HiveConf hiveConf, SparkJobStatus sparkJobStatus) {
    super(hiveConf);
    this.sparkJobStatus = sparkJobStatus;
  }

  public int startMonitor() {
    boolean running = false;
    boolean done = false;
    int rc = 0;
    JobExecutionStatus lastState = null;
    Map<String, SparkStageProgress> lastProgressMap = null;

    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.SPARK_RUN_JOB);
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.SPARK_SUBMIT_TO_RUNNING);

    startTime = System.currentTimeMillis();

    while (true) {
      try {
        JobExecutionStatus state = sparkJobStatus.getState();
        if (LOG.isDebugEnabled()) {
          console.printInfo("state = " + state);
        }

        if (state == null) {
          long timeCount = (System.currentTimeMillis() - startTime)/1000;
          if (timeCount > monitorTimeoutInterval) {
            console.printError("Job hasn't been submitted after " + timeCount + "s. Aborting it.");
            console.printError("Status: " + state);
            running = false;
            done = true;
            rc = 2;
            break;
          }
        } else if (state != lastState || state == JobExecutionStatus.RUNNING) {
          lastState = state;
          Map<String, SparkStageProgress> progressMap = sparkJobStatus.getSparkStageProgress();

          switch (state) {
          case RUNNING:
            if (!running) {
              perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.SPARK_SUBMIT_TO_RUNNING);
              // print job stages.
              console.printInfo("\nQuery Hive on Spark job["
                + sparkJobStatus.getJobId() + "] stages:");
              for (int stageId : sparkJobStatus.getStageIds()) {
                console.printInfo(Integer.toString(stageId));
              }

              console.printInfo("\nStatus: Running (Hive on Spark job["
                + sparkJobStatus.getJobId() + "])");
              running = true;

              console.printInfo("Job Progress Format\nCurrentTime StageId_StageAttemptId: "
                + "SucceededTasksCount(+RunningTasksCount-FailedTasksCount)/TotalTasksCount [StageCost]");
            }

            printStatus(progressMap, lastProgressMap);
            lastProgressMap = progressMap;
            break;
          case SUCCEEDED:
            printStatus(progressMap, lastProgressMap);
            lastProgressMap = progressMap;
            double duration = (System.currentTimeMillis() - startTime) / 1000.0;
            console.printInfo("Status: Finished successfully in "
              + String.format("%.2f seconds", duration));
            running = false;
            done = true;
            break;
          case FAILED:
            console.printError("Status: Failed");
            running = false;
            done = true;
            rc = 3;
            break;
          case UNKNOWN:
            console.printError("Status: Unknown");
            running = false;
            done = true;
            rc = 4;
            break;
          }
        }
        if (!done) {
          Thread.sleep(checkInterval);
        }
      } catch (Exception e) {
        String msg = " with exception '" + Utilities.getNameMessage(e) + "'";
        msg = "Failed to monitor Job[ " + sparkJobStatus.getJobId() + "]" + msg;

        // Has to use full name to make sure it does not conflict with
        // org.apache.commons.lang.StringUtils
        LOG.error(msg, e);
        console.printError(msg, "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
        rc = 1;
        done = true;
        sparkJobStatus.setError(e);
      } finally {
        if (done) {
          break;
        }
      }
    }

    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.SPARK_RUN_JOB);
    return rc;
  }
}
