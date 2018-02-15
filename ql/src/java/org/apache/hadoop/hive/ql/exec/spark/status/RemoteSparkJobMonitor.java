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

import java.util.Arrays;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.spark.status.impl.RemoteSparkJobStatus;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.spark.client.JobHandle;
import org.apache.spark.JobExecutionStatus;

/**
 * RemoteSparkJobMonitor monitor a RSC remote job status in a loop until job finished/failed/killed.
 * It print current job status to console and sleep current thread between monitor interval.
 */
public class RemoteSparkJobMonitor extends SparkJobMonitor {
  private int sparkJobMaxTaskCount = -1;
  private int sparkStageMaxTaskCount = -1;
  private int totalTaskCount = 0;
  private int stageMaxTaskCount = 0;
  private RemoteSparkJobStatus sparkJobStatus;
  private final HiveConf hiveConf;

  public RemoteSparkJobMonitor(HiveConf hiveConf, RemoteSparkJobStatus sparkJobStatus) {
    super(hiveConf);
    this.sparkJobStatus = sparkJobStatus;
    this.hiveConf = hiveConf;
    sparkJobMaxTaskCount = hiveConf.getIntVar(HiveConf.ConfVars.SPARK_JOB_MAX_TASKS);
    sparkStageMaxTaskCount = hiveConf.getIntVar(HiveConf.ConfVars.SPARK_STAGE_MAX_TASKS);
  }

  @Override
  public int startMonitor() {
    boolean running = false;
    boolean done = false;
    int rc = 0;
    Map<String, SparkStageProgress> lastProgressMap = null;

    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.SPARK_RUN_JOB);
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.SPARK_SUBMIT_TO_RUNNING);

    startTime = System.currentTimeMillis();
    JobHandle.State state = null;

    while (true) {
      try {
        state = sparkJobStatus.getRemoteJobState();
        Preconditions.checkState(sparkJobStatus.isRemoteActive(), "Connection to remote Spark driver was lost");

        switch (state) {
        case SENT:
        case QUEUED:
          long timeCount = (System.currentTimeMillis() - startTime) / 1000;
          if ((timeCount > monitorTimeoutInterval)) {
            HiveException he = new HiveException(ErrorMsg.SPARK_JOB_MONITOR_TIMEOUT,
                Long.toString(timeCount));
            console.printError(he.getMessage());
            sparkJobStatus.setError(he);
            running = false;
            done = true;
            rc = 2;
          }
          if (LOG.isDebugEnabled()) {
            console.printInfo("Spark job state = " + state );
          }
          break;
        case STARTED:
          JobExecutionStatus sparkJobState = sparkJobStatus.getState();
          if (sparkJobState == JobExecutionStatus.RUNNING) {
            Map<String, SparkStageProgress> progressMap = sparkJobStatus.getSparkStageProgress();
            if (!running) {
              perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.SPARK_SUBMIT_TO_RUNNING);
              printAppInfo();
              console.printInfo("Hive on Spark Session Web UI URL: " + sparkJobStatus.getWebUIURL());
              // print job stages.
              console.printInfo("\nQuery Hive on Spark job[" + sparkJobStatus.getJobId() +
                  "] stages: " + Arrays.toString(sparkJobStatus.getStageIds()));

              console.printInfo("Spark job[" + sparkJobStatus.getJobId() + "] status = RUNNING");
              running = true;

              String format = "Job Progress Format\nCurrentTime StageId_StageAttemptId: "
                  + "SucceededTasksCount(+RunningTasksCount-FailedTasksCount)/TotalTasksCount";
              if (!inPlaceUpdate) {
                console.printInfo(format);
              } else {
                console.logInfo(format);
              }
            } else {
              // Get the maximum of the number of tasks in the stages of the job and cancel the job if it goes beyond the limit.
              if (sparkStageMaxTaskCount != -1 && stageMaxTaskCount == 0) {
                stageMaxTaskCount = getStageMaxTaskCount(progressMap);
                if (stageMaxTaskCount > sparkStageMaxTaskCount) {
                  rc = 4;
                  done = true;
                  console.printInfo("\nThe number of task in one stage of the Spark job [" + stageMaxTaskCount + "] is greater than the limit [" +
                      sparkStageMaxTaskCount + "]. The Spark job will be cancelled.");
                }
              }

              // Count the number of tasks, and kill application if it goes beyond the limit.
              if (sparkJobMaxTaskCount != -1 && totalTaskCount == 0) {
                totalTaskCount = getTotalTaskCount(progressMap);
                if (totalTaskCount > sparkJobMaxTaskCount) {
                  rc = 4;
                  done = true;
                  console.printInfo("\nThe total number of task in the Spark job [" + totalTaskCount + "] is greater than the limit [" +
                      sparkJobMaxTaskCount + "]. The Spark job will be cancelled.");
                }
              }
            }

            printStatus(progressMap, lastProgressMap);
            lastProgressMap = progressMap;
          }
          break;
        case SUCCEEDED:
          Map<String, SparkStageProgress> progressMap = sparkJobStatus.getSparkStageProgress();
          printStatus(progressMap, lastProgressMap);
          lastProgressMap = progressMap;
          double duration = (System.currentTimeMillis() - startTime) / 1000.0;
          console.printInfo("Spark job[" + sparkJobStatus.getJobId() + "] finished successfully in "
            + String.format("%.2f second(s)", duration));
          running = false;
          done = true;
          break;
        case FAILED:
          String detail = sparkJobStatus.getError().getMessage();
          StringBuilder errBuilder = new StringBuilder();
          errBuilder.append("Job failed with ");
          if (detail == null) {
            errBuilder.append("UNKNOWN reason");
          } else {
            // We SerDe the Throwable as String, parse it for the root cause
            final String CAUSE_CAPTION = "Caused by: ";
            int index = detail.lastIndexOf(CAUSE_CAPTION);
            if (index != -1) {
              String rootCause = detail.substring(index + CAUSE_CAPTION.length());
              index = rootCause.indexOf(System.getProperty("line.separator"));
              if (index != -1) {
                errBuilder.append(rootCause.substring(0, index));
              } else {
                errBuilder.append(rootCause);
              }
            } else {
              errBuilder.append(detail);
            }
            detail = System.getProperty("line.separator") + detail;
          }
          console.printError(errBuilder.toString(), detail);
          running = false;
          done = true;
          rc = 3;
          break;
        case CANCELLED:
          console.printInfo("Spark job[" + sparkJobStatus.getJobId() + " was cancelled");
          running = false;
          done = true;
          rc = 3;
          break;
        }

        if (!done) {
          Thread.sleep(checkInterval);
        }
      } catch (Exception e) {
        Exception finalException = e;
        if (e instanceof InterruptedException ||
                (e instanceof HiveException && e.getCause() instanceof InterruptedException)) {
          finalException = new HiveException(e, ErrorMsg.SPARK_JOB_INTERRUPTED);
          LOG.warn("Interrupted while monitoring the Hive on Spark application, exiting");
        } else {
          String msg = " with exception '" + Utilities.getNameMessage(e) + "' Last known state = " +
                  (state != null ? state.name() : "UNKNOWN");
          msg = "Failed to monitor Job[" + sparkJobStatus.getJobId() + "]" + msg;

          // Has to use full name to make sure it does not conflict with
          // org.apache.commons.lang.StringUtils
          LOG.error(msg, e);
          console.printError(msg, "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
        }
        rc = 1;
        done = true;
        sparkJobStatus.setError(finalException);
      } finally {
        if (done) {
          break;
        }
      }
    }

    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.SPARK_RUN_JOB);
    return rc;
  }

  private void printAppInfo() {
    String sparkMaster = hiveConf.get("spark.master");
    if (sparkMaster != null && sparkMaster.startsWith("yarn")) {
      String appID = sparkJobStatus.getAppID();
      if (appID != null) {
        console.printInfo("Running with YARN Application = " + appID);
        console.printInfo("Kill Command = " +
            HiveConf.getVar(hiveConf, HiveConf.ConfVars.YARNBIN) + " application -kill " + appID);
      }
    }
  }
}
