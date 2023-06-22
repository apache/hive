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
package org.apache.hadoop.hive.ql.exec.tez.monitoring;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.apache.hadoop.hive.common.log.InPlaceUpdate;
import org.apache.hadoop.hive.common.log.ProgressMonitor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

class RenderStrategy {

  interface UpdateFunction {
    void update(DAGStatus status, Map<String, Progress> vertexProgressMap);
  }

  private abstract static class BaseUpdateFunction implements UpdateFunction {
    private final long print_interval;

    final TezJobMonitor monitor;
    private final PerfLogger perfLogger;

    private long lastPrintTime = 0L;
    private String lastReport = null;

    BaseUpdateFunction(TezJobMonitor monitor) {
      this.monitor = monitor;
      print_interval = HiveConf.getTimeVar(
          monitor.getHiveConf(),
          HiveConf.ConfVars.HIVE_LOG_INCREMENTAL_PLAN_PROGRESS_INTERVAL,
          TimeUnit.MILLISECONDS
      );
      perfLogger = SessionState.getPerfLogger();
    }

    @Override
    public void update(DAGStatus status, Map<String, Progress> vertexProgressMap) {
      renderProgress(monitor.progressMonitor(status, vertexProgressMap));
      String report = getReport(vertexProgressMap);
      if (showReport(report)) {
        renderReport(report);
        lastReport = report;
        lastPrintTime = System.currentTimeMillis();
      }
    }

    private boolean showReport(String report) {
      return !report.equals(lastReport)
          || System.currentTimeMillis() >= lastPrintTime + print_interval;
    }

    /*
       This is used to print the progress information as pure text , a sample is as below:
          Map 1: 0/1	Reducer 2: 0/1
          Map 1: 0(+1)/1	Reducer 2: 0/1
          Map 1: 1/1	Reducer 2: 0(+1)/1
          Map 1: 1/1	Reducer 2: 1/1
     */
    private String getReport(Map<String, Progress> progressMap) {
      StringWriter reportBuffer = new StringWriter();

      SortedSet<String> keys = new TreeSet<>(progressMap.keySet());
      for (String s : keys) {
        Progress progress = progressMap.get(s);
        final int complete = progress.getSucceededTaskCount();
        final int total = progress.getTotalTaskCount();
        final int running = progress.getRunningTaskCount();
        final int failed = progress.getFailedTaskAttemptCount();
        if (total <= 0) {
          reportBuffer.append(String.format("%s: -/-\t", s));
        } else {
          if (complete == total) {
          /*
           * We may have missed the start of the vertex due to the 3 seconds interval
           */
            if (!perfLogger.startTimeHasMethod(PerfLogger.TEZ_RUN_VERTEX + s)) {
              perfLogger.perfLogBegin(TezJobMonitor.CLASS_NAME, PerfLogger.TEZ_RUN_VERTEX + s);
            }

            if (!perfLogger.endTimeHasMethod(PerfLogger.TEZ_RUN_VERTEX + s)) {
              perfLogger.perfLogEnd(TezJobMonitor.CLASS_NAME, PerfLogger.TEZ_RUN_VERTEX + s);
            }
          }
          if (complete < total && (complete > 0 || running > 0 || failed > 0)) {

            if (!perfLogger.startTimeHasMethod(PerfLogger.TEZ_RUN_VERTEX + s)) {
              perfLogger.perfLogBegin(TezJobMonitor.CLASS_NAME, PerfLogger.TEZ_RUN_VERTEX + s);
            }

          /* vertex is started, but not complete */
            if (failed > 0) {
              reportBuffer.append(
                  String.format("%s: %d(+%d,-%d)/%d\t", s, complete, running, failed, total));
            } else {
              reportBuffer.append(String.format("%s: %d(+%d)/%d\t", s, complete, running, total));
            }
          } else {
          /* vertex is waiting for input/slots or complete */
            if (failed > 0) {
            /* tasks finished but some failed */
              reportBuffer.append(String.format("%s: %d(-%d)/%d\t", s, complete, failed, total));
            } else {
              reportBuffer.append(String.format("%s: %d/%d\t", s, complete, total));
            }
          }
        }
      }

      return reportBuffer.toString();
    }

    abstract void renderProgress(ProgressMonitor progressMonitor);

    abstract void renderReport(String report);
  }

  /**
   * this adds the required progress update to the session state that is used by HS2 to send the
   * same information to beeline client when requested.
   */
  static class LogToFileFunction extends BaseUpdateFunction {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogToFileFunction.class);
    private static final DateTimeFormatter REPORT_DATE_TIME_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");

    private final boolean hiveServer2InPlaceProgressEnabled =
        SessionState.get().getConf().getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_INPLACE_PROGRESS);
    private final ZoneId localTimeZone = SessionState.get().getConf().getLocalTimeZone();

    LogToFileFunction(TezJobMonitor monitor) {
      super(monitor);
    }

    @Override
    public void renderProgress(ProgressMonitor progressMonitor) {
      SessionState.get().updateProgressMonitor(progressMonitor);
    }

    @Override
    public void renderReport(String report) {
      if (hiveServer2InPlaceProgressEnabled) {
        LOGGER.info(report);
      } else {
        final String time = REPORT_DATE_TIME_FORMATTER.format(LocalDateTime.now(localTimeZone));
        monitor.console.printInfo(time + "\t" + report);
      }
    }
  }

  /**
   * This used when we want the progress update to printed in the same process typically used via
   * hive-cli mode.
   */
  static class InPlaceUpdateFunction extends BaseUpdateFunction {
    /**
     * Have to use the same instance to render else the number lines printed earlier is lost and the
     * screen will print the table again and again.
     */
    private final InPlaceUpdate inPlaceUpdate;

    InPlaceUpdateFunction(TezJobMonitor monitor) {
      super(monitor);
      inPlaceUpdate = new InPlaceUpdate(SessionState.LogHelper.getInfoStream());
    }

    @Override
    public void renderProgress(ProgressMonitor progressMonitor) {
      inPlaceUpdate.render(progressMonitor);
    }

    @Override
    public void renderReport(String report) {
      monitor.console.logInfo(report);
    }
  }
}
