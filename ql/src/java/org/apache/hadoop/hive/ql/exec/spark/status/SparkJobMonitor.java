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
import org.fusesource.jansi.Ansi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.fusesource.jansi.Ansi.ansi;

abstract class SparkJobMonitor {

  protected static final String CLASS_NAME = SparkJobMonitor.class.getName();
  protected static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  protected transient final SessionState.LogHelper console;
  protected final PerfLogger perfLogger = SessionState.getPerfLogger();
  protected final int checkInterval = 1000;
  protected final long monitorTimeoutInterval;

  private final Set<String> completed = new HashSet<String>();
  private final int printInterval = 3000;
  private long lastPrintTime;

  protected long startTime;

  protected enum StageState {
    PENDING,
    RUNNING,
    FINISHED
  }

  // in-place progress update related variables
  protected final boolean inPlaceUpdate;
  private int lines = 0;
  private final PrintStream out;

  private static final int COLUMN_1_WIDTH = 16;
  private static final String HEADER_FORMAT = "%16s%10s %13s  %5s  %9s  %7s  %7s  %6s  ";
  private static final String STAGE_FORMAT = "%-16s%10s %13s  %5s  %9s  %7s  %7s  %6s  ";
  private static final String HEADER = String.format(HEADER_FORMAT,
      "STAGES", "ATTEMPT", "STATUS", "TOTAL", "COMPLETED", "RUNNING", "PENDING", "FAILED");
  private static final int SEPARATOR_WIDTH = 86;
  private static final String SEPARATOR = new String(new char[SEPARATOR_WIDTH]).replace("\0", "-");
  private static final String FOOTER_FORMAT = "%-15s  %-30s %-4s  %-25s";
  private static final int progressBarChars = 30;

  private final NumberFormat secondsFormat = new DecimalFormat("#0.00");

  protected SparkJobMonitor(HiveConf hiveConf) {
    monitorTimeoutInterval = hiveConf.getTimeVar(
        HiveConf.ConfVars.SPARK_JOB_MONITOR_TIMEOUT, TimeUnit.SECONDS);
    inPlaceUpdate = InPlaceUpdate.canRenderInPlace(hiveConf) && !SessionState.getConsole().getIsSilent();
    console = new SessionState.LogHelper(LOG);
    out = SessionState.LogHelper.getInfoStream();
  }

  public abstract int startMonitor();

  private void printStatusInPlace(Map<String, SparkStageProgress> progressMap) {

    StringBuilder reportBuffer = new StringBuilder();

    // Num of total and completed tasks
    int sumTotal = 0;
    int sumComplete = 0;

    // position the cursor to line 0
    repositionCursor();

    // header
    reprintLine(SEPARATOR);
    reprintLineWithColorAsBold(HEADER, Ansi.Color.CYAN);
    reprintLine(SEPARATOR);

    SortedSet<String> keys = new TreeSet<String>(progressMap.keySet());
    int idx = 0;
    final int numKey = keys.size();
    for (String s : keys) {
      SparkStageProgress progress = progressMap.get(s);
      final int complete = progress.getSucceededTaskCount();
      final int total = progress.getTotalTaskCount();
      final int running = progress.getRunningTaskCount();
      final int failed = progress.getFailedTaskCount();
      sumTotal += total;
      sumComplete += complete;

      StageState state = total > 0 ? StageState.PENDING : StageState.FINISHED;
      if (complete > 0 || running > 0 || failed > 0) {
        if (!perfLogger.startTimeHasMethod(PerfLogger.SPARK_RUN_STAGE + s)) {
          perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.SPARK_RUN_STAGE + s);
        }
        if (complete < total) {
          state = StageState.RUNNING;
        } else {
          state = StageState.FINISHED;
          perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.SPARK_RUN_STAGE + s);
          completed.add(s);
        }
      }

      int div = s.indexOf('_');
      String attempt = div > 0 ? s.substring(div + 1) : "-";
      String stageName = "Stage-" + (div > 0 ? s.substring(0, div) : s);
      String nameWithProgress = getNameWithProgress(stageName, complete, total);

      final int pending = total - complete - running;
      String stageStr = String.format(STAGE_FORMAT,
          nameWithProgress, attempt, state, total, complete, running, pending, failed);
      reportBuffer.append(stageStr);
      if (idx++ != numKey - 1) {
        reportBuffer.append("\n");
      }
    }
    reprintMultiLine(reportBuffer.toString());
    reprintLine(SEPARATOR);
    final float progress = (sumTotal == 0) ? 1.0f : (float) sumComplete / (float) sumTotal;
    String footer = getFooter(numKey, completed.size(), progress, startTime);
    reprintLineWithColorAsBold(footer, Ansi.Color.RED);
    reprintLine(SEPARATOR);
  }

  protected void printStatus(Map<String, SparkStageProgress> progressMap,
      Map<String, SparkStageProgress> lastProgressMap) {

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

  protected int getTotalTaskCount(Map<String, SparkStageProgress> progressMap) {
    int totalTasks = 0;
    for (SparkStageProgress progress: progressMap.values() ) {
      totalTasks += progress.getTotalTaskCount();
    }

    return totalTasks;
  }

  protected int getStageMaxTaskCount(Map<String, SparkStageProgress> progressMap) {
    int stageMaxTasks = 0;
    for (SparkStageProgress progress: progressMap.values() ) {
      int tasks = progress.getTotalTaskCount();
      if (tasks > stageMaxTasks) {
        stageMaxTasks = tasks;
      }
    }

    return stageMaxTasks;
  }

  private String getReport(Map<String, SparkStageProgress> progressMap) {
    StringBuilder reportBuffer = new StringBuilder();
    SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
    String currentDate = dt.format(new Date());
    reportBuffer.append(currentDate + "\t");

    // Num of total and completed tasks
    int sumTotal = 0;
    int sumComplete = 0;

    SortedSet<String> keys = new TreeSet<String>(progressMap.keySet());
    for (String s : keys) {
      SparkStageProgress progress = progressMap.get(s);
      final int complete = progress.getSucceededTaskCount();
      final int total = progress.getTotalTaskCount();
      final int running = progress.getRunningTaskCount();
      final int failed = progress.getFailedTaskCount();
      sumTotal += total;
      sumComplete += complete;

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

  private void repositionCursor() {
    if (lines > 0) {
      out.print(ansi().cursorUp(lines).toString());
      out.flush();
      lines = 0;
    }
  }

  private void reprintLine(String line) {
    InPlaceUpdate.reprintLine(out, line);
    lines++;
  }

  private void reprintLineWithColorAsBold(String line, Ansi.Color color) {
    out.print(ansi().eraseLine(Ansi.Erase.ALL).fg(color).bold().a(line).a('\n').boldOff().reset()
        .toString());
    out.flush();
    lines++;
  }

  private String getNameWithProgress(String s, int complete, int total) {
    String result = "";
    if (s != null) {
      float percent = total == 0 ? 1.0f : (float) complete / (float) total;
      // lets use the remaining space in column 1 as progress bar
      int spaceRemaining = COLUMN_1_WIDTH - s.length() - 1;
      String trimmedVName = s;

      // if the vertex name is longer than column 1 width, trim it down
      if (s.length() > COLUMN_1_WIDTH) {
        trimmedVName = s.substring(0, COLUMN_1_WIDTH - 2);
        result = trimmedVName + "..";
      } else {
        result = trimmedVName + " ";
      }

      int toFill = (int) (spaceRemaining * percent);
      for (int i = 0; i < toFill; i++) {
        result += ".";
      }
    }
    return result;
  }

  // STAGES: 03/04            [==================>>-----] 86%  ELAPSED TIME: 1.71 s
  private String getFooter(int keySize, int completedSize, float progress, long startTime) {
    String verticesSummary = String.format("STAGES: %02d/%02d", completedSize, keySize);
    String progressBar = getInPlaceProgressBar(progress);
    final int progressPercent = (int) (progress * 100);
    String progressStr = "" + progressPercent + "%";
    float et = (float) (System.currentTimeMillis() - startTime) / (float) 1000;
    String elapsedTime = "ELAPSED TIME: " + secondsFormat.format(et) + " s";
    String footer = String.format(FOOTER_FORMAT,
        verticesSummary, progressBar, progressStr, elapsedTime);
    return footer;
  }

  // [==================>>-----]
  private String getInPlaceProgressBar(float percent) {
    StringBuilder bar = new StringBuilder("[");
    int remainingChars = progressBarChars - 4;
    int completed = (int) (remainingChars * percent);
    int pending = remainingChars - completed;
    for (int i = 0; i < completed; i++) {
      bar.append("=");
    }
    bar.append(">>");
    for (int i = 0; i < pending; i++) {
      bar.append("-");
    }
    bar.append("]");
    return bar.toString();
  }

  private void reprintMultiLine(String line) {
    int numLines = line.split("\r\n|\r|\n").length;
    out.print(ansi().eraseLine(Ansi.Erase.ALL).a(line).a('\n').toString());
    out.flush();
    lines += numLines;
  }
}
