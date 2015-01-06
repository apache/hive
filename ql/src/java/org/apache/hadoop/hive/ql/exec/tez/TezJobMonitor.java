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

package org.apache.hadoop.hive.ql.exec.tez;

import static org.apache.tez.dag.api.client.DAGStatus.State.RUNNING;
import static org.fusesource.jansi.Ansi.ansi;
import static org.fusesource.jansi.internal.CLibrary.STDOUT_FILENO;
import static org.fusesource.jansi.internal.CLibrary.isatty;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Heartbeater;
import org.apache.hadoop.hive.ql.exec.MapOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;
import org.fusesource.jansi.Ansi;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import jline.TerminalFactory;

/**
 * TezJobMonitor keeps track of a tez job while it's being executed. It will
 * print status to the console and retrieve final status of the job after
 * completion.
 */
public class TezJobMonitor {

  private static final String CLASS_NAME = TezJobMonitor.class.getName();
  private static final int MIN_TERMINAL_WIDTH = 80;
  private static final int COLUMN_1_WIDTH = 16;
  private static final int SEPARATOR_WIDTH = 80;

  // keep this within 80 chars width. If more columns needs to be added then update min terminal
  // width requirement and separator width accordingly
  private static final String HEADER_FORMAT = "%16s%12s  %5s  %9s  %7s  %7s  %6s  %6s";
  private static final String VERTEX_FORMAT = "%-16s%12s  %5s  %9s  %7s  %7s  %6s  %6s";
  private static final String FOOTER_FORMAT = "%-15s  %-30s %-4s  %-25s";
  private static final String HEADER = String.format(HEADER_FORMAT,
      "VERTICES", "STATUS", "TOTAL", "COMPLETED", "RUNNING", "PENDING", "FAILED", "KILLED");

  // method and dag summary format
  private static final String SUMMARY_HEADER_FORMAT = "%-16s %-12s %-12s %-12s %-19s %-19s %-15s %-15s %-15s";
  private static final String SUMMARY_VERTEX_FORMAT = "%-16s %11s %16s %12s %16s %18s %18s %14s %16s";
  private static final String SUMMARY_HEADER = String.format(SUMMARY_HEADER_FORMAT,
      "VERTICES", "TOTAL_TASKS", "FAILED_ATTEMPTS", "KILLED_TASKS", "DURATION_SECONDS",
      "CPU_TIME_MILLIS", "GC_TIME_MILLIS", "INPUT_RECORDS", "OUTPUT_RECORDS");

  private static final String TOTAL_PREP_TIME = "TotalPrepTime";
  private static final String METHOD = "METHOD";
  private static final String DURATION = "DURATION(ms)";

  // in-place progress update related variables
  private int lines;
  private PrintStream out;
  private String separator;

  private transient LogHelper console;
  private final PerfLogger perfLogger = PerfLogger.getPerfLogger();
  private final int checkInterval = 200;
  private final int maxRetryInterval = 2500;
  private final int printInterval = 3000;
  private final int progressBarChars = 30;
  private long lastPrintTime;
  private Set<String> completed;

  /* Pretty print the values */
  private final NumberFormat secondsFormat;
  private final NumberFormat commaFormat;
  private static final List<DAGClient> shutdownList;

  static {
    shutdownList = Collections.synchronizedList(new LinkedList<DAGClient>());
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        for (DAGClient c: shutdownList) {
          TezJobMonitor.killRunningJobs();
        }
        try {
          for (TezSessionState s: TezSessionState.getOpenSessions()) {
            System.err.println("Shutting down tez session.");
            TezSessionPoolManager.getInstance().close(s, false);
          }
        } catch (Exception e) {
          // ignore
        }
      }
    });
  }

  public static void initShutdownHook() {
    Preconditions.checkNotNull(shutdownList,
        "Shutdown hook was not properly initialized");
  }

  public TezJobMonitor() {
    console = SessionState.getConsole();
    secondsFormat = new DecimalFormat("#0.00");
    commaFormat = NumberFormat.getNumberInstance(Locale.US);
    // all progress updates are written to info stream and log file. In-place updates can only be
    // done to info stream (console)
    out = console.getInfoStream();
    separator = "";
    for (int i = 0; i < SEPARATOR_WIDTH; i++) {
      separator += "-";
    }
  }

  private static boolean isUnixTerminal() {

    String os = System.getProperty("os.name");
    if (os.startsWith("Windows")) {
      // we do not support Windows, we will revisit this if we really need it for windows.
      return false;
    }

    // We must be on some unix variant..
    // check if standard out is a terminal
    try {
      // isatty system call will return 1 if the file descriptor is terminal else 0
      if (isatty(STDOUT_FILENO) == 0) {
        return false;
      }
    } catch (NoClassDefFoundError ignore) {
      // These errors happen if the JNI lib is not available for your platform.
      return false;
    } catch (UnsatisfiedLinkError ignore) {
      // These errors happen if the JNI lib is not available for your platform.
      return false;
    }
    return true;
  }

  /**
   * NOTE: Use this method only if isUnixTerminal is true.
   * Erases the current line and prints the given line.
   * @param line - line to print
   */
  public void reprintLine(String line) {
    out.print(ansi().eraseLine(Ansi.Erase.ALL).a(line).a('\n').toString());
    out.flush();
    lines++;
  }

  /**
   * NOTE: Use this method only if isUnixTerminal is true.
   * Erases the current line and prints the given line with the specified color.
   * @param line - line to print
   * @param color - color for the line
   */
  public void reprintLineWithColorAsBold(String line, Ansi.Color color) {
    out.print(ansi().eraseLine(Ansi.Erase.ALL).fg(color).bold().a(line).a('\n').boldOff().reset()
        .toString());
    out.flush();
    lines++;
  }

  /**
   * NOTE: Use this method only if isUnixTerminal is true.
   * Erases the current line and prints the given multiline. Make sure the specified line is not
   * terminated by linebreak.
   * @param line - line to print
   */
  public void reprintMultiLine(String line) {
    int numLines = line.split("\r\n|\r|\n").length;
    out.print(ansi().eraseLine(Ansi.Erase.ALL).a(line).a('\n').toString());
    out.flush();
    lines += numLines;
  }

  /**
   * NOTE: Use this method only if isUnixTerminal is true.
   * Repositions the cursor back to line 0.
   */
  public void repositionCursor() {
    if (lines > 0) {
      out.print(ansi().cursorUp(lines).toString());
      out.flush();
      lines = 0;
    }
  }

  /**
   * NOTE: Use this method only if isUnixTerminal is true.
   * Gets the width of the terminal
   * @return - width of terminal
   */
  public int getTerminalWidth() {
    return TerminalFactory.get().getWidth();
  }

  /**
   * monitorExecution handles status printing, failures during execution and final status retrieval.
   *
   * @param dagClient client that was used to kick off the job
   * @param txnMgr transaction manager for this operation
   * @param conf configuration file for this operation
   * @return int 0 - success, 1 - killed, 2 - failed
   */
  public int monitorExecution(final DAGClient dagClient, HiveTxnManager txnMgr, HiveConf conf,
      DAG dag) throws InterruptedException {
    DAGStatus status = null;
    completed = new HashSet<String>();

    boolean running = false;
    boolean done = false;
    int failedCounter = 0;
    int rc = 0;
    DAGStatus.State lastState = null;
    String lastReport = null;
    Set<StatusGetOpts> opts = new HashSet<StatusGetOpts>();
    Heartbeater heartbeater = new Heartbeater(txnMgr, conf);
    long startTime = 0;
    boolean isProfileEnabled = conf.getBoolVar(conf, HiveConf.ConfVars.TEZ_EXEC_SUMMARY);
    boolean inPlaceUpdates = conf.getBoolVar(conf, HiveConf.ConfVars.TEZ_EXEC_INPLACE_PROGRESS);
    boolean wideTerminal = false;
    boolean isTerminal = inPlaceUpdates == true ? isUnixTerminal() : false;

    // we need at least 80 chars wide terminal to display in-place updates properly
    if (isTerminal) {
      if (getTerminalWidth() >= MIN_TERMINAL_WIDTH) {
        wideTerminal = true;
      }
    }

    boolean inPlaceEligible = false;
    if (inPlaceUpdates && isTerminal && wideTerminal && !console.getIsSilent()) {
      inPlaceEligible = true;
    }

    shutdownList.add(dagClient);

    console.printInfo("\n");
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_RUN_DAG);
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_SUBMIT_TO_RUNNING);

    while (true) {

      try {
        status = dagClient.getDAGStatus(opts);
        Map<String, Progress> progressMap = status.getVertexProgress();
        DAGStatus.State state = status.getState();
        heartbeater.heartbeat();

        if (state != lastState || state == RUNNING) {
          lastState = state;

          switch (state) {
          case SUBMITTED:
            console.printInfo("Status: Submitted");
            break;
          case INITING:
            console.printInfo("Status: Initializing");
            startTime = System.currentTimeMillis();
            break;
          case RUNNING:
            if (!running) {
              perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_SUBMIT_TO_RUNNING);
              console.printInfo("Status: Running (" + dagClient.getExecutionContext() + ")\n");
              startTime = System.currentTimeMillis();
              running = true;
            }

            if (inPlaceEligible) {
              printStatusInPlace(progressMap, startTime, false, dagClient);
              // log the progress report to log file as well
              lastReport = logStatus(progressMap, lastReport, console);
            } else {
              lastReport = printStatus(progressMap, lastReport, console);
            }
            break;
          case SUCCEEDED:
            if (inPlaceEligible) {
              printStatusInPlace(progressMap, startTime, false, dagClient);
              // log the progress report to log file as well
              lastReport = logStatus(progressMap, lastReport, console);
            } else {
              lastReport = printStatus(progressMap, lastReport, console);
            }

            /* Profile info is collected anyways, isProfileEnabled
             * decides if it gets printed or not
             */
            if (isProfileEnabled) {

              double duration = (System.currentTimeMillis() - startTime) / 1000.0;
              console.printInfo("Status: DAG finished successfully in "
                  + String.format("%.2f seconds", duration));
              console.printInfo("\n");

              printMethodsSummary();
              printDagSummary(progressMap, console, dagClient, conf, dag);
            }
            running = false;
            done = true;
            break;
          case KILLED:
            if (inPlaceEligible) {
              printStatusInPlace(progressMap, startTime, true, dagClient);
              // log the progress report to log file as well
              lastReport = logStatus(progressMap, lastReport, console);
            }
            console.printInfo("Status: Killed");
            running = false;
            done = true;
            rc = 1;
            break;
          case FAILED:
          case ERROR:
            if (inPlaceEligible) {
              printStatusInPlace(progressMap, startTime, true, dagClient);
              // log the progress report to log file as well
              lastReport = logStatus(progressMap, lastReport, console);
            }
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
          try {
            console.printInfo("Killing DAG...");
            dagClient.tryKillDAG();
          } catch (IOException io) {
            // best effort
          } catch (TezException te) {
            // best effort
          }
          e.printStackTrace();
          console.printError("Execution has failed.");
          rc = 1;
          done = true;
        } else {
          console.printInfo("Retrying...");
        }
      } finally {
        if (done) {
          if (rc != 0 && status != null) {
            for (String diag : status.getDiagnostics()) {
              console.printError(diag);
            }
          }
          shutdownList.remove(dagClient);
          break;
        }
      }
    }
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_RUN_DAG);
    return rc;
  }

  /**
   * killRunningJobs tries to terminate execution of all
   * currently running tez queries. No guarantees, best effort only.
   */
  public static void killRunningJobs() {
    for (DAGClient c: shutdownList) {
      try {
        System.err.println("Trying to shutdown DAG");
        c.tryKillDAG();
      } catch (Exception e) {
        // ignore
      }
    }
  }

  private static long getCounterValueByGroupName(TezCounters vertexCounters,
      String groupNamePattern,
      String counterName) {
    TezCounter tezCounter = vertexCounters.getGroup(groupNamePattern).findCounter(counterName);
    return (tezCounter == null) ? 0 : tezCounter.getValue();
  }

  private void printMethodsSummary() {
    long totalInPrepTime = 0;

    String[] perfLoggerReportMethods = {
        (PerfLogger.PARSE),
        (PerfLogger.ANALYZE),
        (PerfLogger.TEZ_BUILD_DAG),
        (PerfLogger.TEZ_SUBMIT_TO_RUNNING)
    };

    /* Build the method summary header */
    String methodBreakdownHeader = String.format("%-30s %-13s", METHOD, DURATION);
    console.printInfo(methodBreakdownHeader);

    for (String method : perfLoggerReportMethods) {
      long duration = perfLogger.getDuration(method);
      totalInPrepTime += duration;
      console.printInfo(String.format("%-30s %11s", method, commaFormat.format(duration)));
    }

    /*
     * The counters list above don't capture the total time from TimeToSubmit.startTime till
     * TezRunDag.startTime, so calculate the duration and print it.
     */
    totalInPrepTime = perfLogger.getStartTime(PerfLogger.TEZ_RUN_DAG) -
        perfLogger.getStartTime(PerfLogger.TIME_TO_SUBMIT);

    console.printInfo(String.format("%-30s %11s\n", TOTAL_PREP_TIME, commaFormat.format(
        totalInPrepTime)));
  }

  private void printDagSummary(Map<String, Progress> progressMap, LogHelper console,
      DAGClient dagClient, HiveConf conf, DAG dag) {

    /* Strings for headers and counters */
    String hiveCountersGroup = conf.getVar(conf, HiveConf.ConfVars.HIVECOUNTERGROUP);
    Set<StatusGetOpts> statusGetOpts = EnumSet.of(StatusGetOpts.GET_COUNTERS);
    TezCounters hiveCounters = null;
    try {
      hiveCounters = dagClient.getDAGStatus(statusGetOpts).getDAGCounters();
    } catch (IOException e) {
      // best attempt, shouldn't really kill DAG for this
    } catch (TezException e) {
      // best attempt, shouldn't really kill DAG for this
    }

    /* If the counters are missing there is no point trying to print progress */
    if (hiveCounters == null) {
      return;
    }

    /* Print the per Vertex summary */
    console.printInfo(SUMMARY_HEADER);
    SortedSet<String> keys = new TreeSet<String>(progressMap.keySet());
    Set<StatusGetOpts> statusOptions = new HashSet<StatusGetOpts>(1);
    statusOptions.add(StatusGetOpts.GET_COUNTERS);
    for (String vertexName : keys) {
      Progress progress = progressMap.get(vertexName);
      if (progress != null) {
        final int totalTasks = progress.getTotalTaskCount();
        final int failedTaskAttempts = progress.getFailedTaskAttemptCount();
        final int killedTasks = progress.getKilledTaskCount();
        final double duration =
            perfLogger.getDuration(PerfLogger.TEZ_RUN_VERTEX + vertexName) / 1000.0;
        VertexStatus vertexStatus = null;
        try {
          vertexStatus = dagClient.getVertexStatus(vertexName, statusOptions);
        } catch (IOException e) {
          // best attempt, shouldn't really kill DAG for this
        } catch (TezException e) {
          // best attempt, shouldn't really kill DAG for this
        }

        if (vertexStatus == null) {
          continue;
        }

        Vertex currentVertex = dag.getVertex(vertexName);
        List<Vertex> inputVerticesList = currentVertex.getInputVertices();
        long hiveInputRecordsFromOtherVertices = 0;
        if (inputVerticesList.size() > 0) {

          for (Vertex inputVertex : inputVerticesList) {
            String inputVertexName = inputVertex.getName();
            hiveInputRecordsFromOtherVertices += getCounterValueByGroupName(hiveCounters,
                hiveCountersGroup, String.format("%s_",
                    ReduceSinkOperator.Counter.RECORDS_OUT_INTERMEDIATE.toString()) +
                    inputVertexName.replace(" ", "_"));

            hiveInputRecordsFromOtherVertices += getCounterValueByGroupName(hiveCounters,
                hiveCountersGroup, String.format("%s_",
                    FileSinkOperator.Counter.RECORDS_OUT.toString()) +
                    inputVertexName.replace(" ", "_"));
          }
        }

      /*
       * Get the CPU & GC
       *
       * counters org.apache.tez.common.counters.TaskCounter
       *  GC_TIME_MILLIS=37712
       *  CPU_MILLISECONDS=2774230
       */
        final TezCounters vertexCounters = vertexStatus.getVertexCounters();
        final double cpuTimeMillis = getCounterValueByGroupName(vertexCounters,
            TaskCounter.class.getName(),
            TaskCounter.CPU_MILLISECONDS.name());

        final double gcTimeMillis = getCounterValueByGroupName(vertexCounters,
            TaskCounter.class.getName(),
            TaskCounter.GC_TIME_MILLIS.name());

      /*
       * Get the HIVE counters
       *
       * HIVE
       *  CREATED_FILES=1
       *  DESERIALIZE_ERRORS=0
       *  RECORDS_IN_Map_1=550076554
       *  RECORDS_OUT_INTERMEDIATE_Map_1=854987
       *  RECORDS_OUT_Reducer_2=1
       */

        final long hiveInputRecords =
            getCounterValueByGroupName(
                hiveCounters,
                hiveCountersGroup,
                String.format("%s_", MapOperator.Counter.RECORDS_IN.toString())
                    + vertexName.replace(" ", "_"))
                + hiveInputRecordsFromOtherVertices;
        final long hiveOutputIntermediateRecords =
            getCounterValueByGroupName(
                hiveCounters,
                hiveCountersGroup,
                String.format("%s_", ReduceSinkOperator.Counter.RECORDS_OUT_INTERMEDIATE.toString())
                    + vertexName.replace(" ", "_"));
        final long hiveOutputRecords =
            getCounterValueByGroupName(
                hiveCounters,
                hiveCountersGroup,
                String.format("%s_", FileSinkOperator.Counter.RECORDS_OUT.toString())
                    + vertexName.replace(" ", "_"))
                + hiveOutputIntermediateRecords;

        String vertexExecutionStats = String.format(SUMMARY_VERTEX_FORMAT,
            vertexName,
            totalTasks,
            failedTaskAttempts,
            killedTasks,
            secondsFormat.format((duration)),
            commaFormat.format(cpuTimeMillis),
            commaFormat.format(gcTimeMillis),
            commaFormat.format(hiveInputRecords),
            commaFormat.format(hiveOutputRecords));
        console.printInfo(vertexExecutionStats);
      }
    }
  }

  private void printStatusInPlace(Map<String, Progress> progressMap, long startTime,
      boolean vextexStatusFromAM, DAGClient dagClient) {
    StringBuffer reportBuffer = new StringBuffer();
    int sumComplete = 0;
    int sumTotal = 0;

    // position the cursor to line 0
    repositionCursor();

    // print header
    // -------------------------------------------------------------------------------
    //         VERTICES     STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
    // -------------------------------------------------------------------------------
    reprintLine(separator);
    reprintLineWithColorAsBold(HEADER, Ansi.Color.CYAN);
    reprintLine(separator);

    SortedSet<String> keys = new TreeSet<String>(progressMap.keySet());
    int idx = 0;
    int maxKeys = keys.size();
    for (String s : keys) {
      idx++;
      Progress progress = progressMap.get(s);
      final int complete = progress.getSucceededTaskCount();
      final int total = progress.getTotalTaskCount();
      final int running = progress.getRunningTaskCount();
      final int failed = progress.getFailedTaskAttemptCount();
      final int pending = progress.getTotalTaskCount() - progress.getSucceededTaskCount() -
          progress.getRunningTaskCount();
      final int killed = progress.getKilledTaskCount();

      // To get vertex status we can use DAGClient.getVertexStatus(), but it will be expensive to
      // get status from AM for every refresh of the UI. Lets infer the state from task counts.
      // Only if DAG is FAILED or KILLED the vertex status is fetched from AM.
      VertexStatus.State vertexState = VertexStatus.State.INITIALIZING;

      // INITED state
      if (total > 0) {
        vertexState = VertexStatus.State.INITED;
        sumComplete += complete;
        sumTotal += total;
      }

      // RUNNING state
      if (complete < total && (complete > 0 || running > 0 || failed > 0)) {
        vertexState = VertexStatus.State.RUNNING;
        if (!perfLogger.startTimeHasMethod(PerfLogger.TEZ_RUN_VERTEX + s)) {
          perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_RUN_VERTEX + s);
        }
      }

      // SUCCEEDED state
      if (complete == total) {
        vertexState = VertexStatus.State.SUCCEEDED;
        if (!completed.contains(s)) {
          completed.add(s);

            /* We may have missed the start of the vertex
             * due to the 3 seconds interval
             */
          if (!perfLogger.startTimeHasMethod(PerfLogger.TEZ_RUN_VERTEX + s)) {
            perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_RUN_VERTEX + s);
          }

          perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_RUN_VERTEX + s);
        }
      }

      // DAG might have been killed, lets try to get vertex state from AM before dying
      // KILLED or FAILED state
      if (vextexStatusFromAM) {
        VertexStatus vertexStatus = null;
        try {
          vertexStatus = dagClient.getVertexStatus(s, null);
        } catch (IOException e) {
          // best attempt, shouldn't really kill DAG for this
        } catch (TezException e) {
          // best attempt, shouldn't really kill DAG for this
        }
        if (vertexStatus != null) {
          vertexState = vertexStatus.getState();
        }
      }

      // Map 1 ..........  SUCCEEDED      7          7        0        0       0       0
      String nameWithProgress = getNameWithProgress(s, complete, total);
      String vertexStr = String.format(VERTEX_FORMAT,
          nameWithProgress,
          vertexState.toString(),
          total,
          complete,
          running,
          pending,
          failed,
          killed);
      reportBuffer.append(vertexStr);
      if (idx != maxKeys) {
        reportBuffer.append("\n");
      }
    }

    reprintMultiLine(reportBuffer.toString());

    // -------------------------------------------------------------------------------
    // VERTICES: 03/04            [=================>>-----] 86%  ELAPSED TIME: 1.71 s
    // -------------------------------------------------------------------------------
    reprintLine(separator);
    final float progress = (sumTotal == 0) ? 0.0f : (float) sumComplete / (float) sumTotal;
    String footer = getFooter(keys.size(), completed.size(), progress, startTime);
    reprintLineWithColorAsBold(footer, Ansi.Color.RED);
    reprintLine(separator);
  }

  // Map 1 ..........
  private String getNameWithProgress(String s, int complete, int total) {
    float percent = total == 0 ? 0.0f : (float) complete / (float) total;
    // lets use the remaining space in column 1 as progress bar
    int spaceRemaining = COLUMN_1_WIDTH - s.length() - 1;
    String trimmedVName = s;

    // if the vertex name is longer than column 1 width, trim it down
    // "Tez Merge File Work" will become "Tez Merge File.."
    if (s != null && s.length() > COLUMN_1_WIDTH) {
      trimmedVName = s.substring(0, COLUMN_1_WIDTH - 1);
      trimmedVName = trimmedVName + "..";
    }

    String result = trimmedVName + " ";
    int toFill = (int) (spaceRemaining * percent);
    for (int i = 0; i < toFill; i++) {
      result += ".";
    }
    return result;
  }

  // VERTICES: 03/04            [==================>>-----] 86%  ELAPSED TIME: 1.71 s
  private String getFooter(int keySize, int completedSize, float progress, long startTime) {
    String verticesSummary = String.format("VERTICES: %02d/%02d", completedSize, keySize);
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

  private String printStatus(Map<String, Progress> progressMap, String lastReport, LogHelper console) {
    String report = getReport(progressMap);
    if (!report.equals(lastReport) || System.currentTimeMillis() >= lastPrintTime + printInterval) {
      console.printInfo(report);
      lastPrintTime = System.currentTimeMillis();
    }
    return report;
  }

  private String logStatus(Map<String, Progress> progressMap, String lastReport, LogHelper console) {
    String report = getReport(progressMap);
    if (!report.equals(lastReport) || System.currentTimeMillis() >= lastPrintTime + printInterval) {
      console.logInfo(report);
      lastPrintTime = System.currentTimeMillis();
    }
    return report;
  }

  private String getReport(Map<String, Progress> progressMap) {
    StringBuffer reportBuffer = new StringBuffer();

    SortedSet<String> keys = new TreeSet<String>(progressMap.keySet());
    for (String s: keys) {
      Progress progress = progressMap.get(s);
      final int complete = progress.getSucceededTaskCount();
      final int total = progress.getTotalTaskCount();
      final int running = progress.getRunningTaskCount();
      final int failed = progress.getFailedTaskAttemptCount();
      if (total <= 0) {
        reportBuffer.append(String.format("%s: -/-\t", s, complete, total));
      } else {
        if (complete == total && !completed.contains(s)) {
          completed.add(s);

          /*
           * We may have missed the start of the vertex due to the 3 seconds interval
           */
          if (!perfLogger.startTimeHasMethod(PerfLogger.TEZ_RUN_VERTEX + s)) {
            perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_RUN_VERTEX + s);
          }

          perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_RUN_VERTEX + s);
        }
        if(complete < total && (complete > 0 || running > 0 || failed > 0)) {
          
          if (!perfLogger.startTimeHasMethod(PerfLogger.TEZ_RUN_VERTEX + s)) {
            perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_RUN_VERTEX + s);
          }
          
          /* vertex is started, but not complete */
          if (failed > 0) {
            reportBuffer.append(String.format("%s: %d(+%d,-%d)/%d\t", s, complete, running, failed, total));
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
}
