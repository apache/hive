/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
  <p>
  http://www.apache.org/licenses/LICENSE-2.0
  <p>
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.tez.monitoring;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.TezSessionPoolManager;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.log.ProgressMonitor;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hive.common.util.ShutdownHookManager;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.*;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;

import static org.apache.tez.dag.api.client.DAGStatus.State.KILLED;
import static org.apache.tez.dag.api.client.DAGStatus.State.RUNNING;

/**
 * TezJobMonitor keeps track of a tez job while it's being executed. It will
 * print status to the console and retrieve final status of the job after
 * completion.
 */
public class TezJobMonitor implements ProgressMonitor {

  private static final String CLASS_NAME = TezJobMonitor.class.getName();

  private static final int COLUMN_1_WIDTH = 16;

  private transient LogHelper console;
  private final PerfLogger perfLogger = SessionState.getPerfLogger();
  private static final int CHECK_INTERVAL = 200;
  private static final int MAX_RETRY_INTERVAL = 2500;
  private static final int PRINT_INTERVAL = 3000;
  private long lastPrintTime;
  private Set<String> completed;

  private static final List<DAGClient> shutdownList;
  private final Map<String, BaseWork> workMap;

  private StringBuffer diagnostics;

  static {
    shutdownList = new LinkedList<>();
    ShutdownHookManager.addShutdownHook(new Runnable() {
      @Override
      public void run() {
        TezJobMonitor.killRunningJobs();
        try {
          TezSessionPoolManager.getInstance().closeNonDefaultSessions(false);
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

  private final DAGClient dagClient;
  private final HiveConf hiveConf;
  private final DAG dag;
  private final Context context;
  private long executionStartTime = 0;

  public TezJobMonitor(Map<String, BaseWork> workMap, final DAGClient dagClient, HiveConf conf, DAG dag,
                       Context ctx) {
    this.workMap = workMap;
    this.dagClient = dagClient;
    this.hiveConf = conf;
    this.dag = dag;
    this.context = ctx;
    console = SessionState.getConsole();
  }

  private boolean isProfilingEnabled() {
    return HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.TEZ_EXEC_SUMMARY) ||
        Utilities.isPerfOrAboveLogging(hiveConf);
  }

  public int monitorExecution() {
    boolean done = false;
    boolean success = false;
    int failedCounter = 0;
    int rc = 0;
    DAGStatus status = null;
    Map<String, Progress> progressMap = null;


    long monitorStartTime = System.currentTimeMillis();
    synchronized (shutdownList) {
      shutdownList.add(dagClient);
    }
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_RUN_DAG);
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_SUBMIT_TO_RUNNING);
    completed = new HashSet<>();
    diagnostics = new StringBuffer();
    DAGStatus.State lastState = null;
    String lastReport = null;
    boolean running = false;

    while (true) {

      try {
        if (context != null) {
          context.checkHeartbeaterLockException();
        }

        status = dagClient.getDAGStatus(new HashSet<StatusGetOpts>(), CHECK_INTERVAL);
        progressMap = status.getVertexProgress();
        DAGStatus.State state = status.getState();

        if (state != lastState || state == RUNNING) {
          lastState = state;

          switch (state) {
            case SUBMITTED:
              console.printInfo("Status: Submitted");
              break;
            case INITING:
              console.printInfo("Status: Initializing");
              this.executionStartTime = System.currentTimeMillis();
              break;
            case RUNNING:
              if (!running) {
                perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_SUBMIT_TO_RUNNING);
                console.printInfo("Status: Running (" + dagClient.getExecutionContext() + ")\n");
                this.executionStartTime = System.currentTimeMillis();
                running = true;
              }
              lastReport = printStatus(progressMap, lastReport, console);
              break;
            case SUCCEEDED:
              if (!running) {
                this.executionStartTime = monitorStartTime;
              }
              lastReport = printStatus(progressMap, lastReport, console);
              success = true;
              running = false;
              done = true;
              break;
            case KILLED:
              if (!running) {
                this.executionStartTime = monitorStartTime;
              }
              console.printInfo("Status: Killed");
              running = false;
              done = true;
              rc = 1;
              break;
            case FAILED:
            case ERROR:
              if (!running) {
                this.executionStartTime = monitorStartTime;
              }
              console.printError("Status: Failed");
              running = false;
              done = true;
              rc = 2;
              break;
          }
        }
      } catch (Exception e) {
        console.printInfo("Exception: " + e.getMessage());
        boolean isInterrupted = hasInterruptedException(e);
        if (isInterrupted || (++failedCounter % MAX_RETRY_INTERVAL / CHECK_INTERVAL == 0)) {
          try {
            console.printInfo("Killing DAG...");
            dagClient.tryKillDAG();
          } catch (IOException | TezException tezException) {
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
              diagnostics.append(diag);
            }
          }
          synchronized (shutdownList) {
            shutdownList.remove(dagClient);
          }
          break;
        }
      }
    }

    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_RUN_DAG);

    if (isProfilingEnabled() && success && progressMap != null) {

      double duration = (System.currentTimeMillis() - this.executionStartTime) / 1000.0;
      console.printInfo("Status: DAG finished successfully in "
          + String.format("%.2f seconds", duration));
      console.printInfo("");

      new QueryExecutionBreakdownSummary(perfLogger).print(console);

      new DAGSummary(progressMap, hiveConf, dagClient, dag, perfLogger).print(console);

      //llap IO summary
      if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.LLAP_IO_ENABLED, false)) {
        new LLAPioSummary(progressMap, dagClient).print(console);
        ;

        new FSCountersSummary(progressMap, dagClient).print(console);

      }
      console.printInfo("");
    }

    return rc;
  }

  private static boolean hasInterruptedException(Throwable e) {
    // Hadoop IPC wraps InterruptedException. GRRR.
    while (e != null) {
      if (e instanceof InterruptedException || e instanceof InterruptedIOException) {
        return true;
      }
      e = e.getCause();
    }
    return false;
  }

  /**
   * killRunningJobs tries to terminate execution of all
   * currently running tez queries. No guarantees, best effort only.
   */
  private static void killRunningJobs() {
    synchronized (shutdownList) {
      for (DAGClient c : shutdownList) {
        try {
          System.err.println("Trying to shutdown DAG");
          c.tryKillDAG();
        } catch (Exception e) {
          // ignore
        }
      }
    }
  }

  static long getCounterValueByGroupName(TezCounters vertexCounters,
                                         String groupNamePattern,
                                         String counterName) {
    TezCounter tezCounter = vertexCounters.getGroup(groupNamePattern).findCounter(counterName);
    return (tezCounter == null) ? 0 : tezCounter.getValue();
  }


  private String getMode(String name, Map<String, BaseWork> workMap) {
    String mode = "container";
    BaseWork work = workMap.get(name);
    if (work != null) {
      // uber > llap > container
      if (work.getUberMode()) {
        mode = "uber";
      } else if (work.getLlapMode()) {
        mode = "llap";
      } else {
        mode = "container";
      }
    }
    return mode;
  }

  // Map 1 ..........
  private String getNameWithProgress(String s, int complete, int total) {
    String result = "";
    if (s != null) {
      float percent = total == 0 ? 0.0f : (float) complete / (float) total;
      // lets use the remaining space in column 1 as progress bar
      int spaceRemaining = COLUMN_1_WIDTH - s.length() - 1;
      String trimmedVName = s;

      // if the vertex name is longer than column 1 width, trim it down
      // "Tez Merge File Work" will become "Tez Merge File.."
      if (s.length() > COLUMN_1_WIDTH) {
        trimmedVName = s.substring(0, COLUMN_1_WIDTH - 1);
        trimmedVName = trimmedVName + "..";
      }

      result = trimmedVName + " ";
      int toFill = (int) (spaceRemaining * percent);
      for (int i = 0; i < toFill; i++) {
        result += ".";
      }
    }
    return result;
  }

  private String printStatus(Map<String, Progress> progressMap, String lastReport, LogHelper console) {
    String report = getReport(progressMap);
    if (!report.equals(lastReport) || System.currentTimeMillis() >= lastPrintTime + PRINT_INTERVAL) {
      console.printInfo(report);
      lastPrintTime = System.currentTimeMillis();
    }
    return report;
  }

  private String getReport(Map<String, Progress> progressMap) {
    StringBuilder reportBuffer = new StringBuilder();

    SortedSet<String> keys = new TreeSet<String>(progressMap.keySet());
    for (String s : keys) {
      Progress progress = progressMap.get(s);
      final int complete = progress.getSucceededTaskCount();
      final int total = progress.getTotalTaskCount();
      final int running = progress.getRunningTaskCount();
      final int failed = progress.getFailedTaskAttemptCount();
      if (total <= 0) {
        reportBuffer.append(String.format("%s: -/-\t", s));
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
        if (complete < total && (complete > 0 || running > 0 || failed > 0)) {

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

  public String getDiagnostics() {
    return diagnostics.toString();
  }

  public List<String> headers() {
    return Arrays.asList("VERTICES", "MODE", "STATUS", "TOTAL", "COMPLETED", "RUNNING", "PENDING", "FAILED", "KILLED");
  }

  public List<List<String>> rows() {
    try {
      DAGStatus status = dagClient.getDAGStatus(new HashSet<StatusGetOpts>());
      Map<String, Progress> progressMap = status.getVertexProgress();
      List<List<String>> results = new ArrayList<>();
      SortedSet<String> keys = new TreeSet<>(progressMap.keySet());
      for (String s : keys) {
        Progress progress = progressMap.get(s);
        final int complete = progress.getSucceededTaskCount();
        final int total = progress.getTotalTaskCount();

        // To get vertex status we can use DAGClient.getVertexStatus(), but it will be expensive to
        // get status from AM for every refresh of the UI. Lets infer the state from task counts.
        // Only if DAG is FAILED or KILLED the vertex status is fetched from AM.
        VertexStatus.State vertexState = VertexStatus.State.INITIALIZING;

        // INITED state
        if (total > 0) {
          vertexState = VertexStatus.State.INITED;
        }


        // DAG might have been killed, lets try to get vertex state from AM before dying
        // KILLED or FAILED state
        if (status.getState() == KILLED) {
          VertexStatus vertexStatus = null;
          try {
            vertexStatus = dagClient.getVertexStatus(s, null);
          } catch (IOException e) {
            // best attempt, shouldn't really kill DAG for this
          }
          if (vertexStatus != null) {
            vertexState = vertexStatus.getState();
          }
        }
        // Map 1 .......... container  SUCCEEDED      7          7        0        0       0       0

        results.add(
            Arrays.asList(
                getNameWithProgress(s, complete, total),
                getMode(s, workMap),
                vertexState.toString(),
                String.valueOf(total),
                String.valueOf(complete),
                String.valueOf(progress.getRunningTaskCount()),
                String.valueOf(
                    progress.getTotalTaskCount() - progress.getSucceededTaskCount() - progress.getRunningTaskCount()
                ),
                String.valueOf(progress.getFailedTaskAttemptCount()),
                String.valueOf(progress.getKilledTaskAttemptCount())
            )
        );
      }
      return results;
    } catch (Exception e) {
      console.printInfo("Getting  Progress Bar table rows failed: " + e.getMessage() + " stack trace: "
          + Arrays.toString(e.getStackTrace()));
    }
    return Collections.emptyList();
  }

  // -------------------------------------------------------------------------------
  // VERTICES: 03/04            [=================>>-----] 86%  ELAPSED TIME: 1.71 s
  // -------------------------------------------------------------------------------
  // contains footerSummary , progressedPercentage, starTime

  @Override
  public String footerSummary() {
    try {
      DAGStatus status = dagClient.getDAGStatus(new HashSet<StatusGetOpts>());
      Map<String, Progress> progressMap = status.getVertexProgress();
      return String.format("VERTICES: %02d/%02d", completed.size(), progressMap.keySet().size());
    } catch (IOException | TezException e) {
    }
    return "";
  }

  @Override
  public long startTime() {
    return executionStartTime;
  }

  @Override
  public double progressedPercentage() {
    try {
      int sumTotal = 0, sumComplete = 0;
      Map<String, Progress> progressMap = dagClient.getDAGStatus(new HashSet<StatusGetOpts>()).getVertexProgress();
      for (String s : progressMap.keySet()) {
        Progress progress = progressMap.get(s);
        final int complete = progress.getSucceededTaskCount();
        final int total = progress.getTotalTaskCount();
        if (total > 0) {
          sumTotal += total;
          sumComplete += complete;
        }
      }
      return (sumTotal == 0) ? 0.0f : (float) sumComplete / (float) sumTotal;
    } catch (IOException | TezException e) {
      console.printInfo("Getting  Progress Bar table rows failed: " + e.getMessage() + " stack trace: "
          + Arrays.toString(e.getStackTrace()));
    }
    return 0;
  }

  @Override
  public String executionStatus() {
    try {
      return dagClient.getDAGStatus(new HashSet<StatusGetOpts>()).getState().name();
    } catch (IOException | TezException e) {
      console.printInfo("Getting  Execution Status failed: " + e.getMessage() + " stack trace: "
          + Arrays.toString(e.getStackTrace()));
    }
    return "";
  }
}
