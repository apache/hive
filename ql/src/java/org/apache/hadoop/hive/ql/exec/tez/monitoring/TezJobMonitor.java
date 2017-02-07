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
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.TezSessionPoolManager;
import org.apache.hadoop.hive.common.log.InPlaceUpdate;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.common.log.ProgressMonitor;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hive.common.util.ShutdownHookManager;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.dag.api.client.StatusGetOpts;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.tez.dag.api.client.DAGStatus.State.RUNNING;

/**
 * TezJobMonitor keeps track of a tez job while it's being executed. It will
 * print status to the console and retrieve final status of the job after
 * completion.
 */
public class TezJobMonitor {

  private static final String CLASS_NAME = TezJobMonitor.class.getName();
  private static final int CHECK_INTERVAL = 200;
  private static final int MAX_RETRY_INTERVAL = 2500;
  private static final int PRINT_INTERVAL = 3000;

  private final PerfLogger perfLogger = SessionState.getPerfLogger();
  private static final List<DAGClient> shutdownList;
  private final Map<String, BaseWork> workMap;

  private transient LogHelper console;

  private long lastPrintTime;
  private StringWriter diagnostics = new StringWriter();

  interface UpdateFunction {
    void update(DAGStatus status, Map<String, Progress> vertexProgressMap, String report);
  }

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
  private final UpdateFunction updateFunction;
  /**
   * Have to use the same instance to render else the number lines printed earlier is lost and the
   * screen will print the table again and again.
   */
  private final InPlaceUpdate inPlaceUpdate;

  public TezJobMonitor(Map<String, BaseWork> workMap, final DAGClient dagClient, HiveConf conf, DAG dag,
                       Context ctx) {
    this.workMap = workMap;
    this.dagClient = dagClient;
    this.hiveConf = conf;
    this.dag = dag;
    this.context = ctx;
    console = SessionState.getConsole();
    inPlaceUpdate = new InPlaceUpdate(LogHelper.getInfoStream());
    updateFunction = updateFunction();
  }

  private UpdateFunction updateFunction() {
    UpdateFunction logToFileFunction = new UpdateFunction() {
      @Override
      public void update(DAGStatus status, Map<String, Progress> vertexProgressMap, String report) {
        SessionState.get().updateProgressMonitor(progressMonitor(status, vertexProgressMap));
        console.printInfo(report);
      }
    };
    UpdateFunction inPlaceUpdateFunction = new UpdateFunction() {
      @Override
      public void update(DAGStatus status, Map<String, Progress> vertexProgressMap, String report) {
        inPlaceUpdate.render(progressMonitor(status, vertexProgressMap));
        console.logInfo(report);
      }
    };
    return InPlaceUpdate.canRenderInPlace(hiveConf)
        && !SessionState.getConsole().getIsSilent()
        && !SessionState.get().isHiveServerQuery()
        ? inPlaceUpdateFunction : logToFileFunction;
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
    Map<String, Progress> vertexProgressMap = null;


    long monitorStartTime = System.currentTimeMillis();
    synchronized (shutdownList) {
      shutdownList.add(dagClient);
    }
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_RUN_DAG);
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_SUBMIT_TO_RUNNING);
    DAGStatus.State lastState = null;
    String lastReport = null;
    boolean running = false;

    while (true) {

      try {
        if (context != null) {
          context.checkHeartbeaterLockException();
        }

        status = dagClient.getDAGStatus(new HashSet<StatusGetOpts>(), CHECK_INTERVAL);
        vertexProgressMap = status.getVertexProgress();
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
              lastReport = updateStatus(status, vertexProgressMap, lastReport);
              break;
            case SUCCEEDED:
              if (!running) {
                this.executionStartTime = monitorStartTime;
              }
              lastReport = updateStatus(status, vertexProgressMap, lastReport);
              success = true;
              running = false;
              done = true;
              break;
            case KILLED:
              if (!running) {
                this.executionStartTime = monitorStartTime;
              }
              lastReport = updateStatus(status, vertexProgressMap, lastReport);
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
              lastReport = updateStatus(status, vertexProgressMap, lastReport);
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
          console
              .printError("Execution has failed. stack trace: " + ExceptionUtils.getStackTrace(e));
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
    printSummary(success, vertexProgressMap);
    return rc;
  }

  private void printSummary(boolean success, Map<String, Progress> progressMap) {
    if (isProfilingEnabled() && success && progressMap != null) {

      double duration = (System.currentTimeMillis() - this.executionStartTime) / 1000.0;
      console.printInfo("Status: DAG finished successfully in " + String.format("%.2f seconds", duration));
      console.printInfo("");

      new QueryExecutionBreakdownSummary(perfLogger).print(console);
      new DAGSummary(progressMap, hiveConf, dagClient, dag, perfLogger).print(console);

      //llap IO summary
      if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.LLAP_IO_ENABLED, false)) {
        new LLAPioSummary(progressMap, dagClient).print(console);
        new FSCountersSummary(progressMap, dagClient).print(console);
      }
      console.printInfo("");
    }
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

  static long getCounterValueByGroupName(TezCounters vertexCounters, String groupNamePattern,
                                         String counterName) {
    TezCounter tezCounter = vertexCounters.getGroup(groupNamePattern).findCounter(counterName);
    return (tezCounter == null) ? 0 : tezCounter.getValue();
  }

  private String updateStatus(DAGStatus status, Map<String, Progress> vertexProgressMap,
      String lastReport) {
    String report = getReport(vertexProgressMap);
    if (!report.equals(lastReport) || System.currentTimeMillis() >= lastPrintTime + PRINT_INTERVAL) {
      updateFunction.update(status, vertexProgressMap, report);
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
        if (complete == total) {
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

  private ProgressMonitor progressMonitor(DAGStatus status, Map<String, Progress> progressMap) {
    try {
      return new TezProgressMonitor(dagClient, status, workMap, progressMap, console,
          executionStartTime);
    } catch (IOException | TezException e) {
      console.printInfo("Getting  Progress Information: " + e.getMessage() + " stack trace: " +
          ExceptionUtils.getStackTrace(e));
    }
    return TezProgressMonitor.NULL;
  }
}
