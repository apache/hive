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

import static org.apache.tez.dag.api.client.DAGStatus.State.RUNNING;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.StringWriter;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hive.common.log.InPlaceUpdate;
import org.apache.hadoop.hive.common.log.ProgressMonitor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.TezSessionPoolManager;
import org.apache.hadoop.hive.ql.exec.tez.Utils;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.wm.TimeCounterLimit;
import org.apache.hadoop.hive.ql.wm.VertexCounterLimit;
import org.apache.hadoop.hive.ql.wm.WmContext;
import org.apache.hive.common.util.ShutdownHookManager;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * TezJobMonitor keeps track of a tez job while it's being executed. It will
 * print status to the console and retrieve final status of the job after
 * completion.
 */
public class TezJobMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(TezJobMonitor.class);

  static final String CLASS_NAME = TezJobMonitor.class.getName();
  private static final int MAX_CHECK_INTERVAL = 1000;
  private static final int MAX_RETRY_INTERVAL = 2500;
  private static final int MAX_RETRY_FAILURES = (MAX_RETRY_INTERVAL / MAX_CHECK_INTERVAL) + 1;

  private final PerfLogger perfLogger;
  private static final List<DAGClient> shutdownList;
  private final List<BaseWork> topSortedWorks;

  transient LogHelper console;

  private StringWriter diagnostics = new StringWriter();

  static {
    shutdownList = new LinkedList<>();
    ShutdownHookManager.addShutdownHook(new Runnable() {
      @Override
      public void run() {
        TezJobMonitor.killRunningJobs();
        try {
          // TODO: why does this only kill non-default sessions?
          // Nothing for workload management since that only deals with default ones.
          TezSessionPoolManager.getInstance().closeNonDefaultSessions();
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
  private final RenderStrategy.UpdateFunction updateFunction;
  // compile time tez counters
  private final TezCounters counters;

  public TezJobMonitor(List<BaseWork> topSortedWorks, final DAGClient dagClient, HiveConf conf, DAG dag,
    Context ctx, final TezCounters counters, PerfLogger perfLogger) {
    this.topSortedWorks = topSortedWorks;
    this.dagClient = dagClient;
    this.hiveConf = conf;
    this.dag = dag;
    this.context = ctx;
    console = SessionState.getConsole();
    updateFunction = updateFunction();
    this.counters = counters;
    this.perfLogger = perfLogger;
  }

  private RenderStrategy.UpdateFunction updateFunction() {
    return InPlaceUpdate.canRenderInPlace(hiveConf)
        && !SessionState.getConsole().getIsSilent()
        && !SessionState.get().isHiveServerQuery()
        ? new RenderStrategy.InPlaceUpdateFunction(this)
        : new RenderStrategy.LogToFileFunction(this);
  }

  private boolean isProfilingEnabled() {
    return HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.TEZ_EXEC_SUMMARY) ||
      Utilities.isPerfOrAboveLogging(hiveConf);
  }

  public int monitorExecution() {
    boolean done = false;
    boolean success = false;
    int failedCounter = 0;
    final StopWatch failureTimer = new StopWatch();
    int rc = 0;
    DAGStatus status = null;
    Map<String, Progress> vertexProgressMap = null;


    long monitorStartTime = System.currentTimeMillis();
    synchronized (shutdownList) {
      shutdownList.add(dagClient);
    }
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.TEZ_RUN_DAG);
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.TEZ_SUBMIT_TO_RUNNING);
    DAGStatus.State lastState = null;
    boolean running = false;

    long checkInterval = HiveConf.getTimeVar(hiveConf, HiveConf.ConfVars.TEZ_DAG_STATUS_CHECK_INTERVAL,
      TimeUnit.MILLISECONDS);
    WmContext wmContext = null;
    while (true) {

      try {
        if (context != null) {
          context.checkHeartbeaterLockException();
        }

        wmContext = context.getWmContext();
        EnumSet<StatusGetOpts> opts = null;
        if (wmContext != null) {
          Set<String> desiredCounters = wmContext.getSubscribedCounters();
          if (desiredCounters != null && !desiredCounters.isEmpty()) {
            opts = EnumSet.of(StatusGetOpts.GET_COUNTERS);
          }
        }

        status = dagClient.getDAGStatus(opts, checkInterval);

        vertexProgressMap = status.getVertexProgress();
        List<String> vertexNames = vertexProgressMap.keySet()
          .stream()
          .map(k -> k.replaceAll(" ", "_"))
          .collect(Collectors.toList());
        if (wmContext != null) {
          Set<String> desiredCounters = wmContext.getSubscribedCounters();
          TezCounters dagCounters = status.getDAGCounters();
          // if initial counters exists, merge it with dag counters to get aggregated view
          TezCounters mergedCounters = counters == null ? dagCounters : Utils.mergeTezCounters(dagCounters, counters);
          if (mergedCounters != null && desiredCounters != null && !desiredCounters.isEmpty()) {
            Map<String, Long> currentCounters = getCounterValues(mergedCounters, vertexNames, vertexProgressMap,
              desiredCounters, done);
            LOG.debug("Requested DAG status. checkInterval: {}. currentCounters: {}", checkInterval, currentCounters);
            wmContext.setCurrentCounters(currentCounters);
          }
        }
        DAGStatus.State state = status.getState();

        failedCounter = 0; // AM is responsive again (recovery?)
        failureTimer.reset();

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
                perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.TEZ_SUBMIT_TO_RUNNING);
                console.printInfo("Status: Running (" + dagClient.getExecutionContext() + ")\n");
                this.executionStartTime = System.currentTimeMillis();
                running = true;
              }
              updateFunction.update(status, vertexProgressMap);
              break;
            case SUCCEEDED:
              if (!running) {
                this.executionStartTime = monitorStartTime;
              }
              updateFunction.update(status, vertexProgressMap);
              success = true;
              running = false;
              done = true;
              break;
            case KILLED:
              if (!running) {
                this.executionStartTime = monitorStartTime;
              }
              updateFunction.update(status, vertexProgressMap);
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
              updateFunction.update(status, vertexProgressMap);
              console.printError("Status: Failed");
              running = false;
              done = true;
              rc = 2;
              break;
          }
        }
        if (wmContext != null && done) {
          wmContext.setQueryCompleted(true);
        }
      } catch (Exception e) {
        console.printInfo("Exception: " + e.getMessage());
        boolean isInterrupted = hasInterruptedException(e);
        if (failedCounter == 0) {
          failureTimer.reset();
          failureTimer.start();
        }
        if (isInterrupted
            || (++failedCounter >= MAX_RETRY_FAILURES && failureTimer.now(TimeUnit.MILLISECONDS) > MAX_RETRY_INTERVAL)) {
          try {
            if (isInterrupted) {
              console.printInfo("Killing DAG...");
            } else {
              console.printInfo(String.format("Killing DAG... after %d seconds",
                  failureTimer.now(TimeUnit.SECONDS)));
            }
            dagClient.tryKillDAG();
          } catch (IOException | TezException tezException) {
            // best effort
          }
          console.printError("Execution has failed. stack trace: " + ExceptionUtils.getStackTrace(e));
          rc = 1;
          done = true;
        } else {
          console.printInfo("Retrying...");
        }
        if (wmContext != null && done) {
          wmContext.setQueryCompleted(true);
        }
      } finally {
        if (done) {
          if (wmContext != null && done) {
            wmContext.setQueryCompleted(true);
          }
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

    perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.TEZ_RUN_DAG);
    printSummary(success, vertexProgressMap);
    return rc;
  }

  private Map<String, Long> getCounterValues(final TezCounters dagCounters,
    final List<String> vertexNames, final Map<String, Progress> vertexProgressMap,
    final Set<String> desiredCounters, final boolean done) {
    // DAG specific counters
    Map<String, Long> updatedCounters = new HashMap<>();
    for (CounterGroup counterGroup : dagCounters) {
      for (TezCounter tezCounter : counterGroup) {
        String counterName = tezCounter.getName();
        for (String desiredCounter : desiredCounters) {
          if (counterName.equals(desiredCounter)) {
            updatedCounters.put(counterName, tezCounter.getValue());
          } else if (isDagLevelCounter(desiredCounter)) {
            // by default, we aggregate counters across the entire DAG. Example: SHUFFLE_BYTES would mean SHUFFLE_BYTES
            // of each vertex aggregated together to create DAG level SHUFFLE_BYTES.
            // Use case: If SHUFFLE_BYTES across the entire DAG is > limit perform action
            String prefixRemovedCounterName = getCounterFromDagCounter(desiredCounter);
            aggregateCountersSum(updatedCounters, vertexNames, prefixRemovedCounterName, desiredCounter, tezCounter);
          } else if (isVertexLevelCounter(desiredCounter)) {
            // if counter name starts with VERTEX_ then we just return max value across all vertex since trigger
            // validation is only interested in violation that are greater than limit (*any* vertex violation).
            // Use case: If SHUFFLE_BYTES for any single vertex is > limit perform action
            String prefixRemovedCounterName = getCounterFromVertexCounter(desiredCounter);
            aggregateCountersMax(updatedCounters, vertexNames, prefixRemovedCounterName, desiredCounter, tezCounter);
          } else if (counterName.startsWith(desiredCounter)) {
            // Counters with vertex name as suffix
            // desiredCounter = INPUT_FILES
            // counters: {INPUT_FILES_Map_1 : 5, INPUT_FILES_Map_4 : 10}
            // outcome: INPUT_FILE : 15
            String prefixRemovedCounterName = desiredCounter;
            aggregateCountersSum(updatedCounters, vertexNames, prefixRemovedCounterName, desiredCounter, tezCounter);
          }
        }
      }
    }

    // Process per vertex counters that are available only via vertex Progress
    String counterName = VertexCounterLimit.VertexCounter.VERTEX_TOTAL_TASKS.name();
    if (desiredCounters.contains(counterName) && vertexProgressMap != null) {
      for (Map.Entry<String, Progress> entry : vertexProgressMap.entrySet()) {
        long currentMax = 0;
        if (updatedCounters.containsKey(counterName)) {
          currentMax = updatedCounters.get(counterName);
        }
        long newMax = Math.max(currentMax, entry.getValue().getTotalTaskCount());
        updatedCounters.put(counterName, newMax);
      }
    }

    counterName = VertexCounterLimit.VertexCounter.DAG_TOTAL_TASKS.name();
    if (desiredCounters.contains(counterName) && vertexProgressMap != null) {
      for (Map.Entry<String, Progress> entry : vertexProgressMap.entrySet()) {
        long currentTotal = 0;
        if (updatedCounters.containsKey(counterName)) {
          currentTotal = updatedCounters.get(counterName);
        }
        long newTotal = currentTotal + entry.getValue().getTotalTaskCount();
        updatedCounters.put(counterName, newTotal);
      }
    }

    // Time based counters. If DAG is done already don't update these counters.
    if (!done) {
      counterName = TimeCounterLimit.TimeCounter.EXECUTION_TIME.name();
      if (desiredCounters.contains(counterName) && executionStartTime > 0) {
        updatedCounters.put(counterName, System.currentTimeMillis() - executionStartTime);
      }
    }

    return updatedCounters;
  }

  private void aggregateCountersSum(final Map<String, Long> updatedCounters, final List<String> vertexNames,
    final String prefixRemovedCounterName, final String desiredCounter, final TezCounter tezCounter) {
    long counterValue = checkVertexSuffixAndGetValue(vertexNames, prefixRemovedCounterName, tezCounter);
    long currentTotal = 0;
    if (updatedCounters.containsKey(desiredCounter)) {
      currentTotal = updatedCounters.get(desiredCounter);
    }
    long newTotal = currentTotal + counterValue;
    updatedCounters.put(desiredCounter, newTotal);
  }

  private void aggregateCountersMax(final Map<String, Long> updatedCounters, final List<String> vertexNames,
    final String prefixRemovedCounterName, final String desiredCounter, final TezCounter tezCounter) {
    long counterValue = checkVertexSuffixAndGetValue(vertexNames, prefixRemovedCounterName, tezCounter);
    long currentMax = 0;
    if (updatedCounters.containsKey(desiredCounter)) {
      currentMax = updatedCounters.get(desiredCounter);
    }
    long newMax = Math.max(currentMax, counterValue);
    updatedCounters.put(desiredCounter, newMax);
  }

  private long checkVertexSuffixAndGetValue(final List<String> vertexNames, final String counterName,
    final TezCounter tezCounter) {
    for (String vertexName : vertexNames) {
      if (tezCounter.getName().equalsIgnoreCase(counterName + "_" + vertexName)) {
        return tezCounter.getValue();
      }
    }
    return 0;
  }

  private String getCounterFromDagCounter(final String desiredCounter) {
    return desiredCounter.substring("DAG_".length());
  }

  private String getCounterFromVertexCounter(final String desiredCounter) {
    return desiredCounter.substring("VERTEX_".length());
  }

  private boolean isVertexLevelCounter(final String desiredCounter) {
    return desiredCounter.startsWith("VERTEX_");
  }

  private boolean isDagLevelCounter(final String desiredCounter) {
    return desiredCounter.startsWith("DAG_");
  }

  private void printSummary(boolean success, Map<String, Progress> progressMap) {
    if (isProfilingEnabled() && success && progressMap != null) {

      double duration = (System.currentTimeMillis() - this.executionStartTime) / 1000.0;
      console.printInfo("Status: DAG finished successfully in " + String.format("%.2f seconds", duration));
      console.printInfo("DAG ID: " + this.dagClient.getDagIdentifierString());
      console.printInfo("");

      new QueryExecutionBreakdownSummary(perfLogger).print(console);
      new DAGSummary(progressMap, hiveConf, dagClient, dag, perfLogger).print(console);

      //llap IO summary
      if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.LLAP_IO_ENABLED, false)) {
        new LLAPioSummary(progressMap, dagClient).print(console);
      }
      new FSCountersSummary(progressMap, dagClient).print(console);
      String wmQueue = HiveConf.getVar(hiveConf, ConfVars.HIVE_SERVER2_TEZ_INTERACTIVE_QUEUE);
      if (wmQueue != null && !wmQueue.isEmpty()) {
        new LlapWmSummary(progressMap, dagClient).print(console);
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
   *
   * {@link org.apache.hadoop.hive.ql.exec.tez.TezJobExecHelper#killRunningJobs()} makes use of
   * this method via reflection.
   */
  public static void killRunningJobs() {
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

  public HiveConf getHiveConf() {
    return hiveConf;
  }

  public String getDiagnostics() {
    return diagnostics.toString();
  }

  ProgressMonitor progressMonitor(DAGStatus status, Map<String, Progress> progressMap) {
    try {
      return new TezProgressMonitor(dagClient, status, topSortedWorks, progressMap, console,
          executionStartTime);
    } catch (IOException | TezException e) {
      console.printInfo("Getting  Progress Information: " + e.getMessage() + " stack trace: " +
          ExceptionUtils.getStackTrace(e));
    }
    return TezProgressMonitor.NULL;
  }
}
