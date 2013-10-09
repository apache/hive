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

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;

/**
 * TezJobMonitor keeps track of a tez job while it's being executed. It will
 * print status to the console and retrieve final status of the job after
 * completion.
 */
public class TezJobMonitor {

  private static final Log LOG = LogFactory.getLog(TezJobMonitor.class.getName());
  private static final String CLASS_NAME = TezJobMonitor.class.getName();

  private transient LogHelper console;
  private final PerfLogger perfLogger = PerfLogger.getPerfLogger();
  private Set<String> completed;

  public TezJobMonitor() {
    console = new LogHelper(LOG);
  }

  /**
   * monitorExecution handles status printing, failures during execution and final
   * status retrieval.
   *
   * @param dagClient client that was used to kick off the job
   * @return int 0 - success, 1 - killed, 2 - failed
   */
  public int monitorExecution(DAGClient dagClient) throws InterruptedException {
    DAGStatus status = null;
    completed = new HashSet<String>();

    boolean running = false;
    boolean done = false;
    int checkInterval = 500;
    int printInterval = 3000;
    int maxRetryInterval = 5000;
    int counter = 0;
    int failedCounter = 0;
    int rc = 0;
    DAGStatus.State lastState = null;
    String lastReport = null;

    console.printInfo("\n");
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_RUN_DAG);
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_SUBMIT_TO_RUNNING);

    while(true) {
      ++counter;

      try {
        status = dagClient.getDAGStatus();
        Map<String, Progress> progressMap = status.getVertexProgress();
        failedCounter = 0;
        DAGStatus.State state = status.getState();

        if (state != lastState || state == RUNNING) {
          lastState = state;

          switch(state) {
          case SUBMITTED:
            console.printInfo("Status: Submitted");
            break;
          case INITING:
            console.printInfo("Status: Initializing");
            break;
          case RUNNING:
            if (!running) {
              perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_SUBMIT_TO_RUNNING);
              console.printInfo("Status: Running\n");
              printTaskNumbers(progressMap, console);
              running = true;
            }

            if (counter % printInterval/checkInterval == 0) {
              lastReport = printStatus(progressMap, lastReport, console);
            }
            break;
          case SUCCEEDED:
            lastReport = printStatus(progressMap, lastReport, console);
            console.printInfo("Status: Finished successfully");
            running = false;
            done = true;
            break;
          case KILLED:
            console.printInfo("Status: Killed");
            running = false;
            done = true;
            rc = 1;
            break;
          case FAILED:
          case ERROR:
            console.printError("Status: Failed");
            running = false;
            done = true;
            rc = 2;
            break;
          }
        }
      } catch (Exception e) {
        if (failedCounter % maxRetryInterval/checkInterval == 0) {
          try {
            dagClient.tryKillDAG();
          } catch(IOException io) {
            // best effort
          } catch(TezException te) {
            // best effort
          }
          e.printStackTrace();
          console.printError("Execution has failed.");
          rc = 1;
          done = true;
        }
      }

      if (done) {
        if (rc != 0 && status != null) {
          for (String diag: status.getDiagnostics()) {
            console.printError(diag);
          }
        }
        break;
      }
      Thread.sleep(500);
    }
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_RUN_DAG);
    return rc;
  }

  private String printStatus(Map<String, Progress> progressMap, String lastReport, LogHelper console) {
    StringBuffer reportBuffer = new StringBuffer();

    SortedSet<String> keys = new TreeSet<String>(progressMap.keySet());
    for (String s: keys) {
      Progress progress = progressMap.get(s);
      int percentComplete = (int) (100 * progress.getSucceededTaskCount() / (float) progress.getTotalTaskCount());
      if (percentComplete == 100 && !completed.contains(s)) {
        completed.add(s);
        perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_RUN_VERTEX + s);
      }
      reportBuffer.append(String.format("%s: %3d%% complete\t", s, percentComplete));
    }

    String report = reportBuffer.toString();
    if (!report.equals(lastReport)) {
      console.printInfo(report);
    }

    return report;
  }

  private void printTaskNumbers(Map<String, Progress> progressMap, LogHelper console) {
    StringBuffer reportBuffer = new StringBuffer();

    SortedSet<String> keys = new TreeSet<String>(progressMap.keySet());
    for (String s: keys) {
      perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_RUN_VERTEX + s);
      Progress progress = progressMap.get(s);
      int numTasks = progress.getTotalTaskCount();
      if (numTasks == 1) {
        reportBuffer.append(String.format("%s:        1 task\t", s));
      } else {
        reportBuffer.append(String.format("%s: %7d tasks\t", s, numTasks));
      }
    }

    String report = reportBuffer.toString();
    console.printInfo(report);
    console.printInfo("");
  }
}
