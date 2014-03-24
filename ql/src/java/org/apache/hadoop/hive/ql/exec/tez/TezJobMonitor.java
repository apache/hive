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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Heartbeater;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.dag.api.client.StatusGetOpts;

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
  private final int checkInterval = 200;
  private final int maxRetryInterval = 2500;
  private final int printInterval = 3000;
  private long lastPrintTime;
  private Set<String> completed;
  private static final List<DAGClient> shutdownList;

  static {
    shutdownList = Collections.synchronizedList(new LinkedList<DAGClient>());
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        for (DAGClient c: shutdownList) {
          try {
            System.err.println("Trying to shutdown DAG");
            c.tryKillDAG();
          } catch (Exception e) {
            // ignore
          }
        }
        try {
          for (TezSessionState s: TezSessionState.getOpenSessions()) {
            System.err.println("Shutting down tez session.");
            s.close(false);
          }
        } catch (Exception e) {
          // ignore
        }
      }
    });
  }

  public TezJobMonitor() {
    console = new LogHelper(LOG);
  }

  /**
   * monitorExecution handles status printing, failures during execution and final
   * status retrieval.
   *
   * @param dagClient client that was used to kick off the job
   * @param txnMgr transaction manager for this operation
   * @param conf configuration file for this operation
   * @return int 0 - success, 1 - killed, 2 - failed
   */
  public int monitorExecution(final DAGClient dagClient, HiveTxnManager txnMgr,
                              HiveConf conf) throws InterruptedException {
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

    shutdownList.add(dagClient);

    console.printInfo("\n");
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_RUN_DAG);
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_SUBMIT_TO_RUNNING);

    while(true) {

      try {
        status = dagClient.getDAGStatus(opts);
        Map<String, Progress> progressMap = status.getVertexProgress();
        DAGStatus.State state = status.getState();
        heartbeater.heartbeat();

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
              console.printInfo("Status: Running (application id: "
                +dagClient.getApplicationId()+")\n");
              for (String s: progressMap.keySet()) {
                perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_RUN_VERTEX + s);
              }
              running = true;
            }

            lastReport = printStatus(progressMap, lastReport, console);
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
        if (!done) {
          Thread.sleep(checkInterval);
        }
      } catch (Exception e) {
        console.printInfo("Exception: "+e.getMessage());
        if (++failedCounter % maxRetryInterval/checkInterval == 0
            || e instanceof InterruptedException) {
          try {
            console.printInfo("Killing DAG...");
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
        } else {
          console.printInfo("Retrying...");
        }
      } finally {
        if (done) {
          if (rc != 0 && status != null) {
            for (String diag: status.getDiagnostics()) {
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

  private String printStatus(Map<String, Progress> progressMap, String lastReport, LogHelper console) {
    StringBuffer reportBuffer = new StringBuffer();

    SortedSet<String> keys = new TreeSet<String>(progressMap.keySet());
    for (String s: keys) {
      Progress progress = progressMap.get(s);
      int complete = progress.getSucceededTaskCount();
      int total = progress.getTotalTaskCount();
      if (total <= 0) {
        reportBuffer.append(String.format("%s: -/-\t", s, complete, total));
      } else {
        if (complete == total && !completed.contains(s)) {
          completed.add(s);
          perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_RUN_VERTEX + s);
        }
        reportBuffer.append(String.format("%s: %d/%d\t", s, complete, total));
      }
    }

    String report = reportBuffer.toString();
    if (!report.equals(lastReport) || System.currentTimeMillis() >= lastPrintTime + printInterval) {
      console.printInfo(report);
      lastPrintTime = System.currentTimeMillis();
    }

    return report;
  }
}
