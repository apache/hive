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

import org.apache.hadoop.hive.common.log.ProgressMonitor;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.dag.api.client.VertexStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.tez.dag.api.client.DAGStatus.State.KILLED;

class TezProgressMonitor implements ProgressMonitor {
  private static final int COLUMN_1_WIDTH = 16;
  private final List<BaseWork> topSortedWork;
  private final SessionState.LogHelper console;
  private final long executionStartTime;
  private final DAGStatus status;
  Map<String, VertexStatus> vertexStatusMap = new HashMap<>();
  Map<String, VertexProgress> progressCountsMap = new HashMap<>();

  /**
   * Try to get most the data required from dagClient in the constructor itself so that even after
   * the tez job has finished this object can be used for later use.s
   */
  TezProgressMonitor(DAGClient dagClient, DAGStatus status, List<BaseWork> topSortedWork,
      Map<String, Progress> progressMap, SessionState.LogHelper console, long executionStartTime)
      throws IOException, TezException {
    this.status = status;
    this.topSortedWork = topSortedWork;
    this.console = console;
    this.executionStartTime = executionStartTime;
    for (Map.Entry<String, Progress> entry : progressMap.entrySet()) {
      String vertexName = entry.getKey();
      progressCountsMap.put(vertexName, new VertexProgress(entry.getValue(), status.getState()));
      try {
        vertexStatusMap.put(vertexName, dagClient.getVertexStatus(vertexName, null));
      } catch (IOException e) {
        // best attempt, shouldn't really kill DAG for this
      }
    }
  }

  public List<String> headers() {
    return Arrays.asList(
        "VERTICES",
        "MODE",
        "STATUS",
        "TOTAL",
        "COMPLETED",
        "RUNNING",
        "PENDING",
        "FAILED",
        "KILLED"
    );
  }

  public List<List<String>> rows() {
    try {
      List<List<String>> results = new ArrayList<>();
      for (BaseWork baseWork : topSortedWork) {
        String vertexName = baseWork.getName();
        VertexProgress progress = progressCountsMap.get(vertexName);
        if (progress != null) {
          // Map 1 .......... container  SUCCEEDED      7          7        0        0       0       0

          results.add(
            Arrays.asList(
              getNameWithProgress(vertexName, progress.succeededTaskCount, progress.totalTaskCount),
              getMode(baseWork),
              progress.vertexStatus(vertexStatusMap.get(vertexName)),
              progress.total(),
              progress.completed(),
              progress.running(),
              progress.pending(),
              progress.failed(),
              progress.killed()
            )
          );
        }
      }
      return results;
    } catch (Exception e) {
      console.printInfo(
          "Getting  Progress Bar table rows failed: " + e.getMessage() + " stack trace: " + Arrays
              .toString(e.getStackTrace())
      );
    }
    return Collections.emptyList();
  }

  // -------------------------------------------------------------------------------
  // VERTICES: 03/04            [=================>>-----] 86%  ELAPSED TIME: 1.71 s
  // -------------------------------------------------------------------------------
  // contains footerSummary , progressedPercentage, starTime

  @Override
  public String footerSummary() {
    return String.format("VERTICES: %02d/%02d", completed(), progressCountsMap.keySet().size());
  }

  @Override
  public long startTime() {
    return executionStartTime;
  }

  @Override
  public double progressedPercentage() {
    int sumTotal = 0, sumComplete = 0;
    for (String s : progressCountsMap.keySet()) {
      VertexProgress progress = progressCountsMap.get(s);
      final int complete = progress.succeededTaskCount;
      final int total = progress.totalTaskCount;
      if (total > 0) {
        sumTotal += total;
        sumComplete += complete;
      }
    }
    return (sumTotal == 0) ? 0.0f : (float) sumComplete / (float) sumTotal;
  }

  @Override
  public String executionStatus() {
    return this.status.getState().name();
  }

  private int completed() {
    Set<String> completed = new HashSet<>();
    for (String s : progressCountsMap.keySet()) {
      VertexProgress progress = progressCountsMap.get(s);
      final int complete = progress.succeededTaskCount;
      final int total = progress.totalTaskCount;
      if (total > 0) {
        if (complete == total) {
          completed.add(s);
        }
      }
    }
    return completed.size();
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

  private String getMode(BaseWork work) {
    String mode = "container";
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

  static class VertexProgress {
    private final int totalTaskCount;
    private final int succeededTaskCount;
    private final int failedTaskAttemptCount;
    private final long killedTaskAttemptCount;
    private final int runningTaskCount;
    private final DAGStatus.State dagState;

    VertexProgress(Progress progress, DAGStatus.State dagState) {
      this(progress.getTotalTaskCount(), progress.getSucceededTaskCount(),
          progress.getFailedTaskAttemptCount(), progress.getKilledTaskAttemptCount(),
          progress.getRunningTaskCount(), dagState);
    }

    VertexProgress(int totalTaskCount, int succeededTaskCount, int failedTaskAttemptCount,
        int killedTaskAttemptCount, int runningTaskCount, DAGStatus.State dagState) {
      this.totalTaskCount = totalTaskCount;
      this.succeededTaskCount = succeededTaskCount;
      this.failedTaskAttemptCount = failedTaskAttemptCount;
      this.killedTaskAttemptCount = killedTaskAttemptCount;
      this.runningTaskCount = runningTaskCount;
      this.dagState = dagState;
    }

    boolean isRunning() {
      return succeededTaskCount < totalTaskCount && (succeededTaskCount > 0 || runningTaskCount > 0
          || failedTaskAttemptCount > 0);
    }

    String vertexStatus(VertexStatus vertexStatus) {
      // To get vertex status we can use DAGClient.getVertexStatus(), but it will be expensive to
      // get status from AM for every refresh of the UI. Lets infer the state from task counts.
      // Only if DAG is FAILED or KILLED the vertex status is fetched from AM.
      VertexStatus.State vertexState = VertexStatus.State.INITIALIZING;
      if (totalTaskCount > 0) {
        vertexState = VertexStatus.State.INITED;
      }

      // RUNNING state
      if (isRunning()) {
        vertexState = VertexStatus.State.RUNNING;
      }

      // SUCCEEDED state
      if (succeededTaskCount == totalTaskCount) {
        vertexState = VertexStatus.State.SUCCEEDED;
      }

      // DAG might have been killed, lets try to get vertex state from AM before dying
      // KILLED or FAILED state
      if (dagState == KILLED) {
        if (vertexStatus != null) {
          vertexState = vertexStatus.getState();
        }
      }
      return vertexState.toString();
    }

    //    "TOTAL", "COMPLETED", "RUNNING", "PENDING", "FAILED", "KILLED"

    String total() {
      return String.valueOf(totalTaskCount);
    }

    String completed() {
      return String.valueOf(succeededTaskCount);
    }

    String running() {
      return String.valueOf(runningTaskCount);
    }

    String pending() {
      return String.valueOf(totalTaskCount - succeededTaskCount - runningTaskCount);
    }

    String failed() {
      return String.valueOf(failedTaskAttemptCount);
    }

    String killed() {
      return String.valueOf(killedTaskAttemptCount);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      VertexProgress that = (VertexProgress) o;

      if (totalTaskCount != that.totalTaskCount)
        return false;
      if (succeededTaskCount != that.succeededTaskCount)
        return false;
      if (failedTaskAttemptCount != that.failedTaskAttemptCount)
        return false;
      if (killedTaskAttemptCount != that.killedTaskAttemptCount)
        return false;
      if (runningTaskCount != that.runningTaskCount)
        return false;
      return dagState == that.dagState;
    }

    @Override
    public int hashCode() {
      int result = totalTaskCount;
      result = 31 * result + succeededTaskCount;
      result = 31 * result + failedTaskAttemptCount;
      result = 31 * result + (int) (killedTaskAttemptCount ^ (killedTaskAttemptCount >>> 32));
      result = 31 * result + runningTaskCount;
      result = 31 * result + dagState.hashCode();
      return result;
    }
  }
}
