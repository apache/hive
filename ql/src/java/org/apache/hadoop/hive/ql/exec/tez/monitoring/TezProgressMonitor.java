package org.apache.hadoop.hive.ql.exec.tez.monitoring;

import org.apache.hadoop.hive.ql.log.ProgressMonitor;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.*;

import java.io.IOException;
import java.util.*;

import static org.apache.tez.dag.api.client.DAGStatus.State.KILLED;

class TezProgressMonitor implements ProgressMonitor {
  private static final int COLUMN_1_WIDTH = 16;

  private final DAGStatus status;
  private final DAGClient dagClient;

  private final Map<String, BaseWork> workMap;
  private Map<String, Progress> progressMap;
  private final SessionState.LogHelper console;
  private final long executionStartTime;

  TezProgressMonitor(DAGClient dagClient, Map<String, BaseWork> workMap, SessionState.LogHelper console,
                     long executionStartTime) throws IOException, TezException {
    status = dagClient.getDAGStatus(new HashSet<StatusGetOpts>());
    progressMap = status.getVertexProgress();
    this.dagClient = dagClient;
    this.workMap = workMap;
    this.console = console;
    this.executionStartTime = executionStartTime;
  }


  public List<String> headers() {
    return Arrays.asList("VERTICES", "MODE", "STATUS", "TOTAL", "COMPLETED", "RUNNING", "PENDING", "FAILED", "KILLED");
  }

  public List<List<String>> rows() {
    try {
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
    return String.format("VERTICES: %02d/%02d", completed(), progressMap.keySet().size());
  }

  @Override
  public long startTime() {
    return executionStartTime;
  }

  @Override
  public double progressedPercentage() {
    int sumTotal = 0, sumComplete = 0;
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
  }

  @Override
  public String executionStatus() {
    return this.status.getState().name();
  }

  private int completed() {
    Set<String> completed = new HashSet<>();
    for (String s : progressMap.keySet()) {
      Progress progress = progressMap.get(s);
      final int complete = progress.getSucceededTaskCount();
      final int total = progress.getTotalTaskCount();
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
}
