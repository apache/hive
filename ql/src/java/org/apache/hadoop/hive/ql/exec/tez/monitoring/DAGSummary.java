package org.apache.hadoop.hive.ql.exec.tez.monitoring;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.MapOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.common.log.InPlaceUpdate;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;


class DAGSummary implements PrintSummary {

  private static final int FILE_HEADER_SEPARATOR_WIDTH = InPlaceUpdate.MIN_TERMINAL_WIDTH + 34;
  private static final String FILE_HEADER_SEPARATOR = new String(new char[FILE_HEADER_SEPARATOR_WIDTH]).replace("\0", "-");

  private static final String FORMATTING_PATTERN = "%10s %12s %16s %13s %14s %13s %12s %14s %15s";
  private static final String FILE_HEADER = String.format(
      FORMATTING_PATTERN,
      "VERTICES",
      "TOTAL_TASKS",
      "FAILED_ATTEMPTS",
      "KILLED_TASKS",
      "DURATION(ms)",
      "CPU_TIME(ms)",
      "GC_TIME(ms)",
      "INPUT_RECORDS",
      "OUTPUT_RECORDS"
  );

  private final DecimalFormat secondsFormatter = new DecimalFormat("#0.00");
  private final NumberFormat commaFormatter = NumberFormat.getNumberInstance(Locale.US);

  private final String hiveCountersGroup;
  private final TezCounters hiveCounters;

  private Map<String, Progress> progressMap;
  private DAGClient dagClient;
  private DAG dag;
  private PerfLogger perfLogger;

  DAGSummary(Map<String, Progress> progressMap, HiveConf hiveConf, DAGClient dagClient,
             DAG dag, PerfLogger perfLogger) {
    this.progressMap = progressMap;
    this.dagClient = dagClient;
    this.dag = dag;
    this.perfLogger = perfLogger;
    this.hiveCountersGroup = HiveConf.getVar(hiveConf, HiveConf.ConfVars.HIVECOUNTERGROUP);
    this.hiveCounters = hiveCounters(dagClient);
  }

  private long hiveInputRecordsFromOtherVertices(String vertexName) {
    List<Vertex> inputVerticesList = dag.getVertex(vertexName).getInputVertices();
    long result = 0;
    for (Vertex inputVertex : inputVerticesList) {
      String intermediateRecordsCounterName = formattedName(
          ReduceSinkOperator.Counter.RECORDS_OUT_INTERMEDIATE.toString(),
          inputVertex.getName()
      );
      String recordsOutCounterName = formattedName(FileSinkOperator.Counter.RECORDS_OUT.toString(),
          inputVertex.getName());
      result += (
          hiveCounterValue(intermediateRecordsCounterName)
              + hiveCounterValue(recordsOutCounterName)
      );
    }
    return result;
  }

  private String formattedName(String counterName, String vertexName) {
    return String.format("%s_", counterName) + vertexName.replace(" ", "_");
  }

  private long getCounterValueByGroupName(TezCounters counters, String pattern, String counterName) {
    TezCounter tezCounter = counters.getGroup(pattern).findCounter(counterName);
    return (tezCounter == null) ? 0 : tezCounter.getValue();
  }

  private long hiveCounterValue(String counterName) {
    return getCounterValueByGroupName(hiveCounters, hiveCountersGroup, counterName);
  }

  private TezCounters hiveCounters(DAGClient dagClient) {
    try {
      return dagClient.getDAGStatus(EnumSet.of(StatusGetOpts.GET_COUNTERS)).getDAGCounters();
    } catch (IOException | TezException e) {
      // best attempt, shouldn't really kill DAG for this
    }
    return null;
  }

  @Override
  public void print(SessionState.LogHelper console) {
    console.printInfo("Task Execution Summary");

  /* If the counters are missing there is no point trying to print progress */
    if (hiveCounters == null) {
      return;
    }

  /* Print the per Vertex summary */
    printHeader(console);
    SortedSet<String> keys = new TreeSet<>(progressMap.keySet());
    Set<StatusGetOpts> statusOptions = new HashSet<>(1);
    statusOptions.add(StatusGetOpts.GET_COUNTERS);
    for (String vertexName : keys) {
      Progress progress = progressMap.get(vertexName);
      if (progress == null) continue;

      VertexStatus vertexStatus = vertexStatus(statusOptions, vertexName);
      if (vertexStatus == null) {
        continue;
      }
      console.printInfo(vertexSummary(vertexName, progress, vertexStatus));
    }
    console.printInfo(FILE_HEADER_SEPARATOR);
  }

  private String vertexSummary(String vertexName, Progress progress, VertexStatus vertexStatus) {
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
        hiveCounterValue(formattedName(MapOperator.Counter.RECORDS_IN.toString(), vertexName))
            + hiveInputRecordsFromOtherVertices(vertexName);

    final long hiveOutputRecords =
        hiveCounterValue(formattedName(FileSinkOperator.Counter.RECORDS_OUT.toString(), vertexName)) +
            hiveCounterValue(formattedName(ReduceSinkOperator.Counter.RECORDS_OUT_INTERMEDIATE.toString(), vertexName));

    final double duration = perfLogger.getDuration(PerfLogger.TEZ_RUN_VERTEX + vertexName);

    return String.format(FORMATTING_PATTERN,
        vertexName,
        progress.getTotalTaskCount(),
        progress.getFailedTaskAttemptCount(),
        progress.getKilledTaskAttemptCount(),
        secondsFormatter.format((duration)),
        commaFormatter.format(cpuTimeMillis),
        commaFormatter.format(gcTimeMillis),
        commaFormatter.format(hiveInputRecords),
        commaFormatter.format(hiveOutputRecords));
  }

  private VertexStatus vertexStatus(Set<StatusGetOpts> statusOptions, String vertexName) {
    try {
      return dagClient.getVertexStatus(vertexName, statusOptions);
    } catch (IOException | TezException e) {
      // best attempt, shouldn't really kill DAG for this
    }
    return null;
  }

  private void printHeader(SessionState.LogHelper console) {
    console.printInfo(FILE_HEADER_SEPARATOR);
    console.printInfo(FILE_HEADER);
    console.printInfo(FILE_HEADER_SEPARATOR);
  }
}
