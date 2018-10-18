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
import org.apache.tez.dag.api.TezConfiguration;
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

  private static final String FILE_HEADER_SEPARATOR = new String(new char[InPlaceUpdate.MIN_TERMINAL_WIDTH]).replace("\0", "-");
  private static final String FORMATTING_PATTERN = "%10s %17s %14s %14s %15s %16s";
  private static final String FILE_HEADER = String.format(
      FORMATTING_PATTERN,
      "VERTICES",
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

  private long hiveInputRecordsFromTezCounters(String vertexName, String inputVertexName) {
    // Get the counters for the input vertex.
    Set<StatusGetOpts> statusOptions = new HashSet<>(1);
    statusOptions.add(StatusGetOpts.GET_COUNTERS);
    VertexStatus inputVertexStatus = vertexStatus(statusOptions, inputVertexName);
    final TezCounters inputVertexCounters = inputVertexStatus.getVertexCounters();

    // eg, group name TaskCounter_Map_7_OUTPUT_Reducer_8, counter name OUTPUT_RECORDS
    String groupName = formattedName("TaskCounter", inputVertexName, vertexName);
    String counterName = "OUTPUT_RECORDS";

    // Do not create counter if it does not exist -
    // instead fall back to default behavior for determining input records.
    TezCounter tezCounter = inputVertexCounters.getGroup(groupName).findCounter(counterName, false);
    if (tezCounter == null) {
      return -1;
    } else {
      return tezCounter.getValue();
    }
  }

  private long hiveInputRecordsFromHiveCounters(String inputVertexName) {
    // The record count from these counters may not be correct if the input vertex has
    // edges to more than one vertex, since this value counts the records going to all
    // destination vertices.

    String intermediateRecordsCounterName = formattedName(
        ReduceSinkOperator.Counter.RECORDS_OUT_INTERMEDIATE.toString(),
        inputVertexName
    );
    String recordsOutCounterName = formattedName(FileSinkOperator.Counter.RECORDS_OUT.toString(),
        inputVertexName);
    return hiveCounterValue(intermediateRecordsCounterName) + hiveCounterValue(recordsOutCounterName);
  }

  private long hiveInputRecordsFromOtherVertices(String vertexName) {
    List<Vertex> inputVerticesList = dag.getVertex(vertexName).getInputVertices();
    long result = 0;
    for (Vertex inputVertex : inputVerticesList) {
      long inputVertexRecords = hiveInputRecordsFromTezCounters(vertexName, inputVertex.getName());
      if (inputVertexRecords < 0) {
        inputVertexRecords = hiveInputRecordsFromHiveCounters(inputVertex.getName());
      }
      result += inputVertexRecords;
    }
    return result;
  }

  private String formattedName(String counterName, String srcVertexName, String destVertexName) {
    return String.format("%s_", counterName) + srcVertexName.replace(" ", "_") + "_OUTPUT_" + destVertexName.replace(" ", "_");
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
