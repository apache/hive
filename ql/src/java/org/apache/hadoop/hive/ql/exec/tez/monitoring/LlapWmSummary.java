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

import org.apache.hadoop.hive.llap.counters.LlapWmCounters;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.dag.api.client.StatusGetOpts;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

import static org.apache.hadoop.hive.ql.exec.tez.monitoring.Constants.SEPARATOR;
import static org.apache.hadoop.hive.ql.exec.tez.monitoring.TezJobMonitor.getCounterValueByGroupName;

public class LlapWmSummary implements PrintSummary {

  private static final String LLAP_SUMMARY_HEADER_FORMAT = "%10s %20s %20s %20s %20s";
  private static final String LLAP_SUMMARY_TITLE = "LLAP WM Summary";
  private static final String LLAP_SUMMARY_HEADER = String.format(LLAP_SUMMARY_HEADER_FORMAT,
      "VERTICES", "SPECULATIVE QUEUED", "SPECULATIVE RUNNING",
      "GUARANTEED QUEUED", "GUARANTEED RUNNING");



  private final DecimalFormat secondsFormatter = new DecimalFormat("#0.00");
  private Map<String, Progress> progressMap;
  private DAGClient dagClient;
  private boolean first = false;

  LlapWmSummary(Map<String, Progress> progressMap, DAGClient dagClient) {
    this.progressMap = progressMap;
    this.dagClient = dagClient;
  }

  @Override
  public void print(SessionState.LogHelper console) {
    console.printInfo("");
    console.printInfo(LLAP_SUMMARY_TITLE);

    SortedSet<String> keys = new TreeSet<>(progressMap.keySet());
    Set<StatusGetOpts> statusOptions = Collections.singleton(StatusGetOpts.GET_COUNTERS);
    String counterGroup = LlapWmCounters.class.getName();
    for (String vertexName : keys) {
      TezCounters vertexCounters = vertexCounter(statusOptions, vertexName);
      if (vertexCounters != null) {
        if (!first) {
          console.printInfo(SEPARATOR);
          console.printInfo(LLAP_SUMMARY_HEADER);
          console.printInfo(SEPARATOR);
          first = true;
        }
        console.printInfo(vertexSummary(vertexName, counterGroup, vertexCounters));
      }
    }
    console.printInfo(SEPARATOR);
    console.printInfo("");
  }

  private String vertexSummary(String vertexName, String counterGroup, TezCounters vertexCounters) {
    final long sq = getCounterValueByGroupName(
        vertexCounters, counterGroup, LlapWmCounters.SPECULATIVE_QUEUED_NS.name());
    final long sr = getCounterValueByGroupName(
        vertexCounters, counterGroup, LlapWmCounters.SPECULATIVE_RUNNING_NS.name());
    final long gq = getCounterValueByGroupName(
        vertexCounters, counterGroup, LlapWmCounters.GUARANTEED_QUEUED_NS.name());
    final long gr = getCounterValueByGroupName(
        vertexCounters, counterGroup, LlapWmCounters.GUARANTEED_RUNNING_NS.name());



    return String.format(LLAP_SUMMARY_HEADER_FORMAT, vertexName,
        secondsFormatter.format(sq / 1000_000_000.0) + "s",
        secondsFormatter.format(sr / 1000_000_000.0) + "s",
        secondsFormatter.format(gq / 1000_000_000.0) + "s",
        secondsFormatter.format(gr / 1000_000_000.0) + "s");
  }

  private TezCounters vertexCounter(Set<StatusGetOpts> statusOptions, String vertexName) {
    try {
      return dagClient.getVertexStatus(vertexName, statusOptions).getVertexCounters();
    } catch (IOException | TezException e) {
      // best attempt, shouldn't really kill DAG for this
    }
    return null;
  }

}
