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
package org.apache.hadoop.hive.ql.exec.tez.monitoring;

import org.apache.hadoop.hive.llap.counters.LlapIOCounters;
import org.apache.hadoop.hive.ql.exec.Utilities;
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

public class LLAPioSummary implements PrintSummary {

  private static final String LLAP_SUMMARY_HEADER_FORMAT = "%10s %9s %9s %10s %9s %10s %11s %8s %9s";
  private static final String LLAP_IO_SUMMARY_HEADER = "LLAP IO Summary";
  private static final String LLAP_SUMMARY_HEADER = String.format(LLAP_SUMMARY_HEADER_FORMAT,
      "VERTICES", "ROWGROUPS", "META_HIT", "META_MISS", "DATA_HIT", "DATA_MISS",
      "ALLOCATION", "USED", "TOTAL_IO");



  private final DecimalFormat secondsFormatter = new DecimalFormat("#0.00");
  private Map<String, Progress> progressMap;
  private DAGClient dagClient;
  private boolean first = false;

  LLAPioSummary(Map<String, Progress> progressMap, DAGClient dagClient) {
    this.progressMap = progressMap;
    this.dagClient = dagClient;
  }

  @Override
  public void print(SessionState.LogHelper console) {
    console.printInfo("");
    console.printInfo(LLAP_IO_SUMMARY_HEADER);

    SortedSet<String> keys = new TreeSet<>(progressMap.keySet());
    Set<StatusGetOpts> statusOptions = new HashSet<>(1);
    statusOptions.add(StatusGetOpts.GET_COUNTERS);
    String counterGroup = LlapIOCounters.class.getName();
    for (String vertexName : keys) {
      // Reducers do not benefit from LLAP IO so no point in printing
      if (vertexName.startsWith("Reducer")) {
        continue;
      }
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
    final long selectedRowgroups = getCounterValueByGroupName(vertexCounters,
        counterGroup, LlapIOCounters.SELECTED_ROWGROUPS.name());
    final long metadataCacheHit = getCounterValueByGroupName(vertexCounters,
        counterGroup, LlapIOCounters.METADATA_CACHE_HIT.name());
    final long metadataCacheMiss = getCounterValueByGroupName(vertexCounters,
        counterGroup, LlapIOCounters.METADATA_CACHE_MISS.name());
    final long cacheHitBytes = getCounterValueByGroupName(vertexCounters,
        counterGroup, LlapIOCounters.CACHE_HIT_BYTES.name());
    final long cacheMissBytes = getCounterValueByGroupName(vertexCounters,
        counterGroup, LlapIOCounters.CACHE_MISS_BYTES.name());
    final long allocatedBytes = getCounterValueByGroupName(vertexCounters,
        counterGroup, LlapIOCounters.ALLOCATED_BYTES.name());
    final long allocatedUsedBytes = getCounterValueByGroupName(vertexCounters,
        counterGroup, LlapIOCounters.ALLOCATED_USED_BYTES.name());
    final long totalIoTime = getCounterValueByGroupName(vertexCounters,
        counterGroup, LlapIOCounters.TOTAL_IO_TIME_NS.name());


    return String.format(LLAP_SUMMARY_HEADER_FORMAT,
        vertexName,
        selectedRowgroups,
        metadataCacheHit,
        metadataCacheMiss,
        Utilities.humanReadableByteCount(cacheHitBytes),
        Utilities.humanReadableByteCount(cacheMissBytes),
        Utilities.humanReadableByteCount(allocatedBytes),
        Utilities.humanReadableByteCount(allocatedUsedBytes),
        secondsFormatter.format(totalIoTime / 1000_000_000.0) + "s");
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
