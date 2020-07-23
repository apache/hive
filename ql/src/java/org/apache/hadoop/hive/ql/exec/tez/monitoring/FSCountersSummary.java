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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.tez.common.counters.FileSystemCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.dag.api.client.StatusGetOpts;

import java.io.IOException;
import java.util.*;

import static org.apache.hadoop.hive.ql.exec.tez.monitoring.Constants.SEPARATOR;
import static org.apache.hadoop.hive.ql.exec.tez.monitoring.TezJobMonitor.getCounterValueByGroupName;

public class FSCountersSummary implements PrintSummary {

  private static final String FORMATTING_PATTERN = "%10s %15s %13s %18s %18s %13s";
  private static final String HEADER = String.format(FORMATTING_PATTERN,
      "VERTICES", "BYTES_READ", "READ_OPS", "LARGE_READ_OPS", "BYTES_WRITTEN", "WRITE_OPS");

  private Map<String, Progress> progressMap;
  private DAGClient dagClient;

  FSCountersSummary(Map<String, Progress> progressMap, DAGClient dagClient) {
    this.progressMap = progressMap;
    this.dagClient = dagClient;
  }

  @Override
  public void print(SessionState.LogHelper console) {
    console.printInfo("FileSystem Counters Summary");

    SortedSet<String> keys = new TreeSet<>(progressMap.keySet());
    Set<StatusGetOpts> statusOptions = Collections.singleton(StatusGetOpts.GET_COUNTERS);
    // Assuming FileSystem.getAllStatistics() returns all schemes that are accessed on task side
    // as well. If not, we need a way to get all the schemes that are accessed by the tez task/llap.
    for (FileSystem.Statistics statistics : FileSystem.getAllStatistics()) {
      final String scheme = statistics.getScheme().toUpperCase();

      console.printInfo("");
      console.printInfo("Scheme: " + scheme);
      console.printInfo(SEPARATOR);
      console.printInfo(HEADER);
      console.printInfo(SEPARATOR);

      for (String vertexName : keys) {
        TezCounters vertexCounters = vertexCounters(statusOptions, vertexName);
        if (vertexCounters != null) {
          console.printInfo(summary(scheme, vertexName, vertexCounters));
        }
      }

      console.printInfo(SEPARATOR);
    }
  }

  private String summary(String scheme, String vertexName, TezCounters vertexCounters) {
    final String counterGroup = FileSystemCounter.class.getName();
    final long bytesRead = getCounterValueByGroupName(vertexCounters,
        counterGroup, scheme + "_" + FileSystemCounter.BYTES_READ.name());
    final long bytesWritten = getCounterValueByGroupName(vertexCounters,
        counterGroup, scheme + "_" + FileSystemCounter.BYTES_WRITTEN.name());
    final long readOps = getCounterValueByGroupName(vertexCounters,
        counterGroup, scheme + "_" + FileSystemCounter.READ_OPS.name());
    final long largeReadOps = getCounterValueByGroupName(vertexCounters,
        counterGroup, scheme + "_" + FileSystemCounter.LARGE_READ_OPS.name());
    final long writeOps = getCounterValueByGroupName(vertexCounters,
        counterGroup, scheme + "_" + FileSystemCounter.WRITE_OPS.name());

    return String.format(FORMATTING_PATTERN,
        vertexName,
        Utilities.humanReadableByteCount(bytesRead),
        readOps,
        largeReadOps,
        Utilities.humanReadableByteCount(bytesWritten),
        writeOps);
  }

  private TezCounters vertexCounters(Set<StatusGetOpts> statusOptions, String vertexName) {
    try {
      return dagClient.getVertexStatus(vertexName, statusOptions).getVertexCounters();
    } catch (IOException | TezException e) {
      // best attempt, shouldn't really kill DAG for this
    }
    return null;
  }
}
