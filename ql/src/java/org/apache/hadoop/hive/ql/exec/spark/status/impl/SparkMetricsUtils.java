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
package org.apache.hadoop.hive.ql.exec.spark.status.impl;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hive.spark.client.metrics.Metrics;
import org.apache.hive.spark.client.metrics.ShuffleReadMetrics;

final class SparkMetricsUtils {

  private final static String EXECUTOR_DESERIALIZE_TIME = "ExecutorDeserializeTime";
  private final static String EXECUTOR_RUN_TIME = "ExecutorRunTime";
  private final static String RESULT_SIZE = "ResultSize";
  private final static String JVM_GC_TIME = "JvmGCTime";
  private final static String RESULT_SERIALIZATION_TIME = "ResultSerializationTime";
  private final static String MEMORY_BYTES_SPLIED = "MemoryBytesSpilled";
  private final static String DISK_BYTES_SPILLED = "DiskBytesSpilled";
  private final static String BYTES_READ = "BytesRead";
  private final static String REMOTE_BLOCKS_FETCHED = "RemoteBlocksFetched";
  private final static String LOCAL_BLOCKS_FETCHED = "LocalBlocksFetched";
  private final static String TOTAL_BLOCKS_FETCHED = "TotalBlocksFetched";
  private final static String FETCH_WAIT_TIME = "FetchWaitTime";
  private final static String REMOTE_BYTES_READ = "RemoteBytesRead";
  private final static String SHUFFLE_BYTES_WRITTEN = "ShuffleBytesWritten";
  private final static String SHUFFLE_WRITE_TIME = "ShuffleWriteTime";

  private SparkMetricsUtils(){}

  static Map<String, Long> collectMetrics(Metrics allMetrics) {
    Map<String, Long> results = new LinkedHashMap<String, Long>();
    results.put(EXECUTOR_DESERIALIZE_TIME, allMetrics.executorDeserializeTime);
    results.put(EXECUTOR_RUN_TIME, allMetrics.executorRunTime);
    results.put(RESULT_SIZE, allMetrics.resultSize);
    results.put(JVM_GC_TIME, allMetrics.jvmGCTime);
    results.put(RESULT_SERIALIZATION_TIME, allMetrics.resultSerializationTime);
    results.put(MEMORY_BYTES_SPLIED, allMetrics.memoryBytesSpilled);
    results.put(DISK_BYTES_SPILLED, allMetrics.diskBytesSpilled);
    if (allMetrics.inputMetrics != null) {
      results.put(BYTES_READ, allMetrics.inputMetrics.bytesRead);
    }
    if (allMetrics.shuffleReadMetrics != null) {
      ShuffleReadMetrics shuffleReadMetrics = allMetrics.shuffleReadMetrics;
      long rbf = shuffleReadMetrics.remoteBlocksFetched;
      long lbf = shuffleReadMetrics.localBlocksFetched;
      results.put(REMOTE_BLOCKS_FETCHED, rbf);
      results.put(LOCAL_BLOCKS_FETCHED, lbf);
      results.put(TOTAL_BLOCKS_FETCHED, rbf + lbf);
      results.put(FETCH_WAIT_TIME, shuffleReadMetrics.fetchWaitTime);
      results.put(REMOTE_BYTES_READ, shuffleReadMetrics.remoteBytesRead);
    }
    if (allMetrics.shuffleWriteMetrics != null) {
      results.put(SHUFFLE_BYTES_WRITTEN, allMetrics.shuffleWriteMetrics.shuffleBytesWritten);
      results.put(SHUFFLE_WRITE_TIME, allMetrics.shuffleWriteMetrics.shuffleWriteTime);
    }
    return results;
  }

}
