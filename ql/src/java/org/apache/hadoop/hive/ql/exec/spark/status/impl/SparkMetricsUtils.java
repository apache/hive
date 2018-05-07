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
package org.apache.hadoop.hive.ql.exec.spark.status.impl;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.spark.Statistic.SparkStatisticsNames;
import org.apache.hive.spark.client.metrics.Metrics;
import org.apache.hive.spark.client.metrics.ShuffleReadMetrics;

final class SparkMetricsUtils {

  private SparkMetricsUtils(){}

  static Map<String, Long> collectMetrics(Metrics allMetrics) {
    Map<String, Long> results = new LinkedHashMap<String, Long>();
    results.put(SparkStatisticsNames.EXECUTOR_DESERIALIZE_TIME, allMetrics.executorDeserializeTime);
    results.put(SparkStatisticsNames.EXECUTOR_DESERIALIZE_CPU_TIME,
            allMetrics.executorDeserializeCpuTime);
    results.put(SparkStatisticsNames.EXECUTOR_RUN_TIME, allMetrics.executorRunTime);
    results.put(SparkStatisticsNames.EXECUTOR_CPU_TIME, allMetrics.executorCpuTime);
    results.put(SparkStatisticsNames.RESULT_SIZE, allMetrics.resultSize);
    results.put(SparkStatisticsNames.JVM_GC_TIME, allMetrics.jvmGCTime);
    results.put(SparkStatisticsNames.RESULT_SERIALIZATION_TIME, allMetrics.resultSerializationTime);
    results.put(SparkStatisticsNames.MEMORY_BYTES_SPILLED, allMetrics.memoryBytesSpilled);
    results.put(SparkStatisticsNames.DISK_BYTES_SPILLED, allMetrics.diskBytesSpilled);
    results.put(SparkStatisticsNames.TASK_DURATION_TIME, allMetrics.taskDurationTime);
    if (allMetrics.inputMetrics != null) {
      results.put(SparkStatisticsNames.BYTES_READ, allMetrics.inputMetrics.bytesRead);
    }
    if (allMetrics.shuffleReadMetrics != null) {
      ShuffleReadMetrics shuffleReadMetrics = allMetrics.shuffleReadMetrics;
      long rbf = shuffleReadMetrics.remoteBlocksFetched;
      long lbf = shuffleReadMetrics.localBlocksFetched;
      results.put(SparkStatisticsNames.REMOTE_BLOCKS_FETCHED, rbf);
      results.put(SparkStatisticsNames.LOCAL_BLOCKS_FETCHED, lbf);
      results.put(SparkStatisticsNames.TOTAL_BLOCKS_FETCHED, rbf + lbf);
      results.put(SparkStatisticsNames.FETCH_WAIT_TIME, shuffleReadMetrics.fetchWaitTime);
      results.put(SparkStatisticsNames.REMOTE_BYTES_READ, shuffleReadMetrics.remoteBytesRead);
    }
    if (allMetrics.shuffleWriteMetrics != null) {
      results.put(SparkStatisticsNames.SHUFFLE_BYTES_WRITTEN, allMetrics.shuffleWriteMetrics.shuffleBytesWritten);
      results.put(SparkStatisticsNames.SHUFFLE_WRITE_TIME, allMetrics.shuffleWriteMetrics.shuffleWriteTime);
    }
    return results;
  }

}
