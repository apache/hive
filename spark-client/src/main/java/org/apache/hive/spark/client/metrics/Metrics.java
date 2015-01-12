/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.spark.client.metrics;

import java.io.Serializable;

import org.apache.spark.executor.TaskMetrics;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;

/**
 * Metrics tracked during the execution of a job.
 *
 * Depending on how the metrics object is obtained (by calling methods in the `MetricsCollection`
 * class), metrics will refer to one or more tasks.
 */
@InterfaceAudience.Private
public class Metrics implements Serializable {

  /** Time taken on the executor to deserialize tasks. */
  public final long executorDeserializeTime;
  /** Time the executor spends actually running the task (including fetching shuffle data). */
  public final long executorRunTime;
  /** The number of bytes sent back to the driver by tasks. */
  public final long resultSize;
  /** Amount of time the JVM spent in garbage collection while executing tasks. */
  public final long jvmGCTime;
  /** Amount of time spent serializing task results. */
  public final long resultSerializationTime;
  /** The number of in-memory bytes spilled by tasks. */
  public final long memoryBytesSpilled;
  /** The number of on-disk bytes spilled by tasks. */
  public final long diskBytesSpilled;
  /** If tasks read from a HadoopRDD or from persisted data, metrics on how much data was read. */
  public final InputMetrics inputMetrics;
  /**
   * If tasks read from shuffle output, metrics on getting shuffle data. This includes read metrics
   * aggregated over all the tasks' shuffle dependencies.
   */
  public final ShuffleReadMetrics shuffleReadMetrics;
  /** If tasks wrote to shuffle output, metrics on the written shuffle data. */
  public final ShuffleWriteMetrics shuffleWriteMetrics;

  private Metrics() {
    // For Serialization only.
    this(0L, 0L, 0L, 0L, 0L, 0L, 0L, null, null, null);
  }

  public Metrics(
      long executorDeserializeTime,
      long executorRunTime,
      long resultSize,
      long jvmGCTime,
      long resultSerializationTime,
      long memoryBytesSpilled,
      long diskBytesSpilled,
      InputMetrics inputMetrics,
      ShuffleReadMetrics shuffleReadMetrics,
      ShuffleWriteMetrics shuffleWriteMetrics) {
    this.executorDeserializeTime = executorDeserializeTime;
    this.executorRunTime = executorRunTime;
    this.resultSize = resultSize;
    this.jvmGCTime = jvmGCTime;
    this.resultSerializationTime = resultSerializationTime;
    this.memoryBytesSpilled = memoryBytesSpilled;
    this.diskBytesSpilled = diskBytesSpilled;
    this.inputMetrics = inputMetrics;
    this.shuffleReadMetrics = shuffleReadMetrics;
    this.shuffleWriteMetrics = shuffleWriteMetrics;
  }

  public Metrics(TaskMetrics metrics) {
    this(
      metrics.executorDeserializeTime(),
      metrics.executorRunTime(),
      metrics.resultSize(),
      metrics.jvmGCTime(),
      metrics.resultSerializationTime(),
      metrics.memoryBytesSpilled(),
      metrics.diskBytesSpilled(),
      optionalInputMetric(metrics),
      optionalShuffleReadMetric(metrics),
      optionalShuffleWriteMetrics(metrics));
  }

  private static InputMetrics optionalInputMetric(TaskMetrics metrics) {
    return metrics.inputMetrics().isDefined() ? new InputMetrics(metrics) : null;
  }

  private static ShuffleReadMetrics optionalShuffleReadMetric(TaskMetrics metrics) {
    return metrics.shuffleReadMetrics().isDefined() ? new ShuffleReadMetrics(metrics) : null;
  }

  private static ShuffleWriteMetrics optionalShuffleWriteMetrics(TaskMetrics metrics) {
    return metrics.shuffleWriteMetrics().isDefined() ? new ShuffleWriteMetrics(metrics) : null;
  }

}
