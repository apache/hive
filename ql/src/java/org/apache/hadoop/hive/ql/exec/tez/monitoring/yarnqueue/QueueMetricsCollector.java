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
package org.apache.hadoop.hive.ql.exec.tez.monitoring.yarnqueue;
/**
 * Interface for collecting YARN queue metrics.
 * Implementations include the active collector ({@link YarnQueueMetricsCollector})
 * and a no-op variant ({@link NoOpQueueMetricsCollector}) following the Null Object pattern.
 */
public interface QueueMetricsCollector {
  /**
   * Returns the latest queue metrics snapshot.
   * 
   * @return snapshot of queue metrics, or null if not available
   */
  QueueMetricsSnapshot getLatestSnapshot();
  /**
   * Returns the name of the queue being monitored.
   * 
   * @return queue name
   */
  String getQueueName();
  /**
   * Shuts down the metrics collector and releases resources.
   * Safe to call multiple times.
   */
  void shutdown();
  /**
   * Returns whether queue metrics collection is enabled for this collector.
   *
   * @return true if metrics collection is active, false if disabled (no-op collector)
   */
  boolean isEnabled();
}
