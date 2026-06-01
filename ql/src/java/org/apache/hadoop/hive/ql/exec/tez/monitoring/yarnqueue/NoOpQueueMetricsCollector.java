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
 * Null Object implementation of {@link QueueMetricsCollector}.
 * Used when queue metrics collection is disabled. Provides safe no-op behavior
 * so that callers don't need null checks.
 * 
 * Thread-safe singleton following the Null Object pattern.
 */
public final class NoOpQueueMetricsCollector implements QueueMetricsCollector {
  /** Singleton instance - thread-safe via static initialization. */
  public static final NoOpQueueMetricsCollector INSTANCE = new NoOpQueueMetricsCollector();
  // Private constructor prevents instantiation
  private NoOpQueueMetricsCollector() {
  }
  @Override
  public QueueMetricsSnapshot getLatestSnapshot() {
    // No metrics available when collection is disabled
    return null;
  }
  @Override
  public String getQueueName() {
    // Return empty string instead of null to avoid NPEs
    return "";
  }
  @Override
  public void shutdown() {
    // No-op: nothing to shut down
  }
  @Override
  public boolean isEnabled() {
    // Metrics collection is disabled for no-op collector
    return false;
  }
}
