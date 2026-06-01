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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Collects YARN queue resource metrics using a shared cache to reduce ResourceManager load.
 * Coordinates with other collectors via QueueMetricsCache to prevent duplicate RM calls.
 *
 * Executor pool management (sizing, lifecycle, JMX) is fully delegated to
 * {@link QueueMetricsRefreshPool}. This class focuses solely on per-query/per-queue
 * metrics logic: session registration, refresh scheduling, and cache coordination.
 */
public class YarnQueueMetricsCollector implements QueueMetricsCollector {
  private static final Logger LOG = LoggerFactory.getLogger(YarnQueueMetricsCollector.class);

  private final YarnClient yarnClient;
  private final String queueName;
  private final long refreshIntervalMs;
  private final String queryId;


  /**
   * Creates a collector for the given queue and query. Non-blocking: metrics are
   * fetched asynchronously; the first progress update may show no metrics, subsequent
   * updates will once the first fetch completes (within one refreshIntervalMs).
   *
   * @param yarnClient        Live YarnClient from the Tez session
   * @param queueName         YARN queue this query runs on
   * @param refreshIntervalMs How often to poll YARN RM (ms)
   * @param queryId           DAG name for logging
   * @param hiveConf          Unused (kept for API compatibility)
   */
  public YarnQueueMetricsCollector(YarnClient yarnClient, String queueName, long refreshIntervalMs, String queryId,
      HiveConf hiveConf) {
    if (yarnClient == null) {
      throw new IllegalArgumentException("YarnClient cannot be null");
    }
    if (queueName == null) {
      throw new IllegalArgumentException("Queue name cannot be null");
    }
    if (refreshIntervalMs <= 0) {
      throw new IllegalArgumentException("refreshIntervalMs must be > 0, got: " + refreshIntervalMs);
    }

    this.yarnClient = yarnClient;
    this.queueName = queueName;
    this.refreshIntervalMs = refreshIntervalMs;
    this.queryId = queryId;


    // Register session and start background refresh scheduling.
    initializeSession();

    LOG.info("Started queue metrics collector for queue: {}, refresh interval: {}ms, query: {}", queueName,
        refreshIntervalMs, queryId);
  }


  /**
   * Startup sequence: get or create the cache entry for {@code queueName},
   * register this session's interval, then schedule the refresh task if needed.
   * Concurrent safety: {@link QueueMetricsCache#putPlaceholder} uses
   * {@code putIfAbsent} — two threads seeing null both get back the same entry.
   */
  private void initializeSession() {
    QueueMetricsCache cache = QueueMetricsCache.getInstance();
    QueueMetricsState state = cache.get(queueName);
    if (state == null) {
      state = cache.putPlaceholder(queueName, refreshIntervalMs);
    }
    if (state.registerInterval(refreshIntervalMs)) {
      state.ensureTaskScheduled(QueueMetricsRefreshPool.getInstance(), this::refreshMetrics, queueName);
    }
  }


  private void refreshMetrics() {
    try {
      QueueMetricsState state = QueueMetricsCache.getInstance().get(queueName);
      if (state == null) {
        return;
      }
      if (state.getAgeMs() < state.getMinRefreshIntervalMs()) {
        LOG.debug("Cache entry for queue: {} is fresh, skipping refresh", queueName);
        return;
      }
      if (state.shouldSkipDueToCircuitBreaker(queueName)) {
        return;
      }
      if (!state.tryStartRefresh()) {
        LOG.debug("Another collector is refreshing queue: {}, skipping", queueName);
        return;
      }
      performRefresh(state);
    } catch (Exception e) {
      LOG.error("Unexpected error in refresh task for queue: {}", queueName, e);
    }
  }

  /**
   * Performs the actual refresh operation: fetches metrics from RM and updates cache.
   * Handles refresh lock via try-finally to ensure cleanup even on failure.
   *
   * @param state the queue metrics state holding the refresh lock
   */
  private void performRefresh(QueueMetricsState state) {
    try {
      QueueMetricsSnapshot snapshot = fetchFromRM();
      if (snapshot != null) {
        QueueMetricsCache.getInstance().put(queueName, snapshot, refreshIntervalMs);
        state.recordRefreshSuccess(queueName);
        LOG.debug("Refreshed queue metrics for queue: {}", queueName);
      } else {
        LOG.warn("YARN RM returned no QueueInfo for queue: {}. Check queue name configuration.",
            queueName);
      }
    } catch (Exception e) {
      state.recordRefreshFailure(queueName, e.getMessage());
    } finally {
      state.finishRefresh();
    }
  }

  private QueueMetricsSnapshot fetchFromRM() {
    try {
      QueueInfo queueInfo = yarnClient.getQueueInfo(queueName);
      return queueInfo != null ? new QueueMetricsSnapshot(queueInfo) : null;
    } catch (Exception e) {
      LOG.debug("Failed to fetch queue info from RM for queue: {}: {}", queueName, e.getMessage());
      throw new RuntimeException("RM fetch failed", e);
    }
  }


  /**
   * Returns the latest snapshot from cache (non-blocking). Null if not yet available.
   */
  @Override
  public QueueMetricsSnapshot getLatestSnapshot() {
    QueueMetricsState state = QueueMetricsCache.getInstance().get(queueName);
    return state != null ? state.getSnapshot() : null;
  }

  /**
   * Returns the queue name being monitored.
   */
  @Override
  public String getQueueName() {
    return queueName;
  }

  /**
   * Called when the query finishes. Deregisters this collector's refresh interval from
   * the shared cache state. If other collectors are still monitoring this queue, the
   * refresh task continues at the minimum interval among remaining collectors.
   * <p>
   * Does NOT invalidate the cache entry - it remains available for other queries
   * and automatically expires after 60 minutes of inactivity.
   */
  @Override
  public void shutdown() {
    QueueMetricsState state = QueueMetricsCache.getInstance().get(queueName);
    if (state == null) {
      LOG.info("Cache entry already cleared for queue: {} on shutdown of query: {}", queueName, queryId);
      return;
    }
    LOG.info("Query finished for queue: {}, query: {}", queueName, queryId);
    if (state.deregisterInterval(refreshIntervalMs)) {
      state.ensureTaskScheduled(QueueMetricsRefreshPool.getInstance(), this::refreshMetrics, queueName);
    }
  }

  /**
   * Returns true indicating metrics collection is enabled.
   */
  @Override
  public boolean isEnabled() {
    return true;
  }
}

