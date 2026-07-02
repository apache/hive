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

package org.apache.hadoop.hive.ql.exec.tez;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Collects YARN queue resource metrics in the background using a scheduled executor.
 * Provides thread-safe access to the latest metrics snapshot.
 * <p>
 * Implements {@link AutoCloseable} to support try-with-resources for automatic
 * resource cleanup. The {@link #close()} method is equivalent to {@link #shutdown()}.
 */
public class YarnQueueMetricsCollector implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(YarnQueueMetricsCollector.class);
  private static final Random RANDOM = new Random();

  private final YarnClient yarnClient;
  private final String queueName;
  private final ScheduledExecutorService executorService;
  private final AtomicReference<QueueMetricsSnapshot> snapshotRef;
  private final AtomicBoolean isShutdown;

  // Circuit breaker for handling repeated failures
  private int consecutiveFailures = 0;
  private static final int MAX_CONSECUTIVE_FAILURES = 5;
  private static final int BACKOFF_THRESHOLD = 3;

  /**
   * Creates a new metrics collector that immediately begins collecting queue metrics.
   *
   * @param yarnClient The YarnClient to use for querying queue info
   * @param queueName The queue name to monitor
   * @param refreshIntervalMs How often to refresh metrics in milliseconds
   * @param queryId The query ID for thread naming
   * @throws IllegalArgumentException if yarnClient or queueName is null
   */
  public YarnQueueMetricsCollector(YarnClient yarnClient, String queueName,
                                    long refreshIntervalMs, String queryId) {
    if (yarnClient == null) {
      throw new IllegalArgumentException("YarnClient cannot be null");
    }
    if (queueName == null) {
      throw new IllegalArgumentException("Queue name cannot be null");
    }

    this.yarnClient = yarnClient;
    this.queueName = queueName;
    this.snapshotRef = new AtomicReference<>(null);
    this.isShutdown = new AtomicBoolean(false);

    // Create named daemon thread for metrics collection
    this.executorService = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder()
            .setNameFormat("yarn-queue-metrics-collector-" + queryId)
            .setDaemon(true)
            .build()
    );

    try {
      // Perform eager initial collection
      collectMetrics();

      // Add random jitter (0–20 % of refresh interval) to prevent thundering herd:
      // when many queries start simultaneously they would otherwise all hit YARN RM
      // at the same fixed intervals, causing load spikes.
      // RANDOM.nextLong(1, 100) returns [1, 99], divide by 100 to get percentage, then multiply by 0.2 for 20% max.
      long jitter = (long) (refreshIntervalMs * RANDOM.nextLong(1, 100) / 100.0 * 0.2);
      long initialDelay = refreshIntervalMs + jitter;

      // Schedule periodic collection with jittered initial delay
      executorService.scheduleWithFixedDelay(
              () -> {
                try {
                  collectMetrics();
                } catch (Exception e) {
                  LOG.error("Unexpected error in scheduled metrics collection for queue {}: {}",
                          queueName, e.getMessage(), e);
                }
              },
              initialDelay,
              refreshIntervalMs,
              TimeUnit.MILLISECONDS
      );

      LOG.info("Started YARN queue metrics collector for queue: {}, refresh interval: {}ms, initial delay: {}ms",
          queueName, refreshIntervalMs, initialDelay);
    } catch (IllegalArgumentException e) {
      // scheduleWithFixedDelay rejects a zero or negative period; clean up and
      // rethrow with context so the caller can log and skip gracefully.
      executorService.shutdownNow();
      throw new IllegalArgumentException(
          "Invalid refresh interval " + refreshIntervalMs + "ms for queue: " + queueName, e);
    } catch (RuntimeException e) {
      // Any other runtime failure during initialisation — clean up to prevent thread leak.
      LOG.error("Failed to initialize metrics collector for queue {}, shutting down executor",
          queueName, e);
      executorService.shutdownNow();
      throw new IllegalStateException(
          "Failed to initialize YARN queue metrics collector for queue: " + queueName, e);
    }
  }

  /**
   * Checks if an exception is or was caused by an InterruptedException.
   *
   * @param e The exception to check
   * @return true if the exception is an InterruptedException or has one as its cause
   */
  private boolean isInterruptedException(Exception e) {
    return e instanceof InterruptedException || e.getCause() instanceof InterruptedException;
  }

  /**
   * Collects queue metrics and updates the snapshot.
   * Handles all exceptions gracefully by setting snapshot to null.
   * Implements circuit breaker pattern to back off on repeated failures.
   */
  private void collectMetrics() {
    // Circuit breaker: Skip collection if too many consecutive failures
    // This prevents hammering a struggling YARN ResourceManager
    if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
      if (consecutiveFailures == MAX_CONSECUTIVE_FAILURES) {
        LOG.warn("Queue metrics collection has failed {} times consecutively for queue {}. " +
                "Temporarily reducing collection attempts to avoid overloading YARN RM. " +
                "Will retry periodically.", MAX_CONSECUTIVE_FAILURES, queueName);
        consecutiveFailures++; // Increment to avoid repeated logging
      }
      // Still attempt collection occasionally, but skip most attempts
      if (RANDOM.nextDouble() > 0.1) {  // Only try 10% of the time
        return;
      }
    }

    try {
      QueueInfo queueInfo = yarnClient.getQueueInfo(queueName);
      if (queueInfo != null) {
        QueueMetricsSnapshot snapshot = new QueueMetricsSnapshot(queueInfo);
        snapshotRef.set(snapshot);

        // Success - reset circuit breaker
        if (consecutiveFailures > 0) {
          LOG.info("Queue metrics collection recovered for queue {} after {} failures",
                   queueName, consecutiveFailures);
          consecutiveFailures = 0;
        }

        LOG.debug("Collected queue metrics for {}: memory={}/{} GB, vCores={}/{}",
            queueName, snapshot.memoryUsedGB, snapshot.memoryTotalGB,
            snapshot.vCoresUsed, snapshot.vCoresTotal);
      } else {
        // Null QueueInfo indicates queue doesn't exist or is inaccessible (e.g., wrong name,
        // deleted queue, or permission denied). This is likely a configuration issue, not a
        // transient failure, so we don't increment consecutiveFailures to avoid triggering
        // the circuit breaker. The snapshot remains null, but we'll keep trying each interval.
        LOG.warn("QueueInfo is null for queue: {} - queue may not exist or is inaccessible. " +
                 "Check queue name configuration.", queueName);
        snapshotRef.set(null);
      }
    } catch (Exception e) {
      if (isInterruptedException(e)) {
        LOG.debug("Metrics collection interrupted for queue: {}", queueName);
        Thread.currentThread().interrupt();
        // Don't increment failure counter or set snapshot to null, preserve last good state on interrupt
        return;
      }

      // Increment failure counter
      consecutiveFailures++;

      // Log warnings for first few failures, then reduce logging frequency
      if (consecutiveFailures <= BACKOFF_THRESHOLD) {
        LOG.warn("Failed to collect queue metrics for queue {} (failure {} of {}): {}",
                 queueName, consecutiveFailures, MAX_CONSECUTIVE_FAILURES, e.getMessage());
      } else if (consecutiveFailures == MAX_CONSECUTIVE_FAILURES) {
        LOG.warn("Queue metrics collection failing repeatedly for queue {} ({} consecutive failures). " +
                 "This may indicate YARN RM is under heavy load or unreachable.",
                 queueName, consecutiveFailures);
      }

      LOG.debug("Full exception for queue metrics collection failure", e);
      snapshotRef.set(null);
    }
  }

  /**
   * Gets the latest metrics snapshot in a thread-safe manner.
   *
   * @return The latest snapshot, or null if collection failed or not yet completed
   */
  public QueueMetricsSnapshot getLatestSnapshot() {
    return snapshotRef.get();
  }

  /**
   * Gets the queue name being monitored.
   *
   * @return The queue name
   */
  public String getQueueName() {
    return queueName;
  }

  /**
   * Closes this collector, releasing all resources. Equivalent to calling {@link #shutdown()}.
   * Enables use with try-with-resources statements.
   */
  @Override
  public void close() {
    shutdown();
  }

  /**
   * Shuts down the metrics collector gracefully.
   * This method is idempotent and thread-safe.
   */
  public synchronized void shutdown() {
    if (isShutdown.getAndSet(true)) {
      return; // Already shut down
    }

    LOG.info("Shutting down YARN queue metrics collector for queue: {}", queueName);

    try {
      executorService.shutdownNow();
      boolean terminated = executorService.awaitTermination(5, TimeUnit.SECONDS);
      if (!terminated) {
        LOG.warn("Metrics collector for queue {} did not terminate within timeout", queueName);
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while shutting down metrics collector for queue: {}", queueName);
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Immutable snapshot of queue metrics at a point in time.
   */
  public static final class QueueMetricsSnapshot {
    private final float memoryUsedGB;
    private final float memoryTotalGB;
    private final int vCoresUsed;
    private final int vCoresTotal;
    private final float capacityPercentage;
    private final float currentCapacityPercentage;
    private final int runningApps;
    private final int pendingApps;
    private final int allocatedContainers;
    private final int pendingContainers;
    private final long collectionTimestamp;

    /**
     * Creates a snapshot from QueueInfo.
     *
     * @param queueInfo The queue info to extract metrics from
     * @throws IllegalArgumentException if queueInfo is null
     */
    public QueueMetricsSnapshot(QueueInfo queueInfo) {
      if (queueInfo == null) {
        throw new IllegalArgumentException("QueueInfo cannot be null");
      }

      this.collectionTimestamp = System.currentTimeMillis();

      // Extract queue statistics with null-safe handling
      QueueStatistics stats = queueInfo.getQueueStatistics();
      if (stats != null) {
        // Convert memory from MB to GB. Total = Allocated + Available
        this.memoryUsedGB = stats.getAllocatedMemoryMB() / 1024.0f;
        this.memoryTotalGB = (stats.getAllocatedMemoryMB() + stats.getAvailableMemoryMB()) / 1024.0f;
        this.vCoresUsed = (int) stats.getAllocatedVCores();
        this.vCoresTotal = (int) (stats.getAllocatedVCores() + stats.getAvailableVCores());
        this.runningApps = (int) stats.getNumAppsRunning();
        this.pendingApps = (int) stats.getNumAppsPending();
        this.allocatedContainers = (int) stats.getAllocatedContainers();
        this.pendingContainers = (int) stats.getPendingContainers();
      } else {
        LOG.debug("QueueStatistics is null for queue, using zero values");
        this.memoryUsedGB = 0;
        this.memoryTotalGB = 0;
        this.vCoresUsed = 0;
        this.vCoresTotal = 0;
        this.runningApps = 0;
        this.pendingApps = 0;
        this.allocatedContainers = 0;
        this.pendingContainers = 0;
      }

      // Get capacity percentages
      this.capacityPercentage = queueInfo.getCapacity() * 100;
      this.currentCapacityPercentage = queueInfo.getCurrentCapacity() * 100;
    }

    public float getMemoryUsedGB() {
      return memoryUsedGB;
    }

    public float getMemoryTotalGB() {
      return memoryTotalGB;
    }

    public int getVCoresUsed() {
      return vCoresUsed;
    }

    public int getVCoresTotal() {
      return vCoresTotal;
    }

    public float getCapacityPercentage() {
      return capacityPercentage;
    }

    public float getCurrentCapacityPercentage() {
      return currentCapacityPercentage;
    }

    public int getRunningApps() {
      return runningApps;
    }

    public int getPendingApps() {
      return pendingApps;
    }

    public int getAllocatedContainers() {
      return allocatedContainers;
    }

    public int getPendingContainers() {
      return pendingContainers;
    }

    public long getCollectionTimestamp() {
      return collectionTimestamp;
    }

    /**
     * Gets the memory usage percentage as a formatted string.
     *
     * @return Formatted percentage like "50.25%" or "N/A" if total is zero
     */
    public String getMemoryPercentage() {
      if (memoryTotalGB > 0) {
        return String.format("%.2f%%", (memoryUsedGB / memoryTotalGB) * 100);
      }
      return "N/A";
    }

    /**
     * Gets the vCore usage percentage as a formatted string.
     *
     * @return Formatted percentage like "75.00%" or "N/A" if total is zero
     */
    public String getVCoresPercentage() {
      if (vCoresTotal > 0) {
        return String.format("%.2f%%", ((float) vCoresUsed / vCoresTotal) * 100);
      }
      return "N/A";
    }
  }
}

