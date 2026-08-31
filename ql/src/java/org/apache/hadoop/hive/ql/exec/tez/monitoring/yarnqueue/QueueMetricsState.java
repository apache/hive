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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.exec.tez.monitoring.yarnqueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Holds all runtime state for YARN queue metrics collection on a single queue.
 * One instance exists per active queue name in the JVM, stored in {@link QueueMetricsCache}.
 * <p>
 * Owns all per-queue logic: session interval registration, refresh task scheduling,
 * thundering herd prevention, and circuit breaker. All fields are private — callers
 * interact only through methods.
 * <p>
 * Ownership model:
 * <ul>
 *   <li>{@code sessionIntervals} — a per-queue min-heap with one entry per active session.
 *       {@code peek()} = O(1) minimum. Owned by
 *       {@link #registerInterval}/{@link #deregisterInterval} via {@link PriorityBlockingQueue}
 *       thread-safe operations.</li>
 *   <li>{@code minRefreshIntervalMs} — volatile hint written only inside
 *       {@link #ensureTaskScheduled} (under {@code synchronized}); read lock-free by the
 *       refresh tick for the per-tick freshness check in
 *       {@code YarnQueueMetricsCollector.refreshMetrics()}.</li>
 *   <li>{@code refreshTask}, {@code taskCurrentRefreshIntervalMs}
 *       — owned by {@link #ensureTaskScheduled} under {@code synchronized(this)}</li>
 *   <li>{@code snapshot}, {@code lastWriteNanos} — written by the refresh thread, read by
 *       TezProgressMonitor; {@code volatile} for visibility without synchronization</li>
 * </ul>
 */
public class QueueMetricsState {
  private static final Logger LOG = LoggerFactory.getLogger(QueueMetricsState.class);

  private static final int MAX_CONSECUTIVE_FAILURES = 5;
  private static final int CIRCUIT_BREAKER_PROBE_INTERVAL = 10;

  // Metrics data (written by refresh thread, read by TezProgressMonitor)
  private volatile QueueMetricsSnapshot snapshot;
  private volatile long lastWriteNanos; // System.nanoTime() at last successful write; 0 = never written

  // One entry per active session on this queue. PriorityBlockingQueue keeps the minimum at the
  // head (peek() = O(1)). Duplicates are naturally supported — no count bookkeeping needed.
  // Scoped to a single queue; n = concurrent queries on that queue (typically tens at most).
  private final PriorityBlockingQueue<Long> sessionIntervals = new PriorityBlockingQueue<>();

  // Volatile hint: the minimum interval currently in effect for this queue.
  // Written only inside ensureTaskScheduled (synchronized) so it always reflects the
  // authoritative scheduled interval. Read lock-free by refreshMetrics() every tick.
  private volatile long minRefreshIntervalMs;

  // Refresh task (owned by ensureTaskScheduled under synchronized(this))
  private final AtomicReference<ScheduledFuture<?>> refreshTask = new AtomicReference<>(null);
  private final AtomicLong taskCurrentRefreshIntervalMs;

  // Thundering herd guard
  private final AtomicBoolean isRefreshing = new AtomicBoolean(false);

  // Circuit breaker
  private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
  private final AtomicInteger circuitBreakerSkipCount = new AtomicInteger(0);

  QueueMetricsState(QueueMetricsSnapshot snapshot, long refreshIntervalMs) {
    this.snapshot = snapshot;
    this.lastWriteNanos = 0L; // 0 = "never written" — ensures first fetch fires immediately
    this.minRefreshIntervalMs = refreshIntervalMs;
    this.taskCurrentRefreshIntervalMs = new AtomicLong(refreshIntervalMs);
  }

  /**
   * Returns ms since last successful RM write using a monotonic clock.
   * Large value on first call (lastWriteNanos=0).
   */
  public long getAgeMs() {
    if (lastWriteNanos == 0L) {
      return Long.MAX_VALUE;
    }
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastWriteNanos);
  }

  /**
   * Returns the latest snapshot, or null if not yet fetched.
   */
  public QueueMetricsSnapshot getSnapshot() {
    return snapshot;
  }

  /**
   * Returns the minimum refresh interval across all active sessions on this queue.
   * Volatile read — O(1), safe to call from the refresh tick without locking.
   */
  public long getMinRefreshIntervalMs() {
    return minRefreshIntervalMs;
  }

  /**
   * Updates snapshot and lastWriteNanos after a successful RM fetch.
   * Does not touch minRefreshIntervalMs — that field is owned by ensureTaskScheduled.
   */
  public void applySnapshot(QueueMetricsSnapshot newSnapshot) {
    this.snapshot = newSnapshot;
    this.lastWriteNanos = System.nanoTime();
  }

  /**
   * Registers this session's refresh interval by adding one entry to the min-heap.
   * Returns true if rescheduling may be needed — i.e. no task is running yet, or this
   * session is strictly faster than the previous minimum and may have lowered it.
   * <p>
   * Thread-safe: {@link PriorityBlockingQueue#offer} and {@link PriorityBlockingQueue#peek}
   * are individually thread-safe. The peek-before-offer pattern is not atomic; a concurrent
   * offer between the two may cause an extra {@code ensureTaskScheduled} call, but that
   * method is idempotent under its lock and will simply do nothing in that case.
   */
  public boolean registerInterval(long refreshIntervalMs) {
    Long prevMin = sessionIntervals.peek(); // capture before offer to detect minimum change
    sessionIntervals.offer(refreshIntervalMs);
    LOG.debug("Session registered at {}ms, totalSessions={}", refreshIntervalMs, sessionIntervals.size());
    // Reschedule only if: no task running yet (first session for this queue),
    // OR this session is strictly faster than the previous minimum (lowered it).
    return refreshTask.get() == null || (prevMin != null && refreshIntervalMs < prevMin);
  }

  /**
   * Deregisters this session's refresh interval by removing one occurrence from the min-heap.
   * Returns true if rescheduling may be needed — i.e. the removed interval was at or faster
   * than the current task speed, meaning the task may need to slow down or stop.
   * <p>
   * Thread-safe: {@link PriorityBlockingQueue#remove(Object)} is thread-safe.
   * O(n) scan of the heap — negligible since n = sessions on a single queue (typically tens).
   */
  public boolean deregisterInterval(long refreshIntervalMs) {
    sessionIntervals.remove(refreshIntervalMs);
    LOG.debug("Session deregistered at {}ms, totalSessions={}", refreshIntervalMs, sessionIntervals.size());
    // Reschedule only if the removed session was running at or faster than the current task speed.
    // If it was slower, removing it cannot require the task to change — skip the synchronized call.
    return refreshIntervalMs <= taskCurrentRefreshIntervalMs.get();
  }

  /**
   * Ensures the shared refresh task fires at the minimum interval across active sessions.
   * Cancels and reschedules only when the interval actually changed.
   * Serialized under {@code synchronized} — the single scheduling authority for this queue.
   * Uses {@code sessionIntervals.peek()} for the authoritative O(1) minimum.
   * Updates the volatile {@code minRefreshIntervalMs} hint so refresh ticks can read the
   * current minimum lock-free.
   *
   * @param poolManager pool that owns the scheduled executor
   * @param refreshTask the refresh runnable bound to the calling collector
   * @param queueName   used for logging only
   */
  public synchronized void ensureTaskScheduled(QueueMetricsRefreshPool poolManager,
                                               Runnable refreshTask, String queueName) {
    ScheduledFuture<?> currentTask = this.refreshTask.get();

    if (sessionIntervals.isEmpty()) {
      minRefreshIntervalMs = 0L; // 0 = no active sessions; freshness check (age < min) is always false
      if (currentTask != null) {
        currentTask.cancel(false);
        this.refreshTask.set(null);
        LOG.info("Cancelled refresh task for queue: {} — no active sessions remaining", queueName);
      }
      return;
    }

    long desiredInterval = sessionIntervals.peek(); // O(1) — min-heap head
    long currentInterval = taskCurrentRefreshIntervalMs.get();

    // Update volatile hint so refreshMetrics() reads the correct minimum lock-free every tick
    minRefreshIntervalMs = desiredInterval;

    if (currentTask == null) {
      scheduleTask(poolManager, refreshTask, desiredInterval, queueName);
    } else if (currentInterval != desiredInterval) {
      currentTask.cancel(false);
      this.refreshTask.set(null);
      scheduleTask(poolManager, refreshTask, desiredInterval, queueName);
      LOG.info("Rescheduled refresh task for queue: {} from {}ms to {}ms",
          queueName, currentInterval, desiredInterval);
    } else {
      LOG.debug("Refresh task for queue: {} already at correct interval {}ms",
          queueName, currentInterval);
    }
  }

  private void scheduleTask(QueueMetricsRefreshPool poolManager, Runnable task,
                             long intervalMs, String queueName) {
    long jitter = QueueMetricsRefreshPool.calculateJitter(queueName, intervalMs);
    long intervalWithJitter = intervalMs + jitter;
    ScheduledFuture<?> newTask = poolManager.scheduleRefreshTask(task, intervalWithJitter);
    refreshTask.set(newTask);
    taskCurrentRefreshIntervalMs.set(intervalMs);
    LOG.info("Scheduled refresh task for queue: {} at {}ms interval (base: {}ms, jitter: +{}ms)",
        queueName, intervalWithJitter, intervalMs, jitter);
  }

  /**
   * Returns true if this thread successfully claimed the refresh lock.
   */
  public boolean tryStartRefresh() {
    return isRefreshing.compareAndSet(false, true);
  }

  /**
   * Releases the refresh lock. Always call in a finally block after tryStartRefresh().
   */
  public void finishRefresh() {
    isRefreshing.set(false);
  }

  /**
   * Returns true if the circuit breaker is active and this tick should be skipped.
   * Allows one probe attempt every {@value #CIRCUIT_BREAKER_PROBE_INTERVAL} ticks.
   */
  public boolean shouldSkipDueToCircuitBreaker(String queueName) {
    if (consecutiveFailures.get() < MAX_CONSECUTIVE_FAILURES) {
      return false;
    }
    int skipCount = circuitBreakerSkipCount.incrementAndGet();
    if (skipCount % CIRCUIT_BREAKER_PROBE_INTERVAL == 0) {
      LOG.debug("Circuit breaker active for queue: {}, probe attempt (tick {})", queueName, skipCount);
      return false;
    }
    LOG.debug("Circuit breaker active for queue: {}, skipping (tick {})", queueName, skipCount);
    return true;
  }

  /**
   * Records a refresh failure and activates the circuit breaker after threshold.
   */
  public void recordRefreshFailure(String queueName, String reason) {
    int failures = consecutiveFailures.incrementAndGet();
    if (failures < MAX_CONSECUTIVE_FAILURES) {
      LOG.warn("Failed to refresh queue metrics for queue: {} (failure {} of {}): {}",
          queueName, failures, MAX_CONSECUTIVE_FAILURES, reason);
    } else if (failures == MAX_CONSECUTIVE_FAILURES) {
      LOG.warn("Queue metrics collection failing repeatedly for queue: {} ({} consecutive failures). "
          + "Circuit breaker activated — probing every {} ticks.",
          queueName, failures, CIRCUIT_BREAKER_PROBE_INTERVAL);
    } else {
      LOG.debug("Queue metrics refresh still failing for queue: {} (failure {}): {}",
          queueName, failures, reason);
    }
  }

  /**
   * Resets circuit breaker state after a successful RM fetch.
   */
  public void recordRefreshSuccess(String queueName) {
    if (consecutiveFailures.get() > 0) {
      LOG.info("Queue metrics collection recovered for queue: {} after {} failures",
          queueName, consecutiveFailures.get());
      consecutiveFailures.set(0);
      circuitBreakerSkipCount.set(0);
    }
  }
}
