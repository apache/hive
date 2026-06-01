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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
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
 *   <li>{@code intervalCounts}, {@code minRefreshIntervalMs}, {@code activeSessionCount}
 *       — owned by {@link #registerInterval}/{@link #deregisterInterval} via lock-free atomics</li>
 *   <li>{@code refreshTask}, {@code taskCurrentRefreshIntervalMs}
 *       — owned by {@link #ensureTaskScheduled} under {@code synchronized(this)}</li>
 *   <li>{@code snapshot}, {@code lastWriteTime} — written by the refresh thread, read by
 *       TezProgressMonitor; {@code volatile} for visibility without synchronization</li>
 * </ul>
 */
public class QueueMetricsState {
  private static final Logger LOG = LoggerFactory.getLogger(QueueMetricsState.class);

  private static final int MAX_CONSECUTIVE_FAILURES = 5;
  private static final int CIRCUIT_BREAKER_PROBE_INTERVAL = 10;

  // Metrics data (written by refresh thread, read by TezProgressMonitor)
  private final AtomicReference<QueueMetricsSnapshot> snapshot;
  private volatile long lastWriteTime;

  // Session interval tracking (lock-free atomics)
  private final AtomicLong minRefreshIntervalMs;
  private final ConcurrentHashMap<Long, AtomicInteger> intervalCounts = new ConcurrentHashMap<>();
  private final AtomicInteger activeSessionCount = new AtomicInteger(0);

  // Refresh task (owned by ensureTaskScheduled under synchronized(this))
  private final AtomicReference<ScheduledFuture<?>> refreshTask = new AtomicReference<>(null);
  private final AtomicLong taskCurrentRefreshIntervalMs;

  // Thundering herd guard
  private final AtomicBoolean isRefreshing = new AtomicBoolean(false);

  // Circuit breaker
  private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
  private final AtomicInteger circuitBreakerSkipCount = new AtomicInteger(0);

  QueueMetricsState(QueueMetricsSnapshot snapshot, long refreshIntervalMs) {
    this.snapshot = new AtomicReference<>(snapshot);
    this.lastWriteTime = 0L; // epoch = "never written" — ensures first fetch fires immediately
    this.minRefreshIntervalMs = new AtomicLong(refreshIntervalMs);
    this.taskCurrentRefreshIntervalMs = new AtomicLong(refreshIntervalMs);
  }

  /**
   * Returns the latest snapshot, or null if not yet fetched.
   */
  public QueueMetricsSnapshot getSnapshot() {
    return snapshot.get();
  }

  /**
   * Returns ms since last successful RM write. Large value on first call (lastWriteTime=0).
   */
  public long getAgeMs() {
    return System.currentTimeMillis() - lastWriteTime;
  }

  /**
   * Returns the minimum refresh interval across all active sessions.
   */
  public long getMinRefreshIntervalMs() {
    return minRefreshIntervalMs.get();
  }

  /** Updates snapshot and lastWriteTime after a successful RM fetch. */
  public void applySnapshot(QueueMetricsSnapshot newSnapshot,
                             long refreshIntervalMs) {
    this.snapshot.set(newSnapshot);
    this.lastWriteTime = System.currentTimeMillis();
    minRefreshIntervalMs.updateAndGet(current -> Math.min(current, refreshIntervalMs));
  }


  /**
   * Registers this session's interval. Returns true if rescheduling may be needed
   * (no task running, or this session lowered the minimum interval).
   * Thread-safe: compute() is atomic per-key; getAndAccumulate returns previous value
   * so only the thread that actually lowered the minimum triggers rescheduling.
   */
  public boolean registerInterval(long refreshIntervalMs) {
    intervalCounts.compute(refreshIntervalMs, (k, existing) -> {
      if (existing == null) {
        return new AtomicInteger(1);
      }
      existing.incrementAndGet();
      return existing;
    });
    long prevMin = minRefreshIntervalMs.getAndAccumulate(refreshIntervalMs, Math::min);
    int count = activeSessionCount.incrementAndGet();
    LOG.debug("Session registered at {}ms, activeCount={}", refreshIntervalMs, count);
    return refreshTask.get() == null || refreshIntervalMs < prevMin;
  }

  /**
   * Deregisters this session's interval. Returns true if the task interval may need
   * to change (this thread removed the last session at or below the current task interval).
   * Thread-safe: compute() atomically decrements and conditionally removes the bucket.
   */
  public boolean deregisterInterval(long refreshIntervalMs) {
    boolean[] thisBucketRemoved = {false};
    intervalCounts.compute(refreshIntervalMs, (k, existing) -> {
      if (existing == null) {
        return null; // already removed by concurrent deregister
      }
      if (existing.decrementAndGet() <= 0) {
        thisBucketRemoved[0] = true;
        return null; // atomically removes the key
      }
      return existing;
    });
    int remaining = activeSessionCount.updateAndGet(c -> Math.max(0, c - 1));
    LOG.debug("Session deregistered at {}ms, activeCount={}", refreshIntervalMs, remaining);

    if (!thisBucketRemoved[0]) {
      return false; // other sessions still at this interval — task unchanged
    }
    long currentTaskInterval = taskCurrentRefreshIntervalMs.get();
    if (refreshIntervalMs > currentTaskInterval) {
      return false; // our interval was slower than the task — removing it changes nothing
    }
    // Recompute new minimum from remaining buckets.
    // orElse(MAX_VALUE): no sessions left — safe sentinel that won't corrupt concurrent registers.
    long newMin = intervalCounts.keySet().stream()
        .mapToLong(Long::longValue).min().orElse(Long.MAX_VALUE);
    // updateAndGet(Math.min): don't overwrite a lower value a concurrent register may have set.
    minRefreshIntervalMs.updateAndGet(current -> Math.min(current, newMin));
    return newMin != currentTaskInterval;
  }


  /**
   * Ensures the shared refresh task fires at the minimum interval derived from
   * active sessions. Cancels and reschedules only when the interval actually changed.
   * Serialized under {@code synchronized(this)} — the single scheduling authority for
   * this queue. Re-reads intervalCounts inside the lock for authoritative state.
   *
   * @param poolManager pool that owns the scheduled executor
   * @param refreshTask the refresh runnable bound to the calling collector
   * @param queueName   used for logging only
   */
  public void ensureTaskScheduled(QueueMetricsRefreshPool poolManager,
                                   Runnable refreshTask, String queueName) {
    synchronized (this) {
      OptionalLong minOptional = intervalCounts.keySet().stream()
          .mapToLong(Long::longValue).min();
      ScheduledFuture<?> currentTask = this.refreshTask.get();

      if (minOptional.isEmpty()) {
        if (currentTask != null) {
          currentTask.cancel(false);
          this.refreshTask.set(null);
          LOG.info("Cancelled refresh task for queue: {} — no active sessions remaining", queueName);
        }
        return;
      }

      long desiredInterval = minOptional.getAsLong();
      long currentInterval = taskCurrentRefreshIntervalMs.get();

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

