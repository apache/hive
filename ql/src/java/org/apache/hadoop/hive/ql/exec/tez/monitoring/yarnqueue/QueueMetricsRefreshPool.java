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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Singleton manager for the JVM-wide refresh executor pool used by queue metrics collection.
 * Provides a shared {@link ScheduledExecutorService} that fires periodic YARN RM refresh tasks
 * across all queries in the HiveServer2 process.
 * <p>
 * Initialized during HiveServer2 startup via {@link #init(int)} when Tez session pool is set up.
 * Pool size is configured via {@code hive.server2.tez.queue.metrics.refresh.threads} (default: 4).
 * <p>
 * All {@link YarnQueueMetricsCollector} instances share this single pool, ensuring efficient
 * resource usage and preventing thread explosion when many queries run concurrently.
 * <p>
 * Thread-safe singleton implementation using double-check locking pattern.
 */
public final class QueueMetricsRefreshPool {
  private static final Logger LOG = LoggerFactory.getLogger(QueueMetricsRefreshPool.class);

  private static final int DEFAULT_THREAD_COUNT = 4;
  public static final int JITTER_PERCENT = 10;

  private static final AtomicReference<QueueMetricsRefreshPool> INSTANCE = new AtomicReference<>(null);
  private static final Object INIT_LOCK = new Object();

  private final ScheduledExecutorService refreshPool;


  /**
   * Initializes the singleton pool with the specified thread count.
   * Must be called during HiveServer2 startup. Subsequent calls are ignored.
   *
   * @param threadCount number of threads for the refresh pool
   */
  public static void init(int threadCount) {
    if (INSTANCE.get() != null) {
      LOG.debug("QueueMetricsRefreshPool already initialized, ignoring init call");
      return;
    }
    synchronized (INIT_LOCK) {
      if (INSTANCE.get() == null) {
        INSTANCE.set(new QueueMetricsRefreshPool(threadCount));
      }
    }
  }

  /**
   * Returns the singleton instance. Must be called after {@link #init(int)}.
   * For tests or non-HS2 environments, lazily initializes with default thread count.
   *
   * @return the singleton pool instance
   */
  public static QueueMetricsRefreshPool getInstance() {
    QueueMetricsRefreshPool local = INSTANCE.get();
    if (local != null) {
      return local;
    }
    // Lazy init for tests/non-HS2 with default thread count
    synchronized (INIT_LOCK) {
      local = INSTANCE.get();
      if (local == null) {
        LOG.warn("QueueMetricsRefreshPool not initialized via init(), using default thread count: {}",
            DEFAULT_THREAD_COUNT);
        local = new QueueMetricsRefreshPool(DEFAULT_THREAD_COUNT);
        INSTANCE.set(local);
      }
      return local;
    }
  }


  private QueueMetricsRefreshPool(int threadCount) {
    this.refreshPool = Executors.newScheduledThreadPool(threadCount,
        new ThreadFactoryBuilder()
            .setNameFormat("queue-metrics-refresh-%d")
            .setDaemon(true)
            .build());
    LOG.info("QueueMetricsRefreshPool initialized with {} threads", threadCount);
  }


  /**
   * Schedules a periodic refresh task. initialDelay=0 so the first fetch runs immediately.
   *
   * @param task the refresh task to schedule
   * @param intervalMs the interval in milliseconds between task executions
   * @return a ScheduledFuture representing the scheduled task
   */
  public ScheduledFuture<?> scheduleRefreshTask(Runnable task, long intervalMs) {
    return refreshPool.scheduleWithFixedDelay(task, 0, intervalMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Shuts down the refresh pool gracefully, waiting up to 10 seconds for in-flight tasks
   * to complete before forcing termination. Must be called during HiveServer2 shutdown,
   * after all Tez sessions have been stopped.
   */
  public static void shutdown() {
    synchronized (INIT_LOCK) {
      QueueMetricsRefreshPool current = INSTANCE.get();
      if (current == null) {
        return;
      }
      LOG.info("Shutting down QueueMetricsRefreshPool");
      try {
        current.refreshPool.shutdown();
        if (!current.refreshPool.awaitTermination(10, TimeUnit.SECONDS)) {
          LOG.warn("QueueMetricsRefreshPool did not terminate gracefully, forcing shutdown");
          current.refreshPool.shutdownNow();
          if (!current.refreshPool.awaitTermination(5, TimeUnit.SECONDS)) {
            LOG.error("QueueMetricsRefreshPool did not terminate after forced shutdown");
          }
        }
        LOG.info("QueueMetricsRefreshPool shutdown complete");
      } catch (InterruptedException e) {
        LOG.warn("Interrupted during QueueMetricsRefreshPool shutdown", e);
        current.refreshPool.shutdownNow();
        Thread.currentThread().interrupt();
      } finally {
        INSTANCE.set(null);
      }
    }
  }

  /**
   * Calculates deterministic hash-based jitter for a queue name to prevent thundering herd.
   * Jitter range: 0 to (intervalMs * JITTER_PERCENT / 100)
   *
   * @param queueName the queue name to hash
   * @param intervalMs the refresh interval in milliseconds
   * @return jitter value in milliseconds (0 to intervalMs * 10%)
   */
  public static long calculateJitter(String queueName, long intervalMs) {
    long jitterWindow = intervalMs * JITTER_PERCENT / 100;
    // Guard against zero or negative jitterWindow to prevent ArithmeticException
    if (jitterWindow <= 0) {
      return 0;
    }
    // Use bitwise AND to ensure non-negative hash (handles Integer.MIN_VALUE edge case)
    int nonNegativeHash = queueName.hashCode() & 0x7fffffff;
    return nonNegativeHash % jitterWindow;
  }
  // ─────────────────────────────────────────────────────────
  //  Test Support
  // ─────────────────────────────────────────────────────────

  /**
   * Returns the current singleton instance without lazy initialization.
   * Used for testing to verify if the pool was initialized via init().
   * NEVER call in production code - use {@link #getInstance()} instead.
   *
   * @return the current instance, or null if not initialized
   */
  @VisibleForTesting
  public static QueueMetricsRefreshPool getInstanceForTesting() {
    return INSTANCE.get();
  }

  /**
   * Resets the singleton for test isolation. NEVER call in production code.
   */
  @VisibleForTesting
  public static void resetForTesting() {
    synchronized (INIT_LOCK) {
      QueueMetricsRefreshPool current = INSTANCE.get();
      if (current != null) {
        current.refreshPool.shutdownNow();
        INSTANCE.set(null);
      }
    }
  }
}
