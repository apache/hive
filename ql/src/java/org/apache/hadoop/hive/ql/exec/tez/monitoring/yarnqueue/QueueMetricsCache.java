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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * JVM-wide cache mapping queue names to their {@link QueueMetricsState}.
 * Responsible only for entry lifecycle: get, put, placeholder creation, and expiry.
 * All per-queue logic lives in {@link QueueMetricsState}.
 * <p>
 * Entries expire after {@value #CACHE_EXPIRE_AFTER_ACCESS_MINUTES} minutes of inactivity
 * (no reads or writes). While a queue has active sessions, both the background refresh task
 * (via put) and TezProgressMonitor (via get) continuously reset the access timer — the entry
 * lives as long as the queue is in use. Once all sessions finish and the refresh task is
 * cancelled, no more reads or writes occur and the entry auto-expires after 60 minutes.
 * This provides a natural grace period for brief query gaps on the same queue.
 */
public final class QueueMetricsCache {
  private static final Logger LOG = LoggerFactory.getLogger(QueueMetricsCache.class);

  private static final QueueMetricsCache INSTANCE = new QueueMetricsCache();

  // Entries auto-expire after 60 minutes of no reads or writes.
  // Active queues: reset continuously by get() (TezProgressMonitor polling) and put() (RM refresh).
  // Idle queues: task cancelled + job done → no reads or writes → expires after 60 minutes.
  // 60 minutes safely exceeds any realistic refresh interval or progress polling interval.
  private static final long CACHE_EXPIRE_AFTER_ACCESS_MINUTES = 60;

  private final Cache<String, QueueMetricsState> cache;

  private QueueMetricsCache() {
    this.cache = CacheBuilder.newBuilder()
        .maximumSize(1000)
        .expireAfterAccess(CACHE_EXPIRE_AFTER_ACCESS_MINUTES, TimeUnit.MINUTES)
        .build();

    LOG.info("QueueMetricsCache initialized: max=1000, expireAfterAccess={}min",
        CACHE_EXPIRE_AFTER_ACCESS_MINUTES);
  }

  public static QueueMetricsCache getInstance() {
    return INSTANCE;
  }

  /**
   * Returns the {@link QueueMetricsState} for the given queue, or null if not present.
   * Guava resets the expireAfterAccess timer on every call.
   */
  public QueueMetricsState get(String queueName) {
    if (queueName == null) {
      return null;
    }
    return cache.getIfPresent(queueName);
  }

  /**
   * Updates the snapshot on an existing entry after a successful RM fetch, or creates
   * a new entry if none exists. expireAfterAccess timer resets on this call.
   */
  public void put(String queueName, QueueMetricsSnapshot snapshot,
                  long refreshIntervalMs) {
    if (queueName == null || snapshot == null) {
      return;
    }
    QueueMetricsState existing = cache.getIfPresent(queueName);
    if (existing == null) {
      cache.put(queueName, new QueueMetricsState(snapshot, refreshIntervalMs));
      LOG.debug("Created state entry for queue: {}", queueName);
    } else {
      existing.applySnapshot(snapshot, refreshIntervalMs);
    }
  }

  /**
   * Atomically creates a placeholder {@link QueueMetricsState} with no snapshot.
   * Uses putIfAbsent so concurrent sessions racing to create the first entry are safe:
   * exactly one placeholder wins and all others get the same entry back.
   * Returns the authoritative state (the one actually in the cache).
   */
  public QueueMetricsState putPlaceholder(String queueName, long refreshIntervalMs) {
    if (queueName == null) {
      return null;
    }
    // lastWriteTime=0 (epoch) inside QueueMetricsState signals "never written" —
    // ensures the first refresh fires immediately (age = now - 0 always exceeds any interval).
    QueueMetricsState newState = new QueueMetricsState(null, refreshIntervalMs);
    QueueMetricsState existing = cache.asMap().putIfAbsent(queueName, newState);
    if (existing != null) {
      LOG.debug("State for queue: {} already created by concurrent session, using existing", queueName);
      return existing;
    }
    LOG.debug("Created placeholder state for queue: {}", queueName);
    return newState;
  }

  /** Returns the number of queues currently tracked in the cache. */
  public int getActiveQueueCount() {
    return (int) cache.size();
  }

  /**
   * Invalidates all entries. Called on JVM shutdown.
   */
  public void shutdown() {
    try {
      cache.invalidateAll();
      LOG.info("QueueMetricsCache shutdown complete");
    } catch (Exception e) {
      LOG.warn("Error during cache shutdown", e);
    }
  }

  /**
   * Resets cache for test isolation. NEVER call in production code.
   */
  @VisibleForTesting
  public static void resetForTesting() {
    INSTANCE.cache.invalidateAll();
    LOG.debug("QueueMetricsCache reset for testing");
  }
}
