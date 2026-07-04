/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.search.config;

import java.time.Duration;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

public record IndexConfig(Configuration configuration) {
  public static final String INDEX_RAM_SIZE = "metastore.index.write.ram.size";
  public static final int INDEX_RAM_SIZE_DEFAULT = 128 * 1024 * 1024;

  public static final String FLUSH_INTERVAL_MS = "metastore.index.flush.interval.ms";
  public static final long FLUSH_INTERVAL_MS_DEFAULT = 60 * 1000L;

  public static final String INDEX_NAME = "metastore.index.name";
  public static final String INDEX_NAME_DEFAULT = "hive_tables";

  public static final String POLL_NOTIFICATION_INTERVAL = "metastore.index.poll.notification.ms";
  public static final long POLL_NOTIFICATION_INTERVAL_DEFAULT = 1000;

  public static final String INDEX_SYNC_INTERVAL = "metastore.index.sync.interval.minutes";
  public static final long INDEX_SYNC_INTERVAL_DEFAULT = 360;

  /** Force a metadata-only commit when this many events are ahead of the last committed checkpoint. */
  public static final String FORCE_FLUSH_EVENT_GAP = "metastore.index.force.flush.event.gap";
  public static final long FORCE_FLUSH_EVENT_GAP_DEFAULT = 3000L;

  /** Tables per metastore fetch batch during leader bootstrap. */
  public static final String BOOTSTRAP_BATCH_SIZE = "metastore.index.bootstrap.batch.size";
  public static final int BOOTSTRAP_BATCH_SIZE_DEFAULT = 2000;

  /** In-flight document batches between fetch workers and the index writer. */
  public static final String BOOTSTRAP_QUEUE_DEPTH = "metastore.index.bootstrap.queue.depth";
  public static final int BOOTSTRAP_QUEUE_DEPTH_DEFAULT = 16;

  /** Parallel metastore fetch workers during bootstrap. */
  public static final String BOOTSTRAP_FETCH_THREADS = "metastore.index.bootstrap.fetch.threads";
  public static final int BOOTSTRAP_FETCH_THREADS_DEFAULT = 4;

  /** Lucene commits after this many Lucene auto-flushes during incremental indexing. 0 commits immediately. */
  public static final String COMMIT_FLUSHES = "metastore.index.commit.flushes";
  public static final int COMMIT_FLUSHES_DEFAULT = 3;

  /** Progress log interval during bootstrap. */
  public static final String BOOTSTRAP_PROGRESS_INTERVAL_MS =
      "metastore.index.bootstrap.progress.interval.ms";
  public static final long BOOTSTRAP_PROGRESS_INTERVAL_MS_DEFAULT = 30_000L;

  /** Max HMS events fetched per poll. */
  public static final String EVENT_POLL_MAX = "metastore.index.event.poll.max";
  public static final int EVENT_POLL_MAX_DEFAULT = 3000;

  /** Consecutive batch failures before falling back to single-event apply. */
  public static final String EVENT_BATCH_MAX_FAILURES =
      "metastore.index.event.batch.max.failures";
  public static final int EVENT_BATCH_MAX_FAILURES_DEFAULT = 3;

  /** Extra sleep after a failed batch before the poller retries (ms). */
  public static final String EVENT_FAILURE_BACKOFF_MS = "metastore.index.event.failure.backoff.ms";
  public static final long EVENT_FAILURE_BACKOFF_MS_DEFAULT = 10_000L;

  /** Skip a single poison event during individual fallback (index may diverge for that table). */
  public static final String EVENT_SKIP_POISON = "metastore.index.event.skip.poison";
  public static final boolean EVENT_SKIP_POISON_DEFAULT = false;

  /** Poller sleep after index is marked unhealthy (ms). */
  public static final String EVENT_UNHEALTHY_BACKOFF_MS = "metastore.index.event.unhealthy.backoff.ms";
  public static final long EVENT_UNHEALTHY_BACKOFF_MS_DEFAULT = 10 * 60 * 1000L;

  public int getWriteBufferSize() {
    return configuration.getInt(INDEX_RAM_SIZE, INDEX_RAM_SIZE_DEFAULT);
  }

  public Duration getFlushInterval() {
    return Duration.ofMillis(configuration.getLong(FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS_DEFAULT));
  }

  public double getWriteBufferSizeMb() {
    return getWriteBufferSize() / (1024.0 * 1024.0);
  }

  public long getPollNotificationInterval() {
    return configuration.getLong(POLL_NOTIFICATION_INTERVAL, POLL_NOTIFICATION_INTERVAL_DEFAULT);
  }

  public long getSyncInterval() {
    return configuration.getLong(INDEX_SYNC_INTERVAL, INDEX_SYNC_INTERVAL_DEFAULT) * 60 * 1000;
  }

  public long getForceFlushEventGap() {
    return configuration.getLong(FORCE_FLUSH_EVENT_GAP, FORCE_FLUSH_EVENT_GAP_DEFAULT);
  }

  public int getBootstrapBatchSize() {
    return configuration.getInt(BOOTSTRAP_BATCH_SIZE, BOOTSTRAP_BATCH_SIZE_DEFAULT);
  }

  public int getBootstrapQueueDepth() {
    return configuration.getInt(BOOTSTRAP_QUEUE_DEPTH, BOOTSTRAP_QUEUE_DEPTH_DEFAULT);
  }

  public int getBootstrapFetchThreads() {
    return configuration.getInt(BOOTSTRAP_FETCH_THREADS, BOOTSTRAP_FETCH_THREADS_DEFAULT);
  }

  public int getCommitFlushes() {
    return configuration.getInt(COMMIT_FLUSHES, COMMIT_FLUSHES_DEFAULT);
  }

  public long getBootstrapProgressIntervalMs() {
    return configuration.getLong(BOOTSTRAP_PROGRESS_INTERVAL_MS,
        BOOTSTRAP_PROGRESS_INTERVAL_MS_DEFAULT);
  }

  public int getEventPollMax() {
    return configuration.getInt(EVENT_POLL_MAX, EVENT_POLL_MAX_DEFAULT);
  }

  public int getEventBatchMaxFailures() {
    return configuration.getInt(EVENT_BATCH_MAX_FAILURES, EVENT_BATCH_MAX_FAILURES_DEFAULT);
  }

  public long getEventFailureBackoffMs() {
    return configuration.getLong(EVENT_FAILURE_BACKOFF_MS, EVENT_FAILURE_BACKOFF_MS_DEFAULT);
  }

  public boolean isEventSkipPoison() {
    return configuration.getBoolean(EVENT_SKIP_POISON, EVENT_SKIP_POISON_DEFAULT);
  }

  public long getEventUnhealthyBackoffMs() {
    return configuration.getLong(EVENT_UNHEALTHY_BACKOFF_MS, EVENT_UNHEALTHY_BACKOFF_MS_DEFAULT);
  }

  public String indexName() {
    String name = configuration.get(INDEX_NAME);
    return StringUtils.isEmpty(name) ? INDEX_NAME_DEFAULT : name;
  }
}
