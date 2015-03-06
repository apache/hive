/**
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
package org.apache.hadoop.hive.llap.metrics;

import static org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheInfo.CACHE_ALLOCATED_ARENA;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheInfo.CACHE_CAPACITY_REMAINING;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheInfo.CACHE_CAPACITY_TOTAL;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheInfo.CACHE_CAPACITY_USED;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheInfo.CACHE_HIT_BYTES;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheInfo.CACHE_HIT_RATIO;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheInfo.CACHE_NUM_LOCKED_BUFFERS;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheInfo.CACHE_READ_REQUESTS;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheInfo.CACHE_REQUESTED_BYTES;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheInfo.LLAP_DAEMON_CACHE_METRICS;
import static org.apache.hadoop.metrics2.impl.MsInfo.ProcessName;
import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

/**
 * Llap daemon cache metrics source.
 */
@Metrics(about = "LlapDaemon Cache Metrics", context = "llap")
public class LlapDaemonCacheMetrics implements MetricsSource {
  final String name;
  // TODO: SessionId should come from llap daemon. For now using random UUID.
  private String sessionId;
  private final MetricsRegistry registry;

  @Metric
  MutableCounterLong cacheReadRequests;
  @Metric
  MutableCounterLong cacheCapacityTotal;
  @Metric
  MutableCounterLong cacheCapacityUsed;
  @Metric
  MutableCounterLong cacheRequestedBytes;
  @Metric
  MutableCounterLong cacheHitBytes;
  @Metric
  MutableCounterLong cacheAllocatedArena;
  @Metric
  MutableCounterLong cacheNumLockedBuffers;

  private LlapDaemonCacheMetrics(String name, String sessionId) {
    this.name = name;
    this.sessionId = sessionId;
    this.registry = new MetricsRegistry("LlapDaemonCacheRegistry");
    this.registry.tag(ProcessName, "LlapDaemon").tag(SessionId, sessionId);
  }

  public static LlapDaemonCacheMetrics create(String displayName, String sessionId) {
    MetricsSystem ms = LlapMetricsSystem.instance();
    return ms.register(displayName, null, new LlapDaemonCacheMetrics(displayName, sessionId));
  }

  public void incrCacheCapacityTotal(long delta) {
    cacheCapacityTotal.incr(delta);
  }

  public void incrCacheCapacityUsed(long delta) {
    cacheCapacityUsed.incr(delta);
  }

  public void decrCacheCapacityUsed(int delta) {
    cacheCapacityUsed.incr(-delta);
  }

  public void incrCacheRequestedBytes(long delta) {
    cacheRequestedBytes.incr(delta);
  }

  public void incrCacheHitBytes(long delta) {
    cacheHitBytes.incr(delta);
  }

  public void incrCacheReadRequests() {
    cacheReadRequests.incr();
  }

  public void incrAllocatedArena() {
    cacheAllocatedArena.incr();
  }

  public void incrCacheNumLockedBuffers() {
    cacheNumLockedBuffers.incr();
  }

  public void decrCacheNumLockedBuffers() {
    cacheNumLockedBuffers.incr(-1);
  }

  public String getName() {
    return name;
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean b) {
    MetricsRecordBuilder rb = collector.addRecord(LLAP_DAEMON_CACHE_METRICS)
        .setContext("llap").tag(ProcessName, "LlapDaemon")
        .tag(SessionId, sessionId);
    getCacheStats(rb);
  }

  private void getCacheStats(MetricsRecordBuilder rb) {
    float cacheHitRatio = cacheRequestedBytes.value() == 0 ? 0.0f :
        (float) cacheHitBytes.value() / (float) cacheRequestedBytes.value();

    rb.addCounter(CACHE_CAPACITY_REMAINING, cacheCapacityTotal.value() - cacheCapacityUsed.value())
        .addCounter(CACHE_CAPACITY_TOTAL, cacheCapacityTotal.value())
        .addCounter(CACHE_CAPACITY_USED, cacheCapacityUsed.value())
        .addCounter(CACHE_READ_REQUESTS, cacheReadRequests.value())
        .addCounter(CACHE_REQUESTED_BYTES, cacheRequestedBytes.value())
        .addCounter(CACHE_HIT_BYTES, cacheHitBytes.value())
        .addCounter(CACHE_ALLOCATED_ARENA, cacheAllocatedArena.value())
        .addCounter(CACHE_NUM_LOCKED_BUFFERS, cacheNumLockedBuffers.value())
        .addGauge(CACHE_HIT_RATIO, cacheHitRatio);
  }

}
