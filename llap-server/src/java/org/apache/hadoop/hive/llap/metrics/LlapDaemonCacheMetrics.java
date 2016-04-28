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

import static org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheInfo.CacheAllocatedArena;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheInfo.CacheCapacityRemaining;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheInfo.CacheCapacityRemainingPercentage;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheInfo.CacheCapacityTotal;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheInfo.CacheCapacityUsed;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheInfo.CacheHitBytes;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheInfo.CacheHitRatio;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheInfo.CacheMetrics;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheInfo.CacheNumLockedBuffers;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheInfo.CacheReadRequests;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheInfo.CacheRequestedBytes;
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
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

import com.google.common.annotations.VisibleForTesting;

/**
 * Llap daemon cache metrics source.
 */
@Metrics(about = "LlapDaemon Cache Metrics", context = "cache")
public class LlapDaemonCacheMetrics implements MetricsSource {
  final String name;
  private String sessionId;
  private final MetricsRegistry registry;

  @Metric
  MutableCounterLong cacheReadRequests;
  @Metric
  MutableGaugeLong cacheCapacityTotal;
  @Metric
  MutableCounterLong cacheCapacityUsed; // Not using the gauge to avoid races.
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
    this.registry.tag(ProcessName, MetricsUtils.METRICS_PROCESS_NAME).tag(SessionId, sessionId);
  }

  public static LlapDaemonCacheMetrics create(String displayName, String sessionId) {
    MetricsSystem ms = LlapMetricsSystem.instance();
    return ms.register(displayName, null, new LlapDaemonCacheMetrics(displayName, sessionId));
  }

  public void setCacheCapacityTotal(long value) {
    cacheCapacityTotal.set(value);
  }

  public void incrCacheCapacityUsed(long delta) {
    cacheCapacityUsed.incr(delta);
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

  @VisibleForTesting
  public long getCacheRequestedBytes() {
    return cacheRequestedBytes.value();
  }

  @VisibleForTesting
  public long getCacheHitBytes() {
    return cacheHitBytes.value();
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean b) {
    MetricsRecordBuilder rb = collector.addRecord(CacheMetrics)
        .setContext("cache")
        .tag(ProcessName, MetricsUtils.METRICS_PROCESS_NAME)
        .tag(SessionId, sessionId);
    getCacheStats(rb);
  }

  private void getCacheStats(MetricsRecordBuilder rb) {
    float cacheHitRatio = cacheRequestedBytes.value() == 0 ? 0.0f :
        (float) cacheHitBytes.value() / (float) cacheRequestedBytes.value();

    long cacheCapacityRemaining = cacheCapacityTotal.value() - cacheCapacityUsed.value();
    float cacheRemainingPercent = cacheCapacityTotal.value() == 0 ? 0.0f :
        (float) cacheCapacityRemaining / (float) cacheCapacityTotal.value();
    rb.addCounter(CacheCapacityRemaining, cacheCapacityRemaining)
        .addGauge(CacheCapacityRemainingPercentage, cacheRemainingPercent)
        .addCounter(CacheCapacityTotal, cacheCapacityTotal.value())
        .addCounter(CacheCapacityUsed, cacheCapacityUsed.value())
        .addCounter(CacheReadRequests, cacheReadRequests.value())
        .addCounter(CacheRequestedBytes, cacheRequestedBytes.value())
        .addCounter(CacheHitBytes, cacheHitBytes.value())
        .addCounter(CacheAllocatedArena, cacheAllocatedArena.value())
        .addCounter(CacheNumLockedBuffers, cacheNumLockedBuffers.value())
        .addGauge(CacheHitRatio, cacheHitRatio);
  }

}
