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

package org.apache.hive.kubernetes.operator.autoscaling;

import java.util.List;

import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.spec.AutoscalingSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scaling strategy for LLAP daemons.
 * Formula: avg(QueuedRequests + Configured - Available) across all pods.
 * This represents average "busy slots + queued" per daemon.
 * desired = ceil(avg_busy / scaleUpThreshold)
 * <p>
 * Activation gate: only scale if HS2 has open sessions (prevents zombie scaling).
 */
public class LlapScalingStrategy implements ScalingStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(LlapScalingStrategy.class);

  static final String METRIC_QUEUED = "hadoop_llapdaemon_executornumqueuedrequests";
  static final String METRIC_CONFIGURED = "hadoop_llapdaemon_executornumexecutorsconfigured";
  static final String METRIC_AVAILABLE = "hadoop_llapdaemon_executornumexecutorsavailable";

  private final HiveClusterAutoscaler orchestrator;
  private final HiveCluster cluster;
  private int lastMetric;

  public LlapScalingStrategy(HiveClusterAutoscaler orchestrator, HiveCluster cluster) {
    this.orchestrator = orchestrator;
    this.cluster = cluster;
  }

  @Override
  public int computeDesiredReplicas(List<PodMetrics> podMetrics,
      AutoscalingSpec autoscaling, int maxReplicas) {

    // Activation gate: check if HS2 has any open sessions.
    // If scrape returns empty but LLAP has running pods, treat as "unknown" and preserve.
    // This prevents spurious scale-to-zero from transient scrape failures after operator restart.
    List<PodMetrics> hs2Metrics = orchestrator.scrapeHs2Metrics(cluster);
    Boolean sessionsDetected = detectHs2Sessions(hs2Metrics);
    if (sessionsDetected == null && !podMetrics.isEmpty()) {
      // HS2 scrape returned no data but LLAP is running — hold current state
      LOG.debug("[llap] HS2 scrape returned no pods; preserving LLAP (has {} running pods)", podMetrics.size());
      lastMetric = 0;
      return Math.max(1, autoscaling.minReplicas());
    }
    if (sessionsDetected == null || !sessionsDetected) {
      LOG.debug("[llap] HS2 has no open sessions, scaling to minReplicas");
      lastMetric = 0;
      return autoscaling.minReplicas();
    }

    // HS2 has sessions but LLAP has no pods yet — scale up to at least 1
    if (podMetrics.isEmpty()) {
      LOG.debug("[llap] HS2 has sessions but LLAP has 0 pods, scaling to 1");
      lastMetric = 0;
      return Math.max(1, autoscaling.minReplicas());
    }

    // Compute average busy slots across all LLAP pods
    double totalBusy = 0;
    int podCount = 0;
    for (PodMetrics pm : podMetrics) {
      double queued = pm.metrics().getOrDefault(METRIC_QUEUED, 0.0);
      double configured = pm.metrics().getOrDefault(METRIC_CONFIGURED, 0.0);
      double available = pm.metrics().getOrDefault(METRIC_AVAILABLE, 0.0);
      double busy = queued + configured - available;
      totalBusy += busy;
      podCount++;
    }

    double avgBusy = totalBusy / podCount;
    lastMetric = (int) Math.round(avgBusy);

    if (avgBusy <= 0) {
      // HS2 has sessions (passed activation gate above) but executors are idle between queries.
      // Keep at least 1 daemon to avoid flapping: scaling to 0 here would cause immediate
      // scale-back-up on the next evaluation when the empty-pod path triggers.
      return Math.max(1, autoscaling.minReplicas());
    }

    LOG.debug("[llap] avgBusy={}, threshold={}", String.format("%.2f", avgBusy),
        autoscaling.scaleUpThreshold());

    int desired = (int) Math.ceil(avgBusy / autoscaling.scaleUpThreshold());
    return Math.max(desired, autoscaling.minReplicas());
  }

  @Override
  public int lastMetricValue() {
    return lastMetric;
  }

  /**
   * Detect HS2 open sessions.
   * @return true if sessions > 0, false if scraped and all 0, null if scrape returned no pods
   *         (ambiguous — could be transient failure or HS2 genuinely absent)
   */
  private Boolean detectHs2Sessions(List<PodMetrics> hs2Metrics) {
    if (hs2Metrics.isEmpty()) {
      return null;
    }
    for (PodMetrics pm : hs2Metrics) {
      double sessions = pm.metrics().getOrDefault(
          HiveServer2ScalingStrategy.METRIC_OPEN_SESSIONS, 0.0);
      if (sessions > 0) {
        return true;
      }
    }
    return false;
  }
}
