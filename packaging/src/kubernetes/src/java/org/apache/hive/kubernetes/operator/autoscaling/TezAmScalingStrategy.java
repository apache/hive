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
 * Scaling strategy for Tez Application Master.
 * TezAM scaling tracks HS2 session demand: desired = ceil(sum(hs2_open_sessions)).
 * <p>
 * Activation gate: only scale if HS2 has open sessions.
 */
public class TezAmScalingStrategy implements ScalingStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(TezAmScalingStrategy.class);

  private final HiveClusterAutoscaler orchestrator;
  private final HiveCluster cluster;
  private int lastMetric;

  public TezAmScalingStrategy(HiveClusterAutoscaler orchestrator, HiveCluster cluster) {
    this.orchestrator = orchestrator;
    this.cluster = cluster;
  }

  @Override
  public int computeDesiredReplicas(List<PodMetrics> podMetrics,
      AutoscalingSpec autoscaling, int maxReplicas) {

    List<PodMetrics> hs2Metrics = orchestrator.getHs2MetricsFromCache(cluster);

    // Activation gate: if HS2 scrape returns no data but TezAM has running pods,
    // treat as "unknown" and preserve current state to avoid spurious scale-to-zero.
    if (hs2Metrics.isEmpty() && !podMetrics.isEmpty()) {
      LOG.debug("[tezam] HS2 scrape returned no pods; preserving TezAM (has {} running pods)", podMetrics.size());
      lastMetric = 0;
      return Math.max(1, autoscaling.minReplicas());
    }

    double totalSessions = 0;
    for (PodMetrics pm : hs2Metrics) {
      totalSessions += pm.metrics().getOrDefault(
          HiveServer2ScalingStrategy.METRIC_OPEN_SESSIONS, 0.0);
    }

    if (totalSessions <= 0) {
      LOG.debug("[tezam] No HS2 sessions, scaling to minReplicas");
      lastMetric = 0;
      return autoscaling.minReplicas();
    }

    lastMetric = (int) totalSessions;

    // Scale based on concurrent demand — one TezAM per open HS2 session
    int desired = (int) Math.ceil(totalSessions);
    desired = Math.min(desired, maxReplicas);

    LOG.debug("[tezam] totalSessions={}, desired={}", totalSessions, desired);

    return Math.max(desired, autoscaling.minReplicas());
  }

  @Override
  public int lastMetricValue() {
    return lastMetric;
  }

  @Override
  public boolean usesScaleUpThreshold() {
    return false;
  }
}
