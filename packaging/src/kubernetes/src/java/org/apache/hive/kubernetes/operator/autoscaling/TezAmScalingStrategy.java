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
import org.apache.hive.kubernetes.operator.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scaling strategy for Tez Application Master.
 * TezAM scaling tracks HS2 session demand:
 * - Trigger 1 (concurrent): sum(hs2_open_sessions) — each session may need a TezAM
 * - Trigger 2 (pre-warm): count(hs2_pods_with_sessions) * sessions_per_queue
 * desired = max(trigger1, trigger2)
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

    List<PodMetrics> hs2Metrics = orchestrator.scrapeHs2Metrics(cluster);

    // Activation gate: if HS2 scrape returns no data but TezAM has running pods,
    // treat as "unknown" and preserve current state to avoid spurious scale-to-zero.
    if (hs2Metrics.isEmpty() && !podMetrics.isEmpty()) {
      LOG.debug("[tezam] HS2 scrape returned no pods; preserving TezAM (has {} running pods)", podMetrics.size());
      lastMetric = 0;
      return Math.max(1, autoscaling.minReplicas());
    }

    double totalSessions = 0;
    int podsWithSessions = 0;
    for (PodMetrics pm : hs2Metrics) {
      double sessions = pm.metrics().getOrDefault(
          HiveServer2ScalingStrategy.METRIC_OPEN_SESSIONS, 0.0);
      totalSessions += sessions;
      if (sessions > 0) {
        podsWithSessions++;
      }
    }

    if (totalSessions <= 0) {
      LOG.debug("[tezam] No HS2 sessions, scaling to minReplicas");
      lastMetric = 0;
      return autoscaling.minReplicas();
    }

    lastMetric = (int) totalSessions;

    // Trigger 1: concurrent demand — total open sessions (1 TezAM per session)
    int concurrentDemand = (int) Math.ceil(totalSessions);

    // Trigger 2: pre-warm — only if hive.server2.tez.initialize.default.sessions is true.
    // When true, each HS2 pod pre-warms sessionsPerQueue TezAMs at startup.
    // When false, no pre-warming happens — scale purely on concurrent session demand.
    int prewarmDemand = 0;
    boolean initSessions = ConfigUtils.getBoolean(
        cluster.getSpec().hiveServer2().configOverrides(),
        ConfigUtils.HIVE_SERVER2_TEZ_INITIALIZE_DEFAULT_SESSIONS_KEY,
        ConfigUtils.HIVE_SERVER2_TEZ_INITIALIZE_DEFAULT_SESSIONS_DEFAULT);
    if (initSessions) {
      int sessionsPerQueue = ConfigUtils.getInt(
          cluster.getSpec().hiveServer2().configOverrides(),
          ConfigUtils.HIVE_SERVER2_TEZ_SESSIONS_PER_QUEUE_KEY,
          null, ConfigUtils.HIVE_SERVER2_TEZ_SESSIONS_PER_QUEUE_DEFAULT);
      prewarmDemand = podsWithSessions * sessionsPerQueue;
    }

    int desired = Math.max(concurrentDemand, prewarmDemand);

    LOG.debug("[tezam] totalSessions={}, podsWithSessions={}, initDefaultSessions={}, "
            + "concurrent={}, prewarm={}, desired={}",
        totalSessions, podsWithSessions, initSessions,
        concurrentDemand, prewarmDemand, desired);

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
