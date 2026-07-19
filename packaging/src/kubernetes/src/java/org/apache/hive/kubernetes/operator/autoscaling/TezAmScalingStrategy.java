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

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.spec.AutoscalingSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scaling strategy for per-LLAP TezAM instances.
 * Each TezAM follows its paired LLAP cluster's lifecycle: it should be up
 * when there are sessions targeting that LLAP cluster, and at 0 otherwise.
 * <p>
 * Uses the per-target session metric from HS2: hs2_llap_target_sessions_{llapName}.
 * Falls back to hs2_open_sessions if per-target metrics are not available.
 * <p>
 * Scale-down uses {@link #METRIC_DAG_RUNNING} from each TezAM pod's scraped
 * {@link PodMetrics} to rank idle AMs for safe termination.
 */
public class TezAmScalingStrategy implements ScalingStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(TezAmScalingStrategy.class);
  public static final String METRIC_DAG_RUNNING = "tez_am_dag_running";
  public static final int BUSY_DELETION_COST = Integer.MAX_VALUE;

  private final HiveClusterAutoscaler orchestrator;
  private final HiveCluster cluster;
  private final String llapName;
  private int lastMetric;

  public TezAmScalingStrategy(HiveClusterAutoscaler orchestrator,
      HiveCluster cluster, String llapName) {
    this.orchestrator = orchestrator;
    this.cluster = cluster;
    this.llapName = llapName;
  }

  @Override
  public int computeDesiredReplicas(List<PodMetrics> podMetrics,
      AutoscalingSpec autoscaling, int maxReplicas) {

    List<PodMetrics> hs2Metrics = orchestrator.getHs2MetricsFromCache(cluster);

    // Activation gate: if HS2 scrape returns no data but TezAM has running pods,
    // treat as "unknown" and preserve current state to avoid spurious scale-to-zero.
    if (hs2Metrics.isEmpty() && !podMetrics.isEmpty()) {
      LOG.debug("[tezam-{}] HS2 scrape returned no pods; preserving TezAM", llapName);
      lastMetric = 0;
      return Math.max(1, autoscaling.minReplicas());
    }

    // Use per-LLAP target sessions metric (same logic as LlapScalingStrategy).
    String targetMetric = "hs2_llap_target_sessions_" + llapName;
    boolean anyPerTargetMetricExists = false;
    double targetSessions = 0;

    for (PodMetrics pm : hs2Metrics) {
      // Check if HS2 exposes ANY per-target metric (feature support check)
      for (String key : pm.metrics().keySet()) {
        if (key.startsWith("hs2_llap_target_sessions_")) {
          anyPerTargetMetricExists = true;
          break;
        }
      }
      targetSessions += pm.metrics().getOrDefault(targetMetric, 0.0);
    }

    if (!anyPerTargetMetricExists && !hs2Metrics.isEmpty()) {
      // HS2 doesn't support per-target metrics — fall back to total sessions
      double totalSessions = 0;
      for (PodMetrics pm : hs2Metrics) {
        totalSessions += pm.metrics().getOrDefault(
            HiveServer2ScalingStrategy.METRIC_OPEN_SESSIONS, 0.0);
      }
      targetSessions = totalSessions;
    }

    if (targetSessions <= 0) {
      LOG.debug("[tezam-{}] No sessions targeting this cluster, scaling to minReplicas", llapName);
      lastMetric = 0;
      return autoscaling.minReplicas();
    }

    lastMetric = (int) targetSessions;

    // TezAM desired: at least 1 when there are sessions, capped at maxReplicas
    int desired = (int) Math.ceil(targetSessions);
    desired = Math.min(desired, maxReplicas);

    LOG.debug("[tezam-{}] targetSessions={}, desired={}", llapName, targetSessions, desired);

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

  public static boolean hasActiveDag(Map<String, Double> metrics) {
    return metrics.getOrDefault(METRIC_DAG_RUNNING, 0.0) > 0;
  }

  public static Map<String, Integer> deletionCostsByPod(List<PodMetrics> metrics) {
    Map<String, Integer> costs = new HashMap<>();
    int idleCost = 0;
    for (PodMetrics pm : metrics) {
      if (hasActiveDag(pm.metrics())) {
        costs.put(pm.podName(), BUSY_DELETION_COST);
      } else {
        costs.put(pm.podName(), idleCost++);
      }
    }
    return costs;
  }

  public static List<String> podsToRemove(List<PodMetrics> metrics,
      Map<String, Integer> costs, int removeCount) {
    if (removeCount <= 0) {
      return List.of();
    }
    return metrics.stream()
        .sorted(Comparator.comparingInt(pm -> costs.get(pm.podName())))
        .limit(removeCount)
        .map(PodMetrics::podName)
        .toList();
  }
}
