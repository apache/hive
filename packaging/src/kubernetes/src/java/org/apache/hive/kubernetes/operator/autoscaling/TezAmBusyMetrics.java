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

/**
 * Interprets TezAM JMX Exporter metrics to determine whether an AM is busy executing
 * a DAG or idle and safe to remove during scale-down.
 * <p>
 * Signal: {@code tez_am_dag_running} (SchedulerDagRunning gauge in LlapTaskSchedulerMetrics).
 * Set to 1 when the AM receives its first task for a new DAG, cleared to 0 in dagComplete().
 */
public final class TezAmBusyMetrics {

  public static final String METRIC_DAG_RUNNING = "tez_am_dag_running";
  public static final int BUSY_DELETION_COST = Integer.MAX_VALUE;

  private TezAmBusyMetrics() {}

  /**
   * Returns true when the TezAM has active DAG work (running or pending tasks).
   */
  public static boolean hasActiveDag(Map<String, Double> metrics) {
    return metrics.getOrDefault(METRIC_DAG_RUNNING, 0.0) > 0;
  }

  /**
   * Assigns unique {@code pod-deletion-cost} values per pod so the ReplicaSet controller
   * and the operator pick the same pods during scale-down.
   * <p>
   * Busy AMs receive {@link #BUSY_DELETION_COST}. Idle AMs receive 0, 1, 2, … in list order.
   */
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

  /**
   * Returns the pod names K8s will terminate for a scale-down of {@code toRemove} replicas —
   * those with the lowest deletion costs from the same snapshot used to patch annotations.
   */
  public static List<String> podsToRemove(List<PodMetrics> metrics, Map<String, Integer> costs, int removeCount) {
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
