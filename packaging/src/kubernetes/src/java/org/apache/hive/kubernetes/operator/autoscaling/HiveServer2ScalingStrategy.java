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

import org.apache.hive.kubernetes.operator.model.spec.AutoscalingSpec;

/**
 * Scaling strategy for HiveServer2.
 * desired = ceil(sum(hs2_open_sessions across all pods) / scaleUpThreshold)
 * Uses sum() so that each session is counted — prevents premature scale-down
 * of pods that still have active sessions.
 */
public class HiveServer2ScalingStrategy implements ScalingStrategy {

  static final String METRIC_OPEN_SESSIONS = "hs2_open_sessions";

  private int lastMetric;

  @Override
  public int computeDesiredReplicas(List<PodMetrics> podMetrics,
      AutoscalingSpec autoscaling, int maxReplicas) {
    // HS2 is the cluster entry point — scaling to 0 makes the cluster unreachable.
    // Enforce floor of 1 regardless of CRD defaults or user misconfiguration.
    int safeMinReplicas = Math.max(1, autoscaling.minReplicas());

    double totalSessions = 0;
    for (PodMetrics pm : podMetrics) {
      totalSessions += pm.metrics().getOrDefault(METRIC_OPEN_SESSIONS, 0.0);
    }

    lastMetric = (int) totalSessions;

    if (totalSessions <= 0) {
      return safeMinReplicas;
    }

    int desired = (int) Math.ceil(totalSessions / autoscaling.scaleUpThreshold());
    return Math.max(desired, safeMinReplicas);
  }

  @Override
  public int lastMetricValue() {
    return lastMetric;
  }
}
