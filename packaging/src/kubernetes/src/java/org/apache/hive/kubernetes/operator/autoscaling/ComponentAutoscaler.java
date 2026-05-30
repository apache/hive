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

import java.time.Duration;
import java.util.List;

import org.apache.hive.kubernetes.operator.model.spec.AutoscalingSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-component autoscaler state. Owns the scaling strategy,
 * stabilization windows.
 */
public class ComponentAutoscaler {

  /** Result of an autoscaling evaluation. */
  public record EvaluationResult(int rawMetricValue, int proposedReplicas, Integer patchTo) {}


  private static final Logger LOG = LoggerFactory.getLogger(ComponentAutoscaler.class);

  private final String component;
  private final ScalingStrategy strategy;
  private StabilizationWindow scaleUpWindow;
  private StabilizationWindow scaleDownWindow;
  private int lastScaleUpStabilization = -1;
  private int lastScaleDownStabilization = -1;
  private boolean initialized;

  public ComponentAutoscaler(String component, ScalingStrategy strategy) {
    this.component = component;
    this.strategy = strategy;
  }

  /** Whether the underlying strategy uses scaleUpThreshold for scaling decisions. */
  public boolean usesScaleUpThreshold() {
    return strategy.usesScaleUpThreshold();
  }

  /**
   * Evaluate metrics and return the evaluation result containing
   * raw metric value, proposed replicas, and the actual patch (null if no change).
   */
  public EvaluationResult evaluate(List<PodMetrics> metrics, AutoscalingSpec spec,
      int currentReplicas, int maxReplicas) {

    ensureWindows(spec);

    // On first evaluation, seed the scale-down window with currentReplicas.
    // This prevents immediate scale-down after operator restart when the window has no history.
    if (!initialized) {
      initialized = true;
      scaleDownWindow.record(currentReplicas);
      LOG.debug("[{}] Initialized scale-down window with currentReplicas={}", component, currentReplicas);
    }

    int rawDesired = strategy.computeDesiredReplicas(metrics, spec, maxReplicas);
    int metricValue = strategy.lastMetricValue();
    int clamped = Math.max(spec.minReplicas(), Math.min(rawDesired, maxReplicas));

    scaleUpWindow.record(clamped);
    scaleDownWindow.record(clamped);

    int target;
    if (clamped > currentReplicas) {
      // Scale up: use stabilized max (highest recommendation in window — don't under-scale)
      target = scaleUpWindow.stabilizedMax();
    } else if (clamped < currentReplicas) {
      // Scale down: use stabilized max (highest/most conservative recommendation in window —
      // prevents premature scale-down, matches HPA selectPolicy: Max behavior).
      // The stabilization window duration serves as the cooldown between scale-downs.
      target = scaleDownWindow.stabilizedMax();
    } else {
      target = currentReplicas;
    }

    // Ensure target is still within bounds
    target = Math.max(spec.minReplicas(), Math.min(target, maxReplicas));

    if (target == currentReplicas) {
      return new EvaluationResult(metricValue, clamped, null);
    }

    if (target < currentReplicas) {
      LOG.info("[{}] Scaling down: {} -> {}", component, currentReplicas, target);
    } else {
      LOG.info("[{}] Scaling up: {} -> {}", component, currentReplicas, target);
    }
    return new EvaluationResult(metricValue, clamped, target);
  }

  private void ensureWindows(AutoscalingSpec spec) {
    if (scaleUpWindow == null || lastScaleUpStabilization != spec.scaleUpStabilizationSeconds()) {
      scaleUpWindow = new StabilizationWindow(
          Duration.ofSeconds(spec.scaleUpStabilizationSeconds()));
      lastScaleUpStabilization = spec.scaleUpStabilizationSeconds();
    }
    if (scaleDownWindow == null || lastScaleDownStabilization != spec.scaleDownStabilizationSeconds()) {
      scaleDownWindow = new StabilizationWindow(
          Duration.ofSeconds(spec.scaleDownStabilizationSeconds()));
      lastScaleDownStabilization = spec.scaleDownStabilizationSeconds();
    }
  }
}
