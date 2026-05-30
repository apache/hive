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

/** Strategy for computing desired replica count from scraped pod metrics. */
public interface ScalingStrategy {

  /**
   * Compute desired replica count based on current pod metrics.
   *
   * @param podMetrics  metrics from all pods of this component
   * @param autoscaling the autoscaling configuration
   * @param maxReplicas maximum allowed replicas
   * @return desired replica count (before stabilization/clamping)
   */
  int computeDesiredReplicas(List<PodMetrics> podMetrics,
      AutoscalingSpec autoscaling, int maxReplicas);

  /**
   * Returns the raw metric value from the last evaluation (e.g. total sessions,
   * request rate, busy slots). Used for status reporting.
   */
  default int lastMetricValue() {
    return 0;
  }

  /**
   * Whether this strategy uses scaleUpThreshold from the spec.
   * Strategies that are purely demand-based (e.g. TezAM: 1 TezAM per session)
   * return false so the threshold is not displayed in status.
   */
  default boolean usesScaleUpThreshold() {
    return true;
  }
}
