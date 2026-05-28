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

package org.apache.hive.kubernetes.operator.model.spec;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import io.fabric8.generator.annotation.Default;

/** Autoscaling configuration for a Hive component. Uses KEDA ScaledObjects for metric-based scaling. */
public record AutoscalingSpec(
    @JsonPropertyDescription("Whether autoscaling is enabled for this component")
    @Default("false")
    Boolean enabled,
    @JsonPropertyDescription("Minimum number of replicas (floor for scale-down). "
        + "Set to 0 for scale-to-zero (HS2 requires KEDA HTTP Add-on for wake-from-zero)")
    @Default("0")
    Integer minReplicas,
    @JsonPropertyDescription("Threshold that triggers scale-up (component-specific: "
        + "sessions for HS2, connections for HMS, queue depth for LLAP, "
        + "pending tasks for TezAM)")
    @Default("80")
    Integer scaleUpThreshold,
    @JsonPropertyDescription("Threshold that triggers scale-down for Prometheus-based metrics")
    @Default("20")
    Integer scaleDownThreshold,
    @JsonPropertyDescription("Target CPU average value for scaling (e.g., '1500m' or '1'). "
        + "If omitted, CPU scaling is disabled.")
    String targetCpuValue,
    @JsonPropertyDescription("CPU average value below which the trigger is inactive. "
        + "Required if targetCpuValue is set.")
    String activationCpuValue,
    @JsonPropertyDescription("Cooldown period in seconds after a scaling event before another can occur")
    @Default("600")
    Integer cooldownSeconds,
    @JsonPropertyDescription("Maximum time in seconds to wait for graceful drain "
        + "during scale-down before the pod is forcibly terminated")
    @Default("300")
    Integer gracePeriodSeconds) {

  public AutoscalingSpec {
    enabled = enabled != null ? enabled : false;
    minReplicas = minReplicas != null ? minReplicas : 0;
    scaleUpThreshold = scaleUpThreshold != null ? scaleUpThreshold : 80;
    scaleDownThreshold = scaleDownThreshold != null ? scaleDownThreshold : 20;
    cooldownSeconds = cooldownSeconds != null ? cooldownSeconds : 600;
    gracePeriodSeconds = gracePeriodSeconds != null ? gracePeriodSeconds : 300;
  }

  public boolean isEnabled() {
    return enabled;
  }
}
