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

/** Autoscaling configuration for a Hive component. The operator scrapes JMX metrics directly from pods. */
public record AutoscalingSpec(
    @JsonPropertyDescription("Whether autoscaling is enabled for this component")
    @Default("false")
    Boolean enabled,
    @JsonPropertyDescription("Minimum number of replicas (floor for scale-down). "
        + "Set to 0 for scale-to-zero (LLAP, TezAM only; HS2 minimum is 1)")
    @Default("0")
    Integer minReplicas,
    @JsonPropertyDescription("Threshold that triggers scale-up (component-specific: "
        + "sessions per pod for HS2, request rate for HMS, busy slots per daemon for LLAP). "
        + "Not used by TezAM (demand-based: 1 TezAM per session).")
    @Default("80")
    Integer scaleUpThreshold,
    @JsonPropertyDescription("Stabilization window in seconds for scale-up decisions. "
        + "Picks the highest recommendation within this window to prevent flapping.")
    @Default("60")
    Integer scaleUpStabilizationSeconds,
    @JsonPropertyDescription("Stabilization window in seconds for scale-down decisions. "
        + "How long metrics must consistently indicate fewer replicas before "
        + "scale-down occurs. Also acts as the cooldown between consecutive scale-downs.")
    @Default("600")
    Integer scaleDownStabilizationSeconds,
    @JsonPropertyDescription("Maximum time in seconds to wait for graceful drain "
        + "during scale-down before the pod is forcibly terminated. "
        + "The pod terminates immediately once sessions/connections drain to 0; "
        + "this value is only the upper safety cap.")
    @Default("3600")
    Integer gracePeriodSeconds,
    @JsonPropertyDescription("How often (seconds) the operator scrapes JMX metrics from pods. "
        + "Lower values make autoscaling react faster.")
    @Default("10")
    Integer metricsScrapeIntervalSeconds,
    @JsonPropertyDescription("CPU percentage (0-100) that triggers scale-up. "
        + "Only applies to HS2 and HMS. Set to 0 to disable CPU-based scaling.")
    @Default("90")
    Integer cpuScaleUpThreshold,
    @JsonPropertyDescription("CPU percentage (0-100) below which scale-down is considered. "
        + "Only applies to HS2 and HMS.")
    @Default("30")
    Integer cpuScaleDownThreshold) {

  public AutoscalingSpec {
    enabled = enabled != null ? enabled : false;
    minReplicas = minReplicas != null ? minReplicas : 0;
    scaleUpThreshold = scaleUpThreshold != null ? scaleUpThreshold : 80;
    scaleUpStabilizationSeconds = scaleUpStabilizationSeconds != null ? scaleUpStabilizationSeconds : 60;
    scaleDownStabilizationSeconds = scaleDownStabilizationSeconds != null ? scaleDownStabilizationSeconds : 600;
    gracePeriodSeconds = gracePeriodSeconds != null ? gracePeriodSeconds : 3600;
    metricsScrapeIntervalSeconds = metricsScrapeIntervalSeconds != null ? metricsScrapeIntervalSeconds : 10;
    cpuScaleUpThreshold = cpuScaleUpThreshold != null ? cpuScaleUpThreshold : 90;
    cpuScaleDownThreshold = cpuScaleDownThreshold != null ? cpuScaleDownThreshold : 30;
  }

  public boolean isEnabled() {
    return enabled;
  }
}
