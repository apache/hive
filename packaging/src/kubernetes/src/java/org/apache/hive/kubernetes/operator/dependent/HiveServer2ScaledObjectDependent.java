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

package org.apache.hive.kubernetes.operator.dependent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.GroupVersionKind;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.spec.AutoscalingSpec;
import org.apache.hive.kubernetes.operator.util.Labels;

/**
 * Manages a KEDA ScaledObject for HiveServer2 autoscaling.
 * <p>
 * Scale-up triggers (OR):
 * - hs2_active_sessions > scaleUpThreshold% of hive.server2.session.max (1 min)
 * - Pod CPU > 75%
 * <p>
 * Scale-down triggers (AND):
 * - hs2_open_sessions < scaleDownThreshold% of max
 * - CPU < 30%
 * <p>
 * Cooldown: configurable (default 600s / 10 minutes)
 */
public class HiveServer2ScaledObjectDependent extends HiveGenericDependentResource {

  public HiveServer2ScaledObjectDependent() {
    super(new GroupVersionKind("keda.sh", "v1alpha1", "ScaledObject"));
  }

  @Override
  protected GenericKubernetesResource desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    AutoscalingSpec autoscaling = hiveCluster.getSpec().hiveServer2().autoscaling();
    int maxReplicas = hiveCluster.getSpec().hiveServer2().replicas();
    String targetName = hiveCluster.getMetadata().getName() + "-hiveserver2";

    Map<String, Object> spec = new HashMap<>();
    spec.put("scaleTargetRef", Map.of(
        "apiVersion", "apps/v1",
        "kind", "Deployment",
        "name", targetName
    ));
    // KEDA requires idleReplicaCount < minReplicaCount.
    // For scale-to-zero: min=1 (minimum when active), idle=0 (scale to zero when idle).
    int minReplicaCount = Math.max(1, autoscaling.minReplicas());
    spec.put("minReplicaCount", minReplicaCount);
    spec.put("maxReplicaCount", maxReplicas);
    if (autoscaling.minReplicas() == 0) {
      spec.put("idleReplicaCount", 0);
    }
    spec.put("cooldownPeriod", autoscaling.cooldownSeconds());
    spec.put("pollingInterval", 30);

    // Advanced scaling policy: scale down one pod at a time for graceful drain
    spec.put("advanced", Map.of(
        "horizontalPodAutoscalerConfig", Map.of(
            "behavior", Map.of(
                "scaleDown", Map.of(
                    "stabilizationWindowSeconds", autoscaling.cooldownSeconds(),
                    "policies", List.of(Map.of(
                        "type", "Pods",
                        "value", 1,
                        "periodSeconds", 60
                    ))
                ),
                "scaleUp", Map.of(
                    "stabilizationWindowSeconds", 60,
                    "policies", List.of(Map.of(
                        "type", "Percent",
                        "value", 100,
                        "periodSeconds", 60
                    ))
                )
            )
        )
    ));

    // Triggers: Prometheus for hs2_active_sessions + CPU fallback (only when CPU requests defined)
    // "or vector(0)" ensures the query returns 0 (not empty) when HS2 has no pods.
    List<Map<String, Object>> triggers = new ArrayList<>();
    triggers.add(Map.of(
        "type", "prometheus",
        "metadata", Map.of(
            "serverAddress", "http://prometheus-server.monitoring.svc.cluster.local",
            "metricName", "hs2_active_sessions",
            "query", String.format(
                "avg(hs2_active_sessions{namespace=\"%s\",pod=~\"%s-.*\"}) or vector(0)",
                hiveCluster.getMetadata().getNamespace(), targetName),
            "threshold", String.valueOf(autoscaling.scaleUpThreshold()),
            "activationThreshold", "0"
        )
    ));
    if (hiveCluster.getSpec().hiveServer2().resources() != null) {
      // activationValue prevents idle JVM CPU from keeping the ScaledObject active.
      triggers.add(Map.of(
          "type", "cpu",
          "metricType", "Utilization",
          "metadata", Map.of(
              "value", "75",
              "activationValue", String.valueOf(autoscaling.scaleDownThreshold())
          )
      ));
    }
    spec.put("triggers", triggers);

    return new GenericKubernetesResourceBuilder()
        .withApiVersion("keda.sh/v1alpha1")
        .withKind("ScaledObject")
        .withNewMetadata()
          .withName(resourceName(hiveCluster))
          .withNamespace(hiveCluster.getMetadata().getNamespace())
          .withLabels(Labels.forComponent(hiveCluster, "hiveserver2"))
        .endMetadata()
        .withAdditionalProperties(Map.of("spec", spec))
        .build();
  }

  @Override
  protected String getResourceName(HiveCluster hiveCluster) {
    return resourceName(hiveCluster);
  }

  public static String resourceName(HiveCluster hiveCluster) {
    return hiveCluster.getMetadata().getName() + "-hiveserver2-scaledobject";
  }
}
