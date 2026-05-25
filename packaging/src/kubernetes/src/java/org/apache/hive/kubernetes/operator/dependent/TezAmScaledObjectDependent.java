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
 * Manages a KEDA ScaledObject for Tez Application Master autoscaling.
 * <p>
 * Tez AMs run in a warm pool (StatefulSet). An unclaimed AM sits idle;
 * a claimed AM actively orchestrates a query DAG and consumes CPU.
 * <p>
 * Scale-up trigger:
 * - Pod CPU > 60% across the StatefulSet (most AMs claimed and working)
 * <p>
 * Scale-down trigger:
 * - Pod CPU < 10% (many idle unclaimed AMs)
 * <p>
 * Cooldown: configurable (default 600s / 10 minutes)
 */
public class TezAmScaledObjectDependent extends HiveGenericDependentResource {

  public TezAmScaledObjectDependent() {
    super(new GroupVersionKind("keda.sh", "v1alpha1", "ScaledObject"));
  }

  @Override
  protected GenericKubernetesResource desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    AutoscalingSpec autoscaling = hiveCluster.getSpec().tezAm().autoscaling();
    int maxReplicas = hiveCluster.getSpec().tezAm().replicas();
    String targetName = hiveCluster.getMetadata().getName() + "-tezam";

    Map<String, Object> spec = new HashMap<>();
    spec.put("scaleTargetRef", Map.of(
        "apiVersion", "apps/v1",
        "kind", "StatefulSet",
        "name", targetName
    ));
    // KEDA requires idleReplicaCount < minReplicaCount.
    // For scale-to-zero: min=1 (minimum when active), idle=0 (scale to zero when idle).
    // For non-zero min: just set minReplicaCount (no idle needed).
    int minReplicaCount = Math.max(1, autoscaling.minReplicas());
    spec.put("minReplicaCount", minReplicaCount);
    spec.put("maxReplicaCount", maxReplicas);
    if (autoscaling.minReplicas() == 0) {
      spec.put("idleReplicaCount", 0);
    }
    spec.put("cooldownPeriod", autoscaling.cooldownSeconds());
    spec.put("pollingInterval", 5);

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
                        "type", "Pods",
                        "value", 2,
                        "periodSeconds", 30
                    ))
                )
            )
        )
    ));

    // Triggers:
    // 1. CPU utilization — the primary proportional scaler for warm-pool Tez AMs
    //    (only included when container has CPU requests defined, required by KEDA)
    // 2. HS2 cross-component activation: when HS2 has open sessions,
    //    TezAM should be available (enables wake-from-zero)
    //
    // When CPU IS available: CPU drives proportional scaling, HS2 trigger is activation-only
    //   (threshold set to maxReplicas so it never dominates the HPA calculation).
    // When CPU is NOT available: tez_session_pending_tasks drives proportional scaling
    //   (real query demand — tasks waiting for AM slots), with HS2 sessions for activation only.
    String hs2TargetName = hiveCluster.getMetadata().getName() + "-hiveserver2";
    String namespace = hiveCluster.getMetadata().getNamespace();
    List<Map<String, Object>> triggers = new ArrayList<>();
    if (hiveCluster.getSpec().tezAm().resources() != null) {
      // CPU drives proportional scaling; activationValue prevents idle JVM CPU
      // from keeping the ScaledObject permanently "active" (blocks scale-to-zero).
      triggers.add(Map.of(
          "type", "cpu",
          "metricType", "Utilization",
          "metadata", Map.of(
              "value", String.valueOf(autoscaling.scaleUpThreshold()),
              "activationValue", String.valueOf(autoscaling.scaleDownThreshold())
          )
      ));
      // Activation-only: (sessions > bool 0) returns 0 or 1, with threshold=maxReplicas
      // ensures desired = ceil(1/max) = 1 — never drives replica count above min.
      // activationThreshold=0 ensures any open session wakes TezAM from zero.
      // Uses hs2_open_sessions (connection-level) not hs2_active_sessions (query-level).
      // "or vector(0)" ensures the query returns 0 (not empty) when HS2 has no pods.
      triggers.add(Map.of(
          "type", "prometheus",
          "metadata", Map.of(
              "serverAddress", "http://prometheus-server.monitoring.svc.cluster.local",
              "metricName", "hs2_open_sessions_activation",
              "query", String.format(
                  "(max(hs2_open_sessions{namespace=\"%s\",pod=~\"%s-.*\"}) > bool 0) or vector(0)",
                  namespace, hs2TargetName),
              "threshold", String.valueOf(maxReplicas),
              "activationThreshold", "0"
          )
      ));
    } else {
      // No CPU available: use tez_session_pending_tasks for proportional scaling.
      // This metric reflects real query demand (tasks waiting for AM slots), unlike
      // hs2_open_sessions which includes zombie/idle sessions from ungracefully closed clients.
      // Threshold: scaleUpThreshold interpreted as pending-tasks-per-AM (default 60 when
      // using CPU mode, but for pending tasks a lower value like 5-10 is recommended).
      // "or vector(0)" ensures the query returns 0 when HS2 has no pods.
      triggers.add(Map.of(
          "type", "prometheus",
          "metadata", Map.of(
              "serverAddress", "http://prometheus-server.monitoring.svc.cluster.local",
              "metricName", "tez_session_pending_tasks",
              "query", String.format(
                  "sum(tez_session_pending_tasks{namespace=\"%s\",pod=~\"%s-.*\"}) or vector(0)",
                  namespace, hs2TargetName),
              "threshold", String.valueOf(autoscaling.scaleUpThreshold()),
              "activationThreshold", "0"
          )
      ));
      // Activation-only: (sessions > bool 0) returns 0 or 1, with threshold=maxReplicas
      // ensures desired = ceil(1/max) = 1 — never drives replica count above min.
      // activationThreshold=0 ensures any open session wakes TezAM from zero.
      triggers.add(Map.of(
          "type", "prometheus",
          "metadata", Map.of(
              "serverAddress", "http://prometheus-server.monitoring.svc.cluster.local",
              "metricName", "hs2_open_sessions_activation",
              "query", String.format(
                  "(max(hs2_open_sessions{namespace=\"%s\",pod=~\"%s-.*\"}) > bool 0) or vector(0)",
                  namespace, hs2TargetName),
              "threshold", String.valueOf(maxReplicas),
              "activationThreshold", "0"
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
          .withLabels(Labels.forComponent(hiveCluster, "tezam"))
        .endMetadata()
        .withAdditionalProperties(Map.of("spec", spec))
        .build();
  }

  @Override
  protected String getResourceName(HiveCluster hiveCluster) {
    return resourceName(hiveCluster);
  }

  public static String resourceName(HiveCluster hiveCluster) {
    return hiveCluster.getMetadata().getName() + "-tezam-scaledobject";
  }
}
