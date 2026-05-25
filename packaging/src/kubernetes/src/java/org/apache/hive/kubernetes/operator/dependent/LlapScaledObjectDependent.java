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
 * Manages a KEDA ScaledObject for LLAP daemon autoscaling.
 * <p>
 * Scale-up trigger:
 * - NumQueuedRequests > 0 for 1 minute (queue non-empty means all executors are busy)
 * <p>
 * Scale-down trigger:
 * - NumExecutorsAvailable == NumExecutors (daemon completely idle)
 * <p>
 * Cooldown: configurable (default 900s / 15 minutes — scaling down destroys in-memory cache)
 */
public class LlapScaledObjectDependent extends HiveGenericDependentResource {

  public LlapScaledObjectDependent() {
    super(new GroupVersionKind("keda.sh", "v1alpha1", "ScaledObject"));
  }

  @Override
  protected GenericKubernetesResource desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    AutoscalingSpec autoscaling = hiveCluster.getSpec().llap().autoscaling();
    int maxReplicas = hiveCluster.getSpec().llap().replicas();
    String targetName = hiveCluster.getMetadata().getName() + "-llap";

    Map<String, Object> spec = new HashMap<>();
    spec.put("scaleTargetRef", Map.of(
        "apiVersion", "apps/v1",
        "kind", "StatefulSet",
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
    spec.put("pollingInterval", 5);

    // LLAP scale-up is aggressive: when queries need daemons, scale immediately to max.
    // Scale down is slow (1 pod per cooldown) to preserve in-memory cache.
    spec.put("advanced", Map.of(
        "horizontalPodAutoscalerConfig", Map.of(
            "behavior", Map.of(
                "scaleDown", Map.of(
                    "stabilizationWindowSeconds", autoscaling.cooldownSeconds(),
                    "policies", List.of(Map.of(
                        "type", "Pods",
                        "value", 1,
                        "periodSeconds", autoscaling.cooldownSeconds()
                    ))
                ),
                "scaleUp", Map.of(
                    "stabilizationWindowSeconds", 0,
                    "policies", List.of(Map.of(
                        "type", "Pods",
                        "value", maxReplicas,
                        "periodSeconds", 15
                    ))
                )
            )
        )
    ));

    // Triggers:
    // 1. Prometheus for NumQueuedRequests — drives proportional scaling.
    //    More queued requests = more LLAP daemons needed. Scales up to max.
    // 2. HS2 open sessions — activation only (wake from 0→1).
    //    Threshold set to maxReplicas so desired = 1/max ≈ 1 (never drives above min).
    //    activationThreshold=0 ensures any session activates the ScaledObject.
    //
    // Scale-down: HPA policy removes 1 pod per cooldown period (preserves cache).
    // Idle (all sessions closed + no queued requests): after cooldownPeriod → 0.
    // "or vector(0)" ensures queries return 0 (not empty) when pods don't exist.
    String hs2TargetName = hiveCluster.getMetadata().getName() + "-hiveserver2";
    String namespace = hiveCluster.getMetadata().getNamespace();
    spec.put("triggers", List.of(
        Map.of(
            "type", "prometheus",
            "metadata", Map.of(
                "serverAddress", "http://prometheus-server.monitoring.svc.cluster.local",
                "metricName", "llap_num_queued_requests",
                "query", String.format(
                    "avg(hadoop_llapdaemon_executornumqueuedrequests{namespace=\"%s\",pod=~\"%s-.*\"}) or vector(0)",
                    namespace, targetName),
                "threshold", String.valueOf(autoscaling.scaleUpThreshold()),
                "activationThreshold", "0"
            )
        ),
        Map.of(
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
        )
    ));

    return new GenericKubernetesResourceBuilder()
        .withApiVersion("keda.sh/v1alpha1")
        .withKind("ScaledObject")
        .withNewMetadata()
          .withName(resourceName(hiveCluster))
          .withNamespace(hiveCluster.getMetadata().getNamespace())
          .withLabels(Labels.forComponent(hiveCluster, "llap"))
        .endMetadata()
        .withAdditionalProperties(Map.of("spec", spec))
        .build();
  }

  @Override
  protected String getResourceName(HiveCluster hiveCluster) {
    return resourceName(hiveCluster);
  }

  public static String resourceName(HiveCluster hiveCluster) {
    return hiveCluster.getMetadata().getName() + "-llap-scaledobject";
  }
}
