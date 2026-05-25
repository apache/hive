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
import org.apache.hive.kubernetes.operator.util.ConfigUtils;
import org.apache.hive.kubernetes.operator.util.Labels;

/**
 * Manages a KEDA ScaledObject for Hive Metastore autoscaling.
 * <p>
 * Scale-up triggers (OR):
 * - Open connections exceed threshold (Prometheus)
 * - Pod CPU > 75%
 * <p>
 * Scale-down triggers (AND):
 * - CPU < activationValue
 * - Open connections at 0
 * <p>
 * Cooldown: configurable (default 300s / 5 minutes)
 * Guardrail: replicas should be set based on backend DB max_connections.
 */
public class MetastoreScaledObjectDependent extends HiveGenericDependentResource {

  public MetastoreScaledObjectDependent() {
    super(new GroupVersionKind("keda.sh", "v1alpha1", "ScaledObject"));
  }

  @Override
  protected GenericKubernetesResource desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    AutoscalingSpec autoscaling = hiveCluster.getSpec().metastore().autoscaling();
    int maxReplicas = hiveCluster.getSpec().metastore().replicas();
    String targetName = hiveCluster.getMetadata().getName() + "-metastore";

    // Threshold = max threads per pod (from metastore-site config or default 1000).
    // KEDA divides total open_connections by threshold to determine desired replicas.
    int maxThreads = ConfigUtils.getInt(
        hiveCluster.getSpec().metastore().configOverrides(),
        ConfigUtils.METASTORE_SERVER_MAX_THREADS_KEY,
        ConfigUtils.METASTORE_SERVER_MAX_THREADS_HIVE_KEY,
        ConfigUtils.METASTORE_SERVER_MAX_THREADS_DEFAULT);

    Map<String, Object> spec = new HashMap<>();
    spec.put("scaleTargetRef", Map.of(
        "apiVersion", "apps/v1",
        "kind", "Deployment",
        "name", targetName
    ));
    spec.put("minReplicaCount", autoscaling.minReplicas());
    spec.put("maxReplicaCount", maxReplicas);
    spec.put("cooldownPeriod", autoscaling.cooldownSeconds());
    spec.put("pollingInterval", 30);

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
                    "stabilizationWindowSeconds", 120,
                    "policies", List.of(Map.of(
                        "type", "Percent",
                        "value", 50,
                        "periodSeconds", 60
                    ))
                )
            )
        )
    ));

    // Triggers: Prometheus for open connections + CPU (only when CPU requests are defined)
    // "or vector(0)" ensures the query returns 0 (not empty) when no pods match.
    List<Map<String, Object>> triggers = new ArrayList<>();
    triggers.add(Map.of(
        "type", "prometheus",
        "metadata", Map.of(
            "serverAddress", "http://prometheus-server.monitoring.svc.cluster.local",
            "metricName", "hive_metastore_open_connections",
            "query", String.format(
                "sum(hive_metastore_open_connections{namespace=\"%s\",pod=~\"%s-.*\"}) or vector(0)",
                hiveCluster.getMetadata().getNamespace(), targetName),
            "threshold", String.valueOf(maxThreads),
            "activationThreshold", "0"
        )
    ));
    if (hiveCluster.getSpec().metastore().resources() != null) {
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
          .withLabels(Labels.forComponent(hiveCluster, "metastore"))
        .endMetadata()
        .withAdditionalProperties(Map.of("spec", spec))
        .build();
  }

  @Override
  protected String getResourceName(HiveCluster hiveCluster) {
    return resourceName(hiveCluster);
  }

  public static String resourceName(HiveCluster hiveCluster) {
    return hiveCluster.getMetadata().getName() + "-metastore-scaledobject";
  }
}
