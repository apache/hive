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
import org.apache.hive.kubernetes.operator.util.ConfigUtils;
import org.apache.hive.kubernetes.operator.util.Labels;

/**
 * Manages a KEDA HTTPScaledObject for HiveServer2 scale-to-zero.
 * <p>
 * Requires the KEDA HTTP Add-on to be installed in the cluster.
 * The HTTP Add-on creates an interceptor proxy that:
 * <ul>
 *   <li>Sits in front of the HS2 Service</li>
 *   <li>Queues incoming beeline/HTTP requests when HS2 has 0 pods</li>
 *   <li>Triggers KEDA to scale HS2 from 0 to 1</li>
 *   <li>Forwards the queued request once a pod is ready</li>
 * </ul>
 * <p>
 * This dependent is activated ONLY when minReplicas == 0 (scale-to-zero mode).
 * When minReplicas > 0, the regular ScaledObject (Prometheus-based) is used instead.
 */
public class HiveServer2HttpScaledObjectDependent extends HiveGenericDependentResource {

  public HiveServer2HttpScaledObjectDependent() {
    super(new GroupVersionKind("http.keda.sh", "v1alpha1", "HTTPScaledObject"));
  }

  @Override
  protected GenericKubernetesResource desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    AutoscalingSpec autoscaling = hiveCluster.getSpec().hiveServer2().autoscaling();
    int maxReplicas = hiveCluster.getSpec().hiveServer2().replicas();
    String clusterName = hiveCluster.getMetadata().getName();
    String namespace = hiveCluster.getMetadata().getNamespace();
    String deploymentName = clusterName + "-hiveserver2";
    String serviceName = clusterName + "-hiveserver2";

    int httpPort = ConfigUtils.getInt(
        hiveCluster.getSpec().hiveServer2().configOverrides(),
        ConfigUtils.HIVE_SERVER2_THRIFT_HTTP_PORT_KEY,
        null, ConfigUtils.HIVE_SERVER2_THRIFT_HTTP_PORT_DEFAULT);

    Map<String, Object> spec = new HashMap<>();

    // Hosts the interceptor matches for routing.
    // Uses internal service DNS names (Ingress rewrites Host header to match these)
    // plus localhost for kubectl port-forward scenarios.
    spec.put("hosts", List.of(
        serviceName + "." + namespace + ".svc.cluster.local",
        serviceName,
        "localhost"
    ));
    spec.put("pathPrefixes", List.of("/"));

    // Target deployment and service
    spec.put("scaleTargetRef", Map.of(
        "name", deploymentName,
        "kind", "Deployment",
        "apiVersion", "apps/v1",
        "service", serviceName,
        "port", httpPort
    ));

    // Replica bounds
    spec.put("replicas", Map.of(
        "min", 0,
        "max", maxReplicas
    ));

    // Scaling metric: scale up when there are pending requests
    spec.put("scalingMetric", Map.of(
        "requestRate", Map.of(
            "granularity", "1s",
            "targetValue", autoscaling.scaleUpThreshold(),
            "window", "1m"
        )
    ));

    // Cooldown before scaling back to 0
    spec.put("scaledownPeriod", autoscaling.cooldownSeconds());

    return new GenericKubernetesResourceBuilder()
        .withApiVersion("http.keda.sh/v1alpha1")
        .withKind("HTTPScaledObject")
        .withNewMetadata()
          .withName(resourceName(hiveCluster))
          .withNamespace(namespace)
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
    return hiveCluster.getMetadata().getName() + "-hiveserver2-httpso";
  }
}
