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
 * Manages a KEDA InterceptorRoute for HiveServer2 scale-to-zero routing.
 * <p>
 * Unlike HTTPScaledObject, InterceptorRoute only configures interceptor
 * routing without auto-creating a ScaledObject. This allows us to manage
 * scaling entirely through a single Prometheus-based ScaledObject that
 * combines session/CPU awareness with the HTTP interceptor wake-from-zero
 * trigger.
 * <p>
 * Requires the KEDA HTTP Add-on to be installed in the cluster.
 */
public class HiveServer2InterceptorRouteDependent extends HiveGenericDependentResource {

  public HiveServer2InterceptorRouteDependent() {
    super(new GroupVersionKind("http.keda.sh", "v1beta1", "InterceptorRoute"));
  }

  @Override
  protected GenericKubernetesResource desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    AutoscalingSpec autoscaling = hiveCluster.getSpec().hiveServer2().autoscaling();
    String clusterName = hiveCluster.getMetadata().getName();
    String namespace = hiveCluster.getMetadata().getNamespace();
    String serviceName = clusterName + "-hiveserver2";

    int httpPort = ConfigUtils.getInt(
        hiveCluster.getSpec().hiveServer2().configOverrides(),
        ConfigUtils.HIVE_SERVER2_THRIFT_HTTP_PORT_KEY,
        null, ConfigUtils.HIVE_SERVER2_THRIFT_HTTP_PORT_DEFAULT);

    // Hosts the interceptor matches for routing
    List<String> hosts = new ArrayList<>(List.of(
        serviceName + "." + namespace + ".svc.cluster.local",
        serviceName,
        "keda-add-ons-http-interceptor-proxy.keda.svc",
        "localhost"
    ));

    Map<String, Object> spec = new HashMap<>();

    // Target backend service
    spec.put("target", Map.of(
        "service", serviceName,
        "port", httpPort
    ));

    // Routing rules
    spec.put("rules", List.of(
        Map.of(
            "hosts", hosts,
            "paths", List.of(Map.of("value", "/"))
        )
    ));

    // Scaling metric (required field, used by interceptor for queue management)
    spec.put("scalingMetric", Map.of(
        "concurrency", Map.of(
            "targetValue", autoscaling.scaleUpThreshold()
        )
    ));

    return new GenericKubernetesResourceBuilder()
        .withApiVersion("http.keda.sh/v1beta1")
        .withKind("InterceptorRoute")
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
    return hiveCluster.getMetadata().getName() + "-hiveserver2-route";
  }
}
