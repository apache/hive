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
 * Unified KEDA ScaledObject dependent resource for metric-based autoscaling.
 * Subclassed per component to define component-specific triggers, HPA behavior,
 * and target workload kind.
 * <p>
 * Note: When HS2 minReplicas is 0, the ScaledObject includes an external-push
 * trigger from the KEDA HTTP Add-on (via InterceptorRoute) for wake-from-zero.
 */
public abstract class HiveScaledObjectDependent extends HiveGenericDependentResource {

  private final String component;
  private final String targetKind;

  protected HiveScaledObjectDependent(String component, String targetKind) {
    super(new GroupVersionKind("keda.sh", "v1alpha1", "ScaledObject"));
    this.component = component;
    this.targetKind = targetKind;
  }

  @Override
  protected GenericKubernetesResource desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    AutoscalingSpec autoscaling = getAutoscalingSpec(hiveCluster);
    int maxReplicas = getMaxReplicas(hiveCluster);
    String targetName = hiveCluster.getMetadata().getName() + "-" + component;

    Map<String, Object> spec = new HashMap<>();
    spec.put("scaleTargetRef", Map.of(
        "apiVersion", "apps/v1",
        "kind", targetKind,
        "name", targetName
    ));
    int minReplicaCount = Math.max(1, autoscaling.minReplicas());
    spec.put("minReplicaCount", minReplicaCount);
    spec.put("maxReplicaCount", maxReplicas);
    if (autoscaling.minReplicas() == 0) {
      spec.put("idleReplicaCount", 0);
    }
    spec.put("cooldownPeriod", autoscaling.cooldownSeconds());
    spec.put("pollingInterval", getPollingInterval());
    spec.put("advanced", getAdvanced(hiveCluster, autoscaling, maxReplicas));
    spec.put("triggers", getTriggers(hiveCluster, autoscaling, maxReplicas, targetName));

    return new GenericKubernetesResourceBuilder()
        .withApiVersion("keda.sh/v1alpha1")
        .withKind("ScaledObject")
        .withNewMetadata()
          .withName(targetName + "-scaledobject")
          .withNamespace(hiveCluster.getMetadata().getNamespace())
          .withLabels(Labels.forComponent(hiveCluster, component))
        .endMetadata()
        .withAdditionalProperties(Map.of("spec", spec))
        .build();
  }

  @Override
  protected String getResourceName(HiveCluster hiveCluster) {
    return hiveCluster.getMetadata().getName() + "-" + component + "-scaledobject";
  }

  /** Returns the autoscaling spec for the component. */
  protected abstract AutoscalingSpec getAutoscalingSpec(HiveCluster hiveCluster);

  /** Returns max replicas (typically the static replicas count from spec). */
  protected abstract int getMaxReplicas(HiveCluster hiveCluster);

  /** Returns the KEDA polling interval in seconds. */
  protected abstract int getPollingInterval();

  /** Returns the "advanced" section (HPA behavior configuration). */
  protected abstract Map<String, Object> getAdvanced(
      HiveCluster hiveCluster, AutoscalingSpec autoscaling, int maxReplicas);

  /** Returns the list of KEDA triggers. */
  protected abstract List<Map<String, Object>> getTriggers(
      HiveCluster hiveCluster, AutoscalingSpec autoscaling,
      int maxReplicas, String targetName);

  /**
   * HiveServer2 ScaledObject: scales on hs2_active_sessions + CPU.
   */
  public static class HiveServer2 extends HiveScaledObjectDependent {
    public HiveServer2() { super("hiveserver2", "Deployment"); }

    @Override
    protected AutoscalingSpec getAutoscalingSpec(HiveCluster hiveCluster) {
      return hiveCluster.getSpec().hiveServer2().autoscaling();
    }

    @Override
    protected int getMaxReplicas(HiveCluster hiveCluster) {
      return hiveCluster.getSpec().hiveServer2().replicas();
    }

    @Override
    protected int getPollingInterval() { return 30; }

    @Override
    protected Map<String, Object> getAdvanced(
        HiveCluster hiveCluster, AutoscalingSpec autoscaling, int maxReplicas) {
      return buildHpaBehavior(
          autoscaling.cooldownSeconds(), "Pods", 1, 60,
          60, "Percent", 100, 60);
    }

    @Override
    protected List<Map<String, Object>> getTriggers(
        HiveCluster hiveCluster, AutoscalingSpec autoscaling,
        int maxReplicas, String targetName) {
      List<Map<String, Object>> triggers = new ArrayList<>();
      triggers.add(Map.of(
          "type", "prometheus",
          "metadata", Map.of(
              "serverAddress", "http://prometheus-server.monitoring.svc.cluster.local",
              "metricName", "hs2_open_sessions",
              "query", String.format(
                  "avg(hs2_open_sessions{namespace=\"%s\",pod=~\"%s-.*\"}) or vector(0)",
                  hiveCluster.getMetadata().getNamespace(), targetName),
              "threshold", String.valueOf(autoscaling.scaleUpThreshold()),
              "activationThreshold", "0"
          )
      ));
      if (hiveCluster.getSpec().hiveServer2().resources() != null) {
        triggers.add(Map.of(
            "type", "cpu",
            "metricType", "Utilization",
            "metadata", Map.of(
                "value", String.valueOf(autoscaling.scaleUpThreshold()),
                "activationValue", String.valueOf(autoscaling.scaleDownThreshold())
            )
        ));
      }
      // When scale-to-zero is enabled, add KEDA HTTP Add-on external-push
      // trigger to wake HS2 from 0 when requests arrive at the interceptor.
      if (autoscaling.minReplicas() == 0) {
        String routeName = HiveServer2InterceptorRouteDependent.resourceName(hiveCluster);
        triggers.add(Map.of(
            "type", "external-push",
            "metadata", Map.of(
                "scalerAddress",
                    "keda-add-ons-http-external-scaler.keda:9090",
                "interceptorRoute", routeName
            )
        ));
      }
      return triggers;
    }
  }

  /**
   * Metastore ScaledObject: scales on open_connections + CPU.
   */
  public static class Metastore extends HiveScaledObjectDependent {
    public Metastore() { super("metastore", "Deployment"); }

    @Override
    protected AutoscalingSpec getAutoscalingSpec(HiveCluster hiveCluster) {
      return hiveCluster.getSpec().metastore().autoscaling();
    }

    @Override
    protected int getMaxReplicas(HiveCluster hiveCluster) {
      return hiveCluster.getSpec().metastore().replicas();
    }

    @Override
    protected int getPollingInterval() { return 30; }

    @Override
    protected Map<String, Object> getAdvanced(
        HiveCluster hiveCluster, AutoscalingSpec autoscaling, int maxReplicas) {
      return buildHpaBehavior(
          autoscaling.cooldownSeconds(), "Pods", 1, 60,
          120, "Percent", 50, 60);
    }

    @Override
    protected List<Map<String, Object>> getTriggers(
        HiveCluster hiveCluster, AutoscalingSpec autoscaling,
        int maxReplicas, String targetName) {
      List<Map<String, Object>> triggers = new ArrayList<>();
      triggers.add(Map.of(
          "type", "prometheus",
          "metadata", Map.of(
              "serverAddress", "http://prometheus-server.monitoring.svc.cluster.local",
              "metricName", "hive_metastore_open_connections",
              "query", String.format(
                  "sum(hive_metastore_open_connections{namespace=\"%s\",pod=~\"%s-.*\"}) or vector(0)",
                  hiveCluster.getMetadata().getNamespace(), targetName),
              "threshold", String.valueOf(autoscaling.scaleUpThreshold()),
              "activationThreshold", "0"
          )
      ));
      if (hiveCluster.getSpec().metastore().resources() != null) {
        triggers.add(Map.of(
            "type", "cpu",
            "metricType", "Utilization",
            "metadata", Map.of(
                "value", String.valueOf(autoscaling.scaleUpThreshold()),
                "activationValue", String.valueOf(autoscaling.scaleDownThreshold())
            )
        ));
      }
      return triggers;
    }
  }

  /**
   * LLAP ScaledObject: scales on NumQueuedRequests + HS2 activation trigger.
   * Scale-down is slow (preserves in-memory cache).
   */
  public static class Llap extends HiveScaledObjectDependent {
    public Llap() { super("llap", "StatefulSet"); }

    @Override
    protected AutoscalingSpec getAutoscalingSpec(HiveCluster hiveCluster) {
      return hiveCluster.getSpec().llap().autoscaling();
    }

    @Override
    protected int getMaxReplicas(HiveCluster hiveCluster) {
      return hiveCluster.getSpec().llap().replicas();
    }

    @Override
    protected int getPollingInterval() { return 5; }

    @Override
    protected Map<String, Object> getAdvanced(
        HiveCluster hiveCluster, AutoscalingSpec autoscaling, int maxReplicas) {
      return buildHpaBehavior(
          autoscaling.cooldownSeconds(), "Pods", 1, autoscaling.cooldownSeconds(),
          0, "Pods", maxReplicas, 15);
    }

    @Override
    protected List<Map<String, Object>> getTriggers(
        HiveCluster hiveCluster, AutoscalingSpec autoscaling,
        int maxReplicas, String targetName) {
      String hs2TargetName = hiveCluster.getMetadata().getName() + "-hiveserver2";
      String namespace = hiveCluster.getMetadata().getNamespace();
      return List.of(
          Map.of(
              "type", "prometheus",
              "metadata", Map.of(
                  "serverAddress", "http://prometheus-server.monitoring.svc.cluster.local",
                  "metricName", "llap_total_busy_slots",
                  "query", String.format(
                      "avg("
                          + "hadoop_llapdaemon_executornumqueuedrequests{namespace=\"%1$s\",pod=~\"%2$s-.*\"}"
                          + " + on(pod) hadoop_llapdaemon_executornumexecutorsconfigured{namespace=\"%1$s\",pod=~\"%2$s-.*\"}"
                          + " - on(pod) hadoop_llapdaemon_executornumexecutorsavailable{namespace=\"%1$s\",pod=~\"%2$s-.*\"}"
                          + ") or vector(0)",
                      namespace, targetName),
                  "threshold", String.valueOf(autoscaling.scaleUpThreshold()),
                  "activationThreshold", "0"
              )
          ),
          buildHs2ActivationTrigger(namespace, hs2TargetName, maxReplicas)
      );
    }
  }

  /**
   * TezAM ScaledObject: scales on CPU (or pending tasks) + HS2 activation trigger.
   * Tez AMs run in a warm pool; claimed AMs consume CPU, idle ones do not.
   */
  public static class TezAm extends HiveScaledObjectDependent {
    public TezAm() { super("tezam", "StatefulSet"); }

    @Override
    protected AutoscalingSpec getAutoscalingSpec(HiveCluster hiveCluster) {
      return hiveCluster.getSpec().tezAm().autoscaling();
    }

    @Override
    protected int getMaxReplicas(HiveCluster hiveCluster) {
      return hiveCluster.getSpec().tezAm().replicas();
    }

    @Override
    protected int getPollingInterval() { return 5; }

    @Override
    protected Map<String, Object> getAdvanced(
        HiveCluster hiveCluster, AutoscalingSpec autoscaling, int maxReplicas) {
      return buildHpaBehavior(
          autoscaling.cooldownSeconds(), "Pods", 1, 60,
          60, "Pods", 2, 30);
    }

    @Override
    protected List<Map<String, Object>> getTriggers(
        HiveCluster hiveCluster, AutoscalingSpec autoscaling,
        int maxReplicas, String targetName) {
      String hs2TargetName = hiveCluster.getMetadata().getName() + "-hiveserver2";
      String namespace = hiveCluster.getMetadata().getNamespace();
      List<Map<String, Object>> triggers = new ArrayList<>();
      if (hiveCluster.getSpec().tezAm().resources() != null) {
        triggers.add(Map.of(
            "type", "cpu",
            "metricType", "Utilization",
            "metadata", Map.of(
                "value", String.valueOf(autoscaling.scaleUpThreshold()),
                "activationValue", String.valueOf(autoscaling.scaleDownThreshold())
            )
        ));
        triggers.add(buildHs2ActivationTrigger(namespace, hs2TargetName, maxReplicas));
      } else {
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
        triggers.add(buildHs2ActivationTrigger(namespace, hs2TargetName, maxReplicas));
      }
      return triggers;
    }
  }
}
