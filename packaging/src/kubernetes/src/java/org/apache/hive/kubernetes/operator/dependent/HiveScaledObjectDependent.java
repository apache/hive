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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unified KEDA ScaledObject dependent resource for metric-based autoscaling.
 * Subclassed per component to define component-specific triggers, HPA behavior,
 * and target workload kind.
 * <p>
 * Note: When HS2 minReplicas is 0, the ScaledObject includes an external-push
 * trigger from the KEDA HTTP Add-on (via InterceptorRoute) for wake-from-zero.
 */
public abstract class HiveScaledObjectDependent extends HiveGenericDependentResource {

  private static final Logger LOG = LoggerFactory.getLogger(HiveScaledObjectDependent.class);

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
    spec.put("minReplicaCount", autoscaling.minReplicas());
    spec.put("maxReplicaCount", maxReplicas);
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
    public HiveServer2() {
      super("hiveserver2", "Deployment");
    }

    @Override
    protected AutoscalingSpec getAutoscalingSpec(HiveCluster hiveCluster) {
      return hiveCluster.getSpec().hiveServer2().autoscaling();
    }

    @Override
    protected int getMaxReplicas(HiveCluster hiveCluster) {
      return hiveCluster.getSpec().hiveServer2().replicas();
    }

    @Override
    protected int getPollingInterval() {
      return 30;
    }

    @Override
    protected Map<String, Object> getAdvanced(
        HiveCluster hiveCluster, AutoscalingSpec autoscaling, int maxReplicas) {
      return buildHpaBehavior(
          autoscaling.scaleDownStabilizationSeconds(), "Pods", 1, 60,
          autoscaling.scaleUpStabilizationSeconds(), "Percent", 100, 60);
    }

    @Override
    protected List<Map<String, Object>> getTriggers(
        HiveCluster hiveCluster, AutoscalingSpec autoscaling,
        int maxReplicas, String targetName) {
      List<Map<String, Object>> triggers = new ArrayList<>();
      // Use sum() so KEDA computes desired replicas from total session count.
      // desired = ceil(sum / threshold). With sum=2, threshold=1: desired=2
      // → prevents premature scale-down while sessions are active.
      // avg() would divide across pods, hiding load and causing scale-down
      // of pods with sessions.
      triggers.add(buildPrometheusTrigger(
          "hs2_open_sessions",
          String.format(
              "sum(hs2_open_sessions{namespace=\"%s\",pod=~\"%s-.*\"}) or vector(0)",
              hiveCluster.getMetadata().getNamespace(), targetName),
          String.valueOf(autoscaling.scaleUpThreshold())));
      Map<String, Object> cpuTrigger = buildCpuTrigger(
          autoscaling, hiveCluster.getSpec().hiveServer2().resources(),
          "HiveServer2", LOG);
      if (cpuTrigger != null) {
        triggers.add(cpuTrigger);
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
    public Metastore() {
      super("metastore", "Deployment");
    }

    @Override
    protected AutoscalingSpec getAutoscalingSpec(HiveCluster hiveCluster) {
      return hiveCluster.getSpec().metastore().autoscaling();
    }

    @Override
    protected int getMaxReplicas(HiveCluster hiveCluster) {
      return hiveCluster.getSpec().metastore().replicas();
    }

    @Override
    protected int getPollingInterval() {
      return 30;
    }

    @Override
    protected Map<String, Object> getAdvanced(
        HiveCluster hiveCluster, AutoscalingSpec autoscaling, int maxReplicas) {
      return buildHpaBehavior(
          autoscaling.scaleDownStabilizationSeconds(), "Pods", 1, 60,
          autoscaling.scaleUpStabilizationSeconds(), "Percent", 50, 60);
    }

    @Override
    protected List<Map<String, Object>> getTriggers(
        HiveCluster hiveCluster, AutoscalingSpec autoscaling,
        int maxReplicas, String targetName) {
      List<Map<String, Object>> triggers = new ArrayList<>();
      // HMS runs in HTTP transport mode — connections are per-request (stateless),
      // so open_connections is always ~0. Use aggregate API request rate instead.
      // Note: Prometheus 3.x rejects rate() on __name__ regex selectors, so we
      // compute rate manually as (sum(counters) - sum(counters offset 2m)) / 120.
      triggers.add(buildPrometheusTrigger(
          "hive_metastore_api_rate",
          String.format(
              "(sum({__name__=~\"api_.+_total\",namespace=\"%s\",pod=~\"%s-.*\"})"
                  + " - sum({__name__=~\"api_.+_total\",namespace=\"%s\",pod=~\"%s-.*\"} offset 2m))"
                  + " / 120 or vector(0)",
              hiveCluster.getMetadata().getNamespace(), targetName,
              hiveCluster.getMetadata().getNamespace(), targetName),
          String.valueOf(autoscaling.scaleUpThreshold())));
      Map<String, Object> cpuTrigger = buildCpuTrigger(
          autoscaling, hiveCluster.getSpec().metastore().resources(),
          "Metastore", LOG);
      if (cpuTrigger != null) {
        triggers.add(cpuTrigger);
      }
      return triggers;
    }
  }

  /**
   * LLAP ScaledObject: scales on NumQueuedRequests + HS2 activation trigger.
   * Scale-down is slow (preserves in-memory cache).
   */
  public static class Llap extends HiveScaledObjectDependent {
    public Llap() {
      super("llap", "StatefulSet");
    }

    @Override
    protected AutoscalingSpec getAutoscalingSpec(HiveCluster hiveCluster) {
      return hiveCluster.getSpec().llap().autoscaling();
    }

    @Override
    protected int getMaxReplicas(HiveCluster hiveCluster) {
      return hiveCluster.getSpec().llap().replicas();
    }

    @Override
    protected int getPollingInterval() {
      return 5;
    }

    @Override
    protected Map<String, Object> getAdvanced(
        HiveCluster hiveCluster, AutoscalingSpec autoscaling, int maxReplicas) {
      // Scale-up stabilization=0: LLAP is a reactive dependent that must
      // track HS2 immediately — no delay on scale-up.
      return buildHpaBehavior(
          autoscaling.scaleDownStabilizationSeconds(), "Pods", 1, autoscaling.scaleDownStabilizationSeconds(),
          0, "Pods", maxReplicas, 15);
    }

    @Override
    protected List<Map<String, Object>> getTriggers(
        HiveCluster hiveCluster, AutoscalingSpec autoscaling,
        int maxReplicas, String targetName) {
      String hs2TargetName = hiveCluster.getMetadata().getName() + "-hiveserver2";
      String namespace = hiveCluster.getMetadata().getNamespace();
      return List.of(
          buildPrometheusTrigger(
              "llap_total_busy_slots",
              String.format(
                  "avg("
                      + "hadoop_llapdaemon_executornumqueuedrequests{namespace=\"%1$s\",pod=~\"%2$s-.*\"}"
                      + " + on(pod) hadoop_llapdaemon_executornumexecutorsconfigured{namespace=\"%1$s\",pod=~\"%2$s-.*\"}"
                      + " - on(pod) hadoop_llapdaemon_executornumexecutorsavailable{namespace=\"%1$s\",pod=~\"%2$s-.*\"}"
                      + ") or vector(0)",
                  namespace, targetName),
              String.valueOf(autoscaling.scaleUpThreshold())),
          buildHs2ActivationTrigger(namespace, hs2TargetName, maxReplicas)
      );
    }
  }

  /**
   * TezAM ScaledObject: scales on HS2 session demand.
   * Each HS2 pod claims {@code sessions.per.default.queue} TezAM sessions
   * (exclusive binding). Demand = active HS2 pods × sessions per queue.
   * Primary trigger: count of HS2 pods with open sessions × sessions_per_queue.
   */
  public static class TezAm extends HiveScaledObjectDependent {
    public TezAm() {
      super("tezam", "StatefulSet");
    }

    @Override
    protected AutoscalingSpec getAutoscalingSpec(HiveCluster hiveCluster) {
      return hiveCluster.getSpec().tezAm().autoscaling();
    }

    @Override
    protected int getMaxReplicas(HiveCluster hiveCluster) {
      return hiveCluster.getSpec().tezAm().replicas();
    }

    @Override
    protected int getPollingInterval() {
      return 5;
    }

    @Override
    protected Map<String, Object> getAdvanced(
        HiveCluster hiveCluster, AutoscalingSpec autoscaling, int maxReplicas) {
      // Scale-up stabilization=0: TezAM is a reactive dependent that must
      // track HS2 sessions immediately — no delay on scale-up.
      return buildHpaBehavior(
          autoscaling.scaleDownStabilizationSeconds(), "Pods", 1, 60,
          0, "Pods", maxReplicas, 15);
    }

    @Override
    protected List<Map<String, Object>> getTriggers(
        HiveCluster hiveCluster, AutoscalingSpec autoscaling,
        int maxReplicas, String targetName) {
      String hs2TargetName = hiveCluster.getMetadata().getName() + "-hiveserver2";
      String namespace = hiveCluster.getMetadata().getNamespace();

      // Read sessions.per.default.queue from HS2 configOverrides (default 1).
      // Each HS2 pod pre-warms this many TezAM sessions in its pool.
      int sessionsPerQueue = ConfigUtils.getInt(
          hiveCluster.getSpec().hiveServer2().configOverrides(),
          ConfigUtils.HIVE_SERVER2_TEZ_SESSIONS_PER_QUEUE_KEY,
          null, ConfigUtils.HIVE_SERVER2_TEZ_SESSIONS_PER_QUEUE_DEFAULT);

      List<Map<String, Object>> triggers = new ArrayList<>();
      // Trigger 1: Concurrent demand — total open sessions across all HS2 pods.
      // Each session may run a query needing its own TezAM.
      // threshold=1 → desired = total open sessions.
      triggers.add(buildPrometheusTrigger(
          "hs2_tezam_session_demand",
          String.format(
              "sum(hs2_open_sessions{namespace=\"%s\",pod=~\"%s-.*\"}) or vector(0)",
              namespace, hs2TargetName),
          "1"));
      // Trigger 2: Pre-warm — each running HS2 pod needs sessions_per_queue TezAMs
      // in its pool (claimed eagerly at startup by default).
      // threshold=1 → desired = HS2_pod_count × sessions_per_queue.
      triggers.add(buildPrometheusTrigger(
          "hs2_tezam_prewarm",
          String.format(
              "count(hs2_open_sessions{namespace=\"%s\",pod=~\"%s-.*\"}) * %d or vector(0)",
              namespace, hs2TargetName, sessionsPerQueue),
          "1"));
      // KEDA uses max(trigger1, trigger2) → ensures enough TezAMs for both
      // concurrent queries AND per-HS2 pre-warm pools.
      triggers.add(buildHs2ActivationTrigger(namespace, hs2TargetName, maxReplicas));
      return triggers;
    }
  }
}
