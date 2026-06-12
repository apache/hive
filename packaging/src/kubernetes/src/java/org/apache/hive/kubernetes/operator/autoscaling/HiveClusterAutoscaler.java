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

package org.apache.hive.kubernetes.operator.autoscaling;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.HiveClusterSpec;
import org.apache.hive.kubernetes.operator.model.spec.AutoscalingSpec;
import org.apache.hive.kubernetes.operator.model.status.AutoscalingStatus;
import org.apache.hive.kubernetes.operator.util.ConfigUtils;
import org.apache.hive.kubernetes.operator.util.Labels;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main autoscaling orchestrator. Evaluates all enabled components and
 * returns a map of component → desired replica count for those that need changing.
 * <p>
 * Maintains per-cluster, per-component state (stabilization windows).
 */
public class HiveClusterAutoscaler {

  private static final Logger LOG = LoggerFactory.getLogger(HiveClusterAutoscaler.class);

  /** Result of evaluating all components. */
  public record AutoscalingEvaluation(
      Map<String, Integer> patches,
      Map<String, AutoscalingStatus> statuses) {}

  // Shared replica store: the autoscaler writes its desired replicas here so that
  // dependent resources can read them (avoids informer cache lag reverting patches).
  // Key: "namespace/clusterName/component" → desired replicas
  private static final ConcurrentHashMap<String, Integer> MANAGED_REPLICAS =
      new ConcurrentHashMap<>();

  /**
   * Returns the autoscaler-managed replica count for a component, or null if the
   * autoscaler hasn't made a decision yet (e.g., first reconcile before evaluation runs).
   */
  public static Integer getManagedReplicas(String namespace, String clusterName, String component) {
    return MANAGED_REPLICAS.get(namespace + "/" + clusterName + "/" + component);
  }

  /**
   * Sets the managed replica count for a component. Used by suspend/wake logic
   * to override what the autoscaler would normally compute.
   */
  public static void setManagedReplicas(String namespace, String clusterName,
      String component, int replicas) {
    MANAGED_REPLICAS.put(namespace + "/" + clusterName + "/" + component, replicas);
  }

  private record PendingScaleDown(int targetReplicas, Instant annotatedAt) {}

  private final MetricsScraper scraper;
  private final BackgroundMetricsScraper bgScraper;
  private final MetricsCache metricsCache;
  // Key: "namespace/clusterName/component"
  private final ConcurrentHashMap<String, ComponentAutoscaler> autoscalers =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, String> lastScaleTimes =
      new ConcurrentHashMap<>();
  // Two-phase scale-down: holds deferred scale-down targets while pod-deletion-cost
  // annotations propagate (2s delay before applying the actual scale patch).
  private final ConcurrentHashMap<String, PendingScaleDown> pendingScaleDowns =
      new ConcurrentHashMap<>();

  public HiveClusterAutoscaler(MetricsScraper scraper,
      BackgroundMetricsScraper bgScraper, MetricsCache metricsCache) {
    this.scraper = scraper;
    this.bgScraper = bgScraper;
    this.metricsCache = metricsCache;
  }

  public BackgroundMetricsScraper getBackgroundScraper() {
    return bgScraper;
  }

  /**
   * Removes all in-memory state for a deleted HiveCluster to prevent memory leaks.
   */
  public void cleanupCluster(String namespace, String clusterName) {
    String prefix = namespace + "/" + clusterName + "/";
    MANAGED_REPLICAS.keySet().removeIf(k -> k.startsWith(prefix));
    autoscalers.keySet().removeIf(k -> k.startsWith(prefix));
    lastScaleTimes.keySet().removeIf(k -> k.startsWith(prefix));
    pendingScaleDowns.keySet().removeIf(k -> k.startsWith(prefix));
    LOG.info("Cleaned up autoscaler state for {}/{}", namespace, clusterName);
  }

  /**
   * Returns true if there are pending scale-down operations waiting for
   * annotation propagation. The reconciler should reschedule sooner (2s)
   * when this returns true.
   */
  public boolean hasPendingScaleDowns() {
    return !pendingScaleDowns.isEmpty();
  }

  /**
   * Evaluate all autoscaling-enabled components and return patches and status info.
   *
   * @param cluster the HiveCluster resource
   * @param client  the Kubernetes client (for reading current replica counts)
   * @return evaluation result with patches and per-component autoscaling statuses
   */
  public AutoscalingEvaluation evaluate(HiveCluster cluster, KubernetesClient client) {
    Map<String, Integer> patches = new HashMap<>();
    Map<String, AutoscalingStatus> statuses = new HashMap<>();
    HiveClusterSpec spec = cluster.getSpec();
    String namespace = cluster.getMetadata().getNamespace();
    String clusterName = cluster.getMetadata().getName();

    // HiveServer2
    if (spec.hiveServer2().autoscaling().isEnabled()) {
      AutoscalingSpec hs2Auto = spec.hiveServer2().autoscaling();
      String hs2Key = namespace + "/" + clusterName + "/" + ConfigUtils.COMPONENT_HIVESERVER2;
      Map<String, String> hs2Selector = Labels.selectorForComponent(cluster, ConfigUtils.COMPONENT_HIVESERVER2);
      bgScraper.registerOrUpdate(namespace, clusterName,
          ConfigUtils.COMPONENT_HIVESERVER2, hs2Selector,
          hs2Auto.metricsPort(), hs2Auto.metricsScrapeIntervalSeconds());
      int maxStale = hs2Auto.metricsScrapeIntervalSeconds() * 3;
      List<PodMetrics> hs2Metrics = metricsCache.getOrEmpty(hs2Key, maxStale);

      // Two-phase scale-down: check if a pending scale-down from a prior
      // reconcile is ready to be applied (annotations have propagated).
      PendingScaleDown pending = pendingScaleDowns.get(hs2Key);
      if (pending != null) {
        if (Duration.between(pending.annotatedAt(), Instant.now()).toSeconds() >= 2) {
          patches.put(ConfigUtils.COMPONENT_HIVESERVER2, pending.targetReplicas());
          MANAGED_REPLICAS.put(hs2Key, pending.targetReplicas());
          lastScaleTimes.put(hs2Key, Instant.now().toString());
          pendingScaleDowns.remove(hs2Key);
          LOG.info("[hiveserver2] Applying deferred scale-down to {} replicas", pending.targetReplicas());
        }
        // Build status even when waiting for pending scale-down
        evaluateComponent(cluster, client, namespace, clusterName,
            ConfigUtils.COMPONENT_HIVESERVER2, hs2Auto,
            spec.hiveServer2().replicas(), new HashMap<>(), statuses, hs2Metrics);
      } else {
        // Pod deletion cost only applies to Deployments (ReplicaSet controller).
        // StatefulSets always scale down by highest ordinal regardless of this
        // annotation. LLAP/TezAM graceful drain is handled by preStop hooks.
        updateDeploymentPodDeletionCost(client, namespace, hs2Metrics, "hs2_open_sessions");

        Map<String, Integer> hs2Patches = new HashMap<>();
        evaluateComponent(cluster, client, namespace, clusterName,
            ConfigUtils.COMPONENT_HIVESERVER2, hs2Auto,
            spec.hiveServer2().replicas(), hs2Patches, statuses, hs2Metrics);

        Integer hs2Patch = hs2Patches.get(ConfigUtils.COMPONENT_HIVESERVER2);
        int currentReplicas = getCurrentReplicas(client, namespace, clusterName, ConfigUtils.COMPONENT_HIVESERVER2);
        if (hs2Patch != null && hs2Patch < currentReplicas) {
          // Scale-down: defer to allow deletion-cost annotations to propagate
          pendingScaleDowns.put(hs2Key, new PendingScaleDown(hs2Patch, Instant.now()));
          LOG.info("[hiveserver2] Deferring scale-down to {} (waiting for deletion-cost propagation)",
              hs2Patch);
        } else if (hs2Patch != null) {
          // Scale-up: apply immediately
          patches.put(ConfigUtils.COMPONENT_HIVESERVER2, hs2Patch);
          MANAGED_REPLICAS.put(hs2Key, hs2Patch);
        }
      }
    }

    // Metastore
    if (spec.metastore().isEnabled() && spec.metastore().autoscaling().isEnabled()) {
      AutoscalingSpec msAuto = spec.metastore().autoscaling();
      Map<String, String> msSelector = Labels.selectorForComponent(cluster, ConfigUtils.COMPONENT_METASTORE);
      bgScraper.registerOrUpdate(namespace, clusterName,
          ConfigUtils.COMPONENT_METASTORE, msSelector,
          msAuto.metricsPort(), msAuto.metricsScrapeIntervalSeconds());
      String msKey = namespace + "/" + clusterName + "/" + ConfigUtils.COMPONENT_METASTORE;
      List<PodMetrics> msMetrics = metricsCache.getOrEmpty(msKey, msAuto.metricsScrapeIntervalSeconds() * 3);
      evaluateComponent(cluster, client, namespace, clusterName,
          ConfigUtils.COMPONENT_METASTORE, msAuto,
          spec.metastore().replicas(), patches, statuses, msMetrics);
    }

    // LLAP
    if (spec.llap().isEnabled() && spec.llap().autoscaling().isEnabled()) {
      AutoscalingSpec llapAuto = spec.llap().autoscaling();
      Map<String, String> llapSelector = Labels.selectorForComponent(cluster, ConfigUtils.COMPONENT_LLAP);
      bgScraper.registerOrUpdate(namespace, clusterName,
          ConfigUtils.COMPONENT_LLAP, llapSelector,
          llapAuto.metricsPort(), llapAuto.metricsScrapeIntervalSeconds());
      String llapKey = namespace + "/" + clusterName + "/" + ConfigUtils.COMPONENT_LLAP;
      List<PodMetrics> llapMetrics = metricsCache.getOrEmpty(llapKey, llapAuto.metricsScrapeIntervalSeconds() * 3);
      evaluateComponent(cluster, client, namespace, clusterName,
          ConfigUtils.COMPONENT_LLAP, llapAuto,
          spec.llap().replicas(), patches, statuses, llapMetrics);
    }

    // TezAM
    if (spec.tezAm().isEnabled() && spec.tezAm().autoscaling().isEnabled()) {
      AutoscalingSpec tezAuto = spec.tezAm().autoscaling();
      Map<String, String> tezSelector = Labels.selectorForComponent(cluster, ConfigUtils.COMPONENT_TEZAM);
      bgScraper.registerOrUpdate(namespace, clusterName,
          ConfigUtils.COMPONENT_TEZAM, tezSelector,
          tezAuto.metricsPort(), tezAuto.metricsScrapeIntervalSeconds());
      String tezKey = namespace + "/" + clusterName + "/" + ConfigUtils.COMPONENT_TEZAM;
      List<PodMetrics> tezMetrics = metricsCache.getOrEmpty(tezKey, tezAuto.metricsScrapeIntervalSeconds() * 3);
      evaluateComponent(cluster, client, namespace, clusterName,
          ConfigUtils.COMPONENT_TEZAM, tezAuto,
          spec.tezAm().replicas(), patches, statuses, tezMetrics);
    }

    return new AutoscalingEvaluation(patches, statuses);
  }

  /**
   * Returns cached HS2 metrics (used by LLAP/TezAM activation gate).
   * Non-blocking — reads from the background-scraper cache.
   */
  public List<PodMetrics> getHs2MetricsFromCache(HiveCluster cluster) {
    String namespace = cluster.getMetadata().getNamespace();
    String clusterName = cluster.getMetadata().getName();
    String key = namespace + "/" + clusterName + "/" + ConfigUtils.COMPONENT_HIVESERVER2;
    int maxStale = cluster.getSpec().hiveServer2().autoscaling().metricsScrapeIntervalSeconds() * 3;
    return metricsCache.getOrEmpty(key, maxStale);
  }

  private void evaluateComponent(HiveCluster cluster, KubernetesClient client,
      String namespace, String clusterName, String component,
      AutoscalingSpec autoscaling, int maxReplicas,
      Map<String, Integer> patches, Map<String, AutoscalingStatus> statuses,
      List<PodMetrics> metrics) {

    int currentReplicas = getCurrentReplicas(client, namespace, clusterName, component);

    String key = namespace + "/" + clusterName + "/" + component;

    // For LLAP and TezAM, scaling decisions are based on HS2 metrics (activation gate),
    // not their own pod metrics. Allow evaluation even with 0 own pods.
    boolean usesHs2Activation = ConfigUtils.COMPONENT_LLAP.equals(component)
        || ConfigUtils.COMPONENT_TEZAM.equals(component);

    if (metrics.isEmpty() && !usesHs2Activation) {
      LOG.debug("[{}] No ready pods to scrape, skipping", component);
      MANAGED_REPLICAS.put(key, currentReplicas);
      return;
    }

    ComponentAutoscaler autoscaler = autoscalers.computeIfAbsent(key,
        k -> new ComponentAutoscaler(component, createStrategy(component, cluster)));

    ComponentAutoscaler.EvaluationResult result =
        autoscaler.evaluate(metrics, autoscaling, currentReplicas, maxReplicas);

    // Build status
    if (result.patchTo() != null) {
      lastScaleTimes.put(key, Instant.now().toString());
    }
    AutoscalingStatus as = new AutoscalingStatus();
    as.setCurrentMetricValue(result.rawMetricValue());
    // Only show scaleUpThreshold for strategies that use it (TezAM is demand-based, no threshold)
    if (autoscaler.usesScaleUpThreshold()) {
      as.setScaleUpThreshold(autoscaling.scaleUpThreshold());
    }
    // CPU metrics (only for HS2 and HMS — LLAP/TezAM don't use CPU-based scaling)
    if ((ConfigUtils.COMPONENT_HIVESERVER2.equals(component) || ConfigUtils.COMPONENT_METASTORE.equals(component))
        && autoscaling.cpuScaleUpThreshold() > 0) {
      as.setCurrentCpuPercent(result.cpuPercent());
      as.setCpuScaleUpThreshold(autoscaling.cpuScaleUpThreshold());
      as.setCpuProposedReplicas(result.cpuProposedReplicas());
    }
    as.setProposedReplicas(result.proposedReplicas());
    as.setLastScaleTime(lastScaleTimes.get(key));
    statuses.put(component, as);

    if (result.patchTo() != null) {
      int patchValue = result.patchTo();
      patches.put(component, patchValue);
      MANAGED_REPLICAS.put(key, patchValue);
    } else {
      // No change needed — record current replicas as the managed value
      MANAGED_REPLICAS.put(key, currentReplicas);
    }
  }

  private ScalingStrategy createStrategy(String component, HiveCluster cluster) {
    return switch (component) {
    case ConfigUtils.COMPONENT_HIVESERVER2 -> new HiveServer2ScalingStrategy();
    case ConfigUtils.COMPONENT_METASTORE -> new MetastoreScalingStrategy();
    case ConfigUtils.COMPONENT_LLAP -> new LlapScalingStrategy(this, cluster);
    case ConfigUtils.COMPONENT_TEZAM -> new TezAmScalingStrategy(this, cluster);
    default -> throw new IllegalArgumentException("Unknown component: " + component);
    };
  }

  private int getCurrentReplicas(KubernetesClient client, String namespace,
      String clusterName, String component) {
    String workloadName = clusterName + "-" + component;
    if (ConfigUtils.COMPONENT_LLAP.equals(component) || ConfigUtils.COMPONENT_TEZAM.equals(component)) {
      var ss = client.apps().statefulSets()
          .inNamespace(namespace).withName(workloadName).get();
      return ss != null && ss.getSpec().getReplicas() != null ? ss.getSpec().getReplicas() : 0;
    } else {
      var deploy = client.apps().deployments()
          .inNamespace(namespace).withName(workloadName).get();
      return deploy != null && deploy.getSpec().getReplicas() != null
          ? deploy.getSpec().getReplicas() : 0;
    }
  }

  /**
   * Patches each pod's deletion cost annotation based on its active session count.
   * Kubernetes uses this during scale-down to kill idle pods first (lower cost = killed first).
   * <p>
   * Only meaningful for Deployments (HS2, Metastore) — the ReplicaSet controller
   * respects this annotation. StatefulSets ignore it and always terminate by ordinal.
   */
  private void updateDeploymentPodDeletionCost(KubernetesClient client, String namespace,
      List<PodMetrics> metrics, String metricName) {
    for (PodMetrics pm : metrics) {
      int sessions = pm.metrics().getOrDefault(metricName, 0.0).intValue();
      try {
        client.pods().inNamespace(namespace).withName(pm.podName())
            .edit(pod -> {
              pod.getMetadata().getAnnotations()
                  .put("controller.kubernetes.io/pod-deletion-cost", String.valueOf(sessions));
              return pod;
            });
      } catch (Exception e) {
        LOG.debug("Failed to update deletion cost for pod {}: {}", pm.podName(), e.getMessage());
      }
    }
  }

}
