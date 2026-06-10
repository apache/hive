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

  private final MetricsScraper scraper;
  // Key: "namespace/clusterName/component"
  private final ConcurrentHashMap<String, ComponentAutoscaler> autoscalers =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, String> lastScaleTimes =
      new ConcurrentHashMap<>();

  public HiveClusterAutoscaler(MetricsScraper scraper) {
    this.scraper = scraper;
  }

  public MetricsScraper getScraper() {
    return scraper;
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
      Map<String, String> hs2Selector = Labels.selectorForComponent(cluster, ConfigUtils.COMPONENT_HIVESERVER2);
      List<PodMetrics> hs2Metrics = scraper.scrape(namespace, hs2Selector, hs2Auto.metricsPort());
      updatePodDeletionCost(client, namespace, hs2Metrics, "hs2_open_sessions");
      evaluateComponent(cluster, client, namespace, clusterName,
          ConfigUtils.COMPONENT_HIVESERVER2, hs2Auto,
          spec.hiveServer2().replicas(), patches, statuses, hs2Metrics);
    }

    // Metastore
    if (spec.metastore().isEnabled() && spec.metastore().autoscaling().isEnabled()) {
      evaluateComponent(cluster, client, namespace, clusterName,
          ConfigUtils.COMPONENT_METASTORE, spec.metastore().autoscaling(),
          spec.metastore().replicas(), patches, statuses);
    }

    // LLAP
    if (spec.llap().isEnabled() && spec.llap().autoscaling().isEnabled()) {
      evaluateComponent(cluster, client, namespace, clusterName,
          ConfigUtils.COMPONENT_LLAP, spec.llap().autoscaling(),
          spec.llap().replicas(), patches, statuses);
    }

    // TezAM
    if (spec.tezAm().isEnabled() && spec.tezAm().autoscaling().isEnabled()) {
      evaluateComponent(cluster, client, namespace, clusterName,
          ConfigUtils.COMPONENT_TEZAM, spec.tezAm().autoscaling(),
          spec.tezAm().replicas(), patches, statuses);
    }

    return new AutoscalingEvaluation(patches, statuses);
  }

  /**
   * Scrape metrics for HS2 pods (used by LLAP/TezAM activation gate).
   */
  public List<PodMetrics> scrapeHs2Metrics(HiveCluster cluster) {
    String namespace = cluster.getMetadata().getNamespace();
    Map<String, String> selector = Labels.selectorForComponent(cluster, ConfigUtils.COMPONENT_HIVESERVER2);
    int port = cluster.getSpec().hiveServer2().autoscaling().metricsPort();
    return scraper.scrape(namespace, selector, port);
  }

  private void evaluateComponent(HiveCluster cluster, KubernetesClient client,
      String namespace, String clusterName, String component,
      AutoscalingSpec autoscaling, int maxReplicas,
      Map<String, Integer> patches, Map<String, AutoscalingStatus> statuses) {
    evaluateComponent(cluster, client, namespace, clusterName, component,
        autoscaling, maxReplicas, patches, statuses, null);
  }

  private void evaluateComponent(HiveCluster cluster, KubernetesClient client,
      String namespace, String clusterName, String component,
      AutoscalingSpec autoscaling, int maxReplicas,
      Map<String, Integer> patches, Map<String, AutoscalingStatus> statuses,
      List<PodMetrics> preScrapedMetrics) {

    int currentReplicas = getCurrentReplicas(client, namespace, clusterName, component);

    String key = namespace + "/" + clusterName + "/" + component;

    List<PodMetrics> metrics;
    if (preScrapedMetrics != null) {
      metrics = preScrapedMetrics;
    } else {
      Map<String, String> selector = Labels.selectorForComponent(cluster, component);
      metrics = scraper.scrape(namespace, selector, autoscaling.metricsPort());
    }

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
   */
  private void updatePodDeletionCost(KubernetesClient client, String namespace,
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
