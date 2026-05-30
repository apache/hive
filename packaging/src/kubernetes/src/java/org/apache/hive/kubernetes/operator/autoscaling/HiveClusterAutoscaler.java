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

  private final MetricsScraper scraper;
  // Key: "namespace/clusterName/component"
  private final ConcurrentHashMap<String, ComponentAutoscaler> autoscalers =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, String> lastScaleTimes =
      new ConcurrentHashMap<>();

  public HiveClusterAutoscaler(MetricsScraper scraper) {
    this.scraper = scraper;
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
      evaluateComponent(cluster, client, namespace, clusterName,
          "hiveserver2", spec.hiveServer2().autoscaling(),
          spec.hiveServer2().replicas(), patches, statuses);
    }

    // Metastore
    if (spec.metastore().isEnabled() && spec.metastore().autoscaling().isEnabled()) {
      evaluateComponent(cluster, client, namespace, clusterName,
          "metastore", spec.metastore().autoscaling(),
          spec.metastore().replicas(), patches, statuses);
    }

    // LLAP
    if (spec.llap().isEnabled() && spec.llap().autoscaling().isEnabled()) {
      evaluateComponent(cluster, client, namespace, clusterName,
          "llap", spec.llap().autoscaling(),
          spec.llap().replicas(), patches, statuses);
    }

    // TezAM
    if (spec.tezAm().isEnabled() && spec.tezAm().autoscaling().isEnabled()) {
      evaluateComponent(cluster, client, namespace, clusterName,
          "tezam", spec.tezAm().autoscaling(),
          spec.tezAm().replicas(), patches, statuses);
    }

    return new AutoscalingEvaluation(patches, statuses);
  }

  /**
   * Scrape metrics for HS2 pods (used by LLAP/TezAM activation gate).
   */
  public List<PodMetrics> scrapeHs2Metrics(HiveCluster cluster) {
    String namespace = cluster.getMetadata().getNamespace();
    Map<String, String> selector = Labels.selectorForComponent(cluster, "hiveserver2");
    return scraper.scrape(namespace, selector);
  }

  private void evaluateComponent(HiveCluster cluster, KubernetesClient client,
      String namespace, String clusterName, String component,
      AutoscalingSpec autoscaling, int maxReplicas,
      Map<String, Integer> patches, Map<String, AutoscalingStatus> statuses) {

    int currentReplicas = getCurrentReplicas(client, namespace, clusterName, component);

    String key = namespace + "/" + clusterName + "/" + component;

    Map<String, String> selector = Labels.selectorForComponent(cluster, component);
    List<PodMetrics> metrics = scraper.scrape(namespace, selector);

    // For LLAP and TezAM, scaling decisions are based on HS2 metrics (activation gate),
    // not their own pod metrics. Allow evaluation even with 0 own pods.
    boolean usesHs2Activation = component.equals("llap") || component.equals("tezam");

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
      case "hiveserver2" -> new HiveServer2ScalingStrategy();
      case "metastore" -> new MetastoreScalingStrategy();
      case "llap" -> new LlapScalingStrategy(this, cluster);
      case "tezam" -> new TezAmScalingStrategy(this, cluster);
      default -> throw new IllegalArgumentException("Unknown component: " + component);
    };
  }

  private int getCurrentReplicas(KubernetesClient client, String namespace,
      String clusterName, String component) {
    String workloadName = clusterName + "-" + component;
    if ("llap".equals(component) || "tezam".equals(component)) {
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

}
