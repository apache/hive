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

package org.apache.hive.kubernetes.operator.reconciler;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.fabric8.kubernetes.api.model.Condition;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.apache.hive.kubernetes.operator.autoscaling.HiveClusterAutoscaler;
import org.apache.hive.kubernetes.operator.autoscaling.MetricsScraper;
import org.apache.hive.kubernetes.operator.autoscaling.PodMetrics;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.HiveClusterSpec;
import org.apache.hive.kubernetes.operator.model.HiveClusterStatus;
import org.apache.hive.kubernetes.operator.model.spec.AutoSuspendSpec;
import org.apache.hive.kubernetes.operator.model.status.AutoscalingStatus;
import org.apache.hive.kubernetes.operator.model.status.ComponentStatus;
import org.apache.hive.kubernetes.operator.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main reconciler for the HiveCluster custom resource.
 * Orchestrates all dependent resources with proper dependency ordering.
 */
@ControllerConfiguration
public class HiveClusterReconciler implements Reconciler<HiveCluster> {

  private static final Logger LOG = LoggerFactory.getLogger(HiveClusterReconciler.class);

  private volatile HiveClusterAutoscaler autoscaler;

  @Override
  public UpdateControl<HiveCluster> reconcile(HiveCluster resource, Context<HiveCluster> context) {
    LOG.debug("Reconciling HiveCluster: {}/{}  generation={}",
        resource.getMetadata().getNamespace(),
        resource.getMetadata().getName(),
        resource.getMetadata().getGeneration());

    HiveClusterStatus existingStatus = resource.getStatus();
    HiveClusterStatus newStatus = buildStatus(resource, context, existingStatus);

    // --- Suspend / Wake evaluation (works regardless of autoscaling) ---
    KubernetesClient client = context.getClient();
    SuspendAction action = evaluateSuspendState(resource, existingStatus, client);
    int rescheduleSeconds = 0;

    switch (action) {
      case SUSPEND_NOW:
        suspendCluster(resource, client);
        boolean manual = resource.getSpec().suspend();
        // Auto-suspend: set spec.suspend=true so the cluster stays suspended
        // until the user explicitly sets it to false.
        // The spec patch triggers a watch event → immediate re-reconcile where
        // STAY_SUSPENDED sets the status cleanly.
        if (!manual) {
          patchSuspendSpec(client, resource, true);
          return UpdateControl.noUpdate();
        }
        String reason = "ManualSuspend";
        newStatus.setClusterPhase("Suspended");
        newStatus.setSuspendedSince(Instant.now().toString());
        newStatus.setIdleSince(null);
        newStatus.getConditions().add(buildCondition("Suspended", "True", reason,
            "Cluster suspended via spec.suspend",
            existingStatus != null ? existingStatus.getConditions() : Collections.emptyList()));
        rescheduleSeconds = 30;
        break;

      case STAY_SUSPENDED:
        newStatus.setClusterPhase("Suspended");
        newStatus.setSuspendedSince(existingStatus != null ? existingStatus.getSuspendedSince() : null);
        newStatus.setIdleSince(null);
        newStatus.getConditions().add(buildCondition("Suspended", "True", "Suspended",
            "Cluster is suspended",
            existingStatus != null ? existingStatus.getConditions() : Collections.emptyList()));
        rescheduleSeconds = 30;
        break;

      case WAKE:
        wakeCluster(resource, client);
        newStatus.setClusterPhase("Running");
        newStatus.setSuspendedSince(null);
        newStatus.setIdleSince(null);
        newStatus.getConditions().add(buildCondition("Suspended", "False", "Woken",
            "Cluster woken up",
            existingStatus != null ? existingStatus.getConditions() : Collections.emptyList()));
        rescheduleSeconds = anyAutoscalingEnabled(resource.getSpec())
            ? getMinScrapeInterval(resource.getSpec()) : 30;
        break;

      case IDLE_START:
        newStatus.setClusterPhase("Idle");
        newStatus.setIdleSince(Instant.now().toString());
        newStatus.setIdleForMinutes(0);
        newStatus.setSuspendedSince(null);
        break;

      case IDLE_WAITING:
        String idleSince = existingStatus != null ? existingStatus.getIdleSince() : null;
        newStatus.setClusterPhase("Idle");
        newStatus.setIdleSince(idleSince);
        newStatus.setIdleForMinutes(idleSince != null
            ? (int) Duration.between(Instant.parse(idleSince), Instant.now()).toMinutes() : 0);
        newStatus.setSuspendedSince(null);
        break;

      case RUNNING:
      default:
        newStatus.setClusterPhase("Running");
        newStatus.setIdleSince(null);
        newStatus.setIdleForMinutes(null);
        newStatus.setSuspendedSince(null);
        break;
    }

    // --- Autoscaling evaluation (only when enabled and not suspended) ---
    if (rescheduleSeconds == 0 && anyAutoscalingEnabled(resource.getSpec())) {
      HiveClusterAutoscaler scaler = getOrCreateAutoscaler(client);
      HiveClusterAutoscaler.AutoscalingEvaluation eval = scaler.evaluate(resource, client);
      for (Map.Entry<String, Integer> entry : eval.patches().entrySet()) {
        patchReplicas(client, resource, entry.getKey(), entry.getValue());
      }
      applyAutoscalingStatuses(newStatus, eval.statuses());
      rescheduleSeconds = getMinScrapeInterval(resource.getSpec());
    }

    // --- Single exit point for status update ---
    boolean statusNowChanged = !statusEqualsIgnoringTimestamps(existingStatus, newStatus);
    if (!statusNowChanged && rescheduleSeconds == 0) {
      return UpdateControl.noUpdate();
    }
    resource.setStatus(newStatus);
    if (rescheduleSeconds > 0) {
      return UpdateControl.<HiveCluster>patchStatus(resource)
          .rescheduleAfter(Duration.ofSeconds(rescheduleSeconds));
    }
    return UpdateControl.patchStatus(resource);
  }

  @Override
  public ErrorStatusUpdateControl<HiveCluster> updateErrorStatus(HiveCluster resource, Context<HiveCluster> context,
      Exception e) {
    LOG.error("Error reconciling HiveCluster: {}/{} - {}", resource.getMetadata().getNamespace(),
        resource.getMetadata().getName(), e.getMessage(), e);

    HiveClusterStatus status = resource.getStatus() != null ? resource.getStatus() : new HiveClusterStatus();

    List<Condition> existingConditions =
        status.getConditions() != null ? status.getConditions() : Collections.emptyList();

    status.setConditions(List.of(
        buildCondition("Ready", "False", "ReconciliationError",
            e.getMessage(), existingConditions)
    ));
    status.setObservedGeneration(resource.getMetadata().getGeneration());
    resource.setStatus(status);

    return ErrorStatusUpdateControl.patchStatus(resource);
  }

  private HiveClusterStatus buildStatus(HiveCluster resource,
      Context<HiveCluster> context, HiveClusterStatus existingStatus) {

    HiveClusterStatus status = new HiveClusterStatus();
    status.setObservedGeneration(resource.getMetadata().getGeneration());

    List<Condition> existingConditions = existingStatus != null && existingStatus.getConditions() != null
        ? existingStatus.getConditions() : Collections.emptyList();
    List<Condition> conditions = new ArrayList<>();

    // Schema Init status
    boolean schemaReady;
    if (resource.getSpec().metastore().isEnabled()) {
      schemaReady = context.getSecondaryResource(Job.class)
          .map(j -> j.getStatus() != null && j.getStatus().getSucceeded() != null && j.getStatus().getSucceeded() >= 1)
          .orElse(false);
    } else {
      schemaReady = true;
    }

    conditions.add(buildCondition("SchemaInitialized", schemaReady ? "True" : "False",
        schemaReady ? "JobCompleted" : "JobPending",
        schemaReady ? "Schema initialized successfully" : "Schema initialization pending",
        existingConditions));

    // Metastore status
    boolean metastoreReady;
    if (resource.getSpec().metastore().isEnabled()) {
      int msMin = resource.getSpec().metastore().autoscaling().isEnabled()
          ? Math.max(1, resource.getSpec().metastore().autoscaling().minReplicas())
          : resource.getSpec().metastore().replicas();
      ComponentStatus metastoreStatus =
          buildComponentStatus(context, Deployment.class, resource.getMetadata().getName() + "-metastore",
              resource.getSpec().metastore().replicas(), msMin);
      status.setMetastore(metastoreStatus);

      metastoreReady = metastoreStatus.getReadyReplicas() >= msMin && msMin > 0;

      conditions.add(buildCondition("MetastoreReady", metastoreReady ? "True" : "False",
          metastoreReady ? "DeploymentReady" : "DeploymentNotReady",
          metastoreReady ? "Metastore is ready" : "Metastore not yet ready", existingConditions));
    } else {
      metastoreReady = true;
      conditions.add(buildCondition("MetastoreReady", "True", "ExternalMetastore", "Using external Hive Metastore",
          existingConditions));
    }

    // HiveServer2 status
    int hs2Min = resource.getSpec().hiveServer2().autoscaling().isEnabled()
        ? Math.max(1, resource.getSpec().hiveServer2().autoscaling().minReplicas())
        : resource.getSpec().hiveServer2().replicas();
    ComponentStatus hs2Status = buildComponentStatus(context, Deployment.class,
        resource.getMetadata().getName() + "-hiveserver2",
        resource.getSpec().hiveServer2().replicas(), hs2Min);
    status.setHiveServer2(hs2Status);

    boolean hs2Ready = hs2Status.getReadyReplicas() >= hs2Min;
    conditions.add(buildCondition("HiveServer2Ready", hs2Ready ? "True" : "False",
        hs2Ready ? "DeploymentReady" : "DeploymentNotReady",
        hs2Ready ? "HiveServer2 is ready" : "HiveServer2 not yet ready",
        existingConditions));

    // LLAP status (optional)
    if (resource.getSpec().llap().isEnabled()) {
      int llapMin = resource.getSpec().llap().autoscaling().isEnabled()
          ? resource.getSpec().llap().autoscaling().minReplicas()
          : resource.getSpec().llap().replicas();
      status.setLlap(buildComponentStatus(context, StatefulSet.class,
          resource.getMetadata().getName() + "-llap",
          resource.getSpec().llap().replicas(), llapMin));
    }

    // TezAM status (optional)
    if (resource.getSpec().tezAm().isEnabled()) {
      int tezAmMin = resource.getSpec().tezAm().autoscaling().isEnabled()
          ? resource.getSpec().tezAm().autoscaling().minReplicas()
          : resource.getSpec().tezAm().replicas();
      status.setTezAm(buildComponentStatus(context, StatefulSet.class,
          resource.getMetadata().getName() + "-tezam",
          resource.getSpec().tezAm().replicas(), tezAmMin));
    }

    // Overall Ready condition
    boolean allReady = schemaReady && metastoreReady && hs2Ready;
    conditions.add(buildCondition("Ready", allReady ? "True" : "False",
        allReady ? "AllComponentsReady" : "ComponentsNotReady",
        allReady ? "All Hive components are ready" : "One or more components are not ready",
        existingConditions));

    status.setConditions(conditions);
    return status;
  }

  /**
   * Unified helper to build status for Deployments, StatefulSets, or any HasMetadata type
   * that tracks replicas. Filters by Kubernetes resource name from the informer cache.
   */
  private <T extends HasMetadata> ComponentStatus buildComponentStatus(
      Context<HiveCluster> context, Class<T> resourceClass, String expectedResourceName,
      int maxReplicas, int minReplicas) {

    ComponentStatus cs = new ComponentStatus();
    cs.setMaxReplicas(maxReplicas);
    cs.setMinReplicas(minReplicas);

    // Read actual spec.replicas and readyReplicas from the live workload
    var workload = context.getSecondaryResources(resourceClass).stream()
        .filter(r -> r.getMetadata().getName().equals(expectedResourceName))
        .findFirst();

    int currentReplicas = workload.map(r -> {
      if (r instanceof Deployment d) {
        return d.getSpec() != null && d.getSpec().getReplicas() != null
            ? d.getSpec().getReplicas() : 0;
      } else if (r instanceof StatefulSet s) {
        return s.getSpec() != null && s.getSpec().getReplicas() != null
            ? s.getSpec().getReplicas() : 0;
      }
      return 0;
    }).orElse(0);

    int ready = workload.map(r -> {
      if (r instanceof Deployment d) {
        return d.getStatus() != null && d.getStatus().getReadyReplicas() != null
            ? d.getStatus().getReadyReplicas() : 0;
      } else if (r instanceof StatefulSet s) {
        return s.getStatus() != null && s.getStatus().getReadyReplicas() != null
            ? s.getStatus().getReadyReplicas() : 0;
      }
      return 0;
    }).orElse(0);

    cs.setCurrentReplicas(currentReplicas);
    cs.setReadyReplicas(ready);

    if (currentReplicas == 0 && ready == 0) {
      cs.setPhase("Idle");
    } else if (ready >= currentReplicas && currentReplicas > 0) {
      cs.setPhase("Running");
    } else if (currentReplicas == 0 && ready > 0) {
      cs.setPhase("ScalingDown");
    } else {
      cs.setPhase("Pending");
    }
    return cs;
  }

  private Condition buildCondition(String type, String conditionStatus,
      String reason, String message, List<Condition> existingConditions) {

    Condition condition = new Condition();
    condition.setType(type);
    condition.setStatus(conditionStatus);
    condition.setReason(reason);
    condition.setMessage(message);

    // Preserve lastTransitionTime from ANY existing condition of this type
    // (regardless of status) to avoid generating new timestamps on every
    // reconcile which would cause an infinite status-patch loop.
    String preservedTime = existingConditions.stream()
        .filter(c -> type.equals(c.getType()))
        .map(Condition::getLastTransitionTime)
        .findFirst()
        .orElse(null);

    if (preservedTime != null) {
      // Only update the timestamp if the status actually changed
      String oldStatus = existingConditions.stream()
          .filter(c -> type.equals(c.getType()))
          .map(Condition::getStatus)
          .findFirst()
          .orElse(null);
      if (conditionStatus.equals(oldStatus)) {
        condition.setLastTransitionTime(preservedTime);
      } else {
        condition.setLastTransitionTime(Instant.now().toString());
      }
    } else {
      condition.setLastTransitionTime(Instant.now().toString());
    }
    return condition;
  }

  /**
   * Compares two HiveClusterStatus objects ignoring condition timestamps.
   * This prevents infinite reconciliation loops caused by informer cache lag:
   * after a status patch, the informer may still have the old status, causing
   * the next reconcile to see a "different" status (new timestamp vs old) and
   * patch again, perpetuating the loop.
   */
  private boolean statusEqualsIgnoringTimestamps(HiveClusterStatus a, HiveClusterStatus b) {
    if (a == b) {
      return true;
    }
    if (a == null || b == null) {
      return false;
    }
    if (!Objects.equals(a.getObservedGeneration(), b.getObservedGeneration())) {
      return false;
    }
    if (!Objects.equals(a.getMetastore(), b.getMetastore())) {
      return false;
    }
    if (!Objects.equals(a.getHiveServer2(), b.getHiveServer2())) {
      return false;
    }
    if (!Objects.equals(a.getLlap(), b.getLlap())) {
      return false;
    }
    if (!Objects.equals(a.getTezAm(), b.getTezAm())) {
      return false;
    }
    // Compare conditions by type+status+reason+message, ignoring lastTransitionTime
    return conditionsEqualIgnoringTime(a.getConditions(), b.getConditions());
  }

  private boolean conditionsEqualIgnoringTime(List<Condition> a, List<Condition> b) {
    if (a == b) {
      return true;
    }
    if (a == null || b == null) {
      return a == null && b == null;
    }
    if (a.size() != b.size()) {
      return false;
    }
    for (int i = 0; i < a.size(); i++) {
      Condition ca = a.get(i);
      Condition cb = b.get(i);
      if (!Objects.equals(ca.getType(), cb.getType())
          || !Objects.equals(ca.getStatus(), cb.getStatus())
          || !Objects.equals(ca.getReason(), cb.getReason())
          || !Objects.equals(ca.getMessage(), cb.getMessage())) {
        return false;
      }
    }
    return true;
  }

  private void applyAutoscalingStatuses(HiveClusterStatus status,
      Map<String, AutoscalingStatus> statuses) {
    if (statuses.containsKey(ConfigUtils.COMPONENT_HIVESERVER2) && status.getHiveServer2() != null) {
      status.getHiveServer2().setAutoscaling(statuses.get(ConfigUtils.COMPONENT_HIVESERVER2));
    }
    if (statuses.containsKey(ConfigUtils.COMPONENT_METASTORE) && status.getMetastore() != null) {
      status.getMetastore().setAutoscaling(statuses.get(ConfigUtils.COMPONENT_METASTORE));
    }
    if (statuses.containsKey(ConfigUtils.COMPONENT_LLAP) && status.getLlap() != null) {
      status.getLlap().setAutoscaling(statuses.get(ConfigUtils.COMPONENT_LLAP));
    }
    if (statuses.containsKey(ConfigUtils.COMPONENT_TEZAM) && status.getTezAm() != null) {
      status.getTezAm().setAutoscaling(statuses.get(ConfigUtils.COMPONENT_TEZAM));
    }
  }

  // --- Autoscaling helpers ---

  private HiveClusterAutoscaler getOrCreateAutoscaler(KubernetesClient client) {
    if (autoscaler == null) {
      autoscaler = new HiveClusterAutoscaler(new MetricsScraper(client));
    }
    return autoscaler;
  }

  private static boolean anyAutoscalingEnabled(HiveClusterSpec spec) {
    if (spec.hiveServer2().autoscaling().isEnabled()) {
      return true;
    }
    if (spec.metastore().isEnabled() && spec.metastore().autoscaling().isEnabled()) {
      return true;
    }
    if (spec.llap().isEnabled() && spec.llap().autoscaling().isEnabled()) {
      return true;
    }
    if (spec.tezAm().isEnabled() && spec.tezAm().autoscaling().isEnabled()) {
      return true;
    }
    return false;
  }

  private static int getMinScrapeInterval(HiveClusterSpec spec) {
    int min = Integer.MAX_VALUE;
    if (spec.hiveServer2().autoscaling().isEnabled()) {
      min = Math.min(min, spec.hiveServer2().autoscaling().metricsScrapeIntervalSeconds());
    }
    if (spec.metastore().isEnabled() && spec.metastore().autoscaling().isEnabled()) {
      min = Math.min(min, spec.metastore().autoscaling().metricsScrapeIntervalSeconds());
    }
    if (spec.llap().isEnabled() && spec.llap().autoscaling().isEnabled()) {
      min = Math.min(min, spec.llap().autoscaling().metricsScrapeIntervalSeconds());
    }
    if (spec.tezAm().isEnabled() && spec.tezAm().autoscaling().isEnabled()) {
      min = Math.min(min, spec.tezAm().autoscaling().metricsScrapeIntervalSeconds());
    }
    return min == Integer.MAX_VALUE ? 10 : min;
  }

  private void patchReplicas(KubernetesClient client, HiveCluster resource,
      String component, int replicas) {
    String namespace = resource.getMetadata().getNamespace();
    String workloadName = resource.getMetadata().getName() + "-" + component;
    try {
      if (ConfigUtils.COMPONENT_LLAP.equals(component) || ConfigUtils.COMPONENT_TEZAM.equals(component)) {
        client.apps().statefulSets().inNamespace(namespace).withName(workloadName).scale(replicas);
      } else {
        client.apps().deployments().inNamespace(namespace).withName(workloadName).scale(replicas);
      }
      LOG.info("Scaled {}/{} to {} replicas", namespace, workloadName, replicas);
    } catch (Exception e) {
      LOG.debug("Could not scale {}/{}: {}", namespace, workloadName, e.getMessage());
    }
  }

  private void patchSuspendSpec(KubernetesClient client, HiveCluster resource, boolean suspend) {
    String ns = resource.getMetadata().getNamespace();
    String name = resource.getMetadata().getName();
    client.resources(HiveCluster.class).inNamespace(ns).withName(name)
        .edit(hc -> {
          // Records are immutable so we build a new spec with the updated suspend value
          HiveClusterSpec oldSpec = hc.getSpec();
          HiveClusterSpec newSpec = new HiveClusterSpec(
              oldSpec.image(), oldSpec.imagePullPolicy(), oldSpec.metastore(),
              oldSpec.hiveServer2(), oldSpec.llap(), oldSpec.tezAm(), oldSpec.zookeeper(),
              oldSpec.hadoop(), oldSpec.envVars(), oldSpec.externalJars(),
              oldSpec.volumes(), oldSpec.volumeMounts(), oldSpec.autoSuspend(), suspend);
          hc.setSpec(newSpec);
          return hc;
        });
    LOG.info("Patched spec.suspend={} on {}/{}", suspend, ns, name);
  }

  // --- Auto-Suspend / Wake ---

  enum SuspendAction { RUNNING, IDLE_START, IDLE_WAITING, SUSPEND_NOW, STAY_SUSPENDED, WAKE }

  private SuspendAction evaluateSuspendState(HiveCluster resource,
      HiveClusterStatus existingStatus, KubernetesClient client) {

    // 1. Manual suspend: spec.suspend = true → suspend immediately
    if (resource.getSpec().suspend()) {
      if (existingStatus != null && "Suspended".equals(existingStatus.getClusterPhase())) {
        return SuspendAction.STAY_SUSPENDED;
      }
      return SuspendAction.SUSPEND_NOW;
    }

    // 2. Currently suspended and spec.suspend = false → wake
    if (existingStatus != null && "Suspended".equals(existingStatus.getClusterPhase())) {
      return SuspendAction.WAKE;
    }

    // 3. Auto-suspend evaluation (only if enabled and all autoscaling is on)
    AutoSuspendSpec autoSuspend = resource.getSpec().autoSuspend();
    if (!autoSuspend.isEnabled()) {
      LOG.debug("Auto-suspend disabled");
      return SuspendAction.RUNNING;
    }
    if (!allAutoscalingEnabled(resource.getSpec())) {
      LOG.debug("Auto-suspend skipped: not all components have autoscaling enabled");
      return SuspendAction.RUNNING;
    }

    // 4. Check idle conditions
    boolean allIdle = isClusterIdle(resource, existingStatus, client);
    if (!allIdle) {
      return SuspendAction.RUNNING;
    }

    // 5. Check idle duration
    String idleSince = existingStatus != null ? existingStatus.getIdleSince() : null;
    if (idleSince == null) {
      return SuspendAction.IDLE_START;
    }

    Instant idleStart = Instant.parse(idleSince);
    if (Duration.between(idleStart, Instant.now()).toMinutes() >= autoSuspend.idleTimeoutMinutes()) {
      return SuspendAction.SUSPEND_NOW;
    }

    return SuspendAction.IDLE_WAITING;
  }


  private boolean isClusterIdle(HiveCluster resource, HiveClusterStatus existingStatus,
      KubernetesClient client) {
    HiveClusterSpec spec = resource.getSpec();
    String ns = resource.getMetadata().getNamespace();
    String name = resource.getMetadata().getName();

    // All components must be at minReplicas
    if (spec.llap().isEnabled()
        && !isAtMinReplicas(client, ns, name + "-" + ConfigUtils.COMPONENT_LLAP, true,
            spec.llap().autoscaling().minReplicas())) {
      return false;
    }
    if (spec.tezAm().isEnabled()
        && !isAtMinReplicas(client, ns, name + "-" + ConfigUtils.COMPONENT_TEZAM, true,
            spec.tezAm().autoscaling().minReplicas())) {
      return false;
    }
    if (!isAtMinReplicas(client, ns, name + "-" + ConfigUtils.COMPONENT_HIVESERVER2, false,
        Math.max(1, spec.hiveServer2().autoscaling().minReplicas()))) {
      return false;
    }

    // HS2 must have 0 open sessions.
    // If metrics scrape fails (empty list), assume NOT idle to prevent accidental suspend.
    HiveClusterAutoscaler scaler = getOrCreateAutoscaler(client);
    List<PodMetrics> hs2Metrics = scaler.scrapeHs2Metrics(resource);
    if (hs2Metrics.isEmpty()) {
      LOG.debug("Idle check: HS2 metrics unavailable, assuming not idle");
      return false;
    }
    int totalSessions = hs2Metrics.stream()
        .mapToInt(pm -> pm.metrics().getOrDefault("hs2_open_sessions", 0.0).intValue())
        .sum();
    if (totalSessions > 0) {
      LOG.debug("Idle check failed: HS2 has {} open sessions", totalSessions);
      return false;
    }

    // HMS must be at minReplicas (only checked if includeMetastore=true)
    if (spec.metastore().isEnabled() && spec.autoSuspend().includeMetastore()
        && !isAtMinReplicas(client, ns, name + "-" + ConfigUtils.COMPONENT_METASTORE, false,
            Math.max(1, spec.metastore().autoscaling().minReplicas()))) {
      return false;
    }

    return true;
  }

  /** Returns true if the workload is absent or its replicas <= minReplicas. */
  private boolean isAtMinReplicas(KubernetesClient client, String ns,
      String workloadName, boolean statefulSet, int minReplicas) {
    try {
      Integer currentReplicas = null;
      if (statefulSet) {
        var ss = client.apps().statefulSets().inNamespace(ns).withName(workloadName).get();
        if (ss != null && ss.getSpec() != null) {
          currentReplicas = ss.getSpec().getReplicas();
        }
      } else {
        var deploy = client.apps().deployments().inNamespace(ns).withName(workloadName).get();
        if (deploy != null && deploy.getSpec() != null) {
          currentReplicas = deploy.getSpec().getReplicas();
        }
      }
      if (currentReplicas != null && currentReplicas > minReplicas) {
        LOG.debug("Idle check failed: {} replicas {} > min {}", workloadName, currentReplicas, minReplicas);
        return false;
      }
      return true;
    } catch (Exception e) {
      LOG.debug("Idle check: could not read {}: {}", workloadName, e.getMessage());
      return true;
    }
  }

  private void suspendCluster(HiveCluster resource, KubernetesClient client) {
    String ns = resource.getMetadata().getNamespace();
    String name = resource.getMetadata().getName();
    HiveClusterSpec spec = resource.getSpec();

    // Set MANAGED_REPLICAS to 0 so autoscaler doesn't fight the suspend.
    // Actual scaling to 0 is handled by the DependentResources which check
    // spec.suspend() in resolveReplicaCount().
    HiveClusterAutoscaler.setManagedReplicas(ns, name, ConfigUtils.COMPONENT_HIVESERVER2, 0);
    if (spec.metastore().isEnabled() && spec.autoSuspend().includeMetastore()) {
      HiveClusterAutoscaler.setManagedReplicas(ns, name, ConfigUtils.COMPONENT_METASTORE, 0);
    }
    if (spec.llap().isEnabled()) {
      HiveClusterAutoscaler.setManagedReplicas(ns, name, ConfigUtils.COMPONENT_LLAP, 0);
    }
    if (spec.tezAm().isEnabled()) {
      HiveClusterAutoscaler.setManagedReplicas(ns, name, ConfigUtils.COMPONENT_TEZAM, 0);
    }

    LOG.info("Cluster {}/{} suspended", ns, name);
  }

  private void wakeCluster(HiveCluster resource, KubernetesClient client) {
    HiveClusterSpec spec = resource.getSpec();
    String ns = resource.getMetadata().getNamespace();
    String name = resource.getMetadata().getName();

    // Set MANAGED_REPLICAS to wake values. The JOSDK workflow will recreate
    // the dependent resources (Deployments/StatefulSets) on the next reconcile
    // and use these values for spec.replicas. We don't call patchReplicas()
    // because the workloads may have been garbage-collected while suspended.
    int hs2Min = Math.max(1, spec.hiveServer2().autoscaling().minReplicas());
    HiveClusterAutoscaler.setManagedReplicas(ns, name, ConfigUtils.COMPONENT_HIVESERVER2, hs2Min);

    if (spec.metastore().isEnabled() && spec.autoSuspend().includeMetastore()) {
      int hmsMin = Math.max(1, spec.metastore().autoscaling().minReplicas());
      HiveClusterAutoscaler.setManagedReplicas(ns, name, ConfigUtils.COMPONENT_METASTORE, hmsMin);
    }

    if (spec.llap().isEnabled()) {
      int llapWake = spec.llap().autoscaling().minReplicas();
      HiveClusterAutoscaler.setManagedReplicas(ns, name, ConfigUtils.COMPONENT_LLAP, llapWake);
    }

    if (spec.tezAm().isEnabled()) {
      int tezWake = spec.tezAm().autoscaling().minReplicas();
      HiveClusterAutoscaler.setManagedReplicas(ns, name, ConfigUtils.COMPONENT_TEZAM, tezWake);
    }

    LOG.info("Cluster {}/{} woken up — restored to minReplicas", ns, name);
  }

  private static boolean allAutoscalingEnabled(HiveClusterSpec spec) {
    if (!spec.hiveServer2().autoscaling().isEnabled()) {
      return false;
    }
    // Skip HMS check if includeMetastore=false (HMS doesn't participate in suspend)
    if (spec.metastore().isEnabled() && spec.autoSuspend().includeMetastore()
        && !spec.metastore().autoscaling().isEnabled()) {
      return false;
    }
    if (spec.llap().isEnabled() && !spec.llap().autoscaling().isEnabled()) {
      return false;
    }
    if (spec.tezAm().isEnabled() && !spec.tezAm().autoscaling().isEnabled()) {
      return false;
    }
    return true;
  }
}
