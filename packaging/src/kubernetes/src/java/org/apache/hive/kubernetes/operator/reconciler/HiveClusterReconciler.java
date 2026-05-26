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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import io.fabric8.kubernetes.api.model.Condition;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.HiveClusterStatus;
import org.apache.hive.kubernetes.operator.model.status.ComponentStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main reconciler for the HiveCluster custom resource.
 * Orchestrates all dependent resources with proper dependency ordering.
 */
@ControllerConfiguration
public class HiveClusterReconciler implements Reconciler<HiveCluster> {

  private static final Logger LOG = LoggerFactory.getLogger(HiveClusterReconciler.class);

  @Override
  public UpdateControl<HiveCluster> reconcile(HiveCluster resource, Context<HiveCluster> context) {
    LOG.debug("Reconciling HiveCluster: {}/{}  generation={}",
        resource.getMetadata().getNamespace(),
        resource.getMetadata().getName(),
        resource.getMetadata().getGeneration());

    HiveClusterStatus existingStatus = resource.getStatus();
    HiveClusterStatus newStatus = buildStatus(resource, context, existingStatus);

    if (statusEqualsIgnoringTimestamps(existingStatus, newStatus)) {
      return UpdateControl.noUpdate();
    }

    resource.setStatus(newStatus);
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
      // When autoscaling, desired = minReplicas (KEDA manages beyond that)
      int metastoreDesired = resource.getSpec().metastore().autoscaling().isEnabled()
          ? Math.max(1, resource.getSpec().metastore().autoscaling().minReplicas())
          : resource.getSpec().metastore().replicas();
      ComponentStatus metastoreStatus =
          buildComponentStatus(context, Deployment.class, resource.getMetadata().getName() + "-metastore",
              metastoreDesired,
              d -> d.getStatus() != null && d.getStatus().getReadyReplicas() != null ?
                  d.getStatus().getReadyReplicas() :
                  0);
      status.setMetastore(metastoreStatus);

      metastoreReady = metastoreStatus.getReadyReplicas() >= metastoreStatus.getDesiredReplicas()
          && metastoreStatus.getDesiredReplicas() > 0;

      conditions.add(buildCondition("MetastoreReady", metastoreReady ? "True" : "False",
          metastoreReady ? "DeploymentReady" : "DeploymentNotReady",
          metastoreReady ? "Metastore is ready" : "Metastore not yet ready", existingConditions));
    } else {
      metastoreReady = true;
      conditions.add(buildCondition("MetastoreReady", "True", "ExternalMetastore", "Using external Hive Metastore",
          existingConditions));
    }

    // HiveServer2 status — when scale-to-zero, 0/0 is a valid "ready" state (idle)
    int hs2Desired = resource.getSpec().hiveServer2().autoscaling().isEnabled()
        ? resource.getSpec().hiveServer2().autoscaling().minReplicas()
        : resource.getSpec().hiveServer2().replicas();
    ComponentStatus hs2Status = buildComponentStatus(context, Deployment.class,
        resource.getMetadata().getName() + "-hiveserver2",
        hs2Desired,
        d -> d.getStatus() != null && d.getStatus().getReadyReplicas() != null ? d.getStatus().getReadyReplicas() : 0);
    status.setHiveServer2(hs2Status);

    boolean hs2Ready = hs2Status.getReadyReplicas() >= hs2Status.getDesiredReplicas();
    conditions.add(buildCondition("HiveServer2Ready", hs2Ready ? "True" : "False",
        hs2Ready ? "DeploymentReady" : "DeploymentNotReady",
        hs2Ready ? "HiveServer2 is ready" : "HiveServer2 not yet ready",
        existingConditions));

    // LLAP status (optional)
    if (resource.getSpec().llap().isEnabled()) {
      int llapDesired = resource.getSpec().llap().autoscaling().isEnabled()
          ? resource.getSpec().llap().autoscaling().minReplicas()
          : resource.getSpec().llap().replicas();
      status.setLlap(buildComponentStatus(context, StatefulSet.class,
          resource.getMetadata().getName() + "-llap",
          llapDesired,
          s -> s.getStatus() != null && s.getStatus().getReadyReplicas() != null ?
              s.getStatus().getReadyReplicas() : 0));
    }

    // TezAM status (optional)
    if (resource.getSpec().tezAm().isEnabled()) {
      int tezAmDesired = resource.getSpec().tezAm().autoscaling().isEnabled()
          ? resource.getSpec().tezAm().autoscaling().minReplicas()
          : resource.getSpec().tezAm().replicas();
      status.setTezAm(buildComponentStatus(context, StatefulSet.class, resource.getMetadata().getName() + "-tezam",
          tezAmDesired,
          s -> s.getStatus() != null &&
              s.getStatus().getReadyReplicas() != null ? s.getStatus().getReadyReplicas() : 0));
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
      int desiredReplicas, Function<T, Integer> readyExtractor) {

    ComponentStatus cs = new ComponentStatus();
    cs.setDesiredReplicas(desiredReplicas);

    int ready = context.getSecondaryResources(resourceClass).stream()
        .filter(r -> r.getMetadata().getName().equals(expectedResourceName))
        .findFirst()
        .map(readyExtractor)
        .orElse(0);

    cs.setReadyReplicas(ready);
    cs.setPhase(ready >= desiredReplicas && desiredReplicas > 0 ? "Running" : "Pending");
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
}
