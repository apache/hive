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
import java.util.Optional;
import java.util.function.Function;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusHandler;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import org.apache.hive.kubernetes.operator.dependent.HadoopConfigMapDependent;
import org.apache.hive.kubernetes.operator.dependent.HiveServer2ConfigMapDependent;
import org.apache.hive.kubernetes.operator.dependent.HiveServer2DeploymentDependent;
import org.apache.hive.kubernetes.operator.dependent.HiveServer2ServiceDependent;
import org.apache.hive.kubernetes.operator.dependent.LlapConfigMapDependent;
import org.apache.hive.kubernetes.operator.dependent.LlapServiceDependent;
import org.apache.hive.kubernetes.operator.dependent.LlapStatefulSetDependent;
import org.apache.hive.kubernetes.operator.dependent.MetastoreConfigMapDependent;
import org.apache.hive.kubernetes.operator.dependent.MetastoreDeploymentDependent;
import org.apache.hive.kubernetes.operator.dependent.MetastoreServiceDependent;
import org.apache.hive.kubernetes.operator.dependent.SchemaInitJobDependent;
import org.apache.hive.kubernetes.operator.dependent.ScratchPvcDependent;
import org.apache.hive.kubernetes.operator.dependent.TezAmServiceDependent;
import org.apache.hive.kubernetes.operator.dependent.TezAmStatefulSetDependent;
import org.apache.hive.kubernetes.operator.dependent.condition.LlapEnabledCondition;
import org.apache.hive.kubernetes.operator.dependent.condition.MetastoreReadyCondition;
import org.apache.hive.kubernetes.operator.dependent.condition.SchemaJobCompletedCondition;
import org.apache.hive.kubernetes.operator.dependent.condition.TezAmEnabledCondition;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.HiveClusterStatus;
import org.apache.hive.kubernetes.operator.model.status.ComponentStatus;
import org.apache.hive.kubernetes.operator.model.status.HiveClusterCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main reconciler for the HiveCluster custom resource.
 * Orchestrates all dependent resources with proper dependency ordering.
 */
@ControllerConfiguration(
    dependents = {
        // --- ConfigMap dependents ---
        @Dependent(
            name = "hadoop-configmap",
            type = HadoopConfigMapDependent.class
        ),
        @Dependent(
            name = "metastore-configmap",
            type = MetastoreConfigMapDependent.class
        ),
        @Dependent(
            name = "hiveserver2-configmap",
            type = HiveServer2ConfigMapDependent.class
        ),
        // --- Job dependents ---
        @Dependent(
            name = "schema-init-job",
            type = SchemaInitJobDependent.class,
            dependsOn = {"metastore-configmap", "hadoop-configmap"},
            readyPostcondition = SchemaJobCompletedCondition.class
        ),
        // --- Deployment dependents ---
        @Dependent(
            name = "metastore-deployment",
            type = MetastoreDeploymentDependent.class,
            dependsOn = {"schema-init-job"},
            readyPostcondition = MetastoreReadyCondition.class
        ),
        // --- Service dependents ---
        @Dependent(
            name = "metastore-service",
            type = MetastoreServiceDependent.class,
            dependsOn = {"metastore-configmap"}
        ),
        @Dependent(
            name = "hiveserver2-deployment",
            type = HiveServer2DeploymentDependent.class,
            dependsOn = {"metastore-deployment", "hiveserver2-configmap",
                "hadoop-configmap"}
        ),
        @Dependent(
            name = "hiveserver2-service",
            type = HiveServer2ServiceDependent.class,
            dependsOn = {"hiveserver2-configmap"}
        ),
        // --- LLAP (conditional) ---
        @Dependent(
            name = "llap-configmap",
            type = LlapConfigMapDependent.class,
            activationCondition = LlapEnabledCondition.class
        ),
        @Dependent(
            name = "llap-statefulset",
            type = LlapStatefulSetDependent.class,
            dependsOn = {"llap-configmap", "hadoop-configmap"},
            activationCondition = LlapEnabledCondition.class
        ),
        @Dependent(
            name = "llap-service",
            type = LlapServiceDependent.class,
            activationCondition = LlapEnabledCondition.class
        ),
        // --- TezAM (conditional) ---
        @Dependent(
            name = "scratch-pvc",
            type = ScratchPvcDependent.class,
            activationCondition = TezAmEnabledCondition.class
        ),
        @Dependent(
            name = "tezam-service",
            type = TezAmServiceDependent.class,
            activationCondition = TezAmEnabledCondition.class
        ),
        @Dependent(
            name = "tezam-statefulset",
            type = TezAmStatefulSetDependent.class,
            dependsOn = {"hiveserver2-configmap", "hadoop-configmap",
                "tezam-service", "scratch-pvc"},
            activationCondition = TezAmEnabledCondition.class
        )
    }
)
public class HiveClusterReconciler
    implements Reconciler<HiveCluster>, ErrorStatusHandler<HiveCluster> {

  private static final Logger LOG =
      LoggerFactory.getLogger(HiveClusterReconciler.class);

  @Override
  public UpdateControl<HiveCluster> reconcile(HiveCluster resource,
      Context<HiveCluster> context) {
    LOG.info("Reconciling HiveCluster: {}/{}",
        resource.getMetadata().getNamespace(),
        resource.getMetadata().getName());

    HiveClusterStatus existingStatus = resource.getStatus();
    HiveClusterStatus newStatus = buildStatus(resource, context, existingStatus);

    // Relies on HiveClusterStatus.equals() being properly implemented in the POJO
    if (Objects.equals(existingStatus, newStatus)) {
      return UpdateControl.noUpdate();
    }

    resource.setStatus(newStatus);
    return UpdateControl.patchStatus(resource);
  }

  @Override
  public ErrorStatusUpdateControl<HiveCluster> updateErrorStatus(
      HiveCluster resource, Context<HiveCluster> context, Exception e) {
    LOG.error("Error reconciling HiveCluster: {}/{}",
        resource.getMetadata().getNamespace(),
        resource.getMetadata().getName(), e);

    HiveClusterStatus status = resource.getStatus() != null
        ? resource.getStatus() : new HiveClusterStatus();

    List<HiveClusterCondition> existingConditions = status.getConditions() != null
        ? status.getConditions() : Collections.emptyList();

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

    List<HiveClusterCondition> existingConditions = existingStatus != null && existingStatus.getConditions() != null
        ? existingStatus.getConditions() : Collections.emptyList();
    List<HiveClusterCondition> conditions = new ArrayList<>();

    // Schema Init status
    boolean schemaReady = context.getSecondaryResource(Job.class, "schema-init-job")
        .map(j -> j.getStatus() != null && j.getStatus().getSucceeded() != null && j.getStatus().getSucceeded() >= 1)
        .orElse(false);

    conditions.add(buildCondition("SchemaInitialized", schemaReady ? "True" : "False",
        schemaReady ? "JobCompleted" : "JobPending",
        schemaReady ? "Schema initialized successfully" : "Schema initialization pending",
        existingConditions));

    // Metastore status
    ComponentStatus metastoreStatus = buildComponentStatus(context, Deployment.class, "metastore-deployment",
        resource.getSpec().getMetastore().getReplicas(),
        d -> d.getStatus() != null && d.getStatus().getReadyReplicas() != null ? d.getStatus().getReadyReplicas() : 0);
    status.setMetastore(metastoreStatus);

    boolean metastoreReady = metastoreStatus.getReadyReplicas() >= metastoreStatus.getDesiredReplicas() && metastoreStatus.getDesiredReplicas() > 0;
    conditions.add(buildCondition("MetastoreReady", metastoreReady ? "True" : "False",
        metastoreReady ? "DeploymentReady" : "DeploymentNotReady",
        metastoreReady ? "Metastore is ready" : "Metastore not yet ready",
        existingConditions));

    // HiveServer2 status
    ComponentStatus hs2Status = buildComponentStatus(context, Deployment.class, "hiveserver2-deployment",
        resource.getSpec().getHiveServer2().getReplicas(),
        d -> d.getStatus() != null && d.getStatus().getReadyReplicas() != null ? d.getStatus().getReadyReplicas() : 0);
    status.setHiveServer2(hs2Status);

    boolean hs2Ready = hs2Status.getReadyReplicas() >= hs2Status.getDesiredReplicas() && hs2Status.getDesiredReplicas() > 0;
    conditions.add(buildCondition("HiveServer2Ready", hs2Ready ? "True" : "False",
        hs2Ready ? "DeploymentReady" : "DeploymentNotReady",
        hs2Ready ? "HiveServer2 is ready" : "HiveServer2 not yet ready",
        existingConditions));

    // LLAP status (optional)
    if (resource.getSpec().getLlap().isEnabled()) {
      status.setLlap(buildComponentStatus(context, StatefulSet.class, "llap-statefulset",
          resource.getSpec().getLlap().getReplicas(),
          s -> s.getStatus() != null && s.getStatus().getReadyReplicas() != null ? s.getStatus().getReadyReplicas() : 0));
    }

    // TezAM status (optional)
    if (resource.getSpec().getTezAm().isEnabled()) {
      status.setTezAm(buildComponentStatus(context, StatefulSet.class, "tezam-statefulset",
          resource.getSpec().getTezAm().getReplicas(),
          s -> s.getStatus() != null && s.getStatus().getReadyReplicas() != null ? s.getStatus().getReadyReplicas() : 0));
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
   * that tracks replicas.
   */
  private <T extends HasMetadata> ComponentStatus buildComponentStatus(
      Context<HiveCluster> context, Class<T> resourceClass, String dependentName,
      int desiredReplicas, Function<T, Integer> readyExtractor) {

    ComponentStatus cs = new ComponentStatus();
    cs.setDesiredReplicas(desiredReplicas);

    int ready = context.getSecondaryResource(resourceClass, dependentName)
        .map(readyExtractor)
        .orElse(0);

    cs.setReadyReplicas(ready);
    cs.setPhase(ready >= desiredReplicas && desiredReplicas > 0 ? "Running" : "Pending");
    return cs;
  }

  private HiveClusterCondition buildCondition(String type, String status,
      String reason, String message, List<HiveClusterCondition> existingConditions) {

    HiveClusterCondition condition = new HiveClusterCondition();
    condition.setType(type);
    condition.setStatus(status);
    condition.setReason(reason);
    condition.setMessage(message);

    // Preserve lastTransitionTime when the condition status has not changed
    String preservedTime = existingConditions.stream()
        .filter(c -> type.equals(c.getType()) && status.equals(c.getStatus()))
        .map(HiveClusterCondition::getLastTransitionTime)
        .findFirst()
        .orElse(null);

    condition.setLastTransitionTime(preservedTime != null ? preservedTime : Instant.now().toString());
    return condition;
  }
}