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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.javaoperatorsdk.operator.api.config.dependent.DependentResourceSpec;
import io.javaoperatorsdk.operator.api.config.workflow.WorkflowSpec;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;
import org.apache.hive.kubernetes.operator.dependent.HiveConfigMapDependent;
import org.apache.hive.kubernetes.operator.dependent.HiveServer2DeploymentDependent;
import org.apache.hive.kubernetes.operator.dependent.HivePdbDependent;
import org.apache.hive.kubernetes.operator.dependent.HiveServiceDependent;
import org.apache.hive.kubernetes.operator.dependent.LlapStatefulSetDependent;
import org.apache.hive.kubernetes.operator.dependent.MetastoreDeploymentDependent;
import org.apache.hive.kubernetes.operator.dependent.SchemaInitJobDependent;
import org.apache.hive.kubernetes.operator.dependent.ScratchPvcDependent;
import org.apache.hive.kubernetes.operator.dependent.TezAmStatefulSetDependent;
import org.apache.hive.kubernetes.operator.model.HiveCluster;

/**
 * Programmatic workflow specification for the Hive Kubernetes Operator.
 * Replaces the annotation-based {@code @Workflow} on the reconciler with
 * explicit {@link DependentResourceSpec} entries and inline lambda conditions.
 * This eliminates 12 single-method condition wrapper classes.
 */
public final class HiveWorkflowSpec implements WorkflowSpec {

  private static final Condition<?, HiveCluster> METASTORE_ENABLED =
      (dr, primary, ctx) -> primary.getSpec().metastore().isEnabled();

  private static final Condition<?, HiveCluster> LLAP_ENABLED =
      (dr, primary, ctx) -> primary.getSpec().llap().isEnabled();

  private static final Condition<?, HiveCluster> TEZAM_ENABLED =
      (dr, primary, ctx) -> primary.getSpec().tezAm().isEnabled();

  private static final Condition<?, HiveCluster> METASTORE_AUTOSCALING =
      (dr, primary, ctx) -> primary.getSpec().metastore().isEnabled()
          && primary.getSpec().metastore().autoscaling().isEnabled();

  private static final Condition<?, HiveCluster> LLAP_AUTOSCALING =
      (dr, primary, ctx) -> primary.getSpec().llap().isEnabled()
          && primary.getSpec().llap().autoscaling().isEnabled();

  private static final Condition<?, HiveCluster> TEZAM_AUTOSCALING =
      (dr, primary, ctx) -> primary.getSpec().tezAm().isEnabled()
          && primary.getSpec().tezAm().autoscaling().isEnabled();

  private static final Condition<?, HiveCluster> HS2_AUTOSCALING =
      (dr, primary, ctx) -> primary.getSpec().hiveServer2().autoscaling().isEnabled();


  // SPECS must be declared AFTER all conditions to avoid static init order issues.
  private static final List<DependentResourceSpec> SPECS = buildSpecs();

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static List<DependentResourceSpec> buildSpecs() {
    List<DependentResourceSpec> specs = new ArrayList<>();

    // --- ConfigMap dependents ---
    specs.add(new DependentResourceSpec(
        HiveConfigMapDependent.Hadoop.class, "hadoop-configmap",
        Set.of(), null, null, null, null, null));

    specs.add(new DependentResourceSpec(
        HiveConfigMapDependent.Metastore.class, "metastore-configmap",
        Set.of(), null, null, null, METASTORE_ENABLED, null));

    specs.add(new DependentResourceSpec(
        HiveConfigMapDependent.HiveServer2.class, "hiveserver2-configmap",
        Set.of(), null, null, null, null, null));

    // --- Job dependents ---
    specs.add(new DependentResourceSpec(
        SchemaInitJobDependent.class, "schema-init-job",
        Set.of("metastore-configmap", "hadoop-configmap"),
        schemaJobCompleted(), null, null, METASTORE_ENABLED, null));

    // --- Deployment dependents ---
    specs.add(new DependentResourceSpec(
        MetastoreDeploymentDependent.class, "metastore-deployment",
        Set.of("schema-init-job"),
        metastoreReady(), null, null, METASTORE_ENABLED, null));

    // --- Service dependents ---
    specs.add(new DependentResourceSpec(
        HiveServiceDependent.Metastore.class, "metastore-service",
        Set.of("metastore-configmap"),
        null, null, null, METASTORE_ENABLED, null));

    specs.add(new DependentResourceSpec(
        HiveServer2DeploymentDependent.class, "hiveserver2-deployment",
        Set.of("hiveserver2-configmap", "hadoop-configmap"),
        null, hs2Precondition(), null, null, null));

    specs.add(new DependentResourceSpec(
        HiveServiceDependent.HiveServer2.class, "hiveserver2-service",
        Set.of("hiveserver2-configmap"),
        null, null, null, null, null));

    // --- LLAP (conditional) ---
    specs.add(new DependentResourceSpec(
        HiveConfigMapDependent.Llap.class, "llap-configmap",
        Set.of(), null, null, null, LLAP_ENABLED, null));

    specs.add(new DependentResourceSpec(
        LlapStatefulSetDependent.class, "llap-statefulset",
        Set.of("llap-configmap", "hadoop-configmap"),
        null, null, null, LLAP_ENABLED, null));

    specs.add(new DependentResourceSpec(
        HiveServiceDependent.Llap.class, "llap-service",
        Set.of(), null, null, null, LLAP_ENABLED, null));

    // --- TezAM (conditional) ---
    specs.add(new DependentResourceSpec(
        ScratchPvcDependent.class, "scratch-pvc",
        Set.of(), null, null, null, TEZAM_ENABLED, null));

    specs.add(new DependentResourceSpec(
        HiveServiceDependent.TezAm.class, "tezam-service",
        Set.of(), null, null, null, TEZAM_ENABLED, null));

    specs.add(new DependentResourceSpec(
        TezAmStatefulSetDependent.class, "tezam-statefulset",
        Set.of("hiveserver2-configmap", "hadoop-configmap", "tezam-service", "scratch-pvc"),
        null, null, null, TEZAM_ENABLED, null));


    // --- Autoscaling: PodDisruptionBudgets (conditional) ---
    specs.add(new DependentResourceSpec(
        HivePdbDependent.HiveServer2.class, "hs2-pdb",
        Set.of("hiveserver2-deployment"),
        null, HS2_AUTOSCALING, null, null, null));

    specs.add(new DependentResourceSpec(
        HivePdbDependent.Metastore.class, "metastore-pdb",
        Set.of("metastore-deployment"),
        null, METASTORE_AUTOSCALING, null, null, null));

    specs.add(new DependentResourceSpec(
        HivePdbDependent.Llap.class, "llap-pdb",
        Set.of("llap-statefulset"),
        null, LLAP_AUTOSCALING, null, null, null));

    specs.add(new DependentResourceSpec(
        HivePdbDependent.TezAm.class, "tezam-pdb",
        Set.of("tezam-statefulset"),
        null, TEZAM_AUTOSCALING, null, null, null));

    return Collections.unmodifiableList(specs);
  }

  /**
   * Ready postcondition: schema initialization Job must complete successfully
   * before the Metastore Deployment is created.
   */
  private static Condition<?, HiveCluster> schemaJobCompleted() {
    return (dependentResource, primary, context) -> {
      if (!primary.getSpec().metastore().isEnabled()) {
        return true;
      }
      return dependentResource.getSecondaryResource(primary, context)
          .map(job -> {
            var j = (io.fabric8.kubernetes.api.model.batch.v1.Job) job;
            return j.getStatus() != null
                && j.getStatus().getSucceeded() != null
                && j.getStatus().getSucceeded() >= 1;
          })
          .orElse(false);
    };
  }

  /**
   * Ready postcondition: Metastore Deployment must have the desired number
   * of ready replicas before downstream dependents proceed.
   */
  private static Condition<?, HiveCluster> metastoreReady() {
    return (dependentResource, primary, context) -> {
      if (!primary.getSpec().metastore().isEnabled()) {
        return true;
      }
      int desiredReplicas;
      if (primary.getSpec().metastore().autoscaling().isEnabled()) {
        desiredReplicas = Math.max(1, primary.getSpec().metastore().autoscaling().minReplicas());
      } else {
        desiredReplicas = primary.getSpec().metastore().replicas();
      }
      return dependentResource.getSecondaryResource(primary, context)
          .map(resource -> {
            var deployment = (Deployment) resource;
            return deployment.getStatus() != null
                && deployment.getStatus().getReadyReplicas() != null
                && deployment.getStatus().getReadyReplicas() >= desiredReplicas;
          })
          .orElse(false);
    };
  }

  /**
   * Reconcile precondition for HiveServer2: if Metastore is managed,
   * wait for it to be ready before reconciling HS2.
   */
  private static Condition<?, HiveCluster> hs2Precondition() {
    return (dependentResource, primary, context) -> {
      if (!primary.getSpec().metastore().isEnabled()) {
        return true;
      }
      int desiredReplicas;
      if (primary.getSpec().metastore().autoscaling().isEnabled()) {
        desiredReplicas = Math.max(1, primary.getSpec().metastore().autoscaling().minReplicas());
      } else {
        desiredReplicas = primary.getSpec().metastore().replicas();
      }
      return context.getSecondaryResources(Deployment.class).stream()
          .filter(d -> d.getMetadata().getName().equals(
              primary.getMetadata().getName() + "-metastore"))
          .findFirst()
          .map(deployment -> deployment.getStatus() != null
              && deployment.getStatus().getReadyReplicas() != null
              && deployment.getStatus().getReadyReplicas() >= desiredReplicas)
          .orElse(false);
    };
  }

  @Override
  public List<DependentResourceSpec> getDependentResourceSpecs() {
    return SPECS;
  }

  @Override
  public boolean isExplicitInvocation() {
    return false;
  }

  @Override
  public boolean handleExceptionsInReconciler() {
    return true;
  }
}
