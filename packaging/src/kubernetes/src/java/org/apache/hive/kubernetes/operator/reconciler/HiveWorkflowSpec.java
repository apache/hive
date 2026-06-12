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

  // Dependent resource spec names (used as identifiers and dependency references)
  private static final String HADOOP_CONFIGMAP = "hadoop-configmap";
  private static final String METASTORE_CONFIGMAP = "metastore-configmap";
  private static final String HIVESERVER2_CONFIGMAP = "hiveserver2-configmap";
  private static final String LLAP_CONFIGMAP = "llap-configmap";
  private static final String SCHEMA_INIT_JOB = "schema-init-job";
  private static final String METASTORE_DEPLOYMENT = "metastore-deployment";
  private static final String METASTORE_SERVICE = "metastore-service";
  private static final String HIVESERVER2_DEPLOYMENT = "hiveserver2-deployment";
  private static final String HIVESERVER2_SERVICE = "hiveserver2-service";
  private static final String LLAP_STATEFULSET = "llap-statefulset";
  private static final String LLAP_SERVICE = "llap-service";
  private static final String TEZAM_SERVICE = "tezam-service";
  private static final String TEZAM_STATEFULSET = "tezam-statefulset";
  private static final String SCRATCH_PVC = "scratch-pvc";
  private static final String HS2_PDB = "hs2-pdb";
  private static final String METASTORE_PDB = "metastore-pdb";
  private static final String LLAP_PDB = "llap-pdb";
  private static final String TEZAM_PDB = "tezam-pdb";

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
        HiveConfigMapDependent.Hadoop.class, HADOOP_CONFIGMAP,
        Set.of(), null, null, null, null, null));

    specs.add(new DependentResourceSpec(
        HiveConfigMapDependent.Metastore.class, METASTORE_CONFIGMAP,
        Set.of(), null, null, null, METASTORE_ENABLED, null));

    specs.add(new DependentResourceSpec(
        HiveConfigMapDependent.HiveServer2.class, HIVESERVER2_CONFIGMAP,
        Set.of(), null, null, null, null, null));

    // --- Job dependents ---
    specs.add(new DependentResourceSpec(
        SchemaInitJobDependent.class, SCHEMA_INIT_JOB,
        Set.of(METASTORE_CONFIGMAP, HADOOP_CONFIGMAP),
        schemaJobCompleted(), null, null, METASTORE_ENABLED, null));

    // --- Deployment dependents ---
    specs.add(new DependentResourceSpec(
        MetastoreDeploymentDependent.class, METASTORE_DEPLOYMENT,
        Set.of(SCHEMA_INIT_JOB),
        metastoreReady(), null, null, METASTORE_ENABLED, null));

    // --- Service dependents ---
    specs.add(new DependentResourceSpec(
        HiveServiceDependent.Metastore.class, METASTORE_SERVICE,
        Set.of(METASTORE_CONFIGMAP),
        null, null, null, METASTORE_ENABLED, null));

    specs.add(new DependentResourceSpec(
        HiveServer2DeploymentDependent.class, HIVESERVER2_DEPLOYMENT,
        Set.of(HIVESERVER2_CONFIGMAP, HADOOP_CONFIGMAP),
        null, hs2Precondition(), null, null, null));

    specs.add(new DependentResourceSpec(
        HiveServiceDependent.HiveServer2.class, HIVESERVER2_SERVICE,
        Set.of(HIVESERVER2_CONFIGMAP),
        null, null, null, null, null));

    // --- LLAP (conditional) ---
    specs.add(new DependentResourceSpec(
        HiveConfigMapDependent.Llap.class, LLAP_CONFIGMAP,
        Set.of(), null, null, null, LLAP_ENABLED, null));

    specs.add(new DependentResourceSpec(
        LlapStatefulSetDependent.class, LLAP_STATEFULSET,
        Set.of(LLAP_CONFIGMAP, HADOOP_CONFIGMAP),
        null, null, null, LLAP_ENABLED, null));

    specs.add(new DependentResourceSpec(
        HiveServiceDependent.Llap.class, LLAP_SERVICE,
        Set.of(), null, null, null, LLAP_ENABLED, null));

    // --- TezAM (conditional) ---
    specs.add(new DependentResourceSpec(
        ScratchPvcDependent.class, SCRATCH_PVC,
        Set.of(), null, null, null, TEZAM_ENABLED, null));

    specs.add(new DependentResourceSpec(
        HiveServiceDependent.TezAm.class, TEZAM_SERVICE,
        Set.of(), null, null, null, TEZAM_ENABLED, null));

    specs.add(new DependentResourceSpec(
        TezAmStatefulSetDependent.class, TEZAM_STATEFULSET,
        Set.of(HIVESERVER2_CONFIGMAP, HADOOP_CONFIGMAP, TEZAM_SERVICE, SCRATCH_PVC),
        null, null, null, TEZAM_ENABLED, null));

    // --- Autoscaling: PodDisruptionBudgets (conditional) ---
    specs.add(new DependentResourceSpec(
        HivePdbDependent.HiveServer2.class, HS2_PDB,
        Set.of(HIVESERVER2_DEPLOYMENT),
        null, HS2_AUTOSCALING, null, null, null));

    specs.add(new DependentResourceSpec(
        HivePdbDependent.Metastore.class, METASTORE_PDB,
        Set.of(METASTORE_DEPLOYMENT),
        null, METASTORE_AUTOSCALING, null, null, null));

    specs.add(new DependentResourceSpec(
        HivePdbDependent.Llap.class, LLAP_PDB,
        Set.of(LLAP_STATEFULSET),
        null, LLAP_AUTOSCALING, null, null, null));

    specs.add(new DependentResourceSpec(
        HivePdbDependent.TezAm.class, TEZAM_PDB,
        Set.of(TEZAM_STATEFULSET),
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
