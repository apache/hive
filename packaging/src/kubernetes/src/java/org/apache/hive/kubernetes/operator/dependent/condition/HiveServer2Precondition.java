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

package org.apache.hive.kubernetes.operator.dependent.condition;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;
import org.apache.hive.kubernetes.operator.model.HiveCluster;

/**
 * Precondition for HiveServer2 Deployment.
 * If Metastore is external, proceed immediately.
 * If managed, wait for Metastore pods to be ready.
 */
public class HiveServer2Precondition implements Condition<Deployment, HiveCluster> {

  @Override
  public boolean isMet(
      DependentResource<Deployment, HiveCluster> dependentResource,
      HiveCluster primary,
      Context<HiveCluster> context) {

    if (!primary.getSpec().metastore().isEnabled()) {
      return true;
    }

    // When autoscaling is enabled, wait for minReplicas (KEDA manages scaling beyond that).
    // Without autoscaling, wait for all configured replicas.
    int desiredReplicas;
    if (primary.getSpec().metastore().autoscaling().isEnabled()) {
      desiredReplicas = Math.max(1, primary.getSpec().metastore().autoscaling().minReplicas());
    } else {
      desiredReplicas = primary.getSpec().metastore().replicas();
    }
    return context.getSecondaryResources(Deployment.class).stream()
        .filter(d -> d.getMetadata().getName().equals(primary.getMetadata().getName() + "-metastore"))
        .findFirst()
        .map(deployment -> deployment.getStatus() != null
            && deployment.getStatus().getReadyReplicas() != null
            && deployment.getStatus().getReadyReplicas() >= desiredReplicas)
        .orElse(false);
  }
}
