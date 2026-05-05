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
 * Ready condition that checks whether the Metastore Deployment has the
 * desired number of ready replicas. Used to gate HiveServer2 Deployment.
 */
public class MetastoreReadyCondition
    implements Condition<Deployment, HiveCluster> {

  @Override
  public boolean isMet(
      DependentResource<Deployment, HiveCluster> dependentResource,
      HiveCluster primary,
      Context<HiveCluster> context) {
    int desiredReplicas = primary.getSpec().getMetastore().getReplicas();
    return dependentResource.getSecondaryResource(primary, context)
        .map(deployment -> deployment.getStatus() != null
            && deployment.getStatus().getReadyReplicas() != null
            && deployment.getStatus().getReadyReplicas() >= desiredReplicas)
        .orElse(false);
  }
}
