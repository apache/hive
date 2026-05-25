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

package org.apache.hive.kubernetes.operator.dependent;

import java.util.Optional;
import java.util.Set;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.GarbageCollected;
import io.javaoperatorsdk.operator.processing.GroupVersionKind;
import io.javaoperatorsdk.operator.processing.dependent.Creator;
import io.javaoperatorsdk.operator.processing.dependent.Updater;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.GenericKubernetesDependentResource;
import org.apache.hive.kubernetes.operator.model.HiveCluster;

/**
 * Base class for dependent resources that manage custom resources via
 * {@link GenericKubernetesResource} (e.g. KEDA ScaledObject, HTTPScaledObject).
 * <p>
 * Extends {@link GenericKubernetesDependentResource} which properly configures
 * the informer with the specified GroupVersionKind, avoiding the fabric8
 * "resources cannot be called with a generic type" error.
 * <p>
 * Also overrides {@link #getSecondaryResource} to use the dependent's own
 * event source (same pattern as {@link HiveDependentResource}) so multiple
 * GenericKubernetesResource dependents don't collide in the type-based lookup.
 */
public abstract class HiveGenericDependentResource
    extends GenericKubernetesDependentResource<HiveCluster>
    implements Creator<GenericKubernetesResource, HiveCluster>,
    Updater<GenericKubernetesResource, HiveCluster>,
    GarbageCollected<HiveCluster> {

  protected HiveGenericDependentResource(GroupVersionKind gvk) {
    super(gvk);
  }

  /**
   * Returns the expected Kubernetes resource name for this dependent given the primary.
   * Used to discriminate between multiple secondary resources of the same GVK
   * (e.g. multiple ScaledObjects owned by the same HiveCluster).
   */
  protected abstract String getResourceName(HiveCluster hiveCluster);

  @Override
  public Optional<GenericKubernetesResource> getSecondaryResource(
      HiveCluster primary, Context<HiveCluster> context) {
    String expectedName = getResourceName(primary);
    Set<GenericKubernetesResource> secondaries = eventSource()
        .map(es -> es.getSecondaryResources(primary))
        .orElse(Set.of());
    return secondaries.stream()
        .filter(r -> expectedName.equals(r.getMetadata().getName()))
        .findFirst();
  }
}
