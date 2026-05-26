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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.javaoperatorsdk.operator.api.config.informer.InformerEventSourceConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
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
   * Adds a generation-aware update filter so that KEDA/controller status
   * patches (which don't increment metadata.generation) do not trigger
   * unnecessary reconciliation loops.
   */
  @Override
  protected InformerEventSourceConfiguration.Builder<GenericKubernetesResource>
      informerConfigurationBuilder(EventSourceContext<HiveCluster> context) {
    return super.informerConfigurationBuilder(context)
        .withOnUpdateFilter((newResource, oldResource) -> {
          Long newGen = newResource.getMetadata().getGeneration();
          Long oldGen = oldResource.getMetadata().getGeneration();
          return !Objects.equals(newGen, oldGen);
        });
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

  /**
   * Builds the nested "advanced" HPA behavior configuration for a KEDA ScaledObject.
   *
   * @param scaleDownStabilization  stabilizationWindowSeconds for scale-down
   * @param scaleDownPolicyType     policy type (e.g. "Pods", "Percent")
   * @param scaleDownValue          policy value
   * @param scaleDownPeriod         policy periodSeconds
   * @param scaleUpStabilization    stabilizationWindowSeconds for scale-up
   * @param scaleUpPolicyType       policy type (e.g. "Pods", "Percent")
   * @param scaleUpValue            policy value
   * @param scaleUpPeriod           policy periodSeconds
   */
  protected static Map<String, Object> buildHpaBehavior(
      int scaleDownStabilization, String scaleDownPolicyType,
      int scaleDownValue, int scaleDownPeriod,
      int scaleUpStabilization, String scaleUpPolicyType,
      int scaleUpValue, int scaleUpPeriod) {
    return Map.of(
        "horizontalPodAutoscalerConfig", Map.of(
            "behavior", Map.of(
                "scaleDown", Map.of(
                    "stabilizationWindowSeconds", scaleDownStabilization,
                    "policies", List.of(Map.of(
                        "type", scaleDownPolicyType,
                        "value", scaleDownValue,
                        "periodSeconds", scaleDownPeriod
                    ))
                ),
                "scaleUp", Map.of(
                    "stabilizationWindowSeconds", scaleUpStabilization,
                    "policies", List.of(Map.of(
                        "type", scaleUpPolicyType,
                        "value", scaleUpValue,
                        "periodSeconds", scaleUpPeriod
                    ))
                )
            )
        )
    );
  }

  /**
   * Builds the HS2 cross-component activation trigger used by LLAP and TezAM.
   * Uses {@code (max(hs2_open_sessions{...}) > bool 0) or vector(0)} so the
   * result is always 0 or 1, preventing zombie sessions from driving proportional scaling.
   * Threshold is set to maxReplicas so desired = ceil(1/max) = 1 (activation only).
   *
   * @param namespace     the Kubernetes namespace
   * @param hs2TargetName the HS2 deployment name (for pod label matching)
   * @param maxReplicas   the max replicas of the component (used as threshold)
   */
  protected static Map<String, Object> buildHs2ActivationTrigger(
      String namespace, String hs2TargetName, int maxReplicas) {
    return Map.of(
        "type", "prometheus",
        "metadata", Map.of(
            "serverAddress", "http://prometheus-server.monitoring.svc.cluster.local",
            "metricName", "hs2_open_sessions_activation",
            "query", String.format(
                "(max(hs2_open_sessions{namespace=\"%s\",pod=~\"%s-.*\"}) > bool 0) or vector(0)",
                namespace, hs2TargetName),
            "threshold", String.valueOf(maxReplicas),
            "activationThreshold", "0"
        )
    );
  }
}
