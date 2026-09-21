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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.kubernetes.operator.util;

import java.util.OptionalInt;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.slf4j.Logger;

/**
 * Helpers for the operator's workloads (Deployment/StatefulSet): reading fields without the
 * null-guard boilerplate every caller would otherwise repeat, logging replica changes in one
 * shape, and resolving the workload's K8s name from the autoscaler's component key.
 */
public final class Workloads {

  private Workloads() {}

  /**
   * Returns spec.replicas from a Deployment or StatefulSet. Empty when the resource is absent,
   * has no spec, or the field is unset — in practice that means "the workload isn't there yet",
   * since the API server defaults spec.replicas on write. A non-workload resource is empty too.
   */
  public static OptionalInt replicas(HasMetadata resource) {
    Integer replicas = null;
    if (resource instanceof Deployment d && d.getSpec() != null) {
      replicas = d.getSpec().getReplicas();
    } else if (resource instanceof StatefulSet s && s.getSpec() != null) {
      replicas = s.getSpec().getReplicas();
    }
    return replicas == null ? OptionalInt.empty() : OptionalInt.of(replicas);
  }

  /**
   * Logs the replica count the operator is about to apply to {@code namespace/name}. One line
   * covers both cases: {@code current} is the workload as it exists in the cluster, or null when
   * it doesn't exist yet, which logs as {@code none -> N}. Nothing is logged when the count
   * already matches, so silence means no scale is happening.
   * <p>
   * Shared by every scale path — the dependents' SSA and the imperative LLAP/TezAM SSAs — so the
   * operator log reads the same whichever one ran. The caller passes its own logger to keep the
   * log category pointing at the code that is actually scaling.
   */
  public static void logReplicaChange(Logger log, String component, String namespace, String name,
      HasMetadata current, int desired) {
    OptionalInt actual = replicas(current);
    if (actual.isPresent() && actual.getAsInt() == desired) {
      return;
    }
    log.info("Setting replica count for {} {}/{}: {} -> {}", component, namespace, name,
        actual.isPresent() ? String.valueOf(actual.getAsInt()) : "none", desired);
  }

  /**
   * Maps an autoscaler component key to the K8s workload name it drives. Per-LLAP components
   * carry the LLAP cluster name in their key ("llap-{name}", "tezam-{name}"); everything else
   * (HS2, Metastore) is a plain "{cluster}-{component}". Kept here so every scale path — the
   * autoscaler's `patchReplicas`, the imperative LLAP/TezAM SSAs, and the idle-check reads —
   * resolves the name the same way.
   * <ul>
   *   <li>{@code llap-{name}}  → {@code {cluster}-{name}}</li>
   *   <li>{@code tezam-{name}} → {@code {cluster}-tezam-{name}}</li>
   *   <li>otherwise            → {@code {cluster}-{component}}</li>
   * </ul>
   */
  public static String nameFor(HiveCluster hc, String component) {
    String cluster = hc.getMetadata().getName();
    if (component.startsWith(ConfigUtils.COMPONENT_LLAP + "-")) {
      String llapName = component.substring(ConfigUtils.COMPONENT_LLAP.length() + 1);
      return cluster + "-" + llapName;
    }
    if (component.startsWith(ConfigUtils.COMPONENT_TEZAM + "-")) {
      String llapName = component.substring(ConfigUtils.COMPONENT_TEZAM.length() + 1);
      return cluster + "-" + ConfigUtils.COMPONENT_TEZAM + "-" + llapName;
    }
    return cluster + "-" + component;
  }
}
