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

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudgetBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.config.informer.Informer;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.util.ConfigUtils;
import org.apache.hive.kubernetes.operator.util.Labels;

/**
 * Unified PodDisruptionBudget dependent resource for all Hive components.
 * Uses maxUnavailable=1 to allow at most one pod to be disrupted at a time
 * while still permitting node drains when replicas=1.
 * <p>
 * Subclassed per component (HS2, Metastore, LLAP, TezAM) only to satisfy
 * JOSDK's requirement for distinct no-arg-constructible classes in the workflow.
 */
public abstract class HivePdbDependent
    extends HiveDependentResource<PodDisruptionBudget, HiveCluster> {

  private final String component;

  protected HivePdbDependent(String component) {
    super(PodDisruptionBudget.class);
    this.component = component;
  }

  @Override
  protected String getSecondaryResourceName(HiveCluster primary,
      Context<HiveCluster> context) {
    return primary.getMetadata().getName() + "-" + component + "-pdb";
  }

  @Override
  protected PodDisruptionBudget desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    return new PodDisruptionBudgetBuilder()
        .withNewMetadata()
          .withName(hiveCluster.getMetadata().getName() + "-" + component + "-pdb")
          .withNamespace(hiveCluster.getMetadata().getNamespace())
          .withLabels(Labels.forComponent(hiveCluster, component))
        .endMetadata()
        .withNewSpec()
          .withMaxUnavailable(new IntOrString(1))
          .withNewSelector()
            .withMatchLabels(Labels.selectorForComponent(hiveCluster, component))
          .endSelector()
        .endSpec()
        .build();
  }

  @KubernetesDependent(
      informer = @Informer(labelSelector = "app.kubernetes.io/component=hiveserver2,"
          + "app.kubernetes.io/managed-by=hive-kubernetes-operator")
  )
  public static class HiveServer2 extends HivePdbDependent {
    public HiveServer2() {
      super(ConfigUtils.COMPONENT_HIVESERVER2);
    }
  }

  @KubernetesDependent(
      informer = @Informer(labelSelector = "app.kubernetes.io/component=metastore,"
          + "app.kubernetes.io/managed-by=hive-kubernetes-operator")
  )
  public static class Metastore extends HivePdbDependent {
    public Metastore() {
      super(ConfigUtils.COMPONENT_METASTORE);
    }
  }

  @KubernetesDependent(
      informer = @Informer(labelSelector = "app.kubernetes.io/component=llap,"
          + "app.kubernetes.io/managed-by=hive-kubernetes-operator")
  )
  public static class Llap extends HivePdbDependent {
    public Llap() {
      super(ConfigUtils.COMPONENT_LLAP);
    }
  }

  @KubernetesDependent(
      informer = @Informer(labelSelector = "app.kubernetes.io/component=tezam,"
          + "app.kubernetes.io/managed-by=hive-kubernetes-operator")
  )
  public static class TezAm extends HivePdbDependent {
    public TezAm() {
      super(ConfigUtils.COMPONENT_TEZAM);
    }
  }
}
