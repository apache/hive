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
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.util.Labels;

/**
 * PodDisruptionBudget for LLAP daemons.
 * Ensures at least one LLAP daemon remains available during voluntary disruptions
 * to prevent query failures and cache loss.
 */
public class LlapPdbDependent
    extends CRUDKubernetesDependentResource<PodDisruptionBudget, HiveCluster> {

  public LlapPdbDependent() {
    super(PodDisruptionBudget.class);
  }

  @Override
  protected PodDisruptionBudget desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    return new PodDisruptionBudgetBuilder()
        .withNewMetadata()
          .withName(resourceName(hiveCluster))
          .withNamespace(hiveCluster.getMetadata().getNamespace())
          .withLabels(Labels.forComponent(hiveCluster, "llap"))
        .endMetadata()
        .withNewSpec()
          .withMinAvailable(new IntOrString(1))
          .withNewSelector()
            .withMatchLabels(Labels.selectorForComponent(hiveCluster, "llap"))
          .endSelector()
        .endSpec()
        .build();
  }

  public static String resourceName(HiveCluster hiveCluster) {
    return hiveCluster.getMetadata().getName() + "-llap-pdb";
  }
}
