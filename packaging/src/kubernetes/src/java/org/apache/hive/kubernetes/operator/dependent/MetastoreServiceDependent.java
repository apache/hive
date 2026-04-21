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
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.util.Labels;

/** Manages the Kubernetes Service for the Hive Metastore (Thrift + REST ports). */
@KubernetesDependent(
    labelSelector = "app.kubernetes.io/component=metastore,"
        + "app.kubernetes.io/managed-by=hive-kubernetes-operator"
)
public class MetastoreServiceDependent
    extends HiveDependentResource<Service, HiveCluster> {

  public MetastoreServiceDependent() {
    super(Service.class);
  }

  @Override
  protected Service desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    return new ServiceBuilder()
        .withNewMetadata()
          .withName(hiveCluster.getMetadata().getName() + "-metastore")
          .withNamespace(hiveCluster.getMetadata().getNamespace())
          .withLabels(Labels.forComponent(hiveCluster,
              MetastoreDeploymentDependent.COMPONENT))
        .endMetadata()
        .withNewSpec()
          .withType("ClusterIP")
          .withSelector(Labels.selectorForComponent(hiveCluster,
              MetastoreDeploymentDependent.COMPONENT))
          .addNewPort()
            .withName("thrift")
            .withPort(9083)
            .withTargetPort(new IntOrString(9083))
          .endPort()
          .addNewPort()
            .withName("rest")
            .withPort(9001)
            .withTargetPort(new IntOrString(9001))
          .endPort()
        .endSpec()
        .build();
  }
}
