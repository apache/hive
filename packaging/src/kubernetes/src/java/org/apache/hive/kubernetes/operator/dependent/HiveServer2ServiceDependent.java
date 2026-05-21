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
import io.javaoperatorsdk.operator.api.config.informer.Informer;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.spec.HiveServer2Spec;
import org.apache.hive.kubernetes.operator.util.ConfigUtils;
import org.apache.hive.kubernetes.operator.util.Labels;

/** Manages the Kubernetes Service for HiveServer2 (Thrift and WebUI ports). */
@KubernetesDependent(
    informer = @Informer(labelSelector = "app.kubernetes.io/component=hiveserver2,"
        + "app.kubernetes.io/managed-by=hive-kubernetes-operator")
)
public class HiveServer2ServiceDependent
    extends HiveDependentResource<Service, HiveCluster> {

  public HiveServer2ServiceDependent() {
    super(Service.class);
  }

  @Override
  protected Service desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    HiveServer2Spec hs2 = hiveCluster.getSpec().hiveServer2();
    int thriftPort = ConfigUtils.getInt(hs2.configOverrides(),
        ConfigUtils.HIVE_SERVER2_THRIFT_PORT_KEY,
        null, ConfigUtils.HIVE_SERVER2_THRIFT_PORT_DEFAULT);
    int webUiPort = ConfigUtils.getInt(hs2.configOverrides(),
        ConfigUtils.HIVE_SERVER2_WEBUI_PORT_KEY,
        null, ConfigUtils.HIVE_SERVER2_WEBUI_PORT_DEFAULT);

    return new ServiceBuilder()
        .withNewMetadata()
          .withName(hiveCluster.getMetadata().getName() + "-hiveserver2")
          .withNamespace(hiveCluster.getMetadata().getNamespace())
          .withLabels(Labels.forComponent(hiveCluster,
              HiveServer2DeploymentDependent.COMPONENT))
        .endMetadata()
        .withNewSpec()
          .withType(hs2.serviceType())
          .withSelector(Labels.selectorForComponent(hiveCluster,
              HiveServer2DeploymentDependent.COMPONENT))
          .addNewPort()
            .withName("thrift")
            .withPort(thriftPort)
            .withTargetPort(new IntOrString(thriftPort))
          .endPort()
          .addNewPort()
            .withName("webui")
            .withPort(webUiPort)
            .withTargetPort(new IntOrString(webUiPort))
          .endPort()
        .endSpec()
        .build();
  }
}
