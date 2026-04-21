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

import java.util.Map;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.util.HadoopXmlBuilder;
import org.apache.hive.kubernetes.operator.util.HiveConfigBuilder;
import org.apache.hive.kubernetes.operator.util.Labels;

/** Manages the Hadoop core-site.xml ConfigMap for filesystem configuration. */
@KubernetesDependent(
    labelSelector = "app.kubernetes.io/component=hadoop-config,"
        + "app.kubernetes.io/managed-by=hive-kubernetes-operator"
)
public class HadoopConfigMapDependent
    extends HiveDependentResource<ConfigMap, HiveCluster> {

  public static final String COMPONENT = "hadoop-config";

  public HadoopConfigMapDependent() {
    super(ConfigMap.class);
  }

  @Override
  protected ConfigMap desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    Map<String, String> props =
        HiveConfigBuilder.getHadoopCoreSite(hiveCluster.getSpec());

    return new ConfigMapBuilder()
        .withNewMetadata()
          .withName(resourceName(hiveCluster))
          .withNamespace(hiveCluster.getMetadata().getNamespace())
          .withLabels(Labels.forComponent(hiveCluster, COMPONENT))
        .endMetadata()
        .addToData("core-site.xml", HadoopXmlBuilder.buildXml(props))
        .build();
  }

  /** Returns the ConfigMap resource name for this HiveCluster. */
  public static String resourceName(HiveCluster hiveCluster) {
    return hiveCluster.getMetadata().getName() + "-hadoop-config";
  }
}
