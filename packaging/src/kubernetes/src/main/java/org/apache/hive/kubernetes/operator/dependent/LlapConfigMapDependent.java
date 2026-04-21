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

import java.util.LinkedHashMap;
import java.util.Map;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.HiveClusterSpec;
import org.apache.hive.kubernetes.operator.model.spec.LlapSpec;
import org.apache.hive.kubernetes.operator.util.HadoopXmlBuilder;
import org.apache.hive.kubernetes.operator.util.Labels;

/** Manages the llap-daemon-site.xml ConfigMap for LLAP daemons. */
@KubernetesDependent(
    labelSelector = "app.kubernetes.io/component=llap,"
        + "app.kubernetes.io/managed-by=hive-kubernetes-operator"
)
public class LlapConfigMapDependent
    extends HiveDependentResource<ConfigMap, HiveCluster> {

  public static final String COMPONENT = "llap";

  public LlapConfigMapDependent() {
    super(ConfigMap.class);
  }

  @Override
  protected ConfigMap desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    HiveClusterSpec spec = hiveCluster.getSpec();
    LlapSpec llap = spec.getLlap();
    Map<String, String> props = new LinkedHashMap<>();

    props.put("hive.llap.daemon.memory.per.instance.mb",
        String.valueOf(llap.getMemoryMb()));
    props.put("hive.llap.daemon.num.executors",
        String.valueOf(llap.getExecutors()));
    props.put("hive.llap.daemon.service.hosts", llap.getServiceHosts());
    props.put("hive.zookeeper.quorum",
        spec.getZookeeper().getQuorum());

    if (llap.getConfigOverrides() != null) {
      props.putAll(llap.getConfigOverrides());
    }

    return new ConfigMapBuilder()
        .withNewMetadata()
          .withName(resourceName(hiveCluster))
          .withNamespace(hiveCluster.getMetadata().getNamespace())
          .withLabels(Labels.forComponent(hiveCluster, COMPONENT))
        .endMetadata()
        .addToData("llap-daemon-site.xml",
            HadoopXmlBuilder.buildXml(props))
        .build();
  }

  /** Returns the ConfigMap resource name for this HiveCluster. */
  public static String resourceName(HiveCluster hiveCluster) {
    return hiveCluster.getMetadata().getName() + "-llap-config";
  }
}
