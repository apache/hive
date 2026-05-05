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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.HiveClusterSpec;
import org.apache.hive.kubernetes.operator.model.spec.TezAmSpec;
import org.apache.hive.kubernetes.operator.util.Labels;

/**
 * Manages the Kubernetes StatefulSet for the Tez Application Master.
 * Uses StatefulSet (with a headless Service) so that each TezAM pod
 * gets a stable, DNS-resolvable hostname. HiveServer2 discovers
 * TezAM pods via ZooKeeper and connects over RPC using the hostname,
 * so the hostname must be resolvable within the cluster.
 */
@KubernetesDependent(
    labelSelector = "app.kubernetes.io/component=tezam,"
        + "app.kubernetes.io/managed-by=hive-kubernetes-operator"
)
public class TezAmStatefulSetDependent
    extends HiveDependentResource<StatefulSet, HiveCluster> {

  public static final String COMPONENT = "tezam";
  private static final String CONF_MOUNT_PATH = "/etc/hive/conf";
  private static final String SCRATCH_MOUNT_PATH = "/opt/hive/scratch";

  public TezAmStatefulSetDependent() {
    super(StatefulSet.class);
  }

  @Override
  protected StatefulSet desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    HiveClusterSpec spec = hiveCluster.getSpec();
    TezAmSpec tezAm = spec.getTezAm();
    Map<String, String> selectorLabels =
        Labels.selectorForComponent(hiveCluster, COMPONENT);

    List<EnvVar> envVars = new ArrayList<>();
    envVars.add(new EnvVar("SERVICE_NAME", "tezam", null));
    envVars.add(new EnvVar("IS_RESUME", "true", null));
    envVars.add(new EnvVar("HIVE_CUSTOM_CONF_DIR",
        CONF_MOUNT_PATH, null));
    envVars.add(new EnvVar("HIVE_ZOOKEEPER_QUORUM",
        spec.getZookeeper().getQuorum(), null));
    envVars.add(new EnvVar("TEZ_FRAMEWORK_MODE",
        "STANDALONE_ZOOKEEPER", null));

    if (spec.getLlap().isEnabled()) {
      envVars.add(new EnvVar("HIVE_LLAP_DAEMON_SERVICE_HOSTS",
          spec.getLlap().getServiceHosts(), null));
    }

    // S3A credentials as env vars.
    envVars.addAll(buildS3CredentialEnvVars(spec.getStorage()));

    envVars.add(new EnvVar("HADOOP_CLASSPATH",
        "/opt/hadoop/share/hadoop/tools/lib/*", null));

    String headlessServiceName =
        hiveCluster.getMetadata().getName() + "-tezam";

    return new StatefulSetBuilder()
        .withNewMetadata()
          .withName(resourceName(hiveCluster))
          .withNamespace(hiveCluster.getMetadata().getNamespace())
          .withLabels(Labels.forComponent(hiveCluster, COMPONENT))
        .endMetadata()
        .withNewSpec()
          .withReplicas(tezAm.getReplicas())
          .withServiceName(headlessServiceName)
          .withNewSelector()
            .withMatchLabels(selectorLabels)
          .endSelector()
          .withNewTemplate()
            .withNewMetadata()
              .withLabels(Labels.forComponent(hiveCluster, COMPONENT))
            .endMetadata()
            .withNewSpec()
              .addNewContainer()
                .withName("tezam")
                .withImage(spec.getImage())
                .withImagePullPolicy(spec.getImagePullPolicy())
                .withEnv(envVars)
                .withResources(buildResources(tezAm.getResources()))
                .addNewVolumeMount()
                  .withName("hive-config")
                  .withMountPath(CONF_MOUNT_PATH)
                .endVolumeMount()
                .addNewVolumeMount()
                  .withName("scratch")
                  .withMountPath(SCRATCH_MOUNT_PATH)
                .endVolumeMount()
              .endContainer()
              .addNewVolume()
                .withName("hive-config")
                .withNewProjected()
                  .addNewSource()
                    .withNewConfigMap()
                      .withName(HiveServer2ConfigMapDependent
                          .resourceName(hiveCluster))
                    .endConfigMap()
                  .endSource()
                  .addNewSource()
                    .withNewConfigMap()
                      .withName(HadoopConfigMapDependent
                          .resourceName(hiveCluster))
                    .endConfigMap()
                  .endSource()
                .endProjected()
              .endVolume()
              .addNewVolume()
                .withName("scratch")
                .withNewPersistentVolumeClaim()
                  .withClaimName(ScratchPvcDependent
                      .resourceName(hiveCluster))
                .endPersistentVolumeClaim()
              .endVolume()
            .endSpec()
          .endTemplate()
        .endSpec()
        .build();
  }

  /** Returns the StatefulSet resource name for this HiveCluster. */
  public static String resourceName(HiveCluster hiveCluster) {
    return hiveCluster.getMetadata().getName() + "-tezam";
  }
}
