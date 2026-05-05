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

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.HiveClusterSpec;
import org.apache.hive.kubernetes.operator.model.spec.LlapSpec;
import org.apache.hive.kubernetes.operator.model.spec.ResourceRequirementsSpec;
import org.apache.hive.kubernetes.operator.util.Labels;

/**
 * Manages the Kubernetes StatefulSet for LLAP daemons.
 * Uses StatefulSet for stable pod identities required by ZooKeeper registration.
 */
@KubernetesDependent(
    labelSelector = "app.kubernetes.io/component=llap,"
        + "app.kubernetes.io/managed-by=hive-kubernetes-operator"
)
public class LlapStatefulSetDependent
    extends HiveDependentResource<StatefulSet, HiveCluster> {

  public static final String COMPONENT = "llap";
  private static final String CONF_MOUNT_PATH = "/etc/hive/conf";
  private static final String HADOOP_CONF_MOUNT_PATH = "/etc/hadoop/conf";

  public LlapStatefulSetDependent() {
    super(StatefulSet.class);
  }

  @Override
  protected StatefulSet desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    HiveClusterSpec spec = hiveCluster.getSpec();
    LlapSpec llap = spec.getLlap();
    Map<String, String> selectorLabels =
        Labels.selectorForComponent(hiveCluster, COMPONENT);

    List<EnvVar> envVars = new ArrayList<>();
    envVars.add(new EnvVar("SERVICE_NAME", "llap", null));
    envVars.add(new EnvVar("IS_RESUME", "true", null));
    envVars.add(new EnvVar("HIVE_CUSTOM_CONF_DIR",
        CONF_MOUNT_PATH, null));
    envVars.add(new EnvVar("LLAP_MEMORY_MB",
        String.valueOf(llap.getMemoryMb()), null));
    envVars.add(new EnvVar("LLAP_EXECUTORS",
        String.valueOf(llap.getExecutors()), null));
    envVars.add(new EnvVar("HIVE_ZOOKEEPER_QUORUM",
        spec.getZookeeper().getQuorum(), null));
    envVars.add(new EnvVar("HIVE_LLAP_DAEMON_SERVICE_HOSTS",
        llap.getServiceHosts(), null));

    // S3A credentials so LLAP tasks can read/write S3.
    envVars.addAll(buildS3CredentialEnvVars(spec.getStorage()));
    envVars.add(new EnvVar("HADOOP_CLASSPATH",
        "/opt/hadoop/share/hadoop/tools/lib/*", null));

    List<ContainerPort> ports = List.of(
        new ContainerPortBuilder()
            .withName("management").withContainerPort(15004).build(),
        new ContainerPortBuilder()
            .withName("shuffle").withContainerPort(15551).build(),
        new ContainerPortBuilder()
            .withName("web").withContainerPort(15002).build(),
        new ContainerPortBuilder()
            .withName("output").withContainerPort(15003).build()
    );

    Probe readinessProbe = new ProbeBuilder()
        .withNewTcpSocket()
          .withPort(new IntOrString(15004))
        .endTcpSocket()
        .withInitialDelaySeconds(15)
        .withPeriodSeconds(10)
        .withFailureThreshold(3)
        .build();

    String headlessServiceName =
        hiveCluster.getMetadata().getName() + "-llap";

    return new StatefulSetBuilder()
        .withNewMetadata()
          .withName(resourceName(hiveCluster))
          .withNamespace(hiveCluster.getMetadata().getNamespace())
          .withLabels(Labels.forComponent(hiveCluster, COMPONENT))
        .endMetadata()
        .withNewSpec()
          .withReplicas(llap.getReplicas())
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
                .withName("llap")
                .withImage(spec.getImage())
                .withImagePullPolicy(spec.getImagePullPolicy())
                .withEnv(envVars)
                .withPorts(ports)
                .withReadinessProbe(readinessProbe)
                .withResources(buildResources(llap.getResources()))
                .addNewVolumeMount()
                  .withName("llap-config")
                  .withMountPath(CONF_MOUNT_PATH)
                .endVolumeMount()
                .addNewVolumeMount()
                  .withName("hadoop-config")
                  .withMountPath(HADOOP_CONF_MOUNT_PATH)
                .endVolumeMount()
              .endContainer()
              .addNewVolume()
                .withName("llap-config")
                .withNewConfigMap()
                  .withName(LlapConfigMapDependent
                      .resourceName(hiveCluster))
                .endConfigMap()
              .endVolume()
              .addNewVolume()
                .withName("hadoop-config")
                .withNewConfigMap()
                  .withName(HadoopConfigMapDependent
                      .resourceName(hiveCluster))
                .endConfigMap()
              .endVolume()
            .endSpec()
          .endTemplate()
        .endSpec()
        .build();
  }

  /** Returns the StatefulSet resource name for this HiveCluster. */
  public static String resourceName(HiveCluster hiveCluster) {
    return hiveCluster.getMetadata().getName() + "-llap";
  }
}
