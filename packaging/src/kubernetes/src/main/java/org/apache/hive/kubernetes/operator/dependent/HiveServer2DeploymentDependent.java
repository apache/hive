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
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.HiveClusterSpec;
import org.apache.hive.kubernetes.operator.model.spec.HiveServer2Spec;
import org.apache.hive.kubernetes.operator.util.Labels;

/** Manages the Kubernetes Deployment for HiveServer2. */
@KubernetesDependent(
    labelSelector = "app.kubernetes.io/component=hiveserver2,"
        + "app.kubernetes.io/managed-by=hive-kubernetes-operator"
)
public class HiveServer2DeploymentDependent
    extends HiveDependentResource<Deployment, HiveCluster> {

  public static final String COMPONENT = "hiveserver2";
  private static final String CONF_MOUNT_PATH = "/etc/hive/conf";
  private static final String SCRATCH_MOUNT_PATH = "/opt/hive/scratch";

  public HiveServer2DeploymentDependent() {
    super(Deployment.class);
  }

  @Override
  protected Deployment desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    HiveClusterSpec spec = hiveCluster.getSpec();
    HiveServer2Spec hs2 = spec.getHiveServer2();
    Map<String, String> selectorLabels =
        Labels.selectorForComponent(hiveCluster, COMPONENT);

    List<EnvVar> envVars = new ArrayList<>();
    envVars.add(new EnvVar("SERVICE_NAME", "hiveserver2", null));
    envVars.add(new EnvVar("IS_RESUME", "true", null));
    envVars.add(new EnvVar("HIVE_CUSTOM_CONF_DIR",
        CONF_MOUNT_PATH, null));
    envVars.add(new EnvVar("HADOOP_CLASSPATH",
        "/opt/hadoop/share/hadoop/tools/lib/*", null));
    envVars.add(new EnvVar("TEZ_AM_EXTERNAL_ID",
        "tez-session-hs2", null));

    // S3A credentials as env vars so Tez tasks can resolve them
    // via the default AWS credential provider chain.
    envVars.addAll(buildS3CredentialEnvVars(spec.getStorage()));

    String metastoreUri = "thrift://"
        + hiveCluster.getMetadata().getName() + "-metastore:9083";
    StringBuilder serviceOpts = new StringBuilder();
    serviceOpts.append("-Dhive.metastore.uris=").append(metastoreUri);
    if (spec.getLlap().isEnabled()) {
      serviceOpts.append(" -Dhive.execution.mode=llap");
      serviceOpts.append(" -Dhive.llap.daemon.service.hosts=")
          .append(spec.getLlap().getServiceHosts());
    }
    if (spec.getTezAm().isEnabled()) {
      serviceOpts.append(" -Dhive.zookeeper.quorum=")
          .append(spec.getZookeeper().getQuorum());
    }
    envVars.add(new EnvVar("SERVICE_OPTS",
        serviceOpts.toString(), null));

    List<ContainerPort> ports = List.of(
        new ContainerPortBuilder()
            .withName("thrift")
            .withContainerPort(hs2.getThriftPort()).build(),
        new ContainerPortBuilder()
            .withName("webui")
            .withContainerPort(hs2.getWebUiPort()).build()
    );

    Probe readinessProbe = new ProbeBuilder()
        .withNewTcpSocket()
          .withPort(new IntOrString(hs2.getThriftPort()))
        .endTcpSocket()
        .withInitialDelaySeconds(15)
        .withPeriodSeconds(10)
        .withFailureThreshold(3)
        .build();

    Probe livenessProbe = new ProbeBuilder()
        .withNewTcpSocket()
          .withPort(new IntOrString(hs2.getThriftPort()))
        .endTcpSocket()
        .withInitialDelaySeconds(120)
        .withPeriodSeconds(30)
        .withFailureThreshold(10)
        .build();

    boolean tezAmEnabled = spec.getTezAm().isEnabled();

    // Build volume mounts and volumes lists up front so the
    // Deployment is constructed in a single builder chain.
    // Using editFirstContainer() caused JOSDK SSA comparison
    // mismatches that triggered infinite reconciliation loops.
    List<io.fabric8.kubernetes.api.model.VolumeMount> volumeMounts =
        new ArrayList<>();
    volumeMounts.add(new io.fabric8.kubernetes.api.model.VolumeMountBuilder()
        .withName("hive-config").withMountPath(CONF_MOUNT_PATH).build());

    List<io.fabric8.kubernetes.api.model.Volume> volumes =
        new ArrayList<>();
    volumes.add(new io.fabric8.kubernetes.api.model.VolumeBuilder()
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
        .build());

    if (tezAmEnabled) {
      volumeMounts.add(
          new io.fabric8.kubernetes.api.model.VolumeMountBuilder()
              .withName("scratch")
              .withMountPath(SCRATCH_MOUNT_PATH).build());
      volumes.add(new io.fabric8.kubernetes.api.model.VolumeBuilder()
          .withName("scratch")
          .withNewPersistentVolumeClaim()
            .withClaimName(ScratchPvcDependent
                .resourceName(hiveCluster))
          .endPersistentVolumeClaim()
          .build());
    }

    Deployment deployment = new DeploymentBuilder()
        .withNewMetadata()
          .withName(resourceName(hiveCluster))
          .withNamespace(hiveCluster.getMetadata().getNamespace())
          .withLabels(Labels.forComponent(hiveCluster, COMPONENT))
        .endMetadata()
        .withNewSpec()
          .withReplicas(hs2.getReplicas())
          .withNewSelector()
            .withMatchLabels(selectorLabels)
          .endSelector()
          .withNewTemplate()
            .withNewMetadata()
              .withLabels(Labels.forComponent(hiveCluster, COMPONENT))
            .endMetadata()
            .withNewSpec()
              .addNewContainer()
                .withName("hiveserver2")
                .withImage(spec.getImage())
                .withImagePullPolicy(spec.getImagePullPolicy())
                .withEnv(envVars)
                .withPorts(ports)
                .withReadinessProbe(readinessProbe)
                .withLivenessProbe(livenessProbe)
                .withResources(buildResources(hs2.getResources()))
                .withVolumeMounts(volumeMounts)
              .endContainer()
              .withVolumes(volumes)
            .endSpec()
          .endTemplate()
        .endSpec()
        .build();
    return deployment;
  }

  /** Returns the Deployment resource name for this HiveCluster. */
  public static String resourceName(HiveCluster hiveCluster) {
    return hiveCluster.getMetadata().getName() + "-hiveserver2";
  }
}
