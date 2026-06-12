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

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.config.informer.Informer;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.HiveClusterSpec;
import org.apache.hive.kubernetes.operator.model.spec.AutoscalingSpec;
import org.apache.hive.kubernetes.operator.model.spec.TezAmSpec;
import org.apache.hive.kubernetes.operator.util.ConfigUtils;
import org.apache.hive.kubernetes.operator.util.HadoopXmlBuilder;
import org.apache.hive.kubernetes.operator.util.HiveConfigBuilder;
import org.apache.hive.kubernetes.operator.util.Labels;

/**
 * Manages the Kubernetes StatefulSet for the Tez Application Master.
 * Uses StatefulSet (with a headless Service) so that each TezAM pod
 * gets a stable, DNS-resolvable hostname. HiveServer2 discovers
 * TezAM pods via ZooKeeper and connects over RPC using the hostname,
 * so the hostname must be resolvable within the cluster.
 */
@KubernetesDependent(
    informer = @Informer(labelSelector = "app.kubernetes.io/component=tezam,"
        + "app.kubernetes.io/managed-by=hive-kubernetes-operator")
)
public class TezAmStatefulSetDependent
    extends HiveDependentResource<StatefulSet, HiveCluster> {

  public static final String COMPONENT = ConfigUtils.COMPONENT_TEZAM;
  private static final String SCRATCH_MOUNT_PATH = "/opt/hive/scratch";

  public TezAmStatefulSetDependent() {
    super(StatefulSet.class);
  }

  @Override
  protected String getSecondaryResourceName(HiveCluster primary,
      Context<HiveCluster> context) {
    return resourceName(primary);
  }

  @Override
  protected String getComponentName() {
    return COMPONENT;
  }

  @Override
  protected StatefulSet desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    HiveClusterSpec spec = hiveCluster.getSpec();
    TezAmSpec tezAm = spec.tezAm();
    Map<String, String> selectorLabels =
        Labels.selectorForComponent(hiveCluster, COMPONENT);

    List<EnvVar> envVars = new ArrayList<>();
    envVars.add(new EnvVar("SERVICE_NAME", COMPONENT, null));
    envVars.add(new EnvVar("IS_RESUME", "true", null));
    envVars.add(new EnvVar("HIVE_ZOOKEEPER_QUORUM",
        spec.zookeeper().quorum(), null));
    envVars.add(new EnvVar("TEZ_FRAMEWORK_MODE",
        "STANDALONE_ZOOKEEPER", null));

    if (spec.llap().isEnabled()) {
      envVars.add(new EnvVar("HIVE_LLAP_DAEMON_SERVICE_HOSTS",
          spec.llap().serviceHosts(), null));
    }

    // User-provided env vars (storage credentials, etc.)
    if (spec.envVars() != null) {
      envVars.addAll(spec.envVars());
    }

    String headlessServiceName =
        hiveCluster.getMetadata().getName() + "-tezam";

    List<io.fabric8.kubernetes.api.model.VolumeMount> volumeMounts =
        new ArrayList<>();
    volumeMounts.add(new io.fabric8.kubernetes.api.model.VolumeMountBuilder()
        .withName("hive-config")
        .withMountPath(CONF_MOUNT_PATH).build());
    volumeMounts.add(new io.fabric8.kubernetes.api.model.VolumeMountBuilder()
        .withName("scratch")
        .withMountPath(SCRATCH_MOUNT_PATH).build());

    List<io.fabric8.kubernetes.api.model.Volume> volumes =
        new ArrayList<>();
    volumes.add(buildProjectedConfigVolume("hive-config",
        HiveConfigMapDependent.HiveServer2.resourceName(hiveCluster),
        HiveConfigMapDependent.Hadoop.resourceName(hiveCluster)));
    volumes.add(new io.fabric8.kubernetes.api.model.VolumeBuilder()
        .withName("scratch")
        .withNewPersistentVolumeClaim()
          .withClaimName(ScratchPvcDependent.resourceName(hiveCluster))
        .endPersistentVolumeClaim()
        .build());

    List<ContainerPort> ports = new ArrayList<>();
    List<Container> initContainers = new ArrayList<>();
    addExternalJars(spec.image(), spec.externalJars(),
        initContainers, volumeMounts, volumes, envVars);
    replaceConfMountWithSubPaths(volumeMounts, "hive-config",
        "hive-site.xml", "tez-site.xml", "core-site.xml");

    // Pre-compute config hash for the pod template annotation.
    // TezAM uses the same ConfigMaps as HS2 (hive-site.xml + tez-site.xml + core-site.xml).
    String configHash = sha256(
        HadoopXmlBuilder.buildXml(HiveConfigBuilder.getHiveServer2HiveSite(hiveCluster, spec)),
        HadoopXmlBuilder.buildXml(HiveConfigBuilder.getTezSite(spec)),
        HadoopXmlBuilder.buildXml(HiveConfigBuilder.getHadoopCoreSite(spec)));

    AutoscalingSpec tezAmAutoscaling = tezAm.autoscaling();
    int initialReplicas = tezAmAutoscaling != null && tezAmAutoscaling.isEnabled()
        ? tezAmAutoscaling.minReplicas() : tezAm.replicas();
    Integer replicas = resolveReplicaCount(
        hiveCluster, context, tezAmAutoscaling, tezAm.replicas(), initialReplicas);

    StatefulSet statefulSet = new StatefulSetBuilder()
        .withNewMetadata()
          .withName(resourceName(hiveCluster))
          .withNamespace(hiveCluster.getMetadata().getNamespace())
          .withLabels(Labels.forComponent(hiveCluster, COMPONENT))
        .endMetadata()
        .withNewSpec()
          .withReplicas(replicas)
          .withServiceName(headlessServiceName)
          .withNewSelector()
            .withMatchLabels(selectorLabels)
          .endSelector()
          .withNewTemplate()
            .withNewMetadata()
              .withLabels(Labels.forComponent(hiveCluster, COMPONENT))
              .addToAnnotations("kubectl.kubernetes.io/default-container", COMPONENT)
              .addToAnnotations("hive.apache.org/config-hash", configHash)
            .endMetadata()
            .withNewSpec()
              .withInitContainers(initContainers)
              .addNewContainer()
                .withName(COMPONENT)
                .withImage(spec.image())
                .withImagePullPolicy(spec.imagePullPolicy())
                .withEnv(envVars)
                .withPorts(ports)
                .withResources(buildResources(tezAm.resources()))
                .withVolumeMounts(volumeMounts)
              .endContainer()
              .withVolumes(volumes)
            .endSpec()
          .endTemplate()
        .endSpec()
        .build();

    applySpreadAffinityIfAbsent(
        statefulSet.getSpec().getTemplate().getSpec(), selectorLabels);

    appendUserVolumes(statefulSet.getSpec().getTemplate().getSpec(),
        spec.volumes(), spec.volumeMounts(),
        tezAm.extraVolumes(), tezAm.extraVolumeMounts());

    return statefulSet;
  }

  /** Returns the StatefulSet resource name for this HiveCluster. */
  public static String resourceName(HiveCluster hiveCluster) {
    return hiveCluster.getMetadata().getName() + "-tezam";
  }
}
