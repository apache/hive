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
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.config.informer.Informer;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.HiveClusterSpec;
import org.apache.hive.kubernetes.operator.model.spec.TezAmSpec;
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

  public static final String COMPONENT = "tezam";
  private static final String SCRATCH_MOUNT_PATH = "/opt/hive/scratch";

  public TezAmStatefulSetDependent() {
    super(StatefulSet.class);
  }

  @Override
  protected StatefulSet desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    HiveClusterSpec spec = hiveCluster.getSpec();
    TezAmSpec tezAm = spec.tezAm();
    Map<String, String> selectorLabels =
        Labels.selectorForComponent(hiveCluster, COMPONENT);

    List<EnvVar> envVars = new ArrayList<>();
    envVars.add(new EnvVar("SERVICE_NAME", "tezam", null));
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
        HiveServer2ConfigMapDependent.resourceName(hiveCluster),
        HadoopConfigMapDependent.resourceName(hiveCluster)));
    volumes.add(new io.fabric8.kubernetes.api.model.VolumeBuilder()
        .withName("scratch")
        .withNewPersistentVolumeClaim()
          .withClaimName(ScratchPvcDependent.resourceName(hiveCluster))
        .endPersistentVolumeClaim()
        .build());

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

    StatefulSet statefulSet = new StatefulSetBuilder()
        .withNewMetadata()
          .withName(resourceName(hiveCluster))
          .withNamespace(hiveCluster.getMetadata().getNamespace())
          .withLabels(Labels.forComponent(hiveCluster, COMPONENT))
        .endMetadata()
        .withNewSpec()
          .withReplicas(tezAm.replicas())
          .withServiceName(headlessServiceName)
          .withNewSelector()
            .withMatchLabels(selectorLabels)
          .endSelector()
          .withNewTemplate()
            .withNewMetadata()
              .withLabels(Labels.forComponent(hiveCluster, COMPONENT))
              .addToAnnotations("kubectl.kubernetes.io/default-container", "tezam")
              .addToAnnotations("hive.apache.org/config-hash", configHash)
            .endMetadata()
            .withNewSpec()
              .withInitContainers(initContainers)
              .addNewContainer()
                .withName("tezam")
                .withImage(spec.image())
                .withImagePullPolicy(spec.imagePullPolicy())
                .withEnv(envVars)
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

    if (spec.volumes() != null) {
      statefulSet.getSpec().getTemplate().getSpec().getVolumes().addAll(spec.volumes());
    }
    if (spec.volumeMounts() != null) {
      statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts()
          .addAll(spec.volumeMounts());
    }
    if (tezAm.extraVolumes() != null) {
      statefulSet.getSpec().getTemplate().getSpec().getVolumes().addAll(tezAm.extraVolumes());
    }
    if (tezAm.extraVolumeMounts() != null) {
      statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts()
          .addAll(tezAm.extraVolumeMounts());
    }
    return statefulSet;
  }

  /** Returns the StatefulSet resource name for this HiveCluster. */
  public static String resourceName(HiveCluster hiveCluster) {
    return hiveCluster.getMetadata().getName() + "-tezam";
  }
}
