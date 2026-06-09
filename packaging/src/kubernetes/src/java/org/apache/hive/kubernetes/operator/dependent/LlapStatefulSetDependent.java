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
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.config.informer.Informer;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.HiveClusterSpec;
import org.apache.hive.kubernetes.operator.model.spec.AutoscalingSpec;
import org.apache.hive.kubernetes.operator.model.spec.LlapSpec;
import org.apache.hive.kubernetes.operator.util.ConfigUtils;
import org.apache.hive.kubernetes.operator.util.HadoopXmlBuilder;
import org.apache.hive.kubernetes.operator.util.HiveConfigBuilder;
import org.apache.hive.kubernetes.operator.util.Labels;

/**
 * Manages the Kubernetes StatefulSet for LLAP daemons.
 * Uses StatefulSet for stable pod identities required by ZooKeeper registration.
 */
@KubernetesDependent(
    informer = @Informer(labelSelector = "app.kubernetes.io/component=llap,"
        + "app.kubernetes.io/managed-by=hive-kubernetes-operator")
)
public class LlapStatefulSetDependent
    extends HiveDependentResource<StatefulSet, HiveCluster> {

  public static final String COMPONENT = ConfigUtils.COMPONENT_LLAP;

  public LlapStatefulSetDependent() {
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
    LlapSpec llap = spec.llap();
    Map<String, String> selectorLabels =
        Labels.selectorForComponent(hiveCluster, COMPONENT);

    List<EnvVar> envVars = new ArrayList<>();
    envVars.add(new EnvVar("SERVICE_NAME", COMPONENT, null));
    envVars.add(new EnvVar("IS_RESUME", "true", null));
    envVars.add(new EnvVar("LLAP_MEMORY_MB",
        String.valueOf(llap.memoryMb()), null));
    envVars.add(new EnvVar("LLAP_EXECUTORS",
        String.valueOf(llap.executors()), null));
    envVars.add(new EnvVar("HIVE_ZOOKEEPER_QUORUM",
        spec.zookeeper().quorum(), null));
    envVars.add(new EnvVar("HIVE_LLAP_DAEMON_SERVICE_HOSTS",
        llap.serviceHosts(), null));
    envVars.add(new EnvVar("LLAP_LOG4J2_PROPERTIES_FILE_NAME",
        "llap-daemon-log4j2.properties", null));

    // User-provided env vars (storage credentials, etc.)
    if (spec.envVars() != null) {
      envVars.addAll(spec.envVars());
    }

    List<ContainerPort> ports = new ArrayList<>();
    ports.add(new ContainerPortBuilder()
        .withName("management").withContainerPort(15004).withProtocol("TCP").build());
    ports.add(new ContainerPortBuilder()
        .withName("shuffle").withContainerPort(15551).withProtocol("TCP").build());
    ports.add(new ContainerPortBuilder()
        .withName("web").withContainerPort(15002).withProtocol("TCP").build());
    ports.add(new ContainerPortBuilder()
        .withName("output").withContainerPort(15003).withProtocol("TCP").build());

    Probe readinessProbe = buildTcpProbe(15004, llap.readinessProbe(), 15, 10, 3);

    String headlessServiceName =
        hiveCluster.getMetadata().getName() + "-llap";

    List<io.fabric8.kubernetes.api.model.VolumeMount> volumeMounts =
        new ArrayList<>();
    volumeMounts.add(new io.fabric8.kubernetes.api.model.VolumeMountBuilder()
        .withName("llap-config")
        .withMountPath(CONF_MOUNT_PATH).build());

    List<io.fabric8.kubernetes.api.model.Volume> volumes =
        new ArrayList<>();
    volumes.add(buildProjectedConfigVolume("llap-config",
        HiveConfigMapDependent.Llap.resourceName(hiveCluster),
        HiveConfigMapDependent.Hadoop.resourceName(hiveCluster)));

    List<Container> initContainers = new ArrayList<>();
    addExternalJars(spec.image(), spec.externalJars(),
        initContainers, volumeMounts, volumes, envVars);
    replaceConfMountWithSubPaths(volumeMounts, "llap-config",
        "llap-daemon-site.xml", "core-site.xml");

    // Add Prometheus JMX Exporter when autoscaling is enabled
    AutoscalingSpec autoscaling = llap.autoscaling();
    if (autoscaling.isEnabled()) {
      addJmxExporter(spec.image(), COMPONENT, autoscaling.metricsPort(),
          initContainers, volumeMounts, volumes, envVars, ports);
    }

    // Pre-compute config hash for the pod template annotation.
    String configHash = sha256(
        HadoopXmlBuilder.buildXml(HiveConfigBuilder.getLlapDaemonSite(spec)),
        HadoopXmlBuilder.buildXml(HiveConfigBuilder.getHadoopCoreSite(spec)));

    AutoscalingSpec llapAutoscaling = llap.autoscaling();
    int initialReplicas = llapAutoscaling != null && llapAutoscaling.isEnabled()
        ? llapAutoscaling.minReplicas() : llap.replicas();
    Integer replicas = resolveReplicaCount(
        hiveCluster, context, llapAutoscaling, llap.replicas(), initialReplicas);

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
                .withReadinessProbe(readinessProbe)
                .withResources(buildResources(llap.resources()))
                .withVolumeMounts(volumeMounts)
              .endContainer()
              .withVolumes(volumes)
            .endSpec()
          .endTemplate()
        .endSpec()
        .build();

    applySpreadAffinityIfAbsent(
        statefulSet.getSpec().getTemplate().getSpec(), selectorLabels);

    // Graceful scale-down: poll JMX Exporter (port 9404) until all executors idle.
    if (autoscaling.isEnabled()) {
      String preStopScript = buildDualMetricDrainScript(
          "Waiting for LLAP executors to become idle",
          "hadoop_llapdaemon_executornumexecutorsavailable{", "AVAILABLE",
          "hadoop_llapdaemon_executornumexecutors{", "TOTAL",
          "LLAP executor metrics not found. JMX Exporter may not be configured.",
          "All executors idle. Shutting down.",
          "Executors available=$AVAILABLE / total=$TOTAL \u2014 waiting...",
          10, 6);
      applyAutoscalingLifecycle(
          statefulSet.getSpec().getTemplate().getSpec(),
          statefulSet.getSpec().getTemplate().getMetadata(),
          preStopScript, autoscaling.gracePeriodSeconds(),
          autoscaling.metricsScrapeIntervalSeconds());
    }

    appendUserVolumes(statefulSet.getSpec().getTemplate().getSpec(),
        spec.volumes(), spec.volumeMounts(),
        llap.extraVolumes(), llap.extraVolumeMounts());

    return statefulSet;
  }

  /** Returns the StatefulSet resource name for this HiveCluster. */
  public static String resourceName(HiveCluster hiveCluster) {
    return hiveCluster.getMetadata().getName() + "-llap";
  }
}
