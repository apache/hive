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
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.config.informer.Informer;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.HiveClusterSpec;
import org.apache.hive.kubernetes.operator.model.spec.AutoscalingSpec;
import org.apache.hive.kubernetes.operator.model.spec.DatabaseConfig;
import org.apache.hive.kubernetes.operator.util.ConfigUtils;
import org.apache.hive.kubernetes.operator.util.HadoopXmlBuilder;
import org.apache.hive.kubernetes.operator.util.HiveConfigBuilder;
import org.apache.hive.kubernetes.operator.util.Labels;

/** Manages the Kubernetes Deployment for the Hive Metastore. */
@KubernetesDependent(
    informer = @Informer(labelSelector = "app.kubernetes.io/component=metastore,"
        + "app.kubernetes.io/managed-by=hive-kubernetes-operator")
)
public class MetastoreDeploymentDependent
    extends HiveDependentResource<Deployment, HiveCluster> {

  public static final String COMPONENT = "metastore";

  public MetastoreDeploymentDependent() {
    super(Deployment.class);
  }

  @Override
  protected String getSecondaryResourceName(HiveCluster primary,
      Context<HiveCluster> context) {
    return resourceName(primary);
  }

  @Override
  protected Deployment desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    HiveClusterSpec spec = hiveCluster.getSpec();
    DatabaseConfig db = spec.metastore().database();
    Map<String, String> selectorLabels =
        Labels.selectorForComponent(hiveCluster, COMPONENT);

    List<EnvVar> envVars = new ArrayList<>();
    envVars.add(new EnvVar("SERVICE_NAME", "metastore", null));
    envVars.add(new EnvVar("IS_RESUME", "true", null));
    envVars.addAll(buildDbEnvVars(db));
    if (spec.envVars() != null) {
      envVars.addAll(spec.envVars());
    }

    int thriftPort = ConfigUtils.getInt(
        spec.metastore().configOverrides(),
        ConfigUtils.METASTORE_THRIFT_PORT_KEY,
        ConfigUtils.METASTORE_THRIFT_PORT_HIVE_KEY,
        ConfigUtils.METASTORE_THRIFT_PORT_DEFAULT);
    List<ContainerPort> ports = new ArrayList<>();
    ports.add(new ContainerPortBuilder()
        .withName("thrift").withContainerPort(thriftPort).withProtocol("TCP").build());
    ports.add(new ContainerPortBuilder()
        .withName("rest").withContainerPort(9001).withProtocol("TCP").build());

    Probe readinessProbe = buildTcpProbe(thriftPort, spec.metastore().readinessProbe(), 15, 10, 3);
    Probe livenessProbe = buildTcpProbe(thriftPort, spec.metastore().livenessProbe(), 60, 30, 5);

    List<Container> initContainers = new ArrayList<>();
    List<VolumeMount> volumeMounts = new ArrayList<>();
    List<Volume> volumes = new ArrayList<>();
    buildMetastoreVolumes(hiveCluster, volumeMounts, volumes);

    // Merge JDBC driver JAR with global externalJars into one list
    List<String> allJars = new ArrayList<>();
    if (db.driverJarUrl() != null) {
      allJars.add(db.driverJarUrl());
    }
    if (spec.externalJars() != null) {
      allJars.addAll(spec.externalJars());
    }
    addExternalJars(spec.image(), allJars,
        initContainers, volumeMounts, volumes, envVars);
    // Replace directory mount with subPath mounts to avoid
    // broken symlinks from K8s ConfigMap rotation.
    replaceConfMountWithSubPaths(volumeMounts, "hive-config",
        "metastore-site.xml", "core-site.xml");

    // Add Prometheus JMX Exporter when autoscaling is enabled
    AutoscalingSpec autoscaling = spec.metastore().autoscaling();
    if (autoscaling.isEnabled()) {
      addJmxExporter(spec.image(), COMPONENT,
          initContainers, volumeMounts, volumes, envVars, ports);
    }

    // Pre-compute config hash for the pod template annotation.
    // This ensures the Deployment is created with the correct hash
    // from the start (single ReplicaSet) and triggers rolling
    // updates when ConfigMap content changes.
    String configHash = sha256(
        HadoopXmlBuilder.buildXml(HiveConfigBuilder.getMetastoreSite(spec)),
        HadoopXmlBuilder.buildXml(HiveConfigBuilder.getHadoopCoreSite(spec)));

    // When autoscaling is enabled, preserve current replica count (KEDA/HPA manages it).
    AutoscalingSpec msAutoscaling = spec.metastore().autoscaling();
    int initialReplicas = msAutoscaling != null
        ? Math.max(1, msAutoscaling.minReplicas()) : spec.metastore().replicas();
    Integer replicas = resolveReplicaCount(
        hiveCluster, context, msAutoscaling, spec.metastore().replicas(), initialReplicas);

    Deployment deployment = new DeploymentBuilder()
        .withNewMetadata()
          .withName(resourceName(hiveCluster))
          .withNamespace(hiveCluster.getMetadata().getNamespace())
          .withLabels(Labels.forComponent(hiveCluster, COMPONENT))
        .endMetadata()
        .withNewSpec()
          .withReplicas(replicas)
          .withNewSelector()
            .withMatchLabels(selectorLabels)
          .endSelector()
          .withNewTemplate()
            .withNewMetadata()
              .withLabels(Labels.forComponent(hiveCluster, COMPONENT))
              .addToAnnotations("kubectl.kubernetes.io/default-container", "metastore")
              .addToAnnotations("hive.apache.org/config-hash", configHash)
            .endMetadata()
            .withNewSpec()
              .withInitContainers(initContainers)
              .addNewContainer()
                .withName("metastore")
                .withImage(spec.image())
                .withImagePullPolicy(spec.imagePullPolicy())
                .withEnv(envVars)
                .withPorts(ports)
                .withReadinessProbe(readinessProbe)
                .withLivenessProbe(livenessProbe)
                .withResources(buildResources(
                    spec.metastore().resources()))
                .withVolumeMounts(volumeMounts)
              .endContainer()
              .withVolumes(volumes)
            .endSpec()
          .endTemplate()
        .endSpec()
        .build();

    applySpreadAffinityIfAbsent(
        deployment.getSpec().getTemplate().getSpec(), selectorLabels);

    // HMS uses HTTP transport mode — connections are stateless, so no session
    // drain is needed. The preStop hook simply sends SIGTERM directly to the
    // JVM (the shell entrypoint doesn't forward signals from K8s).
    if (autoscaling.isEnabled()) {
      String preStopScript = String.join("\n",
          "#!/bin/bash",
          "echo '[preStop] Sending SIGTERM to Metastore Java process...'",
          "kill $(pgrep -f 'java.*org.apache') 2>/dev/null",
          "exit 0");
      applyAutoscalingLifecycle(
          deployment.getSpec().getTemplate().getSpec(),
          deployment.getSpec().getTemplate().getMetadata(),
          preStopScript, autoscaling.gracePeriodSeconds(),
          autoscaling.metricsScrapeIntervalSeconds());
    }

    appendUserVolumes(deployment.getSpec().getTemplate().getSpec(),
        spec.volumes(), spec.volumeMounts(),
        spec.metastore().extraVolumes(), spec.metastore().extraVolumeMounts());
    return deployment;
  }

  /** Returns the Deployment resource name for this HiveCluster. */
  public static String resourceName(HiveCluster hiveCluster) {
    return hiveCluster.getMetadata().getName() + "-metastore";
  }
}
