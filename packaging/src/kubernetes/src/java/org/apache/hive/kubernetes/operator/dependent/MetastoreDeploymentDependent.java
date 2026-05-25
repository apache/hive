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
import io.fabric8.kubernetes.api.model.Lifecycle;
import io.fabric8.kubernetes.api.model.LifecycleBuilder;
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
        .withName("thrift").withContainerPort(thriftPort).build());
    ports.add(new ContainerPortBuilder()
        .withName("rest").withContainerPort(9001).build());

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

    // When autoscaling is enabled and the Deployment already exists, preserve the current
    // replica count (managed by KEDA/HPA). On initial creation, start at minReplicas
    // and let KEDA scale up based on load.
    boolean autoscalingEnabled = spec.metastore().autoscaling() != null
        && spec.metastore().autoscaling().isEnabled();
    Integer replicas = spec.metastore().replicas();
    if (autoscalingEnabled) {
      int initialReplicas = Math.max(1, spec.metastore().autoscaling().minReplicas());
      replicas = getSecondaryResource(hiveCluster, context)
          .map(d -> d.getSpec().getReplicas())
          .orElse(initialReplicas);
    }

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

    // Graceful scale-down: poll JMX Exporter (port 9404) for open_connections to drain.
    // K8s removes the pod from Service Endpoints on termination, so no new requests arrive.
    // Uses flat Prometheus text format — same metric KEDA reads — not brittle JSON parsing.
    if (autoscaling.isEnabled()) {
      String preStopScript = String.join("\n",
          "#!/bin/bash",
          "echo '[preStop] Waiting for open connections to drain (polling localhost:9404/metrics)...'",
          "RETRIES=0",
          "while true; do",
          "  RESPONSE=$(curl -sf http://localhost:9404/metrics)",
          "  if [ $? -ne 0 ]; then",
          "    RETRIES=$((RETRIES+1))",
          "    echo \"[preStop] ERROR: JMX Exporter unreachable on port 9404 (attempt $RETRIES)\"",
          "    if [ $RETRIES -ge 6 ]; then",
          "      echo '[preStop] JMX Exporter not responding after 30s. Proceeding with shutdown.'",
          "      break",
          "    fi",
          "    sleep 5; continue",
          "  fi",
          "  CONNS=$(echo \"$RESPONSE\" | grep '^hive_metastore_open_connections ' | awk '{print $2}')",
          "  if [ -z \"$CONNS\" ]; then",
          "    echo '[preStop] WARNING: hive_metastore_open_connections metric not found. JMX Exporter may not be configured.'",
          "    break",
          "  fi",
          "  if [ \"${CONNS%.*}\" -le 0 ] 2>/dev/null; then",
          "    echo '[preStop] All connections drained. Shutting down.'",
          "    break",
          "  fi",
          "  echo \"[preStop] hive_metastore_open_connections=$CONNS — waiting...\"",
          "  RETRIES=0",
          "  sleep 5",
          "done");
      Lifecycle lifecycle = new LifecycleBuilder()
          .withNewPreStop()
            .withNewExec()
              .withCommand("/bin/bash", "-c", preStopScript)
            .endExec()
          .endPreStop()
          .build();
      deployment.getSpec().getTemplate().getSpec().getContainers().get(0).setLifecycle(lifecycle);
      deployment.getSpec().getTemplate().getSpec()
          .setTerminationGracePeriodSeconds((long) autoscaling.gracePeriodSeconds());
      // Prometheus scrape annotations for JMX Exporter metrics endpoint
      deployment.getSpec().getTemplate().getMetadata().getAnnotations()
          .put("prometheus.io/scrape", "true");
      deployment.getSpec().getTemplate().getMetadata().getAnnotations()
          .put("prometheus.io/port", String.valueOf(ConfigUtils.PROMETHEUS_JMX_EXPORTER_PORT));
      deployment.getSpec().getTemplate().getMetadata().getAnnotations()
          .put("prometheus.io/path", "/metrics");
    }

    if (spec.volumes() != null) {
      deployment.getSpec().getTemplate().getSpec().getVolumes().addAll(spec.volumes());
    }
    if (spec.volumeMounts() != null) {
      deployment.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts()
          .addAll(spec.volumeMounts());
    }
    if (spec.metastore().extraVolumes() != null) {
      deployment.getSpec().getTemplate().getSpec().getVolumes().addAll(spec.metastore().extraVolumes());
    }
    if (spec.metastore().extraVolumeMounts() != null) {
      deployment.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts()
          .addAll(spec.metastore().extraVolumeMounts());
    }
    return deployment;
  }

  /** Returns the Deployment resource name for this HiveCluster. */
  public static String resourceName(HiveCluster hiveCluster) {
    return hiveCluster.getMetadata().getName() + "-metastore";
  }
}
