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
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.config.informer.Informer;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.HiveClusterSpec;
import org.apache.hive.kubernetes.operator.model.spec.AutoscalingSpec;
import org.apache.hive.kubernetes.operator.model.spec.HiveServer2Spec;
import org.apache.hive.kubernetes.operator.util.ConfigUtils;
import org.apache.hive.kubernetes.operator.util.HadoopXmlBuilder;
import org.apache.hive.kubernetes.operator.util.HiveConfigBuilder;
import org.apache.hive.kubernetes.operator.util.Labels;

/** Manages the Kubernetes Deployment for HiveServer2. */
@KubernetesDependent(
    informer = @Informer(labelSelector = "app.kubernetes.io/component=hiveserver2,"
        + "app.kubernetes.io/managed-by=hive-kubernetes-operator")
)
public class HiveServer2DeploymentDependent
    extends HiveDependentResource<Deployment, HiveCluster> {

  public static final String COMPONENT = "hiveserver2";
  private static final String SCRATCH_MOUNT_PATH = "/opt/hive/scratch";

  public HiveServer2DeploymentDependent() {
    super(Deployment.class);
  }

  @Override
  protected Deployment desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    HiveClusterSpec spec = hiveCluster.getSpec();
    HiveServer2Spec hs2 = spec.hiveServer2();
    Map<String, String> selectorLabels =
        Labels.selectorForComponent(hiveCluster, COMPONENT);

    List<EnvVar> envVars = new ArrayList<>();
    envVars.add(new EnvVar("SERVICE_NAME", "hiveserver2", null));
    envVars.add(new EnvVar("IS_RESUME", "true", null));
    envVars.add(new EnvVar("TEZ_AM_EXTERNAL_ID",
        "tez-session-hs2", null));

    // User-provided env vars (storage credentials, etc.)
    if (spec.envVars() != null) {
      envVars.addAll(spec.envVars());
    }

    // Env vars consumed by the Hive Docker entrypoint.sh to
    // configure Tez execution mode at container startup.
    if (spec.tezAm().isEnabled()) {
      envVars.add(new EnvVar("HIVE_SERVER2_TEZ_USE_EXTERNAL_SESSIONS",
          "true", null));
      envVars.add(new EnvVar("TEZ_FRAMEWORK_MODE",
          "STANDALONE_ZOOKEEPER", null));
      envVars.add(new EnvVar("HIVE_ZOOKEEPER_QUORUM",
          spec.zookeeper().quorum(), null));
    }

    if (spec.llap().isEnabled()) {
      envVars.add(new EnvVar("HIVE_LLAP_DAEMON_SERVICE_HOSTS",
          spec.llap().serviceHosts(), null));
    }

    int metastorePort = ConfigUtils.getInt(
        spec.metastore().configOverrides(),
        ConfigUtils.METASTORE_THRIFT_PORT_KEY,
        ConfigUtils.METASTORE_THRIFT_PORT_HIVE_KEY,
        ConfigUtils.METASTORE_THRIFT_PORT_DEFAULT);
    String metastoreUri = spec.metastore().isEnabled() ?
        "thrift://" + hiveCluster.getMetadata().getName()
            + "-metastore:" + metastorePort :
        spec.metastore().externalUri();
    StringBuilder serviceOpts = new StringBuilder();
    if (metastoreUri != null && !metastoreUri.isEmpty()) {
      serviceOpts.append("-D")
          .append(ConfigUtils.HIVE_METASTORE_URIS_KEY)
          .append("=").append(metastoreUri);
    }
    if (spec.llap().isEnabled()) {
      serviceOpts.append(" -D")
          .append(ConfigUtils.HIVE_EXECUTION_MODE_KEY)
          .append("=llap");
      serviceOpts.append(" -D")
          .append(ConfigUtils.HIVE_LLAP_DAEMON_SERVICE_HOSTS_KEY)
          .append("=").append(spec.llap().serviceHosts());
    }
    if (spec.tezAm().isEnabled()) {
      serviceOpts.append(" -D")
          .append(ConfigUtils.HIVE_ZOOKEEPER_QUORUM_KEY)
          .append("=").append(spec.zookeeper().quorum());
    }
    envVars.add(new EnvVar("SERVICE_OPTS",
        serviceOpts.toString(), null));

    int hs2ThriftPort = ConfigUtils.getInt(
        hs2.configOverrides(),
        ConfigUtils.HIVE_SERVER2_THRIFT_PORT_KEY,
        null, ConfigUtils.HIVE_SERVER2_THRIFT_PORT_DEFAULT);
    int hs2HttpPort = ConfigUtils.getInt(
        hs2.configOverrides(),
        ConfigUtils.HIVE_SERVER2_THRIFT_HTTP_PORT_KEY,
        null, ConfigUtils.HIVE_SERVER2_THRIFT_HTTP_PORT_DEFAULT);
    int hs2WebUiPort = ConfigUtils.getInt(
        hs2.configOverrides(),
        ConfigUtils.HIVE_SERVER2_WEBUI_PORT_KEY,
        null, ConfigUtils.HIVE_SERVER2_WEBUI_PORT_DEFAULT);
    List<ContainerPort> ports = new ArrayList<>();
    ports.add(new ContainerPortBuilder()
        .withName("thrift")
        .withContainerPort(hs2ThriftPort).build());
    ports.add(new ContainerPortBuilder()
        .withName("http")
        .withContainerPort(hs2HttpPort).build());
    ports.add(new ContainerPortBuilder()
        .withName("webui")
        .withContainerPort(hs2WebUiPort).build());

    // Probes target the HTTP transport port (default mode)
    Probe readinessProbe = buildTcpProbe(hs2HttpPort, hs2.readinessProbe(), 15, 10, 3);
    Probe livenessProbe = buildTcpProbe(hs2HttpPort, hs2.livenessProbe(), 120, 30, 10);

    boolean tezAmEnabled = spec.tezAm().isEnabled();

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
    volumes.add(buildProjectedConfigVolume("hive-config",
        HiveServer2ConfigMapDependent.resourceName(hiveCluster),
        HadoopConfigMapDependent.resourceName(hiveCluster)));

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

    List<Container> initContainers = new ArrayList<>();
    List<String> allJars = new ArrayList<>();
    if (spec.externalJars() != null) {
      allJars.addAll(spec.externalJars());
    }
    if (hs2.externalJars() != null) {
      allJars.addAll(hs2.externalJars());
    }
    addExternalJars(spec.image(), allJars,
        initContainers, volumeMounts, volumes, envVars);
    replaceConfMountWithSubPaths(volumeMounts, "hive-config",
        "hive-site.xml", "tez-site.xml", "core-site.xml");

    // Add Prometheus JMX Exporter when autoscaling is enabled
    AutoscalingSpec autoscaling = hs2.autoscaling();
    if (autoscaling.isEnabled()) {
      addJmxExporter(spec.image(), COMPONENT,
          initContainers, volumeMounts, volumes, envVars, ports);
    }

    // Pre-compute config hash for the pod template annotation.
    // This ensures the Deployment is created with the correct hash
    // from the start (single ReplicaSet) and triggers rolling
    // updates when ConfigMap content changes.
    String configHash = sha256(
        HadoopXmlBuilder.buildXml(HiveConfigBuilder.getHiveServer2HiveSite(hiveCluster, spec)),
        HadoopXmlBuilder.buildXml(HiveConfigBuilder.getTezSite(spec)),
        HadoopXmlBuilder.buildXml(HiveConfigBuilder.getHadoopCoreSite(spec)));

    // When autoscaling is enabled and the Deployment already exists, preserve the current
    // replica count (managed by KEDA/HPA). On initial creation:
    // - minReplicas == 0 (scale-to-zero): start at 0, KEDA HTTPScaledObject handles wake-up
    // - minReplicas > 0: start at configured replicas
    boolean autoscalingEnabled = hs2.autoscaling() != null && hs2.autoscaling().isEnabled();
    Integer replicas = hs2.replicas();
    if (autoscalingEnabled) {
      int initialReplicas = hs2.autoscaling().minReplicas() == 0 ? 0 : hs2.replicas();
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
              .addToAnnotations("kubectl.kubernetes.io/default-container", "hiveserver2")
              .addToAnnotations("hive.apache.org/config-hash", configHash)
            .endMetadata()
            .withNewSpec()
              .withInitContainers(initContainers)
              .addNewContainer()
                .withName("hiveserver2")
                .withImage(spec.image())
                .withImagePullPolicy(spec.imagePullPolicy())
                .withEnv(envVars)
                .withPorts(ports)
                .withReadinessProbe(readinessProbe)
                .withLivenessProbe(livenessProbe)
                .withResources(buildResources(hs2.resources()))
                .withVolumeMounts(volumeMounts)
              .endContainer()
              .withVolumes(volumes)
            .endSpec()
          .endTemplate()
        .endSpec()
        .build();

    applySpreadAffinityIfAbsent(
        deployment.getSpec().getTemplate().getSpec(), selectorLabels);

    // Graceful scale-down: deregister from ZK, then poll JMX Exporter (port 9404) for sessions.
    // Uses flat Prometheus text format — same metric KEDA reads — not brittle JSON parsing.
    if (autoscaling.isEnabled()) {
      String preStopScript = String.join("\n",
          "#!/bin/bash",
          "echo '[preStop] Deregistering HiveServer2 from ZooKeeper...'",
          "hive --service hiveserver2 --deregister || echo '[preStop] WARNING: ZK deregister failed'",
          "echo '[preStop] Waiting for open sessions to drain (polling localhost:9404/metrics)...'",
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
          "  SESSIONS=$(echo \"$RESPONSE\" | grep '^hs2_open_sessions ' | awk '{print $2}')",
          "  if [ -z \"$SESSIONS\" ]; then",
          "    echo '[preStop] WARNING: hs2_open_sessions metric not found. JMX Exporter may not be configured.'",
          "    break",
          "  fi",
          "  if [ \"${SESSIONS%.*}\" -le 0 ] 2>/dev/null; then",
          "    echo '[preStop] All sessions drained. Shutting down.'",
          "    break",
          "  fi",
          "  echo \"[preStop] hs2_open_sessions=$SESSIONS — waiting...\"",
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
    if (hs2.extraVolumes() != null) {
      deployment.getSpec().getTemplate().getSpec().getVolumes().addAll(hs2.extraVolumes());
    }
    if (hs2.extraVolumeMounts() != null) {
      deployment.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts()
          .addAll(hs2.extraVolumeMounts());
    }

    return deployment;
  }

  /** Returns the Deployment resource name for this HiveCluster. */
  public static String resourceName(HiveCluster hiveCluster) {
    return hiveCluster.getMetadata().getName() + "-hiveserver2";
  }
}
