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

  public static final String COMPONENT = ConfigUtils.COMPONENT_HIVESERVER2;
  private static final String SCRATCH_MOUNT_PATH = "/opt/hive/scratch";

  public HiveServer2DeploymentDependent() {
    super(Deployment.class);
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
  protected Deployment desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    HiveClusterSpec spec = hiveCluster.getSpec();
    HiveServer2Spec hs2 = spec.hiveServer2();
    Map<String, String> selectorLabels =
        Labels.selectorForComponent(hiveCluster, COMPONENT);

    List<EnvVar> envVars = new ArrayList<>();
    envVars.add(new EnvVar("SERVICE_NAME", COMPONENT, null));
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

    spec.llapClusters().stream()
        .filter(l -> l.isEnabled())
        .findFirst()
        .ifPresent(llap -> envVars.add(new EnvVar("HIVE_LLAP_DAEMON_SERVICE_HOSTS",
            llap.serviceHosts(), null)));

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
    spec.llapClusters().stream()
        .filter(l -> l.isEnabled())
        .findFirst()
        .ifPresent(llap -> {
          serviceOpts.append(" -D")
              .append(ConfigUtils.HIVE_EXECUTION_MODE_KEY)
              .append("=llap");
          serviceOpts.append(" -D")
              .append(ConfigUtils.HIVE_LLAP_DAEMON_SERVICE_HOSTS_KEY)
              .append("=").append(llap.serviceHosts());
        });
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
        .withContainerPort(hs2ThriftPort).withProtocol("TCP").build());
    ports.add(new ContainerPortBuilder()
        .withName("http")
        .withContainerPort(hs2HttpPort).withProtocol("TCP").build());
    ports.add(new ContainerPortBuilder()
        .withName("webui")
        .withContainerPort(hs2WebUiPort).withProtocol("TCP").build());

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
        HiveConfigMapDependent.HiveServer2.resourceName(hiveCluster),
        HiveConfigMapDependent.Hadoop.resourceName(hiveCluster)));

    if (tezAmEnabled) {
      volumeMounts.add(
          new io.fabric8.kubernetes.api.model.VolumeMountBuilder()
              .withName("scratch")
              .withMountPath(SCRATCH_MOUNT_PATH).build());
      volumes.add(new io.fabric8.kubernetes.api.model.VolumeBuilder()
          .withName("scratch")
          .withNewPersistentVolumeClaim()
            .withClaimName(ScratchPvcDependent.resourceName(hiveCluster))
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

    // Add Prometheus JMX Exporter when HS2 autoscaling is enabled, or when
    // LLAP/TezAM autoscaling needs the HS2 activation gate metric.
    AutoscalingSpec autoscaling = hs2.autoscaling();
    boolean llapOrTezAmAutoscales = spec.llapClusters().stream().anyMatch(
        l -> l.isEnabled() && (l.autoscaling().isEnabled()
            || (spec.tezAm().isEnabled() && l.tezAm().autoscaling().isEnabled())));
    if (autoscaling.isEnabled() || llapOrTezAmAutoscales) {
      addJmxExporter(spec.image(), COMPONENT, autoscaling.metricsPort(),
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

    AutoscalingSpec hs2Autoscaling = hs2.autoscaling();
    int initialReplicas = hs2Autoscaling != null && hs2Autoscaling.isEnabled()
        ? Math.max(1, hs2Autoscaling.minReplicas()) : hs2.replicas();
    Integer replicas = resolveReplicaCount(
        hiveCluster, context, hs2Autoscaling, hs2.replicas(), initialReplicas);

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
              .addToAnnotations("kubectl.kubernetes.io/default-container", COMPONENT)
              .addToAnnotations("hive.apache.org/config-hash", configHash)
            .endMetadata()
            .withNewSpec()
              .withServiceAccountName(spec.serviceAccountName())
              .withInitContainers(initContainers)
              .addNewContainer()
                .withName(COMPONENT)
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

    // Graceful scale-down: deregister from ZK, then poll JMX Exporter for sessions.
    if (autoscaling.isEnabled()) {
      List<String> zkDeregister = List.of(
          "echo '[preStop] Deregistering HiveServer2 from ZooKeeper...'",
          "hive --service hiveserver2 --deregister $(hive --service version 2>/dev/null | head -1 || echo '4.0.0')"
              + " || echo '[preStop] WARNING: ZK deregister failed'");
      String preStopScript = buildDrainScript(
          "Waiting for open sessions to drain",
          "hs2_open_sessions", "SESSIONS",
          "All sessions drained. Shutting down.",
          5, 6, zkDeregister, autoscaling.metricsPort());
      applyAutoscalingLifecycle(
          deployment.getSpec().getTemplate().getSpec(),
          deployment.getSpec().getTemplate().getMetadata(),
          preStopScript, autoscaling.gracePeriodSeconds(),
          autoscaling.metricsScrapeIntervalSeconds());
    }

    appendUserVolumes(deployment.getSpec().getTemplate().getSpec(),
        spec.volumes(), spec.volumeMounts(),
        hs2.extraVolumes(), hs2.extraVolumeMounts());

    return deployment;
  }

  /** Returns the Deployment resource name for this HiveCluster. */
  public static String resourceName(HiveCluster hiveCluster) {
    return hiveCluster.getMetadata().getName() + "-hiveserver2";
  }
}
