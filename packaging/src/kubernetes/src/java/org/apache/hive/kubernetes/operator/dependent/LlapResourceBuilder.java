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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.discovery.v1.Endpoint;
import io.fabric8.kubernetes.api.model.discovery.v1.EndpointBuilder;
import io.fabric8.kubernetes.api.model.discovery.v1.EndpointSlice;
import io.fabric8.kubernetes.api.model.discovery.v1.EndpointSliceBuilder;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudgetBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.apache.commons.lang3.StringUtils;
import org.apache.hive.kubernetes.operator.autoscaling.TezAmScalingStrategy;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.HiveClusterSpec;
import org.apache.hive.kubernetes.operator.model.spec.AutoscalingSpec;
import org.apache.hive.kubernetes.operator.model.spec.LlapSpec;
import org.apache.hive.kubernetes.operator.util.ConfigUtils;
import org.apache.hive.kubernetes.operator.util.HadoopXmlBuilder;
import org.apache.hive.kubernetes.operator.util.HiveConfigBuilder;
import org.apache.hive.kubernetes.operator.util.Labels;

import static org.apache.hive.kubernetes.operator.autoscaling.MetricsScraper.isPodReady;

/**
 * Static builder methods for LLAP Kubernetes resources.
 * Used by the reconciler to imperatively manage multiple LLAP clusters.
 * <p>
 * Extends {@link HiveDependentResource} solely to access protected helper methods
 * (buildTcpProbe, addExternalJars, addJmxExporter, etc.).
 */
public class LlapResourceBuilder
    extends HiveDependentResource<StatefulSet, HiveCluster> {

  private static final LlapResourceBuilder INSTANCE = new LlapResourceBuilder();
  private static final String TEZAM_INFIX = "-tezam-";
  private static final String HIVE_CONFIG_VOLUME = "hive-config";
  private static final String LLAP_CONFIG_VOLUME = "llap-config";

  LlapResourceBuilder() {
    super(StatefulSet.class);
  }

  @Override
  protected String getSecondaryResourceName(HiveCluster primary,
      Context<HiveCluster> context) {
    throw new UnsupportedOperationException("LlapResourceBuilder is not a managed dependent");
  }

  @Override
  protected StatefulSet desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    throw new UnsupportedOperationException("LlapResourceBuilder is not a managed dependent");
  }

  /** Creates an OwnerReference pointing to the HiveCluster CR for garbage collection. */
  private static OwnerReference ownerRef(HiveCluster hc) {
    return new OwnerReferenceBuilder()
        .withApiVersion(hc.getApiVersion())
        .withKind(hc.getKind())
        .withName(hc.getMetadata().getName())
        .withUid(hc.getMetadata().getUid())
        .withController(true)
        .withBlockOwnerDeletion(true)
        .build();
  }

  /** Resource name for a specific LLAP cluster: {clusterName}-{llapName}. */
  public static String resourceName(HiveCluster hc, LlapSpec llap) {
    return hc.getMetadata().getName() + "-" + llap.name();
  }

  /** ConfigMap name for a specific LLAP cluster. */
  public static String configMapName(HiveCluster hc, LlapSpec llap) {
    return hc.getMetadata().getName() + "-" + llap.name() + "-config";
  }

  /** PDB name for a specific LLAP cluster. */
  public static String pdbName(HiveCluster hc, LlapSpec llap) {
    return hc.getMetadata().getName() + "-" + llap.name() + "-pdb";
  }

  /** Builds the StatefulSet for a specific LLAP cluster. */
  public static StatefulSet buildStatefulSet(HiveCluster hc, LlapSpec llap, Integer replicas) {
    return INSTANCE.doBuildStatefulSet(hc, llap, replicas);
  }

  /** Builds the headless Service for a specific LLAP cluster. */
  public static Service buildService(HiveCluster hc, LlapSpec llap) {
    String ns = hc.getMetadata().getNamespace();
    String name = resourceName(hc, llap);
    Map<String, String> labels = Labels.forLlapCluster(hc, llap.name());
    Map<String, String> selector = Labels.selectorForLlapCluster(hc, llap.name());

    int managementPort = ConfigUtils.getInt(llap.configOverrides(),
        ConfigUtils.HIVE_LLAP_MANAGEMENT_RPC_PORT_KEY, null,
        ConfigUtils.HIVE_LLAP_MANAGEMENT_RPC_PORT_DEFAULT);
    int shufflePort = ConfigUtils.getInt(llap.configOverrides(),
        ConfigUtils.HIVE_LLAP_DAEMON_SHUFFLE_PORT_KEY, null,
        ConfigUtils.HIVE_LLAP_DAEMON_SHUFFLE_PORT_DEFAULT);
    int webPort = ConfigUtils.getInt(llap.configOverrides(),
        ConfigUtils.HIVE_LLAP_DAEMON_WEB_PORT_KEY, null,
        ConfigUtils.HIVE_LLAP_DAEMON_WEB_PORT_DEFAULT);

    return new ServiceBuilder()
        .withNewMetadata()
          .withName(name)
          .withNamespace(ns)
          .withLabels(labels)
          .withOwnerReferences(ownerRef(hc))
        .endMetadata()
        .withNewSpec()
          .withClusterIP("None")
          .withSelector(selector)
          .addNewPort().withName("management").withProtocol("TCP")
            .withPort(managementPort)
            .withTargetPort(new IntOrString(managementPort)).endPort()
          .addNewPort().withName("shuffle").withProtocol("TCP")
            .withPort(shufflePort)
            .withTargetPort(new IntOrString(shufflePort)).endPort()
          .addNewPort().withName("web").withProtocol("TCP")
            .withPort(webPort)
            .withTargetPort(new IntOrString(webPort)).endPort()
        .endSpec()
        .build();
  }

  /** Builds the ConfigMap for a specific LLAP cluster. */
  public static ConfigMap buildConfigMap(HiveCluster hc, LlapSpec llap) {
    HiveClusterSpec spec = hc.getSpec();
    Map<String, String> labels = Labels.forLlapCluster(hc, llap.name());
    Map<String, String> llapDaemonSite = HiveConfigBuilder.getLlapDaemonSite(spec, llap);

    return new ConfigMapBuilder()
        .withNewMetadata()
          .withName(configMapName(hc, llap))
          .withNamespace(hc.getMetadata().getNamespace())
          .withLabels(labels)
          .withOwnerReferences(ownerRef(hc))
        .endMetadata()
        .addToData("llap-daemon-site.xml", HadoopXmlBuilder.buildXml(llapDaemonSite))
        .build();
  }

  /** Builds the PodDisruptionBudget for a specific LLAP cluster. */
  public static PodDisruptionBudget buildPdb(HiveCluster hc, LlapSpec llap) {
    Map<String, String> labels = Labels.forLlapCluster(hc, llap.name());
    Map<String, String> selector = Labels.selectorForLlapCluster(hc, llap.name());

    return new PodDisruptionBudgetBuilder()
        .withNewMetadata()
          .withName(pdbName(hc, llap))
          .withNamespace(hc.getMetadata().getNamespace())
          .withLabels(labels)
          .withOwnerReferences(ownerRef(hc))
        .endMetadata()
        .withNewSpec()
          .withMaxUnavailable(new IntOrString(1))
          .withNewSelector()
            .withMatchLabels(selector)
          .endSelector()
        .endSpec()
        .build();
  }

  // --- TezAM resource builders (one TezAM per LLAP cluster) ---

  /** TezAM Deployment/Service name for a specific LLAP cluster. */
  public static String tezAmResourceName(HiveCluster hc, LlapSpec llap) {
    return hc.getMetadata().getName() + TEZAM_INFIX + llap.name();
  }

  /** TezAM ConfigMap name for a specific LLAP cluster. */
  public static String tezAmConfigMapName(HiveCluster hc, LlapSpec llap) {
    return hc.getMetadata().getName() + TEZAM_INFIX + llap.name() + "-config";
  }

  /** TezAM PDB name for a specific LLAP cluster. */
  public static String tezAmPdbName(HiveCluster hc, LlapSpec llap) {
    return hc.getMetadata().getName() + TEZAM_INFIX + llap.name() + "-pdb";
  }

  /** Builds the PodDisruptionBudget for a per-LLAP-cluster TezAM. */
  public static PodDisruptionBudget buildTezAmPdb(HiveCluster hc, LlapSpec llap) {
    Map<String, String> labels = Labels.forTezAmCluster(hc, llap.name());
    Map<String, String> selector = Labels.selectorForTezAmCluster(hc, llap.name());

    return new PodDisruptionBudgetBuilder()
        .withNewMetadata()
          .withName(tezAmPdbName(hc, llap))
          .withNamespace(hc.getMetadata().getNamespace())
          .withLabels(labels)
          .withOwnerReferences(ownerRef(hc))
        .endMetadata()
        .withNewSpec()
          .withMaxUnavailable(new IntOrString(1))
          .withNewSelector()
            .withMatchLabels(selector)
          .endSelector()
        .endSpec()
        .build();
  }

  /** Builds the TezAM Deployment for a specific LLAP cluster. */
  public static Deployment buildTezAmDeployment(HiveCluster hc, LlapSpec llap, Integer replicas) {
    return INSTANCE.doBuildTezAmDeployment(hc, llap, replicas);
  }

  /**
   * Name for the operator-managed EndpointSlice that provides per-pod DNS for TezAM.
   * CoreDNS creates {@code <pod-name>.<svc>.<ns>.svc.cluster.local} A-records using it.
   */
  public static String tezAmEndpointSliceName(HiveCluster hc, LlapSpec llap) {
    return tezAmResourceName(hc, llap) + "-hostnames";
  }

  /**
   * Builds a custom EndpointSlice for the TezAM headless Service.
   * <p>
   * Kubernetes only creates per-pod DNS records ({@code <pod>.<svc>.<ns>.svc.cluster.local})
   * when the Endpoints/EndpointSlice has a {@code hostname} field for each address. The
   * default EndpointSlice controller omits {@code hostname} for Deployment pods (it only
   * sets it automatically for StatefulSet pods). This operator-managed EndpointSlice fills
   * that gap, giving every ready TezAM pod a resolvable FQDN.
   *
   * @param pods list of TezAM pods
   * @return the EndpointSlice, or {@code null} if there are no pod IPs yet or if pods have mixed IPv4/IPv6 addresses
   */
  public static EndpointSlice buildTezAmEndpointSlice(HiveCluster hc, LlapSpec llap, List<Pod> pods) {
    String ns = hc.getMetadata().getNamespace();
    String svcName = tezAmResourceName(hc, llap);
    Map<String, String> labels = new HashMap<>(Map.of(
        "kubernetes.io/service-name", svcName,
        "endpointslice.kubernetes.io/managed-by", "hive-kubernetes-operator",
        Labels.MANAGED_BY, Labels.MANAGED_BY_VALUE,
        Labels.APP_INSTANCE, hc.getMetadata().getName(),
        Labels.APP_COMPONENT, ConfigUtils.COMPONENT_TEZAM));

    List<Endpoint> endpoints = new ArrayList<>();
    String addressType = null;
    for (var pod : pods) {
      String ip = pod.getStatus() != null ? pod.getStatus().getPodIP() : null;
      if (ip == null || ip.isEmpty()) {
        continue;
      }
      String ipFamily = ipAddressType(ip);
      if (addressType == null) {
        addressType = ipFamily;
      } else if (!addressType.equals(ipFamily)) {
        return null;
      }
      boolean ready = isPodReady(pod);
      String hostname = pod.getMetadata().getName();
      if (hostname.length() > 63) {
        hostname = StringUtils.stripEnd(hostname.substring(0, 63), "-");
      }
      endpoints.add(new EndpointBuilder()
          .withHostname(hostname)
          .withAddresses(ip)
          .withNewConditions()
            .withReady(ready)
            .withServing(ready)
            .withTerminating(false)
          .endConditions()
          .withNewTargetRef()
            .withKind("Pod")
            .withNamespace(ns)
            .withName(pod.getMetadata().getName())
          .endTargetRef()
          .build());
    }

    if (endpoints.isEmpty()) {
      return null;
    }

    return new EndpointSliceBuilder()
        .withNewMetadata()
          .withName(tezAmEndpointSliceName(hc, llap))
          .withNamespace(ns)
          .withLabels(labels)
          .withOwnerReferences(ownerRef(hc))
        .endMetadata()
        .withAddressType(addressType)
        .withEndpoints(endpoints)
        .build();
  }

  /** Returns {@code IPv4} or {@code IPv6} for a pod IP string. */
  private static String ipAddressType(String ip) {
    return ip.indexOf(':') >= 0 ? "IPv6" : "IPv4";
  }

  /** Builds the headless Service for a TezAM cluster. */
  public static Service buildTezAmService(HiveCluster hc, LlapSpec llap) {
    String ns = hc.getMetadata().getNamespace();
    String name = tezAmResourceName(hc, llap);
    Map<String, String> labels = Labels.forTezAmCluster(hc, llap.name());
    Map<String, String> selector = Labels.selectorForTezAmCluster(hc, llap.name());

    return new ServiceBuilder()
        .withNewMetadata()
          .withName(name)
          .withNamespace(ns)
          .withLabels(labels)
          .withOwnerReferences(ownerRef(hc))
        .endMetadata()
        .withNewSpec()
          .withClusterIP("None")
          .withSelector(selector)
        .endSpec()
        .build();
  }

  /** Builds the ConfigMap for a per-LLAP-cluster TezAM (tez-site.xml). */
  public static ConfigMap buildTezAmConfigMap(HiveCluster hc, LlapSpec llap) {
    HiveClusterSpec spec = hc.getSpec();
    Map<String, String> labels = Labels.forTezAmCluster(hc, llap.name());
    Map<String, String> tezSite = HiveConfigBuilder.getTezSite(spec, llap);

    return new ConfigMapBuilder()
        .withNewMetadata()
          .withName(tezAmConfigMapName(hc, llap))
          .withNamespace(hc.getMetadata().getNamespace())
          .withLabels(labels)
          .withOwnerReferences(ownerRef(hc))
        .endMetadata()
        .addToData("tez-site.xml", HadoopXmlBuilder.buildXml(tezSite))
        .build();
  }

  // --- Private instance methods that use protected helpers from HiveDependentResource ---

  private Deployment doBuildTezAmDeployment(HiveCluster hiveCluster, LlapSpec llap,
      Integer replicas) {
    HiveClusterSpec spec = hiveCluster.getSpec();
    String ns = hiveCluster.getMetadata().getNamespace();
    String deployName = tezAmResourceName(hiveCluster, llap);
    Map<String, String> allLabels = Labels.forTezAmCluster(hiveCluster, llap.name());
    Map<String, String> selectorLabels = Labels.selectorForTezAmCluster(hiveCluster, llap.name());

    List<EnvVar> envVars = new ArrayList<>();
    envVars.add(new EnvVar("SERVICE_NAME", ConfigUtils.COMPONENT_TEZAM, null));
    envVars.add(new EnvVar("IS_RESUME", "true", null));
    envVars.add(new EnvVar("HIVE_ZOOKEEPER_QUORUM",
        spec.zookeeper().quorum(), null));
    envVars.add(new EnvVar("TEZ_FRAMEWORK_MODE", "STANDALONE_ZOOKEEPER", null));
    envVars.add(new EnvVar("HIVE_LLAP_DAEMON_SERVICE_HOSTS",
        llap.serviceHosts(), null));

    if (spec.envVars() != null) {
      envVars.addAll(spec.envVars());
    }

    List<io.fabric8.kubernetes.api.model.VolumeMount> volumeMounts = new ArrayList<>();
    volumeMounts.add(new io.fabric8.kubernetes.api.model.VolumeMountBuilder()
        .withName(HIVE_CONFIG_VOLUME)
        .withMountPath(CONF_MOUNT_PATH).build());
    volumeMounts.add(new io.fabric8.kubernetes.api.model.VolumeMountBuilder()
        .withName("scratch")
        .withMountPath("/opt/hive/scratch").build());

    List<Volume> volumes = new ArrayList<>();
    // Projected volume: hive-site.xml from HS2 CM, tez-site.xml from per-LLAP CM, core-site.xml from Hadoop CM
    String hs2CmName = HiveConfigMapDependent.HiveServer2.resourceName(hiveCluster);
    String hadoopCmName = HiveConfigMapDependent.Hadoop.resourceName(hiveCluster);
    String tezAmCmName = tezAmConfigMapName(hiveCluster, llap);
    volumes.add(buildProjectedConfigVolume(HIVE_CONFIG_VOLUME, hs2CmName, tezAmCmName, hadoopCmName));
    volumes.add(new io.fabric8.kubernetes.api.model.VolumeBuilder()
        .withName("scratch")
        .withNewPersistentVolumeClaim()
          .withClaimName(ScratchPvcDependent.resourceName(hiveCluster))
        .endPersistentVolumeClaim()
        .build());

    List<io.fabric8.kubernetes.api.model.ContainerPort> ports = new ArrayList<>();
    List<Container> initContainers = new ArrayList<>();
    addExternalJars(spec.image(), spec.externalJars(),
        initContainers, volumeMounts, volumes, envVars);
    replaceConfMountWithSubPaths(volumeMounts, HIVE_CONFIG_VOLUME,
        "hive-site.xml", "tez-site.xml", "core-site.xml");

    AutoscalingSpec tezAutoscaling = llap.tezAm().autoscaling();
    if (tezAutoscaling.isEnabled()) {
      addJmxExporter(spec.image(), ConfigUtils.COMPONENT_TEZAM, tezAutoscaling.metricsPort(),
          initContainers, volumeMounts, volumes, envVars, ports);
    }

    String configHash = sha256(
        HadoopXmlBuilder.buildXml(HiveConfigBuilder.getHiveServer2HiveSite(hiveCluster, spec)),
        HadoopXmlBuilder.buildXml(HiveConfigBuilder.getTezSite(spec, llap)),
        HadoopXmlBuilder.buildXml(HiveConfigBuilder.getHadoopCoreSite(spec)));

    Deployment deployment = new DeploymentBuilder()
        .withNewMetadata()
          .withName(deployName)
          .withNamespace(ns)
          .withLabels(allLabels)
          .withOwnerReferences(ownerRef(hiveCluster))
        .endMetadata()
        .withNewSpec()
          .withReplicas(replicas)
          .withNewSelector()
            .withMatchLabels(selectorLabels)
          .endSelector()
          .withNewTemplate()
            .withNewMetadata()
              .withLabels(allLabels)
              .addToAnnotations("kubectl.kubernetes.io/default-container",
                  ConfigUtils.COMPONENT_TEZAM)
              .addToAnnotations("hive.apache.org/config-hash", configHash)
            .endMetadata()
            .withNewSpec()
              .withServiceAccountName(spec.serviceAccountName())
              .withSubdomain(deployName)
              .withInitContainers(initContainers)
              .addNewContainer()
                .withName(ConfigUtils.COMPONENT_TEZAM)
                .withImage(spec.image())
                .withImagePullPolicy(spec.imagePullPolicy())
                .withEnv(envVars)
                .withPorts(ports)
                .withResources(buildResources(spec.tezAm().resources()))
                .withVolumeMounts(volumeMounts)
              .endContainer()
              .withVolumes(volumes)
            .endSpec()
          .endTemplate()
        .endSpec()
        .build();

    applySpreadAffinityIfAbsent(
        deployment.getSpec().getTemplate().getSpec(), selectorLabels);

    appendUserVolumes(deployment.getSpec().getTemplate().getSpec(),
        spec.volumes(), spec.volumeMounts(),
        spec.tezAm().extraVolumes(), spec.tezAm().extraVolumeMounts());

    // When autoscaling is enabled, add a preStop hook that waits for any in-flight DAG
    // to complete before exiting. The operator has already deleted the ZK registration
    // node (see HiveClusterAutoscaler) so no new DAGs can arrive. If a DAG arrives in
    // via the brief race window before ZK delete, we wait for it to finish.
    // Once tez_am_dag_running reaches 0, the hook exits and Kubernetes terminates the pod.
    if (tezAutoscaling.isEnabled()) {
      String preStopScript = buildDrainScript(
          "Waiting for active DAG to complete",
          TezAmScalingStrategy.METRIC_DAG_RUNNING, "DAG",
          "TezAM is idle. preStop complete, K8s will terminate pod.",
          10, 6, null, tezAutoscaling.metricsPort());
      applyAutoscalingLifecycle(
          deployment.getSpec().getTemplate().getSpec(),
          deployment.getSpec().getTemplate().getMetadata(),
          preStopScript, tezAutoscaling.gracePeriodSeconds(),
          tezAutoscaling.metricsScrapeIntervalSeconds());
    }

    return deployment;
  }

  private StatefulSet doBuildStatefulSet(HiveCluster hiveCluster, LlapSpec llap, Integer replicas) {
    HiveClusterSpec spec = hiveCluster.getSpec();
    String ns = hiveCluster.getMetadata().getNamespace();
    String ssName = resourceName(hiveCluster, llap);
    Map<String, String> allLabels = Labels.forLlapCluster(hiveCluster, llap.name());
    Map<String, String> selectorLabels = Labels.selectorForLlapCluster(hiveCluster, llap.name());

    List<EnvVar> envVars = new ArrayList<>();
    envVars.add(new EnvVar("SERVICE_NAME", "llap", null));
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

    if (spec.envVars() != null) {
      envVars.addAll(spec.envVars());
    }

    int managementPort = ConfigUtils.getInt(llap.configOverrides(),
        ConfigUtils.HIVE_LLAP_MANAGEMENT_RPC_PORT_KEY, null,
        ConfigUtils.HIVE_LLAP_MANAGEMENT_RPC_PORT_DEFAULT);
    int shufflePort = ConfigUtils.getInt(llap.configOverrides(),
        ConfigUtils.HIVE_LLAP_DAEMON_SHUFFLE_PORT_KEY, null,
        ConfigUtils.HIVE_LLAP_DAEMON_SHUFFLE_PORT_DEFAULT);
    int webPort = ConfigUtils.getInt(llap.configOverrides(),
        ConfigUtils.HIVE_LLAP_DAEMON_WEB_PORT_KEY, null,
        ConfigUtils.HIVE_LLAP_DAEMON_WEB_PORT_DEFAULT);
    int outputPort = ConfigUtils.getInt(llap.configOverrides(),
        ConfigUtils.HIVE_LLAP_DAEMON_OUTPUT_SERVICE_PORT_KEY, null,
        ConfigUtils.HIVE_LLAP_DAEMON_OUTPUT_SERVICE_PORT_DEFAULT);

    List<ContainerPort> ports = new ArrayList<>();
    ports.add(new ContainerPortBuilder()
        .withName("management").withContainerPort(managementPort)
        .withProtocol("TCP").build());
    ports.add(new ContainerPortBuilder()
        .withName("shuffle").withContainerPort(shufflePort)
        .withProtocol("TCP").build());
    ports.add(new ContainerPortBuilder()
        .withName("web").withContainerPort(webPort)
        .withProtocol("TCP").build());
    ports.add(new ContainerPortBuilder()
        .withName("output").withContainerPort(outputPort)
        .withProtocol("TCP").build());

    Probe readinessProbe = buildTcpProbe(managementPort, llap.readinessProbe(), 15, 10, 3);

    List<VolumeMount> volumeMounts = new ArrayList<>();
    volumeMounts.add(new io.fabric8.kubernetes.api.model.VolumeMountBuilder()
        .withName(LLAP_CONFIG_VOLUME)
        .withMountPath(CONF_MOUNT_PATH).build());

    List<Volume> volumes = new ArrayList<>();
    String cmName = configMapName(hiveCluster, llap);
    String hadoopCmName = HiveConfigMapDependent.Hadoop.resourceName(hiveCluster);
    volumes.add(buildProjectedConfigVolume(LLAP_CONFIG_VOLUME, cmName, hadoopCmName));

    List<Container> initContainers = new ArrayList<>();
    addExternalJars(spec.image(), spec.externalJars(),
        initContainers, volumeMounts, volumes, envVars);
    replaceConfMountWithSubPaths(volumeMounts, LLAP_CONFIG_VOLUME,
        "llap-daemon-site.xml", "core-site.xml");

    AutoscalingSpec autoscaling = llap.autoscaling();
    if (autoscaling.isEnabled()) {
      addJmxExporter(spec.image(), ConfigUtils.COMPONENT_LLAP, autoscaling.metricsPort(),
          initContainers, volumeMounts, volumes, envVars, ports);
    }

    String configHash = sha256(
        HadoopXmlBuilder.buildXml(HiveConfigBuilder.getLlapDaemonSite(spec, llap)),
        HadoopXmlBuilder.buildXml(HiveConfigBuilder.getHadoopCoreSite(spec)));

    StatefulSet statefulSet = new StatefulSetBuilder()
        .withNewMetadata()
          .withName(ssName)
          .withNamespace(ns)
          .withLabels(allLabels)
          .withOwnerReferences(ownerRef(hiveCluster))
        .endMetadata()
        .withNewSpec()
          .withReplicas(replicas)
          .withPodManagementPolicy("Parallel")
          .withServiceName(ssName)
          .withNewSelector()
            .withMatchLabels(selectorLabels)
          .endSelector()
          .withNewTemplate()
            .withNewMetadata()
              .withLabels(allLabels)
              .addToAnnotations("kubectl.kubernetes.io/default-container",
                  ConfigUtils.COMPONENT_LLAP)
              .addToAnnotations("hive.apache.org/config-hash", configHash)
            .endMetadata()
            .withNewSpec()
              .withServiceAccountName(spec.serviceAccountName())
              .withInitContainers(initContainers)
              .addNewContainer()
                .withName(ConfigUtils.COMPONENT_LLAP)
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

    if (autoscaling.isEnabled()) {
      String preStopScript = buildDualMetricDrainScript(
          "Waiting for LLAP executors to become idle",
          "hadoop_llapdaemon_executornumexecutorsavailable{", "AVAILABLE",
          "hadoop_llapdaemon_executornumexecutors{", "TOTAL",
          "LLAP executor metrics not found. JMX Exporter may not be configured.",
          "All executors idle. Shutting down.",
          "Executors available=$AVAILABLE / total=$TOTAL \u2014 waiting...",
          10, 6, autoscaling.metricsPort());
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
}
