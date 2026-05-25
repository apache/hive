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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.Matcher;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.spec.AutoscalingSpec;
import org.apache.hive.kubernetes.operator.model.spec.DatabaseConfig;
import org.apache.hive.kubernetes.operator.model.spec.ResourceRequirementsSpec;

import org.apache.hive.kubernetes.operator.model.spec.SecretKeyRef;
import org.apache.hive.kubernetes.operator.model.spec.ProbeSpec;
import org.apache.hive.kubernetes.operator.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all Hive operator dependent resources.
 * <p>
 * Overrides {@link #getSecondaryResource} to use this dependent's own
 * event source instead of the generic type-based lookup. This is
 * required because JOSDK's default implementation calls
 * {@code context.getSecondaryResource(type)} which throws when
 * multiple dependents manage the same Kubernetes resource type
 * (e.g. multiple ConfigMap or Service dependents).
 */
public abstract class HiveDependentResource<R extends HasMetadata,
    P extends HasMetadata>
    extends CRUDKubernetesDependentResource<R, P> {

  private static final Logger LOG =
      LoggerFactory.getLogger(HiveDependentResource.class);

  protected static final String CONF_MOUNT_PATH = "/etc/hive/conf";
  protected static final String HIVE_CONF_DIR = "/opt/hive/conf";
  protected static final String EXT_JARS_PATH = "/tmp/ext-jars";

  protected HiveDependentResource(Class<R> resourceType) {
    super(resourceType);
  }

  /**
   * Catches 409 AlreadyExists during resource creation caused by
   * informer lag — the resource exists on the API server but
   * the informer cache hasn't indexed it yet, so JOSDK calls
   * create directly.
   */
  @Override
  protected R handleCreate(R desired, P primary, Context<P> context) {
    try {
      return super.handleCreate(desired, primary, context);
    } catch (KubernetesClientException e) {
      if (e.getCode() == 409) {
        LOG.info("Resource {} already exists (informer lag), "
            + "will reconcile on next event",
            desired.getMetadata().getName());
        return desired;
      }
      throw e;
    }
  }

  @Override
  public Optional<R> getSecondaryResource(P primary,
      Context<P> context) {
    return eventSource()
        .flatMap(es -> es.getSecondaryResource(primary));
  }

  /**
   * Jobs and PVCs are immutable after creation — Kubernetes rejects
   * any PUT that modifies spec.selector, spec.template (Job) or
   * spec.resources/accessModes (PVC). Short-circuit the match to
   * prevent the framework from attempting updates on these resources.
   */
  @Override
  public Matcher.Result<R> match(R actualResource, R desired,
      P primary, Context<P> context) {
    if (actualResource != null) {
      String kind = actualResource.getKind();
      if ("Job".equals(kind)
          || "PersistentVolumeClaim".equals(kind)) {
        return Matcher.Result.nonComputed(true);
      }
    }
    return super.match(actualResource, desired, primary, context);
  }

  /**
   * Computes a SHA-256 hash of the given input strings.
   * Used to annotate pod templates so that config changes trigger rolling updates.
   */
  protected static String sha256(String... inputs) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      for (String input : inputs) {
        if (input != null) {
          digest.update(input.getBytes(StandardCharsets.UTF_8));
        }
      }
      byte[] hash = digest.digest();
      StringBuilder sb = new StringBuilder(64);
      for (byte b : hash) {
        sb.append(String.format("%02x", b));
      }
      return sb.toString();
    } catch (Exception e) {
      return "unknown";
    }
  }

  /**
   * Builds the database connection env vars: DB_DRIVER, DBPASSWORD
   * (from SecretKeyRef), and SERVICE_OPTS with javax.jdo connection
   * properties. Shared by MetastoreDeploymentDependent and
   * SchemaInitJobDependent.
   */
  protected static List<EnvVar> buildDbEnvVars(DatabaseConfig db) {
    List<EnvVar> envVars = new ArrayList<>();
    envVars.add(new EnvVar("DB_DRIVER", db.type(), null));

    // DBPASSWORD must be defined before SERVICE_OPTS so that
    // Kubernetes $(DBPASSWORD) interpolation resolves correctly.
    SecretKeyRef passwordRef = db.passwordSecretRef();
    if (passwordRef != null) {
      envVars.add(new EnvVarBuilder()
          .withName("DBPASSWORD")
          .withNewValueFrom()
            .withNewSecretKeyRef()
              .withName(passwordRef.name())
              .withKey(passwordRef.key())
            .endSecretKeyRef()
          .endValueFrom()
          .build());
    }

    StringBuilder serviceOpts = new StringBuilder();
    if (db.url() != null) {
      serviceOpts.append("-Djavax.jdo.option.ConnectionURL=")
          .append(db.url());
    }
    if (db.driver() != null) {
      serviceOpts.append(" -Djavax.jdo.option.ConnectionDriverName=")
          .append(db.driver());
    }
    if (db.username() != null) {
      serviceOpts.append(" -Djavax.jdo.option.ConnectionUserName=")
          .append(db.username());
    }
    if (passwordRef != null) {
      serviceOpts.append(
          " -Djavax.jdo.option.ConnectionPassword=$(DBPASSWORD)");
    }
    if (!serviceOpts.isEmpty()) {
      envVars.add(new EnvVar("SERVICE_OPTS",
          serviceOpts.toString().trim(), null));
    }
    return envVars;
  }


  /** Builds a projected Volume merging multiple ConfigMaps. */
  protected static Volume buildProjectedConfigVolume(
      String volumeName, String... configMapNames) {
    List<io.fabric8.kubernetes.api.model.VolumeProjection>
        projections = new ArrayList<>();
    for (String cmName : configMapNames) {
      projections.add(
          new io.fabric8.kubernetes.api.model.VolumeProjectionBuilder()
              .withNewConfigMap().withName(cmName).endConfigMap()
              .build());
    }
    return new VolumeBuilder()
        .withName(volumeName)
        .withNewProjected()
          .withSources(projections)
        .endProjected()
        .build();
  }


  /**
   * Populates volume mounts and volumes for the Metastore pod spec
   * (shared by MetastoreDeploymentDependent and SchemaInitJobDependent).
   * Adds the projected hive-config volume (merging metastore + hadoop
   * ConfigMaps). External JARs (JDBC driver + global externalJars)
   * should be handled separately via {@link #addExternalJars}.
   */
  protected static void buildMetastoreVolumes(
      HiveCluster hiveCluster,
      List<io.fabric8.kubernetes.api.model.VolumeMount> volumeMounts,
      List<Volume> volumes) {

    volumeMounts.add(new VolumeMountBuilder()
        .withName("hive-config")
        .withMountPath(CONF_MOUNT_PATH).build());

    volumes.add(buildProjectedConfigVolume("hive-config",
        MetastoreConfigMapDependent.resourceName(hiveCluster),
        HadoopConfigMapDependent.resourceName(hiveCluster)));
  }

  /** Builds Kubernetes ResourceRequirements from the operator's spec. */
  protected static ResourceRequirements buildResources(ResourceRequirementsSpec spec) {
    if (spec == null) {
      return new ResourceRequirements();
    }
    ResourceRequirementsBuilder builder = new ResourceRequirementsBuilder();
    if (spec.requestsCpu() != null) {
      builder.addToRequests("cpu", new Quantity(spec.requestsCpu()));
    }
    if (spec.requestsMemory() != null) {
      builder.addToRequests("memory", new Quantity(spec.requestsMemory()));
    }
    if (spec.limitsCpu() != null) {
      builder.addToLimits("cpu", new Quantity(spec.limitsCpu()));
    }
    if (spec.limitsMemory() != null) {
      builder.addToLimits("memory", new Quantity(spec.limitsMemory()));
    }
    return builder.build();
  }

  /**
   * Sets a preferred pod anti-affinity on the pod spec if no affinity is
   * already defined. This spreads replicas across nodes while allowing
   * future user-defined affinity to take precedence.
   */
  protected static void applySpreadAffinityIfAbsent(
      io.fabric8.kubernetes.api.model.PodSpec podSpec,
      Map<String, String> selectorLabels) {
    if (podSpec.getAffinity() != null) {
      return;
    }
    podSpec.setAffinity(new AffinityBuilder()
        .withNewPodAntiAffinity()
          .addNewPreferredDuringSchedulingIgnoredDuringExecution()
            .withWeight(100)
            .withNewPodAffinityTerm()
              .withNewLabelSelector()
                .withMatchLabels(selectorLabels)
              .endLabelSelector()
              .withTopologyKey("kubernetes.io/hostname")
            .endPodAffinityTerm()
          .endPreferredDuringSchedulingIgnoredDuringExecution()
        .endPodAntiAffinity()
        .build());
  }

  /**
   * Builds an init container that downloads external JARs via wget
   * (for http/https URLs) or hadoop fs (for HDFS/cloud paths).
   */
  protected static Container buildExternalJarsInitContainer(
      String image, List<String> externalJars,
      List<EnvVar> envVars, List<VolumeMount> volumeMounts,
      String containerName) {

    // Determine target directory from the first volume mount
    String targetDir = volumeMounts.get(0).getMountPath();

    StringBuilder cmd = new StringBuilder();
    cmd.append("export HADOOP_CONF_DIR=").append(CONF_MOUNT_PATH).append(" && ");

    for (String jarUrl : externalJars) {
      if (jarUrl.startsWith("http://") || jarUrl.startsWith("https://")) {
        cmd.append("wget -q --tries=3 --waitretry=5 -P ").append(targetDir)
            .append(" '").append(jarUrl).append("' && ");
      } else {
        cmd.append("{ ok=0; for i in 1 2 3; do hadoop fs -copyToLocal '").append(jarUrl)
            .append("' ").append(targetDir).append("/ && ok=1 && break || sleep 5; done; ")
            .append("[ $ok -eq 1 ]; } && ");
      }
    }
    cmd.append("echo 'All external JARs downloaded successfully.'");

    return new ContainerBuilder()
        .withName(containerName)
        .withImage(image)
        .withCommand("/bin/bash", "-c", cmd.toString())
        .withEnv(envVars)
        .withVolumeMounts(volumeMounts)
        .build();
  }

  /**
   * Replaces the directory-level CONF_MOUNT_PATH volume mount with
   * individual subPath mounts into HIVE_CONF_DIR (/opt/hive/conf/).
   * <p>
   * This avoids the broken-symlink problem: Kubernetes projected volumes
   * use internal timestamped directories that rotate on ConfigMap updates.
   * The Hive Docker entrypoint symlinks resolved paths (not the stable
   * {@code ..data/} link), so symlinks break when the directory rotates.
   * subPath mounts place files directly without symlink indirection.
   * <p>
   * Call this AFTER {@code addGlobalExternalJars} so the init container
   * can still find the CONF_MOUNT_PATH mount.
   */
  protected static void replaceConfMountWithSubPaths(
      List<VolumeMount> volumeMounts, String volumeName,
      String... fileNames) {
    volumeMounts.removeIf(
        vm -> vm.getMountPath().equals(CONF_MOUNT_PATH));
    for (String file : fileNames) {
      volumeMounts.add(new VolumeMountBuilder()
          .withName(volumeName)
          .withMountPath(HIVE_CONF_DIR + "/" + file)
          .withSubPath(file)
          .build());
    }
  }


  /**
   * Adds external JAR download init container, volume, and
   * volume mount. Downloads to /tmp/ext-jars so the native
   * Hive entrypoint.sh automatically copies them to $HIVE_HOME/lib.
   */
  protected static void addExternalJars(
      String image,
      List<String> jars,
      List<Container> initContainers,
      List<VolumeMount> volumeMounts,
      List<Volume> volumes,
      List<EnvVar> envVars) {
    if (jars == null || jars.isEmpty()) {
      return;
    }

    VolumeMount extMount = new VolumeMountBuilder()
        .withName("ext-jars")
        .withMountPath(EXT_JARS_PATH).build();

    // Add volume mount for the main container
    volumeMounts.add(extMount);

    // Add emptyDir volume
    volumes.add(new VolumeBuilder()
        .withName("ext-jars")
        .withNewEmptyDir().endEmptyDir().build());

    // Build init container with config mount + ext-jars mount
    List<VolumeMount> initMounts = new ArrayList<>();
    initMounts.add(extMount);
    for (VolumeMount vm : volumeMounts) {
      if (vm.getMountPath().equals(CONF_MOUNT_PATH)) {
        initMounts.add(vm);
        break;
      }
    }

    initContainers.add(
        buildExternalJarsInitContainer(image, jars,
            envVars, initMounts, "download-ext-jars"));
  }

  /**
   * Builds a TCP socket probe using user-provided overrides or fallback defaults.
   */
  protected static Probe buildTcpProbe(int port, ProbeSpec spec, int defaultInitialDelay, int defaultPeriod,
      int defaultFailureThreshold) {

    int initialDelay =
        (spec != null && spec.initialDelaySeconds() != null) ? spec.initialDelaySeconds() : defaultInitialDelay;
    int period = (spec != null && spec.periodSeconds() != null) ? spec.periodSeconds() : defaultPeriod;
    int failureThreshold =
        (spec != null && spec.failureThreshold() != null) ? spec.failureThreshold() : defaultFailureThreshold;

    ProbeBuilder builder = new ProbeBuilder()
        .withNewTcpSocket()
          .withPort(new IntOrString(port))
        .endTcpSocket()
        .withInitialDelaySeconds(initialDelay)
        .withPeriodSeconds(period)
        .withFailureThreshold(failureThreshold);

    if (spec != null && spec.timeoutSeconds() != null) {
      builder.withTimeoutSeconds(spec.timeoutSeconds());
    }
    if (spec != null && spec.successThreshold() != null) {
      builder.withSuccessThreshold(spec.successThreshold());
    }
    return builder.build();
  }

  /** Path where the JMX Exporter agent JAR is stored inside the pod. */
  protected static final String JMX_EXPORTER_DIR = "/opt/jmx-exporter";
  protected static final String JMX_EXPORTER_JAR = JMX_EXPORTER_DIR + "/jmx_prometheus_javaagent.jar";
  protected static final String JMX_EXPORTER_CONFIG = JMX_EXPORTER_DIR + "/config.yaml";

  /**
   * Adds the Prometheus JMX Exporter agent infrastructure to a pod spec when
   * autoscaling is enabled. This includes:
   * <ul>
   *   <li>An emptyDir volume for the JMX exporter JAR and config</li>
   *   <li>An init container that downloads the agent JAR and writes a config file</li>
   *   <li>A volume mount on the main container</li>
   *   <li>A container port for the metrics endpoint (9404)</li>
   *   <li>The javaagent JVM argument appended to SERVICE_OPTS</li>
   * </ul>
   *
   * @param image the container image (used for the init container)
   * @param component the Hive component name (for JMX bean pattern matching)
   * @param initContainers list to add the download init container to
   * @param volumeMounts list to add the jmx-exporter mount to (main container)
   * @param volumes list to add the emptyDir volume to
   * @param envVars list of env vars — SERVICE_OPTS will be updated with the javaagent flag
   * @param ports list to add the metrics port to
   */
  protected static void addJmxExporter(
      String image, String component,
      List<Container> initContainers,
      List<VolumeMount> volumeMounts,
      List<Volume> volumes,
      List<EnvVar> envVars,
      List<io.fabric8.kubernetes.api.model.ContainerPort> ports) {

    // Volume for the JMX exporter JAR + config
    volumes.add(new VolumeBuilder()
        .withName("jmx-exporter")
        .withNewEmptyDir().endEmptyDir().build());
    VolumeMount exporterMount = new VolumeMountBuilder()
        .withName("jmx-exporter")
        .withMountPath(JMX_EXPORTER_DIR).build();
    volumeMounts.add(exporterMount);

    // JMX exporter config: export all beans in a catch-all pattern
    // The agent exposes metrics in Prometheus text format at /metrics
    String jmxConfig = buildJmxExporterConfig(component);

    // Init container: download JAR + write config
    String downloadCmd = String.format(
        "wget -q --tries=3 --waitretry=5 -O %s '%s' && "
            + "cat > %s << 'JMXEOF'\n%s\nJMXEOF",
        JMX_EXPORTER_JAR, ConfigUtils.JMX_EXPORTER_JAR_URL,
        JMX_EXPORTER_CONFIG, jmxConfig);
    initContainers.add(new ContainerBuilder()
        .withName("jmx-exporter-init")
        .withImage(image)
        .withCommand("/bin/bash", "-c", downloadCmd)
        .withVolumeMounts(exporterMount)
        .build());

    // Expose the metrics port
    ports.add(new io.fabric8.kubernetes.api.model.ContainerPortBuilder()
        .withName("metrics")
        .withContainerPort(ConfigUtils.PROMETHEUS_JMX_EXPORTER_PORT).build());

    // Add javaagent flag to the appropriate JVM opts env var.
    // LLAP uses LLAP_DAEMON_OPTS (its startup script ignores SERVICE_OPTS).
    String agentArg = String.format("-javaagent:%s=%d:%s",
        JMX_EXPORTER_JAR, ConfigUtils.PROMETHEUS_JMX_EXPORTER_PORT, JMX_EXPORTER_CONFIG);
    String optsEnvVar = "llap".equals(component) ? "LLAP_DAEMON_OPTS" : "SERVICE_OPTS";
    boolean found = false;
    for (int i = 0; i < envVars.size(); i++) {
      if (optsEnvVar.equals(envVars.get(i).getName())) {
        String existing = envVars.get(i).getValue();
        envVars.set(i, new EnvVar(optsEnvVar,
            existing + " " + agentArg, null));
        found = true;
        break;
      }
    }
    if (!found) {
      envVars.add(new EnvVar(optsEnvVar, agentArg, null));
    }
  }

  /**
   * Builds the JMX Exporter YAML config for a Hive component.
   * Uses broad patterns to export all Hive/Hadoop metrics relevant to autoscaling.
   */
  private static String buildJmxExporterConfig(String component) {
    StringBuilder sb = new StringBuilder();
    sb.append("lowercaseOutputName: true\n");
    sb.append("lowercaseOutputLabelNames: true\n");
    sb.append("rules:\n");

    switch (component) {
      case "hiveserver2":
        // HS2 session and operation metrics
        sb.append("- pattern: 'metrics<name=hs2_(.+)><>Value'\n");
        sb.append("  name: hs2_$1\n");
        sb.append("  type: GAUGE\n");
        sb.append("- pattern: 'metrics<name=active_calls_(.+)><>Count'\n");
        sb.append("  name: hs2_active_calls_$1\n");
        sb.append("  type: GAUGE\n");
        // Tez session pool metrics (pending tasks, backlog ratio, running tasks)
        sb.append("- pattern: 'metrics<name=tez_session_(.+)><>Value'\n");
        sb.append("  name: tez_session_$1\n");
        sb.append("  type: GAUGE\n");
        break;
      case "metastore":
        // HMS API call metrics
        sb.append("- pattern: 'metrics<name=api_(.+)><>Count'\n");
        sb.append("  name: api_$1_total\n");
        sb.append("  type: COUNTER\n");
        sb.append("- pattern: 'metrics<name=open_connections><>Value'\n");
        sb.append("  name: hive_metastore_open_connections\n");
        sb.append("  type: GAUGE\n");
        break;
      case "llap":
        // LLAP uses its own MetricsSystem (not DefaultMetricsSystem).
        // Default JMX exporter pattern (.*) exports Hadoop Metrics2 MBeans as:
        //   hadoop_llapdaemon_<attribute>{name="<source>"}
        // e.g., hadoop_llapdaemon_executornumqueuedrequests{name="LlapDaemonExecutorMetrics-..."}
        // No custom rules needed — the default naming is usable directly.
        sb.append("- pattern: '.*'\n");
        break;
      case "tezam":
        // TezAM DAG execution metrics
        sb.append("- pattern: 'Hadoop<service=TezAppMaster, name=TezAppMaster><>(.+)'\n");
        sb.append("  name: tez_am_$1\n");
        sb.append("  type: GAUGE\n");
        break;
      default:
        sb.append("- pattern: '.*'\n");
        break;
    }
    return sb.toString();
  }

}
