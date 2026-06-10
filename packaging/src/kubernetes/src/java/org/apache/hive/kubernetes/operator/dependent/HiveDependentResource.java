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
import java.util.Set;
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
import org.apache.hive.kubernetes.operator.autoscaling.HiveClusterAutoscaler;
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
   * Returns the expected Kubernetes resource name for this dependent.
   * Used to disambiguate when multiple dependents share the same resource
   * type (e.g., multiple ConfigMap or Service dependents). Subclasses that
   * share a resource type MUST override this method.
   *
   * @throws IllegalStateException if not overridden and disambiguation is needed
   */
  protected String getSecondaryResourceName(P primary, Context<P> context) {
    throw new IllegalStateException(
        getClass().getSimpleName() + " must override getSecondaryResourceName() "
            + "when multiple dependents share the same resource type");
  }

  @Override
  public Optional<R> getSecondaryResource(P primary,
      Context<P> context) {
    return eventSource()
        .flatMap(es -> {
          Set<R> resources = es.getSecondaryResources(primary);
          if (resources.isEmpty()) {
            return Optional.empty();
          }
          // Always filter by expected name — even when only one resource
          // is in the cache. Without this, a single Deployment (e.g.
          // metastore) would be handed to HiveServer2's matcher, causing
          // a cross-component update loop.
          String expectedName = getSecondaryResourceName(primary,
              context);
          return resources.stream()
              .filter(r -> expectedName.equals(
                  r.getMetadata().getName()))
              .findFirst();
        });
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
   * Handles 409 Conflict errors during resource creation caused by informer
   * cache lag. When the operator creates a resource but the informer hasn't
   * yet received the creation event, the framework may attempt to create it
   * again. Kubernetes rejects the duplicate with 409 — this handler absorbs
   * that expected race and lets the next reconciliation pick up the resource
   * from the updated cache.
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

  /**
   * Resolves the replica count to set in the desired workload spec.
   * <p>
   * Always returns an explicit value — never null. Returning null would cause
   * JOSDK/SSA to omit spec.replicas, and Kubernetes would default it to 1.
   * <p>
   * When autoscaling is enabled:
   * - On CREATE: returns initialReplicas (minReplicas for the component)
   * - On UPDATE: returns the autoscaler's managed value, or falls back to
   *   the current actual replicas from the informer cache.
   * <p>
   * When autoscaling is disabled: returns staticReplicas (the spec value).
   */
  protected Integer resolveReplicaCount(P primary, Context<P> context,
      AutoscalingSpec autoscaling, int staticReplicas, int initialReplicas) {
    // Suspended cluster → 0 replicas (dependent resources natively respect suspend).
    // Exception: HMS stays running if includeMetastore=false in autoSuspend config.
    if (primary instanceof HiveCluster hc && hc.getSpec().suspend()) {
      boolean isMetastore = ConfigUtils.COMPONENT_METASTORE.equals(getComponentName());
      if (!isMetastore || hc.getSpec().autoSuspend().includeMetastore()) {
        return 0;
      }
    }
    if (autoscaling == null || !autoscaling.isEnabled()) {
      return staticReplicas;
    }
    Optional<R> existing = getSecondaryResource(primary, context);
    if (existing.isPresent()) {
      // Check if the autoscaler has made a decision during this operator's lifecycle
      Integer managed = HiveClusterAutoscaler.getManagedReplicas(
          primary.getMetadata().getNamespace(),
          primary.getMetadata().getName(),
          getComponentName());
      if (managed != null) {
        return managed;
      }
      // Fallback: operator restarted and MANAGED_REPLICAS is empty — read current value
      R resource = existing.get();
      if (resource instanceof io.fabric8.kubernetes.api.model.apps.Deployment d) {
        return d.getSpec() != null && d.getSpec().getReplicas() != null
            ? d.getSpec().getReplicas() : initialReplicas;
      }
      if (resource instanceof io.fabric8.kubernetes.api.model.apps.StatefulSet s) {
        return s.getSpec() != null && s.getSpec().getReplicas() != null
            ? s.getSpec().getReplicas() : initialReplicas;
      }
      return initialReplicas;
    }
    // First creation: start at minReplicas.
    return initialReplicas;
  }


  /**
   * Returns the component name for this dependent (used for autoscaler replica lookup).
   * Subclasses should override if they manage a workload with autoscaling.
   */
  protected String getComponentName() {
    return null;
  }

  /**
   * Builds a preStop drain script that polls a single Prometheus metric
   * (from the JMX Exporter at localhost:9404/metrics) until the value
   * reaches zero, then exits to allow graceful pod termination.
   *
   * @param startupMessage  logged at the start (e.g. "Waiting for open connections to drain")
   * @param metricName      Prometheus metric name (used in grep and log messages)
   * @param varName         shell variable name for the extracted value (e.g. "CONNS")
   * @param idleMessage     logged when idle condition is met (e.g. "All connections drained. Shutting down.")
   * @param sleepSeconds    polling interval in seconds
   * @param maxRetries      max consecutive curl failures before giving up
   * @param prefixCommands  optional commands to run before the polling loop (may be null)
   */
  protected static String buildDrainScript(
      String startupMessage, String metricName, String varName,
      String idleMessage, int sleepSeconds, int maxRetries,
      List<String> prefixCommands) {
    List<String> lines = new ArrayList<>();
    lines.add("#!/bin/bash");
    if (prefixCommands != null) {
      lines.addAll(prefixCommands);
    }
    lines.add("echo '[preStop] " + startupMessage
        + " (polling localhost:9404/metrics)...'");
    lines.add("RETRIES=0");
    lines.add("while true; do");
    lines.add("  RESPONSE=$(curl -sf http://localhost:9404/metrics)");
    lines.add("  if [ $? -ne 0 ]; then");
    lines.add("    RETRIES=$((RETRIES+1))");
    lines.add("    echo \"[preStop] ERROR: JMX Exporter unreachable on port 9404 (attempt $RETRIES)\"");
    lines.add("    if [ $RETRIES -ge " + maxRetries + " ]; then");
    lines.add("      echo '[preStop] JMX Exporter not responding after "
        + (maxRetries * sleepSeconds) + "s. Proceeding with shutdown.'");
    lines.add("      break");
    lines.add("    fi");
    lines.add("    sleep " + sleepSeconds + "; continue");
    lines.add("  fi");
    lines.add("  " + varName + "=$(echo \"$RESPONSE\" | grep '^"
        + metricName + " ' | awk '{print $2}')");
    lines.add("  if [ -z \"$" + varName + "\" ]; then");
    lines.add("    echo '[preStop] WARNING: " + metricName
        + " metric not found. JMX Exporter may not be configured.'");
    lines.add("    break");
    lines.add("  fi");
    lines.add("  if [ \"${" + varName + "%.*}\" -le 0 ] 2>/dev/null; then");
    lines.add("    echo '[preStop] " + idleMessage + "'");
    lines.add("    break");
    lines.add("  fi");
    lines.add("  echo \"[preStop] " + metricName + "=$" + varName + " - waiting...\"");
    lines.add("  RETRIES=0");
    lines.add("  sleep " + sleepSeconds);
    lines.add("done");
    // Send SIGTERM directly to the Java process. Shell entrypoint scripts
    // (PID 1) often don't forward signals, so K8s SIGTERM never reaches
    // the JVM — causing a full grace-period wait before SIGKILL.
    // Use 'java' pattern to avoid matching this script itself.
    lines.add("echo '[preStop] Sending SIGTERM to Java process...'");
    lines.add("pkill -f 'java.*org.apache' || true");
    lines.add("exit 0");
    return String.join("\n", lines);
  }

  /**
   * Builds a preStop drain script that polls two Prometheus metrics and
   * waits until available >= total (all executors idle). Used by LLAP.
   *
   * @param startupMessage  logged at the start
   * @param metricGrepA     grep pattern for the first metric (e.g. includes trailing '{')
   * @param varNameA        shell variable for the first metric value (e.g. "AVAILABLE")
   * @param metricGrepB     grep pattern for the second metric
   * @param varNameB        shell variable for the second metric value (e.g. "TOTAL")
   * @param notFoundWarning warning message when metrics are not found
   * @param idleMessage     logged when idle condition is met
   * @param waitingFormat   format for waiting log (with shell variable references)
   * @param sleepSeconds    polling interval in seconds
   * @param maxRetries      max consecutive curl failures before giving up
   */
  protected static String buildDualMetricDrainScript(
      String startupMessage,
      String metricGrepA, String varNameA,
      String metricGrepB, String varNameB,
      String notFoundWarning, String idleMessage,
      String waitingFormat, int sleepSeconds, int maxRetries) {
    List<String> lines = new ArrayList<>();
    lines.add("#!/bin/bash");
    lines.add("echo '[preStop] " + startupMessage
        + " (polling localhost:9404/metrics)...'");
    lines.add("RETRIES=0");
    lines.add("while true; do");
    lines.add("  RESPONSE=$(curl -sf http://localhost:9404/metrics)");
    lines.add("  if [ $? -ne 0 ]; then");
    lines.add("    RETRIES=$((RETRIES+1))");
    lines.add("    echo \"[preStop] ERROR: JMX Exporter unreachable on port 9404 (attempt $RETRIES)\"");
    lines.add("    if [ $RETRIES -ge " + maxRetries + " ]; then");
    lines.add("      echo '[preStop] JMX Exporter not responding after "
        + (maxRetries * sleepSeconds) + "s. Proceeding with shutdown.'");
    lines.add("      break");
    lines.add("    fi");
    lines.add("    sleep " + sleepSeconds + "; continue");
    lines.add("  fi");
    lines.add("  " + varNameA + "=$(echo \"$RESPONSE\" | grep '^"
        + metricGrepA + "' | awk '{print $2}')");
    lines.add("  " + varNameB + "=$(echo \"$RESPONSE\" | grep '^"
        + metricGrepB + "' | awk '{print $2}')");
    lines.add("  if [ -z \"$" + varNameA + "\" ] || [ -z \"$" + varNameB + "\" ]; then");
    lines.add("    echo '[preStop] WARNING: " + notFoundWarning + "'");
    lines.add("    break");
    lines.add("  fi");
    lines.add("  if [ \"${" + varNameA + "%.*}\" -ge \"${" + varNameB + "%.*}\" ] 2>/dev/null; then");
    lines.add("    echo '[preStop] " + idleMessage + "'");
    lines.add("    break");
    lines.add("  fi");
    lines.add("  echo \"[preStop] " + waitingFormat + "\"");
    lines.add("  RETRIES=0");
    lines.add("  sleep " + sleepSeconds);
    lines.add("done");
    // Send SIGTERM directly to the Java process. Shell entrypoint scripts
    // (PID 1) often don't forward signals, so K8s SIGTERM never reaches
    // the JVM — causing a full grace-period wait before SIGKILL.
    // Use 'java' pattern to avoid matching this script itself.
    lines.add("echo '[preStop] Sending SIGTERM to Java process...'");
    lines.add("pkill -f 'java.*org.apache' || true");
    lines.add("exit 0");
    return String.join("\n", lines);
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
        HiveConfigMapDependent.Metastore.resourceName(hiveCluster),
        HiveConfigMapDependent.Hadoop.resourceName(hiveCluster)));
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

  /**
   * Applies the autoscaling lifecycle to a workload's pod template: sets a preStop
   * exec lifecycle hook, terminationGracePeriodSeconds, and Prometheus scrape annotations.
   *
   * @param podSpec           the pod spec of the workload (Deployment or StatefulSet)
   * @param podMetadata       the pod template metadata (for annotations)
   * @param preStopScript     the shell script to run in the preStop hook
   * @param gracePeriodSeconds termination grace period
   */
  protected static void applyAutoscalingLifecycle(
      io.fabric8.kubernetes.api.model.PodSpec podSpec,
      io.fabric8.kubernetes.api.model.ObjectMeta podMetadata,
      String preStopScript, int gracePeriodSeconds,
      int metricsScrapeIntervalSeconds) {
    io.fabric8.kubernetes.api.model.Lifecycle lifecycle =
        new io.fabric8.kubernetes.api.model.LifecycleBuilder()
            .withNewPreStop()
              .withNewExec()
                .withCommand("/bin/bash", "-c", preStopScript)
              .endExec()
            .endPreStop()
            .build();
    podSpec.getContainers().get(0).setLifecycle(lifecycle);
    podSpec.setTerminationGracePeriodSeconds((long) gracePeriodSeconds);
    applyPrometheusScrapeAnnotations(podMetadata, metricsScrapeIntervalSeconds);
  }

  /**
   * Adds Prometheus scrape annotations to a pod template so that
   * the JMX Exporter metrics endpoint is discovered by Prometheus.
   */
  private static void applyPrometheusScrapeAnnotations(
      io.fabric8.kubernetes.api.model.ObjectMeta podMetadata,
      int scrapeIntervalSeconds) {
    podMetadata.getAnnotations().put("prometheus.io/scrape", "true");
    podMetadata.getAnnotations().put("prometheus.io/port",
        String.valueOf(ConfigUtils.PROMETHEUS_JMX_EXPORTER_PORT));
    podMetadata.getAnnotations().put("prometheus.io/path", "/metrics");
    podMetadata.getAnnotations().put("prometheus.io/scrape-interval",
        scrapeIntervalSeconds + "s");
  }

  /**
   * Appends user-provided volumes and volume mounts to a workload's pod template.
   * Handles both global (spec-level) and component-specific extras.
   *
   * @param podSpec             the pod spec
   * @param globalVolumes       spec.volumes() (may be null)
   * @param globalVolumeMounts  spec.volumeMounts() (may be null)
   * @param extraVolumes        component-specific extraVolumes (may be null)
   * @param extraVolumeMounts   component-specific extraVolumeMounts (may be null)
   */
  protected static void appendUserVolumes(
      io.fabric8.kubernetes.api.model.PodSpec podSpec,
      List<Volume> globalVolumes,
      List<VolumeMount> globalVolumeMounts,
      List<Volume> extraVolumes,
      List<VolumeMount> extraVolumeMounts) {
    if (globalVolumes != null) {
      podSpec.getVolumes().addAll(globalVolumes);
    }
    if (globalVolumeMounts != null) {
      podSpec.getContainers().get(0).getVolumeMounts().addAll(globalVolumeMounts);
    }
    if (extraVolumes != null) {
      podSpec.getVolumes().addAll(extraVolumes);
    }
    if (extraVolumeMounts != null) {
      podSpec.getContainers().get(0).getVolumeMounts().addAll(extraVolumeMounts);
    }
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
      String image, String component, int metricsPort,
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
        .withContainerPort(metricsPort)
        .withProtocol("TCP").build());

    // Add javaagent flag to the appropriate JVM opts env var.
    // LLAP uses LLAP_DAEMON_OPTS (its startup script ignores SERVICE_OPTS).
    String agentArg = String.format("-javaagent:%s=%d:%s",
        JMX_EXPORTER_JAR, metricsPort, JMX_EXPORTER_CONFIG);
    String optsEnvVar = ConfigUtils.COMPONENT_LLAP.equals(component) ? "LLAP_DAEMON_OPTS" : "SERVICE_OPTS";
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
    case ConfigUtils.COMPONENT_HIVESERVER2:
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
      // JVM CPU usage for CPU-based autoscaling
      sb.append("- pattern: 'java.lang<type=OperatingSystem><>ProcessCpuLoad'\n");
      sb.append("  name: jvm_process_cpu_load\n");
      sb.append("  type: GAUGE\n");
      break;
    case ConfigUtils.COMPONENT_METASTORE:
      // HMS API call metrics
      sb.append("- pattern: 'metrics<name=api_(.+)><>Count'\n");
      sb.append("  name: api_$1_total\n");
      sb.append("  type: COUNTER\n");
      sb.append("- pattern: 'metrics<name=open_connections><>Count'\n");
      sb.append("  name: hive_metastore_open_connections\n");
      sb.append("  type: GAUGE\n");
      // JVM CPU usage for CPU-based autoscaling
      sb.append("- pattern: 'java.lang<type=OperatingSystem><>ProcessCpuLoad'\n");
      sb.append("  name: jvm_process_cpu_load\n");
      sb.append("  type: GAUGE\n");
      break;
    case ConfigUtils.COMPONENT_LLAP:
      // Only export the executor metrics the autoscaler and drain script need.
      // A wildcard '.*' pattern serializes 600+ metrics every scrape interval,
      // causing CPU spikes and GC pressure on the LLAP JVM.
      // Internal format: Hadoop<service=LlapDaemon, name=LlapDaemonExecutorMetrics-<pod>><>Attribute
      // Separate rules per attribute — JMX Exporter 1.x caches per-bean, not per-attribute.
      sb.append("- pattern: 'Hadoop<service=LlapDaemon, name=LlapDaemonExecutorMetrics.+><>ExecutorNumQueuedRequests'\n");
      sb.append("  name: hadoop_llapdaemon_executornumqueuedrequests\n");
      sb.append("  type: GAUGE\n");
      sb.append("- pattern: 'Hadoop<service=LlapDaemon, name=LlapDaemonExecutorMetrics.+><>ExecutorNumExecutorsConfigured'\n");
      sb.append("  name: hadoop_llapdaemon_executornumexecutorsconfigured\n");
      sb.append("  type: GAUGE\n");
      sb.append("- pattern: 'Hadoop<service=LlapDaemon, name=LlapDaemonExecutorMetrics.+><>ExecutorNumExecutorsAvailable'\n");
      sb.append("  name: hadoop_llapdaemon_executornumexecutorsavailable\n");
      sb.append("  type: GAUGE\n");
      sb.append("- pattern: 'Hadoop<service=LlapDaemon, name=LlapDaemonExecutorMetrics.+><>ExecutorNumExecutors'\n");
      sb.append("  name: hadoop_llapdaemon_executornumexecutors\n");
      sb.append("  type: GAUGE\n");
      break;
    case ConfigUtils.COMPONENT_TEZAM:
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
