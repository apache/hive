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
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.Matcher;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.spec.DatabaseConfig;
import org.apache.hive.kubernetes.operator.model.spec.ResourceRequirementsSpec;

import org.apache.hive.kubernetes.operator.model.spec.SecretKeyRef;
import org.apache.hive.kubernetes.operator.model.spec.ProbeSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all Hive operator dependent resources.
 * <p>
 * Overrides {@link #getSecondaryResource} to use this dependent's own
 * event source instead of the generic type-based lookup. This is
 * required because JOSDK 4.9.x's default implementation calls
 * {@code context.getSecondaryResource(type)} which throws when
 * multiple dependents manage the same Kubernetes resource type
 * (e.g. multiple ConfigMap or Service dependents).
 */
public abstract class HiveDependentResource<R extends HasMetadata,
    P extends HasMetadata>
    extends CRUDKubernetesDependentResource<R, P> {

  private static final Logger LOG =
      LoggerFactory.getLogger(HiveDependentResource.class);
  private static final ObjectMapper MAPPER = new ObjectMapper()
      .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

  protected static final String CONF_MOUNT_PATH = "/etc/hive/conf";
  protected static final String HIVE_CONF_DIR = "/opt/hive/conf";
  protected static final String EXT_JARS_PATH = "/tmp/ext-jars";
  /**
   * Stores SHA-256 hashes of the last desired spec that was applied
   * for each resource, keyed by namespace/name.  This lets us skip
   * updates when the desired state has not changed, avoiding the
   * annotation writes and generation bumps that cause infinite
   * reconciliation loops on Docker Desktop Kubernetes.
   */
  private static final ConcurrentHashMap<String, String>
      LAST_DESIRED_HASHES = new ConcurrentHashMap<>();

  protected HiveDependentResource(Class<R> resourceType) {
    super(resourceType);
  }

  /**
   * Disable Server-Side Apply. SSA on Docker Desktop Kubernetes causes
   * dual ReplicaSet creation (two SSA applies within the same second
   * produce different pod template hashes). Standard create/update
   * combined with our custom hash-based {@link #match} is sufficient.
   */
  @Override
  protected boolean useSSA(Context<P> context) {
    return false;
  }

  @Override
  public Optional<R> getSecondaryResource(P primary,
      Context<P> context) {
    return eventSource()
        .flatMap(es -> es.getSecondaryResource(primary));
  }

  /**
   * Custom match that compares an SHA-256 hash of the desired resource
   * spec against the last applied hash.  Overrides the 3-arg entry
   * point because that is what JOSDK's reconcile loop actually calls.
   * <p>
   * The parent's 3-arg match delegates to a 5-arg method that calls
   * {@code addMetadata()} <em>unconditionally</em> — writing the
   * {@code javaoperatorsdk.io/previous} annotation on every
   * reconciliation.  On Docker Desktop, that annotation write bumps
   * {@code metadata.generation}, which triggers a new informer event,
   * causing an infinite reconciliation loop.
   * <p>
   * By intercepting here we avoid both the annotation write and the
   * false-positive diffs from K8s-injected defaults (protocol: TCP,
   * terminationGracePeriodSeconds, etc.) when the desired spec has
   * not actually changed.
   */
  @Override
  public Matcher.Result<R> match(R actual, P primary,
      Context<P> context) {
    R desired = desired(primary, context);
    String resourceKey = desired.getKind()
        + "/" + desired.getMetadata().getNamespace()
        + "/" + desired.getMetadata().getName();
    String desiredHash = computeHash(desired);
    if (actual == null) {
      if (desiredHash != null) {
        String previousHash = LAST_DESIRED_HASHES.get(resourceKey);
        if (Objects.equals(previousHash, desiredHash)) {
          // Resource was created in a previous reconciliation but
          // the informer hasn't indexed it yet.  Returning false
          // would trigger another SSA apply, which fires another
          // informer event, creating an infinite reconciliation
          // loop on Docker Desktop.  Skip the re-creation.
          LOG.debug("Resource {} already created (informer lag), "
              + "skipping re-create", resourceKey);
          return Matcher.Result.computed(true, desired);
        }
        // First creation — cache the hash so the next
        // reconciliation can detect informer lag.
        LOG.info("Creating resource {}", resourceKey);
        LAST_DESIRED_HASHES.put(resourceKey, desiredHash);
      }
      return Matcher.Result.computed(false, desired);
    }
    if (desiredHash == null) {
      // Serialization failed — delegate to parent which will
      // call addMetadata + the real matcher
      return super.match(actual, primary, context);
    }
    // Jobs and PVCs are immutable after creation — never update.
    String kind = actual.getKind();
    if ("Job".equals(kind) || "PersistentVolumeClaim".equals(kind)) {
      LAST_DESIRED_HASHES.put(resourceKey, desiredHash);
      return Matcher.Result.computed(true, desired);
    }
    String previousHash = LAST_DESIRED_HASHES.get(resourceKey);
    if (previousHash == null) {
      // First reconciliation after operator start — the resource
      // already exists so seed the cache without triggering an
      // update. This prevents a gratuitous rolling update caused
      // by K8s default-value injection (protocol: TCP, etc.).
      LOG.info("Seeding hash for existing resource {}, skipping update",
          resourceKey);
      LAST_DESIRED_HASHES.put(resourceKey, desiredHash);
      return Matcher.Result.computed(true, desired);
    }
    if (desiredHash.equals(previousHash)) {
      LOG.debug("Desired spec unchanged for {}, skipping update",
          resourceKey);
      return Matcher.Result.computed(true, desired);
    }
    LOG.info("Desired spec changed for {}, will update", resourceKey);
    LAST_DESIRED_HASHES.put(resourceKey, desiredHash);
    return Matcher.Result.computed(false, desired);
  }

  private String computeHash(R resource) {
    try {
      JsonNode tree = MAPPER.valueToTree(resource);
      sortJsonNode(tree);
      String json = MAPPER.writeValueAsString(tree);
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(
          json.getBytes(StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder(64);
      for (byte b : hash) {
        sb.append(String.format("%02x", b));
      }
      return sb.toString();
    } catch (Exception e) {
      LOG.warn("Failed to compute hash for resource {}: {}",
          resource.getMetadata().getName(), e.getMessage());
      return null;
    }
  }

  /** Recursively sort all object node keys for deterministic JSON. */
  private static void sortJsonNode(JsonNode node) {
    if (node.isObject()) {
      ObjectNode obj = (ObjectNode) node;
      TreeMap<String, JsonNode> sorted = new TreeMap<>();
      Iterator<String> fieldNames = obj.fieldNames();
      while (fieldNames.hasNext()) {
        String name = fieldNames.next();
        JsonNode child = obj.get(name);
        sortJsonNode(child);
        sorted.put(name, child);
      }
      obj.removeAll();
      sorted.forEach(obj::set);
    } else if (node.isArray()) {
      ArrayNode arr = (ArrayNode) node;
      for (int i = 0; i < arr.size(); i++) {
        sortJsonNode(arr.get(i));
      }
      sortArrayNode(arr);
    }
  }

  /**
   * Sort array elements by a stable key to make hashing order-independent.
   * Uses "name" field if present (env vars, volumes, containers, ports),
   * falls back to "mountPath" (volume mounts), then serialized form.
   */
  private static void sortArrayNode(ArrayNode arr) {
    if (arr.size() <= 1 || !arr.get(0).isObject()) {
      return;
    }

    List<JsonNode> sortedElements = StreamSupport.stream(arr.spliterator(), false)
        .sorted(Comparator.comparing(node ->
            node.has("name") ? node.get("name").asText() :
            node.has("mountPath") ? node.get("mountPath").asText() :
            node.toString()
        ))
        .collect(Collectors.toList());

    arr.removeAll();
    sortedElements.forEach(arr::add);
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
        cmd.append("wget -q -P ").append(targetDir).append(" '").append(jarUrl).append("' && ");
      } else {
        cmd.append("hadoop fs -copyToLocal '").append(jarUrl).append("' ").append(targetDir).append("/ && ");
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

}
