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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.Matcher;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import org.apache.hive.kubernetes.operator.model.spec.ResourceRequirementsSpec;
import org.apache.hive.kubernetes.operator.model.spec.StorageSpec;
import org.apache.hive.kubernetes.operator.model.spec.SecretKeyRef;
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

  @Override
  public Optional<R> getSecondaryResource(P primary,
      Context<P> context) {
    return eventSource()
        .flatMap(es -> es.getSecondaryResource(primary));
  }

  /**
   * Custom match that compares a SHA-256 hash of the desired resource
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
        if (previousHash != null && previousHash.equals(desiredHash)) {
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
    String previousHash = LAST_DESIRED_HASHES.get(resourceKey);
    if (previousHash == null) {
      // First reconciliation after operator start — the resource
      // already exists so seed the cache without triggering an
      // update.  This prevents a gratuitous rolling update caused
      // by the annotation write that addMetadata() would perform.
      LOG.info("Seeding hash for existing resource {}", resourceKey);
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
    }
  }

  /**
   * Builds the {@code AWS_ACCESS_KEY_ID} and {@code AWS_SECRET_ACCESS_KEY}
   * environment variables for S3A credential propagation.
   * <p>
   * Priority order:
   * <ol>
   *   <li>{@code SecretKeyRef} &rarr; valueFrom secretKeyRef</li>
   *   <li>Plain-text fields &rarr; literal value</li>
   * </ol>
   */
  protected static List<EnvVar> buildS3CredentialEnvVars(
      StorageSpec storage) {
    if (storage == null) {
      return Collections.emptyList();
    }

    List<EnvVar> envVars = new ArrayList<>();

    // Access key: prefer SecretKeyRef, fall back to plain text
    SecretKeyRef accessRef = storage.getAccessKeySecretRef();
    if (accessRef != null) {
      envVars.add(new EnvVarBuilder()
          .withName("AWS_ACCESS_KEY_ID")
          .withNewValueFrom()
            .withNewSecretKeyRef()
              .withName(accessRef.getName())
              .withKey(accessRef.getKey())
            .endSecretKeyRef()
          .endValueFrom()
          .build());
    } else if (storage.getAccessKey() != null) {
      envVars.add(new EnvVar("AWS_ACCESS_KEY_ID",
          storage.getAccessKey(), null));
    }

    // Secret key: prefer SecretKeyRef, fall back to plain text
    SecretKeyRef secretRef = storage.getSecretKeySecretRef();
    if (secretRef != null) {
      envVars.add(new EnvVarBuilder()
          .withName("AWS_SECRET_ACCESS_KEY")
          .withNewValueFrom()
            .withNewSecretKeyRef()
              .withName(secretRef.getName())
              .withKey(secretRef.getKey())
            .endSecretKeyRef()
          .endValueFrom()
          .build());
    } else if (storage.getSecretKey() != null) {
      envVars.add(new EnvVar("AWS_SECRET_ACCESS_KEY",
          storage.getSecretKey(), null));
    }

    return envVars;
  }

  /** Builds Kubernetes ResourceRequirements from the operator's spec. */
  protected static ResourceRequirements buildResources(ResourceRequirementsSpec spec) {
    if (spec == null) {
      return new ResourceRequirements();
    }
    ResourceRequirementsBuilder builder = new ResourceRequirementsBuilder();
    if (spec.getRequestsCpu() != null) {
      builder.addToRequests("cpu", new Quantity(spec.getRequestsCpu()));
    }
    if (spec.getRequestsMemory() != null) {
      builder.addToRequests("memory", new Quantity(spec.getRequestsMemory()));
    }
    if (spec.getLimitsCpu() != null) {
      builder.addToLimits("cpu", new Quantity(spec.getLimitsCpu()));
    }
    if (spec.getLimitsMemory() != null) {
      builder.addToLimits("memory", new Quantity(spec.getLimitsMemory()));
    }
    return builder.build();
  }
}
