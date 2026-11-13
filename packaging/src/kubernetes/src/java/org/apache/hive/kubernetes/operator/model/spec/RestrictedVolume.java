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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.kubernetes.operator.model.spec;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import io.fabric8.generator.annotation.Required;
import io.fabric8.generator.annotation.ValidationRule;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSource;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimVolumeSource;
import io.fabric8.kubernetes.api.model.SecretVolumeSource;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;

/**
 * A restricted pod volume source for HiveCluster specs. Only types that
 * reference namespaced API objects or pod-local storage are permitted.
 */
@ValidationRule(
    value = "(has(self.configMap) ? 1 : 0) + (has(self.secret) ? 1 : 0) "
        + "+ (has(self.emptyDir) ? 1 : 0) + (has(self.persistentVolumeClaim) ? 1 : 0) == 1",
    message = "must specify exactly one of configMap, secret, emptyDir, persistentVolumeClaim")
public class RestrictedVolume {
  @Required
  @JsonPropertyDescription("Volume name, referenced by volumeMounts")
  public String name;

  @JsonPropertyDescription("ConfigMap volume source")
  public ConfigMapVolumeSource configMap;

  @JsonPropertyDescription("Secret volume source")
  public SecretVolumeSource secret;

  @JsonPropertyDescription("EmptyDir volume source")
  public EmptyDirVolumeSource emptyDir;

  @JsonPropertyDescription("PersistentVolumeClaim volume source")
  public PersistentVolumeClaimVolumeSource persistentVolumeClaim;

  public Volume toKubernetesVolume() {
    return new VolumeBuilder()
        .withName(name)
        .withConfigMap(configMap)
        .withSecret(secret)
        .withEmptyDir(emptyDir)
        .withPersistentVolumeClaim(persistentVolumeClaim)
        .build();
  }

  public static List<Volume> toKubernetesVolumes(List<RestrictedVolume> volumes) {
    if (volumes == null || volumes.isEmpty()) {
      return List.of();
    }
    return volumes.stream().map(RestrictedVolume::toKubernetesVolume).toList();
  }
}
