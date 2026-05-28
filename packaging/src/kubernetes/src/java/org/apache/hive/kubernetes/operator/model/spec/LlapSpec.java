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

package org.apache.hive.kubernetes.operator.model.spec;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import io.fabric8.crd.generator.annotation.PreserveUnknownFields;
import io.fabric8.crd.generator.annotation.SchemaFrom;
import io.fabric8.generator.annotation.Default;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;

/** Configuration for LLAP (Live Long and Process) daemons. */
public record LlapSpec(
    @JsonPropertyDescription("Number of replicas")
    @Default("1")
    Integer replicas,
    @JsonPropertyDescription("Resource requirements for pods")
    ResourceRequirementsSpec resources,
    @JsonPropertyDescription("Additional configuration overrides as key-value pairs")
    Map<String, String> configOverrides,
    @JsonPropertyDescription("Additional volumes to attach to the pod (e.g., for keytabs or truststores)")
    @SchemaFrom(type = Object[].class) @PreserveUnknownFields
    List<Volume> extraVolumes,
    @JsonPropertyDescription("Additional volume mounts for the container")
    @SchemaFrom(type = Object[].class) @PreserveUnknownFields
    List<VolumeMount> extraVolumeMounts,
    @JsonPropertyDescription("Whether LLAP is enabled")
    @Default("true")
    Boolean enabled,
    @JsonPropertyDescription("Number of LLAP executors per daemon")
    @Default("1")
    Integer executors,
    @JsonPropertyDescription("Memory in MB per LLAP daemon instance")
    @Default("1024")
    Integer memoryMb,
    @JsonPropertyDescription("LLAP service hosts identifier for ZooKeeper registration")
    String serviceHosts,
    @JsonPropertyDescription("Readiness probe configuration")
    ProbeSpec readinessProbe,
    @JsonPropertyDescription("Autoscaling configuration (requires KEDA installed in the cluster)")
    AutoscalingSpec autoscaling) {

  public LlapSpec {
    replicas = replicas != null ? replicas : 1;
    enabled = enabled != null ? enabled : true;
    executors = executors != null ? executors : 1;
    memoryMb = memoryMb != null ? memoryMb : 1024;
    serviceHosts = serviceHosts != null ? serviceHosts : "@llap0";
    extraVolumes = extraVolumes != null ? extraVolumes : List.of();
    extraVolumeMounts = extraVolumeMounts != null ? extraVolumeMounts : List.of();
    autoscaling = autoscaling != null ? autoscaling : new AutoscalingSpec(
        false, 0, 1, 0, null, null, 900, 600);
  }

  public boolean isEnabled() {
    return enabled;
  }
}
