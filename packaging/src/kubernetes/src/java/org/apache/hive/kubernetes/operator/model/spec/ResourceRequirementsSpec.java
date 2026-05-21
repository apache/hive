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

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import io.fabric8.generator.annotation.Default;

/** Kubernetes resource requirements specification for CPU and memory. */
public record ResourceRequirementsSpec(
    @JsonPropertyDescription("CPU request (e.g. 500m, 1)")
    @Default("500m")
    String requestsCpu,
    @JsonPropertyDescription("Memory request (e.g. 1Gi, 512Mi)")
    @Default("1Gi")
    String requestsMemory,
    @JsonPropertyDescription("CPU limit (e.g. 2, 1000m)")
    String limitsCpu,
    @JsonPropertyDescription("Memory limit (e.g. 2Gi, 1024Mi)")
    String limitsMemory) {

  public ResourceRequirementsSpec {
    requestsCpu = requestsCpu != null ? requestsCpu : "500m";
    requestsMemory = requestsMemory != null ? requestsMemory : "1Gi";
  }
}
