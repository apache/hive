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

/** Kubernetes probe (liveness/readiness) timing configurations. */
public record ProbeSpec(
    @JsonPropertyDescription("Number of seconds after the container has started before probes are initiated.")
    Integer initialDelaySeconds,
    @JsonPropertyDescription("How often (in seconds) to perform the probe.")
    Integer periodSeconds,
    @JsonPropertyDescription("Number of seconds after which the probe times out.")
    Integer timeoutSeconds,
    @JsonPropertyDescription("Minimum consecutive failures for the probe to be considered failed after having succeeded.")
    Integer failureThreshold,
    @JsonPropertyDescription("Minimum consecutive successes for the probe to be considered successful after having failed.")
    Integer successThreshold) {
}
