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

/**
 * Auto-suspend configuration. When enabled and all components are idle for the
 * configured timeout, the operator scales the entire cluster to 0 replicas.
 * Requires autoscaling to be enabled on all active components.
 */
public record AutoSuspendSpec(
    @JsonPropertyDescription("Whether auto-suspend is enabled. Requires autoscaling "
        + "to be enabled on all active components (HS2, LLAP if enabled, TezAM if enabled, "
        + "and HMS if includeMetastore is true).")
    @Default("false")
    Boolean enabled,
    @JsonPropertyDescription("Minutes of idle time (HS2=0 sessions, LLAP/TezAM at minReplicas) "
        + "before the cluster auto-suspends.")
    @Default("15")
    Integer idleTimeoutMinutes,
    @JsonPropertyDescription("Whether Metastore participates in auto-suspend. "
        + "When false, HMS stays at minReplicas during suspend and HMS autoscaling "
        + "is not required for auto-suspend to activate.")
    @Default("true")
    Boolean includeMetastore) {

  public AutoSuspendSpec {
    enabled = enabled != null && enabled;
    idleTimeoutMinutes = idleTimeoutMinutes != null ? idleTimeoutMinutes : 15;
    includeMetastore = includeMetastore == null || includeMetastore;
  }

  public boolean isEnabled() {
    return enabled;
  }
}
