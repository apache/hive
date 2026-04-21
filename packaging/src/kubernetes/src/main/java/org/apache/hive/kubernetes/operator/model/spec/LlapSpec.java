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

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;

/** Configuration for LLAP (Live Long and Process) daemons. */
public class LlapSpec {

  @JsonPropertyDescription("Whether LLAP is enabled")
  private boolean enabled = false;

  @JsonPropertyDescription("Number of LLAP daemon replicas")
  private int replicas = 2;

  @JsonPropertyDescription("Resource requirements for LLAP daemon pods")
  private ResourceRequirementsSpec resources;

  @JsonPropertyDescription("Number of LLAP executors per daemon")
  private int executors = 1;

  @JsonPropertyDescription("Memory in MB per LLAP daemon instance")
  private int memoryMb = 2048;

  @JsonPropertyDescription(
      "LLAP service hosts identifier for ZooKeeper registration")
  private String serviceHosts = "@llap0";

  @JsonPropertyDescription(
      "Additional llap-daemon-site.xml configuration overrides")
  private Map<String, String> configOverrides;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public int getReplicas() {
    return replicas;
  }

  public void setReplicas(int replicas) {
    this.replicas = replicas;
  }

  public ResourceRequirementsSpec getResources() {
    return resources;
  }

  public void setResources(ResourceRequirementsSpec resources) {
    this.resources = resources;
  }

  public int getExecutors() {
    return executors;
  }

  public void setExecutors(int executors) {
    this.executors = executors;
  }

  public int getMemoryMb() {
    return memoryMb;
  }

  public void setMemoryMb(int memoryMb) {
    this.memoryMb = memoryMb;
  }

  public String getServiceHosts() {
    return serviceHosts;
  }

  public void setServiceHosts(String serviceHosts) {
    this.serviceHosts = serviceHosts;
  }

  public Map<String, String> getConfigOverrides() {
    return configOverrides;
  }

  public void setConfigOverrides(Map<String, String> configOverrides) {
    this.configOverrides = configOverrides;
  }
}
