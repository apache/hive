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

/** Configuration for the Tez Application Master component. */
public class TezAmSpec {

  @JsonPropertyDescription("Whether Tez AM is enabled")
  private boolean enabled = false;

  @JsonPropertyDescription("Number of Tez AM replicas")
  private int replicas = 1;

  @JsonPropertyDescription("Resource requirements for Tez AM pods")
  private ResourceRequirementsSpec resources;

  @JsonPropertyDescription("Storage size for the shared scratch PVC "
      + "(ReadWriteMany) mounted on HS2 and TezAM at /opt/hive/scratch")
  private String scratchStorageSize = "1Gi";

  @JsonPropertyDescription("StorageClass for the shared scratch PVC. "
      + "Must support ReadWriteMany access. If null, uses cluster default.")
  private String scratchStorageClassName;

  @JsonPropertyDescription("Additional tez-site.xml configuration overrides")
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

  public String getScratchStorageSize() {
    return scratchStorageSize;
  }

  public void setScratchStorageSize(String scratchStorageSize) {
    this.scratchStorageSize = scratchStorageSize;
  }

  public String getScratchStorageClassName() {
    return scratchStorageClassName;
  }

  public void setScratchStorageClassName(String scratchStorageClassName) {
    this.scratchStorageClassName = scratchStorageClassName;
  }

  public Map<String, String> getConfigOverrides() {
    return configOverrides;
  }

  public void setConfigOverrides(Map<String, String> configOverrides) {
    this.configOverrides = configOverrides;
  }
}
