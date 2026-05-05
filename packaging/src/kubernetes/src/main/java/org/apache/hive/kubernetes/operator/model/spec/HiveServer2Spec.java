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

/** Configuration for the HiveServer2 component. */
public class HiveServer2Spec {

  @JsonPropertyDescription("Number of HiveServer2 replicas")
  private int replicas = 1;

  @JsonPropertyDescription("Resource requirements for HiveServer2 pods")
  private ResourceRequirementsSpec resources;

  @JsonPropertyDescription(
      "Additional hive-site.xml configuration overrides as key-value pairs")
  private Map<String, String> configOverrides;

  @JsonPropertyDescription(
      "Kubernetes Service type: ClusterIP, LoadBalancer, or NodePort")
  private String serviceType = "ClusterIP";

  @JsonPropertyDescription("HiveServer2 Thrift port")
  private int thriftPort = 10000;

  @JsonPropertyDescription("HiveServer2 Web UI port")
  private int webUiPort = 10002;

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

  public Map<String, String> getConfigOverrides() {
    return configOverrides;
  }

  public void setConfigOverrides(Map<String, String> configOverrides) {
    this.configOverrides = configOverrides;
  }

  public String getServiceType() {
    return serviceType;
  }

  public void setServiceType(String serviceType) {
    this.serviceType = serviceType;
  }

  public int getThriftPort() {
    return thriftPort;
  }

  public void setThriftPort(int thriftPort) {
    this.thriftPort = thriftPort;
  }

  public int getWebUiPort() {
    return webUiPort;
  }

  public void setWebUiPort(int webUiPort) {
    this.webUiPort = webUiPort;
  }
}
