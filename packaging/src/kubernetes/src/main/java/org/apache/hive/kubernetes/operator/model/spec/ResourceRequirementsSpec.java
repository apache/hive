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

/** Kubernetes resource requirements specification for CPU and memory. */
public class ResourceRequirementsSpec {

  @JsonPropertyDescription("CPU request (e.g. 500m, 1)")
  private String requestsCpu = "500m";

  @JsonPropertyDescription("Memory request (e.g. 1Gi, 512Mi)")
  private String requestsMemory = "1Gi";

  @JsonPropertyDescription("CPU limit (e.g. 2, 1000m)")
  private String limitsCpu;

  @JsonPropertyDescription("Memory limit (e.g. 2Gi, 1024Mi)")
  private String limitsMemory;

  public String getRequestsCpu() {
    return requestsCpu;
  }

  public void setRequestsCpu(String requestsCpu) {
    this.requestsCpu = requestsCpu;
  }

  public String getRequestsMemory() {
    return requestsMemory;
  }

  public void setRequestsMemory(String requestsMemory) {
    this.requestsMemory = requestsMemory;
  }

  public String getLimitsCpu() {
    return limitsCpu;
  }

  public void setLimitsCpu(String limitsCpu) {
    this.limitsCpu = limitsCpu;
  }

  public String getLimitsMemory() {
    return limitsMemory;
  }

  public void setLimitsMemory(String limitsMemory) {
    this.limitsMemory = limitsMemory;
  }
}
