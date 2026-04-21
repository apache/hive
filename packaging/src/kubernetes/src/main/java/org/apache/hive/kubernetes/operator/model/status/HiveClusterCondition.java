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

package org.apache.hive.kubernetes.operator.model.status;

/**
 * Standard Kubernetes-style condition for the HiveCluster status.
 * Condition types: Ready, MetastoreReady, HiveServer2Ready, SchemaInitialized.
 */
public class HiveClusterCondition {

  private String type;
  private String status;
  private String reason;
  private String message;
  private String lastTransitionTime;

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String getReason() {
    return reason;
  }

  public void setReason(String reason) {
    this.reason = reason;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public String getLastTransitionTime() {
    return lastTransitionTime;
  }

  public void setLastTransitionTime(String lastTransitionTime) {
    this.lastTransitionTime = lastTransitionTime;
  }

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    HiveClusterCondition that = (HiveClusterCondition) o;
    return java.util.Objects.equals(type, that.type) &&
           java.util.Objects.equals(status, that.status) &&
           java.util.Objects.equals(reason, that.reason) &&
           java.util.Objects.equals(message, that.message) &&
           java.util.Objects.equals(lastTransitionTime, that.lastTransitionTime);
  }

  public int hashCode() {
    return java.util.Objects.hash(type, status, reason, message, lastTransitionTime);
  }
}
