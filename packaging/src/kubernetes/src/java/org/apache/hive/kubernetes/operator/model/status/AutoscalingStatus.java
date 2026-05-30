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

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Autoscaling status for a component, surfacing the operator's scaling decisions
 * in the HiveCluster status subresource (replaces kubectl get hpa).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AutoscalingStatus {

  private int currentMetricValue;
  private Integer scaleUpThreshold;
  private int proposedReplicas;
  private String lastScaleTime;

  public int getCurrentMetricValue() {
    return currentMetricValue;
  }

  public void setCurrentMetricValue(int currentMetricValue) {
    this.currentMetricValue = currentMetricValue;
  }

  public Integer getScaleUpThreshold() {
    return scaleUpThreshold;
  }

  public void setScaleUpThreshold(Integer scaleUpThreshold) {
    this.scaleUpThreshold = scaleUpThreshold;
  }

  public int getProposedReplicas() {
    return proposedReplicas;
  }

  public void setProposedReplicas(int proposedReplicas) {
    this.proposedReplicas = proposedReplicas;
  }

  public String getLastScaleTime() {
    return lastScaleTime;
  }

  public void setLastScaleTime(String lastScaleTime) {
    this.lastScaleTime = lastScaleTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AutoscalingStatus that = (AutoscalingStatus) o;
    return currentMetricValue == that.currentMetricValue
        && Objects.equals(scaleUpThreshold, that.scaleUpThreshold)
        && proposedReplicas == that.proposedReplicas
        && Objects.equals(lastScaleTime, that.lastScaleTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(currentMetricValue, scaleUpThreshold,
        proposedReplicas, lastScaleTime);
  }
}
