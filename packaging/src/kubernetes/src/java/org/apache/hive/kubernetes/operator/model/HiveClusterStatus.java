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

package org.apache.hive.kubernetes.operator.model;

import java.util.ArrayList;
import java.util.List;

import io.fabric8.crd.generator.annotation.PrinterColumn;
import io.fabric8.kubernetes.api.model.Condition;
import org.apache.hive.kubernetes.operator.model.status.ComponentStatus;

/** Status subresource for the HiveCluster custom resource. */
public class HiveClusterStatus {

  private List<Condition> conditions = new ArrayList<>();
  private ComponentStatus metastore;
  private ComponentStatus hiveServer2;
  private ComponentStatus llap;
  private ComponentStatus tezAm;
  private Long observedGeneration;
  @PrinterColumn(name = "Phase")
  private String clusterPhase;
  private String idleSince;
  @PrinterColumn(name = "Idle (min)")
  private Integer idleForMinutes;
  @PrinterColumn(name = "Suspended Since", priority = 1)
  private String suspendedSince;

  public List<Condition> getConditions() {
    return conditions;
  }

  public void setConditions(List<Condition> conditions) {
    this.conditions = conditions;
  }

  public ComponentStatus getMetastore() {
    return metastore;
  }

  public void setMetastore(ComponentStatus metastore) {
    this.metastore = metastore;
  }

  public ComponentStatus getHiveServer2() {
    return hiveServer2;
  }

  public void setHiveServer2(ComponentStatus hiveServer2) {
    this.hiveServer2 = hiveServer2;
  }

  public ComponentStatus getLlap() {
    return llap;
  }

  public void setLlap(ComponentStatus llap) {
    this.llap = llap;
  }

  public ComponentStatus getTezAm() {
    return tezAm;
  }

  public void setTezAm(ComponentStatus tezAm) {
    this.tezAm = tezAm;
  }

  public Long getObservedGeneration() {
    return observedGeneration;
  }

  public void setObservedGeneration(Long observedGeneration) {
    this.observedGeneration = observedGeneration;
  }

  public String getClusterPhase() {
    return clusterPhase;
  }

  public void setClusterPhase(String clusterPhase) {
    this.clusterPhase = clusterPhase;
  }

  public String getIdleSince() {
    return idleSince;
  }

  public void setIdleSince(String idleSince) {
    this.idleSince = idleSince;
  }

  public Integer getIdleForMinutes() {
    return idleForMinutes;
  }

  public void setIdleForMinutes(Integer idleForMinutes) {
    this.idleForMinutes = idleForMinutes;
  }

  public String getSuspendedSince() {
    return suspendedSince;
  }

  public void setSuspendedSince(String suspendedSince) {
    this.suspendedSince = suspendedSince;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HiveClusterStatus that = (HiveClusterStatus) o;
    return java.util.Objects.equals(observedGeneration, that.observedGeneration) &&
           java.util.Objects.equals(conditions, that.conditions) &&
           java.util.Objects.equals(metastore, that.metastore) &&
           java.util.Objects.equals(hiveServer2, that.hiveServer2) &&
           java.util.Objects.equals(llap, that.llap) &&
           java.util.Objects.equals(tezAm, that.tezAm) &&
           java.util.Objects.equals(clusterPhase, that.clusterPhase) &&
           java.util.Objects.equals(idleSince, that.idleSince) &&
           java.util.Objects.equals(idleForMinutes, that.idleForMinutes) &&
           java.util.Objects.equals(suspendedSince, that.suspendedSince);
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(conditions, metastore, hiveServer2, llap, tezAm,
        observedGeneration, clusterPhase, idleSince, idleForMinutes, suspendedSince);
  }
}
