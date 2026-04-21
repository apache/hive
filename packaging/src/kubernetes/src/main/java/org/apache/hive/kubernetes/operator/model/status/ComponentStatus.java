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
 * Status of an individual Hive component (Metastore, HS2, LLAP, TezAM).
 */
public class ComponentStatus {

  private int readyReplicas;
  private int desiredReplicas;
  private String phase;

  public int getReadyReplicas() {
    return readyReplicas;
  }

  public void setReadyReplicas(int readyReplicas) {
    this.readyReplicas = readyReplicas;
  }

  public int getDesiredReplicas() {
    return desiredReplicas;
  }

  public void setDesiredReplicas(int desiredReplicas) {
    this.desiredReplicas = desiredReplicas;
  }

  public String getPhase() {
    return phase;
  }

  public void setPhase(String phase) {
    this.phase = phase;
  }

  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    ComponentStatus that = (ComponentStatus) o;
    return readyReplicas == that.readyReplicas && desiredReplicas == that.desiredReplicas && java.util.Objects.equals(
        phase, that.phase);
  }

  public int hashCode() {
    return java.util.Objects.hash(readyReplicas, desiredReplicas, phase);
  }
}
