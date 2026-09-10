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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.kubernetes.operator.util;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;

/**
 * Small helpers over Deployment/StatefulSet that read fields without the
 * null-guard boilerplate every caller would otherwise repeat.
 */
public final class Workloads {

  private Workloads() {}

  /**
   * Returns spec.replicas from a Deployment or StatefulSet, or null when the resource is
   * absent, has no spec, or the field is unset. A non-workload resource returns null too.
   */
  public static Integer replicas(HasMetadata resource) {
    if (resource instanceof Deployment d) {
      return d.getSpec() == null ? null : d.getSpec().getReplicas();
    }
    if (resource instanceof StatefulSet s) {
      return s.getSpec() == null ? null : s.getSpec().getReplicas();
    }
    return null;
  }
}
