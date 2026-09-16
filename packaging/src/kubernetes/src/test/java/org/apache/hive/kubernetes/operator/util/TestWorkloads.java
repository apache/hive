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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class TestWorkloads {

  @Test
  void replicasReturnsValueFromDeployment() {
    Deployment d = new DeploymentBuilder()
        .withNewSpec().withReplicas(3).endSpec()
        .build();
    assertEquals(3, Workloads.replicas(d));
  }

  @Test
  void replicasReturnsValueFromStatefulSet() {
    StatefulSet s = new StatefulSetBuilder()
        .withNewSpec().withReplicas(5).endSpec()
        .build();
    assertEquals(5, Workloads.replicas(s));
  }

  @Test
  void replicasReturnsNullWhenDeploymentSpecMissing() {
    // fabric8 Deployment with no spec set at all
    Deployment d = new DeploymentBuilder().build();
    assertNull(Workloads.replicas(d));
  }

  @Test
  void replicasReturnsNullWhenStatefulSetSpecMissing() {
    StatefulSet s = new StatefulSetBuilder().build();
    assertNull(Workloads.replicas(s));
  }

  @Test
  void replicasReturnsNullWhenDeploymentReplicasUnset() {
    // spec present, but replicas field not set
    Deployment d = new DeploymentBuilder().withNewSpec().endSpec().build();
    assertNull(Workloads.replicas(d));
  }

  @Test
  void replicasReturnsNullWhenStatefulSetReplicasUnset() {
    StatefulSet s = new StatefulSetBuilder().withNewSpec().endSpec().build();
    assertNull(Workloads.replicas(s));
  }

  @Test
  void replicasReturnsNullForNonWorkloadResource() {
    // Anything that's neither a Deployment nor a StatefulSet returns null,
    // even if it happens to have a "spec" (e.g., a ConfigMap here has none).
    ConfigMap cm = new ConfigMapBuilder()
        .withNewMetadata().withName("cm").endMetadata()
        .build();
    assertNull(Workloads.replicas(cm));
  }

  @Test
  void replicasReturnsNullForNullResource() {
    assertNull(Workloads.replicas(null));
  }

  /**
   * Covers the three branches of {@link Workloads#nameFor}:
   * <ul>
   *   <li>{@code llap-{name}}  → {@code {cluster}-{name}}</li>
   *   <li>{@code tezam-{name}} → {@code {cluster}-tezam-{name}}</li>
   *   <li>otherwise            → {@code {cluster}-{component}}</li>
   * </ul>
   * The last two rows exercise the "no dash after the prefix" fall-through: a bare
   * {@code "llap"} or {@code "tezam"} isn't a per-cluster component key and must land in the
   * plain-{cluster}-{component} branch, not be treated as an empty LLAP name.
   */
  @ParameterizedTest
  @CsvSource({
      // component,                expected workload name
      "llap-llap0,                 hive-llap0",
      "llap-my-llap-cluster,       hive-my-llap-cluster",
      "tezam-llap0,                hive-tezam-llap0",
      "tezam-my-llap-cluster,      hive-tezam-my-llap-cluster",
      "hiveserver2,                hive-hiveserver2",
      "metastore,                  hive-metastore",
      "llap,                       hive-llap",
      "tezam,                      hive-tezam",
  })
  void nameForMapsComponentKeyToWorkloadName(String component, String expected) {
    HiveCluster hc = hiveCluster("hive");
    assertEquals(expected, Workloads.nameFor(hc, component));
  }

  private static HiveCluster hiveCluster(String name) {
    HiveCluster hc = new HiveCluster();
    hc.setMetadata(new ObjectMetaBuilder().withName(name).build());
    return hc;
  }
}
