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
package org.apache.hive.kubernetes.operator.reconciler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.apache.hive.kubernetes.operator.dependent.LlapResourceBuilder;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.HiveClusterSpec;
import org.apache.hive.kubernetes.operator.model.spec.LlapSpec;
import org.apache.hive.kubernetes.operator.model.spec.UpdateStrategy;
import org.apache.hive.kubernetes.operator.model.spec.ZookeeperSpec;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;

/**
 * How a pod template change reaches the running daemons. A StatefulSet's RollingUpdate is
 * strictly ordinal -- podManagementPolicy governs scaling and creation, not updates -- so the
 * Recreate strategy switches it to OnDelete and lets the reconciler replace them in one pass.
 */
public class TestHiveClusterRolloutStrategy {

  private static final String NS = "hive-test";
  private static final String CLUSTER = "hive";

  private HiveCluster cluster(UpdateStrategy updateStrategy) {
    LlapSpec llap = new LlapSpec("llap0", 3, null, null, null, null, null, null, null,
        true, null, null, null, null, null, null);
    HiveClusterSpec spec = new HiveClusterSpec("hive:test", null, null, null, List.of(llap),
        null, null, new ZookeeperSpec("zk-0:2181"), null, null, null, null, null, null,
        updateStrategy, null, null);
    HiveCluster hiveCluster = new HiveCluster();
    hiveCluster.setMetadata(
        new ObjectMetaBuilder().withName(CLUSTER).withNamespace(NS).build());
    hiveCluster.setSpec(spec);
    return hiveCluster;
  }

  private LlapSpec llapOf(HiveCluster hiveCluster) {
    return hiveCluster.getSpec().llapClusters().get(0);
  }

  /** An unset field keeps the behaviour a live cluster already has. */
  @Test
  public void theDefaultIsRollingUpdate() {
    assertEquals(UpdateStrategy.RollingUpdate, cluster(null).getSpec().updateStrategy());
    assertFalse(cluster(null).getSpec().recreateOnUpdate());
  }

  /**
   * The constants are named as the custom resource spells them, so renaming one to Java's usual
   * SCREAMING_CASE would silently stop every existing CR from parsing.
   */
  @Test
  public void theCustomResourceSpellingMapsToTheEnum() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    assertEquals(UpdateStrategy.Recreate,
        mapper.readValue("\"Recreate\"", UpdateStrategy.class));
    assertEquals(UpdateStrategy.RollingUpdate,
        mapper.readValue("\"RollingUpdate\"", UpdateStrategy.class));
    assertThrows(JsonMappingException.class,
        () -> mapper.readValue("\"Recreat\"", UpdateStrategy.class));
  }

  @Test
  public void recreateIsAccepted() {
    assertEquals(UpdateStrategy.Recreate,
        cluster(UpdateStrategy.Recreate).getSpec().updateStrategy());
    assertTrue(cluster(UpdateStrategy.Recreate).getSpec().recreateOnUpdate());
  }

  /**
   * OnDelete is what makes the bulk replacement possible: Kubernetes records the new revision
   * but leaves the pods for the reconciler to delete.
   */
  @Test
  public void recreateMakesTheStatefulSetOnDelete() {
    HiveCluster hiveCluster = cluster(UpdateStrategy.Recreate);
    assertEquals("OnDelete", LlapResourceBuilder
        .buildStatefulSet(hiveCluster, llapOf(hiveCluster), 3)
        .getSpec().getUpdateStrategy().getType());
  }

  @Test
  public void rollingUpdateLeavesTheStatefulSetRolling() {
    HiveCluster hiveCluster = cluster(null);
    assertEquals("RollingUpdate", LlapResourceBuilder
        .buildStatefulSet(hiveCluster, llapOf(hiveCluster), 3)
        .getSpec().getUpdateStrategy().getType());
  }

  /** A surging rollout would briefly run a second AM against the same ZooKeeper registration. */
  @Test
  public void recreateMakesTheTezAmDeploymentRecreate() {
    HiveCluster hiveCluster = cluster(UpdateStrategy.Recreate);
    assertEquals("Recreate", LlapResourceBuilder
        .buildTezAmDeployment(hiveCluster, llapOf(hiveCluster), 1)
        .getSpec().getStrategy().getType());
  }

  @Test
  public void rollingUpdateLeavesTheTezAmDeploymentRolling() {
    HiveCluster hiveCluster = cluster(null);
    assertEquals("RollingUpdate", LlapResourceBuilder
        .buildTezAmDeployment(hiveCluster, llapOf(hiveCluster), 1)
        .getSpec().getStrategy().getType());
  }
}
