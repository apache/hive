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
package org.apache.hive.kubernetes.operator.dependent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hive.kubernetes.operator.model.HiveClusterSpec;
import org.junit.jupiter.api.Test;

/**
 * The shape every component's workload is applied with. The rollingUpdate sub-object has to be
 * present under RollingUpdate so this field manager owns it: the API server defaults it either
 * way, and a field nobody owns survives a server-side apply, where validation then rejects it
 * alongside OnDelete or Recreate -- which is what stops an existing cluster adopting Recreate.
 */
public class TestUpdateStrategyShape {

  private HiveClusterSpec spec(boolean recreate) {
    HiveClusterSpec spec = mock(HiveClusterSpec.class);
    when(spec.recreateOnUpdate()).thenReturn(recreate);
    return spec;
  }

  @Test
  public void aRollingDeploymentOwnsItsRollingUpdate() {
    var strategy = HiveDependentResource.deploymentStrategy(spec(false));

    assertEquals("RollingUpdate", strategy.getType());
    assertNotNull(strategy.getRollingUpdate());
    assertEquals("25%", strategy.getRollingUpdate().getMaxSurge().getStrVal());
    assertEquals("25%", strategy.getRollingUpdate().getMaxUnavailable().getStrVal());
  }

  /** Left in place, the apply is rejected: the two may not be set together. */
  @Test
  public void aRecreateDeploymentDropsIt() {
    var strategy = HiveDependentResource.deploymentStrategy(spec(true));

    assertEquals("Recreate", strategy.getType());
    assertNull(strategy.getRollingUpdate());
  }

  @Test
  public void aRollingStatefulSetOwnsItsRollingUpdate() {
    var strategy = HiveDependentResource.statefulSetUpdateStrategy(spec(false));

    assertEquals("RollingUpdate", strategy.getType());
    assertNotNull(strategy.getRollingUpdate());
    assertEquals(0, strategy.getRollingUpdate().getPartition());
  }

  /** A StatefulSet has no Recreate; OnDelete is what leaves the pods to the reconciler. */
  @Test
  public void anOnDeleteStatefulSetDropsIt() {
    var strategy = HiveDependentResource.statefulSetUpdateStrategy(spec(true));

    assertEquals("OnDelete", strategy.getType());
    assertNull(strategy.getRollingUpdate());
  }
}
