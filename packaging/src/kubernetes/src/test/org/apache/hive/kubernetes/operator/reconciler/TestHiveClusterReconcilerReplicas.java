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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hive.kubernetes.operator.autoscaling.HiveClusterAutoscaler;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.HiveClusterSpec;
import org.apache.hive.kubernetes.operator.model.spec.AutoscalingSpec;
import org.apache.hive.kubernetes.operator.model.spec.LlapSpec;
import org.apache.hive.kubernetes.operator.util.ConfigUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * How many LLAP daemons a reconcile asks for. The autoscaler's managed-replica store is static
 * and process-lived, so a value put there outlives any single reconcile; these pin which source
 * wins when both have an opinion.
 */
public class TestHiveClusterReconcilerReplicas {

  private static final String NS = "hive-test";
  private static final String CLUSTER = "hive";
  private static final String LLAP_NAME = "llap0";

  private HiveClusterReconciler reconciler;

  @BeforeEach
  public void setUp() {
    reconciler = new HiveClusterReconciler();
    HiveClusterAutoscaler.clearManagedReplicas(NS, CLUSTER,
        ConfigUtils.llapComponentKey(LLAP_NAME));
    HiveClusterAutoscaler.clearManagedReplicas(NS, CLUSTER,
        ConfigUtils.tezAmComponentKey(LLAP_NAME));
  }

  private LlapSpec llapSpec(int replicas, boolean autoscalingEnabled) {
    return llapSpec(replicas, autoscalingEnabled, 1, null);
  }

  /**
   * @param llapMinReplicas the LLAP autoscaler's floor, which doubles as the count before it has
   *                        evaluated anything
   * @param tezAm the paired TezAM, or null for one that is static with a single replica
   */
  private LlapSpec llapSpec(int replicas, boolean autoscalingEnabled, int llapMinReplicas,
      LlapSpec.LlapTezAmSpec tezAm) {
    // Built before the stubbing below: nesting a mock() inside when(...) starts a second
    // stubbing that Mockito rejects as unfinished.
    LlapSpec.LlapTezAmSpec pairedTezAm = tezAm != null ? tezAm : tezAmSpec(1, false, 1);
    AutoscalingSpec autoscaling = mock(AutoscalingSpec.class);
    when(autoscaling.isEnabled()).thenReturn(autoscalingEnabled);
    when(autoscaling.minReplicas()).thenReturn(llapMinReplicas);
    LlapSpec llap = mock(LlapSpec.class);
    when(llap.name()).thenReturn(LLAP_NAME);
    when(llap.replicas()).thenReturn(replicas);
    when(llap.autoscaling()).thenReturn(autoscaling);
    when(llap.tezAm()).thenReturn(pairedTezAm);
    return llap;
  }

  private LlapSpec.LlapTezAmSpec tezAmSpec(int replicas, boolean autoscalingEnabled,
      int minReplicas) {
    AutoscalingSpec autoscaling = mock(AutoscalingSpec.class);
    when(autoscaling.isEnabled()).thenReturn(autoscalingEnabled);
    when(autoscaling.minReplicas()).thenReturn(minReplicas);
    LlapSpec.LlapTezAmSpec tezAm = mock(LlapSpec.LlapTezAmSpec.class);
    when(tezAm.replicas()).thenReturn(replicas);
    when(tezAm.autoscaling()).thenReturn(autoscaling);
    return tezAm;
  }

  private HiveCluster cluster(boolean suspended) {
    HiveClusterSpec spec = mock(HiveClusterSpec.class);
    when(spec.suspend()).thenReturn(suspended);
    HiveCluster hc = mock(HiveCluster.class);
    when(hc.getSpec()).thenReturn(spec);
    return hc;
  }

  /**
   * The regression: waking a cluster used to copy the spec's replica count into the managed
   * store even with autoscaling off, and the store was read first. A later resize then landed
   * in the custom resource and never reached the StatefulSet -- silently, for as long as the
   * operator process lived.
   */
  @Test
  public void aStaleManagedCountDoesNotOutrankTheSpec() {
    HiveClusterAutoscaler.setManagedReplicas(NS, CLUSTER,
        ConfigUtils.llapComponentKey(LLAP_NAME), 10);

    assertEquals(24,
        reconciler.resolveLlapReplicaCount(cluster(false), llapSpec(24, false), NS, CLUSTER));
  }

  /** And the stale entry is gone, so nothing can resurrect it on a later reconcile. */
  @Test
  public void aStaleManagedCountIsForgotten() {
    HiveClusterAutoscaler.setManagedReplicas(NS, CLUSTER,
        ConfigUtils.llapComponentKey(LLAP_NAME), 10);
    reconciler.resolveLlapReplicaCount(cluster(false), llapSpec(24, false), NS, CLUSTER);

    assertEquals(null, HiveClusterAutoscaler.getManagedReplicas(NS, CLUSTER,
        ConfigUtils.llapComponentKey(LLAP_NAME)));
  }

  /** With autoscaling on, the autoscaler owns the count and the spec must not override it. */
  @Test
  public void anAutoscaledComponentKeepsItsManagedCount() {
    HiveClusterAutoscaler.setManagedReplicas(NS, CLUSTER,
        ConfigUtils.llapComponentKey(LLAP_NAME), 10);

    assertEquals(10,
        reconciler.resolveLlapReplicaCount(cluster(false), llapSpec(24, true), NS, CLUSTER));
  }

  /** Before the autoscaler's first evaluation there is no decision yet, so it starts at the floor. */
  @Test
  public void anAutoscaledComponentStartsAtMinReplicas() {
    assertEquals(1,
        reconciler.resolveLlapReplicaCount(cluster(false), llapSpec(24, true), NS, CLUSTER));
  }

  /** A suspended cluster holds everything at zero whatever either source says. */
  @Test
  public void aSuspendedClusterResolvesToZero() {
    HiveClusterAutoscaler.setManagedReplicas(NS, CLUSTER,
        ConfigUtils.llapComponentKey(LLAP_NAME), 10);

    assertEquals(0,
        reconciler.resolveLlapReplicaCount(cluster(true), llapSpec(24, false), NS, CLUSTER));
  }

  /**
   * The same stale entry, reached through the TezAM: it follows LLAP, so a count left behind for
   * a LLAP cluster nobody autoscales must not decide the AM's size either.
   */
  @Test
  public void aStaleLlapCountDoesNotGateTheTezAm() {
    HiveClusterAutoscaler.setManagedReplicas(NS, CLUSTER,
        ConfigUtils.llapComponentKey(LLAP_NAME), 0);
    LlapSpec llap = llapSpec(24, false, 1, tezAmSpec(2, false, 1));

    assertEquals(24, reconciler.resolveLlapReplicaCount(cluster(false), llap, NS, CLUSTER));
    assertEquals(2, reconciler.resolveTezAmReplicaCount(cluster(false), NS, CLUSTER, llap));
  }

  /**
   * An idle autoscaled cluster settles at LLAP 0 while the TezAM autoscaler still wants one AM
   * per session. Gating the AM on LLAP here would write 0, the autoscaler would patch it back,
   * and the pod would churn every scrape interval.
   */
  @Test
  public void anAutoscaledTezAmKeepsItsManagedCountWhileLlapIsIdle() {
    HiveClusterAutoscaler.setManagedReplicas(NS, CLUSTER,
        ConfigUtils.llapComponentKey(LLAP_NAME), 0);
    HiveClusterAutoscaler.setManagedReplicas(NS, CLUSTER,
        ConfigUtils.tezAmComponentKey(LLAP_NAME), 3);
    LlapSpec llap = llapSpec(24, true, 0, tezAmSpec(1, true, 1));

    assertEquals(3, reconciler.resolveTezAmReplicaCount(cluster(false), NS, CLUSTER, llap));
  }

  /** Without daemons an AM has nothing to schedule onto, so it stays down with them. */
  @Test
  public void theTezAmFollowsAnAutoscaledLlapToZero() {
    HiveClusterAutoscaler.setManagedReplicas(NS, CLUSTER,
        ConfigUtils.llapComponentKey(LLAP_NAME), 0);
    LlapSpec llap = llapSpec(24, true, 0, tezAmSpec(2, false, 1));

    assertEquals(0, reconciler.resolveTezAmReplicaCount(cluster(false), NS, CLUSTER, llap));
  }

  /** Before the LLAP autoscaler has evaluated anything, a floor of 0 keeps the AM down too. */
  @Test
  public void theTezAmStaysDownUntilAnAutoscaledLlapHasEvaluated() {
    LlapSpec llap = llapSpec(24, true, 0, tezAmSpec(2, false, 1));

    assertEquals(0, reconciler.resolveTezAmReplicaCount(cluster(false), NS, CLUSTER, llap));
  }
}
