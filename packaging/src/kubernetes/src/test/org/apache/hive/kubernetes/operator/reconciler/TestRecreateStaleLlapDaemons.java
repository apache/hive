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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.spec.LlapSpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodListBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;

/**
 * Which LLAP daemons a Recreate rollout deletes. Under OnDelete nothing else replaces them, so
 * deleting too few leaves the cluster on the old template, and deleting before Kubernetes has
 * published the new revision would delete every pod against a revision that is still the old one.
 */
public class TestRecreateStaleLlapDaemons {

  private static final String NS = "hive-test";
  private static final String CLUSTER = "hive";
  private static final String LLAP_NAME = "llap0";
  private static final String REVISION_LABEL = "controller-revision-hash";
  private static final String OLD_REVISION = "hive-llap0-5f4c";
  private static final String NEW_REVISION = "hive-llap0-7b9d";

  private HiveClusterReconciler reconciler;
  private KubernetesClient client;
  private FilterWatchListDeletable<Pod, PodList, PodResource> podQuery;

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void setUp() {
    reconciler = new HiveClusterReconciler();
    client = mock(KubernetesClient.class);
    // Each level is stubbed on its own: the fabric8 DSL returns type variables, which deep
    // stubs cannot synthesise and hand back as null.
    MixedOperation<Pod, PodList, PodResource> pods = mock(MixedOperation.class);
    NonNamespaceOperation<Pod, PodList, PodResource> podsInNs = mock(NonNamespaceOperation.class);
    podQuery = mock(FilterWatchListDeletable.class);
    when(client.pods()).thenReturn(pods);
    when(pods.inNamespace(NS)).thenReturn(podsInNs);
    when(podsInNs.withLabels(anyMap())).thenReturn(podQuery);
  }

  private HiveCluster cluster() {
    HiveCluster hiveCluster = mock(HiveCluster.class);
    when(hiveCluster.getMetadata())
        .thenReturn(new ObjectMetaBuilder().withName(CLUSTER).withNamespace(NS).build());
    return hiveCluster;
  }

  private LlapSpec llapSpec() {
    LlapSpec llap = mock(LlapSpec.class);
    when(llap.name()).thenReturn(LLAP_NAME);
    return llap;
  }

  /** @param observedGeneration one behind the generation means the controller has not caught up */
  private StatefulSet statefulSet(long generation, long observedGeneration, String updateRevision) {
    return new StatefulSetBuilder()
        .withNewMetadata().withName(CLUSTER + "-" + LLAP_NAME).withNamespace(NS)
          .withGeneration(generation)
        .endMetadata()
        .withNewStatus()
          .withObservedGeneration(observedGeneration).withUpdateRevision(updateRevision)
        .endStatus()
        .build();
  }

  private Pod pod(String name, String revision, boolean terminating) {
    ObjectMetaBuilder metadata = new ObjectMetaBuilder().withName(name).withNamespace(NS)
        .withLabels(Map.of(REVISION_LABEL, revision));
    if (terminating) {
      metadata.withDeletionTimestamp("2026-09-22T10:00:00Z");
    }
    return new PodBuilder().withMetadata(metadata.build()).build();
  }

  private void givenPods(Pod... pods) {
    when(podQuery.list()).thenReturn(new PodListBuilder().withItems(pods).build());
  }

  @SuppressWarnings("unchecked")
  private void givenDeleteSucceeds() {
    when(client.resourceList(ArgumentMatchers.<Collection<HasMetadata>>any()))
        .thenReturn(mock(NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable.class));
  }

  @SuppressWarnings("unchecked")
  private List<Pod> deletedPods() {
    ArgumentCaptor<Collection<HasMetadata>> captor = ArgumentCaptor.forClass(Collection.class);
    verify(client).resourceList(captor.capture());
    return captor.getValue().stream().map(Pod.class::cast).toList();
  }

  @SuppressWarnings("unchecked")
  private void verifyNothingDeleted() {
    verify(client, never()).resourceList(ArgumentMatchers.<Collection<HasMetadata>>any());
  }

  /**
   * The one that matters: Kubernetes writes the status asynchronously, so right after the apply
   * updateRevision still names the previous template and every pod matches it. Comparing against
   * it there would find nothing stale and the rollout would never happen.
   */
  @Test
  public void waitsWhileTheControllerIsBehindTheApply() {
    givenPods(pod("hive-llap0-0", OLD_REVISION, false));

    assertTrue(reconciler.recreateStaleLlapDaemons(client, cluster(), llapSpec(),
        statefulSet(7, 6, OLD_REVISION), NS));
    verifyNothingDeleted();
  }

  /** Once it has caught up, only the daemons behind the new revision go. */
  @Test
  public void deletesOnlyTheDaemonsOnAnOlderRevision() {
    givenDeleteSucceeds();
    givenPods(
        pod("hive-llap0-0", OLD_REVISION, false),
        pod("hive-llap0-1", NEW_REVISION, false),
        pod("hive-llap0-2", OLD_REVISION, false));

    assertFalse(reconciler.recreateStaleLlapDaemons(client, cluster(), llapSpec(),
        statefulSet(7, 7, NEW_REVISION), NS));
    assertEquals(List.of("hive-llap0-0", "hive-llap0-2"),
        deletedPods().stream().map(p -> p.getMetadata().getName()).toList());
  }

  /** A pod already going away is not deleted again by the reconciles that follow. */
  @Test
  public void skipsDaemonsThatAreAlreadyTerminating() {
    givenPods(pod("hive-llap0-0", OLD_REVISION, true));

    assertFalse(reconciler.recreateStaleLlapDaemons(client, cluster(), llapSpec(),
        statefulSet(7, 7, NEW_REVISION), NS));
    verifyNothingDeleted();
  }

  @Test
  public void doesNothingWhenEveryDaemonIsCurrent() {
    givenPods(
        pod("hive-llap0-0", NEW_REVISION, false),
        pod("hive-llap0-1", NEW_REVISION, false));

    assertFalse(reconciler.recreateStaleLlapDaemons(client, cluster(), llapSpec(),
        statefulSet(7, 7, NEW_REVISION), NS));
    verifyNothingDeleted();
  }

  /**
   * A denied or conflicting delete must not escape: it would abort the reconcile before
   * autoscaling and garbage collection run, and the status conditions would be replaced.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void reportsBackWhenTheDeleteFails() {
    givenPods(pod("hive-llap0-0", OLD_REVISION, false));
    when(client.resourceList(ArgumentMatchers.<Collection<HasMetadata>>any()))
        .thenThrow(new KubernetesClientException("forbidden"));

    assertTrue(reconciler.recreateStaleLlapDaemons(client, cluster(), llapSpec(),
        statefulSet(7, 7, NEW_REVISION), NS));
  }
}
