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

import java.util.List;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.spec.TezAmSpec;
import org.apache.hive.kubernetes.operator.util.Labels;

/**
 * Manages the shared scratch PersistentVolumeClaim mounted by both
 * HiveServer2 and TezAM at /opt/hive/scratch.
 * <p>
 * This mirrors the Docker Compose pattern where a named volume
 * {@code scratch:/opt/hive/scratch} is shared between the hs2 and
 * tezam containers so that the {@code dummy_path} written by HS2
 * (for VALUES clause) is accessible by the TezAM.
 * <p>
 * The PVC uses ReadWriteMany access mode so both pods can mount it
 * simultaneously.
 */
@KubernetesDependent(
    labelSelector = "app.kubernetes.io/component=scratch,"
        + "app.kubernetes.io/managed-by=hive-kubernetes-operator"
)
public class ScratchPvcDependent
    extends HiveDependentResource<PersistentVolumeClaim, HiveCluster> {

  public static final String COMPONENT = "scratch";

  public ScratchPvcDependent() {
    super(PersistentVolumeClaim.class);
  }

  @Override
  protected PersistentVolumeClaim desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    TezAmSpec tezAm = hiveCluster.getSpec().getTezAm();

    PersistentVolumeClaimBuilder builder = new PersistentVolumeClaimBuilder()
        .withNewMetadata()
          .withName(resourceName(hiveCluster))
          .withNamespace(hiveCluster.getMetadata().getNamespace())
          .withLabels(Labels.forComponent(hiveCluster, COMPONENT))
        .endMetadata()
        .withNewSpec()
          .withAccessModes(List.of("ReadWriteMany"))
          .withNewResources()
            .addToRequests("storage",
                new Quantity(tezAm.getScratchStorageSize()))
          .endResources()
        .endSpec();

    if (tezAm.getScratchStorageClassName() != null) {
      builder.editSpec()
          .withStorageClassName(tezAm.getScratchStorageClassName())
          .endSpec();
    }

    return builder.build();
  }

  /** Returns the PVC resource name for this HiveCluster. */
  public static String resourceName(HiveCluster hiveCluster) {
    return hiveCluster.getMetadata().getName() + "-scratch";
  }
}
