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

package org.apache.hive.kubernetes.operator.dependent.condition;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;
import org.apache.hive.kubernetes.operator.model.HiveCluster;

/**
 * Activation condition for LLAP dependent resources.
 * Returns true only when spec.llap.enabled is true.
 */
public class LlapEnabledCondition
    implements Condition<HasMetadata, HiveCluster> {

  @Override
  public boolean isMet(
      DependentResource<HasMetadata, HiveCluster> dependentResource,
      HiveCluster primary,
      Context<HiveCluster> context) {
    return primary.getSpec().getLlap().isEnabled();
  }
}
