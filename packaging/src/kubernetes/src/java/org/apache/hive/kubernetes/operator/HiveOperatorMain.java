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

package org.apache.hive.kubernetes.operator;

import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.api.config.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.config.ResolvedControllerConfiguration;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.reconciler.HiveClusterReconciler;
import org.apache.hive.kubernetes.operator.reconciler.HiveWorkflowSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Entry point for the Hive Kubernetes Operator process. */
public final class HiveOperatorMain {

  private static final Logger LOG =
      LoggerFactory.getLogger(HiveOperatorMain.class);

  private HiveOperatorMain() {
  }

  /** Starts the operator, registers reconcilers, and blocks until shutdown. */
  public static void main(String[] args) {
    LOG.info("Starting Hive Kubernetes Operator");
    Operator operator = new Operator();
    HiveClusterReconciler reconciler = new HiveClusterReconciler();
    // Get the annotation-derived base config, then inject our programmatic workflow spec.
    ControllerConfiguration<HiveCluster> baseConfig =
        operator.getConfigurationService().getConfigurationFor(reconciler);
    HiveWorkflowSpec workflowSpec = new HiveWorkflowSpec();
    ((ResolvedControllerConfiguration<HiveCluster>) baseConfig)
        .setWorkflowSpec(workflowSpec);
    LOG.info("Registered workflow with {} dependent resource specs",
        workflowSpec.getDependentResourceSpecs().size());
    operator.register(reconciler, baseConfig);
    operator.start();
    LOG.info("Hive Kubernetes Operator started successfully");
  }
}
