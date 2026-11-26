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

package org.apache.hive.kubernetes.operator;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.api.config.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.config.ResolvedControllerConfiguration;
import io.javaoperatorsdk.operator.api.config.ControllerConfigurationOverrider;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.reconciler.HiveClusterReconciler;
import org.apache.hive.kubernetes.operator.reconciler.HiveWorkflowSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Entry point for the Hive Kubernetes Operator process. */
public final class HiveOperatorMain {

  private static final Logger LOG =
      LoggerFactory.getLogger(HiveOperatorMain.class);
  private static final String WATCH_NAMESPACES_ENV_KEY = "HIVE_OPERATOR_WATCH_NAMESPACES";
  private static final String WATCH_ALL_NAMESPACES_VALUE = "*";

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

    // By Default watch only our own namespace: cluster-wide, a HiveCluster elsewhere using a field this
    // build does not know fails to deserialise in the informer, which stops the process.
    // Watching other namespaces is configurable via HIVE_OPERATOR_WATCH_NAMESPACES: a comma-separated
    // list of namespaces, or "*" for all namespaces.
    String watchNamespaces = System.getenv(WATCH_NAMESPACES_ENV_KEY);
    String watchNsValue = watchNamespaces == null ? "" : watchNamespaces.trim();
    ControllerConfigurationOverrider<HiveCluster> overrider = ControllerConfigurationOverrider.override(baseConfig);
    if (watchNsValue.isEmpty()) {
      overrider.watchingOnlyCurrentNamespace();
      LOG.info("Watching only this operator's own namespace");
    } else if (WATCH_ALL_NAMESPACES_VALUE.equals(watchNsValue)) {
      overrider.watchingAllNamespaces();
      LOG.info("Watching all namespaces in the cluster");
    } else {
      Set<String> namespaces = Arrays.stream(watchNsValue.split(","))
          .map(String::trim)
          .filter(ns -> !ns.isEmpty())
          .collect(Collectors.toCollection(HashSet::new));
      if (namespaces.isEmpty() || namespaces.contains(WATCH_ALL_NAMESPACES_VALUE)) {
        throw new IllegalArgumentException("Invalid " + WATCH_NAMESPACES_ENV_KEY + "='"
            + watchNamespaces + "': expected a comma-separated list of namespaces, or '"
            + WATCH_ALL_NAMESPACES_VALUE + "' for all namespaces");
      }
      overrider.settingNamespaces(namespaces);
      LOG.info("Watching namespaces: {}", namespaces);
    }
    baseConfig = overrider.build();

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
