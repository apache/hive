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

package org.apache.hive.kubernetes.operator.autoscaling;

import java.util.Map;

/**
 * Interprets TezAM JMX Exporter metrics to determine whether an AM is busy executing
 * a DAG or idle and safe to remove during scale-down.
 * <p>
 * Signal: {@code tez_am_dag_running} (SchedulerDagRunning gauge in LlapTaskSchedulerMetrics).
 * Set to 1 when the AM receives its first task for a new DAG, cleared to 0 in dagComplete().
 */
public final class TezAmBusyMetrics {

  public static final String METRIC_DAG_RUNNING = "tez_am_dag_running";

  private TezAmBusyMetrics() {}

  /**
   * Returns true when the TezAM has active DAG work (running or pending tasks).
   */
  public static boolean hasActiveDag(Map<String, Double> metrics) {
    return metrics.getOrDefault(METRIC_DAG_RUNNING, 0.0) > 0;
  }

  /**
   * Returns the pod-deletion-cost for this AM:
   * 0 if the AM is idle
   * 1 if the AM is running a DAG
   */
  public static int deletionCost(Map<String, Double> metrics) {
    return hasActiveDag(metrics) ? 1 : 0;
  }
}
