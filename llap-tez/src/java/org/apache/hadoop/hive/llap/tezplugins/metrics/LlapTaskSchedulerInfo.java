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
package org.apache.hadoop.hive.llap.tezplugins.metrics;

import com.google.common.base.MoreObjects;
import org.apache.hadoop.metrics2.MetricsInfo;

import com.google.common.base.Objects;

/**
 * Metrics information for llap task scheduler.
 */
public enum LlapTaskSchedulerInfo implements MetricsInfo {
  SchedulerMetrics("Llap task scheduler related metrics"),
  SchedulerClusterNodeCount("Number of nodes in the cluster"),
  SchedulerExecutorsPerInstance("Total number of executor threads per node"),
  SchedulerMemoryPerInstance("Total memory for executors per node in bytes"),
  SchedulerCpuCoresPerInstance("Total CPU vCores per node"),
  SchedulerDisabledNodeCount("Number of nodes disabled temporarily"),
  SchedulerPendingTaskCount("Number of pending tasks"),
  SchedulerSchedulableTaskCount("Current slots available for scheduling tasks"),
  SchedulerSuccessfulTaskCount("Total number of successful tasks"),
  SchedulerRunningTaskCount("Total number of running tasks"),
  SchedulerPendingPreemptionTaskCount("Total number of tasks pending for pre-emption"),
  SchedulerPreemptedTaskCount("Total number of tasks pre-empted"),
  SchedulerCompletedDagCount("Number of DAGs completed");

  private final String desc;

  LlapTaskSchedulerInfo(String desc) {
    this.desc = desc;
  }

  @Override
  public String description() {
    return desc;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name()).add("description", desc)
        .toString();
  }
}