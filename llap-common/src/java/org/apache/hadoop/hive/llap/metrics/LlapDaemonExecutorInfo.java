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
package org.apache.hadoop.hive.llap.metrics;

import com.google.common.base.MoreObjects;
import org.apache.hadoop.metrics2.MetricsInfo;

import com.google.common.base.Objects;

/**
 * Metrics information for llap daemon container.
 */
public enum LlapDaemonExecutorInfo implements MetricsInfo {
  ExecutorMetrics("Llap daemon cache related metrics"),
  ExecutorMaxFreeSlots("Sum of wait queue size and number of executors"),
  ExecutorNumExecutors("Total number of executor threads"),
  ExecutorMaxFreeSlotsConfigured("Sum of the configured wait queue size and the configured number of executors"),
  ExecutorNumExecutorsConfigured("Total number of executor threads per node"),
  ExecutorNumExecutorsAvailable("Total number of executor threads per node that are free"),
  ExecutorNumExecutorsAvailableAverage("Total number of executor threads per node that are free averaged over time"),
  ExecutorAvailableFreeSlots("Number of free slots available"),
  ExecutorAvailableFreeSlotsPercent("Percent of free slots available"),
  ExecutorThreadCPUTime("Cpu time in nanoseconds"),
  ExecutorMemoryPerInstance("Total memory for executors per node in bytes"),
  ExecutorCacheMemoryPerInstance("Total Cache memory per node in bytes"),
  ExecutorJvmMaxMemory("Max memory available for JVM in bytes"),
  ExecutorWaitQueueSize("Size of wait queue"),
  ExecutorWaitQueueSizeConfigured("Size of wait queue configured per node"),
  ExecutorThreadUserTime("User time in nanoseconds"),
  ExecutorTotalRequestsHandled("Total number of requests handled by the container"),
  ExecutorNumQueuedRequests("Number of requests queued by the container for processing"),
  ExecutorNumQueuedRequestsAverage("Number of requests queued by the container for processing averaged over time"),
  ExecutorNumPreemptableRequests("Number of queued requests that are pre-emptable"),
  ExecutorTotalRejectedRequests("Total number of requests rejected as wait queue being full"),
  ExecutorTotalSuccess("Total number of requests handled by the container that succeeded"),
  ExecutorTotalFailed("Total number of requests handled by the container that failed execution"),
  ExecutorTotalKilled("Total number of requests handled by the container that got interrupted"),
  ExecutorTotalAskedToDie("Total number of requests handled by the container that were asked to die"),
  ExecutorTotalPreemptionTimeToKill("Total amount of time taken for killing tasks due to pre-emption"),
  ExecutorTotalPreemptionTimeLost("Total useful cluster time lost because of pre-emption"),
  ExecutorPercentileTimeToKill("Percentile time to kill for pre-empted tasks"),
  ExecutorPercentileTimeLost("Percentile cluster time wasted due to pre-emption"),
  ExecutorMaxPreemptionTimeToKill("Max time for killing pre-empted task"),
  ExecutorMaxPreemptionTimeLost("Max cluster time lost due to pre-emption"),
  ExecutorTotalEvictedFromWaitQueue("Total number of tasks evicted from wait queue because of low priority"),
  ExecutorFallOffSuccessTimeLost("Total time lost in an executor completing after informing the AM - successful fragments"),
  ExecutorFallOffSuccessMaxTimeLost("Max value of time lost in an executor completing after informing the AM - successful fragments"),
  ExecutorFallOffFailedTimeLost("Total time lost in an executor completing after informing the AM - failed fragments"),
  ExecutorFallOffFailedMaxTimeLost("Max value of time lost in an executor completing after informing the AM - failed fragments"),
  ExecutorFallOffKilledTimeLost("Total time lost in an executor completing after informing the AM - killed fragments"),
  ExecutorFallOffKilledMaxTimeLost("Max value of time lost in an executor completing after informing the AM - killed fragments"),
  ExecutorFallOffNumCompletedFragments("Number of completed fragments w.r.t falloff values"),
  AverageQueueTime("Average queue time for tasks"),
  AverageResponseTime("Average response time for successful tasks"),
  ;

  private final String desc;

  LlapDaemonExecutorInfo(String desc) {
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