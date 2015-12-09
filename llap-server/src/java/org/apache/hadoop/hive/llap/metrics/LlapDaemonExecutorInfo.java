/**
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

import org.apache.hadoop.metrics2.MetricsInfo;

import com.google.common.base.Objects;

/**
 * Metrics information for llap daemon container.
 */
public enum LlapDaemonExecutorInfo implements MetricsInfo {
  ExecutorMetrics("Llap daemon cache related metrics"),
  ExecutorThreadCPUTime("Cpu time in nanoseconds"),
  ExecutorThreadUserTime("User time in nanoseconds"),
  ExecutorTotalRequestsHandled("Total number of requests handled by the container"),
  ExecutorNumQueuedRequests("Number of requests queued by the container for processing"),
  ExecutorTotalSuccess("Total number of requests handled by the container that succeeded"),
  ExecutorTotalExecutionFailure("Total number of requests handled by the container that failed execution"),
  ExecutorTotalInterrupted("Total number of requests handled by the container that got interrupted"),
  ExecutorTotalAskedToDie("Total number of requests handled by the container that were asked to die"),
  PreemptionTimeLost("Total time lost due to task preemptions");

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
    return Objects.toStringHelper(this)
        .add("name", name()).add("description", desc)
        .toString();
  }
}