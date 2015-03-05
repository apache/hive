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
  LLAP_DAEMON_EXECUTOR_METRICS("Llap daemon cache related metrics"),
  EXECUTOR_THREAD_CPU_TIME("Cpu time in nanoseconds"),
  EXECUTOR_THREAD_USER_TIME("User time in nanoseconds"),
  EXECUTOR_THREAD_SYSTEM_TIME("System time in nanoseconds"),
  EXECUTOR_TOTAL_REQUESTS_HANDLED("Total number of requests handled by the container"),
  EXECUTOR_NUM_QUEUED_REQUESTS("Number of requests queued by the container for processing"),
  EXECUTOR_TOTAL_SUCCESS("Total number of requests handled by the container that succeeded"),
  EXECUTOR_TOTAL_EXECUTION_FAILURE("Total number of requests handled by the container that failed execution"),
  EXECUTOR_TOTAL_INTERRUPTED("Total number of requests handled by the container that got interrupted"),
  EXECUTOR_TOTAL_ASKED_TO_DIE("Total number of requests handled by the container that were asked to die");

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