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
 * Llap daemon JVM info. These are some additional metrics that are not exposed via
 * {@link org.apache.hadoop.metrics.jvm.JvmMetrics}
 *
 * NOTE: These metrics are for sinks supported by hadoop-metrics2. There is already a /jmx endpoint
 * that gives all these info.
 */
public enum LlapDaemonJvmInfo implements MetricsInfo {
  LlapDaemonJVMMetrics("Llap daemon JVM related metrics"),
  LlapDaemonDirectBufferCount("Total number of direct byte buffers"),
  LlapDaemonDirectBufferTotalCapacity("Estimate of total capacity of all allocated direct byte buffers in bytes"),
  LlapDaemonDirectBufferMemoryUsed("Estimate of memory that JVM is using for the allocated buffers in bytes"),
  LlapDaemonMappedBufferCount("Total number of mapped byte buffers"),
  LlapDaemonMappedBufferTotalCapacity("Estimate of total capacity of all mapped byte buffers in bytes"),
  LlapDaemonMappedBufferMemoryUsed("Estimate of memory that JVM is using for mapped byte buffers in bytes"),
  LlapDaemonOpenFileDescriptorCount("Number of open file descriptors"),
  LlapDaemonMaxFileDescriptorCount("Maximum number of file descriptors used so far"),
  LlapDaemonLimitFileDescriptorCount("Limit for file descriptors allowed by system"),
  LlapDaemonResidentSetSize("Resident memory (RSS) used by llap daemon process in bytes"),
  LlapDaemonVirtualMemorySize("Virtual memory (VMEM) used by llap daemon process in bytes")
  ;

  private final String desc;

  LlapDaemonJvmInfo(String desc) {
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
