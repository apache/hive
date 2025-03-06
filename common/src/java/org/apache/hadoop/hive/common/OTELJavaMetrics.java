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
package org.apache.hadoop.hive.common;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;

import com.sun.management.UnixOperatingSystemMXBean;
import io.opentelemetry.api.metrics.DoubleGauge;
import io.opentelemetry.api.metrics.Meter;

public class OTELJavaMetrics {

  // The MXBean used to fetch values
  private final MemoryMXBean memoryMXBean;
  private final ThreadMXBean threadMXBean;
  private final OperatingSystemMXBean osMXBean;

  // Memory Level Gauge
  private final DoubleGauge memNonHeapUsedMGauge;
  private final DoubleGauge memNonHeapMaxM;
  private final DoubleGauge memHeapUsedM;
  private final DoubleGauge memHeapCommittedM;
  private final DoubleGauge memHeapMaxM;
  private final DoubleGauge memMaxM;
  private final DoubleGauge memNonHeapCommittedM;

  // Thread Level Gauge
  private final DoubleGauge threadsNew;
  private final DoubleGauge threadsRunnable;
  private final DoubleGauge threadsBlocked;
  private final DoubleGauge threadsWaiting;
  private final DoubleGauge threadsTimedWaiting;
  private final DoubleGauge threadsTerminated;

  // OS Level Gauge
  private final DoubleGauge systemLoadAverage;
  private final DoubleGauge systemCpuLoad;
  private final DoubleGauge committedVirtualMemorySize;
  private final DoubleGauge processCpuTime;
  private final DoubleGauge freePhysicalMemorySize;
  private final DoubleGauge freeSwapSpaceSize;
  private final DoubleGauge totalPhysicalMemorySize;
  private final DoubleGauge processCpuLoad;

  // 1 MB Constant
  static final float M = 1024 * 1024;

  public OTELJavaMetrics(Meter meter) {
    memoryMXBean = ManagementFactory.getMemoryMXBean();
    threadMXBean = ManagementFactory.getThreadMXBean();
    osMXBean = ManagementFactory.getOperatingSystemMXBean();
    memNonHeapUsedMGauge = meter.gaugeBuilder(JvmMetricsInfo.MemNonHeapUsedM.name()).build();
    memNonHeapCommittedM = meter.gaugeBuilder(JvmMetricsInfo.MemNonHeapCommittedM.name()).build();
    memNonHeapMaxM = meter.gaugeBuilder(JvmMetricsInfo.MemNonHeapMaxM.name()).build();
    memHeapUsedM = meter.gaugeBuilder(JvmMetricsInfo.MemHeapUsedM.name()).build();
    memHeapCommittedM = meter.gaugeBuilder(JvmMetricsInfo.MemHeapCommittedM.name()).build();
    memHeapMaxM = meter.gaugeBuilder(JvmMetricsInfo.MemHeapMaxM.name()).build();
    memMaxM = meter.gaugeBuilder(JvmMetricsInfo.MemMaxM.name()).build();

    // Thread Level Counters
    threadsNew = meter.gaugeBuilder(JvmMetricsInfo.ThreadsNew.name()).build();
    threadsRunnable = meter.gaugeBuilder(JvmMetricsInfo.ThreadsRunnable.name()).build();
    threadsBlocked = meter.gaugeBuilder(JvmMetricsInfo.ThreadsBlocked.name()).build();
    threadsWaiting = meter.gaugeBuilder(JvmMetricsInfo.ThreadsWaiting.name()).build();
    threadsTimedWaiting = meter.gaugeBuilder(JvmMetricsInfo.ThreadsTimedWaiting.name()).build();
    threadsTerminated = meter.gaugeBuilder(JvmMetricsInfo.ThreadsTerminated.name()).build();

    // Os Level Counters
    systemLoadAverage = meter.gaugeBuilder("SystemLoadAverage").build();
    systemCpuLoad = meter.gaugeBuilder("SystemCpuLoad").build();
    committedVirtualMemorySize = meter.gaugeBuilder("CommittedVirtualMemorySize").build();

    processCpuTime = meter.gaugeBuilder("ProcessCpuTime").build();
    freePhysicalMemorySize = meter.gaugeBuilder("FreePhysicalMemorySize").build();

    freeSwapSpaceSize = meter.gaugeBuilder("FreeSwapSpaceSize").build();
    totalPhysicalMemorySize = meter.gaugeBuilder("TotalPhysicalMemorySize").build();
    processCpuLoad = meter.gaugeBuilder("ProcessCpuLoad").build();
  }

  public void setJvmMetrics() {
    setMemoryValuesValues();
    setThreadCountValues();
    setOsLevelValues();
  }

  private void setMemoryValuesValues() {
    MemoryUsage memNonHeap = memoryMXBean.getNonHeapMemoryUsage();
    MemoryUsage memHeap = memoryMXBean.getHeapMemoryUsage();
    Runtime runtime = Runtime.getRuntime();
    memNonHeapUsedMGauge.set(memNonHeap.getUsed() / M);
    memNonHeapCommittedM.set(memNonHeap.getCommitted() / M);
    memNonHeapMaxM.set(memNonHeap.getMax() / M);
    memHeapUsedM.set(memHeap.getUsed() / M);
    memHeapCommittedM.set(memHeap.getCommitted() / M);
    memHeapMaxM.set(memHeap.getMax() / M);
    memMaxM.set(runtime.maxMemory() / M);
  }

  private void setThreadCountValues() {
    JvmMetrics.ThreadCountResult threadCountResult = JvmMetrics.getThreadCountResult(threadMXBean);
    threadsNew.set(threadCountResult.threadsNew);
    threadsRunnable.set(threadCountResult.threadsRunnable);
    threadsBlocked.set(threadCountResult.threadsBlocked);
    threadsWaiting.set(threadCountResult.threadsWaiting);
    threadsTimedWaiting.set(threadCountResult.threadsTimedWaiting);
    threadsTerminated.set(threadCountResult.threadsTerminated);
  }

  private void setOsLevelValues() {
    systemLoadAverage.set(osMXBean.getSystemLoadAverage());
    if (osMXBean instanceof UnixOperatingSystemMXBean) {
      UnixOperatingSystemMXBean unixMxBean = (UnixOperatingSystemMXBean) osMXBean;
      systemCpuLoad.set(unixMxBean.getSystemCpuLoad());
      committedVirtualMemorySize.set(unixMxBean.getCommittedVirtualMemorySize());
      processCpuTime.set(unixMxBean.getProcessCpuTime());
      freePhysicalMemorySize.set(unixMxBean.getFreePhysicalMemorySize());
      freeSwapSpaceSize.set(unixMxBean.getFreeSwapSpaceSize());
      totalPhysicalMemorySize.set(unixMxBean.getTotalPhysicalMemorySize());
      processCpuLoad.set(unixMxBean.getProcessCpuLoad());
    }
  }
}
