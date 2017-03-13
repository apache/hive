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

import static org.apache.hadoop.hive.llap.metrics.LlapDaemonJvmInfo.LlapDaemonDirectBufferCount;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonJvmInfo.LlapDaemonDirectBufferMemoryUsed;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonJvmInfo.LlapDaemonDirectBufferTotalCapacity;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonJvmInfo.LlapDaemonJVMMetrics;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonJvmInfo.LlapDaemonMappedBufferCount;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonJvmInfo.LlapDaemonMappedBufferMemoryUsed;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonJvmInfo.LlapDaemonMappedBufferTotalCapacity;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonJvmInfo.LlapDaemonMaxFileDescriptorCount;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonJvmInfo.LlapDaemonOpenFileDescriptorCount;
import static org.apache.hadoop.metrics2.impl.MsInfo.ProcessName;
import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.List;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;

import com.sun.management.UnixOperatingSystemMXBean;

/**
 * Class to report LLAP Daemon's JVM metrics to the hadoop metrics2 API.
 */
@Metrics(about = "LlapDaemon JVM Metrics", context = "jvm")
public class LlapDaemonJvmMetrics implements MetricsSource {
  private final String name;
  private final String sessionId;
  private final MetricsRegistry registry;

  private LlapDaemonJvmMetrics(String displayName, String sessionId) {
    this.name = displayName;
    this.sessionId = sessionId;
    this.registry = new MetricsRegistry("LlapDaemonJvmRegistry");
    this.registry.tag(ProcessName, MetricsUtils.METRICS_PROCESS_NAME).tag(SessionId, sessionId);
  }

  public static LlapDaemonJvmMetrics create(String displayName, String sessionId) {
    MetricsSystem ms = LlapMetricsSystem.instance();
    return ms.register(displayName, "LlapDaemon JVM Metrics", new LlapDaemonJvmMetrics(displayName, sessionId));
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean b) {
    MetricsRecordBuilder rb = collector.addRecord(LlapDaemonJVMMetrics)
        .setContext("jvm")
        .tag(ProcessName, MetricsUtils.METRICS_PROCESS_NAME)
        .tag(SessionId, sessionId);
    getJvmMetrics(rb);
  }

  private void getJvmMetrics(final MetricsRecordBuilder rb) {
    List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
    long directBufferCount = 0;
    long directBufferTotalCapacity = 0;
    long directBufferMemoryUsed = 0;
    long mappedBufferCount = 0;
    long mappedBufferTotalCapacity = 0;
    long mappedBufferMemoryUsed = 0;
    for (BufferPoolMXBean pool : pools) {
      if (pool.getName().equals("direct")) {
        directBufferCount = pool.getCount();
        directBufferTotalCapacity = pool.getTotalCapacity();
        directBufferMemoryUsed = pool.getMemoryUsed();
      } else if (pool.getName().equals("mapped")) {
        mappedBufferCount = pool.getCount();
        mappedBufferTotalCapacity = pool.getTotalCapacity();
        mappedBufferMemoryUsed = pool.getMemoryUsed();
      }
    }

    OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
    long openFileHandles = 0;
    long maxFileHandles = 0;
    if(os instanceof UnixOperatingSystemMXBean){
      openFileHandles = ((UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount();
      maxFileHandles = ((UnixOperatingSystemMXBean) os).getMaxFileDescriptorCount();
    }
    rb.addGauge(LlapDaemonDirectBufferCount, directBufferCount)
      .addGauge(LlapDaemonDirectBufferTotalCapacity, directBufferTotalCapacity)
      .addGauge(LlapDaemonDirectBufferMemoryUsed, directBufferMemoryUsed)
      .addGauge(LlapDaemonMappedBufferCount, mappedBufferCount)
      .addGauge(LlapDaemonMappedBufferTotalCapacity, mappedBufferTotalCapacity)
      .addGauge(LlapDaemonMappedBufferMemoryUsed, mappedBufferMemoryUsed)
      .addGauge(LlapDaemonOpenFileDescriptorCount, openFileHandles)
      .addGauge(LlapDaemonMaxFileDescriptorCount, maxFileHandles);
  }

  public String getName() {
    return name;
  }
}
