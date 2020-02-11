/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.tezplugins.metrics;

import static org.apache.hadoop.hive.llap.tezplugins.metrics.LlapTaskSchedulerInfo.SchedulerClusterNodeCount;
import static org.apache.hadoop.hive.llap.tezplugins.metrics.LlapTaskSchedulerInfo.SchedulerCompletedDagCount;
import static org.apache.hadoop.hive.llap.tezplugins.metrics.LlapTaskSchedulerInfo.SchedulerCpuCoresPerInstance;
import static org.apache.hadoop.hive.llap.tezplugins.metrics.LlapTaskSchedulerInfo.SchedulerDisabledNodeCount;
import static org.apache.hadoop.hive.llap.tezplugins.metrics.LlapTaskSchedulerInfo.SchedulerExecutorsPerInstance;
import static org.apache.hadoop.hive.llap.tezplugins.metrics.LlapTaskSchedulerInfo.SchedulerMemoryPerInstance;
import static org.apache.hadoop.hive.llap.tezplugins.metrics.LlapTaskSchedulerInfo.SchedulerMetrics;
import static org.apache.hadoop.hive.llap.tezplugins.metrics.LlapTaskSchedulerInfo.SchedulerPendingPreemptionTaskCount;
import static org.apache.hadoop.hive.llap.tezplugins.metrics.LlapTaskSchedulerInfo.SchedulerPendingTaskCount;
import static org.apache.hadoop.hive.llap.tezplugins.metrics.LlapTaskSchedulerInfo.SchedulerPreemptedTaskCount;
import static org.apache.hadoop.hive.llap.tezplugins.metrics.LlapTaskSchedulerInfo.SchedulerRunningTaskCount;
import static org.apache.hadoop.hive.llap.tezplugins.metrics.LlapTaskSchedulerInfo.SchedulerSchedulableTaskCount;
import static org.apache.hadoop.hive.llap.tezplugins.metrics.LlapTaskSchedulerInfo.SchedulerSuccessfulTaskCount;
import static org.apache.hadoop.metrics2.impl.MsInfo.ProcessName;
import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;

import org.apache.hadoop.hive.common.JvmMetrics;
import org.apache.hadoop.hive.llap.metrics.LlapMetricsSystem;
import org.apache.hadoop.hive.llap.metrics.MetricsUtils;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.impl.MsInfo;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

/**
 * Metrics about the llap task scheduler.
 */
@Metrics(about = "Llap Task Scheduler Metrics", context = "scheduler")
public class LlapTaskSchedulerMetrics implements MetricsSource {
  private final String name;
  private final JvmMetrics jvmMetrics;
  private final String sessionId;
  private final MetricsRegistry registry;
  private String dagId = null;
  @Metric
  MutableGaugeInt numExecutors;
  @Metric
  MutableGaugeLong memoryPerInstance;
  @Metric
  MutableGaugeInt cpuCoresPerInstance;
  @Metric
  MutableGaugeInt clusterNodeCount;
  @Metric
  MutableGaugeInt disabledNodeCount;
  @Metric
  MutableCounterInt pendingTasksCount;
  @Metric
  MutableCounterInt schedulableTasksCount;
  @Metric
  MutableCounterInt runningTasksCount;
  @Metric
  MutableCounterInt successfulTasksCount;
  @Metric
  MutableCounterInt preemptedTasksCount;
  @Metric
  MutableCounterInt completedDagcount;
  @Metric
  MutableCounterInt pendingPreemptionTasksCount;
  @Metric
  MutableGaugeInt wmUnusedGuaranteedCount;
  @Metric
  MutableGaugeInt wmTotalGuaranteedCount;
  @Metric
  MutableCounterInt wmSpeculativePendingCount;
  @Metric
  MutableCounterInt wmGuaranteedPendingCount;
  @Metric
  MutableCounterInt wmSpeculativeCount;
  @Metric
  MutableCounterInt wmGuaranteedCount;

  private LlapTaskSchedulerMetrics(String displayName, JvmMetrics jm, String sessionId) {
    this.name = displayName;
    this.jvmMetrics = jm;
    this.sessionId = sessionId;
    this.registry = new MetricsRegistry("LlapTaskSchedulerMetricsRegistry");
    this.registry.tag(ProcessName, MetricsUtils.METRICS_PROCESS_NAME).tag(SessionId, sessionId);
  }

  public static LlapTaskSchedulerMetrics create(String displayName, String sessionId) {
    MetricsSystem ms = LlapMetricsSystem.instance();
    JvmMetrics jm = JvmMetrics.create(MetricsUtils.METRICS_PROCESS_NAME, sessionId, ms);
    return ms.register(displayName, "Llap Task Scheduler Metrics",
        new LlapTaskSchedulerMetrics(displayName, jm, sessionId));
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean b) {
    MetricsRecordBuilder rb = collector.addRecord(SchedulerMetrics)
        .setContext("scheduler")
        .tag(ProcessName, "DAGAppMaster")
        .tag(SessionId, sessionId);
    if (dagId != null) {
        rb.tag(MsInfo.Context, dagId);
    }
    getTaskSchedulerStats(rb);
  }

  public void setDagId(String dagId) {
    this.dagId = dagId;
  }

  public void setNumExecutors(int value) {
    numExecutors.set(value);
  }

  public void setMemoryPerInstance(long value) {
    memoryPerInstance.set(value);
  }

  public void setCpuCoresPerInstance(int value) {
    cpuCoresPerInstance.set(value);
  }

  public void setClusterNodeCount(int value) {
    clusterNodeCount.set(value);
  }

  public void setDisabledNodeCount(int value) {
    disabledNodeCount.set(value);
  }

  public void incrPendingTasksCount() {
    pendingTasksCount.incr();
  }

  public void decrPendingTasksCount() {
    pendingTasksCount.incr(-1);
  }

  public void incrSchedulableTasksCount(int delta) {
    schedulableTasksCount.incr(delta);
  }

  public void incrSchedulableTasksCount() {
    schedulableTasksCount.incr();
  }

  public void decrSchedulableTasksCount() {
    schedulableTasksCount.incr(-1);
  }

  public void incrSuccessfulTasksCount() {
    successfulTasksCount.incr();
  }

  public void incrRunningTasksCount() {
    runningTasksCount.incr();
  }

  public void decrRunningTasksCount() {
    runningTasksCount.incr(-1);
  }

  public void incrPreemptedTasksCount() {
    preemptedTasksCount.incr();
  }

  public void incrCompletedDagCount() {
    completedDagcount.incr();
  }

  public void incrPendingPreemptionTasksCount() {
    pendingPreemptionTasksCount.incr();
  }

  public void decrPendingPreemptionTasksCount() {
    pendingPreemptionTasksCount.incr(-1);
  }

  public void setWmPendingStarted(boolean isGuaranteed) {
    if (isGuaranteed) {
      wmSpeculativeCount.incr(-1);
      wmGuaranteedPendingCount.incr();
    } else {
      wmGuaranteedCount.incr(-1);
      wmSpeculativePendingCount.incr();
    }
  }

  public void setWmPendingDone(boolean isGuaranteed) {
    if (isGuaranteed) {
      wmGuaranteedPendingCount.incr(-1);
      wmGuaranteedCount.incr();
    } else {
      wmSpeculativePendingCount.incr(-1);
      wmSpeculativeCount.incr();
    }
  }

  public void setWmPendingFailed(boolean requestedGuaranteed) {
    if (requestedGuaranteed) {
      wmGuaranteedPendingCount.incr(-1);
      wmSpeculativeCount.incr();
    } else {
      wmSpeculativePendingCount.incr(-1);
      wmGuaranteedCount.incr();
    }
  }

  public void setWmTaskStarted(boolean isGuaranteed) {
    if (isGuaranteed) {
      wmGuaranteedPendingCount.incr();
    } else {
      wmSpeculativePendingCount.incr();
    }
  }

  public void setWmTaskFinished(boolean isGuaranteed, boolean isPendingUpdate) {
    if (isPendingUpdate) {
      if (isGuaranteed) {
        wmGuaranteedPendingCount.incr(-1);
      } else {
        wmSpeculativePendingCount.incr(-1);
      }
    } else {
      if (isGuaranteed) {
        wmGuaranteedCount.incr(-1);
      } else {
        wmSpeculativeCount.incr(-1);
      }
    }
  }

  public void setWmTotalGuaranteed(int totalGuaranteed) {
    wmTotalGuaranteedCount.set(totalGuaranteed);
  }

  public void setWmUnusedGuaranteed(int unusedGuaranteed) {
    wmUnusedGuaranteedCount.set(unusedGuaranteed);
  }

  public void resetWmMetrics() {
    wmTotalGuaranteedCount.set(0);
    wmUnusedGuaranteedCount.set(0);
    wmGuaranteedCount.incr(-wmGuaranteedCount.value());
    wmSpeculativeCount.incr(-wmSpeculativeCount.value());
    wmGuaranteedPendingCount.incr(-wmGuaranteedPendingCount.value());
    wmSpeculativePendingCount.incr(-wmSpeculativePendingCount.value());
  }

  private void getTaskSchedulerStats(MetricsRecordBuilder rb) {
    rb.addGauge(SchedulerClusterNodeCount, clusterNodeCount.value())
        .addGauge(SchedulerExecutorsPerInstance, numExecutors.value())
        .addGauge(SchedulerMemoryPerInstance, memoryPerInstance.value())
        .addGauge(SchedulerCpuCoresPerInstance, cpuCoresPerInstance.value())
        .addGauge(SchedulerDisabledNodeCount, disabledNodeCount.value())
        .addCounter(SchedulerPendingTaskCount, pendingTasksCount.value())
        .addCounter(SchedulerSchedulableTaskCount, schedulableTasksCount.value())
        .addCounter(SchedulerRunningTaskCount, runningTasksCount.value())
        .addCounter(SchedulerSuccessfulTaskCount, successfulTasksCount.value())
        .addCounter(SchedulerPendingPreemptionTaskCount, pendingPreemptionTasksCount.value())
        .addCounter(SchedulerPreemptedTaskCount, preemptedTasksCount.value())
        .addCounter(SchedulerCompletedDagCount, completedDagcount.value());
  }

  public JvmMetrics getJvmMetrics() {
    return jvmMetrics;
  }

  public String getName() {
    return name;
  }
}
