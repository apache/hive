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

import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorAvailableFreeSlots;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorAvailableFreeSlotsPercent;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorCacheMemoryPerInstance;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorFallOffNumCompletedFragments;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorJvmMaxMemory;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorMaxFreeSlots;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorMaxPreemptionTimeLost;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorMaxPreemptionTimeToKill;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorMemoryPerInstance;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorNumExecutorsAvailable;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorNumPreemptableRequests;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorNumQueuedRequests;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorThreadCPUTime;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorNumExecutorsPerInstance;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorThreadUserTime;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorTotalEvictedFromWaitQueue;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorTotalFailed;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorTotalKilled;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorTotalRejectedRequests;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorTotalRequestsHandled;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorTotalSuccess;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorMetrics;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorTotalPreemptionTimeLost;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorTotalPreemptionTimeToKill;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorWaitQueueSize;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorFallOffSuccessTimeLost;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorFallOffSuccessMaxTimeLost;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorFallOffFailedTimeLost;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorFallOffFailedMaxTimeLost;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorFallOffKilledTimeLost;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorFallOffKilledMaxTimeLost;
import static org.apache.hadoop.metrics2.impl.MsInfo.ProcessName;
import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.Maps;
import org.apache.hadoop.hive.common.JvmMetrics;
import org.apache.hadoop.hive.llap.daemon.impl.ContainerRunnerImpl;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;

/**
 * Metrics about the llap daemon executors.
 */
@Metrics(about = "LlapDaemon Executor Metrics", context = "executors")
public class LlapDaemonExecutorMetrics implements MetricsSource {

  private final String name;
  private final JvmMetrics jvmMetrics;
  private final String sessionId;
  private final MetricsRegistry registry;
  private final int numExecutors;
  private final ThreadMXBean threadMXBean;
  private final Map<Integer, MetricsInfo> cpuMetricsInfoMap;
  private final Map<Integer, MetricsInfo> userMetricsInfoMap;
  private long maxTimeLost = Long.MIN_VALUE;
  private long maxTimeToKill = Long.MIN_VALUE;

  private long fallOffMaxSuccessTimeLostLong = 0L;
  private long fallOffMaxFailedTimeLostLong = 0L;
  private long fallOffMaxKilledTimeLostLong = 0L;

  private final Map<String, Integer> executorNames;

  final MutableGaugeLong[] executorThreadCpuTime;
  final MutableGaugeLong[] executorThreadUserTime;
  @Metric
  MutableCounterLong executorTotalRequestHandled;
  @Metric
  MutableGaugeInt executorNumQueuedRequests;
  @Metric
  MutableGaugeInt executorNumPreemptableRequests;
  @Metric
  MutableGaugeInt numExecutorsAvailable;
  @Metric
  MutableCounterLong totalRejectedRequests;
  @Metric
  MutableCounterLong totalEvictedFromWaitQueue;
  @Metric
  MutableCounterLong executorTotalSuccess;
  @Metric
  MutableCounterLong executorTotalIKilled;
  @Metric
  MutableCounterLong executorTotalExecutionFailed;
  @Metric
  MutableGaugeLong cacheMemoryPerInstance;
  @Metric
  MutableGaugeLong memoryPerInstance;
  @Metric
  MutableGaugeLong jvmMaxMemory;
  @Metric
  MutableGaugeInt waitQueueSize;
  @Metric
  MutableCounterLong totalPreemptionTimeToKill;
  @Metric
  MutableCounterLong totalPreemptionTimeLost;
  @Metric
  MutableGaugeLong maxPreemptionTimeToKill;
  @Metric
  MutableGaugeLong maxPreemptionTimeLost;
  @Metric
  final MutableQuantiles[] percentileTimeToKill;
  @Metric
  final MutableQuantiles[] percentileTimeLost;

  @Metric
  MutableCounterLong fallOffNumCompletedFragments;
  @Metric
  MutableCounterLong fallOffSuccessTimeLost;
  @Metric
  MutableCounterLong fallOffFailedTimeLost;
  @Metric
  MutableCounterLong fallOffKilledTimeLost;
  @Metric
  MutableGaugeLong fallOffMaxSuccessTimeLost;
  @Metric
  MutableGaugeLong fallOffMaxFailedTimeLost;
  @Metric
  MutableGaugeLong fallOffMaxKilledTimeLost;



  private LlapDaemonExecutorMetrics(String displayName, JvmMetrics jm, String sessionId,
      int numExecutors, final int[] intervals) {
    this.name = displayName;
    this.jvmMetrics = jm;
    this.sessionId = sessionId;
    this.registry = new MetricsRegistry("LlapDaemonExecutorRegistry");
    this.registry.tag(ProcessName, MetricsUtils.METRICS_PROCESS_NAME).tag(SessionId, sessionId);
    this.numExecutors = numExecutors;
    this.threadMXBean = ManagementFactory.getThreadMXBean();
    this.executorThreadCpuTime = new MutableGaugeLong[numExecutors];
    this.executorThreadUserTime = new MutableGaugeLong[numExecutors];
    this.cpuMetricsInfoMap = new ConcurrentHashMap<>();
    this.userMetricsInfoMap = new ConcurrentHashMap<>();

    final int len = intervals == null ? 0 : intervals.length;
    this.percentileTimeToKill = new MutableQuantiles[len];
    this.percentileTimeLost = new MutableQuantiles[len];
    for (int i=0; i<len; i++) {
      int interval = intervals[i];
      percentileTimeToKill[i] = registry.newQuantiles(
          LlapDaemonExecutorInfo.ExecutorMaxPreemptionTimeToKill.name() + "_" + interval + "s",
          LlapDaemonExecutorInfo.ExecutorMaxPreemptionTimeToKill.description(),
          "ops", "latency", interval);
      percentileTimeLost[i] = registry.newQuantiles(
          LlapDaemonExecutorInfo.ExecutorMaxPreemptionTimeLost.name() + "_" + interval + "s",
          LlapDaemonExecutorInfo.ExecutorMaxPreemptionTimeLost.description(),
          "ops", "latency", interval);
    }

    this.executorNames = Maps.newHashMap();
    for (int i = 0; i < numExecutors; i++) {
      MetricsInfo mic = new LlapDaemonCustomMetricsInfo(ExecutorThreadCPUTime.name() + "_" + i,
          ExecutorThreadCPUTime.description());
      MetricsInfo miu = new LlapDaemonCustomMetricsInfo(ExecutorThreadUserTime.name() + "_" + i,
          ExecutorThreadUserTime.description());
      this.cpuMetricsInfoMap.put(i, mic);
      this.userMetricsInfoMap.put(i, miu);
      this.executorThreadCpuTime[i] = registry.newGauge(mic, 0L);
      this.executorThreadUserTime[i] = registry.newGauge(miu, 0L);
      this.executorNames.put(ContainerRunnerImpl.THREAD_NAME_FORMAT_PREFIX + i, i);
    }
  }

  public static LlapDaemonExecutorMetrics create(String displayName, String sessionId,
      int numExecutors, final int[] intervals) {
    MetricsSystem ms = LlapMetricsSystem.instance();
    JvmMetrics jm = JvmMetrics.create(MetricsUtils.METRICS_PROCESS_NAME, sessionId, ms);
    return ms.register(displayName, "LlapDaemon Executor Metrics",
        new LlapDaemonExecutorMetrics(displayName, jm, sessionId, numExecutors, intervals));
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean b) {
    MetricsRecordBuilder rb = collector.addRecord(ExecutorMetrics)
        .setContext("executors")
        .tag(ProcessName, MetricsUtils.METRICS_PROCESS_NAME)
        .tag(SessionId, sessionId);
    getExecutorStats(rb);
  }

  public void incrExecutorTotalRequestsHandled() {
    executorTotalRequestHandled.incr();
  }

  public void setExecutorNumQueuedRequests(int value) {
    executorNumQueuedRequests.set(value);
  }

  public void setExecutorNumPreemptableRequests(int value) {
    executorNumPreemptableRequests.set(value);
  }

  public void setNumExecutorsAvailable(int value) {
    numExecutorsAvailable.set(value);
  }

  public void incrTotalEvictedFromWaitQueue() {
    totalEvictedFromWaitQueue.incr();
  }

  public void incrTotalRejectedRequests() {
    totalRejectedRequests.incr();
  }

  public void incrExecutorTotalSuccess() {
    executorTotalSuccess.incr();
  }

  public void incrExecutorTotalExecutionFailed() {
    executorTotalExecutionFailed.incr();
  }

  public void addMetricsPreemptionTimeLost(long value) {
    totalPreemptionTimeLost.incr(value);

    if (value > maxTimeLost) {
      maxTimeLost = value;
      maxPreemptionTimeLost.set(maxTimeLost);
    }

    for (MutableQuantiles q : percentileTimeLost) {
      q.add(value);
    }
  }

  public void addMetricsPreemptionTimeToKill(long value) {
    totalPreemptionTimeToKill.incr(value);

    if (value > maxTimeToKill) {
      maxTimeToKill = value;
      maxPreemptionTimeToKill.set(maxTimeToKill);
    }

    for (MutableQuantiles q : percentileTimeToKill) {
      q.add(value);
    }
  }

  public void addMetricsFallOffSuccessTimeLost(long timeLost) {
    fallOffNumCompletedFragments.incr();
    fallOffSuccessTimeLost.incr(timeLost);
    if (timeLost > fallOffMaxSuccessTimeLostLong) {
      fallOffMaxSuccessTimeLostLong = timeLost;
      fallOffMaxSuccessTimeLost.set(timeLost);
    }
  }

  public void addMetricsFallOffFailedTimeLost(long timeLost) {
    fallOffNumCompletedFragments.incr();
    fallOffFailedTimeLost.incr(timeLost);
    if (timeLost > fallOffMaxFailedTimeLostLong) {
      fallOffMaxFailedTimeLostLong = timeLost;
      fallOffMaxFailedTimeLost.set(timeLost);
    }
  }

  public void addMetricsFallOffKilledTimeLost(long timeLost) {
    fallOffNumCompletedFragments.incr();
    fallOffKilledTimeLost.incr(timeLost);
    if (timeLost > fallOffMaxKilledTimeLostLong) {
      fallOffMaxKilledTimeLostLong = timeLost;
      fallOffMaxKilledTimeLost.set(timeLost);
    }
  }

  public void incrExecutorTotalKilled() {
    executorTotalIKilled.incr();
  }

  public void setCacheMemoryPerInstance(long value) {
    cacheMemoryPerInstance.set(value);
  }

  public void setMemoryPerInstance(long value) {
    memoryPerInstance.set(value);
  }

  public void setJvmMaxMemory(long value) {
    jvmMaxMemory.set(value);
  }

  public void setWaitQueueSize(int size) {
    waitQueueSize.set(size);
  }

  private void getExecutorStats(MetricsRecordBuilder rb) {
    updateThreadMetrics(rb);
    final int totalSlots = waitQueueSize.value() + numExecutors;
    final int slotsAvailableInQueue = waitQueueSize.value() - executorNumQueuedRequests.value();
    final int slotsAvailableTotal = slotsAvailableInQueue + numExecutorsAvailable.value();
    final float slotsAvailablePercent = totalSlots <= 0 ? 0.0f :
        (float) slotsAvailableTotal / (float) totalSlots;

    rb.addCounter(ExecutorTotalRequestsHandled, executorTotalRequestHandled.value())
        .addCounter(ExecutorTotalSuccess, executorTotalSuccess.value())
        .addCounter(ExecutorTotalFailed, executorTotalExecutionFailed.value())
        .addCounter(ExecutorTotalKilled, executorTotalIKilled.value())
        .addCounter(ExecutorTotalEvictedFromWaitQueue, totalEvictedFromWaitQueue.value())
        .addCounter(ExecutorTotalRejectedRequests, totalRejectedRequests.value())
        .addGauge(ExecutorNumQueuedRequests, executorNumQueuedRequests.value())
        .addGauge(ExecutorNumPreemptableRequests, executorNumPreemptableRequests.value())
        .addGauge(ExecutorMemoryPerInstance, memoryPerInstance.value())
        .addGauge(ExecutorCacheMemoryPerInstance, cacheMemoryPerInstance.value())
        .addGauge(ExecutorJvmMaxMemory, jvmMaxMemory.value())
        .addGauge(ExecutorMaxFreeSlots, totalSlots)
        .addGauge(ExecutorNumExecutorsPerInstance, numExecutors)
        .addGauge(ExecutorWaitQueueSize, waitQueueSize.value())
        .addGauge(ExecutorNumExecutorsAvailable, numExecutorsAvailable.value())
        .addGauge(ExecutorAvailableFreeSlots, slotsAvailableTotal)
        .addGauge(ExecutorAvailableFreeSlotsPercent, slotsAvailablePercent)
        .addCounter(ExecutorTotalPreemptionTimeToKill, totalPreemptionTimeToKill.value())
        .addCounter(ExecutorTotalPreemptionTimeLost, totalPreemptionTimeLost.value())
        .addGauge(ExecutorMaxPreemptionTimeToKill, maxPreemptionTimeToKill.value())
        .addGauge(ExecutorMaxPreemptionTimeLost, maxPreemptionTimeLost.value())
        .addCounter(ExecutorFallOffSuccessTimeLost, fallOffSuccessTimeLost.value())
        .addGauge(ExecutorFallOffSuccessMaxTimeLost, fallOffMaxSuccessTimeLost.value())
        .addCounter(ExecutorFallOffFailedTimeLost, fallOffFailedTimeLost.value())
        .addGauge(ExecutorFallOffFailedMaxTimeLost, fallOffMaxFailedTimeLost.value())
        .addCounter(ExecutorFallOffKilledTimeLost, fallOffKilledTimeLost.value())
        .addGauge(ExecutorFallOffKilledMaxTimeLost, fallOffMaxKilledTimeLost.value())
        .addCounter(ExecutorFallOffNumCompletedFragments, fallOffNumCompletedFragments.value());

    for (MutableQuantiles q : percentileTimeToKill) {
      q.snapshot(rb, true);
    }

    for (MutableQuantiles q : percentileTimeLost) {
      q.snapshot(rb, true);
    }
  }

  private void updateThreadMetrics(MetricsRecordBuilder rb) {
    if (threadMXBean.isThreadCpuTimeSupported() && threadMXBean.isThreadCpuTimeEnabled()) {
      final long[] ids = threadMXBean.getAllThreadIds();
      final ThreadInfo[] infos = threadMXBean.getThreadInfo(ids);
      for (int i = 0; i < ids.length; i++) {
        ThreadInfo threadInfo = infos[i];
        if (threadInfo == null) {
          continue;
        }
        String threadName = threadInfo.getThreadName();
        long threadId = ids[i];
        Integer id = executorNames.get(threadName);
        if (id != null) {
          executorThreadCpuTime[id].set(threadMXBean.getThreadCpuTime(threadId));
          executorThreadUserTime[id].set(threadMXBean.getThreadUserTime(threadId));
        }
      }

      for (int i=0; i<numExecutors; i++) {
        rb.addGauge(cpuMetricsInfoMap.get(i), executorThreadCpuTime[i].value());
        rb.addGauge(userMetricsInfoMap.get(i), executorThreadUserTime[i].value());
      }
    }
  }

  public JvmMetrics getJvmMetrics() {
    return jvmMetrics;
  }

  public String getName() {
    return name;
  }
}
