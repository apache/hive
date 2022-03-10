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

import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.AverageQueueTime;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.AverageResponseTime;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorAvailableFreeSlots;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorAvailableFreeSlotsPercent;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorCacheMemoryPerInstance;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorFallOffNumCompletedFragments;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorJvmMaxMemory;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorMaxFreeSlotsConfigured;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorMaxFreeSlots;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorMaxPreemptionTimeLost;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorMaxPreemptionTimeToKill;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorMemoryPerInstance;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorNumExecutors;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorNumExecutorsAvailable;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorNumExecutorsAvailableAverage;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorNumExecutorsConfigured;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorNumPreemptableRequests;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorNumQueuedRequests;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorNumQueuedRequestsAverage;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorThreadCPUTime;
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
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorWaitQueueSizeConfigured;
import static org.apache.hadoop.metrics2.impl.MsInfo.ProcessName;
import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.apache.hadoop.hive.common.JvmMetrics;
import org.apache.hadoop.hive.llap.daemon.impl.ContainerRunnerImpl;
import org.apache.hadoop.hive.llap.daemon.impl.TaskExecutorService;
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
  private final int numExecutorsConfigured;
  private final int waitQueueSizeConfigured;
  private final ThreadMXBean threadMXBean;
  private final Map<Integer, MetricsInfo> cpuMetricsInfoMap;
  private final Map<Integer, MetricsInfo> userMetricsInfoMap;
  private long maxTimeLost = Long.MIN_VALUE;
  private long maxTimeToKill = Long.MIN_VALUE;

  private long fallOffMaxSuccessTimeLostLong = 0L;
  private long fallOffMaxFailedTimeLostLong = 0L;
  private long fallOffMaxKilledTimeLostLong = 0L;

  private TimedAverageMetrics executorNumQueuedRequestsAverage;
  private TimedAverageMetrics numExecutorsAvailableAverage;

  private final Map<String, Integer> executorNames;

  private final DescriptiveStatistics queueTime;
  private final DescriptiveStatistics runningTime;

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
  @Metric
  MutableGaugeInt numExecutors;

  private LlapDaemonExecutorMetrics(String displayName, JvmMetrics jm, String sessionId,
      int numExecutorsConfigured, int waitQueueSizeConfigured, final int[] intervals, int timedWindowAverageDataPoints,
      long timedWindowAverageWindowLength, int simpleAverageWindowDataSize) {
    this.name = displayName;
    this.jvmMetrics = jm;
    this.sessionId = sessionId;
    this.registry = new MetricsRegistry("LlapDaemonExecutorRegistry");
    this.registry.tag(ProcessName, MetricsUtils.METRICS_PROCESS_NAME).tag(SessionId, sessionId);
    this.numExecutorsConfigured = numExecutorsConfigured;
    this.waitQueueSizeConfigured = waitQueueSizeConfigured;
    this.threadMXBean = ManagementFactory.getThreadMXBean();
    this.executorThreadCpuTime = new MutableGaugeLong[numExecutorsConfigured];
    this.executorThreadUserTime = new MutableGaugeLong[numExecutorsConfigured];
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
    for (int i = 0; i < numExecutorsConfigured; i++) {
      MetricsInfo mic = new LlapDaemonCustomMetricsInfo(ExecutorThreadCPUTime.name() + "_" + i,
          ExecutorThreadCPUTime.description());
      MetricsInfo miu = new LlapDaemonCustomMetricsInfo(ExecutorThreadUserTime.name() + "_" + i,
          ExecutorThreadUserTime.description());
      this.cpuMetricsInfoMap.put(i, mic);
      this.userMetricsInfoMap.put(i, miu);
      this.executorThreadCpuTime[i] = registry.newGauge(mic, 0L);
      this.executorThreadUserTime[i] = registry.newGauge(miu, 0L);
      this.executorNames.put(TaskExecutorService.TASK_EXECUTOR_THREAD_NAME_FORMAT_PREFIX + i, i);
    }

    if (timedWindowAverageDataPoints > 0) {
      this.executorNumQueuedRequestsAverage = new TimedAverageMetrics(timedWindowAverageDataPoints,
          timedWindowAverageWindowLength);
      this.numExecutorsAvailableAverage = new TimedAverageMetrics(timedWindowAverageDataPoints,
          timedWindowAverageWindowLength);
    }
    if (simpleAverageWindowDataSize > 0) {
      this.queueTime = new SynchronizedDescriptiveStatistics(simpleAverageWindowDataSize);
      this.runningTime = new SynchronizedDescriptiveStatistics(simpleAverageWindowDataSize);
    } else {
      this.queueTime = null;
      this.runningTime = null;
    }
  }

  public static LlapDaemonExecutorMetrics create(String displayName, String sessionId,
      int numExecutorsConfigured, int waitQueueSizeConfigured, final int[] intervals, int timedWindowAverageDataPoints,
      long timedWindowAverageWindowLength, int simpleAverageWindowDataSize) {
    MetricsSystem ms = LlapMetricsSystem.instance();
    JvmMetrics jm = JvmMetrics.create(MetricsUtils.METRICS_PROCESS_NAME, sessionId, ms);
    return ms.register(displayName, "LlapDaemon Executor Metrics",
        new LlapDaemonExecutorMetrics(displayName, jm, sessionId, numExecutorsConfigured, waitQueueSizeConfigured,
            intervals, timedWindowAverageDataPoints, timedWindowAverageWindowLength, simpleAverageWindowDataSize));
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
    if (executorNumQueuedRequestsAverage != null) {
      executorNumQueuedRequestsAverage.add(value);
    }
  }

  public void setExecutorNumPreemptableRequests(int value) {
    executorNumPreemptableRequests.set(value);
  }

  public void setNumExecutorsAvailable(int value) {
    numExecutorsAvailable.set(value);
    if (numExecutorsAvailableAverage != null) {
      numExecutorsAvailableAverage.add(value);
    }
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

  public void addMetricsQueueTime(long queueTime) {
    if (this.queueTime != null) {
      this.queueTime.addValue(queueTime);
    }
  }

  public void addMetricsRunningTime(long runningTime) {
    if (this.runningTime != null) {
      this.runningTime.addValue(runningTime);
    }
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
  public void setNumExecutors(int size) {
    numExecutors.set(size);
  }

  private void getExecutorStats(MetricsRecordBuilder rb) {
    updateThreadMetrics(rb);
    final int totalConfiguredSlots = waitQueueSizeConfigured + numExecutorsConfigured;
    final int totalSlots = waitQueueSize.value() + numExecutors.value();
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
        .addGauge(ExecutorMaxFreeSlotsConfigured, totalConfiguredSlots)
        .addGauge(ExecutorMaxFreeSlots, totalSlots)
        .addGauge(ExecutorNumExecutors, numExecutors.value())
        .addGauge(ExecutorNumExecutorsConfigured, numExecutorsConfigured)
        .addGauge(ExecutorWaitQueueSizeConfigured, waitQueueSizeConfigured)
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
    if (numExecutorsAvailableAverage != null) {
      rb.addGauge(ExecutorNumExecutorsAvailableAverage, numExecutorsAvailableAverage.value());
    }
    if (executorNumQueuedRequestsAverage != null) {
      rb.addGauge(ExecutorNumQueuedRequestsAverage, executorNumQueuedRequestsAverage.value());
    }

    if (queueTime != null) {
      rb.addGauge(AverageQueueTime, queueTime.getSum() / queueTime.getN());
    }
    if (runningTime != null) {
      rb.addGauge(AverageResponseTime, runningTime.getSum() / runningTime.getN());
    }

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

      for (int i=0; i<numExecutorsConfigured; i++) {
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

  public int getNumExecutorsAvailable() {
    return numExecutorsAvailable.value();
  }

  public int getWaitQueueSize() {
    return waitQueueSize.value();
  }

  /**
   * Generate time aware average for data points.
   * For example if we have 3s when the queue size is 1, and 1s when the queue size is 2 then the
   * calculated average should be (3*1+1*2)/4 = 1.25.
   */
  @VisibleForTesting
  static class TimedAverageMetrics {
    private final int windowDataSize;
    private final long windowTimeSize;
    private final Data[] data;
    private int nextPos = 0;

    /**
     * Creates and initializes the metrics object.
     * @param windowDataSize The maximum number of samples stored
     * @param windowTimeSize The time window used to generate the average in nanoseconds
     */
    TimedAverageMetrics(int windowDataSize, long windowTimeSize) {
      this(windowDataSize, windowTimeSize, System.nanoTime() - windowTimeSize - 1);
    }

    @VisibleForTesting
    TimedAverageMetrics(int windowDataSize, long windowTimeSize,
        long defaultTime) {
      assert(windowDataSize > 0);
      this.windowDataSize = windowDataSize;
      this.windowTimeSize = windowTimeSize;
      this.data = new Data[windowDataSize];
      Arrays.setAll(data, i -> new Data(defaultTime, 0L));
    }

    /**
     * Adds a new sample value to the metrics.
     * @param value The new sample value
     */
    public synchronized void add(long value) {
      add(System.nanoTime(), value);
    }

    /**
     * Calculates the average for the last windowTimeSize window.
     * @return The average
     */
    public synchronized long value() {
      return value(System.nanoTime());
    }

    @VisibleForTesting
    void add(long time, long value) {
      data[nextPos].nanoTime = time;
      data[nextPos].value = value;
      nextPos++;
      if (nextPos == windowDataSize) {
        nextPos = 0;
      }
    }

    @VisibleForTesting
    long value(long time) {
      // We expect that the data time positions are strictly increasing and the time is greater than
      // any of the data position time. This is ensured by using System.nanoTime().
      long sum = 0L;
      long lastTime = time;
      long minTime = lastTime - windowTimeSize;
      int pos = nextPos - 1;
      do {
        // Loop the window
        if (pos < 0) {
          pos = windowDataSize - 1;
        }
        // If we are at the end of the window
        if (data[pos].nanoTime < minTime) {
          sum += (lastTime - minTime) * data[pos].value;
          break;
        }
        sum += (lastTime - data[pos].nanoTime) * data[pos].value;
        lastTime = data[pos].nanoTime;
        pos--;
      } while (pos != nextPos - 1);
      // If we exited the loop and we did not have enough data point estimate the data with the last
      // known point
      if (pos == nextPos - 1 && data[nextPos].nanoTime > minTime) {
        sum += (lastTime - minTime) * data[nextPos].value;
      }
      return Math.round((double)sum / (double)windowTimeSize);
    }
  }

  /**
   * Single sample data.
   */
  private static class Data {
    private long nanoTime;
    private long value;
    Data(long nanoTime, long value) {
      this.nanoTime = nanoTime;
      this.value = value;
    }
  }
}
