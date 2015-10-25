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

import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorNumQueuedRequests;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorThreadCPUTime;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorThreadUserTime;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorTotalAskedToDie;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorTotalExecutionFailure;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorTotalInterrupted;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorTotalRequestsHandled;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorTotalSuccess;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorMetrics;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.PreemptionTimeLost;
import static org.apache.hadoop.metrics2.impl.MsInfo.ProcessName;
import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.source.JvmMetrics;

/**
 * Metrics about the llap daemon executors.
 */
@Metrics(about = "LlapDaemon Executor Metrics", context = MetricsUtils.METRICS_CONTEXT)
public class LlapDaemonExecutorMetrics implements MetricsSource {

  private final String name;
  private final JvmMetrics jvmMetrics;
  private final String sessionId;
  private final MetricsRegistry registry;
  private final int numExecutors;
  private final ThreadMXBean threadMXBean;
  private final Map<Integer, MetricsInfo> cpuMetricsInfoMap;
  private final Map<Integer, MetricsInfo> userMetricsInfoMap;

  final MutableGaugeLong[] executorThreadCpuTime;
  final MutableGaugeLong[] executorThreadUserTime;
  @Metric
  MutableCounterLong executorTotalRequestHandled;
  @Metric
  MutableCounterLong executorNumQueuedRequests;
  @Metric
  MutableCounterLong executorTotalSuccess;
  @Metric
  MutableCounterLong executorTotalIKilled;
  @Metric
  MutableCounterLong executorTotalExecutionFailed;
  @Metric
  MutableCounterLong preemptionTimeLost;


  private LlapDaemonExecutorMetrics(String displayName, JvmMetrics jm, String sessionId,
      int numExecutors) {
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

    for (int i = 0; i < numExecutors; i++) {
      MetricsInfo mic = new LlapDaemonCustomMetricsInfo(ExecutorThreadCPUTime.name() + "_" + i,
          ExecutorThreadCPUTime.description());
      MetricsInfo miu = new LlapDaemonCustomMetricsInfo(ExecutorThreadUserTime.name() + "_" + i,
          ExecutorThreadUserTime.description());
      this.cpuMetricsInfoMap.put(i, mic);
      this.userMetricsInfoMap.put(i, miu);
      this.executorThreadCpuTime[i] = registry.newGauge(mic, 0L);
      this.executorThreadUserTime[i] = registry.newGauge(miu, 0L);
    }
  }

  public static LlapDaemonExecutorMetrics create(String displayName, String sessionId,
      int numExecutors) {
    MetricsSystem ms = LlapMetricsSystem.instance();
    JvmMetrics jm = JvmMetrics.create(MetricsUtils.METRICS_PROCESS_NAME, sessionId, ms);
    return ms.register(displayName, "LlapDaemon Executor Metrics",
        new LlapDaemonExecutorMetrics(displayName, jm, sessionId, numExecutors));
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean b) {
    MetricsRecordBuilder rb = collector.addRecord(ExecutorMetrics)
        .setContext(MetricsUtils.METRICS_CONTEXT)
        .tag(ProcessName, MetricsUtils.METRICS_PROCESS_NAME)
        .tag(SessionId, sessionId);
    getExecutorStats(rb);
  }

  public void incrExecutorTotalRequestsHandled() {
    executorTotalRequestHandled.incr();
  }

  public void incrExecutorNumQueuedRequests() {
    executorNumQueuedRequests.incr();
  }

  public void decrExecutorNumQueuedRequests() {
    executorNumQueuedRequests.incr(-1);
  }

  public void incrExecutorTotalSuccess() {
    executorTotalSuccess.incr();
  }

  public void incrExecutorTotalExecutionFailed() {
    executorTotalExecutionFailed.incr();
  }

  public void incrPreemptionTimeLost(long value) {
    preemptionTimeLost.incr(value);
  }

  public void incrExecutorTotalKilled() {
    executorTotalIKilled.incr();
  }


  private void getExecutorStats(MetricsRecordBuilder rb) {
    updateThreadMetrics(rb);

    rb.addCounter(ExecutorTotalRequestsHandled, executorTotalRequestHandled.value())
        .addCounter(ExecutorNumQueuedRequests, executorNumQueuedRequests.value())
        .addCounter(ExecutorTotalSuccess, executorTotalSuccess.value())
        .addCounter(ExecutorTotalExecutionFailure, executorTotalExecutionFailed.value())
        .addCounter(ExecutorTotalInterrupted, executorTotalIKilled.value())
        .addCounter(PreemptionTimeLost, preemptionTimeLost.value());
  }

  private void updateThreadMetrics(MetricsRecordBuilder rb) {
    if (threadMXBean.isThreadCpuTimeSupported() && threadMXBean.isThreadCpuTimeEnabled()) {
      final long[] ids = threadMXBean.getAllThreadIds();
      final ThreadInfo[] infos = threadMXBean.getThreadInfo(ids);
      for (int i = 0; i < ids.length; i++) {
        ThreadInfo threadInfo = infos[i];
        String threadName = threadInfo.getThreadName();
        long threadId = ids[i];
        for (int j = 0; j < numExecutors; j++) {
          if (threadName.equals(ContainerRunnerImpl.THREAD_NAME_FORMAT_PREFIX + j)) {
            executorThreadCpuTime[j].set(threadMXBean.getThreadCpuTime(threadId));
            executorThreadUserTime[j].set(threadMXBean.getThreadUserTime(threadId));
          }
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
