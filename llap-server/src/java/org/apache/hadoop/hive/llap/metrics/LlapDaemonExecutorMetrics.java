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

import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.EXECUTOR_NUM_QUEUED_REQUESTS;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.EXECUTOR_TOTAL_ASKED_TO_DIE;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.EXECUTOR_TOTAL_EXECUTION_FAILURE;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.EXECUTOR_TOTAL_INTERRUPTED;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.EXECUTOR_TOTAL_REQUESTS_HANDLED;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.EXECUTOR_TOTAL_SUCCESS;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.LLAP_DAEMON_EXECUTOR_METRICS;
import static org.apache.hadoop.metrics2.impl.MsInfo.ProcessName;
import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.source.JvmMetrics;

/**
 * Metrics about the llap daemon executors.
 */
@Metrics(about = "LlapDaemon Executor Metrics", context = "llap")
public class LlapDaemonExecutorMetrics implements MetricsSource {

  private final String name;
  private final JvmMetrics jvmMetrics;
  private final String sessionId;
  private final MetricsRegistry registry;
  private final int numExecutors;

  @Metric
  MutableCounterLong[] executorThreadCpuTime;
  @Metric
  MutableCounterLong[] executorThreadUserTime;
  @Metric
  MutableCounterLong[] executorThreadSystemTime;
  @Metric
  MutableCounterLong executorTotalRequestHandled;
  @Metric
  MutableCounterLong executorNumQueuedRequests;
  @Metric
  MutableCounterLong executorTotalSuccess;
  @Metric
  MutableCounterLong executorTotalInterrupted;
  @Metric
  MutableCounterLong executorTotalExecutionFailed;
  @Metric
  MutableCounterLong executorTotalAskedToDie;

  private LlapDaemonExecutorMetrics(String displayName, JvmMetrics jm, String sessionId,
      int numExecutors) {
    this.name = displayName;
    this.jvmMetrics = jm;
    this.sessionId = sessionId;
    this.registry = new MetricsRegistry("LlapDaemonExecutorRegistry");
    this.numExecutors = numExecutors;
    this.executorThreadCpuTime = new MutableCounterLong[numExecutors];
    this.executorThreadUserTime = new MutableCounterLong[numExecutors];
    this.executorThreadSystemTime = new MutableCounterLong[numExecutors];
  }

  public static LlapDaemonExecutorMetrics create(String displayName, String sessionId,
      int numExecutors) {
    MetricsSystem ms = LlapMetricsSystem.instance();
    JvmMetrics jm = JvmMetrics.create("LlapDaemon", sessionId, ms);
    return ms.register(displayName, "LlapDaemon Executor Metrics",
        new LlapDaemonExecutorMetrics(displayName, jm, sessionId, numExecutors));
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean b) {
    MetricsRecordBuilder rb = collector.addRecord(LLAP_DAEMON_EXECUTOR_METRICS)
        .setContext("llap").tag(ProcessName, "LlapDaemon")
        .tag(SessionId, sessionId);
    getExecutorStats(rb);
  }

  // Assumption here is threadId is from 0 to numExecutors - 1
  public void incrExecutorThreadCpuTime(int threadId, int delta) {
    executorThreadCpuTime[threadId].incr(delta);
  }

  public void incrExecutorThreadUserTime(int threadId, int delta) {
    executorThreadUserTime[threadId].incr(delta);
  }

  public void incrExecutorThreadSystemTime(int threadId, int delta) {
    executorThreadSystemTime[threadId].incr(delta);
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

  public void incrExecutorTotalInterrupted() {
    executorTotalInterrupted.incr();
  }

  public void incrExecutorTotalAskedToDie() {
    executorTotalAskedToDie.incr();
  }

  private void getExecutorStats(MetricsRecordBuilder rb) {
    // TODO: Enable this after adding InstrumentedThreadPool executor
//    for (int i = 0; i < numExecutors; i++) {
//      rb.addCounter(EXECUTOR_THREAD_CPU_TIME, executorThreadCpuTime[i].value())
//          .addCounter(EXECUTOR_THREAD_USER_TIME, executorThreadUserTime[i].value())
//          .addCounter(EXECUTOR_THREAD_SYSTEM_TIME, executorThreadSystemTime[i].value());
//    }

    rb.addCounter(EXECUTOR_TOTAL_REQUESTS_HANDLED, executorTotalRequestHandled.value())
        .addCounter(EXECUTOR_NUM_QUEUED_REQUESTS, executorNumQueuedRequests.value())
        .addCounter(EXECUTOR_TOTAL_SUCCESS, executorTotalSuccess.value())
        .addCounter(EXECUTOR_TOTAL_EXECUTION_FAILURE, executorTotalExecutionFailed.value())
        .addCounter(EXECUTOR_TOTAL_INTERRUPTED, executorTotalInterrupted.value())
        .addCounter(EXECUTOR_TOTAL_ASKED_TO_DIE, executorTotalAskedToDie.value());
  }

  public JvmMetrics getJvmMetrics() {
    return jvmMetrics;
  }

  public String getName() {
    return name;
  }
}
