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
package org.apache.hadoop.hive.ql.exec.tez;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.common.MetricsVariable;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * TezSessionPoolManagerMetrics collects, aggregates and exposes data from Tez sessions to Hive's own MetricsFactory.
 * This is independent of the Tez DagClient which is active only while running DAGs.
 */
public class TezSessionPoolManagerMetrics {
  private static final Logger LOG = LoggerFactory.getLogger(TezSessionPoolManagerMetrics.class);

  // metric names to be used by Tez sessions when returning data
  public static final String TEZ_SESSION_METRIC_RUNNING_TASKS = "tez_session_running_tasks";
  public static final String TEZ_SESSION_METRIC_PENDING_TASKS = "tez_session_pending_tasks";

  // derived metrics (not set by tez sessions, calculated here)
  public static final String TEZ_SESSION_METRIC_TASK_BACKLOG_RATIO = "tez_session_task_backlog_ratio";
  public static final String TEZ_SESSION_METRIC_TASK_BACKLOG_RATIO_PRESENT_SINCE =
      "tez_session_task_backlog_ratio_%d_present_since";
  // For scaling decisions, TezSessionPoolManagerMetrics tracks the duration
  // since the task backlog ratio exceeded a certain threshold.
  private static final Integer[] TRACKED_TASK_BACKLOG_RATIOS = {1, 2, 5, 10, 20, 50, 100};

  private final TezSessionPoolManager poolManager;
  private final Metrics metrics;
  private ScheduledExecutorService mainExecutor;

  // session pool metrics
  @VisibleForTesting
  final TezSessionMetric<Integer> runningTasksCount = new TezSessionMetric<>(TEZ_SESSION_METRIC_RUNNING_TASKS, 0);
  @VisibleForTesting
  final TezSessionMetric<Integer> pendingTasksCount = new TezSessionMetric<>(TEZ_SESSION_METRIC_PENDING_TASKS, 0);
  @VisibleForTesting
  final TezSessionMetric<Double> taskBacklogRatio = new TezSessionMetric<>(TEZ_SESSION_METRIC_TASK_BACKLOG_RATIO, 0.0);
  private final TaskBacklogRatioMetric[] taskBacklogRatioDurations = Arrays.stream(TRACKED_TASK_BACKLOG_RATIOS)
      .map(
          ratio -> new TaskBacklogRatioMetric(String.format(TEZ_SESSION_METRIC_TASK_BACKLOG_RATIO_PRESENT_SINCE, ratio),
              ratio))
      .toArray(TaskBacklogRatioMetric[]::new);


  public TezSessionPoolManagerMetrics(TezSessionPoolManager poolManager) {
    this.poolManager = poolManager;
    this.metrics = MetricsFactory.getInstance();
  }

  private static void waitForTasksToFinish(List<Future<?>> collectTasks, ExecutorService collectorExecutor) {
    try {
      CompletableFuture.allOf(collectTasks.toArray(new CompletableFuture[0])).get();
    } catch (InterruptedException e) {
      LOG.info("Interrupted while collecting session metrics", e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) { // ExecutionException wraps the original exception
      LOG.error("Exception while collecting session metrics", e.getCause());
      throw new RuntimeException(e.getCause());
    } finally {
      collectorExecutor.shutdown();
    }
  }

  public TezSessionPoolManagerMetrics start(HiveConf conf) {
    if (metrics == null) {
      LOG.warn(
          "Metrics are not enabled, cannot start TezSessionPoolManagerMetrics (check {})",
          HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED.varname);
      return this;
    }
    this.mainExecutor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("TezSessionPoolManagerMetrics worker thread").build());
    int collectIntervalSeconds = HiveConf.getIntVar(conf,
        HiveConf.ConfVars.HIVE_SERVER2_TEZ_SESSIONS_METRICS_COLLECTION_INTERVAL_SECONDS);
    mainExecutor.scheduleAtFixedRate(this::collectMetrics, 0, collectIntervalSeconds, TimeUnit.SECONDS);
    LOG.info("Starting TezSessionPoolManagerMetrics, collection interval: {}s", collectIntervalSeconds);

    runningTasksCount.createGauge(metrics);
    pendingTasksCount.createGauge(metrics);
    taskBacklogRatio.createGauge(metrics);
    for (TaskBacklogRatioMetric m : taskBacklogRatioDurations) {
      m.createGauge(metrics);
    }
    clearMetrics(); //initial empty values for gauges
    return this;
  }

  public void stop() {
    if (this.mainExecutor != null) {
      mainExecutor.shutdownNow();
    }
  }

  void collectMetrics() {
    List<TezSessionState> sessions = poolManager.getSessions();
    LOG.debug("Updating metrics, session count: {}", sessions.size());

    if (sessions.isEmpty()) {
      // no active session, don't mess with the rest
      clearMetrics();
      return;
    }

    AtomicDouble currentRunningTasksCount = new AtomicDouble();
    AtomicDouble currentPendingTasksCount = new AtomicDouble();
    List<Future<?>> collectTasks = new ArrayList<>();

    // run on maximum 10 threads
    ExecutorService collectorExecutor = Executors.newFixedThreadPool(Math.min(sessions.size(), 10),
        new ThreadFactoryBuilder().setNameFormat("TezSessionPoolManagerMetrics collector thread - " + "#%d").build());
    long start = Time.monotonicNow();

    for (TezSessionState session : sessions) {
      collectTasks.add(CompletableFuture.runAsync(() -> {
        Map<String, Double> metrics = session.getMetrics();
        LOG.debug("Achieved metrics from Tez session ({}): {}", session.getSessionId(), metrics);

        currentRunningTasksCount.addAndGet(metrics.getOrDefault(runningTasksCount.name, 0.0));
        currentPendingTasksCount.addAndGet(metrics.getOrDefault(pendingTasksCount.name, 0.0));
      }, collectorExecutor));
    }

    waitForTasksToFinish(collectTasks, collectorExecutor);
    long elapsed = Time.monotonicNow() - start;

    refreshMetrics(currentRunningTasksCount, currentPendingTasksCount);

    LOG.debug("Collected metrics from {} sessions (in {}ms): {}", sessions.size(), elapsed, toMetricsString());
  }

  @VisibleForTesting
  void refreshMetrics(AtomicDouble currentRunningTasksCount, AtomicDouble currentPendingTasksCount) {
    this.runningTasksCount.setValue((int) currentRunningTasksCount.get());
    this.pendingTasksCount.setValue((int) currentPendingTasksCount.get());
    double currentTaskBacklogRatio = (double) this.pendingTasksCount.value / (this.runningTasksCount.value + 1);
    this.taskBacklogRatio.setValue(currentTaskBacklogRatio);

    for (TaskBacklogRatioMetric m : taskBacklogRatioDurations) {
      m.refresh(currentTaskBacklogRatio);
    }
  }

  private void clearMetrics() {
    runningTasksCount.setValue(0);
    pendingTasksCount.setValue(0);
    taskBacklogRatio.setValue(0.0);
    Arrays.stream(taskBacklogRatioDurations).forEach(m -> m.timeSet = 0);
  }

  private String toMetricsString() {
    return String.format("[running tasks: %d, pending tasks: %d, task backlog ratio: %.2f, task backlog ratios seen: " +
            "{%s}]", this.runningTasksCount.value, this.pendingTasksCount.value, this.taskBacklogRatio.value,
        getBackLogRatioDurationsString());
  }

  private String getBackLogRatioDurationsString() {
    long now = Time.monotonicNow();
    return Arrays.stream(taskBacklogRatioDurations).filter(m -> m.timeSet > 0)
        .map(m -> m.name + ": " + ((now - m.timeSet) / 1000) + "s")
        .collect(Collectors.joining(", "));
  }

  /**
   * TezSessionMetric is a base adapter class for metrics to be exposed from simple primitive types to Hive metrics.
   */
  @VisibleForTesting
  static class TezSessionMetric<T> {
    String name;
    T value;
    long timeSet = 0L;

    TezSessionMetric(String name, T value) {
      this.name = name;
      this.value = value;
    }

    public void createGauge(Metrics metrics) {
      metrics.addGauge(name, (MetricsVariable<T>) () -> this.value);
    }

    public void setValue(T value) {
      this.value = value;
      this.timeSet = Time.monotonicNow();
    }
  }

  /**
   * TaskBacklogRatioMetric is a special type of metric where the key information is the time elapsed
   * since the initial timeSet value was recorded (rather than a static value).
   * Here's how it works:
   * 1. TaskBacklogRatioMetric is instantiated with a specific ratio to monitor.
   * 2. On every updateMetrics call, the refresh() method is invoked with the current task backlog ratio.
   * 3. If currentTaskBacklogRatio is greater than or equal to the monitored ratio, it begins tracking time
   *    by setting timeSet to now()—but only if it hasn't been set already.
   * 4. If currentTaskBacklogRatio drops below the monitored ratio, timeSet is reset to 0.
   */
  private static class TaskBacklogRatioMetric extends TezSessionMetric<Long> {
    private final Integer ratio;

    public TaskBacklogRatioMetric(String name, Integer ratio) {
      super(name, 0L);
      this.ratio = ratio;
    }

    public void createGauge(Metrics metrics) {
      // The gauge returns timeSet - now() if timeSet > 0.
      metrics.addGauge(name, (MetricsVariable<Long>) () -> (timeSet == 0) ? 0 :
          ((Time.monotonicNow() - timeSet) / 1000));
    }

    public void refresh(double currentTaskBacklogRatio) {
      if (currentTaskBacklogRatio >= this.ratio) {
        if (timeSet == 0) { // this is the first time we see this backlog ratio
          timeSet = Time.monotonicNow();
        }
      } else {
        // current backlog ratio is lower than this threshold, let's clear
        timeSet = 0;
      }
    }
  }
}
