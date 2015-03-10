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

import static org.apache.hadoop.hive.llap.metrics.LlapDaemonQueueInfo.MaxProcessingTime;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonQueueInfo.QueueMetrics;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonQueueInfo.QueueSize;
import static org.apache.hadoop.metrics2.impl.MsInfo.ProcessName;
import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 *
 */
@Metrics(about = "LlapDaemon Queue Metrics", context = MetricsUtils.METRICS_CONTEXT)
public class LlapDaemonQueueMetrics implements MetricsSource {
  private final String name;
  private final String sessionId;
  private final MetricsRegistry registry;
  private long maxTime = Long.MIN_VALUE;

  @Metric
  MutableGaugeInt queueSize;
  @Metric
  MutableRate rateOfProcessing;
  final MutableQuantiles[] processingTimes;
  @Metric
  MutableGaugeLong maxProcessingTime;

  private LlapDaemonQueueMetrics(String displayName, String sessionId, int[] intervals) {
    this.name = displayName;
    this.sessionId = sessionId;
    this.registry = new MetricsRegistry("LlapDaemonQueueRegistry");
    this.registry.tag(ProcessName, MetricsUtils.METRICS_PROCESS_NAME).tag(SessionId, sessionId);

    final int len = intervals == null ? 0 : intervals.length;
    this.processingTimes = new MutableQuantiles[len];
    for (int i=0; i<len; i++) {
      int interval = intervals[i];
      processingTimes[i] = registry.newQuantiles(
          LlapDaemonQueueInfo.PercentileProcessingTime.name() + "_" + interval + "s",
          LlapDaemonQueueInfo.PercentileProcessingTime.description(),
          "ops", "latency", interval);
    }
  }

  public static LlapDaemonQueueMetrics create(String displayName, String sessionId, int[] intervals) {
    MetricsSystem ms = LlapMetricsSystem.instance();
    return ms.register(displayName, null, new LlapDaemonQueueMetrics(displayName, sessionId, intervals));
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean b) {
    MetricsRecordBuilder rb = collector.addRecord(QueueMetrics)
        .setContext(MetricsUtils.METRICS_CONTEXT)
        .tag(ProcessName, MetricsUtils.METRICS_PROCESS_NAME)
        .tag(SessionId, sessionId);
    getQueueStats(rb);
  }

  public String getName() {
    return name;
  }

  public void setQueueSize(int size) {
    queueSize.set(size);
  }

  public void addProcessingTime(long latency) {
    rateOfProcessing.add(latency);
    if (latency > maxTime) {
      maxTime = latency;
      maxProcessingTime.set(maxTime);
    }
    for (MutableQuantiles q : processingTimes) {
      q.add(latency);
    }
  }

  private void getQueueStats(MetricsRecordBuilder rb) {
    rb.addGauge(QueueSize, queueSize.value())
        .addGauge(MaxProcessingTime, maxProcessingTime.value())
        .addGauge(MaxProcessingTime, maxProcessingTime.value());
    rateOfProcessing.snapshot(rb, true);

    for (MutableQuantiles q : processingTimes) {
      q.snapshot(rb, true);
    }
  }
}
