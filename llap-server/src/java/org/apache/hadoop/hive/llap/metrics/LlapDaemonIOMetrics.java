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

import static org.apache.hadoop.hive.llap.metrics.LlapDaemonIOInfo.IOMetrics;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonIOInfo.MaxDecodingTime;
import static org.apache.hadoop.metrics2.impl.MsInfo.ProcessName;
import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@Metrics(about = "LlapDaemon IO Metrics", context = "io")
public class LlapDaemonIOMetrics implements MetricsSource {
  protected static final Logger LOG = LoggerFactory.getLogger(LlapDaemonIOMetrics.class);
  private final String name;
  private final String sessionId;
  private final MetricsRegistry registry;
  private long maxTime = Long.MIN_VALUE;

  @Metric
  MutableRate rateOfDecoding;
  final MutableQuantiles[] decodingTimes;
  @Metric
  MutableGaugeLong maxDecodingTime;

  private LlapDaemonIOMetrics(String displayName, String sessionId, int[] intervals) {
    this.name = displayName;
    this.sessionId = sessionId;
    this.registry = new MetricsRegistry("LlapDaemonIORegistry");
    this.registry.tag(ProcessName, MetricsUtils.METRICS_PROCESS_NAME).tag(SessionId, sessionId);

    final int len = intervals == null ? 0 : intervals.length;
    this.decodingTimes = new MutableQuantiles[len];
    for (int i=0; i<len; i++) {
      int interval = intervals[i];
      LOG.info("Created interval " + LlapDaemonIOInfo.PercentileDecodingTime.name() + "_" + interval + "s");
      decodingTimes[i] = registry.newQuantiles(
          LlapDaemonIOInfo.PercentileDecodingTime.name() + "_" + interval + "s",
          LlapDaemonIOInfo.PercentileDecodingTime.description(),
          "ops", "latency", interval);
    }
  }

  public static LlapDaemonIOMetrics create(String displayName, String sessionId, int[] intervals) {
    MetricsSystem ms = LlapMetricsSystem.instance();
    return ms.register(displayName, null, new LlapDaemonIOMetrics(displayName, sessionId, intervals));
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean b) {
    MetricsRecordBuilder rb = collector.addRecord(IOMetrics)
        .setContext("io")
        .tag(ProcessName, MetricsUtils.METRICS_PROCESS_NAME)
        .tag(SessionId, sessionId);
    getIoStats(rb);
  }

  public String getName() {
    return name;
  }

  public void addDecodeBatchTime(long latency) {
    rateOfDecoding.add(latency);
    if (latency > maxTime) {
      maxTime = latency;
      maxDecodingTime.set(maxTime);
    }
    for (MutableQuantiles q : decodingTimes) {
      q.add(latency);
    }
  }

  private void getIoStats(MetricsRecordBuilder rb) {
    rb.addGauge(MaxDecodingTime, maxDecodingTime.value());
    rateOfDecoding.snapshot(rb, true);

    for (MutableQuantiles q : decodingTimes) {
      q.snapshot(rb, true);
    }
  }

}
