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

package org.apache.hive.kubernetes.operator.autoscaling;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hive.kubernetes.operator.model.spec.AutoscalingSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scaling strategy for Hive Metastore.
 * HMS uses HTTP transport — connections are per-request (stateless), so
 * open_connections is always ~0. Instead we compute API request rate:
 * rate = (sum(api_*_total) - previous_sum) / elapsed_seconds.
 * desired = ceil(rate / scaleUpThreshold)
 */
public class MetastoreScalingStrategy implements ScalingStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(MetastoreScalingStrategy.class);
  private static final String API_COUNTER_PREFIX = "api_";
  private static final String API_COUNTER_SUFFIX = "_total";

  // Previous scrape state for rate computation
  private final ConcurrentHashMap<String, Double> previousCounters = new ConcurrentHashMap<>();
  private long previousTimestampMs = 0;
  private int lastMetric;

  @Override
  public int computeDesiredReplicas(List<PodMetrics> podMetrics,
      AutoscalingSpec autoscaling, int maxReplicas) {

    // Sum all api_*_total counters across all pods
    double currentTotal = 0;
    for (PodMetrics pm : podMetrics) {
      for (Map.Entry<String, Double> entry : pm.metrics().entrySet()) {
        String name = entry.getKey();
        if (name.startsWith(API_COUNTER_PREFIX) && name.endsWith(API_COUNTER_SUFFIX)) {
          currentTotal += entry.getValue();
        }
      }
    }

    long now = System.currentTimeMillis();
    double rate = 0;

    if (previousTimestampMs > 0) {
      double elapsedSeconds = (now - previousTimestampMs) / 1000.0;
      if (elapsedSeconds > 0) {
        double previousTotal = previousCounters.values().stream()
            .mapToDouble(Double::doubleValue).sum();
        double delta = currentTotal - previousTotal;
        if (delta < 0) {
          // Counter reset (pod restart) — skip this sample
          delta = 0;
        }
        rate = delta / elapsedSeconds;
      }
    }

    // Store current state for next evaluation
    previousCounters.clear();
    previousCounters.put(API_COUNTER_SUFFIX, currentTotal);
    previousTimestampMs = now;

    lastMetric = (int) Math.round(rate);

    if (rate <= 0) {
      return autoscaling.minReplicas();
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("[metastore] API request rate: {}/s, threshold: {}",
          String.format("%.2f", rate), autoscaling.scaleUpThreshold());
    }

    int threshold = Math.max(1, autoscaling.scaleUpThreshold());
    int desired = (int) Math.ceil(rate / threshold);
    return Math.max(desired, autoscaling.minReplicas());
  }

  @Override
  public int lastMetricValue() {
    return lastMetric;
  }
}
