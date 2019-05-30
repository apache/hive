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

package org.apache.hadoop.hive.llap.tezplugins.metrics;


import static org.junit.Assert.assertEquals;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.hive.llap.metrics.MockMetricsCollector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Test the LlapTaskScheduleMetrics methods.
 */
public class TestLlapTaskSchedulerMetrics {
  private LlapTaskSchedulerMetrics metrics = null;

  @Before
  public void setUp() {
    metrics = LlapTaskSchedulerMetrics.create("TestMetrics", "TestSession", 1024);
  }

  /**
   * Test if adding / removing daemons are working as excepted.
   */
  @Test(timeout = 1000000000)
  public void testDaemonCount() {
    metrics.addTaskLatency("key1", 10);
    metrics.addTaskLatency("key2", 20);
    metrics.addTaskLatency("key1", 15);

    MockMetricsCollector tmc = null;
    Map<MetricsInfo, Number> metricMap = null;

    tmc = new MockMetricsCollector();
    metrics.getMetrics(tmc, true);
    metricMap = tmc.getRecords().get(0).getMetrics();
    verifyDaemonMetrics(metricMap, Arrays.asList("key1", "key2"));
  }

  private void verifyDaemonMetrics(Map<MetricsInfo, Number> metricsMap, List<String> expectedKeys) {
    List<String> foundKeys = new ArrayList<>(expectedKeys.size());
    metricsMap.keySet().forEach(info -> {
      if (info instanceof LlapTaskSchedulerMetrics.DaemonLatencyMetric) {
        LlapTaskSchedulerMetrics.DaemonLatencyMetric dlm = (LlapTaskSchedulerMetrics.DaemonLatencyMetric)info;
        foundKeys.add(dlm.name());
      }
    });
    Collections.sort(expectedKeys);
    Collections.sort(foundKeys);
    assertEquals("Did not found every expected key", expectedKeys, foundKeys);
  }
}
