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

import com.google.common.collect.Maps;
import org.apache.hadoop.hive.llap.daemon.impl.DumpingMetricsCollector;
import org.junit.Test;

import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorAvailableFreeSlots;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorMaxFreeSlotsConfigured;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorMaxFreeSlots;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorNumExecutors;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorNumExecutorsAvailable;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorNumExecutorsConfigured;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorNumQueuedRequests;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorWaitQueueSize;
import static org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo.ExecutorWaitQueueSizeConfigured;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorMetrics.TimedAverageMetrics;
import java.util.Map;

/**
 * Test class for LlapDaemonExecutorMetrics.
 */
public class TestLlapDaemonExecutorMetrics {

  /**
   * Test generated values for TimedAverageMetrics when the time window is smaller than the amount
   * of data we have stored.
   */
  @Test
  public void testTimedAverageMetricsTimeBound() {
    TimedAverageMetrics metrics;

    // Window 1
    metrics = generateTimedAverageMetrics(1, 10, 1, 100, 0, 0, 1);
    assertEquals("Window size 1", 100, metrics.value(100));

    // Window 1 with multiple data
    metrics = generateTimedAverageMetrics(1, 10, 50, 100, 0, 0, 1);
    assertEquals("Window size 1 with multiple data", 100, metrics.value(100));

    // Single point in the middle
    metrics = generateTimedAverageMetrics(10, 100, 1, 100, 0, 50, 1);
    assertEquals("Single point in the middle", 50, metrics.value(100));

    // Single point at 3/4
    metrics = generateTimedAverageMetrics(10, 100, 1, 100, 0, 75, 1);
    assertEquals("Single point at 3/4", 25, metrics.value(100));

    // Single point at 1/4
    metrics = generateTimedAverageMetrics(10, 100, 1, 100, 0, 25, 1);
    assertEquals("Single point at 1/4", 75, metrics.value(100));

    // Multiple points after 1/4
    metrics = generateTimedAverageMetrics(10, 100, 3, 100, 0, 25, 25);
    assertEquals("Multiple points after 1/4", 75, metrics.value(100));

    // More points with overflow
    metrics = generateTimedAverageMetrics(10, 100, 18, 100, 0, 25, 25);
    assertEquals("More points with overflow", 100, metrics.value(450));

    // Very old points
    metrics = generateTimedAverageMetrics(10, 100, 20, 100, 0, 25, 25);
    assertEquals("Very old points", 100, metrics.value(5000));
    metrics.add(1000, 10);
    assertEquals("Very old points but not that old", 10, metrics.value(5000));
  }

  /**
   * Test generated values for TimedAverageMetrics when we have less data points than the window.
   */
  @Test
  public void testTimedAverageMetricsDataBound() {
    TimedAverageMetrics metrics;

    // Window 1
    metrics = generateTimedAverageMetrics(1, 100, 1, 100, 0, 50, 1);
    assertEquals("Window size 1", 100, metrics.value(100));

    // Overflow at the bottom
    metrics = generateTimedAverageMetrics(3, 100, 4, 50, 10, 50, 10);
    assertEquals("Window size 1 with multiple data", 65, metrics.value(100));

  }

  /**
   * Test that TimedAverageMetrics throws an exception if the window size is 0.
   */
  @Test(expected = AssertionError.class)
  public void testTimedAverageMetricsWindowSizeZero() {
    generateTimedAverageMetrics(0, 100, 2, 50, 50, 0, 50);
  }

  /**
   * Test TimedAverageMetrics with changing data to see that we handle array edge cases correctly.
   */
  @Test
  public void testTimedAverageMetricsChanging() {
    TimedAverageMetrics metrics;

    metrics = generateTimedAverageMetrics(3, 30, 6, 0, 10, 0, 10);
    assertEquals("Position 0", 40, metrics.value(60));

    metrics = generateTimedAverageMetrics(3, 30, 5, 0, 10, 0, 10);
    assertEquals("Position windowDataSize - 1", 30, metrics.value(50));
  }

  /**
   * Test the real interfaces of the TimedAverageMetrics.
   */
  @Test
  public void testTimedAverageMetricsReal() {
    TimedAverageMetrics metrics =
        new TimedAverageMetrics(10, 6 * 1000 * 1000);

    for (int i = 0; i < 50; i++) {
      metrics.add(100);
      try {
        Thread.sleep(10);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }
    assertEquals("Checking the calculated value", 100, metrics.value());
  }

  /**
   * Check for the maximum expected amount of data for TimedAverageMetrics.
   * 15000 data point / 10 minute window / data every 50 ms.
   */
  @Test
  public void testTimedAverageMetricsBigData() {
    long currentTime = System.nanoTime();
    // Data range in metrics from [0 / 250s] till [14999 / (1000s - 50ms)]
    TimedAverageMetrics metrics = generateTimedAverageMetrics(15000,
        10L * 60L * 1000L * 1000L * 1000L, 20000, -5000, 1, currentTime, 50L * 1000L * 1000L);

    // Checking value from [3000/600s] - [14999/1000s] -> 8999.5
    assertEquals("Checking the calculated value", 9000, metrics.value(currentTime + 50L * 1000L * 1000L * 20000L));
  }

  @Test
  public void testSimpleAndDerivedMetricsCalculations() {
    int numExecutorsConfigured = 4;
    int numExecutors = 2;
    int numExecutorsAvailable = 1;
    int waitQueueSizeConfigured = 10;
    int waitQueueSize = 5;
    int queuedRequests = 3;

    LlapDaemonExecutorMetrics metrics = LlapDaemonExecutorMetrics.create("test", "test", numExecutorsConfigured,
        waitQueueSizeConfigured, new int[]{1}, 1, 1, 1);
    metrics.setNumExecutors(numExecutors);
    metrics.setNumExecutorsAvailable(numExecutorsAvailable);
    metrics.setWaitQueueSize(waitQueueSize);
    metrics.setExecutorNumQueuedRequests(queuedRequests);
    Map<String, Long> data = Maps.newHashMap();
    metrics.getMetrics(new DumpingMetricsCollector(data), true);

    // Simple values
    assertTrue(numExecutorsConfigured == data.get(ExecutorNumExecutorsConfigured.name()));
    assertTrue(numExecutors == data.get(ExecutorNumExecutors.name()));
    assertTrue(waitQueueSizeConfigured == data.get(ExecutorWaitQueueSizeConfigured.name()));
    assertTrue(waitQueueSize == data.get(ExecutorWaitQueueSize.name()));
    assertTrue(queuedRequests == data.get(ExecutorNumQueuedRequests.name()));
    assertTrue(numExecutorsAvailable == data.get(ExecutorNumExecutorsAvailable.name()));

    // Derived values
    assertTrue((waitQueueSizeConfigured + numExecutorsConfigured) == data.get(ExecutorMaxFreeSlotsConfigured.name()));
    assertTrue((waitQueueSize + numExecutors) == data.get(ExecutorMaxFreeSlots.name()));
    assertTrue((waitQueueSize + numExecutorsAvailable - queuedRequests) == data.get(ExecutorAvailableFreeSlots.name()));
  }

  private TimedAverageMetrics generateTimedAverageMetrics(int windowDataSize, long windowTimeSize, int dataNum,
      long firstData, long dataDelta, long firstTime, long timeDelta) {
    TimedAverageMetrics metrics =
        new TimedAverageMetrics(windowDataSize, windowTimeSize, firstTime - windowTimeSize);

    for (int i = 0; i < dataNum; i++) {
      metrics.add(firstTime + i * timeDelta, firstData + i * dataDelta);
    }
    return metrics;
  }
}
