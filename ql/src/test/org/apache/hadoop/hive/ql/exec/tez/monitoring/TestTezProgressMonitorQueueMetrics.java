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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.exec.tez.monitoring;

import org.apache.hadoop.hive.ql.exec.tez.monitoring.yarnqueue.QueueMetricsCollector;
import org.apache.hadoop.hive.ql.exec.tez.monitoring.yarnqueue.QueueMetricsSnapshot;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.hadoop.hive.common.log.InPlaceUpdate;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

/**
 * Test cases for TezProgressMonitor queue metrics functionality.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestTezProgressMonitorQueueMetrics {

  @Mock
  private DAGClient mockDagClient;
  @Mock
  private DAGStatus mockDagStatus;
  @Mock
  private QueueMetricsCollector mockMetricsCollector;
  @Mock
  private QueueMetricsSnapshot mockSnapshot;
  @Mock
  private SessionState.LogHelper mockConsole;

  @Before
  public void setUp() {
    // Default: metrics are enabled and snapshot uses standard baseline values.
    // Lenient because not every test exercises all snapshot fields.
    lenient().when(mockMetricsCollector.isEnabled()).thenReturn(true);
    lenient().when(mockMetricsCollector.getLatestSnapshot()).thenReturn(mockSnapshot);
    lenient().when(mockMetricsCollector.getQueueName()).thenReturn("default");
    lenient().when(mockDagStatus.getState()).thenReturn(DAGStatus.State.RUNNING);

    long now = System.currentTimeMillis();
    lenient().when(mockSnapshot.getMemoryUsedGB()).thenReturn(1.0f);
    lenient().when(mockSnapshot.getMemoryTotalGB()).thenReturn(10.0f);
    lenient().when(mockSnapshot.getMemoryPercentage()).thenReturn("10.00%");
    lenient().when(mockSnapshot.getVCoresUsed()).thenReturn(10);
    lenient().when(mockSnapshot.getVCoresTotal()).thenReturn(100);
    lenient().when(mockSnapshot.getVCoresPercentage()).thenReturn("10.00%");
    lenient().when(mockSnapshot.getCapacityPercentage()).thenReturn(50.0f);
    lenient().when(mockSnapshot.getCurrentCapacityPercentage()).thenReturn(10.0f);
    lenient().when(mockSnapshot.getRunningApps()).thenReturn(1);
    lenient().when(mockSnapshot.getPendingApps()).thenReturn(0);
    lenient().when(mockSnapshot.getAllocatedContainers()).thenReturn(2);
    lenient().when(mockSnapshot.getPendingContainers()).thenReturn(0);
    lenient().when(mockSnapshot.getCollectionTimestamp()).thenReturn(now - 5000);
  }

  @Test
  public void testQueueMetricsWithNullCollector() throws Exception {

    TezProgressMonitor monitor = new TezProgressMonitor(
        mockDagClient, mockDagStatus, new ArrayList<>(), new HashMap<>(), mockConsole,
        System.currentTimeMillis(), null);

    assertEquals("Should return empty string when collector is null", "", monitor.queueMetrics());
  }

  @Test
  public void testQueueMetricsUnavailableWhenSnapshotNull() throws Exception {

    when(mockMetricsCollector.getLatestSnapshot()).thenReturn(null);

    TezProgressMonitor monitor = new TezProgressMonitor(
        mockDagClient, mockDagStatus, new ArrayList<>(), new HashMap<>(), mockConsole,
        System.currentTimeMillis(), mockMetricsCollector);

    assertEquals("Should return 'unavailable' when enabled but snapshot is null",
        "QUEUE: unavailable", monitor.queueMetrics());
  }

  @Test
  public void testQueueMetricsDisabled() throws Exception {

    when(mockMetricsCollector.isEnabled()).thenReturn(false);

    TezProgressMonitor monitor = new TezProgressMonitor(
        mockDagClient, mockDagStatus, new ArrayList<>(), new HashMap<>(), mockConsole,
        System.currentTimeMillis(), mockMetricsCollector);

    assertEquals("Should return empty string when metrics are disabled", "", monitor.queueMetrics());
  }

  @Test
  public void testQueueMetricsFormatting() throws Exception {

    when(mockSnapshot.getMemoryUsedGB()).thenReturn(8.5f);
    when(mockSnapshot.getMemoryTotalGB()).thenReturn(16.0f);
    when(mockSnapshot.getMemoryPercentage()).thenReturn("53.12%");
    when(mockSnapshot.getVCoresUsed()).thenReturn(100);
    when(mockSnapshot.getVCoresTotal()).thenReturn(200);
    when(mockSnapshot.getVCoresPercentage()).thenReturn("50.00%");
    when(mockSnapshot.getCapacityPercentage()).thenReturn(60.0f);
    when(mockSnapshot.getCurrentCapacityPercentage()).thenReturn(25.0f);
    when(mockSnapshot.getRunningApps()).thenReturn(5);
    when(mockSnapshot.getPendingApps()).thenReturn(2);
    when(mockSnapshot.getAllocatedContainers()).thenReturn(12);
    when(mockSnapshot.getPendingContainers()).thenReturn(10);

    TezProgressMonitor monitor = new TezProgressMonitor(
        mockDagClient, mockDagStatus, new ArrayList<>(), new HashMap<>(), mockConsole,
        System.currentTimeMillis(), mockMetricsCollector);

    String[] lines = monitor.queueMetrics().split("\n");
    assertEquals("Should have 4 lines", 4, lines.length);
    assertTrue("Line 1 should contain queue name", lines[0].contains("QUEUE: default"));
    assertFalse("Line 1 should NOT contain staleness", lines[0].contains("ago"));
    assertTrue("Line 2 should contain memory info", lines[1].contains("MEMORY: 8.5/16.0 GB"));
    assertTrue("Line 2 should contain 'used' label", lines[1].contains("53.12% used"));
    assertTrue("Line 2 should contain vCores info", lines[1].contains("VCORES: 100/200"));
    assertTrue("Line 2 should contain vCores 'used' label", lines[1].contains("50.00% used"));
    assertTrue("Line 3 should contain capacity used", lines[2].contains("CAPACITY: 25.00% (used)"));
    assertTrue("Line 3 should contain capacity allocated", lines[2].contains("60.00% (allocated)"));
    assertTrue("Line 4 should contain running apps", lines[3].contains("APPS: 5 running"));
    assertTrue("Line 4 should contain pending apps", lines[3].contains("2 pending"));
    assertTrue("Line 4 should contain allocated containers", lines[3].contains("CONTAINERS: 12 allocated"));
    assertTrue("Line 4 should contain pending containers", lines[3].contains("10 pending"));
  }

  /**
   * Tests that staleness is NOT shown in the new format regardless of age.
   * Tests multiple staleness scenarios: fresh (0s), at boundary (60s), and stale (90s).
   */
  @Test
  public void testQueueMetricsStalenessNotShown() throws Exception {
    long[] snapshotAgesSeconds = {0L, 60L, 90L};
    String[] descriptions = {"fresh (0s)", "at boundary (60s)", "stale (90s)"};

    for (int i = 0; i < snapshotAgesSeconds.length; i++) {
      long ageSeconds = snapshotAgesSeconds[i];
      String desc = descriptions[i];

      // Update snapshot timestamp for this iteration
      long now = System.currentTimeMillis();
      when(mockSnapshot.getCollectionTimestamp()).thenReturn(now - (ageSeconds * 1000));

      TezProgressMonitor monitor = new TezProgressMonitor(
          mockDagClient, mockDagStatus, new ArrayList<>(), new HashMap<>(), mockConsole,
          now, mockMetricsCollector);

      String[] lines = monitor.queueMetrics().split("\n");
      assertEquals("Should have 4 lines for " + desc, 4, lines.length);
      assertFalse("Line 1 should NOT show staleness (removed from new format) for " + desc,
          lines[0].contains("ago"));
      assertTrue("Line 1 should contain QUEUE: default for " + desc, lines[0].contains("QUEUE: default"));
    }
  }

  @Test
  public void testQueueNameTruncation() throws Exception {

    when(mockMetricsCollector.getQueueName()).thenReturn(
        "root.production.analytics.data-engineering.team-alpha.project-beta");

    TezProgressMonitor monitor = new TezProgressMonitor(
        mockDagClient, mockDagStatus, new ArrayList<>(), new HashMap<>(), mockConsole,
        System.currentTimeMillis(), mockMetricsCollector);

    String[] lines = monitor.queueMetrics().split("\n");
    assertEquals("Should have 4 lines", 4, lines.length);
    assertTrue("Line 1 should not exceed 94 characters", lines[0].length() <= 94);
    if (lines[0].contains("...")) {
      assertFalse("Full long queue name should not appear",
          lines[0].contains("root.production.analytics.data-engineering.team-alpha.project-beta"));
    }
    assertFalse("Line 1 should NOT contain staleness", lines[0].contains("ago"));
    assertTrue("Line 2 should contain MEMORY", lines[1].contains("MEMORY:"));
    assertTrue("Line 3 should contain CAPACITY", lines[2].contains("CAPACITY:"));
  }

  @Test
  public void testQueueMetricsWithZeroPercentages() throws Exception {

    when(mockMetricsCollector.getQueueName()).thenReturn("empty");
    when(mockSnapshot.getMemoryUsedGB()).thenReturn(0.0f);
    when(mockSnapshot.getMemoryTotalGB()).thenReturn(0.0f);
    when(mockSnapshot.getMemoryPercentage()).thenReturn("N/A");
    when(mockSnapshot.getVCoresUsed()).thenReturn(0);
    when(mockSnapshot.getVCoresTotal()).thenReturn(0);
    when(mockSnapshot.getVCoresPercentage()).thenReturn("N/A");
    when(mockSnapshot.getCurrentCapacityPercentage()).thenReturn(0.0f);
    when(mockSnapshot.getRunningApps()).thenReturn(0);
    when(mockSnapshot.getAllocatedContainers()).thenReturn(0);

    TezProgressMonitor monitor = new TezProgressMonitor(
        mockDagClient, mockDagStatus, new ArrayList<>(), new HashMap<>(), mockConsole,
        System.currentTimeMillis(), mockMetricsCollector);

    String[] lines = monitor.queueMetrics().split("\n");
    assertEquals("Should have 4 lines", 4, lines.length);
    assertTrue("Line 1 should contain queue name", lines[0].contains("QUEUE: empty"));
    assertFalse("Line 1 should NOT contain staleness", lines[0].contains("ago"));
    assertTrue("Line 2 should contain N/A for memory percentage", lines[1].contains("N/A"));
    assertTrue("Line 2 should handle zero values", lines[1].contains("0.0/0.0 GB"));
    assertTrue("Line 3 should contain capacity", lines[2].contains("CAPACITY:"));
    assertTrue("Line 4 should contain APPS:", lines[3].contains("APPS:"));
    assertTrue("Line 4 should contain CONTAINERS:", lines[3].contains("CONTAINERS:"));
  }

  @Test
  public void testQueueMetricsExceptionHandling() throws Exception {

    when(mockMetricsCollector.getLatestSnapshot()).thenThrow(new RuntimeException("Unexpected error"));

    TezProgressMonitor monitor = new TezProgressMonitor(
        mockDagClient, mockDagStatus, new ArrayList<>(), new HashMap<>(), mockConsole,
        System.currentTimeMillis(), mockMetricsCollector);

    assertEquals("Should return unavailable on exception",
        "QUEUE: unavailable", monitor.queueMetrics());
  }


  @Test
  public void testQueueNameExactlyAtMaxLength() throws Exception {

    int maxLen = InPlaceUpdate.MIN_TERMINAL_WIDTH - "QUEUE: ".length();
    String exactName = "q".repeat(maxLen);
    when(mockMetricsCollector.getQueueName()).thenReturn(exactName);

    TezProgressMonitor monitor = new TezProgressMonitor(
        mockDagClient, mockDagStatus, new ArrayList<>(), new HashMap<>(), mockConsole,
        System.currentTimeMillis(), mockMetricsCollector);

    String[] lines = monitor.queueMetrics().split("\n");
    assertEquals("Should have 4 lines", 4, lines.length);
    assertFalse("Queue name at exact max length should not be truncated", lines[0].contains("..."));
    assertTrue("Full queue name should appear", lines[0].contains(exactName));
    assertTrue("Line 1 should still be within terminal width",
        lines[0].length() <= InPlaceUpdate.MIN_TERMINAL_WIDTH);
    assertFalse("Should NOT show staleness", lines[0].contains("ago"));
  }
}

