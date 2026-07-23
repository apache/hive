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

package org.apache.hadoop.hive.ql.exec.tez.monitoring;

import org.apache.hadoop.hive.ql.exec.tez.monitoring.yarnqueue.QueueMetricsCollector;
import org.apache.hadoop.hive.ql.exec.tez.monitoring.yarnqueue.QueueMetricsSnapshot;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;
import org.apache.hadoop.hive.common.log.InPlaceUpdate;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Test cases for TezProgressMonitor queue metrics functionality.
 */
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
    MockitoAnnotations.openMocks(this);
    // Default: metrics are enabled (can be overridden in individual tests)
    when(mockMetricsCollector.isEnabled()).thenReturn(true);
  }

  @Test
  public void testQueueMetricsWithNullCollector() throws Exception {
    List<BaseWork> works = new ArrayList<>();
    Map<String, Progress> progressMap = new HashMap<>();

    when(mockDagStatus.getState()).thenReturn(DAGStatus.State.RUNNING);

    TezProgressMonitor monitor = new TezProgressMonitor(
        mockDagClient, mockDagStatus, works, progressMap, mockConsole,
        System.currentTimeMillis(), null);

    String result = monitor.queueMetrics();

    assertEquals("Should return empty string when collector is null", "", result);
  }

  @Test
  public void testQueueMetricsUnavailableWhenSnapshotNull() throws Exception {
    List<BaseWork> works = new ArrayList<>();
    Map<String, Progress> progressMap = new HashMap<>();

    when(mockDagStatus.getState()).thenReturn(DAGStatus.State.RUNNING);
    when(mockMetricsCollector.isEnabled()).thenReturn(true); // Enabled but snapshot unavailable
    when(mockMetricsCollector.getLatestSnapshot()).thenReturn(null);

    TezProgressMonitor monitor = new TezProgressMonitor(
        mockDagClient, mockDagStatus, works, progressMap, mockConsole,
        System.currentTimeMillis(), mockMetricsCollector);

    String result = monitor.queueMetrics();

    assertEquals("Should return 'unavailable' when enabled but snapshot is null",
        "QUEUE: unavailable", result);
  }

  @Test
  public void testQueueMetricsDisabled() throws Exception {
    List<BaseWork> works = new ArrayList<>();
    Map<String, Progress> progressMap = new HashMap<>();

    when(mockDagStatus.getState()).thenReturn(DAGStatus.State.RUNNING);
    when(mockMetricsCollector.isEnabled()).thenReturn(false); // Metrics disabled (0s interval)
    when(mockMetricsCollector.getLatestSnapshot()).thenReturn(null);

    TezProgressMonitor monitor = new TezProgressMonitor(
        mockDagClient, mockDagStatus, works, progressMap, mockConsole,
        System.currentTimeMillis(), mockMetricsCollector);

    String result = monitor.queueMetrics();

    assertEquals("Should return empty string when metrics are disabled",
        "", result);
  }

  @Test
  public void testQueueMetricsFormatting() throws Exception {
    List<BaseWork> works = new ArrayList<>();
    Map<String, Progress> progressMap = new HashMap<>();

    // Setup snapshot with known values
    long now = System.currentTimeMillis();
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
    when(mockSnapshot.getCollectionTimestamp()).thenReturn(now - 5000); // 5 seconds ago

    when(mockDagStatus.getState()).thenReturn(DAGStatus.State.RUNNING);
    when(mockMetricsCollector.getLatestSnapshot()).thenReturn(mockSnapshot);
    when(mockMetricsCollector.getQueueName()).thenReturn("default");

    TezProgressMonitor monitor = new TezProgressMonitor(
        mockDagClient, mockDagStatus, works, progressMap, mockConsole,
        System.currentTimeMillis(), mockMetricsCollector);

    String result = monitor.queueMetrics();

    // Verify 4-line format (no staleness)
    String[] lines = result.split("\n");
    assertEquals("Should have 4 lines", 4, lines.length);

    // Line 1: Queue name (no staleness)
    assertTrue("Line 1 should contain queue name", lines[0].contains("QUEUE: default"));
    assertFalse("Line 1 should NOT contain staleness", lines[0].contains("ago"));

    // Line 2: Memory + VCores with "used" label
    assertTrue("Line 2 should contain memory info", lines[1].contains("MEMORY: 8.5/16.0 GB"));
    assertTrue("Line 2 should contain 'used' label", lines[1].contains("53.12% used"));
    assertTrue("Line 2 should contain vCores info", lines[1].contains("VCORES: 100/200"));
    assertTrue("Line 2 should contain vCores 'used' label", lines[1].contains("50.00% used"));

    // Line 3: Capacity with (used) and (allocated) labels
    assertTrue("Line 3 should contain capacity used", lines[2].contains("CAPACITY: 25.00% (used)"));
    assertTrue("Line 3 should contain capacity allocated", lines[2].contains("60.00% (allocated)"));

    // Line 4: Apps and Containers
    assertTrue("Line 4 should contain running apps", lines[3].contains("APPS: 5 running"));
    assertTrue("Line 4 should contain pending apps", lines[3].contains("2 pending"));
    assertTrue("Line 4 should contain allocated containers", lines[3].contains("CONTAINERS: 12 allocated"));
    assertTrue("Line 4 should contain pending containers", lines[3].contains("10 pending"));

  }

  @Test
  public void testQueueMetricsStalenessBeyond60Seconds() throws Exception {
    List<BaseWork> works = new ArrayList<>();
    Map<String, Progress> progressMap = new HashMap<>();

    long now = System.currentTimeMillis();
    when(mockSnapshot.getMemoryUsedGB()).thenReturn(1.0f);
    when(mockSnapshot.getMemoryTotalGB()).thenReturn(10.0f);
    when(mockSnapshot.getMemoryPercentage()).thenReturn("10.00%");
    when(mockSnapshot.getVCoresUsed()).thenReturn(10);
    when(mockSnapshot.getVCoresTotal()).thenReturn(100);
    when(mockSnapshot.getVCoresPercentage()).thenReturn("10.00%");
    when(mockSnapshot.getCapacityPercentage()).thenReturn(50.0f);
    when(mockSnapshot.getCurrentCapacityPercentage()).thenReturn(10.0f);
    when(mockSnapshot.getRunningApps()).thenReturn(1);
    when(mockSnapshot.getPendingApps()).thenReturn(0);
    when(mockSnapshot.getAllocatedContainers()).thenReturn(2);
    when(mockSnapshot.getPendingContainers()).thenReturn(0);
    when(mockSnapshot.getCollectionTimestamp()).thenReturn(now - 120000); // 120 seconds ago

    when(mockDagStatus.getState()).thenReturn(DAGStatus.State.RUNNING);
    when(mockMetricsCollector.getLatestSnapshot()).thenReturn(mockSnapshot);
    when(mockMetricsCollector.getQueueName()).thenReturn("default");

    TezProgressMonitor monitor = new TezProgressMonitor(
        mockDagClient, mockDagStatus, works, progressMap, mockConsole,
        System.currentTimeMillis(), mockMetricsCollector);

    String result = monitor.queueMetrics();

    String[] lines = result.split("\n");
    assertEquals("Should have 4 lines", 4, lines.length);
    // Staleness is removed in new format, so line 1 should only have queue name
    assertFalse("Line 1 should NOT show staleness (removed from new format)", lines[0].contains("ago"));
    assertTrue("Line 1 should contain QUEUE: default", lines[0].contains("QUEUE: default"));
  }

  @Test
  public void testQueueNameTruncation() throws Exception {
    List<BaseWork> works = new ArrayList<>();
    Map<String, Progress> progressMap = new HashMap<>();

    long now = System.currentTimeMillis();
    when(mockSnapshot.getMemoryUsedGB()).thenReturn(1.0f);
    when(mockSnapshot.getMemoryTotalGB()).thenReturn(10.0f);
    when(mockSnapshot.getMemoryPercentage()).thenReturn("10.00%");
    when(mockSnapshot.getVCoresUsed()).thenReturn(10);
    when(mockSnapshot.getVCoresTotal()).thenReturn(100);
    when(mockSnapshot.getVCoresPercentage()).thenReturn("10.00%");
    when(mockSnapshot.getCapacityPercentage()).thenReturn(50.0f);
    when(mockSnapshot.getCurrentCapacityPercentage()).thenReturn(10.0f);
    when(mockSnapshot.getRunningApps()).thenReturn(1);
    when(mockSnapshot.getPendingApps()).thenReturn(0);
    when(mockSnapshot.getAllocatedContainers()).thenReturn(2);
    when(mockSnapshot.getPendingContainers()).thenReturn(0);
    when(mockSnapshot.getCollectionTimestamp()).thenReturn(now - 1000);

    when(mockDagStatus.getState()).thenReturn(DAGStatus.State.RUNNING);
    when(mockMetricsCollector.getLatestSnapshot()).thenReturn(mockSnapshot);
    // Very long queue name
    when(mockMetricsCollector.getQueueName()).thenReturn(
        "root.production.analytics.data-engineering.team-alpha.project-beta");

    TezProgressMonitor monitor = new TezProgressMonitor(
        mockDagClient, mockDagStatus, works, progressMap, mockConsole,
        System.currentTimeMillis(), mockMetricsCollector);

    String result = monitor.queueMetrics();

    String[] lines = result.split("\n");
    assertEquals("Should have 4 lines", 4, lines.length);

    // Line 1 should not exceed separator width (94 chars) - without staleness, more room for queue name
    assertTrue("Line 1 should not exceed 94 characters", lines[0].length() <= 94);

    // When the full queue name would cause line 1 overflow, it should be truncated from start with "..."
    if (lines[0].contains("...")) {
      // Queue name was truncated
      assertFalse("Full long queue name should not appear",
          lines[0].contains("root.production.analytics.data-engineering.team-alpha.project-beta"));
      assertTrue("Truncated queue name should contain ...", lines[0].contains("..."));
    }

    // Line 1 should NOT contain staleness (removed in new format)
    assertFalse("Line 1 should NOT contain staleness", lines[0].contains("ago"));

    // Line 2 should contain resource info
    assertTrue("Line 2 should contain MEMORY", lines[1].contains("MEMORY:"));

    // Line 3 should contain capacity
    assertTrue("Line 3 should contain CAPACITY", lines[2].contains("CAPACITY:"));

  }

  @Test
  public void testQueueMetricsWithZeroPercentages() throws Exception {
    List<BaseWork> works = new ArrayList<>();
    Map<String, Progress> progressMap = new HashMap<>();

    long now = System.currentTimeMillis();
    when(mockSnapshot.getMemoryUsedGB()).thenReturn(0.0f);
    when(mockSnapshot.getMemoryTotalGB()).thenReturn(0.0f);
    when(mockSnapshot.getMemoryPercentage()).thenReturn("N/A");
    when(mockSnapshot.getVCoresUsed()).thenReturn(0);
    when(mockSnapshot.getVCoresTotal()).thenReturn(0);
    when(mockSnapshot.getVCoresPercentage()).thenReturn("N/A");
    when(mockSnapshot.getCapacityPercentage()).thenReturn(50.0f);
    when(mockSnapshot.getCurrentCapacityPercentage()).thenReturn(0.0f);
    when(mockSnapshot.getRunningApps()).thenReturn(0);
    when(mockSnapshot.getPendingApps()).thenReturn(0);
    when(mockSnapshot.getAllocatedContainers()).thenReturn(0);
    when(mockSnapshot.getPendingContainers()).thenReturn(0);
    when(mockSnapshot.getCollectionTimestamp()).thenReturn(now);

    when(mockDagStatus.getState()).thenReturn(DAGStatus.State.RUNNING);
    when(mockMetricsCollector.getLatestSnapshot()).thenReturn(mockSnapshot);
    when(mockMetricsCollector.getQueueName()).thenReturn("empty");

    TezProgressMonitor monitor = new TezProgressMonitor(
        mockDagClient, mockDagStatus, works, progressMap, mockConsole,
        System.currentTimeMillis(), mockMetricsCollector);

    String result = monitor.queueMetrics();

    String[] lines = result.split("\n");
    assertEquals("Should have 4 lines", 4, lines.length);
    // Line 1: queue name (no staleness)
    assertTrue("Line 1 should contain queue name", lines[0].contains("QUEUE: empty"));
    assertFalse("Line 1 should NOT contain staleness", lines[0].contains("ago"));
    // Line 2: memory + vcores with N/A
    assertTrue("Line 2 should contain N/A for memory percentage", lines[1].contains("N/A"));
    assertTrue("Line 2 should handle zero values", lines[1].contains("0.0/0.0 GB"));
    // Line 3: capacity
    assertTrue("Line 3 should contain capacity", lines[2].contains("CAPACITY:"));
    // Line 4: apps and containers
    assertTrue("Line 4 should contain APPS:", lines[3].contains("APPS:"));
    assertTrue("Line 4 should contain CONTAINERS:", lines[3].contains("CONTAINERS:"));
  }

  @Test
  public void testQueueMetricsExceptionHandling() throws Exception {
    List<BaseWork> works = new ArrayList<>();
    Map<String, Progress> progressMap = new HashMap<>();

    when(mockDagStatus.getState()).thenReturn(DAGStatus.State.RUNNING);
    when(mockMetricsCollector.getLatestSnapshot()).thenThrow(
        new RuntimeException("Unexpected error"));

    TezProgressMonitor monitor = new TezProgressMonitor(
        mockDagClient, mockDagStatus, works, progressMap, mockConsole,
        System.currentTimeMillis(), mockMetricsCollector);

    String result = monitor.queueMetrics();

    // Should gracefully handle exceptions and return unavailable
    assertEquals("Should return unavailable on exception",
        "QUEUE: unavailable", result);
  }

  @Test
  public void testStalenessAtExactly60Seconds() throws Exception {
    List<BaseWork> works = new ArrayList<>();
    Map<String, Progress> progressMap = new HashMap<>();

    long now = System.currentTimeMillis();
    when(mockSnapshot.getMemoryUsedGB()).thenReturn(1.0f);
    when(mockSnapshot.getMemoryTotalGB()).thenReturn(10.0f);
    when(mockSnapshot.getMemoryPercentage()).thenReturn("10.00%");
    when(mockSnapshot.getVCoresUsed()).thenReturn(10);
    when(mockSnapshot.getVCoresTotal()).thenReturn(100);
    when(mockSnapshot.getVCoresPercentage()).thenReturn("10.00%");
    when(mockSnapshot.getCapacityPercentage()).thenReturn(50.0f);
    when(mockSnapshot.getCurrentCapacityPercentage()).thenReturn(25.0f);
    when(mockSnapshot.getRunningApps()).thenReturn(1);
    when(mockSnapshot.getPendingApps()).thenReturn(0);
    when(mockSnapshot.getAllocatedContainers()).thenReturn(2);
    when(mockSnapshot.getPendingContainers()).thenReturn(0);
    when(mockSnapshot.getCollectionTimestamp()).thenReturn(now - 60000L); // exactly 60s ago

    when(mockDagStatus.getState()).thenReturn(DAGStatus.State.RUNNING);
    when(mockMetricsCollector.getLatestSnapshot()).thenReturn(mockSnapshot);
    when(mockMetricsCollector.getQueueName()).thenReturn("default");

    TezProgressMonitor monitor = new TezProgressMonitor(
        mockDagClient, mockDagStatus, works, progressMap, mockConsole,
        System.currentTimeMillis(), mockMetricsCollector);

    String result = monitor.queueMetrics();
    String[] lines = result.split("\n");

    assertEquals("Should have 4 lines", 4, lines.length);
    // Staleness is removed in new format
    assertFalse("Should NOT show staleness (removed in new format)", lines[0].contains("ago"));
    assertTrue("Line 1 should contain queue name", lines[0].contains("QUEUE: default"));
  }

  @Test
  public void testStalenessAtZeroSeconds() throws Exception {
    List<BaseWork> works = new ArrayList<>();
    Map<String, Progress> progressMap = new HashMap<>();

    long now = System.currentTimeMillis();
    when(mockSnapshot.getMemoryUsedGB()).thenReturn(2.0f);
    when(mockSnapshot.getMemoryTotalGB()).thenReturn(10.0f);
    when(mockSnapshot.getMemoryPercentage()).thenReturn("20.00%");
    when(mockSnapshot.getVCoresUsed()).thenReturn(5);
    when(mockSnapshot.getVCoresTotal()).thenReturn(50);
    when(mockSnapshot.getVCoresPercentage()).thenReturn("10.00%");
    when(mockSnapshot.getCapacityPercentage()).thenReturn(60.0f);
    when(mockSnapshot.getCurrentCapacityPercentage()).thenReturn(30.0f);
    when(mockSnapshot.getRunningApps()).thenReturn(2);
    when(mockSnapshot.getPendingApps()).thenReturn(0);
    when(mockSnapshot.getAllocatedContainers()).thenReturn(4);
    when(mockSnapshot.getPendingContainers()).thenReturn(0);
    when(mockSnapshot.getCollectionTimestamp()).thenReturn(now); // right now

    when(mockDagStatus.getState()).thenReturn(DAGStatus.State.RUNNING);
    when(mockMetricsCollector.getLatestSnapshot()).thenReturn(mockSnapshot);
    when(mockMetricsCollector.getQueueName()).thenReturn("default");

    TezProgressMonitor monitor = new TezProgressMonitor(
        mockDagClient, mockDagStatus, works, progressMap, mockConsole,
        System.currentTimeMillis(), mockMetricsCollector);

    String result = monitor.queueMetrics();
    String[] lines = result.split("\n");

    assertEquals("Should have 4 lines", 4, lines.length);
    // Staleness is removed in new format
    assertFalse("Should NOT show staleness (removed in new format)", lines[0].contains("ago"));
    assertTrue("Line 1 should contain queue name", lines[0].contains("QUEUE: default"));
  }

  @Test
  public void testQueueNameExactlyAtMaxLength() throws Exception {
    List<BaseWork> works = new ArrayList<>();
    Map<String, Progress> progressMap = new HashMap<>();

    long now = System.currentTimeMillis();
    when(mockSnapshot.getMemoryUsedGB()).thenReturn(1.0f);
    when(mockSnapshot.getMemoryTotalGB()).thenReturn(10.0f);
    when(mockSnapshot.getMemoryPercentage()).thenReturn("10.00%");
    when(mockSnapshot.getVCoresUsed()).thenReturn(1);
    when(mockSnapshot.getVCoresTotal()).thenReturn(10);
    when(mockSnapshot.getVCoresPercentage()).thenReturn("10.00%");
    when(mockSnapshot.getCapacityPercentage()).thenReturn(40.0f);
    when(mockSnapshot.getCurrentCapacityPercentage()).thenReturn(20.0f);
    when(mockSnapshot.getRunningApps()).thenReturn(0);
    when(mockSnapshot.getPendingApps()).thenReturn(0);
    when(mockSnapshot.getAllocatedContainers()).thenReturn(0);
    when(mockSnapshot.getPendingContainers()).thenReturn(0);
    when(mockSnapshot.getCollectionTimestamp()).thenReturn(now - 2000); // not used since staleness removed

    when(mockDagStatus.getState()).thenReturn(DAGStatus.State.RUNNING);
    when(mockMetricsCollector.getLatestSnapshot()).thenReturn(mockSnapshot);

    // Build a queue name that exactly fills the allowed space:
    // Line 1 budget: MIN_TERMINAL_WIDTH (94) - "QUEUE: ".length(7) = 87 (no staleness)
    int maxLen = InPlaceUpdate.MIN_TERMINAL_WIDTH - "QUEUE: ".length();
    String exactName = "q".repeat(maxLen); // exactly maxLen characters
    when(mockMetricsCollector.getQueueName()).thenReturn(exactName);

    TezProgressMonitor monitor = new TezProgressMonitor(
        mockDagClient, mockDagStatus, works, progressMap, mockConsole,
        System.currentTimeMillis(), mockMetricsCollector);

    String result = monitor.queueMetrics();
    String[] lines = result.split("\n");

    assertEquals("Should have 4 lines", 4, lines.length);
    // Exactly at max length — should NOT be truncated
    assertFalse("Queue name at exact max length should not be truncated",
        lines[0].contains("..."));
    assertTrue("Full queue name should appear", lines[0].contains(exactName));
    assertTrue("Line 1 should still be within terminal width",
        lines[0].length() <= InPlaceUpdate.MIN_TERMINAL_WIDTH);
    // No staleness in new format
    assertFalse("Should NOT show staleness", lines[0].contains("ago"));
  }
}

