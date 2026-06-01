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

import org.apache.hadoop.hive.ql.exec.tez.YarnQueueMetricsCollector;
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
  private YarnQueueMetricsCollector mockMetricsCollector;

  @Mock
  private YarnQueueMetricsCollector.QueueMetricsSnapshot mockSnapshot;

  @Mock
  private SessionState.LogHelper mockConsole;


  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
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
  public void testQueueMetricsWithNullSnapshot() throws Exception {
    List<BaseWork> works = new ArrayList<>();
    Map<String, Progress> progressMap = new HashMap<>();

    when(mockDagStatus.getState()).thenReturn(DAGStatus.State.RUNNING);
    when(mockMetricsCollector.getLatestSnapshot()).thenReturn(null);

    TezProgressMonitor monitor = new TezProgressMonitor(
        mockDagClient, mockDagStatus, works, progressMap, mockConsole,
        System.currentTimeMillis(), mockMetricsCollector);

    String result = monitor.queueMetrics();

    assertEquals("Should return 'unavailable' when snapshot is null",
        "QUEUE: unavailable", result);
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
    when(mockSnapshot.getCapacityPercentage()).thenReturn(25.0f);
    when(mockSnapshot.getRunningApps()).thenReturn(5);
    when(mockSnapshot.getPendingContainers()).thenReturn(10);
    when(mockSnapshot.getCollectionTimestamp()).thenReturn(now - 5000); // 5 seconds ago

    when(mockDagStatus.getState()).thenReturn(DAGStatus.State.RUNNING);
    when(mockMetricsCollector.getLatestSnapshot()).thenReturn(mockSnapshot);
    when(mockMetricsCollector.getQueueName()).thenReturn("default");

    TezProgressMonitor monitor = new TezProgressMonitor(
        mockDagClient, mockDagStatus, works, progressMap, mockConsole,
        System.currentTimeMillis(), mockMetricsCollector);

    String result = monitor.queueMetrics();

    // Verify 3-line format
    String[] lines = result.split("\n");
    assertEquals("Should have 3 lines", 3, lines.length);

    // Line 1: Queue name + staleness
    assertTrue("Line 1 should contain queue name", lines[0].contains("QUEUE: default"));
    assertTrue("Line 1 should contain staleness", lines[0].contains("s ago"));

    // Line 2: Memory + VCores
    assertTrue("Line 2 should contain memory info", lines[1].contains("MEMORY: 8.5/16.0 GB"));
    assertTrue("Line 2 should contain memory percentage", lines[1].contains("53.12%"));
    assertTrue("Line 2 should contain vCores info", lines[1].contains("VCORES: 100/200"));
    assertTrue("Line 2 should contain vCores percentage", lines[1].contains("50.00%"));

    // Line 3: Capacity + Apps + Pending
    assertTrue("Line 3 should contain capacity", lines[2].contains("CAPACITY: 25.00%"));
    assertTrue("Line 3 should contain running apps", lines[2].contains("ACTIVE_APPS: 5"));
    assertTrue("Line 3 should contain pending containers", lines[2].contains("PENDING: 10"));

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
    when(mockSnapshot.getCapacityPercentage()).thenReturn(10.0f);
    when(mockSnapshot.getRunningApps()).thenReturn(1);
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
    assertEquals("Should have 3 lines", 3, lines.length);
    // Staleness now on line 1 with queue name
    assertTrue("Line 1 should show >60s ago for stale metrics", lines[0].contains(">60s ago"));
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
    when(mockSnapshot.getCapacityPercentage()).thenReturn(10.0f);
    when(mockSnapshot.getRunningApps()).thenReturn(1);
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
    assertEquals("Should have 3 lines", 3, lines.length);

    // Line 1 should not exceed separator width (94 chars)
    assertTrue("Line 1 should not exceed 94 characters", lines[0].length() <= 94);

    // When the full queue name would cause line 1 overflow, it should be truncated from start with "..."
    if (lines[0].contains("...")) {
      // Queue name was truncated
      assertFalse("Full long queue name should not appear",
          lines[0].contains("root.production.analytics.data-engineering.team-alpha.project-beta"));
      assertTrue("Truncated queue name should contain ...", lines[0].contains("..."));
    }

    // Line 1 should always contain staleness
    assertTrue("Line 1 should contain staleness", lines[0].contains("s ago"));

    // Line 2 should contain resource info
    assertTrue("Line 2 should contain MEMORY", lines[2 - 1].contains("MEMORY:"));

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
    when(mockSnapshot.getCapacityPercentage()).thenReturn(0.0f);
    when(mockSnapshot.getRunningApps()).thenReturn(0);
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
    assertEquals("Should have 3 lines", 3, lines.length);
    // Line 1: queue name + staleness
    assertTrue("Line 1 should contain queue name", lines[0].contains("QUEUE: empty"));
    // Line 2: memory + vcores with N/A
    assertTrue("Line 2 should contain N/A for memory percentage", lines[1].contains("N/A"));
    assertTrue("Line 2 should handle zero values", lines[1].contains("0.0/0.0 GB"));
    // Line 3: capacity + apps + pending
    assertTrue("Line 3 should contain capacity", lines[2].contains("CAPACITY:"));
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
    when(mockSnapshot.getRunningApps()).thenReturn(1);
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

    // Exactly 60 seconds is > 60 in integer division (60000/1000 = 60, NOT > 60)
    // so staleness string should be "60s ago", NOT ">60s ago"
    assertTrue("60s ago should NOT show >60s ago marker", lines[0].contains("60s ago"));
    assertFalse("60s ago should not be labelled >60s ago", lines[0].contains(">60s ago"));
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
    when(mockSnapshot.getRunningApps()).thenReturn(2);
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

    assertTrue("Fresh snapshot should show 0s ago", lines[0].contains("0s ago"));
    assertFalse("Fresh snapshot should not show >60s ago", lines[0].contains(">60s ago"));
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
    when(mockSnapshot.getRunningApps()).thenReturn(0);
    when(mockSnapshot.getPendingContainers()).thenReturn(0);
    when(mockSnapshot.getCollectionTimestamp()).thenReturn(now - 2000); // "2s ago"

    when(mockDagStatus.getState()).thenReturn(DAGStatus.State.RUNNING);
    when(mockMetricsCollector.getLatestSnapshot()).thenReturn(mockSnapshot);

    // Build a queue name that exactly fills the allowed space:
    // Line 1 budget: MIN_TERMINAL_WIDTH (94) - "QUEUE: ".length(7) - " (2s ago)".length(9) = 78
    String stalenessAppend = " (2s ago)";
    int maxLen = InPlaceUpdate.MIN_TERMINAL_WIDTH - "QUEUE: ".length() - stalenessAppend.length();
    String exactName = "q".repeat(maxLen); // exactly maxLen characters
    when(mockMetricsCollector.getQueueName()).thenReturn(exactName);

    TezProgressMonitor monitor = new TezProgressMonitor(
        mockDagClient, mockDagStatus, works, progressMap, mockConsole,
        System.currentTimeMillis(), mockMetricsCollector);

    String result = monitor.queueMetrics();
    String[] lines = result.split("\n");

    // Exactly at max length — should NOT be truncated
    assertFalse("Queue name at exact max length should not be truncated",
        lines[0].contains("..."));
    assertTrue("Full queue name should appear", lines[0].contains(exactName));
    assertTrue("Line 1 should still be within terminal width",
        lines[0].length() <= InPlaceUpdate.MIN_TERMINAL_WIDTH);
  }
}

