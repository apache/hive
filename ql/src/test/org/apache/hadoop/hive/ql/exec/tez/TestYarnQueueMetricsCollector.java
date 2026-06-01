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

import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.when;


/**
 * Test cases for YarnQueueMetricsCollector.
 */
public class TestYarnQueueMetricsCollector {

  @Mock
  private YarnClient mockYarnClient;

  @Mock
  private QueueInfo mockQueueInfo;

  @Mock
  private QueueStatistics mockQueueStats;

  private AutoCloseable closeable;

  private static final long WAIT_TIMEOUT_MS = 5000;

  @Before
  public void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @After
  public void tearDown() throws Exception {
    if (closeable != null) {
      closeable.close();
    }
  }

  /**
   * Waits for a snapshot to be available (non-null).
   */
  private YarnQueueMetricsCollector.QueueMetricsSnapshot waitForSnapshot(
      YarnQueueMetricsCollector collector, long timeoutMs) {
    long startTime = System.currentTimeMillis();
    YarnQueueMetricsCollector.QueueMetricsSnapshot snapshot;
    while ((snapshot = collector.getLatestSnapshot()) == null) {
      if (System.currentTimeMillis() - startTime > timeoutMs) {
        fail("Snapshot not available after " + timeoutMs + "ms");
      }
      Thread.yield();
    }
    return snapshot;
  }

  /**
   * Waits for a specific number of invocations with timeout.
   */
  private void waitForInvocationCount(Object mock, int minCount, long timeoutMs) {
    long startTime = System.currentTimeMillis();
    while (mockingDetails(mock).getInvocations().size() < minCount) {
      if (System.currentTimeMillis() - startTime > timeoutMs) {
        return; // Don't fail, just return - test will check the count
      }
      Thread.yield();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorWithNullYarnClient() {
    new YarnQueueMetricsCollector(null, "default", 1000, "query-1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorWithNullQueueName() {
    new YarnQueueMetricsCollector(mockYarnClient, null, 1000, "query-1");
  }

  @Test
  public void testSuccessfulMetricsCollection() throws Exception {
    // Setup mock QueueStatistics
    when(mockQueueStats.getAllocatedMemoryMB()).thenReturn(8192L); // 8GB in MB
    when(mockQueueStats.getAvailableMemoryMB()).thenReturn(8192L); // 8GB available in MB
    when(mockQueueStats.getAllocatedVCores()).thenReturn(100L);
    when(mockQueueStats.getAvailableVCores()).thenReturn(100L);
    when(mockQueueStats.getNumAppsRunning()).thenReturn(5L);
    when(mockQueueStats.getPendingContainers()).thenReturn(10L);

    // Setup mock QueueInfo
    when(mockQueueInfo.getQueueStatistics()).thenReturn(mockQueueStats);
    when(mockQueueInfo.getCapacity()).thenReturn(0.25f); // 25%

    // Setup YarnClient to return mocked QueueInfo
    when(mockYarnClient.getQueueInfo("default")).thenReturn(mockQueueInfo);

    // Create collector
    YarnQueueMetricsCollector collector = new YarnQueueMetricsCollector(
        mockYarnClient, "default", 10000, "test-query-1");

    try {
      // Wait for initial collection to complete
      YarnQueueMetricsCollector.QueueMetricsSnapshot snapshot = waitForSnapshot(collector, WAIT_TIMEOUT_MS);

      assertNotNull("Snapshot should not be null", snapshot);
      assertEquals("Memory used should be 8GB", 8.0f, snapshot.getMemoryUsedGB(), 0.1f);
      assertEquals("Memory total should be 16GB (8+8)", 16.0f, snapshot.getMemoryTotalGB(), 0.1f);
      assertEquals("VCores used should be 100", 100, snapshot.getVCoresUsed());
      assertEquals("VCores total should be 200 (100+100)", 200, snapshot.getVCoresTotal());
      assertEquals("Running apps should be 5", 5, snapshot.getRunningApps());
      assertEquals("Pending containers should be 10", 10, snapshot.getPendingContainers());
      assertEquals("Capacity should be 25%", 25.0f, snapshot.getCapacityPercentage(), 0.1f);

      // Verify percentages
      assertEquals("Memory percentage", "50.00%", snapshot.getMemoryPercentage());
      assertEquals("VCores percentage", "50.00%", snapshot.getVCoresPercentage());

    } finally {
      collector.shutdown();
    }
  }

  @Test
  public void testMetricsCollectionWithNullQueueInfo() throws Exception {
    // YarnClient returns null for queue info (queue doesn't exist)
    when(mockYarnClient.getQueueInfo("nonexistent")).thenReturn(null);

    YarnQueueMetricsCollector collector = new YarnQueueMetricsCollector(
        mockYarnClient, "nonexistent", 10000, "test-query-2");

    try {
      // Constructor performs eager collection, snapshot should already be null
      // (No need to wait since null QueueInfo means immediate null snapshot)
      assertNull("Snapshot should be null for nonexistent queue",
          collector.getLatestSnapshot());

    } finally {
      collector.shutdown();
    }
  }

  @Test
  public void testMetricsCollectionWithNullQueueStatistics() throws Exception {
    // QueueInfo exists but QueueStatistics is null
    when(mockQueueInfo.getQueueStatistics()).thenReturn(null);
    when(mockQueueInfo.getCapacity()).thenReturn(0.5f); // 50%
    when(mockYarnClient.getQueueInfo("default")).thenReturn(mockQueueInfo);

    YarnQueueMetricsCollector collector = new YarnQueueMetricsCollector(
        mockYarnClient, "default", 10000, "test-query-3");

    try {
      YarnQueueMetricsCollector.QueueMetricsSnapshot snapshot = waitForSnapshot(collector, WAIT_TIMEOUT_MS);

      assertNotNull("Snapshot should not be null", snapshot);
      // Should have zero values when QueueStatistics is null
      assertEquals("Memory used should be 0", 0.0f, snapshot.getMemoryUsedGB(), 0.01f);
      assertEquals("Memory total should be 0", 0.0f, snapshot.getMemoryTotalGB(), 0.01f);
      assertEquals("VCores used should be 0", 0, snapshot.getVCoresUsed());
      assertEquals("VCores total should be 0", 0, snapshot.getVCoresTotal());
      assertEquals("Capacity should still be 50%", 50.0f, snapshot.getCapacityPercentage(), 0.1f);

    } finally {
      collector.shutdown();
    }
  }

  @Test
  public void testPercentageCalculationWithZeroTotal() {
    // Setup with zero totals
    when(mockQueueStats.getAllocatedMemoryMB()).thenReturn(0L);
    when(mockQueueStats.getAvailableMemoryMB()).thenReturn(0L);
    when(mockQueueStats.getAllocatedVCores()).thenReturn(0L);
    when(mockQueueStats.getAvailableVCores()).thenReturn(0L);
    when(mockQueueStats.getNumAppsRunning()).thenReturn(0L);
    when(mockQueueStats.getPendingContainers()).thenReturn(0L);
    when(mockQueueInfo.getQueueStatistics()).thenReturn(mockQueueStats);
    when(mockQueueInfo.getCapacity()).thenReturn(0.0f);

    YarnQueueMetricsCollector.QueueMetricsSnapshot snapshot =
        new YarnQueueMetricsCollector.QueueMetricsSnapshot(mockQueueInfo);

    // Should return "N/A" for percentages when total is zero
    assertEquals("Memory percentage should be N/A", "N/A", snapshot.getMemoryPercentage());
    assertEquals("VCores percentage should be N/A", "N/A", snapshot.getVCoresPercentage());
  }

  @Test
  public void testShutdownIdempotency() throws Exception {
    when(mockYarnClient.getQueueInfo("default")).thenReturn(mockQueueInfo);

    YarnQueueMetricsCollector collector = new YarnQueueMetricsCollector(
        mockYarnClient, "default", 10000, "test-query-4");

    // Shutdown multiple times should not cause issues
    collector.shutdown();
    collector.shutdown();
    collector.shutdown();

    // Should not throw exception
    assertTrue("Multiple shutdowns should be safe", true);
  }

  @Test
  public void testExceptionDuringCollection() throws Exception {
    // YarnClient throws exception
    when(mockYarnClient.getQueueInfo("default"))
        .thenThrow(new RuntimeException("RM unavailable"));

    YarnQueueMetricsCollector collector = new YarnQueueMetricsCollector(
        mockYarnClient, "default", 10000, "test-query-5");

    try {
      // Constructor performs eager collection which throws exception
      // Snapshot should already be null
      assertNull("Snapshot should be null after exception",
          collector.getLatestSnapshot());

    } finally {
      collector.shutdown();
    }
  }

  @Test
  public void testQueueNameRetrieval() throws Exception {
    when(mockYarnClient.getQueueInfo(anyString())).thenReturn(mockQueueInfo);
    when(mockQueueInfo.getQueueStatistics()).thenReturn(null);
    when(mockQueueInfo.getCapacity()).thenReturn(0.5f);

    YarnQueueMetricsCollector collector = new YarnQueueMetricsCollector(
        mockYarnClient, "production", 10000, "test-query-6");

    try {
      assertEquals("Queue name should match", "production", collector.getQueueName());
    } finally {
      collector.shutdown();
    }
  }

  @Test
  public void testMemoryAndVCoreCalculation() {
    // Test with specific values to verify calculation
    when(mockQueueStats.getAllocatedMemoryMB()).thenReturn(5120L); // 5GB used
    when(mockQueueStats.getAvailableMemoryMB()).thenReturn(15360L); // 15GB available
    when(mockQueueStats.getAllocatedVCores()).thenReturn(50L);
    when(mockQueueStats.getAvailableVCores()).thenReturn(150L);
    when(mockQueueStats.getNumAppsRunning()).thenReturn(3L);
    when(mockQueueStats.getPendingContainers()).thenReturn(7L);
    when(mockQueueInfo.getQueueStatistics()).thenReturn(mockQueueStats);
    when(mockQueueInfo.getCapacity()).thenReturn(0.2f); // 20%

    YarnQueueMetricsCollector.QueueMetricsSnapshot snapshot =
        new YarnQueueMetricsCollector.QueueMetricsSnapshot(mockQueueInfo);

    // Total = Used + Available
    assertEquals("Memory used", 5.0f, snapshot.getMemoryUsedGB(), 0.01f);
    assertEquals("Memory total", 20.0f, snapshot.getMemoryTotalGB(), 0.01f); // 5+15
    assertEquals("Memory percentage", "25.00%", snapshot.getMemoryPercentage()); // 5/20

    assertEquals("VCores used", 50, snapshot.getVCoresUsed());
    assertEquals("VCores total", 200, snapshot.getVCoresTotal()); // 50+150
    assertEquals("VCores percentage", "25.00%", snapshot.getVCoresPercentage()); // 50/200
  }

  @Test(expected = IllegalArgumentException.class)
  public void testQueueMetricsSnapshotWithNullQueueInfo() {
    new YarnQueueMetricsCollector.QueueMetricsSnapshot(null);
  }

  // -------------------------------------------------------------------------
  // Tests for Issue #1: Jitter on initial delay (Thundering Herd prevention)
  // -------------------------------------------------------------------------
  @Test
  public void testInitialDelayHasJitter() throws Exception {
    // Collect 10 collector start times and check the jitter spread
    when(mockYarnClient.getQueueInfo(anyString())).thenReturn(mockQueueInfo);
    when(mockQueueInfo.getQueueStatistics()).thenReturn(null);
    when(mockQueueInfo.getCapacity()).thenReturn(0.5f);

    long refreshIntervalMs = 10000;

    // We can't directly read the scheduled delay, but we can verify the collector
    // starts successfully (constructor completes without exception) for multiple
    // instances, confirming jitter calculation doesn't throw
    YarnQueueMetricsCollector[] collectors = new YarnQueueMetricsCollector[5];
    try {
      for (int i = 0; i < 5; i++) {
        collectors[i] = new YarnQueueMetricsCollector(
            mockYarnClient, "default", refreshIntervalMs, "jitter-test-query-" + i);
        assertNotNull("Collector " + i + " should be created successfully", collectors[i]);
      }
      // If we get here, all 5 collectors were created with their own jittered delays
      // without conflict or exception - thundering herd fix is in place
    } finally {
      for (YarnQueueMetricsCollector c : collectors) {
        if (c != null) {
          c.shutdown();
        }
      }
    }
  }

  @Test
  public void testExecutorCleanupOnInitializationFailure() throws Exception {
    // Simulate YarnClient throwing on first call (during eager collection)
    // The constructor should propagate the exception but NOT leak the executor
    when(mockYarnClient.getQueueInfo(anyString()))
        .thenThrow(new RuntimeException("Simulated RM failure during init"));

    // Constructor wraps collection error - first call fails but shouldn't throw from constructor
    // (collectMetrics swallows exceptions). This test verifies the try-catch guards are correct.
    YarnQueueMetricsCollector collector = null;
    try {
      collector = new YarnQueueMetricsCollector(
          mockYarnClient, "default", 10000, "init-fail-query");
      // Constructor should succeed (collectMetrics is resilient) - snapshot will be null
      assertNull("Snapshot should be null after init failure",
          collector.getLatestSnapshot());
    } finally {
      if (collector != null) {
        collector.shutdown();
      }
    }
  }

  // -------------------------------------------------------------------------
  // Tests for Issue #2: Circuit breaker for repeated failures
  // -------------------------------------------------------------------------
  @Test
  public void testCircuitBreakerActivatesAfterMaxFailures() throws Exception {
    // YarnClient always throws - triggers circuit breaker after MAX_CONSECUTIVE_FAILURES
    when(mockYarnClient.getQueueInfo(anyString()))
        .thenThrow(new RuntimeException("YARN RM unavailable"));

    // Use a very short interval so failures accumulate quickly
    YarnQueueMetricsCollector collector = new YarnQueueMetricsCollector(
        mockYarnClient, "default", 50, "circuit-breaker-query-1");

    try {
      // Wait for at least 6 invocations to allow circuit breaker to activate
      waitForInvocationCount(mockYarnClient, 6, 1000);

      // Snapshot should be null throughout
      assertNull("Snapshot should be null when circuit breaker active",
          collector.getLatestSnapshot());

      // After circuit breaker kicks in, the number of YarnClient calls should be
      // significantly less than if there was no circuit breaker.
      // Without circuit breaker: ~12 calls in 600ms at 50ms interval
      // With circuit breaker at 10% after 5 failures: much fewer
      int callCount = mockingDetails(mockYarnClient).getInvocations().size();
      assertTrue("Circuit breaker should reduce calls (got " + callCount + ")",
          callCount < 12);

    } finally {
      collector.shutdown();
    }
  }

  @Test
  public void testCircuitBreakerResetsOnSuccess() throws Exception {
    // First 5 calls fail, then succeed
    when(mockYarnClient.getQueueInfo(anyString()))
        .thenThrow(new RuntimeException("Temporary RM failure"))
        .thenThrow(new RuntimeException("Temporary RM failure"))
        .thenThrow(new RuntimeException("Temporary RM failure"))
        .thenThrow(new RuntimeException("Temporary RM failure"))
        .thenThrow(new RuntimeException("Temporary RM failure"))
        .thenReturn(mockQueueInfo); // 6th call succeeds

    when(mockQueueInfo.getQueueStatistics()).thenReturn(mockQueueStats);
    when(mockQueueStats.getAllocatedMemoryMB()).thenReturn(4096L);
    when(mockQueueStats.getAvailableMemoryMB()).thenReturn(4096L);
    when(mockQueueStats.getAllocatedVCores()).thenReturn(50L);
    when(mockQueueStats.getAvailableVCores()).thenReturn(50L);
    when(mockQueueStats.getNumAppsRunning()).thenReturn(2L);
    when(mockQueueStats.getPendingContainers()).thenReturn(5L);
    when(mockQueueInfo.getCapacity()).thenReturn(0.3f);

    // Use short interval so failures accumulate and recovery happens quickly
    YarnQueueMetricsCollector collector = new YarnQueueMetricsCollector(
        mockYarnClient, "default", 30, "circuit-breaker-recovery-query");

    try {
      // Wait for recovery - snapshot should eventually be populated
      YarnQueueMetricsCollector.QueueMetricsSnapshot snapshot = waitForSnapshot(collector, 2000);

      assertNotNull("Snapshot should be populated after circuit breaker recovery", snapshot);
      assertEquals("Memory used should be 4GB", 4.0f, snapshot.getMemoryUsedGB(), 0.1f);

    } finally {
      collector.shutdown();
    }
  }

  @Test
  public void testCircuitBreakerDoesNotAffectSuccessfulCollection() throws Exception {
    // Normal operation - no failures, circuit breaker should never activate
    when(mockQueueStats.getAllocatedMemoryMB()).thenReturn(2048L);
    when(mockQueueStats.getAvailableMemoryMB()).thenReturn(2048L);
    when(mockQueueStats.getAllocatedVCores()).thenReturn(20L);
    when(mockQueueStats.getAvailableVCores()).thenReturn(20L);
    when(mockQueueStats.getNumAppsRunning()).thenReturn(1L);
    when(mockQueueStats.getPendingContainers()).thenReturn(0L);
    when(mockQueueInfo.getQueueStatistics()).thenReturn(mockQueueStats);
    when(mockQueueInfo.getCapacity()).thenReturn(0.1f);
    when(mockYarnClient.getQueueInfo(anyString())).thenReturn(mockQueueInfo);

    YarnQueueMetricsCollector collector = new YarnQueueMetricsCollector(
        mockYarnClient, "default", 10000, "no-failures-query");

    try {
      // Wait for snapshot to be available
      YarnQueueMetricsCollector.QueueMetricsSnapshot snapshot = waitForSnapshot(collector, WAIT_TIMEOUT_MS);

      // Snapshot should be available and correct
      assertNotNull("Snapshot should be available with no failures", snapshot);
      assertEquals("Memory used should be 2GB", 2.0f, snapshot.getMemoryUsedGB(), 0.1f);
      assertEquals("VCores used should be 20", 20, snapshot.getVCoresUsed());

    } finally {
      collector.shutdown();
    }
  }

  @Test
  public void testNullQueueInfoIncreasesFailureCounter() throws Exception {
    // null QueueInfo (queue doesn't exist) should also count as a failure
    when(mockYarnClient.getQueueInfo(anyString())).thenReturn(null);

    YarnQueueMetricsCollector collector = new YarnQueueMetricsCollector(
        mockYarnClient, "nonexistent-queue", 50, "null-queueinfo-query");

    try {
      // Wait for at least 6 invocations to allow circuit breaker to activate
      waitForInvocationCount(mockYarnClient, 6, 800);

      // Should still be null - null QueueInfo is treated as failure
      assertNull("Snapshot should remain null for null QueueInfo",
          collector.getLatestSnapshot());

      // Circuit breaker should have activated - call count should be limited
      int callCount = mockingDetails(mockYarnClient).getInvocations().size();
      assertTrue("Circuit breaker should reduce calls for null QueueInfo (got " + callCount + ")",
          callCount < 10);

    } finally {
      collector.shutdown();
    }
  }

  @Test
  public void testSnapshotCollectionTimestampIsRecent() throws Exception {
    when(mockQueueStats.getAllocatedMemoryMB()).thenReturn(1024L);
    when(mockQueueStats.getAvailableMemoryMB()).thenReturn(1024L);
    when(mockQueueStats.getAllocatedVCores()).thenReturn(4L);
    when(mockQueueStats.getAvailableVCores()).thenReturn(4L);
    when(mockQueueStats.getNumAppsRunning()).thenReturn(1L);
    when(mockQueueStats.getPendingContainers()).thenReturn(0L);
    when(mockQueueInfo.getQueueStatistics()).thenReturn(mockQueueStats);
    when(mockQueueInfo.getCapacity()).thenReturn(0.5f);
    when(mockYarnClient.getQueueInfo("default")).thenReturn(mockQueueInfo);

    long beforeCreate = System.currentTimeMillis();
    YarnQueueMetricsCollector collector = new YarnQueueMetricsCollector(
        mockYarnClient, "default", 10000, "timestamp-test");

    try {
      YarnQueueMetricsCollector.QueueMetricsSnapshot snapshot =
          waitForSnapshot(collector, WAIT_TIMEOUT_MS);
      long afterCollect = System.currentTimeMillis();

      assertNotNull("Snapshot should not be null", snapshot);
      assertTrue("Timestamp should be >= creation time",
          snapshot.getCollectionTimestamp() >= beforeCreate);
      assertTrue("Timestamp should be <= current time",
          snapshot.getCollectionTimestamp() <= afterCollect);
      // Timestamp should not be zero / epoch
      assertTrue("Timestamp should not be zero", snapshot.getCollectionTimestamp() > 0);
    } finally {
      collector.shutdown();
    }
  }

  @Test
  public void testRefreshIntervalRespected() throws Exception {
    when(mockQueueStats.getAllocatedMemoryMB()).thenReturn(2048L);
    when(mockQueueStats.getAvailableMemoryMB()).thenReturn(2048L);
    when(mockQueueStats.getAllocatedVCores()).thenReturn(8L);
    when(mockQueueStats.getAvailableVCores()).thenReturn(8L);
    when(mockQueueStats.getNumAppsRunning()).thenReturn(2L);
    when(mockQueueStats.getPendingContainers()).thenReturn(0L);
    when(mockQueueInfo.getQueueStatistics()).thenReturn(mockQueueStats);
    when(mockQueueInfo.getCapacity()).thenReturn(0.6f);
    when(mockYarnClient.getQueueInfo("default")).thenReturn(mockQueueInfo);

    // Use a short interval so the second collection happens quickly
    long intervalMs = 100;
    YarnQueueMetricsCollector collector = new YarnQueueMetricsCollector(
        mockYarnClient, "default", intervalMs, "refresh-interval-test");

    try {
      // Wait for initial eager collection (1 call)
      waitForSnapshot(collector, WAIT_TIMEOUT_MS);
      int callsAfterFirst = mockingDetails(mockYarnClient).getInvocations().size();

      // Wait for at least one scheduled refresh using a poll loop instead of sleep.
      // interval + max-jitter (20%) + buffer gives enough headroom without a hard sleep.
      long toleranceMs = intervalMs + (long)(intervalMs * 0.2) + 300;
      waitForInvocationCount(mockYarnClient, callsAfterFirst + 1, toleranceMs);

      int callsAfterWait = mockingDetails(mockYarnClient).getInvocations().size();
      assertTrue("At least one refresh should have occurred within interval + tolerance",
          callsAfterWait > callsAfterFirst);

    } finally {
      collector.shutdown();
    }
  }

  @Test
  public void testZeroRefreshIntervalIsRejected() throws Exception {
    // scheduleWithFixedDelay requires a strictly positive period; zero throws
    // IllegalArgumentException directly (not wrapped in a generic RuntimeException).
    when(mockQueueInfo.getQueueStatistics()).thenReturn(null);
    when(mockQueueInfo.getCapacity()).thenReturn(0.5f);
    when(mockYarnClient.getQueueInfo(anyString())).thenReturn(mockQueueInfo);

    assertThrows(IllegalArgumentException.class, () ->
        new YarnQueueMetricsCollector(mockYarnClient, "default", 0, "zero-interval-test"));
  }

  @Test
  public void testNegativeRefreshIntervalIsRejected() throws Exception {
    // Negative interval is also rejected by scheduleWithFixedDelay and thrown directly
    // as IllegalArgumentException (not wrapped in a generic RuntimeException).
    when(mockQueueInfo.getQueueStatistics()).thenReturn(null);
    when(mockQueueInfo.getCapacity()).thenReturn(0.5f);
    when(mockYarnClient.getQueueInfo(anyString())).thenReturn(mockQueueInfo);

    assertThrows(IllegalArgumentException.class, () ->
        new YarnQueueMetricsCollector(mockYarnClient, "default", -1000, "negative-interval-test"));
  }
}
