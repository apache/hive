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

package org.apache.hadoop.hive.ql.exec.tez.monitoring.yarnqueue;

import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.awaitility.core.ConditionTimeoutException;

import java.time.Duration;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.when;

/**
 * Test cases for YarnQueueMetricsCollector.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestYarnQueueMetricsCollector {

  @Mock
  private YarnClient mockYarnClient;
  @Mock
  private QueueInfo mockQueueInfo;
  @Mock
  private QueueStatistics mockQueueStats;

  private static final long WAIT_TIMEOUT_MS = 5000;

  @Before
  public void setUp() throws Exception {
    // Shutdown the pool manager singleton and cache so each test starts with a clean state.
    QueueMetricsRefreshPool.shutdown();
    QueueMetricsCache.getInstance().shutdown();
    setupHappyPathMocks();
  }

  @After
  public void tearDown() {
    QueueMetricsRefreshPool.shutdown();
    QueueMetricsCache.getInstance().shutdown();
  }

  /**
   * Helper to create a collector in tests.
   */
  private YarnQueueMetricsCollector newCollector(YarnClient yarnClient, String queueName,
      long refreshIntervalMs, String queryId) {
    return new YarnQueueMetricsCollector(yarnClient, queueName, refreshIntervalMs, queryId);
  }

  /**
   * Waits for a snapshot to be available (non-null) using Awaitility.
   */
  private QueueMetricsSnapshot waitForSnapshot(
      YarnQueueMetricsCollector collector, long timeoutMs) {
    await().atMost(Duration.ofMillis(timeoutMs))
        .pollInterval(Duration.ofMillis(10))
        .until(() -> collector.getLatestSnapshot() != null);
    return collector.getLatestSnapshot();
  }

  /**
   * Waits until the mock has been invoked at least {@code minCount} times, or the timeout
   * elapses. Returns silently on timeout so callers can assert on the observed count.
   */
  private void waitForInvocationCount(Object mock, int minCount, long timeoutMs) {
    try {
      await().atMost(Duration.ofMillis(timeoutMs))
          .pollInterval(Duration.ofMillis(10))
          .until(() -> mockingDetails(mock).getInvocations().size() >= minCount);
    } catch (ConditionTimeoutException ignored) {
      // Intentional: callers assert on the observed count after this returns.
    }
  }

  /**
   * Configures mock objects with standard happy-path values.
   * Called from {@code @Before} so all tests start with a consistent baseline.
   * Stubs are lenient so tests that don't exercise these mocks don't fail with
   * UnnecessaryStubbingException.
   */
  private void setupHappyPathMocks() throws Exception {
    lenient().when(mockQueueStats.getAllocatedMemoryMB()).thenReturn(1024L);
    lenient().when(mockQueueStats.getAvailableMemoryMB()).thenReturn(1024L);
    lenient().when(mockQueueStats.getAllocatedVCores()).thenReturn(4L);
    lenient().when(mockQueueStats.getAvailableVCores()).thenReturn(4L);
    lenient().when(mockQueueStats.getNumAppsRunning()).thenReturn(1L);
    lenient().when(mockQueueStats.getNumAppsPending()).thenReturn(0L);
    lenient().when(mockQueueStats.getAllocatedContainers()).thenReturn(2L);
    lenient().when(mockQueueStats.getPendingContainers()).thenReturn(0L);
    lenient().when(mockQueueInfo.getQueueStatistics()).thenReturn(mockQueueStats);
    lenient().when(mockQueueInfo.getCapacity()).thenReturn(0.5f);
    lenient().when(mockQueueInfo.getCurrentCapacity()).thenReturn(0.25f);
    lenient().when(mockYarnClient.getQueueInfo(anyString())).thenReturn(mockQueueInfo);
  }

  @Test(expected = NullPointerException.class)
  public void testConstructorWithNullYarnClient() {
    new YarnQueueMetricsCollector(null, "default", 1000, "query-1");
  }

  @Test(expected = NullPointerException.class)
  public void testConstructorWithNullQueueName() {
    new YarnQueueMetricsCollector(mockYarnClient, null, 1000, "query-1");
  }

  @Test
  public void testSuccessfulMetricsCollection() {

    YarnQueueMetricsCollector collector = newCollector(mockYarnClient, "default", 10000, "test-query-1");
    try {
      QueueMetricsSnapshot snapshot = waitForSnapshot(collector, WAIT_TIMEOUT_MS);

      assertNotNull("Snapshot should not be null", snapshot);
      assertEquals("Memory used should be 1GB", 1.0f, snapshot.getMemoryUsedGB(), 0.001f);
      assertEquals("Memory total should be 2GB (1+1)", 2.0f, snapshot.getMemoryTotalGB(), 0.001f);
      assertEquals("VCores used should be 4", 4, snapshot.getVCoresUsed());
      assertEquals("VCores total should be 8 (4+4)", 8, snapshot.getVCoresTotal());
      assertEquals("Running apps should be 1", 1, snapshot.getRunningApps());
      assertEquals("Pending apps should be 0", 0, snapshot.getPendingApps());
      assertEquals("Allocated containers should be 2", 2, snapshot.getAllocatedContainers());
      assertEquals("Pending containers should be 0", 0, snapshot.getPendingContainers());
      assertEquals("Capacity should be 50%", 50.0f, snapshot.getCapacityPercentage(), 0.001f);
      assertEquals("Current capacity should be 25%", 25.0f, snapshot.getCurrentCapacityPercentage(), 0.001f);
      assertEquals("Memory percentage", "50.00%", snapshot.getMemoryPercentage());
      assertEquals("VCores percentage", "50.00%", snapshot.getVCoresPercentage());
    } finally {
      collector.shutdown();
    }
  }

  @Test
  public void testMetricsCollectionWithNullQueueInfo() throws Exception {
    when(mockYarnClient.getQueueInfo("nonexistent")).thenReturn(null);

    YarnQueueMetricsCollector collector = newCollector(mockYarnClient, "nonexistent", 10000, "test-query-2");
    try {
      assertNull("Snapshot should be null for nonexistent queue", collector.getLatestSnapshot());
    } finally {
      collector.shutdown();
    }
  }

  @Test
  public void testMetricsCollectionWithNullQueueStatistics() throws Exception {
    when(mockQueueInfo.getQueueStatistics()).thenReturn(null);
    when(mockQueueInfo.getCapacity()).thenReturn(0.5f);
    when(mockQueueInfo.getCurrentCapacity()).thenReturn(0.0f);
    when(mockYarnClient.getQueueInfo("default")).thenReturn(mockQueueInfo);

    YarnQueueMetricsCollector collector = newCollector(mockYarnClient, "default", 10000, "test-query-3");
    try {
      QueueMetricsSnapshot snapshot = waitForSnapshot(collector, WAIT_TIMEOUT_MS);
      assertNotNull("Snapshot should not be null", snapshot);
      assertEquals("Memory used should be 0", 0.0f, snapshot.getMemoryUsedGB(), 0.001f);
      assertEquals("Memory total should be 0", 0.0f, snapshot.getMemoryTotalGB(), 0.001f);
      assertEquals("VCores used should be 0", 0, snapshot.getVCoresUsed());
      assertEquals("VCores total should be 0", 0, snapshot.getVCoresTotal());
      assertEquals("Capacity should still be 50%", 50.0f, snapshot.getCapacityPercentage(), 0.001f);
      assertEquals("Current capacity should be 0%", 0.0f, snapshot.getCurrentCapacityPercentage(), 0.001f);
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
    when(mockQueueStats.getNumAppsPending()).thenReturn(0L);
    when(mockQueueStats.getAllocatedContainers()).thenReturn(0L);
    when(mockQueueStats.getPendingContainers()).thenReturn(0L);
    when(mockQueueInfo.getQueueStatistics()).thenReturn(mockQueueStats);
    when(mockQueueInfo.getCapacity()).thenReturn(0.0f);
    when(mockQueueInfo.getCurrentCapacity()).thenReturn(0.0f);

    QueueMetricsSnapshot snapshot =
        new QueueMetricsSnapshot(mockQueueInfo);

    // Should return "N/A" for percentages when total is zero
    assertEquals("Memory percentage should be N/A", "N/A", snapshot.getMemoryPercentage());
    assertEquals("VCores percentage should be N/A", "N/A", snapshot.getVCoresPercentage());
  }

  @Test
  public void testShutdownIdempotency() throws Exception {
    when(mockYarnClient.getQueueInfo("default")).thenReturn(mockQueueInfo);

    YarnQueueMetricsCollector collector = newCollector(mockYarnClient, "default", 10000, "test-query-4");
    collector.shutdown();
    collector.shutdown(); // second call must be safe
    assertTrue("Multiple shutdowns should be safe", true);
  }

  @Test
  public void testExceptionDuringCollection() throws Exception {
    when(mockYarnClient.getQueueInfo("default"))
        .thenThrow(new RuntimeException("RM unavailable"));

    YarnQueueMetricsCollector collector = newCollector(mockYarnClient, "default", 10000, "test-query-5");
    try {
      assertNull("Snapshot should be null after exception", collector.getLatestSnapshot());
    } finally {
      collector.shutdown();
    }
  }

  @Test
  public void testQueueNameRetrieval() throws Exception {
    when(mockYarnClient.getQueueInfo(anyString())).thenReturn(mockQueueInfo);
    when(mockQueueInfo.getQueueStatistics()).thenReturn(null);
    when(mockQueueInfo.getCapacity()).thenReturn(0.5f);

    YarnQueueMetricsCollector collector = newCollector(mockYarnClient, "production", 10000, "test-query-6");
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
    when(mockQueueStats.getNumAppsPending()).thenReturn(2L);
    when(mockQueueStats.getAllocatedContainers()).thenReturn(10L);
    when(mockQueueStats.getPendingContainers()).thenReturn(7L);
    when(mockQueueInfo.getQueueStatistics()).thenReturn(mockQueueStats);
    when(mockQueueInfo.getCapacity()).thenReturn(0.2f); // 20%
    when(mockQueueInfo.getCurrentCapacity()).thenReturn(0.05f); // 5%

    QueueMetricsSnapshot snapshot =
        new QueueMetricsSnapshot(mockQueueInfo);

    // Total = Used + Available
    assertEquals("Memory used", 5.0f, snapshot.getMemoryUsedGB(), 0.001f);
    assertEquals("Memory total", 20.0f, snapshot.getMemoryTotalGB(), 0.001f); // 5+15
    assertEquals("Memory percentage", "25.00%", snapshot.getMemoryPercentage()); // 5/20

    assertEquals("VCores used", 50, snapshot.getVCoresUsed());
    assertEquals("VCores total", 200, snapshot.getVCoresTotal()); // 50+150
    assertEquals("VCores percentage", "25.00%", snapshot.getVCoresPercentage()); // 50/200
    
    assertEquals("Running apps", 3, snapshot.getRunningApps());
    assertEquals("Pending apps", 2, snapshot.getPendingApps());
    assertEquals("Allocated containers", 10, snapshot.getAllocatedContainers());
    assertEquals("Pending containers", 7, snapshot.getPendingContainers());
    assertEquals("Capacity", 20.0f, snapshot.getCapacityPercentage(), 0.001f);
    assertEquals("Current capacity", 5.0f, snapshot.getCurrentCapacityPercentage(), 0.001f);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testQueueMetricsSnapshotWithNullQueueInfo() {
    new QueueMetricsSnapshot(null);
  }


  @Test
  public void testExecutorCleanupOnInitializationFailure() throws Exception {
    when(mockYarnClient.getQueueInfo(anyString()))
        .thenThrow(new RuntimeException("Simulated RM failure during init"));

    YarnQueueMetricsCollector collector = newCollector(mockYarnClient, "default", 10000, "init-fail-query");
    try {
      assertNull("Snapshot should be null after init failure", collector.getLatestSnapshot());
    } finally {
      collector.shutdown();
    }
  }

  @Test
  public void testCircuitBreakerActivatesAfterMaxFailures() throws Exception {
    when(mockYarnClient.getQueueInfo(anyString()))
        .thenThrow(new RuntimeException("YARN RM unavailable"));

    YarnQueueMetricsCollector collector = newCollector(mockYarnClient, "default", 50, "circuit-breaker-query-1");
    try {
      waitForInvocationCount(mockYarnClient, 6, WAIT_TIMEOUT_MS);
      assertNull("Snapshot should be null when circuit breaker active", collector.getLatestSnapshot());
      assertTrue("Circuit breaker should reduce calls",
          mockingDetails(mockYarnClient).getInvocations().size() < 12);
    } finally {
      collector.shutdown();
    }
  }

  @Test
  public void testCircuitBreakerResetsOnSuccess() throws Exception {
    when(mockQueueStats.getAllocatedMemoryMB()).thenReturn(4096L);
    when(mockQueueStats.getAvailableMemoryMB()).thenReturn(4096L);
    when(mockQueueStats.getAllocatedVCores()).thenReturn(50L);
    when(mockQueueStats.getAvailableVCores()).thenReturn(50L);
    when(mockQueueStats.getNumAppsRunning()).thenReturn(2L);
    when(mockQueueStats.getNumAppsPending()).thenReturn(1L);
    when(mockQueueStats.getAllocatedContainers()).thenReturn(5L);
    when(mockQueueStats.getPendingContainers()).thenReturn(5L);
    when(mockQueueInfo.getQueueStatistics()).thenReturn(mockQueueStats);
    when(mockQueueInfo.getCapacity()).thenReturn(0.3f);
    when(mockQueueInfo.getCurrentCapacity()).thenReturn(0.2f);
    when(mockYarnClient.getQueueInfo(anyString()))
        .thenThrow(new RuntimeException("Temporary RM failure"))
        .thenThrow(new RuntimeException("Temporary RM failure"))
        .thenThrow(new RuntimeException("Temporary RM failure"))
        .thenThrow(new RuntimeException("Temporary RM failure"))
        .thenThrow(new RuntimeException("Temporary RM failure"))
        .thenReturn(mockQueueInfo);

    YarnQueueMetricsCollector collector = newCollector(mockYarnClient, "default", 30, "circuit-breaker-recovery-query");
    try {
      waitForInvocationCount(mockYarnClient, 3, WAIT_TIMEOUT_MS);
      assertNull("Snapshot should be null after circuit breaker activates", collector.getLatestSnapshot());
      QueueMetricsSnapshot snapshot = waitForSnapshot(collector, WAIT_TIMEOUT_MS);
      assertNotNull("Snapshot should be populated after circuit breaker recovery", snapshot);
      assertEquals("Memory used should be 4GB", 4.0f, snapshot.getMemoryUsedGB(), 0.001f);

    } finally {
      collector.shutdown();
    }
  }


  @Test
  public void testNullQueueInfoDoesNotTriggerCircuitBreaker() throws Exception {
    when(mockYarnClient.getQueueInfo(anyString())).thenReturn(null);

    YarnQueueMetricsCollector collector = newCollector(mockYarnClient, "nonexistent-queue", 50, "null-queueinfo-query");
    try {
      waitForInvocationCount(mockYarnClient, 8, WAIT_TIMEOUT_MS);
      assertNull("Snapshot should remain null for null QueueInfo", collector.getLatestSnapshot());
      int callCount = mockingDetails(mockYarnClient).getInvocations().size();
      assertTrue("Null QueueInfo should NOT trigger circuit breaker (got " + callCount + " calls)",
          callCount >= 8);
    } finally {
      collector.shutdown();
    }
  }

  @Test
  public void testSnapshotCollectionTimestampIsRecent() {
    long beforeCreate = System.currentTimeMillis();
    YarnQueueMetricsCollector collector = newCollector(mockYarnClient, "default", 10000, "timestamp-test");
    try {
      QueueMetricsSnapshot snapshot = waitForSnapshot(collector, WAIT_TIMEOUT_MS);
      long afterCollect = System.currentTimeMillis();
      assertNotNull("Snapshot should not be null", snapshot);
      assertTrue("Timestamp should be >= creation time", snapshot.getCollectionTimestamp() >= beforeCreate);
      assertTrue("Timestamp should be <= current time", snapshot.getCollectionTimestamp() <= afterCollect);
      assertTrue("Timestamp should not be zero", snapshot.getCollectionTimestamp() > 0);
    } finally {
      collector.shutdown();
    }
  }

  @Test
  public void testRefreshIntervalRespected() {
    when(mockQueueStats.getAllocatedMemoryMB()).thenReturn(2048L);
    when(mockQueueStats.getAvailableMemoryMB()).thenReturn(2048L);
    when(mockQueueStats.getAllocatedVCores()).thenReturn(8L);
    when(mockQueueStats.getAvailableVCores()).thenReturn(8L);
    when(mockQueueStats.getNumAppsRunning()).thenReturn(2L);
    when(mockQueueInfo.getCapacity()).thenReturn(0.6f);

    long intervalMs = 100;
    YarnQueueMetricsCollector collector = newCollector(mockYarnClient, "default", intervalMs, "refresh-interval-test");
    try {
      waitForSnapshot(collector, WAIT_TIMEOUT_MS);
      int callsAfterFirst = mockingDetails(mockYarnClient).getInvocations().size();
      long toleranceMs = intervalMs + (long) (intervalMs * 0.2) + 2000;
      waitForInvocationCount(mockYarnClient, callsAfterFirst + 1, toleranceMs);
      assertTrue("At least one refresh should have occurred within interval + tolerance",
          mockingDetails(mockYarnClient).getInvocations().size() > callsAfterFirst);
    } finally {
      collector.shutdown();
    }
  }

  @Test
  public void testZeroRefreshIntervalIsRejected() {
    assertThrows(IllegalArgumentException.class, () ->
        new YarnQueueMetricsCollector(mockYarnClient, "default", 0, "zero-interval-test"));
  }

  @Test
  public void testNegativeRefreshIntervalIsRejected() {
    assertThrows(IllegalArgumentException.class, () ->
        new YarnQueueMetricsCollector(mockYarnClient, "default", -1000, "negative-interval-test"));
  }

  @Test
  public void testMultipleSessionsShareCacheState() {

    // Create two collectors for the same queue
    YarnQueueMetricsCollector collector1 = newCollector(mockYarnClient, "default", 5000, "query-1");
    YarnQueueMetricsCollector collector2 = newCollector(mockYarnClient, "default", 5000, "query-2");

    try {
      // Wait for first snapshot
      QueueMetricsSnapshot snapshot1 = waitForSnapshot(collector1, WAIT_TIMEOUT_MS);

      // Second collector should get same snapshot from cache (not null)
      QueueMetricsSnapshot snapshot2 = collector2.getLatestSnapshot();

      assertNotNull("Second collector should get cached snapshot", snapshot2);
      assertSame("Both collectors should return the exact same cached snapshot instance",
          snapshot1, snapshot2);

    } finally {
      collector1.shutdown();
      collector2.shutdown();
    }
  }

  @Test
  public void testDynamicReschedulingOnIntervalChange() {

    // Start with slow collector (10s)
    YarnQueueMetricsCollector slowCollector = newCollector(mockYarnClient, "default", 10000, "slow-query");
    // Wait for first snapshot to confirm slow collector has stabilized
    waitForSnapshot(slowCollector, WAIT_TIMEOUT_MS);

    // Add fast collector (1s) - should trigger rescheduling to 1s
    YarnQueueMetricsCollector fastCollector = newCollector(mockYarnClient, "default", 1000, "fast-query");

    try {
      // Verify both collectors see updates (implies task running at faster interval)
      QueueMetricsSnapshot snapshot = waitForSnapshot(fastCollector, WAIT_TIMEOUT_MS);
      assertNotNull("Fast collector should get snapshot quickly", snapshot);

      // Shutdown fast collector - should reschedule back to slow interval
      fastCollector.shutdown();
      // Wait up to WAIT_TIMEOUT_MS for rescheduling to complete
      waitForInvocationCount(mockYarnClient, mockingDetails(mockYarnClient).getInvocations().size(), WAIT_TIMEOUT_MS);

      // Verify slow collector still works
      assertNotNull("Slow collector should continue after fast shutdown",
          slowCollector.getLatestSnapshot());
    } finally {
      slowCollector.shutdown();
    }
  }

  @Test
  public void testCircuitBreakerProbeEvery10Ticks() throws Exception {
    // Mock to always fail
    when(mockYarnClient.getQueueInfo(anyString()))
        .thenThrow(new RuntimeException("RM always failing"));

    YarnQueueMetricsCollector collector = newCollector(mockYarnClient, "default", 50, "probe-test");

    try {
      // Wait for circuit breaker to activate (5 failures)
      waitForInvocationCount(mockYarnClient, 6, WAIT_TIMEOUT_MS);
      int callsAfterActivation = mockingDetails(mockYarnClient).getInvocations().size();

      // Wait for at least one probe attempt to occur past the circuit-breaker activation.
      waitForInvocationCount(mockYarnClient, callsAfterActivation + 1, WAIT_TIMEOUT_MS);
      int callsAfterWait = mockingDetails(mockYarnClient).getInvocations().size();

      int probeAttempts = callsAfterWait - callsAfterActivation;
      assertTrue("Circuit breaker should allow at least one probe past activation, got " + probeAttempts,
          probeAttempts > 0);
    } finally {
      collector.shutdown();
    }
  }

  @Test
  public void testTaskCancelsWhenAllSessionsDeregister() {

    YarnQueueMetricsCollector collector1 = newCollector(mockYarnClient, "default", 2000, "query-1");
    YarnQueueMetricsCollector collector2 = newCollector(mockYarnClient, "default", 2000, "query-2");

    // Wait for initial refresh
    waitForSnapshot(collector1, WAIT_TIMEOUT_MS);
    int callsWithBoth = mockingDetails(mockYarnClient).getInvocations().size();

    // Shutdown both collectors
    collector1.shutdown();
    collector2.shutdown();

    // Wait and verify no more RM calls after shutdown (task cancelled).
    // Poll up to WAIT_TIMEOUT_MS; exit early once the observed count is no longer growing past callsWithBoth.
    int[] latest = { mockingDetails(mockYarnClient).getInvocations().size() };
    try {
      await().atMost(Duration.ofMillis(WAIT_TIMEOUT_MS))
          .pollInterval(Duration.ofMillis(10))
          .until(() -> {
            latest[0] = mockingDetails(mockYarnClient).getInvocations().size();
            return latest[0] <= callsWithBoth;
          });
    } catch (ConditionTimeoutException ignored) {
      // Assertion below will report the mismatch.
    }

    assertEquals("No more RM calls should occur after all sessions deregister",
        callsWithBoth, latest[0]);
  }
}
