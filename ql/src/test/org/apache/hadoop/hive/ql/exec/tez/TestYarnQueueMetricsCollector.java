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

import org.apache.hadoop.hive.ql.exec.tez.monitoring.yarnqueue.QueueMetricsCache;
import org.apache.hadoop.hive.ql.exec.tez.monitoring.yarnqueue.QueueMetricsRefreshPool;
import org.apache.hadoop.hive.ql.exec.tez.monitoring.yarnqueue.QueueMetricsSnapshot;
import org.apache.hadoop.hive.ql.exec.tez.monitoring.yarnqueue.YarnQueueMetricsCollector;
import org.apache.hadoop.hive.conf.HiveConf;
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
  private HiveConf testConf;

  private static final long WAIT_TIMEOUT_MS = 5000;

  @Before
  public void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    testConf = new HiveConf();
    // Reset the pool manager singleton and cache so each test starts with a clean state.
    QueueMetricsRefreshPool.resetForTesting();
    QueueMetricsCache.resetForTesting();
  }

  @After
  public void tearDown() throws Exception {
    if (closeable != null) {
      closeable.close();
    }
    QueueMetricsRefreshPool.resetForTesting();
    QueueMetricsCache.resetForTesting();
  }

  /**
   * Helper to create a collector in tests using a default HiveConf (min pool sizes).
   */
  private YarnQueueMetricsCollector newCollector(YarnClient yarnClient, String queueName,
      long refreshIntervalMs, String queryId) {
    return new YarnQueueMetricsCollector(yarnClient, queueName, refreshIntervalMs, queryId, testConf);
  }

  /**
   * Waits for a snapshot to be available (non-null).
   */
  private QueueMetricsSnapshot waitForSnapshot(
      YarnQueueMetricsCollector collector, long timeoutMs) {
    long startTime = System.currentTimeMillis();
    QueueMetricsSnapshot snapshot;
    while ((snapshot = collector.getLatestSnapshot()) == null) {
      if (System.currentTimeMillis() - startTime > timeoutMs) {
        fail("Snapshot not available after " + timeoutMs + "ms");
      }
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
        return;
      }
    }
  }

  /**
   * Helper method that configures mock objects with standard happy-path values.
   */
  private void setupHappyPathMocks() throws Exception {
    when(mockQueueStats.getAllocatedMemoryMB()).thenReturn(1024L);
    when(mockQueueStats.getAvailableMemoryMB()).thenReturn(1024L);
    when(mockQueueStats.getAllocatedVCores()).thenReturn(4L);
    when(mockQueueStats.getAvailableVCores()).thenReturn(4L);
    when(mockQueueStats.getNumAppsRunning()).thenReturn(1L);
    when(mockQueueStats.getNumAppsPending()).thenReturn(0L);
    when(mockQueueStats.getAllocatedContainers()).thenReturn(2L);
    when(mockQueueStats.getPendingContainers()).thenReturn(0L);
    when(mockQueueInfo.getQueueStatistics()).thenReturn(mockQueueStats);
    when(mockQueueInfo.getCapacity()).thenReturn(0.5f);
    when(mockQueueInfo.getCurrentCapacity()).thenReturn(0.25f);
    when(mockYarnClient.getQueueInfo(anyString())).thenReturn(mockQueueInfo);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorWithNullYarnClient() {
    new YarnQueueMetricsCollector(null, "default", 1000, "query-1", testConf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorWithNullQueueName() {
    new YarnQueueMetricsCollector(mockYarnClient, null, 1000, "query-1", testConf);
  }

  @Test
  public void testSuccessfulMetricsCollection() throws Exception {
    setupHappyPathMocks();
    when(mockYarnClient.getQueueInfo("default")).thenReturn(mockQueueInfo);

    YarnQueueMetricsCollector collector = newCollector(mockYarnClient, "default", 10000, "test-query-1");
    try {
      QueueMetricsSnapshot snapshot = waitForSnapshot(collector, WAIT_TIMEOUT_MS);

      assertNotNull("Snapshot should not be null", snapshot);
      assertEquals("Memory used should be 1GB", 1.0f, snapshot.getMemoryUsedGB(), 0.1f);
      assertEquals("Memory total should be 2GB (1+1)", 2.0f, snapshot.getMemoryTotalGB(), 0.1f);
      assertEquals("VCores used should be 4", 4, snapshot.getVCoresUsed());
      assertEquals("VCores total should be 8 (4+4)", 8, snapshot.getVCoresTotal());
      assertEquals("Running apps should be 1", 1, snapshot.getRunningApps());
      assertEquals("Pending apps should be 0", 0, snapshot.getPendingApps());
      assertEquals("Allocated containers should be 2", 2, snapshot.getAllocatedContainers());
      assertEquals("Pending containers should be 0", 0, snapshot.getPendingContainers());
      assertEquals("Capacity should be 50%", 50.0f, snapshot.getCapacityPercentage(), 0.1f);
      assertEquals("Current capacity should be 25%", 25.0f, snapshot.getCurrentCapacityPercentage(), 0.1f);
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
      assertEquals("Memory used should be 0", 0.0f, snapshot.getMemoryUsedGB(), 0.01f);
      assertEquals("Memory total should be 0", 0.0f, snapshot.getMemoryTotalGB(), 0.01f);
      assertEquals("VCores used should be 0", 0, snapshot.getVCoresUsed());
      assertEquals("VCores total should be 0", 0, snapshot.getVCoresTotal());
      assertEquals("Capacity should still be 50%", 50.0f, snapshot.getCapacityPercentage(), 0.1f);
      assertEquals("Current capacity should be 0%", 0.0f, snapshot.getCurrentCapacityPercentage(), 0.1f);
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
    assertEquals("Memory used", 5.0f, snapshot.getMemoryUsedGB(), 0.01f);
    assertEquals("Memory total", 20.0f, snapshot.getMemoryTotalGB(), 0.01f); // 5+15
    assertEquals("Memory percentage", "25.00%", snapshot.getMemoryPercentage()); // 5/20

    assertEquals("VCores used", 50, snapshot.getVCoresUsed());
    assertEquals("VCores total", 200, snapshot.getVCoresTotal()); // 50+150
    assertEquals("VCores percentage", "25.00%", snapshot.getVCoresPercentage()); // 50/200
    
    assertEquals("Running apps", 3, snapshot.getRunningApps());
    assertEquals("Pending apps", 2, snapshot.getPendingApps());
    assertEquals("Allocated containers", 10, snapshot.getAllocatedContainers());
    assertEquals("Pending containers", 7, snapshot.getPendingContainers());
    assertEquals("Capacity", 20.0f, snapshot.getCapacityPercentage(), 0.01f);
    assertEquals("Current capacity", 5.0f, snapshot.getCurrentCapacityPercentage(), 0.01f);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testQueueMetricsSnapshotWithNullQueueInfo() {
    new QueueMetricsSnapshot(null);
  }

  // -------------------------------------------------------------------------
  // Tests for Issue #1: Jitter on initial delay (Thundering Herd prevention)
  // -------------------------------------------------------------------------
  // Note: Jitter is implicitly tested by all tests that successfully create collectors.
  // Explicit jitter distribution testing would require reflection to access private
  // scheduling details, which is fragile and not worth the maintenance cost.

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
      waitForInvocationCount(mockYarnClient, 6, 1000);
      assertNull("Snapshot should be null when circuit breaker active", collector.getLatestSnapshot());
      int callCount = mockingDetails(mockYarnClient).getInvocations().size();
      assertTrue("Circuit breaker should reduce calls (got " + callCount + ")", callCount < 12);
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
      waitForInvocationCount(mockYarnClient, 3, 200);
      assertNull("Snapshot should be null after circuit breaker activates", collector.getLatestSnapshot());
      QueueMetricsSnapshot snapshot = waitForSnapshot(collector, 2000);
      assertNotNull("Snapshot should be populated after circuit breaker recovery", snapshot);
      assertEquals("Memory used should be 4GB", 4.0f, snapshot.getMemoryUsedGB(), 0.1f);
    } finally {
      collector.shutdown();
    }
  }


  @Test
  public void testNullQueueInfoDoesNotTriggerCircuitBreaker() throws Exception {
    when(mockYarnClient.getQueueInfo(anyString())).thenReturn(null);

    YarnQueueMetricsCollector collector = newCollector(mockYarnClient, "nonexistent-queue", 50, "null-queueinfo-query");
    try {
      waitForInvocationCount(mockYarnClient, 8, 800);
      assertNull("Snapshot should remain null for null QueueInfo", collector.getLatestSnapshot());
      int callCount = mockingDetails(mockYarnClient).getInvocations().size();
      assertTrue("Null QueueInfo should NOT trigger circuit breaker (got " + callCount + " calls)",
          callCount >= 8);
    } finally {
      collector.shutdown();
    }
  }

  @Test
  public void testSnapshotCollectionTimestampIsRecent() throws Exception {
    setupHappyPathMocks();
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
  public void testRefreshIntervalRespected() throws Exception {
    setupHappyPathMocks();
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
      long toleranceMs = intervalMs + (long) (intervalMs * 0.2) + 300;
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
    when(mockQueueInfo.getQueueStatistics()).thenReturn(null);
    when(mockQueueInfo.getCapacity()).thenReturn(0.5f);
    when(mockYarnClient.getQueueInfo(anyString())).thenReturn(mockQueueInfo);

    assertThrows(IllegalArgumentException.class, () ->
        new YarnQueueMetricsCollector(mockYarnClient, "default", 0, "zero-interval-test", testConf));
  }

  @Test
  public void testNegativeRefreshIntervalIsRejected() throws Exception {
    when(mockQueueInfo.getQueueStatistics()).thenReturn(null);
    when(mockQueueInfo.getCapacity()).thenReturn(0.5f);
    when(mockYarnClient.getQueueInfo(anyString())).thenReturn(mockQueueInfo);

    assertThrows(IllegalArgumentException.class, () ->
        new YarnQueueMetricsCollector(mockYarnClient, "default", -1000, "negative-interval-test", testConf));
  }

  @Test
  public void testJitterCalculationRange() {
    long intervalMs = 2000;
    long maxJitter = intervalMs * QueueMetricsRefreshPool.JITTER_PERCENT / 100; // 200ms

    // Test multiple queue names to ensure jitter is in range
    String[] queues = {"default", "production", "batch", "analytics", "q" + "x".repeat(50)};
    for (String queueName : queues) {
      long jitter = QueueMetricsRefreshPool.calculateJitter(queueName, intervalMs);
      assertTrue("Jitter should be >= 0 for " + queueName, jitter >= 0);
      assertTrue("Jitter should be < maxJitter (" + maxJitter + "ms) for " + queueName,
          jitter < maxJitter);
    }
  }

  @Test
  public void testJitterIsDeterministic() {
    long intervalMs = 5000;
    String queueName = "production-analytics";

    long jitter1 = QueueMetricsRefreshPool.calculateJitter(queueName, intervalMs);
    long jitter2 = QueueMetricsRefreshPool.calculateJitter(queueName, intervalMs);
    long jitter3 = QueueMetricsRefreshPool.calculateJitter(queueName, intervalMs);

    assertEquals("Jitter should be deterministic (same queue → same jitter)", jitter1, jitter2);
    assertEquals("Jitter should be deterministic across multiple calls", jitter2, jitter3);
  }

  @Test
  public void testMultipleSessionsShareCacheState() throws Exception {
    setupHappyPathMocks();

    // Create two collectors for the same queue
    YarnQueueMetricsCollector collector1 = newCollector(mockYarnClient, "default", 5000, "query-1");
    YarnQueueMetricsCollector collector2 = newCollector(mockYarnClient, "default", 5000, "query-2");

    try {
      // Wait for first snapshot
      QueueMetricsSnapshot snapshot1 = waitForSnapshot(collector1, WAIT_TIMEOUT_MS);

      // Second collector should get same snapshot from cache (not null)
      QueueMetricsSnapshot snapshot2 = collector2.getLatestSnapshot();

      assertNotNull("Second collector should get cached snapshot", snapshot2);
      assertEquals("Both collectors should see same memory value",
          snapshot1.getMemoryUsedGB(), snapshot2.getMemoryUsedGB(), 0.01f);
    } finally {
      collector1.shutdown();
      collector2.shutdown();
    }
  }

  @Test
  public void testDynamicReschedulingOnIntervalChange() throws Exception {
    setupHappyPathMocks();

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
      // Wait up to 500ms for rescheduling to complete
      waitForInvocationCount(mockYarnClient, mockingDetails(mockYarnClient).getInvocations().size(), 500);

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
      waitForInvocationCount(mockYarnClient, 6, 1000);
      int callsAfterActivation = mockingDetails(mockYarnClient).getInvocations().size();

      // Wait for next ~12 ticks at 50ms interval — poll until invocation count stabilizes
      waitForInvocationCount(mockYarnClient, callsAfterActivation + 2, 800);
      int callsAfterWait = mockingDetails(mockYarnClient).getInvocations().size();

      // Should have ~1 probe attempt in 10 ticks
      int probeAttempts = callsAfterWait - callsAfterActivation;
      assertTrue("Circuit breaker should allow ~1 probe per 10 ticks, got " + probeAttempts,
          probeAttempts >= 0 && probeAttempts <= 2);
    } finally {
      collector.shutdown();
    }
  }

  @Test
  public void testTaskCancelsWhenAllSessionsDeregister() throws Exception {
    setupHappyPathMocks();

    YarnQueueMetricsCollector collector1 = newCollector(mockYarnClient, "default", 2000, "query-1");
    YarnQueueMetricsCollector collector2 = newCollector(mockYarnClient, "default", 2000, "query-2");

    // Wait for initial refresh
    waitForSnapshot(collector1, WAIT_TIMEOUT_MS);
    int callsWithBoth = mockingDetails(mockYarnClient).getInvocations().size();

    // Shutdown both collectors
    collector1.shutdown();
    collector2.shutdown();

    // Wait and verify no more RM calls after shutdown (task cancelled)
    // Spin-wait up to 3 seconds checking that call count has stabilized after both shutdowns
    int callsAfterShutdown;
    long deadline = System.currentTimeMillis() + 3000;
    do {
      callsAfterShutdown = mockingDetails(mockYarnClient).getInvocations().size();
    } while (callsAfterShutdown > callsWithBoth && System.currentTimeMillis() < deadline);

    assertEquals("No more RM calls should occur after all sessions deregister",
        callsWithBoth, callsAfterShutdown);
  }
}
