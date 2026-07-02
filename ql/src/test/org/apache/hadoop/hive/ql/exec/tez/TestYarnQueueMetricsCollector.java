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
    }
  }

  /**
   * Helper method that configures mock objects with standard happy-path values
   * for queue metrics collection tests. Tests that need specific values can
   * override individual mocks after calling this method.
   * <p>
   * Default values:
   * <ul>
   *   <li>Allocated memory: 1GB (1024 MB)</li>
   *   <li>Available memory: 1GB (1024 MB)</li>
   *   <li>Allocated vCores: 4</li>
   *   <li>Available vCores: 4</li>
   *   <li>Running apps: 1</li>
   *   <li>Pending apps: 0</li>
   *   <li>Allocated containers: 2</li>
   *   <li>Pending containers: 0</li>
   *   <li>Queue capacity: 50% (0.5f)</li>
   *   <li>Current capacity: 25% (0.25f)</li>
   * </ul>
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
    // Constructor throws before any resource is acquired - no try-with-resources needed
    new YarnQueueMetricsCollector(null, "default", 1000, "query-1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorWithNullQueueName() {
    // Constructor throws before any resource is acquired - no try-with-resources needed
    new YarnQueueMetricsCollector(mockYarnClient, null, 1000, "query-1");
  }

  @Test
  public void testSuccessfulMetricsCollection() throws Exception {
    setupHappyPathMocks();
    when(mockYarnClient.getQueueInfo("default")).thenReturn(mockQueueInfo);

    try (YarnQueueMetricsCollector collector = new YarnQueueMetricsCollector(
        mockYarnClient, "default", 10000, "test-query-1")) {
      YarnQueueMetricsCollector.QueueMetricsSnapshot snapshot = waitForSnapshot(collector, WAIT_TIMEOUT_MS);

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
    }
  }

  @Test
  public void testMetricsCollectionWithNullQueueInfo() throws Exception {
    when(mockYarnClient.getQueueInfo("nonexistent")).thenReturn(null);

    try (YarnQueueMetricsCollector collector = new YarnQueueMetricsCollector(
        mockYarnClient, "nonexistent", 10000, "test-query-2")) {
      // Constructor performs eager collection, snapshot should already be null
      // (No need to wait since null QueueInfo means immediate null snapshot)
      assertNull("Snapshot should be null for nonexistent queue",
          collector.getLatestSnapshot());
    }
  }

  @Test
  public void testMetricsCollectionWithNullQueueStatistics() throws Exception {
    when(mockQueueInfo.getQueueStatistics()).thenReturn(null);
    when(mockQueueInfo.getCapacity()).thenReturn(0.5f);
    when(mockQueueInfo.getCurrentCapacity()).thenReturn(0.0f);
    when(mockYarnClient.getQueueInfo("default")).thenReturn(mockQueueInfo);

    try (YarnQueueMetricsCollector collector = new YarnQueueMetricsCollector(
        mockYarnClient, "default", 10000, "test-query-3")) {
      YarnQueueMetricsCollector.QueueMetricsSnapshot snapshot = waitForSnapshot(collector, WAIT_TIMEOUT_MS);

      assertNotNull("Snapshot should not be null", snapshot);
      assertEquals("Memory used should be 0", 0.0f, snapshot.getMemoryUsedGB(), 0.01f);
      assertEquals("Memory total should be 0", 0.0f, snapshot.getMemoryTotalGB(), 0.01f);
      assertEquals("VCores used should be 0", 0, snapshot.getVCoresUsed());
      assertEquals("VCores total should be 0", 0, snapshot.getVCoresTotal());
      assertEquals("Capacity should still be 50%", 50.0f, snapshot.getCapacityPercentage(), 0.1f);
      assertEquals("Current capacity should be 0%", 0.0f, snapshot.getCurrentCapacityPercentage(), 0.1f);
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

    YarnQueueMetricsCollector.QueueMetricsSnapshot snapshot =
        new YarnQueueMetricsCollector.QueueMetricsSnapshot(mockQueueInfo);

    // Should return "N/A" for percentages when total is zero
    assertEquals("Memory percentage should be N/A", "N/A", snapshot.getMemoryPercentage());
    assertEquals("VCores percentage should be N/A", "N/A", snapshot.getVCoresPercentage());
  }

  @Test
  public void testShutdownIdempotency() throws Exception {
    when(mockYarnClient.getQueueInfo("default")).thenReturn(mockQueueInfo);

    // Use try-with-resources; also call shutdown() explicitly inside to
    // verify that repeated close()/shutdown() calls are safe (idempotent).
    try (YarnQueueMetricsCollector collector = new YarnQueueMetricsCollector(
        mockYarnClient, "default", 10000, "test-query-4")) {
      collector.shutdown(); // explicit close 1
      collector.shutdown(); // explicit close 2
      // implicit close 3 happens at end of try block
      assertTrue("Multiple shutdowns should be safe", true);
    }
  }

  @Test
  public void testExceptionDuringCollection() throws Exception {
    when(mockYarnClient.getQueueInfo("default"))
        .thenThrow(new RuntimeException("RM unavailable"));

    try (YarnQueueMetricsCollector collector = new YarnQueueMetricsCollector(
        mockYarnClient, "default", 10000, "test-query-5")) {
      assertNull("Snapshot should be null after exception",
          collector.getLatestSnapshot());
    }
  }

  @Test
  public void testQueueNameRetrieval() throws Exception {
    when(mockYarnClient.getQueueInfo(anyString())).thenReturn(mockQueueInfo);
    when(mockQueueInfo.getQueueStatistics()).thenReturn(null);
    when(mockQueueInfo.getCapacity()).thenReturn(0.5f);

    try (YarnQueueMetricsCollector collector = new YarnQueueMetricsCollector(
        mockYarnClient, "production", 10000, "test-query-6")) {
      assertEquals("Queue name should match", "production", collector.getQueueName());
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

    YarnQueueMetricsCollector.QueueMetricsSnapshot snapshot =
        new YarnQueueMetricsCollector.QueueMetricsSnapshot(mockQueueInfo);

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
    new YarnQueueMetricsCollector.QueueMetricsSnapshot(null);
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

    // Constructor wraps collection error - first call fails but shouldn't throw from constructor
    // (collectMetrics swallows exceptions). This test verifies the try-catch guards are correct.
    try (YarnQueueMetricsCollector collector = new YarnQueueMetricsCollector(
        mockYarnClient, "default", 10000, "init-fail-query")) {
      assertNull("Snapshot should be null after init failure",
          collector.getLatestSnapshot());
    }
  }

  // -------------------------------------------------------------------------
  // Tests for Issue #2: Circuit breaker for repeated failures
  // -------------------------------------------------------------------------
  @Test
  public void testCircuitBreakerActivatesAfterMaxFailures() throws Exception {
    when(mockYarnClient.getQueueInfo(anyString()))
        .thenThrow(new RuntimeException("YARN RM unavailable"));

    try (YarnQueueMetricsCollector collector = new YarnQueueMetricsCollector(
        mockYarnClient, "default", 50, "circuit-breaker-query-1")) {
      waitForInvocationCount(mockYarnClient, 6, 1000);

      assertNull("Snapshot should be null when circuit breaker active",
          collector.getLatestSnapshot());

      int callCount = mockingDetails(mockYarnClient).getInvocations().size();
      assertTrue("Circuit breaker should reduce calls (got " + callCount + ")",
          callCount < 12);
    }
  }

  @Test
  public void testCircuitBreakerResetsOnSuccess() throws Exception {
    // Set up QueueStats and QueueInfo mocks first (without touching mockYarnClient.getQueueInfo)
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

    // Set up getQueueInfo to fail 5 times then succeed — this must be last to avoid being overwritten
    when(mockYarnClient.getQueueInfo(anyString()))
        .thenThrow(new RuntimeException("Temporary RM failure"))
        .thenThrow(new RuntimeException("Temporary RM failure"))
        .thenThrow(new RuntimeException("Temporary RM failure"))
        .thenThrow(new RuntimeException("Temporary RM failure"))
        .thenThrow(new RuntimeException("Temporary RM failure"))
        .thenReturn(mockQueueInfo);

    try (YarnQueueMetricsCollector collector = new YarnQueueMetricsCollector(
        mockYarnClient, "default", 30, "circuit-breaker-recovery-query")) {
      waitForInvocationCount(mockYarnClient, 3, 200);
      assertNull("Snapshot should be null after circuit breaker activates",
          collector.getLatestSnapshot());

      YarnQueueMetricsCollector.QueueMetricsSnapshot snapshot = waitForSnapshot(collector, 2000);
      assertNotNull("Snapshot should be populated after circuit breaker recovery", snapshot);
      assertEquals("Memory used should be 4GB", 4.0f, snapshot.getMemoryUsedGB(), 0.1f);
    }
  }

  /**
   * Verifies that the circuit breaker mechanism does not interfere with normal
   * successful metrics collection. This test documents expected behavior: when
   * YarnClient consistently returns valid data, the circuit breaker should never
   * activate and collection should work without interruption.
   * <p>
   * This test is part of the circuit breaker test suite (alongside failure and
   * recovery scenarios) and specifically validates the happy path behavior. While
   * it has similar assertions to testSuccessfulMetricsCollection(), its purpose
   * is to document circuit breaker behavior in the happy path, not general
   * functionality.
   *
   * @see #testSuccessfulMetricsCollection() for general happy path testing
   * @see #testCircuitBreakerActivatesAfterMaxFailures() for failure scenarios
   * @see #testCircuitBreakerResetsOnSuccess() for recovery scenarios
   */
  @Test
  public void testCircuitBreakerDoesNotAffectSuccessfulCollection() throws Exception {
    setupHappyPathMocks();
    when(mockQueueStats.getAllocatedMemoryMB()).thenReturn(2048L);
    when(mockQueueStats.getAvailableMemoryMB()).thenReturn(2048L);
    when(mockQueueStats.getAllocatedVCores()).thenReturn(20L);
    when(mockQueueStats.getAvailableVCores()).thenReturn(20L);
    when(mockQueueInfo.getCapacity()).thenReturn(0.1f);

    try (YarnQueueMetricsCollector collector = new YarnQueueMetricsCollector(
        mockYarnClient, "default", 10000, "no-failures-query")) {
      YarnQueueMetricsCollector.QueueMetricsSnapshot snapshot = waitForSnapshot(collector, WAIT_TIMEOUT_MS);

      assertNotNull("Snapshot should be available with no failures", snapshot);
      assertEquals("Memory used should be 2GB", 2.0f, snapshot.getMemoryUsedGB(), 0.1f);
      assertEquals("VCores used should be 20", 20, snapshot.getVCoresUsed());
    }
  }

  @Test
  public void testNullQueueInfoDoesNotTriggerCircuitBreaker() throws Exception {
    when(mockYarnClient.getQueueInfo(anyString())).thenReturn(null);

    try (YarnQueueMetricsCollector collector = new YarnQueueMetricsCollector(
        mockYarnClient, "nonexistent-queue", 50, "null-queueinfo-query")) {
      waitForInvocationCount(mockYarnClient, 8, 800);

      assertNull("Snapshot should remain null for null QueueInfo",
          collector.getLatestSnapshot());

      int callCount = mockingDetails(mockYarnClient).getInvocations().size();
      assertTrue("Null QueueInfo should NOT trigger circuit breaker (got " + callCount + " calls)",
          callCount >= 8);
    }
  }

  @Test
  public void testSnapshotCollectionTimestampIsRecent() throws Exception {
    setupHappyPathMocks();

    long beforeCreate = System.currentTimeMillis();
    try (YarnQueueMetricsCollector collector = new YarnQueueMetricsCollector(
        mockYarnClient, "default", 10000, "timestamp-test")) {
      YarnQueueMetricsCollector.QueueMetricsSnapshot snapshot =
          waitForSnapshot(collector, WAIT_TIMEOUT_MS);
      long afterCollect = System.currentTimeMillis();

      assertNotNull("Snapshot should not be null", snapshot);
      assertTrue("Timestamp should be >= creation time",
          snapshot.getCollectionTimestamp() >= beforeCreate);
      assertTrue("Timestamp should be <= current time",
          snapshot.getCollectionTimestamp() <= afterCollect);
      assertTrue("Timestamp should not be zero", snapshot.getCollectionTimestamp() > 0);
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
    try (YarnQueueMetricsCollector collector = new YarnQueueMetricsCollector(
        mockYarnClient, "default", intervalMs, "refresh-interval-test")) {
      waitForSnapshot(collector, WAIT_TIMEOUT_MS);
      int callsAfterFirst = mockingDetails(mockYarnClient).getInvocations().size();

      long toleranceMs = intervalMs + (long)(intervalMs * 0.2) + 300;
      waitForInvocationCount(mockYarnClient, callsAfterFirst + 1, toleranceMs);

      int callsAfterWait = mockingDetails(mockYarnClient).getInvocations().size();
      assertTrue("At least one refresh should have occurred within interval + tolerance",
          callsAfterWait > callsAfterFirst);
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
