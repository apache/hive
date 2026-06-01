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

package org.apache.hadoop.hive.ql.exec.tez.monitoring.yarnqueue;

import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Unit tests for QueueMetricsState - tests state management logic in isolation.
 * Tests interval registration, circuit breaker, refresh locking, and other state logic.
 */
public class TestQueueMetricsState {

  @Mock
  private QueueInfo mockQueueInfo;

  @Mock
  private QueueStatistics mockQueueStats;


  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    setupMockQueueInfo();
  }

  private void setupMockQueueInfo() {
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
  }

  @Test
  public void testConstructorWithNullSnapshot() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);

    assertNull("Snapshot should be null when constructed with null", state.getSnapshot());
    assertEquals("Min interval should be set", 5000L, state.getMinRefreshIntervalMs());
  }

  @Test
  public void testConstructorWithSnapshot() {
    QueueMetricsSnapshot snapshot = new QueueMetricsSnapshot(mockQueueInfo);
    QueueMetricsState state = new QueueMetricsState(snapshot, 10000L);

    assertNotNull("Snapshot should not be null", state.getSnapshot());
    assertEquals("Min interval should be set", 10000L, state.getMinRefreshIntervalMs());
  }

  @Test
  public void testGetAgeMsReturnsLargeValueInitially() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);

    long age = state.getAgeMs();

    // Age should be very large when lastWriteTime = 0 (epoch)
    assertTrue("Age should be > 1 year in ms", age > 365L * 24 * 60 * 60 * 1000);
  }

  @Test
  public void testApplySnapshotUpdatesSnapshot() {
    QueueMetricsState state = new QueueMetricsState(null, 10000L);
    assertNull("Initial snapshot should be null", state.getSnapshot());

    QueueMetricsSnapshot snapshot = new QueueMetricsSnapshot(mockQueueInfo);
    state.applySnapshot(snapshot, 5000L);

    assertNotNull("Snapshot should be updated", state.getSnapshot());
    assertEquals("Memory should match", 1.0f, state.getSnapshot().getMemoryUsedGB(), 0.01f);
  }

  @Test
  public void testApplySnapshotReducesAgeMs() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);
    long initialAge = state.getAgeMs();

    // Spin-wait up to 200ms to ensure time has passed so the age comparison is meaningful
    long deadline = System.currentTimeMillis() + 200;
    while (state.getAgeMs() <= initialAge && System.currentTimeMillis() < deadline) {
      Thread.onSpinWait(); // Hint to JVM that this is a spin-wait loop
    }

    QueueMetricsSnapshot snapshot = new QueueMetricsSnapshot(mockQueueInfo);
    state.applySnapshot(snapshot, 5000L);

    long newAge = state.getAgeMs();
    assertTrue("Age should be much smaller after apply", newAge < initialAge);
    assertTrue("Age should be recent (< 1s)", newAge < 1000);
  }

  @Test
  public void testApplySnapshotUpdatesMinRefreshInterval() {
    QueueMetricsState state = new QueueMetricsState(null, 10000L);
    assertEquals("Initial min interval", 10000L, state.getMinRefreshIntervalMs());

    // Apply snapshot with smaller interval
    QueueMetricsSnapshot snapshot = new QueueMetricsSnapshot(mockQueueInfo);
    state.applySnapshot(snapshot, 3000L);

    assertEquals("Min interval should be reduced", 3000L, state.getMinRefreshIntervalMs());
  }

  @Test
  public void testApplySnapshotDoesNotIncreaseMinInterval() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);

    // Apply snapshot with larger interval
    QueueMetricsSnapshot snapshot = new QueueMetricsSnapshot(mockQueueInfo);
    state.applySnapshot(snapshot, 10000L);

    // Min interval should stay at smaller value
    assertEquals("Min interval should not increase", 5000L, state.getMinRefreshIntervalMs());
  }

  @Test
  public void testRegisterIntervalReturnsTrueWhenNoTaskExists() {
    QueueMetricsState state = new QueueMetricsState(null, 10000L);

    boolean shouldSchedule = state.registerInterval(5000L);

    assertTrue("Should return true when no task exists", shouldSchedule);
  }

  @Test
  public void testRegisterIntervalReturnsTrueWhenLoweringMinimum() {
    QueueMetricsState state = new QueueMetricsState(null, 10000L);

    // First registration - creates task
    state.registerInterval(10000L);

    // Second registration with faster interval
    boolean shouldSchedule = state.registerInterval(5000L);

    assertTrue("Should return true when lowering minimum", shouldSchedule);
    assertEquals("Min interval should be updated", 5000L, state.getMinRefreshIntervalMs());
  }

  @Test
  public void testRegisterIntervalReturnsTrueWhenTaskIsNull() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);

    // Register faster interval first (task is null, should return true)
    boolean shouldSchedule1 = state.registerInterval(5000L);
    assertTrue("Should return true when task is null", shouldSchedule1);

    // Register slower interval (task still null, should still return true)
    boolean shouldSchedule2 = state.registerInterval(10000L);
    assertTrue("Should return true when task is null even with slower interval", shouldSchedule2);

    assertEquals("Min interval should stay at faster value", 5000L, state.getMinRefreshIntervalMs());
  }

  @Test
  public void testDeregisterIntervalReturnsFalseWhenOtherSessionsRemain() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);

    // Register two sessions at same interval
    state.registerInterval(5000L);
    state.registerInterval(5000L);

    // Deregister one session
    boolean shouldReschedule = state.deregisterInterval(5000L);

    assertFalse("Should return false when other sessions remain at this interval", shouldReschedule);
  }

  @Test
  public void testDeregisterIntervalRemovesBucket() {
    QueueMetricsState state = new QueueMetricsState(null, 10000L);

    // Register fast and slow sessions
    state.registerInterval(2000L);  // Fast
    state.registerInterval(10000L); // Slow

    // After registration, min should be 2000
    assertEquals("Min interval should be 2000ms", 2000L, state.getMinRefreshIntervalMs());

    // Deregister fast session - removes the 2000ms bucket
    state.deregisterInterval(2000L);

    // The minRefreshIntervalMs field uses Math.min logic (line 217) so it won't increase
    // back to 10000. This is by design - the field tracks historical minimum, not current.
    // The actual task rescheduling logic in ensureTaskScheduled() recomputes from intervalCounts.
    assertEquals("Min interval field remains at historical min (by design)",
        2000L, state.getMinRefreshIntervalMs());
  }

  @Test
  public void testTryStartRefreshPreventsRace() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);

    boolean first = state.tryStartRefresh();
    boolean second = state.tryStartRefresh();

    assertTrue("First call should succeed", first);
    assertFalse("Second call should fail (already refreshing)", second);
  }

  @Test
  public void testFinishRefreshReleasesLock() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);

    state.tryStartRefresh();
    state.finishRefresh();

    boolean canRefresh = state.tryStartRefresh();
    assertTrue("Should be able to refresh after finish", canRefresh);
  }

  @Test
  public void testRecordRefreshSuccessResetsCircuitBreaker() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);

    // Simulate failures
    for (int i = 0; i < 6; i++) {
      state.recordRefreshFailure("test-queue", "failure " + i);
    }

    // Circuit breaker should be active
    assertTrue("Circuit breaker should block",
        state.shouldSkipDueToCircuitBreaker("test-queue"));

    // Record success
    state.recordRefreshSuccess("test-queue");

    // Circuit breaker should be reset
    assertFalse("Circuit breaker should be reset after success",
        state.shouldSkipDueToCircuitBreaker("test-queue"));
  }

  @Test
  public void testRecordRefreshFailureActivatesCircuitBreaker() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);

    // First 4 failures should not activate circuit breaker
    for (int i = 0; i < 4; i++) {
      state.recordRefreshFailure("test-queue", "failure " + i);
      assertFalse("Circuit breaker should not activate yet at failure " + i,
          state.shouldSkipDueToCircuitBreaker("test-queue"));
    }

    // 5th failure activates circuit breaker
    state.recordRefreshFailure("test-queue", "failure 5");
    assertTrue("Circuit breaker should activate after 5 failures",
        state.shouldSkipDueToCircuitBreaker("test-queue"));
  }

  @Test
  public void testCircuitBreakerAllowsProbeEvery10Ticks() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);

    // Activate circuit breaker
    for (int i = 0; i < 5; i++) {
      state.recordRefreshFailure("test-queue", "failure " + i);
    }

    // First few ticks should be blocked
    int blockedCount = 0;
    int allowedCount = 0;

    for (int tick = 1; tick <= 20; tick++) {
      if (state.shouldSkipDueToCircuitBreaker("test-queue")) {
        blockedCount++;
      } else {
        allowedCount++;
      }
    }

    // Should allow approximately 2 probes in 20 ticks (ticks 10 and 20)
    assertTrue("Should have some blocked ticks", blockedCount > 10);
    assertTrue("Should have some allowed probes", allowedCount >= 1 && allowedCount <= 3);
  }

  @Test
  public void testShouldSkipDueToCircuitBreakerReturnsFalseWhenHealthy() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);

    // No failures recorded
    assertFalse("Circuit breaker should not skip when healthy",
        state.shouldSkipDueToCircuitBreaker("test-queue"));
  }
}





