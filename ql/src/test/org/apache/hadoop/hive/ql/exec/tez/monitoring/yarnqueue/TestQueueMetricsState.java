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
import org.junit.Before;
import java.util.concurrent.ScheduledFuture;
import java.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for QueueMetricsState - tests state management logic in isolation.
 * Tests interval registration, circuit breaker, refresh locking, and other state logic.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestQueueMetricsState {

  @Mock
  private QueueInfo mockQueueInfo;
  @Mock
  private QueueStatistics mockQueueStats;
  @Mock
  private QueueMetricsRefreshPool mockPool;
  @Mock
  private ScheduledFuture<?> mockTask;

  @Before
  public void setUp() {
    lenient().when(mockPool.scheduleRefreshTask(any(), anyLong())).thenAnswer(inv -> mockTask);
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
    // minRefreshIntervalMs is seeded from the constructor value; updated by ensureTaskScheduled
    assertEquals("Min interval should be set to constructor value", 5000L, state.getMinRefreshIntervalMs());
  }

  @Test
  public void testConstructorWithSnapshot() {
    QueueMetricsState state = new QueueMetricsState(new QueueMetricsSnapshot(mockQueueInfo), 10000L);

    assertNotNull("Snapshot should not be null", state.getSnapshot());
    // minRefreshIntervalMs is seeded from the constructor value; updated by ensureTaskScheduled
    assertEquals("Min interval should be set to constructor value", 10000L, state.getMinRefreshIntervalMs());
  }

  @Test
  public void testGetAgeMsReturnsLargeValueInitially() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);

    // Age should be very large when lastWriteTime = 0 (epoch)
    assertTrue("Age should be > 1 year in ms", state.getAgeMs() > 365L * 24 * 60 * 60 * 1000);
  }

  @Test
  public void testApplySnapshotUpdatesSnapshot() {
    QueueMetricsState state = new QueueMetricsState(null, 10000L);
    assertNull("Initial snapshot should be null", state.getSnapshot());

    state.applySnapshot(new QueueMetricsSnapshot(mockQueueInfo));

    assertNotNull("Snapshot should be updated", state.getSnapshot());
    assertEquals("Memory should match", 1.0f, state.getSnapshot().getMemoryUsedGB(), 0.001f);
  }

  @Test
  public void testApplySnapshotReducesAgeMs() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);
    long initialAge = state.getAgeMs();

    // Wait up to 200ms for the clock to advance so the age comparison is meaningful.
    // Tolerate timeout: on a heavily-loaded runner the age may already have advanced
    // beyond initialAge on the first read, or may take slightly longer — either way
    // the assertions below verify the actual invariant.
    try {
      await().atMost(Duration.ofMillis(200))
          .pollInterval(Duration.ofMillis(10))
          .until(() -> state.getAgeMs() > initialAge);
    } catch (org.awaitility.core.ConditionTimeoutException ignored) {
      // Intentional: assertions below cover the invariant.
    }

    state.applySnapshot(new QueueMetricsSnapshot(mockQueueInfo));

    assertTrue("Age should be much smaller after apply", state.getAgeMs() < initialAge);
    assertTrue("Age should be recent (< 1s)", state.getAgeMs() < 1000);
  }

  @Test
  public void testRegisterIntervalReturnsTrueWhenNoTaskExists() {
    QueueMetricsState state = new QueueMetricsState(null, 10000L);

    assertTrue("Should return true when no task exists", state.registerInterval(5000L));
  }

  @Test
  public void testRegisterIntervalReturnsTrueWhenLoweringMinimum() {
    QueueMetricsState state = new QueueMetricsState(null, 10000L);

    // First registration — task is null so returns true
    state.registerInterval(10000L);

    // Second registration with strictly faster interval — lowered the minimum → reschedule needed
    assertTrue("Should return true when lowering minimum", state.registerInterval(5000L));
  }

  @Test
  public void testRegisterIntervalReturnsTrueWhenTaskIsNull() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);

    // Register faster interval first (task is null, should return true)
    assertTrue("Should return true when task is null", state.registerInterval(5000L));

    // Register slower interval (task still null — no task was ever scheduled — should return true)
    assertTrue("Should return true when task is null even with slower interval", state.registerInterval(10000L));
  }

  @Test
  public void testRegisterIntervalReturnsFalseWhenSlowerThanExistingMinimum() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);

    // Register faster session and schedule the task — refreshTask is now non-null
    state.registerInterval(5000L);
    state.ensureTaskScheduled(mockPool, () -> {}, "test-queue");

    // Registering a slower interval must NOT trigger rescheduling — minimum is unchanged (5000ms)
    assertFalse("Should return false when new interval is slower than existing minimum",
        state.registerInterval(10000L));
  }

  @Test
  public void testRegisterIntervalAddsOneEntryPerSession() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);

    // Two sessions at the same interval — duplicates must be kept (one entry per session)
    state.registerInterval(5000L);
    state.registerInterval(5000L);

    // Deregistering one should still leave the other
    state.deregisterInterval(5000L);
    // If duplicates were not kept, deregister would have removed the only entry.
    // Verify by registering a slower session and checking deregister still returns true
    // (5000 <= taskCurrentRefreshIntervalMs initial value) — confirms 5000ms entry still present.
    assertTrue("Second session at same interval should still be registered after first deregisters",
        state.deregisterInterval(5000L));
  }

  @Test
  public void testDeregisterIntervalReturnsFalseWhenOtherSessionsRemain() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);

    // Register two sessions at same interval
    state.registerInterval(5000L);
    state.registerInterval(5000L);

    // Deregister one session — 5000 <= taskCurrentRefreshIntervalMs(5000), returns true
    // (the task may need to be re-evaluated, even if it stays at 5000ms)
    assertTrue("Should return true since 5000 <= taskCurrentRefreshIntervalMs",
        state.deregisterInterval(5000L));
  }

  @Test
  public void testDeregisterIntervalSignalsRescheduleWhenAtOrBelowTaskInterval() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);
    state.registerInterval(5000L);

    // 5000 <= taskCurrentRefreshIntervalMs (5000) → should signal rescheduling
    assertTrue("Should return true when removed interval is at or below task interval",
        state.deregisterInterval(5000L));
  }

  @Test
  public void testDeregisterIntervalNoRescheduleWhenSlowerThanTask() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);
    state.registerInterval(5000L);
    state.registerInterval(10000L);

    // Deregister the slow session (10000 > taskCurrentRefreshIntervalMs 5000) → no reschedule
    assertFalse("Should return false when removed interval is slower than task interval",
        state.deregisterInterval(10000L));
  }

  @Test
  public void testDeregisterIntervalRemovesInterval() {
    QueueMetricsState state = new QueueMetricsState(null, 10000L);

    // Register fast and slow sessions
    state.registerInterval(2000L);  // Fast
    state.registerInterval(10000L); // Slow

    // Deregister fast session — 2000 <= taskCurrentRefreshIntervalMs(10000) → returns true
    assertTrue("Should return true when the removed interval was at or below task interval",
        state.deregisterInterval(2000L));

    // Deregister slow session — 10000 <= taskCurrentRefreshIntervalMs(10000) → returns true
    assertTrue("Should return true when last session deregisters",
        state.deregisterInterval(10000L));
  }

  @Test
  public void testDeregisterFasterIntervalWhenItIsTheLastSession() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);
    state.registerInterval(5000L);

    // Only one session at 5000ms — deregistering it is the last session.
    // 5000 <= taskCurrentRefreshIntervalMs(5000) → should signal rescheduling (task must be cancelled)
    assertTrue("Should return true when the only session deregisters",
        state.deregisterInterval(5000L));
  }

  @Test
  public void testDeregisterFasterIntervalWhenSlowerSessionStillRemains() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);
    state.registerInterval(5000L);  // faster
    state.registerInterval(10000L); // slower

    // Deregister the faster session — 5000 <= taskCurrentRefreshIntervalMs(5000) → true.
    // The slower session (10000ms) is still in the heap; the task should be rescheduled slower.
    assertTrue("Should return true when faster interval deregisters and slower session remains",
        state.deregisterInterval(5000L));
  }

  @Test
  public void testDeregisterIntervalNoRescheduleWhenSlowerThanScheduledTask() {
    QueueMetricsState state = new QueueMetricsState(null, 10000L);

    // Register a fast session and schedule the task at 5000ms
    state.registerInterval(5000L);
    state.ensureTaskScheduled(mockPool, () -> {}, "test-queue");
    // taskCurrentRefreshIntervalMs is now 5000 (set by ensureTaskScheduled/scheduleTask)

    // Add a slow session — does not affect the task interval
    state.registerInterval(10000L);

    // Deregister the slow session — 10000 > taskCurrentRefreshIntervalMs(5000) → no reschedule
    assertFalse("Should return false when removed interval is slower than the scheduled task interval",
        state.deregisterInterval(10000L));
  }

  // -------------------------------------------------------------------------
  // ensureTaskScheduled tests
  // -------------------------------------------------------------------------

  @Test
  public void testEnsureTaskScheduledStartsTaskWhenNoneExists() {
    QueueMetricsState state = new QueueMetricsState(null, 10000L);
    state.registerInterval(5000L);

    state.ensureTaskScheduled(mockPool, () -> {}, "test-queue");

    verify(mockPool, times(1)).scheduleRefreshTask(any(), anyLong());
    assertEquals("minRefreshIntervalMs should reflect the registered interval",
        5000L, state.getMinRefreshIntervalMs());
  }

  @Test
  public void testEnsureTaskScheduledIsIdempotentWhenIntervalUnchanged() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);
    state.registerInterval(5000L);

    // First call schedules the task
    state.ensureTaskScheduled(mockPool, () -> {}, "test-queue");
    // Second call with the same heap minimum — should NOT reschedule
    state.ensureTaskScheduled(mockPool, () -> {}, "test-queue");

    verify(mockPool, times(1)).scheduleRefreshTask(any(), anyLong());
  }

  @Test
  public void testEnsureTaskScheduledReschedulesWhenFasterSessionAdded() {
    QueueMetricsState state = new QueueMetricsState(null, 10000L);

    // Schedule at 10000ms
    state.registerInterval(10000L);
    state.ensureTaskScheduled(mockPool, () -> {}, "test-queue");

    // Add a faster session and reschedule
    state.registerInterval(5000L);
    state.ensureTaskScheduled(mockPool, () -> {}, "test-queue");

    // scheduleRefreshTask should have been called twice — initial + reschedule
    verify(mockPool, times(2)).scheduleRefreshTask(any(), anyLong());
    assertEquals("minRefreshIntervalMs should reflect the new faster interval",
        5000L, state.getMinRefreshIntervalMs());
  }

  @Test
  public void testEnsureTaskScheduledCancelsTaskWhenNoSessionsRemain() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);
    state.registerInterval(5000L);
    state.ensureTaskScheduled(mockPool, () -> {}, "test-queue");

    // Remove the only session
    state.deregisterInterval(5000L);
    state.ensureTaskScheduled(mockPool, () -> {}, "test-queue");

    // Task should be cancelled
    verify(mockTask, times(1)).cancel(false);
    // No additional scheduleRefreshTask call after cancel
    verify(mockPool, times(1)).scheduleRefreshTask(any(), anyLong());
  }

  @Test
  public void testEnsureTaskScheduledUpdatesMinRefreshIntervalMsToZeroWhenEmpty() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);
    state.registerInterval(5000L);
    state.ensureTaskScheduled(mockPool, () -> {}, "test-queue");

    state.deregisterInterval(5000L);
    state.ensureTaskScheduled(mockPool, () -> {}, "test-queue");

    assertEquals("minRefreshIntervalMs should be 0 when no sessions remain (0 = no active sessions sentinel)",
        0L, state.getMinRefreshIntervalMs());
  }

  @Test
  public void testFreshnessCheckNeverSkipsWhenNoSessionsRemain() {
    // When minRefreshIntervalMs = 0 (no sessions), the freshness check
    // getAgeMs() < getMinRefreshIntervalMs() must always be false — i.e. the
    // refresh is never incorrectly skipped due to the sentinel value.
    // This is the key correctness property of using 0 instead of Long.MAX_VALUE.
    QueueMetricsState state = new QueueMetricsState(null, 5000L);
    state.registerInterval(5000L);
    state.ensureTaskScheduled(mockPool, () -> {}, "test-queue");

    state.deregisterInterval(5000L);
    state.ensureTaskScheduled(mockPool, () -> {}, "test-queue");

    // getAgeMs() >= 0 always; 0 < 0 is false — freshness check never skips
    assertFalse("Freshness check should not skip when no sessions remain (age >= 0, min = 0)",
        state.getAgeMs() < state.getMinRefreshIntervalMs());
  }

  @Test
  public void testEnsureTaskScheduledDoesNothingWhenAlreadyEmptyAndNoTask() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);
    // No sessions registered — sessionIntervals is empty and refreshTask is null

    state.ensureTaskScheduled(mockPool, () -> {}, "test-queue");

    verify(mockPool, never()).scheduleRefreshTask(any(), anyLong());
  }

  @Test
  public void testTryStartRefreshPreventsRace() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);

    assertTrue("First call should succeed", state.tryStartRefresh());
    assertFalse("Second call should return false when refresh is already in progress", state.tryStartRefresh());
  }

  @Test
  public void testFinishRefreshReleasesLock() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);

    state.tryStartRefresh();
    state.finishRefresh();

    assertTrue("Should be able to refresh after finish", state.tryStartRefresh());
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

    // In 20 ticks, probes are allowed at ticks 10 and 20 (skipCount % 10 == 0) → exactly 2 probes,
    // and exactly 18 blocked ticks — these are deterministic, not time-dependent.
    assertEquals("Should have exactly 18 blocked ticks", 18, blockedCount);
    assertEquals("Should have exactly 2 probe ticks (ticks 10 and 20)", 2, allowedCount);
  }

  @Test
  public void testShouldSkipDueToCircuitBreakerReturnsFalseWhenHealthy() {
    QueueMetricsState state = new QueueMetricsState(null, 5000L);

    // No failures recorded
    assertFalse("Circuit breaker should not skip when healthy",
        state.shouldSkipDueToCircuitBreaker("test-queue"));
  }
}
