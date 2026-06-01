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

import org.junit.After;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Test cases for QueueMetricsRefreshPool.
 */
public class TestQueueMetricsRefreshPool {

  @After
  public void tearDown() {
    // Reset singleton after each test for isolation
    QueueMetricsRefreshPool.resetForTesting();
  }

  @Test
  public void testInitWithCustomThreadCount() {
    QueueMetricsRefreshPool.init(8);
    QueueMetricsRefreshPool pool = QueueMetricsRefreshPool.getInstance();

    assertNotNull("Pool should be initialized", pool);
  }

  @Test
  public void testInitCalledTwiceIgnoresSecondCall() {
    QueueMetricsRefreshPool.init(4);
    QueueMetricsRefreshPool pool1 = QueueMetricsRefreshPool.getInstance();

    QueueMetricsRefreshPool.init(10); // Second init should be ignored
    QueueMetricsRefreshPool pool2 = QueueMetricsRefreshPool.getInstance();

    assertSame("Should return same instance", pool1, pool2);
  }

  @Test
  public void testGetInstanceWithoutInitUsesDefaultThreadCount() {
    // Don't call init(), directly call getInstance()
    QueueMetricsRefreshPool pool = QueueMetricsRefreshPool.getInstance();

    assertNotNull("Pool should be lazily initialized", pool);
  }

  @Test
  public void testGetInstanceReturnsSameInstance() {
    QueueMetricsRefreshPool pool1 = QueueMetricsRefreshPool.getInstance();
    QueueMetricsRefreshPool pool2 = QueueMetricsRefreshPool.getInstance();

    assertSame("getInstance should return same singleton", pool1, pool2);
  }

  @Test
  public void testCalculateJitterIsDeterministic() {
    String queueName = "test-queue";
    long intervalMs = 10000L;

    long jitter1 = QueueMetricsRefreshPool.calculateJitter(queueName, intervalMs);
    long jitter2 = QueueMetricsRefreshPool.calculateJitter(queueName, intervalMs);

    assertEquals("Same queue name should produce same jitter", jitter1, jitter2);
  }

  @Test
  public void testCalculateJitterIsWithinRange() {
    String queueName = "production-queue";
    long intervalMs = 10000L;
    long expectedMaxJitter = intervalMs * QueueMetricsRefreshPool.JITTER_PERCENT / 100;

    long jitter = QueueMetricsRefreshPool.calculateJitter(queueName, intervalMs);

    assertTrue("Jitter should be >= 0", jitter >= 0);
    assertTrue("Jitter should be <= 10% of interval (1000ms)", jitter <= expectedMaxJitter);
  }

  @Test
  public void testCalculateJitterDifferentQueuesProduceDifferentValues() {
    long intervalMs = 10000L;
    String queue1 = "queue-alpha";
    String queue2 = "queue-beta";

    long jitter1 = QueueMetricsRefreshPool.calculateJitter(queue1, intervalMs);
    long jitter2 = QueueMetricsRefreshPool.calculateJitter(queue2, intervalMs);

    // While theoretically they could be equal, hash collisions are rare enough
    // that this test is reliable in practice
    assertNotEquals("Different queues should produce different jitter values", jitter1, jitter2);
  }

  @Test
  public void testCalculateJitterWithDifferentIntervals() {
    String queueName = "test-queue";

    long jitter5s = QueueMetricsRefreshPool.calculateJitter(queueName, 5000L);
    long jitter10s = QueueMetricsRefreshPool.calculateJitter(queueName, 10000L);

    assertTrue("Jitter for 5s should be <= 500ms", jitter5s <= 500L);
    assertTrue("Jitter for 10s should be <= 1000ms", jitter10s <= 1000L);
  }

  @Test
  public void testScheduleRefreshTaskExecutesTask() throws Exception {
    QueueMetricsRefreshPool.init(2);
    QueueMetricsRefreshPool pool = QueueMetricsRefreshPool.getInstance();

    CountDownLatch latch = new CountDownLatch(2);
    AtomicInteger executionCount = new AtomicInteger(0);

    Runnable task = () -> {
      executionCount.incrementAndGet();
      latch.countDown();
    };

    ScheduledFuture<?> future = pool.scheduleRefreshTask(task, 50L);

    assertNotNull("Scheduled future should not be null", future);
    assertTrue("Task should execute at least twice", latch.await(500, TimeUnit.MILLISECONDS));
    assertTrue("Execution count should be >= 2", executionCount.get() >= 2);

    future.cancel(false);
  }

  @Test
  public void testScheduleRefreshTaskWithInitialDelayZero() throws Exception {
    QueueMetricsRefreshPool.init(1);
    QueueMetricsRefreshPool pool = QueueMetricsRefreshPool.getInstance();

    CountDownLatch firstExecutionLatch = new CountDownLatch(1);
    long startTime = System.currentTimeMillis();

    Runnable task = firstExecutionLatch::countDown;

    ScheduledFuture<?>future = pool.scheduleRefreshTask(task, 100L);

    assertTrue("First execution should happen quickly", firstExecutionLatch.await(200, TimeUnit.MILLISECONDS));
    long firstExecutionTime = System.currentTimeMillis() - startTime;
    assertTrue("Initial delay should be ~0 (< 200ms)", firstExecutionTime < 200);

    future.cancel(false);
  }


  @Test
  public void testResetForTestingShutdownsAndNullsInstance() {
    QueueMetricsRefreshPool.init(4);
    QueueMetricsRefreshPool pool1 = QueueMetricsRefreshPool.getInstance();
    assertNotNull("Pool should exist", pool1);

    QueueMetricsRefreshPool.resetForTesting();

    // After reset, getInstance should create a new instance
    QueueMetricsRefreshPool pool2 = QueueMetricsRefreshPool.getInstance();
    assertNotNull("New pool should be created", pool2);
    assertNotSame("Should be different instance after reset", pool1, pool2);
  }

  @Test
  public void testResetForTestingIsIdempotent() {
    QueueMetricsRefreshPool.resetForTesting();
    QueueMetricsRefreshPool.resetForTesting(); // Second call should not throw
    QueueMetricsRefreshPool.resetForTesting(); // Third call should not throw

    // Should still be able to get instance
    QueueMetricsRefreshPool pool = QueueMetricsRefreshPool.getInstance();
    assertNotNull("Pool should be available after multiple resets", pool);
  }

  @Test
  public void testScheduleMultipleTasksConcurrently() throws Exception {
    QueueMetricsRefreshPool.init(4);
    QueueMetricsRefreshPool pool = QueueMetricsRefreshPool.getInstance();

    int taskCount = 5;
    CountDownLatch latch = new CountDownLatch(taskCount * 2); // Each task should run at least twice
    AtomicInteger[] counters = new AtomicInteger[taskCount];
    ScheduledFuture<?>[] futures = new ScheduledFuture[taskCount];

    for (int i = 0; i < taskCount; i++) {
      counters[i] = new AtomicInteger(0);
      final int taskId = i;
      futures[i] = pool.scheduleRefreshTask(() -> {
        counters[taskId].incrementAndGet();
        latch.countDown();
      }, 50L);
    }

    assertTrue("All tasks should execute multiple times", latch.await(1, TimeUnit.SECONDS));

    // Cancel all tasks
    for (ScheduledFuture<?> future : futures) {
      future.cancel(false);
    }

    // Verify all tasks executed at least once
    for (int i = 0; i < taskCount; i++) {
      assertTrue("Task " + i + " should have executed", counters[i].get() >= 2);
    }
  }

  @Test
  public void testJitterPreventsSynchronization() {
    // Test that jitter would prevent thundering herd
    String[] queues = {"q1", "q2", "q3", "q4", "q5"};
    long intervalMs = 10000L;

    long[] jitters = new long[queues.length];
    for (int i = 0; i < queues.length; i++) {
      jitters[i] = QueueMetricsRefreshPool.calculateJitter(queues[i], intervalMs);
    }

    // Check that not all jitters are the same (spreading effect)
    boolean hasDifferentJitter = false;
    for (int i = 1; i < jitters.length; i++) {
      if (jitters[i] != jitters[0]) {
        hasDifferentJitter = true;
        break;
      }
    }

    assertTrue("Jitter should vary across different queue names", hasDifferentJitter);
  }

  @Test
  public void testCalculateJitterWithZeroInterval() {
    // Test edge case where jitterWindow becomes 0 (very small interval)
    String queueName = "test-queue";
    long smallInterval = 5L; // 5ms interval -> jitterWindow = 0 (5 * 10 / 100 = 0)

    long jitter = QueueMetricsRefreshPool.calculateJitter(queueName, smallInterval);

    assertEquals("Jitter should be 0 when jitterWindow is 0", 0, jitter);
  }

  @Test
  public void testCalculateJitterWithNegativeInterval() {
    // Test edge case with negative interval
    String queueName = "test-queue";
    long negativeInterval = -1000L;

    long jitter = QueueMetricsRefreshPool.calculateJitter(queueName, negativeInterval);

    assertEquals("Jitter should be 0 when intervalMs is negative", 0, jitter);
  }

  @Test
  public void testCalculateJitterWithHashCodeIntMinValue() {
    // Test edge case where queue name generates Integer.MIN_VALUE hash
    // We need to find a string that produces Integer.MIN_VALUE hashCode
    // "polygenelubricants" is known to produce Integer.MIN_VALUE hashCode
    String specialQueue = "polygenelubricants";
    long intervalMs = 10000L;

    // Verify the queue name indeed produces Integer.MIN_VALUE
    assertEquals("Test string should produce Integer.MIN_VALUE", Integer.MIN_VALUE, specialQueue.hashCode());

    long jitter = QueueMetricsRefreshPool.calculateJitter(specialQueue, intervalMs);

    // Verify jitter is non-negative and within bounds
    assertTrue("Jitter should be >= 0 even for Integer.MIN_VALUE hashCode", jitter >= 0);
    assertTrue("Jitter should be <= 10% of interval", jitter < intervalMs * QueueMetricsRefreshPool.JITTER_PERCENT / 100);
  }

  @Test
  public void testCalculateJitterAlwaysNonNegative() {
    // Test with various queue names to ensure jitter is always non-negative
    String[] testQueues = {
        "queue-1", "queue-2", "production", "default",
        "polygenelubricants", // Integer.MIN_VALUE
        "test-queue-alpha", "test-queue-beta"
    };
    long intervalMs = 10000L;

    for (String queueName : testQueues) {
      long jitter = QueueMetricsRefreshPool.calculateJitter(queueName, intervalMs);
      assertTrue("Jitter for queue '" + queueName + "' should be >= 0, but was: " + jitter,
          jitter >= 0);
      assertTrue("Jitter for queue '" + queueName + "' should be < " + (intervalMs * QueueMetricsRefreshPool.JITTER_PERCENT / 100),
          jitter < intervalMs * QueueMetricsRefreshPool.JITTER_PERCENT / 100);
    }
  }

  @Test
  public void testShutdownNullsInstance() {
    QueueMetricsRefreshPool.init(2);
    assertNotNull("Pool should be initialized before shutdown", QueueMetricsRefreshPool.getInstance());

    QueueMetricsRefreshPool.shutdown();

    // After shutdown the singleton is cleared; getInstance() must create a fresh pool
    QueueMetricsRefreshPool newPool = QueueMetricsRefreshPool.getInstance();
    assertNotNull("A new pool should be lazily created after shutdown", newPool);
  }

  @Test
  public void testShutdownIsIdempotent() {
    QueueMetricsRefreshPool.init(2);

    // Calling shutdown multiple times must not throw
    QueueMetricsRefreshPool.shutdown();
    QueueMetricsRefreshPool.shutdown();
    QueueMetricsRefreshPool.shutdown();

    // Pool must still be obtainable afterwards
    assertNotNull("Pool should be available after multiple shutdowns",
        QueueMetricsRefreshPool.getInstance());
  }

  @Test
  public void testShutdownStopsScheduledTasks() throws Exception {
    QueueMetricsRefreshPool.init(2);
    QueueMetricsRefreshPool pool = QueueMetricsRefreshPool.getInstance();

    AtomicInteger counter = new AtomicInteger(0);
    // Wait for at least two executions so the task is clearly running before we shut down
    CountDownLatch twoExecutions = new CountDownLatch(2);

    pool.scheduleRefreshTask(() -> {
      counter.incrementAndGet();
      twoExecutions.countDown();
    }, 50L);

    assertTrue("Task should execute at least twice before shutdown",
        twoExecutions.await(2, TimeUnit.SECONDS));
    int countAtShutdown = counter.get();

    // shutdown() blocks until all threads have terminated (awaitTermination),
    // so when it returns no further executions can happen.
    QueueMetricsRefreshPool.shutdown();

    int countAfterShutdown = counter.get();
    assertTrue("No new task executions should occur after shutdown",
        countAfterShutdown <= countAtShutdown + 1);
  }
}

