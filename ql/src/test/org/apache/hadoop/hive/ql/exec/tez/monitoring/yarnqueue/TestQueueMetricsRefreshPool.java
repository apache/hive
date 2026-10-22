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

import org.junit.After;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Test cases for QueueMetricsRefreshPool.
 */
public class TestQueueMetricsRefreshPool {

  @After
  public void tearDown() {
    // Shutdown singleton after each test for isolation
    QueueMetricsRefreshPool.shutdown();
  }

  @Test
  public void testInitWithCustomThreadCount() {
    QueueMetricsRefreshPool.init(8);
    QueueMetricsRefreshPool pool = QueueMetricsRefreshPool.getInstance();

    assertNotNull("Pool should be initialized", pool);
    assertEquals("Thread count should match configured value", 8, pool.getThreadCount());
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
  public void testGetInstanceWithoutInitLazilyInitializes() {
    // Don't call init(), directly call getInstance()
    QueueMetricsRefreshPool pool = QueueMetricsRefreshPool.getInstance();

    assertNotNull("Pool should be lazily initialized", pool);
    assertEquals("Default thread count should be 4", 4, pool.getThreadCount());
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

    assertEquals("Same queue name should produce same jitter",
        QueueMetricsRefreshPool.calculateJitter(queueName, intervalMs),
        QueueMetricsRefreshPool.calculateJitter(queueName, intervalMs));
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

    // While theoretically they could be equal, hash collisions are rare enough
    // that this test is reliable in practice
    assertNotEquals("Different queues should produce different jitter values",
        QueueMetricsRefreshPool.calculateJitter("queue-alpha", intervalMs),
        QueueMetricsRefreshPool.calculateJitter("queue-beta", intervalMs));
  }

  @Test
  public void testCalculateJitterWithDifferentIntervals() {
    String queueName = "test-queue";

    assertTrue("Jitter for 5s should be <= 500ms",
        QueueMetricsRefreshPool.calculateJitter(queueName, 5000L) <= 500L);
    assertTrue("Jitter for 10s should be <= 1000ms",
        QueueMetricsRefreshPool.calculateJitter(queueName, 10000L) <= 1000L);
  }

  @Test
  public void testScheduleRefreshTaskExecutesTask() throws Exception {
    QueueMetricsRefreshPool.init(2);
    QueueMetricsRefreshPool pool = QueueMetricsRefreshPool.getInstance();

    CountDownLatch latch = new CountDownLatch(2);

    ScheduledFuture<?> future = pool.scheduleRefreshTask(latch::countDown, 50L);

    assertNotNull("Scheduled future should not be null", future);
    assertTrue("Task should execute at least twice", latch.await(500, TimeUnit.MILLISECONDS));

    future.cancel(false);
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
    assertTrue("Jitter should vary across different queue names",
        Arrays.stream(jitters).distinct().count() > 1);
  }

  @Test
  public void testCalculateJitterWithZeroInterval() {
    // Test edge case where jitterWindow becomes 0 (very small interval)
    // 5ms interval -> jitterWindow = 0 (5 * 10 / 100 = 0)
    assertEquals("Jitter should be 0 when jitterWindow is 0",
        0, QueueMetricsRefreshPool.calculateJitter("test-queue", 5L));
  }

  @Test
  public void testCalculateJitterWithNegativeInterval() {
    // Test edge case with negative interval
    assertEquals("Jitter should be 0 when intervalMs is negative",
        0, QueueMetricsRefreshPool.calculateJitter("test-queue", -1000L));
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
    assertTrue("Jitter should be <= 10% of interval",
        jitter < intervalMs * QueueMetricsRefreshPool.JITTER_PERCENT / 100);
  }

  @Test
  public void testCalculateJitterAlwaysNonNegative() {
    // Test with various queue names to ensure jitter is always non-negative
    String[] testQueues = {
        "queue-1", "queue-2", "production", "default",
        "test-queue-alpha", "test-queue-beta"
    };
    long intervalMs = 10000L;

    for (String queueName : testQueues) {
      long jitter = QueueMetricsRefreshPool.calculateJitter(queueName, intervalMs);
      assertTrue("Jitter for queue '" + queueName + "' should be >= 0, but was: " + jitter,
          jitter >= 0);
      long maxJitter = intervalMs * QueueMetricsRefreshPool.JITTER_PERCENT / 100;
      assertTrue("Jitter for queue '" + queueName + "' should be < " + maxJitter,
          jitter < maxJitter);
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

    assertEquals("No new task executions should occur after shutdown",
        countAtShutdown, counter.get());
  }
}

