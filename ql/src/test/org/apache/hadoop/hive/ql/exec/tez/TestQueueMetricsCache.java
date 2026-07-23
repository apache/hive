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
import org.apache.hadoop.hive.ql.exec.tez.monitoring.yarnqueue.QueueMetricsSnapshot;
import org.apache.hadoop.hive.ql.exec.tez.monitoring.yarnqueue.QueueMetricsState;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Test cases for QueueMetricsCache.
 */
public class TestQueueMetricsCache {

  @Mock
  private QueueInfo mockQueueInfo;

  @Mock
  private QueueStatistics mockQueueStats;

  private AutoCloseable closeable;
  private QueueMetricsCache cache;

  @Before
  public void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    cache = QueueMetricsCache.getInstance();
    // Note: We can't fully clear the cache between tests since it's a singleton,
    // but we use unique queue names per test to avoid interference
  }

  @After
  public void tearDown() throws Exception {
    if (closeable != null) {
      closeable.close();
    }
  }

  @Test
  public void testSingletonInstanceConsistency() {
    QueueMetricsCache instance1 = QueueMetricsCache.getInstance();
    QueueMetricsCache instance2 = QueueMetricsCache.getInstance();

    assertNotNull("Instance should not be null", instance1);
    assertSame("getInstance should return same instance", instance1, instance2);
  }

  @Test
  public void testGetReturnsNullForNonExistentQueue() {
    String nonExistentQueue = "test-nonexistent-" + System.nanoTime();
    QueueMetricsState state = cache.get(nonExistentQueue);

    assertNull("Should return null for non-existent queue", state);
  }

  @Test
  public void testGetReturnsNullForNullQueueName() {
    QueueMetricsState state = cache.get(null);

    assertNull("Should return null for null queue name", state);
  }

  @Test
  public void testPutCreatesNewEntry() {
    setupMockQueueInfo();
    String queueName = "test-new-entry-" + System.nanoTime();
    QueueMetricsSnapshot snapshot = new QueueMetricsSnapshot(mockQueueInfo);

    // Verify queue doesn't exist yet
    assertNull("Queue should not exist initially", cache.get(queueName));

    // Put creates new entry
    cache.put(queueName, snapshot, 5000L);

    QueueMetricsState state = cache.get(queueName);
    assertNotNull("Queue state should exist after put", state);
    assertNotNull("Snapshot should be available", state.getSnapshot());
    assertEquals("Memory used should match", 1.0f, state.getSnapshot().getMemoryUsedGB(), 0.01f);
  }

  @Test
  public void testPutUpdatesExistingEntry() {
    setupMockQueueInfo();
    String queueName = "test-update-entry-" + System.nanoTime();

    // Create initial entry
    QueueMetricsSnapshot snapshot1 = new QueueMetricsSnapshot(mockQueueInfo);
    cache.put(queueName, snapshot1, 5000L);

    // Update with new snapshot
    when(mockQueueStats.getAllocatedMemoryMB()).thenReturn(4096L);
    QueueMetricsSnapshot snapshot2 = new QueueMetricsSnapshot(mockQueueInfo);
    cache.put(queueName, snapshot2, 3000L);

    QueueMetricsState state = cache.get(queueName);
    assertNotNull("State should exist", state);
    assertEquals("Memory should be updated", 4.0f, state.getSnapshot().getMemoryUsedGB(), 0.01f);
  }

  @Test
  public void testPutWithNullQueueNameIsNoOp() {
    setupMockQueueInfo();
    QueueMetricsSnapshot snapshot = new QueueMetricsSnapshot(mockQueueInfo);

    // Should not throw exception
    cache.put(null, snapshot, 5000L);
    // Confirms null queue name was silently ignored - no entry created
    assertNull("Null queue name should not create a cache entry", cache.get(null));
  }

  @Test
  public void testPutWithNullSnapshotIsNoOp() {
    String queueName = "test-null-snapshot-" + System.nanoTime();

    // Should not throw exception
    cache.put(queueName, null, 5000L);

    // Queue should not be created
    assertNull("Queue should not exist after put with null snapshot", cache.get(queueName));
  }

  @Test
  public void testPutPlaceholderCreatesEmptyEntry() {
    String queueName = "test-placeholder-" + System.nanoTime();

    QueueMetricsState state = cache.putPlaceholder(queueName, 10000L);

    assertNotNull("Placeholder state should be created", state);
    assertNull("Snapshot should be null initially", state.getSnapshot());
    assertEquals("Min interval should match", 10000L, state.getMinRefreshIntervalMs());
  }

  @Test
  public void testPutPlaceholderWithNullQueueName() {
    QueueMetricsState state = cache.putPlaceholder(null, 5000L);

    assertNull("Should return null for null queue name", state);
  }

  @Test
  public void testConcurrentPutPlaceholderRaces() throws Exception {
    String queueName = "test-concurrent-placeholder-" + System.nanoTime();
    int threadCount = 10;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threadCount);
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);

    ConcurrentHashMap<Integer, QueueMetricsState> results = new ConcurrentHashMap<>();

    // Launch threads that all try to create placeholder simultaneously
    for (int i = 0; i < threadCount; i++) {
      final int threadId = i;
      executor.submit(() -> {
        try {
          startLatch.await(); // Wait for signal to start
          QueueMetricsState state = cache.putPlaceholder(queueName, 5000L + threadId * 100);
          results.put(threadId, state);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          doneLatch.countDown();
        }
      });
    }

    // Start all threads at once
    startLatch.countDown();

    // Wait for completion
    assertTrue("Threads should complete", doneLatch.await(5, TimeUnit.SECONDS));
    executor.shutdown();

    // Verify all threads got the same state instance (putIfAbsent semantics)
    QueueMetricsState firstState = results.get(0);
    assertNotNull("First state should exist", firstState);

    for (int i = 1; i < threadCount; i++) {
      assertSame("All threads should get same state instance", firstState, results.get(i));
    }

    // Verify only one entry exists in cache
    QueueMetricsState cachedState = cache.get(queueName);
    assertSame("Cached state should match returned state", firstState, cachedState);
  }

  @Test
  public void testGetActiveQueueCount() {
    int initialCount = cache.getActiveQueueCount();
    String queueName1 = "test-count-1-" + System.nanoTime();
    String queueName2 = "test-count-2-" + System.nanoTime();

    cache.putPlaceholder(queueName1, 5000L);
    assertEquals("Count should increase by 1", initialCount + 1, cache.getActiveQueueCount());

    cache.putPlaceholder(queueName2, 5000L);
    assertEquals("Count should increase by 2", initialCount + 2, cache.getActiveQueueCount());
  }


  @Test
  public void testShutdownDoesNotThrow() {
    // Should handle gracefully even if called multiple times
    cache.shutdown();
    cache.shutdown(); // Second call should also be safe
    // After shutdown all entries should be cleared
    assertEquals("Cache should be empty after shutdown", 0, cache.getActiveQueueCount());
  }

  @Test
  public void testPutPlaceholderThenPutUpdatesSnapshot() {
    setupMockQueueInfo();
    String queueName = "test-placeholder-update-" + System.nanoTime();

    // Create placeholder first
    QueueMetricsState state1 = cache.putPlaceholder(queueName, 10000L);
    assertNull("Snapshot should be null initially", state1.getSnapshot());

    // Now put actual snapshot
    QueueMetricsSnapshot snapshot = new QueueMetricsSnapshot(mockQueueInfo);
    cache.put(queueName, snapshot, 5000L);

    // Get updated state
    QueueMetricsState state2 = cache.get(queueName);
    assertSame("Should be same state instance", state1, state2);
    assertNotNull("Snapshot should now be populated", state2.getSnapshot());
    assertEquals("Memory should match", 1.0f, state2.getSnapshot().getMemoryUsedGB(), 0.01f);
  }

  @Test
  public void testConcurrentPutAndGetNoDeadlock() throws Exception {
    setupMockQueueInfo();
    String queueName = "test-concurrent-ops-" + System.nanoTime();
    int iterationsPerThread = 100;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(3);
    ExecutorService executor = Executors.newFixedThreadPool(3);
    AtomicInteger successCount = new AtomicInteger(0);

    // Writer thread 1: putPlaceholder
    executor.submit(() -> {
      try {
        startLatch.await();
        for (int i = 0; i < iterationsPerThread; i++) {
          cache.putPlaceholder(queueName, 5000L);
        }
        successCount.incrementAndGet();
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        doneLatch.countDown();
      }
    });

    // Writer thread 2: put
    executor.submit(() -> {
      try {
        startLatch.await();
        QueueMetricsSnapshot snapshot = new QueueMetricsSnapshot(mockQueueInfo);
        for (int i = 0; i < iterationsPerThread; i++) {
          cache.put(queueName, snapshot, 3000L);
        }
        successCount.incrementAndGet();
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        doneLatch.countDown();
      }
    });

    // Reader thread: get
    executor.submit(() -> {
      try {
        startLatch.await();
        for (int i = 0; i < iterationsPerThread; i++) {
          cache.get(queueName);
        }
        successCount.incrementAndGet();
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        doneLatch.countDown();
      }
    });

    startLatch.countDown();
    assertTrue("All threads should complete without deadlock", doneLatch.await(10, TimeUnit.SECONDS));
    assertEquals("All threads should succeed", 3, successCount.get());

    executor.shutdown();
  }

  // Helper method to setup mock QueueInfo with standard values
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
}

