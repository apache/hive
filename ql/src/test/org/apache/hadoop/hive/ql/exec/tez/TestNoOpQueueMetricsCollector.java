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

import org.apache.hadoop.hive.ql.exec.tez.monitoring.yarnqueue.NoOpQueueMetricsCollector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

/**
 * Test cases for NoOpQueueMetricsCollector (Null Object pattern implementation).
 */
public class TestNoOpQueueMetricsCollector {

  @Test
  public void testInstanceIsSingleton() {
    NoOpQueueMetricsCollector instance1 = NoOpQueueMetricsCollector.INSTANCE;
    NoOpQueueMetricsCollector instance2 = NoOpQueueMetricsCollector.INSTANCE;

    assertNotNull("Instance should not be null", instance1);
    assertSame("INSTANCE should return same reference", instance1, instance2);
  }

  @Test
  public void testIsEnabledReturnsFalse() {
    NoOpQueueMetricsCollector collector = NoOpQueueMetricsCollector.INSTANCE;

    assertFalse("isEnabled should return false", collector.isEnabled());
  }

  @Test
  public void testGetLatestSnapshotReturnsNull() {
    NoOpQueueMetricsCollector collector = NoOpQueueMetricsCollector.INSTANCE;

    assertNull("getLatestSnapshot should return null", collector.getLatestSnapshot());
  }

  @Test
  public void testGetQueueNameReturnsEmptyString() {
    NoOpQueueMetricsCollector collector = NoOpQueueMetricsCollector.INSTANCE;

    String queueName = collector.getQueueName();
    assertNotNull("Queue name should not be null", queueName);
    assertEquals("Queue name should be empty string", "", queueName);
  }

  @Test
  public void testShutdownIsNoOp() {
    NoOpQueueMetricsCollector collector = NoOpQueueMetricsCollector.INSTANCE;

    // Should not throw exception
    collector.shutdown();
    // Collector still usable after shutdown - confirms it is truly a no-op
    assertFalse("Collector should remain disabled after shutdown", collector.isEnabled());
    assertNull("Snapshot should remain null after shutdown", collector.getLatestSnapshot());
  }

  @Test
  public void testShutdownIsIdempotent() {
    NoOpQueueMetricsCollector collector = NoOpQueueMetricsCollector.INSTANCE;

    // Multiple calls should all be safe no-ops
    collector.shutdown();
    collector.shutdown();
    collector.shutdown();

    // Verify instance still works after shutdown calls
    assertFalse("isEnabled should still return false", collector.isEnabled());
    assertNull("getLatestSnapshot should still return null", collector.getLatestSnapshot());
  }


  @Test
  public void testNullObjectPatternAllowsSafePolymorphism() {
    // NoOp collector can be used wherever QueueMetricsCollector is expected
    // without null checks
    NoOpQueueMetricsCollector collector = NoOpQueueMetricsCollector.INSTANCE;

    // Simulate typical usage pattern
    if (collector.isEnabled()) {
      // This branch should never execute
      fail("NoOp collector should never report as enabled");
    }

    // Safe to call getLatestSnapshot without null check on collector
    // (though snapshot itself will be null)
    assertNull("Snapshot should be null", collector.getLatestSnapshot());

    // Safe to get queue name without null check
    assertNotNull("Queue name should not be null", collector.getQueueName());

    // Safe to call shutdown without null check
    collector.shutdown(); // No exception
  }

  @Test
  public void testCanBeUsedInPlaceOfNullCollector() {
    // Common pattern: use NoOp instead of null to avoid null checks
    NoOpQueueMetricsCollector collector = NoOpQueueMetricsCollector.INSTANCE;

    // This would NPE if collector was null
    String queueName = collector.getQueueName();
    assertNotNull("Should not throw NPE", queueName);

    boolean enabled = collector.isEnabled();
    assertFalse("Should safely return false", enabled);
  }

  @Test
  public void testToStringDoesNotThrow() {
    NoOpQueueMetricsCollector collector = NoOpQueueMetricsCollector.INSTANCE;

    String str = collector.toString();
    assertNotNull("toString should not return null", str);
  }

  @Test
  public void testHashCodeIsConsistent() {
    NoOpQueueMetricsCollector collector = NoOpQueueMetricsCollector.INSTANCE;

    int hash1 = collector.hashCode();
    int hash2 = collector.hashCode();

    assertEquals("hashCode should be consistent", hash1, hash2);
  }
}

