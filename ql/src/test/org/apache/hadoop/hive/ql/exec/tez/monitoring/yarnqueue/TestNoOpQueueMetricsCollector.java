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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * Test cases for NoOpQueueMetricsCollector (Null Object pattern implementation).
 */
public class TestNoOpQueueMetricsCollector {

  @Test
  public void testIsEnabledReturnsFalse() {
    assertFalse("isEnabled should return false", NoOpQueueMetricsCollector.INSTANCE.isEnabled());
  }

  @Test
  public void testGetLatestSnapshotReturnsNull() {
    assertNull("getLatestSnapshot should return null", NoOpQueueMetricsCollector.INSTANCE.getLatestSnapshot());
  }

  @Test
  public void testGetQueueNameReturnsEmptyString() {
    assertEquals("Queue name should be empty string", "", NoOpQueueMetricsCollector.INSTANCE.getQueueName());
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

    assertFalse("isEnabled should still return false", collector.isEnabled());
    assertNull("getLatestSnapshot should still return null", collector.getLatestSnapshot());
  }
}

