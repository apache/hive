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

package org.apache.hadoop.hive.llap.tezplugins.metrics;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

/**
 * Tests for {@link LlapTaskSchedulerMetrics}.
 */
public class TestLlapTaskSchedulerMetrics {

  /**
   * Verifies that creating metrics for multiple Tez sessions within the same JVM does not fail.
   * Each call to {@code LlapTaskSchedulerMetrics.create()} with the same display name must return
   * the existing instance without re-registering, otherwise the metrics system (a static singleton)
   * would throw {@code MetricsException: Metrics source X already exists!} on the second call.
   */
  @Test
  public void testCreateIsIdempotentAcrossSessions() {
    LlapTaskSchedulerMetrics first = LlapTaskSchedulerMetrics.create("LlapTaskSchedulerMetrics-hiveserver2",
        "session-1");
    assertNotNull(first);

    // Must not throw MetricsException: Metrics source LlapTaskScheduler already exists!
    LlapTaskSchedulerMetrics second = LlapTaskSchedulerMetrics.create("LlapTaskSchedulerMetrics-hiveserver2",
        "session-2");
    assertNotNull(second);
  }
}
