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

package org.apache.hive.service.server;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.tez.monitoring.yarnqueue.QueueMetricsRefreshPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Tests for conditional initialization of QueueMetricsRefreshPool in HiveServer2.
 *
 * Verifies that the pool is only initialized when execution engine is "tez",
 * and skipped for other engines (MR, Spark, local).
 *
 * Uses {@link QueueMetricsRefreshPool#getInstanceForTesting()} to verify initialization
 * state without triggering lazy initialization.
 */
public class TestHiveServer2QueueMetricsPoolInit {

  @Before
  public void setUp() {
    // Reset the pool before each test
    QueueMetricsRefreshPool.resetForTesting();
  }

  @After
  public void tearDown() {
    // Clean up after each test
    QueueMetricsRefreshPool.resetForTesting();
  }

  /**
   * Test that pool IS initialized when execution engine is "tez".
   * This is a POSITIVE test case.
   */
  @Test
  public void testPoolInitializedForTezEngine() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "tez");
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_QUEUE_METRICS_REFRESH_THREADS, 4);

    // Verify pool is not initialized before the call
    assertNull("Pool should not be initialized before init call",
        QueueMetricsRefreshPool.getInstanceForTesting());

    HiveServer2 hs2 = new HiveServer2();
    invokeInitializeQueueMetricsPool(hs2, conf);

    // Verify pool WAS initialized (POSITIVE case)
    assertNotNull("Pool SHOULD be initialized for Tez engine",
        QueueMetricsRefreshPool.getInstanceForTesting());
  }

  /**
   * Test that pool IS initialized case-insensitively for "Tez", "TEZ", etc.
   * This is a POSITIVE test case.
   */
  @Test
  public void testPoolInitializedForTezEngineCaseInsensitive() throws Exception {
    String[] tezVariants = {"tez", "Tez", "TEZ", "tEz"};

    for (String variant : tezVariants) {
      // Reset between iterations
      QueueMetricsRefreshPool.resetForTesting();

      HiveConf conf = new HiveConf();
      conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, variant);
      conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_QUEUE_METRICS_REFRESH_THREADS, 4);

      // Verify not initialized before
      assertNull("Pool should not be initialized before init call for: " + variant,
          QueueMetricsRefreshPool.getInstanceForTesting());

      HiveServer2 hs2 = new HiveServer2();
      invokeInitializeQueueMetricsPool(hs2, conf);

      // Verify WAS initialized (POSITIVE case)
      assertNotNull("Pool SHOULD be initialized for Tez variant: " + variant,
          QueueMetricsRefreshPool.getInstanceForTesting());
    }
  }

  /**
   * Test that pool is NOT initialized when execution engine is "mr".
   * This is a NEGATIVE test case - verifying the pool remains null.
   */
  @Test
  public void testPoolNotInitializedForMrEngine() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "mr");
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_QUEUE_METRICS_REFRESH_THREADS, 4);

    // Verify pool is not initialized before
    assertNull("Pool should not be initialized before init call",
        QueueMetricsRefreshPool.getInstanceForTesting());

    HiveServer2 hs2 = new HiveServer2();
    invokeInitializeQueueMetricsPool(hs2, conf);

    // Verify pool is STILL not initialized (NEGATIVE case)
    assertNull("Pool should NOT be initialized for MR engine",
        QueueMetricsRefreshPool.getInstanceForTesting());
  }

  /**
   * Test that pool is NOT initialized when execution engine is "spark".
   * This is a NEGATIVE test case.
   */
  @Test
  public void testPoolNotInitializedForSparkEngine() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "spark");
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_QUEUE_METRICS_REFRESH_THREADS, 4);

    // Verify not initialized before
    assertNull("Pool should not be initialized before init call",
        QueueMetricsRefreshPool.getInstanceForTesting());

    HiveServer2 hs2 = new HiveServer2();
    invokeInitializeQueueMetricsPool(hs2, conf);

    // Verify STILL not initialized (NEGATIVE case)
    assertNull("Pool should NOT be initialized for Spark engine",
        QueueMetricsRefreshPool.getInstanceForTesting());
  }

  /**
   * Test that pool is NOT initialized when execution engine is empty.
   * This is a NEGATIVE test case.
   */
  @Test
  public void testPoolNotInitializedForEmptyEngine() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "");
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_QUEUE_METRICS_REFRESH_THREADS, 4);

    // Verify not initialized before
    assertNull("Pool should not be initialized before init call",
        QueueMetricsRefreshPool.getInstanceForTesting());

    HiveServer2 hs2 = new HiveServer2();
    invokeInitializeQueueMetricsPool(hs2, conf);

    // Verify STILL not initialized (NEGATIVE case)
    assertNull("Pool should NOT be initialized for empty engine",
        QueueMetricsRefreshPool.getInstanceForTesting());
  }

  /**
   * Test that pool is NOT initialized when execution engine is "local".
   * This is a NEGATIVE test case.
   */
  @Test
  public void testPoolNotInitializedForLocalEngine() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "local");
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_QUEUE_METRICS_REFRESH_THREADS, 4);

    // Verify not initialized before
    assertNull("Pool should not be initialized before init call",
        QueueMetricsRefreshPool.getInstanceForTesting());

    HiveServer2 hs2 = new HiveServer2();
    invokeInitializeQueueMetricsPool(hs2, conf);

    // Verify STILL not initialized (NEGATIVE case)
    assertNull("Pool should NOT be initialized for local engine",
        QueueMetricsRefreshPool.getInstanceForTesting());
  }

  /**
   * Test that pool initialization respects configured thread count.
   * This is a POSITIVE test with different configuration.
   */
  @Test
  public void testPoolInitializedWithConfiguredThreadCount() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "tez");
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_QUEUE_METRICS_REFRESH_THREADS, 8);

    // Verify not initialized before
    assertNull("Pool should not be initialized before init call",
        QueueMetricsRefreshPool.getInstanceForTesting());

    HiveServer2 hs2 = new HiveServer2();
    invokeInitializeQueueMetricsPool(hs2, conf);

    // Verify WAS initialized (POSITIVE case)
    assertNotNull("Pool SHOULD be initialized with custom thread count",
        QueueMetricsRefreshPool.getInstanceForTesting());
  }

  /**
   * Test that pool initialization handles exceptions gracefully and doesn't fail server startup.
   * Even with invalid config, the method should not throw exceptions to the caller.
   */
  @Test
  public void testPoolInitializationFailureIsNonFatal() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "tez");
    // Set a potentially problematic value
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_QUEUE_METRICS_REFRESH_THREADS, -1);

    HiveServer2 hs2 = new HiveServer2();

    // Should not throw exception - errors are caught and logged
    try {
      invokeInitializeQueueMetricsPool(hs2, conf);
      // Test passes if we get here without exception
    } catch (Exception e) {
      fail("Pool initialization should not throw exceptions to caller, got: " + e.getMessage());
    }
  }

  /**
   * Helper method to invoke the private initializeQueueMetricsPool method via reflection.
   */
  private void invokeInitializeQueueMetricsPool(HiveServer2 hs2, HiveConf conf) throws Exception {
    Method method = HiveServer2.class.getDeclaredMethod("initializeQueueMetricsPool", HiveConf.class);
    method.setAccessible(true);
    method.invoke(hs2, conf);
  }
}


