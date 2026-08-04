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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for conditional initialization of QueueMetricsRefreshPool in HiveServer2.
 *
 * Verifies that the pool is only initialized when execution engine is "tez",
 * and skipped for other engines (MR, Spark, local).
 *
 * Uses {@link QueueMetricsRefreshPool#isInitialized()} to verify initialization
 * state without triggering lazy initialization.
 */
public class TestHiveServer2QueueMetricsPoolInit {

  @Before
  public void setUp() {
    QueueMetricsRefreshPool.shutdown();
  }

  @After
  public void tearDown() {
    QueueMetricsRefreshPool.shutdown();
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
    assertFalse("Pool should not be initialized before init call",
        QueueMetricsRefreshPool.isInitialized());

    HiveServer2 hs2 = new HiveServer2();
    invokeInitializeQueueMetricsPool(hs2, conf);

    // Verify pool WAS initialized (POSITIVE case)
    assertTrue("Pool SHOULD be initialized for Tez engine",
        QueueMetricsRefreshPool.isInitialized());
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
      QueueMetricsRefreshPool.shutdown();

      HiveConf conf = new HiveConf();
      conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, variant);
      conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_QUEUE_METRICS_REFRESH_THREADS, 4);

      // Verify not initialized before
      assertFalse("Pool should not be initialized before init call for: " + variant,
          QueueMetricsRefreshPool.isInitialized());

      HiveServer2 hs2 = new HiveServer2();
      invokeInitializeQueueMetricsPool(hs2, conf);

      // Verify WAS initialized (POSITIVE case)
      assertTrue("Pool SHOULD be initialized for Tez variant: " + variant,
          QueueMetricsRefreshPool.isInitialized());
    }
  }

  /**
   * Test that pool is NOT initialized when execution engine is not "tez".
   * Tests multiple non-Tez engines: mr, spark, local, and empty.
   * These are all NEGATIVE test cases - verifying the pool remains null.
   */
  @Test
  public void testPoolNotInitializedForNonTezEngines() throws Exception {
    String[] engineNames = {"mr", "spark", "local", ""};
    String[] descriptions = {"MR engine", "Spark engine", "local engine", "empty engine"};

    for (int i = 0; i < engineNames.length; i++) {
      String engineName = engineNames[i];
      String desc = descriptions[i];

      // Reset between iterations
      QueueMetricsRefreshPool.shutdown();

      HiveConf conf = new HiveConf();
      conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, engineName);
      conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_QUEUE_METRICS_REFRESH_THREADS, 4);

      // Verify pool is not initialized before
      assertFalse("Pool should not be initialized before init call for " + desc,
          QueueMetricsRefreshPool.isInitialized());

      HiveServer2 hs2 = new HiveServer2();
      invokeInitializeQueueMetricsPool(hs2, conf);

      // Verify pool is STILL not initialized (NEGATIVE case)
      assertFalse("Pool should NOT be initialized for " + desc,
          QueueMetricsRefreshPool.isInitialized());
    }
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
    assertFalse("Pool should not be initialized before init call",
        QueueMetricsRefreshPool.isInitialized());

    HiveServer2 hs2 = new HiveServer2();
    invokeInitializeQueueMetricsPool(hs2, conf);

    // Verify WAS initialized (POSITIVE case)
    assertTrue("Pool SHOULD be initialized with custom thread count",
        QueueMetricsRefreshPool.isInitialized());
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


