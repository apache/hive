/**
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

package org.apache.hive.service.cli.session;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hive.common.metrics.MetricsTestUtils;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics;
import org.apache.hadoop.hive.common.metrics.metrics2.MetricsReporting;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.server.HiveServer2;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test metrics from SessionManager.
 */
public class TestSessionManagerMetrics {

  private static SessionManager sm;
  private static CodahaleMetrics metrics;
  private static final int BARRIER_AWAIT_TIMEOUT = 30;
  private static final String FAIL_TO_START_MSG = "The tasks could not be started within "
      + BARRIER_AWAIT_TIMEOUT + " seconds before the %s metrics verification.";
  private static final String FAIL_TO_COMPLETE_MSG = "The tasks could not be completed within "
      + BARRIER_AWAIT_TIMEOUT + " seconds after the %s metrics verification.";
  private final CyclicBarrier ready = new CyclicBarrier(3);
  private final CyclicBarrier completed = new CyclicBarrier(3);

  @BeforeClass
  public static void setup() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_ASYNC_EXEC_THREADS, 2);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_ASYNC_EXEC_WAIT_QUEUE_SIZE, 10);
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_ASYNC_EXEC_KEEPALIVE_TIME, "1000000s");

    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_REPORTER, MetricsReporting.JSON_FILE.name() + "," + MetricsReporting.JMX.name());
    MetricsFactory.init(conf);

    HiveServer2 hs2 = new HiveServer2();
    sm = new SessionManager(hs2);
    sm.init(conf);

    metrics = (CodahaleMetrics) MetricsFactory.getInstance();
  }

  class BarrierRunnable implements Runnable {
    @Override
    public void run() {
      try {
        ready.await();
        completed.await();
      } catch (InterruptedException | BrokenBarrierException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Tests metrics regarding async thread pool.
   *
   * The test does the following steps:
   * - Submit four tasks
   * - Wait with the metrics verification, until the first two tasks are running.
   * If, for some reason, the tasks are not started within a timeout period, make the test fail.
   * - Make the tasks wait until the metrics are checked.
   * - Verify the metrics. Both the EXEC_ASYNC_POOL_SIZE and EXEC_ASYNC_QUEUE_SIZE should be 2.
   * - Let the first two tasks complete, so the remaining two tasks can be removed from the queue and started.
   * - Wait until the remaining tasks are running.
   * Do the metrics check only if they are started to avoid the failures when the queue size was not 0.
   * If, for some reason, the tasks are not started within a timeout period, make the test fail.
   * - Verify the metrics.
   * The EXEC_ASYNC_POOL_SIZE should be 2 and the EXEC_ASYNC_QUEUE_SIZE should be 0.
   * - Let the remaining tasks complete.
   */
  @Test
  public void testThreadPoolMetrics() throws Exception {

    String errorMessage = null;
    try {
      sm.submitBackgroundOperation(new BarrierRunnable());
      sm.submitBackgroundOperation(new BarrierRunnable());
      sm.submitBackgroundOperation(new BarrierRunnable());
      sm.submitBackgroundOperation(new BarrierRunnable());

      errorMessage = String.format(FAIL_TO_START_MSG, "first");
      ready.await(BARRIER_AWAIT_TIMEOUT, TimeUnit.SECONDS);
      ready.reset();

      String json = metrics.dumpJson();
      MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.EXEC_ASYNC_POOL_SIZE, 2);
      MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.EXEC_ASYNC_QUEUE_SIZE, 2);

      errorMessage = String.format(FAIL_TO_COMPLETE_MSG, "first");
      completed.await(BARRIER_AWAIT_TIMEOUT, TimeUnit.SECONDS);
      completed.reset();

      errorMessage = String.format(FAIL_TO_START_MSG, "second");
      ready.await(BARRIER_AWAIT_TIMEOUT, TimeUnit.SECONDS);

      json = metrics.dumpJson();
      MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.EXEC_ASYNC_POOL_SIZE, 2);
      MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.EXEC_ASYNC_QUEUE_SIZE, 0);

      errorMessage = String.format(FAIL_TO_COMPLETE_MSG, "second");
      completed.await(BARRIER_AWAIT_TIMEOUT, TimeUnit.SECONDS);

    } catch (TimeoutException e) {
      Assert.fail(errorMessage);
    }
  }
}
