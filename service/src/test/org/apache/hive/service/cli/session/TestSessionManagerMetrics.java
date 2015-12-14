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

import org.apache.hadoop.hive.common.metrics.MetricsTestUtils;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics;
import org.apache.hadoop.hive.common.metrics.metrics2.MetricsReporting;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.server.HiveServer2;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

/**
 * Test metrics from SessionManager.
 */
public class TestSessionManagerMetrics {

  private static SessionManager sm;
  private static CodahaleMetrics metrics;

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

  final Object barrier = new Object();

  class BarrierRunnable implements Runnable {
    @Override
    public void run() {
      synchronized (barrier) {
        try {
          barrier.wait();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * Tests metrics regarding async thread pool.
   */
  @Test
  public void testThreadPoolMetrics() throws Exception {

    sm.submitBackgroundOperation(new BarrierRunnable());
    sm.submitBackgroundOperation(new BarrierRunnable());
    sm.submitBackgroundOperation(new BarrierRunnable());
    sm.submitBackgroundOperation(new BarrierRunnable());

    String json = metrics.dumpJson();

    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.EXEC_ASYNC_POOL_SIZE, 2);
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.EXEC_ASYNC_QUEUE_SIZE, 2);

    synchronized (barrier) {
      barrier.notifyAll();
    }
    json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.EXEC_ASYNC_POOL_SIZE, 2);
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.EXEC_ASYNC_QUEUE_SIZE, 0);
  }
}
