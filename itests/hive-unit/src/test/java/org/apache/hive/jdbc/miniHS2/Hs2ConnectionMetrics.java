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
package org.apache.hive.jdbc.miniHS2;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.common.metrics.MetricsTestUtils;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Abstract class for connection metrics tests. Implementors of this class are
 * {@link TestHs2ConnectionMetricsBinary} and {@link TestHs2ConnectionMetricsBinary}. These two
 * classes are responsible for testing the connection metrics either in binary or in http mode.
 */

public abstract class Hs2ConnectionMetrics {

  protected static MiniHS2 miniHS2;
  protected static Map<String, String> confOverlay = new HashMap<>();

  protected static final String USERNAME = System.getProperty("user.name");
  protected static final String PASSWORD = "foo";

  public static void setup() throws Exception {
    miniHS2 = new MiniHS2(new HiveConf());

    confOverlay.put(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    confOverlay.put(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
            TestHs2Metrics.MetricCheckingHook.class.getName());
    confOverlay.put(HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED.varname, "true");

    miniHS2.start(confOverlay);
  }

  protected void verifyConnectionMetrics(String metricsJson, int expectedOpenConnections,
                                         int expectedCumulativeConnections) throws Exception {
    MetricsTestUtils.verifyMetricsJson(metricsJson, MetricsTestUtils.COUNTER,
            MetricsConstant.OPEN_CONNECTIONS, expectedOpenConnections);
    MetricsTestUtils.verifyMetricsJson(metricsJson, MetricsTestUtils.COUNTER,
            MetricsConstant.CUMULATIVE_CONNECTION_COUNT, expectedCumulativeConnections);
  }

  public static void tearDown() {
    miniHS2.stop();
  }

}
