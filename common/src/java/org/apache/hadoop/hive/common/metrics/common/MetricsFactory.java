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
package org.apache.hadoop.hive.common.metrics.common;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Class that manages a static Metric instance for this process.
 */
public class MetricsFactory {

  private static Metrics metrics;
  private static Object initLock = new Object();

  public synchronized static void init(HiveConf conf) throws Exception {
    if (metrics == null) {
      metrics = (Metrics) ReflectionUtils.newInstance(conf.getClassByName(
        conf.getVar(HiveConf.ConfVars.HIVE_METRICS_CLASS)), conf);
    }
    metrics.init(conf);
  }

  public synchronized static Metrics getMetricsInstance() {
    return metrics;
  }

  public synchronized static void deInit() throws Exception {
    if (metrics != null) {
      metrics.deInit();
    }
  }
}
